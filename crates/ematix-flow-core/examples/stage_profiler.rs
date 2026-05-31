//! Per-stage profiler for a single TPC-H query.
//!
//! Walks the executed physical plan and prints one line per
//! ExecutionPlan node: operator name, output partition, output rows,
//! elapsed_compute (ms), and a stable "tree path" indent so the reader
//! can see parent/child relationships.
//!
//! Run:
//!     TPCH_QUERY=1 TPCH_DATA_DIR=examples/tpch/data/sf10 \
//!       cargo run --release -p ematix-flow-core --example stage_profiler
//!
//! Env knobs:
//!   TPCH_DATA_DIR  parquet root (required)
//!   TPCH_QUERY     query number, 1..=22 (required)
//!   TPCH_TRIALS    timed trials whose metrics are aggregated (default 3)
//!   TPCH_WARMUPS   untimed warmups (default 1)
//!
//! Output is plain text on stdout (Markdown-friendly stage table at the
//! end); the caller can redirect to docs/PERF_Q<n>.md.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, displayable};
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::dedupe_aggregate_rule::DedupeAggregateForFloatDeterminism;
use ematix_flow_core::dict_aggregate_rule::EnableDictGroupCountRule;
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fused_aggregate_filter_multi_agg_rule::InjectFilterMultiAggRule;
use ematix_flow_core::fused_aggregate_filter_sum_rule::InjectFilterSumRule;
use ematix_flow_core::push_down_left_semi_rule::PushDownLeftSemiRule;
use ematix_flow_core::robin_hood_sum_f64_exec::EnableRobinHoodSumF64Rule;
use ematix_flow_core::runtime_bloom_sideband_rule::EnableRuntimeBloomSidebandRule;
use ematix_flow_core::swap_semi_join_build_rule::SwapSemiJoinBuildSideRule;
use futures_util::TryStreamExt;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TPCH_TABLES: &[&str] = &[
    "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
];

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .ok_or("workspace root not found")?
        .to_path_buf();
    let data_dir = std::env::var("TPCH_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| workspace.join("examples/tpch/data/sf10"));
    let q: u8 = std::env::var("TPCH_QUERY")
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or("TPCH_QUERY env var required (1..=22)")?;
    let trials: usize = std::env::var("TPCH_TRIALS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3);
    let warmups: usize = std::env::var("TPCH_WARMUPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);

    let sql_path = workspace.join(format!("examples/tpch/queries/q{q:02}.sql"));
    let sql = std::fs::read_to_string(&sql_path)?;

    let ctx = build_ctx(&data_dir).await?;

    println!("# Stage profile — Q{q:02} SF=10");
    println!();
    println!("- data: `{}`", data_dir.display());
    println!("- query: `{}`", sql_path.display());
    println!("- trials: {trials} (after {warmups} warmups)");
    println!();

    // Warmups (untimed, metrics discarded).
    for _ in 0..warmups {
        let df = ctx.sql(&sql).await?;
        let stream = df.execute_stream().await?;
        let _: Vec<_> = stream.try_collect().await?;
    }

    // Timed trials. Sum each node's metrics across trials by walking
    // the plan tree per trial (each trial gets a fresh physical plan,
    // so we keep per-trial structures and merge by tree path).
    let mut per_trial: Vec<TrialReport> = Vec::with_capacity(trials);
    for _ in 0..trials {
        let df = ctx.sql(&sql).await?;
        let plan = df.create_physical_plan().await?;
        let task_ctx = ctx.task_ctx();
        let t0 = Instant::now();
        // Drive every partition: spawn a task per output partition
        // and join. execute_stream() only drives partition 0 in some
        // configs; this captures full work.
        let n_parts = plan.output_partitioning().partition_count();
        let mut handles = Vec::with_capacity(n_parts);
        for p in 0..n_parts {
            let plan_c = Arc::clone(&plan);
            let ctx_c = Arc::clone(&task_ctx);
            handles.push(tokio::spawn(async move {
                let stream = plan_c.execute(p, ctx_c)?;
                let batches: Vec<_> = stream.try_collect().await?;
                Ok::<_, datafusion::error::DataFusionError>(batches)
            }));
        }
        let mut rows = 0usize;
        for h in handles {
            let b = h.await??;
            rows += b.iter().map(|rb| rb.num_rows()).sum::<usize>();
        }
        let wall_ms = t0.elapsed().as_secs_f64() * 1000.0;

        let mut nodes = Vec::new();
        walk(&plan, 0, &mut Vec::new(), &mut nodes);
        per_trial.push(TrialReport {
            wall_ms,
            rows,
            nodes,
            plan_render: format!("{}", displayable(plan.as_ref()).indent(true)),
        });
    }

    // Print plan tree (from first trial — structure is identical
    // across trials).
    println!("## Plan");
    println!();
    println!("```");
    print!("{}", per_trial[0].plan_render);
    println!("```");
    println!();

    // Aggregate by tree path.
    let mut agg: std::collections::BTreeMap<Vec<usize>, AggNode> =
        std::collections::BTreeMap::new();
    for tr in &per_trial {
        for n in &tr.nodes {
            let entry = agg.entry(n.path.clone()).or_insert_with(|| AggNode {
                name: n.name.clone(),
                depth: n.depth,
                first_partition_rows: n.output_rows,
                elapsed_ns_per_trial: Vec::new(),
                output_rows_per_trial: Vec::new(),
            });
            entry.elapsed_ns_per_trial.push(n.elapsed_ns);
            entry.output_rows_per_trial.push(n.output_rows);
        }
    }

    let wall_ms_vec: Vec<f64> = per_trial.iter().map(|t| t.wall_ms).collect();
    let rows_first = per_trial.first().map(|t| t.rows).unwrap_or(0);
    println!("## Wall time");
    println!();
    println!("- per-trial (ms): {wall_ms_vec:?}");
    println!("- median: {:.2} ms", median(&wall_ms_vec));
    println!("- rows: {rows_first}");
    println!();

    // Sort by median elapsed_ns descending and print top-N table.
    let mut rows_sorted: Vec<&AggNode> = agg.values().collect();
    rows_sorted.sort_by(|a, b| {
        median(&to_f64(&b.elapsed_ns_per_trial))
            .partial_cmp(&median(&to_f64(&a.elapsed_ns_per_trial)))
            .unwrap()
    });

    println!("## Stages (top 25 by median elapsed_compute_ms)");
    println!();
    println!("| Rank | Operator | Depth | Median ms | Min ms | Max ms | Out rows |");
    println!("|-----:|:---------|------:|----------:|-------:|-------:|---------:|");
    let total_compute_ms: f64 = rows_sorted
        .iter()
        .map(|n| median(&to_f64(&n.elapsed_ns_per_trial)) / 1e6)
        .sum();
    let median_wall = median(&wall_ms_vec);
    for (i, n) in rows_sorted.iter().take(25).enumerate() {
        let v_ns = to_f64(&n.elapsed_ns_per_trial);
        let med_ms = median(&v_ns) / 1e6;
        let min_ms = v_ns.iter().cloned().fold(f64::INFINITY, f64::min) / 1e6;
        let max_ms = v_ns.iter().cloned().fold(f64::NEG_INFINITY, f64::max) / 1e6;
        let med_rows = median_usize(&n.output_rows_per_trial);
        println!(
            "| {:>4} | {} | {} | {:>9.2} | {:>6.2} | {:>6.2} | {:>8} |",
            i + 1,
            short_name(&n.name),
            n.depth,
            med_ms,
            min_ms,
            max_ms,
            med_rows,
        );
    }
    println!();
    println!(
        "Σ median elapsed_compute across all nodes: {:.2} ms  (vs wall median {:.2} ms; \
         ratio {:.2}× — values >1.0 mean parallel work overlaps)",
        total_compute_ms,
        median_wall,
        total_compute_ms / median_wall.max(0.0001)
    );

    Ok(())
}

#[derive(Clone)]
struct PlanNode {
    name: String,
    depth: usize,
    path: Vec<usize>,
    output_rows: usize,
    elapsed_ns: u64,
}

struct TrialReport {
    wall_ms: f64,
    rows: usize,
    nodes: Vec<PlanNode>,
    plan_render: String,
}

struct AggNode {
    name: String,
    depth: usize,
    #[allow(dead_code)]
    first_partition_rows: usize,
    elapsed_ns_per_trial: Vec<u64>,
    output_rows_per_trial: Vec<usize>,
}

fn walk(
    plan: &Arc<dyn ExecutionPlan>,
    depth: usize,
    path: &mut Vec<usize>,
    out: &mut Vec<PlanNode>,
) {
    let metrics_opt = plan.metrics();
    let (output_rows, elapsed_ns) = if let Some(m) = metrics_opt {
        let agg = m.aggregate_by_name();
        let or = agg
            .iter()
            .find_map(|x| match x.value() {
                datafusion::physical_plan::metrics::MetricValue::OutputRows(c) => Some(c.value()),
                _ => None,
            })
            .unwrap_or(0);
        let ec = agg
            .iter()
            .find_map(|x| match x.value() {
                datafusion::physical_plan::metrics::MetricValue::ElapsedCompute(t) => {
                    Some(t.value() as u64)
                }
                _ => None,
            })
            .unwrap_or(0);
        (or, ec)
    } else {
        (0, 0)
    };
    out.push(PlanNode {
        name: plan.name().to_string(),
        depth,
        path: path.clone(),
        output_rows,
        elapsed_ns,
    });
    for (i, c) in plan.children().iter().enumerate() {
        path.push(i);
        walk(c, depth + 1, path, out);
        path.pop();
    }
}

fn short_name(n: &str) -> String {
    // Some operator names are very long display strings — trim.
    if let Some(idx) = n.find(':') {
        n[..idx].to_string()
    } else {
        n.to_string()
    }
}

fn to_f64(v: &[u64]) -> Vec<f64> {
    v.iter().map(|x| *x as f64).collect()
}

fn median(v: &[f64]) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    let mut s = v.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = s.len();
    if n % 2 == 1 {
        s[n / 2]
    } else {
        (s[n / 2 - 1] + s[n / 2]) / 2.0
    }
}

fn median_usize(v: &[usize]) -> usize {
    if v.is_empty() {
        return 0;
    }
    let mut s = v.to_vec();
    s.sort();
    let n = s.len();
    if n % 2 == 1 {
        s[n / 2]
    } else {
        (s[n / 2 - 1] + s[n / 2]) / 2
    }
}

async fn build_ctx(
    data_dir: &std::path::Path,
) -> Result<SessionContext, Box<dyn std::error::Error>> {
    let partitions: usize = std::env::var("PARTITIONS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(14)
        });
    // Mirror milestone-default rule set in tpch_triangulation_bench's
    // "all" path, minus the bloom-pushdown rule (which only fires when
    // EMAT_BLOOM_PUSHDOWN=1, off here for fewer moving parts).
    let mut builder = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(partitions))
        .with_default_features();
    builder = builder
        .with_physical_optimizer_rule(Arc::new(DedupeAggregateForFloatDeterminism::default()))
        .with_physical_optimizer_rule(Arc::new(EnableDictGroupCountRule))
        .with_physical_optimizer_rule(Arc::new(InjectFilterMultiAggRule))
        .with_physical_optimizer_rule(Arc::new(InjectFilterSumRule))
        .with_physical_optimizer_rule(Arc::new(SwapSemiJoinBuildSideRule))
        .with_physical_optimizer_rule(Arc::new(EnableRobinHoodSumF64Rule::default()))
        .with_physical_optimizer_rule(Arc::new(EnableRuntimeBloomSidebandRule {
            min_probe_to_build_ratio: 1024,
            allow_inner_join: true,
            require_filtered_build: true,
            max_expected_keys_per_partition: 0,
        }))
        .with_optimizer_rule(Arc::new(PushDownLeftSemiRule));
    let state = builder.build();
    let ctx = SessionContext::new_with_state(state);
    for t in TPCH_TABLES {
        let path = data_dir
            .join(format!("{t}.parquet"))
            .to_string_lossy()
            .into_owned();
        let prov = EmatixFastParquetTableProvider::try_new(path)?;
        ctx.register_table(*t, Arc::new(prov))?;
    }
    Ok(ctx)
}
