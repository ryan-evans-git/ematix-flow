//! Q10 FD-aggregate operator END-TO-END spike (the load-bearing measurement).
//!
//! The synthetic kernel microbench (`q10_agg_kernel_bench`) showed the real operator
//! beats DataFusion's 7-col group-by by only ~1.10× (memory-viable); the 1.71× was a
//! retain-all-input memory cheat. But the synthetic data uses freshly-built contiguous
//! `StringArray`s, while production Q10 feeds StringView-ish columns decoded from
//! parquet through the joins — and the SF=100 root-cause measured the agg's
//! `time_calculating_group_ids` at 4.86 CPU-s vs the bench's ~2.3. So the microbench
//! may UNDER-represent the production wide-key-encode cost.
//!
//! Rule #1 (profile the real thing, don't infer): this splices `FdAggregateExec` into
//! Q10's REAL physical plan over real TPC-H parquet and A/Bs it against the stock plan,
//! checksum-verified. If the real-query win is materially bigger than 1.10×, the
//! correctness-critical FD-detection rule is justified; if not, the ROI is bounded.
//!
//! SPIKE SCOPE: the key subset is picked by COLUMN NAME ({c_custkey, n_name}) — a
//! TPC-H-specific measurement hack, NOT shippable. The shipped rule will derive the key
//! from proven functional dependencies. Correctness here is checked empirically
//! (row count + revenue sum vs stock). Run at SF=10 (retain-all kernel is memory-safe
//! at SF=10; SF=100 needs the copy-on-first kernel first).
//!
//!   TPCH_DATA_DIR (default examples/tpch/data/sf10), TRIALS (5), WARMUPS (2)

use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::{Array, Float64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, collect, displayable};
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fd_aggregate_exec::FdAggregateExec;
use ematix_flow_core::preset;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TABLES: &[&str] = &["lineitem", "orders", "customer", "nation"];

fn cpu_secs() -> f64 {
    let mut u: libc::rusage = unsafe { std::mem::zeroed() };
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut u) } != 0 {
        return f64::NAN;
    }
    let tv = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
    tv(u.ru_utime) + tv(u.ru_stime)
}
fn median(v: &mut [f64]) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn build_ctx(data_dir: &str) -> Result<SessionContext, Box<dyn std::error::Error>> {
    let state = preset::with_optimizer_rules(
        SessionStateBuilder::new()
            .with_config(SessionConfig::new())
            .with_default_features(),
    )
    .build();
    let ctx = SessionContext::new_with_state(state);
    for t in TABLES {
        let p = format!("{data_dir}/{t}.parquet");
        ctx.register_table(*t, Arc::new(EmatixFastParquetTableProvider::try_new(p)?))?;
    }
    Ok(ctx)
}

/// (rows, revenue_sum) — order-independent equivalence check.
fn checksum(batches: &[RecordBatch]) -> (usize, f64) {
    let mut rows = 0usize;
    let mut sum = 0.0f64;
    for b in batches {
        rows += b.num_rows();
        if let Ok(idx) = b.schema().index_of("revenue") {
            if let Some(a) = b.column(idx).as_any().downcast_ref::<Float64Array>() {
                for i in 0..a.len() {
                    if a.is_valid(i) {
                        sum += a.value(i);
                    }
                }
            }
        }
    }
    (rows, sum)
}

/// Descend through single-child wrappers (Repartition/Coalesce*) to the Partial
/// AggregateExec carrying the original group/sum exprs over the join output.
fn descend_to_partial(cur: &Arc<dyn ExecutionPlan>) -> Option<&AggregateExec> {
    if let Some(a) = cur.as_any().downcast_ref::<AggregateExec>() {
        if *a.mode() == AggregateMode::Partial {
            return Some(a);
        }
    }
    let ch = cur.children();
    if ch.len() == 1 {
        descend_to_partial(ch[0])
    } else {
        None
    }
}

/// Build an FdAggregateExec replacement from the "original" aggregate (the Partial in a
/// 2-phase plan, or a Single agg), using `out_schema` for the exact output field
/// names/types so the swap is transparent to the projection above. `key_names` are the
/// group-column names that form the FD-minimal key (spike: by name).
fn build_fd(
    orig: &AggregateExec,
    child: Arc<dyn ExecutionPlan>,
    needs_repart: bool,
    out_names: &[String],
    key_names: &[&str],
) -> Option<Arc<dyn ExecutionPlan>> {
    let group = orig.group_expr();
    if !group.is_single() {
        return None; // no grouping sets
    }
    let gexprs = group.expr();
    if gexprs.len() < 2 {
        return None;
    }
    let aggr = orig.aggr_expr();
    if aggr.len() != 1 {
        return None;
    }
    let a = &aggr[0];
    if !a.fun().name().eq_ignore_ascii_case("sum") {
        return None;
    }
    if a.field().data_type() != &DataType::Float64 {
        return None;
    }
    let sum_args = a.expressions();
    if sum_args.len() != 1 {
        return None;
    }
    let sum_expr = sum_args[0].clone();

    // out_names = [group names..., sum name] from the FINAL agg's schema.
    if out_names.len() != gexprs.len() + 1 {
        return None;
    }
    let group_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = gexprs
        .iter()
        .enumerate()
        .map(|(i, (e, _))| (e.clone(), out_names[i].clone()))
        .collect();
    let sum_name = out_names[gexprs.len()].clone();

    // key positions: group cols whose name matches a key_name (exact or qualified suffix).
    let key_positions: Vec<usize> = gexprs
        .iter()
        .enumerate()
        .filter(|(_, (_, n))| {
            key_names
                .iter()
                .any(|k| n == k || n.ends_with(&format!(".{k}")))
        })
        .map(|(i, _)| i)
        .collect();
    if key_positions.is_empty() {
        eprintln!(
            "  build_fd: no group col matched key_names {key_names:?}; group names = {:?}",
            gexprs.iter().map(|(_, n)| n).collect::<Vec<_>>()
        );
        return None;
    }

    // The input feeding FdAggregateExec. For a `Single`/`SinglePartitioned` stock agg
    // (Q10), the input is ALREADY hash-partitioned on c_custkey (from the
    // c_custkey=o_custkey join), and since c_custkey -> n_name (FD) that partitioning
    // already places every {c_custkey, n_name} group in one partition — so NO extra
    // shuffle is needed (feed it directly, exactly like the stock SinglePartitioned agg
    // does). Only the 2-phase `Final` case (where we descend to the Partial over the raw
    // join output) needs us to insert the key shuffle ourselves.
    let input: Arc<dyn ExecutionPlan> = if needs_repart {
        let nparts = child.output_partitioning().partition_count().max(1);
        // Partition on the i64 lead key only (c_custkey): it determines the full group
        // (FD), so it's sufficient AND avoids shuffling on the wide composite tuple.
        let lead = key_positions[0];
        let key_exprs = vec![group_exprs[lead].0.clone()];
        Arc::new(RepartitionExec::try_new(child, Partitioning::Hash(key_exprs, nparts)).ok()?)
    } else {
        child
    };
    let op =
        FdAggregateExec::try_new(input, group_exprs, key_positions, sum_expr, sum_name).ok()?;
    Some(Arc::new(op))
}

/// Swap the Q10 customer group-by (a Final/Single AggregateExec cluster) for
/// FdAggregateExec. Returns (new_plan, fired).
fn fd_swap(
    plan: Arc<dyn ExecutionPlan>,
    key_names: &[&str],
) -> Result<(Arc<dyn ExecutionPlan>, bool), Box<dyn std::error::Error>> {
    let mut fired = false;
    let out = plan.transform_down(|node| {
        if let Some(agg) = node.as_any().downcast_ref::<AggregateExec>() {
            let out_names: Vec<String> = agg
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
            // (orig agg carrying the raw exprs, input to feed, whether we must shuffle).
            let target: Option<(&AggregateExec, Arc<dyn ExecutionPlan>, bool)> = match *agg.mode() {
                // Single-phase: input already adequately partitioned → feed directly.
                AggregateMode::Single | AggregateMode::SinglePartitioned => {
                    Some((agg, agg.input().clone(), false))
                }
                // 2-phase: descend to the Partial (raw exprs over join output) → shuffle.
                AggregateMode::Final | AggregateMode::FinalPartitioned => {
                    descend_to_partial(agg.input()).map(|p| (p, p.input().clone(), true))
                }
                AggregateMode::Partial | AggregateMode::PartialReduce => None,
            };
            if let Some((orig, child, needs_repart)) = target {
                if let Some(repl) = build_fd(orig, child, needs_repart, &out_names, key_names) {
                    fired = true;
                    return Ok(Transformed::yes(repl));
                }
            }
        }
        Ok(Transformed::no(node))
    })?;
    Ok((out.data, fired))
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir =
        std::env::var("TPCH_DATA_DIR").unwrap_or_else(|_| "examples/tpch/data/sf10".to_string());
    let trials: usize = std::env::var("TRIALS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);
    let warmups: usize = std::env::var("WARMUPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2);
    let dump = std::env::var("DUMP_PLAN").is_ok();

    let sql = "select c_custkey, c_name, sum(l_extendedprice*(1-l_discount)) as revenue, \
        c_acctbal, n_name, c_address, c_phone, c_comment \
        from customer, orders, lineitem, nation \
        where c_custkey=o_custkey and l_orderkey=o_orderkey \
        and o_orderdate>=date '1993-10-01' and o_orderdate<date '1994-01-01' \
        and l_returnflag='R' and c_nationkey=n_nationkey \
        group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment \
        order by revenue desc";

    // RepartitionExec (in the joins) is single-use per instance, so we rebuild a FRESH
    // physical plan for every collect. Disable the preset plan cache so each build is a
    // distinct plan (no shared Arc → no "partition not used yet").
    unsafe {
        std::env::set_var("EMAT_PLAN_CACHE", "0");
    }
    let ctx = build_ctx(&data_dir)?;
    let key = ["c_custkey", "n_name"];

    // EMAT_EXPLAIN_ANALYZE=1: per-operator compute breakdown of the STOCK Q10 plan
    // (localizes where the SF=100 COMPUTE-EXCESS lives — decode vs join vs agg). Warm
    // once, then print the annotated-with-metrics plan tree.
    if std::env::var("EMAT_EXPLAIN_ANALYZE").is_ok() {
        let _ = collect(
            ctx.sql(sql).await?.create_physical_plan().await?,
            ctx.task_ctx(),
        )
        .await?;
        let ea = ctx
            .sql(&format!("EXPLAIN ANALYZE {sql}"))
            .await?
            .collect()
            .await?;
        for b in &ea {
            let c = b.column(b.num_columns() - 1);
            if let Some(s) = c
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
            {
                for i in 0..s.len() {
                    println!("{}", s.value(i));
                }
            }
        }
        return Ok(());
    }

    println!("Q10 FD-operator e2e spike  data={data_dir}  warmups={warmups} trials={trials}");
    // One plan just to confirm the swap fires + dump structure (never executed below).
    let stock0 = ctx.sql(sql).await?.create_physical_plan().await?;
    let (swap0, fired) = fd_swap(stock0.clone(), &key)?;
    println!("FD-swap fired: {fired}");
    if dump || !fired {
        println!(
            "\n===== STOCK =====\n{}",
            displayable(stock0.as_ref()).indent(true)
        );
        println!(
            "\n===== SWAPPED =====\n{}",
            displayable(swap0.as_ref()).indent(true)
        );
    }
    if !fired {
        println!("\nFD-swap did NOT fire — matcher needs adjustment (see plan above).");
        return Ok(());
    }

    // A fresh, single-use stock physical plan.
    let fresh_stock = |ctx: &SessionContext| {
        let ctx = ctx.clone();
        let sql = sql.to_string();
        async move { ctx.sql(&sql).await?.create_physical_plan().await }
    };

    // Correctness gate: row count + revenue sum must match stock exactly.
    let stock_out = collect(fresh_stock(&ctx).await?, ctx.task_ctx()).await?;
    let swap_plan = fd_swap(fresh_stock(&ctx).await?, &key)?.0;
    let swap_out = collect(swap_plan, ctx.task_ctx()).await?;
    let (sr, ss) = checksum(&stock_out);
    let (kr, ks) = checksum(&swap_out);
    let correct = sr == kr && (ss - ks).abs() < (ss.abs() * 1e-9 + 1e-6);
    println!(
        "correctness: stock=({sr} rows, sum {ss:.2})  swapped=({kr} rows, sum {ks:.2})  => {}",
        if correct { "MATCH ✓" } else { "MISMATCH ✗" }
    );
    if !correct {
        println!("ABORT: swapped plan produces different results — the spike key/shape is wrong.");
        return Ok(());
    }

    // A/B timing: only collect() is timed; the fresh plan build + swap happen BEFORE the
    // timer starts (excluded from both arms equally).
    for _ in 0..warmups {
        let _ = collect(fresh_stock(&ctx).await?, ctx.task_ctx()).await?;
        let p = fd_swap(fresh_stock(&ctx).await?, &key)?.0;
        let _ = collect(p, ctx.task_ctx()).await?;
    }
    let (mut ws, mut cs, mut wk, mut ck) = (vec![], vec![], vec![], vec![]);
    for _ in 0..trials {
        let p = fresh_stock(&ctx).await?;
        let c0 = cpu_secs();
        let t = Instant::now();
        let _ = collect(p, ctx.task_ctx()).await?;
        ws.push(t.elapsed().as_secs_f64() * 1000.0);
        cs.push(cpu_secs() - c0);

        let p = fd_swap(fresh_stock(&ctx).await?, &key)?.0;
        let c0 = cpu_secs();
        let t = Instant::now();
        let _ = collect(p, ctx.task_ctx()).await?;
        wk.push(t.elapsed().as_secs_f64() * 1000.0);
        ck.push(cpu_secs() - c0);
    }
    let (wsm, csm, wkm, ckm) = (
        median(&mut ws),
        median(&mut cs),
        median(&mut wk),
        median(&mut ck),
    );
    println!(
        "\n{:<16} {:>9} {:>8} {:>6}",
        "arm", "wall_ms", "cpu_s", "eff"
    );
    println!(
        "{:<16} {wsm:>9.1} {csm:>8.2} {:>6.1}",
        "stock (DF agg)",
        csm / (wsm / 1000.0)
    );
    println!(
        "{:<16} {wkm:>9.1} {ckm:>8.2} {:>6.1}",
        "FdAggregateExec",
        ckm / (wkm / 1000.0)
    );
    println!(
        "\nQ10 e2e: stock {wsm:.0}ms vs FD-agg {wkm:.0}ms  =>  {:.3}x wall  ({:.3}x CPU)",
        wsm / wkm,
        csm / ckm
    );
    let verdict = if wkm < wsm * 0.97 {
        format!(
            "WIN: FD-agg {:.1}% faster — production wide-encode IS costlier; build the rule",
            (1.0 - wkm / wsm) * 100.0
        )
    } else if wkm > wsm * 1.03 {
        "LOSS: FD-agg slower end-to-end — the repartition tax dominates; bank as infra".to_string()
    } else {
        "NEUTRAL: FD-agg ≈ stock — synthetic 1.10x does NOT translate; ROI bounded, bank as infra"
            .to_string()
    };
    println!("VERDICT: {verdict}");
    Ok(())
}
