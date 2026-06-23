//! Q10 wide-string LATE-MATERIALIZATION end-to-end spike (the KILL-GATE).
//!
//! Fresh SF=100 profiling (2026-06-23) reframed Q10's loss: decode is at parity
//! with DuckDB; the dominant engine-fixable excess is carrying the 5 wide
//! customer/nation strings through the 11.46M-row join+agg pipeline. A "narrow"
//! Q10 (group by c_custkey only) runs 1584ms / 18.9 CPU-s — BEATING DuckDB's
//! 1950ms / 20.5 CPU-s — proving the wide strings (1134ms / 13.5 CPU-s) are the
//! lever and the headroom is real.
//!
//! This wires the BANKED-but-never-measured `LateGatherExec` + `BuildRowId` infra
//! into Q10's REAL SF=100 plan to convert the estimate into a number. The
//! transform (mirrors the never-built planner walker):
//!   - build side = customer ⋈ nation (so n_name is gathered too, NOT carried) —
//!     all 6 wide cols + c_custkey retained in the resident build batches.
//!   - `EmatixHashJoinExec` CollectLeft (c_custkey = o_custkey) emits a compact
//!     `__cust_rowid` (u32) instead of the wide strings → narrow 11.46M intermediate.
//!   - RepartitionExec(rowid) → native AggregateExec(SinglePartitioned, gby=[rowid])
//!     → 3.88M groups, i64-key group-id (cheap).
//!   - `LateGatherExec` re-attaches the 6 wide cols at the 3.88M outputs from the
//!     SHARED customer⋈nation build (interleave-gather, no re-scan).
//!   - the stock Sort + SortPreservingMerge are reused on top (revenue@2 DESC).
//!
//! Correctness: row count + revenue sum vs stock (order-independent). A MATCH +
//! a wall under stock (≈ DuckDB parity, per the estimate) is the GO signal; the
//! decisive win comes when stacked with filter-fusion.
//!
//!   TPCH_DATA_DIR (default examples/tpch/data/sf100), TRIALS (5), WARMUPS (2)
//!   DUMP_PLAN=1 to print stock + swapped plans.

use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::{Array, Float64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::aggregate::AggregateExprBuilder;
use datafusion::physical_expr::{Partitioning, PhysicalExpr};
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::expressions::{BinaryExpr, Column, lit};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, collect, displayable};
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::emat_hash_join::JoinColumn;
use ematix_flow_core::emat_hash_join_exec::EmatixHashJoinExec;
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::late_gather_exec::{LateGatherColumn, LateGatherExec};
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
    // EMAT_BATCH_SIZE: a large value makes scans emit few, large batches. The
    // late-gather reattach `interleave`s the wide StringView build cols across the
    // retained build batches; at the default 8192 the 15M build = ~1830 sources →
    // byte-copying interleave (~2.2s). Fewer sources → near-free StringView
    // buffer-sharing gather.
    let mut cfg = SessionConfig::new();
    if let Ok(bs) = std::env::var("EMAT_BATCH_SIZE") {
        if let Ok(n) = bs.parse::<usize>() {
            cfg = cfg.with_batch_size(n);
        }
    }
    let state = preset::with_optimizer_rules(
        SessionStateBuilder::new()
            .with_config(cfg)
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

fn checksum(batches: &[datafusion::arrow::record_batch::RecordBatch]) -> (usize, f64) {
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

fn col_idx(schema: &SchemaRef, name: &str) -> usize {
    schema
        .fields()
        .iter()
        .position(|f| f.name() == name || f.name().ends_with(&format!(".{name}")))
        .unwrap_or_else(|| panic!("column {name} not in schema {:?}", schema))
}

const STOCK_SQL: &str = "select c_custkey, c_name, sum(l_extendedprice*(1-l_discount)) as revenue, \
    c_acctbal, n_name, c_address, c_phone, c_comment \
    from customer, orders, lineitem, nation \
    where c_custkey=o_custkey and l_orderkey=o_orderkey \
    and o_orderdate>=date '1993-10-01' and o_orderdate<date '1994-01-01' \
    and l_returnflag='R' and c_nationkey=n_nationkey \
    group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment \
    order by revenue desc";

// customer ⋈ nation — the wide build side (all 6 wide cols + c_custkey retained).
const BUILD_SQL: &str = "select c_custkey, c_name, c_address, c_phone, c_acctbal, c_comment, n_name \
    from customer, nation where c_nationkey=n_nationkey";

// orders ⋈ lineitem (filtered) — the narrow probe side. Carries o_custkey + revenue cols.
const PROBE_SQL: &str = "select o_custkey, l_extendedprice, l_discount \
    from orders, lineitem \
    where l_orderkey=o_orderkey and o_orderdate>=date '1993-10-01' \
    and o_orderdate<date '1994-01-01' and l_returnflag='R'";

/// Replace the post-aggregate ProjectionExec (over the 7-col group-by) with the
/// late-materialization subtree. Returns (new_plan, fired).
fn lategather_swap(
    plan: Arc<dyn ExecutionPlan>,
    build_plan: Arc<dyn ExecutionPlan>,
    probe_plan: Arc<dyn ExecutionPlan>,
) -> Result<(Arc<dyn ExecutionPlan>, bool), Box<dyn std::error::Error>> {
    let mut fired = false;
    let out = plan.transform_down(|node| {
        // Match the ProjectionExec whose child is the 7-col AggregateExec.
        let is_target = node
            .as_any()
            .downcast_ref::<ProjectionExec>()
            .map(|p| {
                p.children()
                    .first()
                    .and_then(|c| c.as_any().downcast_ref::<AggregateExec>())
                    .map(|a| a.group_expr().expr().len() == 7)
                    .unwrap_or(false)
            })
            .unwrap_or(false);
        if !is_target {
            return Ok(Transformed::no(node));
        }
        let final_schema: SchemaRef = node.schema(); // = Q10 output schema

        // --- EmatixHashJoinExec: c_custkey = o_custkey, emit __cust_rowid ---
        let bsch = build_plan.schema();
        let psch = probe_plan.schema();
        let ck = col_idx(&bsch, "c_custkey");
        let ok = col_idx(&psch, "o_custkey");
        let ep = col_idx(&psch, "l_extendedprice");
        let dc = col_idx(&psch, "l_discount");
        let emat_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("__cust_rowid", DataType::UInt32, false),
            psch.field(ep).clone(),
            psch.field(dc).clone(),
        ]));
        // P1 (lean re-attach) — MEASURED NO-GO, default OFF. Coalescing each build
        // partition into ~1 large batch halves the late-gather interleave (2.6s→1.3s
        // SortExec) BUT CoalesceBatchesExec is a serialization barrier that drops eff
        // 7.2→6.0 and makes wall WORSE (late/stock 0.97→1.31). The reattach is not the
        // dominant obstacle anyway — the EmatixHashJoinExec serial 15M build is (eff ~7
        // vs the narrow DataFusion path's eff 12). EMAT_LG_COALESCE=1 to re-measure.
        let build_for_join: Arc<dyn ExecutionPlan> = if std::env::var("EMAT_LG_COALESCE")
            .map(|v| v != "0")
            .unwrap_or(false)
        {
            #[allow(deprecated)]
            {
                Arc::new(
                    datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec::new(
                        build_plan.clone(),
                        16_000_000,
                    ),
                )
            }
        } else {
            build_plan.clone()
        };
        let join: Arc<EmatixHashJoinExec> = Arc::new(EmatixHashJoinExec::new(
            build_for_join,
            probe_plan.clone(),
            ck,
            ok,
            vec![
                JoinColumn::BuildRowId,
                JoinColumn::Probe(ep),
                JoinColumn::Probe(dc),
            ],
            emat_schema.clone(),
        ));
        let build_once = join.build_once();
        let join_dyn: Arc<dyn ExecutionPlan> = join;

        // --- Repartition by rowid so the agg can run SinglePartitioned ---
        let nparts = join_dyn.output_partitioning().partition_count().max(1);
        let rowid_e: Arc<dyn PhysicalExpr> = Arc::new(Column::new("__cust_rowid", 0));
        let repart: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
            join_dyn,
            Partitioning::Hash(vec![rowid_e.clone()], nparts),
        )?);

        // --- Native AggregateExec(SinglePartitioned, gby=[rowid], sum(revenue)) ---
        // revenue = l_extendedprice * (1 - l_discount)  over emat_schema (1,2).
        let ep_e: Arc<dyn PhysicalExpr> = Arc::new(Column::new("l_extendedprice", 1));
        let dc_e: Arc<dyn PhysicalExpr> = Arc::new(Column::new("l_discount", 2));
        let one_minus_dc: Arc<dyn PhysicalExpr> =
            Arc::new(BinaryExpr::new(lit(1.0_f64), Operator::Minus, dc_e));
        let revenue_e: Arc<dyn PhysicalExpr> =
            Arc::new(BinaryExpr::new(ep_e, Operator::Multiply, one_minus_dc));
        let sum = AggregateExprBuilder::new(
            datafusion::functions_aggregate::sum::sum_udaf(),
            vec![revenue_e],
        )
        .schema(repart.schema())
        .alias("revenue")
        .build()
        .map(Arc::new)?;
        let group_by = PhysicalGroupBy::new_single(vec![(rowid_e, "__cust_rowid".to_string())]);
        let agg: Arc<dyn ExecutionPlan> = Arc::new(AggregateExec::try_new(
            AggregateMode::SinglePartitioned,
            group_by,
            vec![sum],
            vec![None],
            repart.clone(),
            repart.schema(),
        )?);
        // agg output schema = [__cust_rowid (0), revenue (1)].
        let revenue_in_agg = col_idx(&agg.schema(), "revenue");

        // --- LateGatherExec: re-attach the wide cols from the shared build ---
        // Map each final-schema field to Input(revenue) or Build(<build idx by name>).
        let output: Vec<LateGatherColumn> = final_schema
            .fields()
            .iter()
            .map(|f| {
                let n = f.name();
                if n == "revenue" {
                    LateGatherColumn::Input(revenue_in_agg)
                } else {
                    LateGatherColumn::Build(col_idx(&bsch, n))
                }
            })
            .collect();
        let late: Arc<dyn ExecutionPlan> = Arc::new(LateGatherExec::new(
            agg,
            build_once,
            0, // __cust_rowid at agg output index 0
            output,
            final_schema,
        ));
        fired = true;
        Ok(Transformed::yes(late))
    })?;
    Ok((out.data, fired))
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir =
        std::env::var("TPCH_DATA_DIR").unwrap_or_else(|_| "examples/tpch/data/sf100".to_string());
    let trials: usize = std::env::var("TRIALS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);
    let warmups: usize = std::env::var("WARMUPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2);
    let dump = std::env::var("DUMP_PLAN").is_ok();
    unsafe {
        std::env::set_var("EMAT_PLAN_CACHE", "0");
    }
    let ctx = build_ctx(&data_dir)?;

    println!("Q10 LateGather e2e (kill-gate)  data={data_dir}  warmups={warmups} trials={trials}");

    let fresh_stock = |ctx: &SessionContext| {
        let ctx = ctx.clone();
        async move { ctx.sql(STOCK_SQL).await?.create_physical_plan().await }
    };
    // Build the late-mat plan: stock skeleton + grafted subtree (built from sub-SQLs).
    let make_late = |ctx: &SessionContext| {
        let ctx = ctx.clone();
        async move {
            let stock = ctx.sql(STOCK_SQL).await?.create_physical_plan().await?;
            let build = ctx.sql(BUILD_SQL).await?.create_physical_plan().await?;
            let probe = ctx.sql(PROBE_SQL).await?.create_physical_plan().await?;
            let (p, fired) = lategather_swap(stock, build, probe)
                .map_err(|e| datafusion::error::DataFusionError::External(e.to_string().into()))?;
            Ok::<_, datafusion::error::DataFusionError>((p, fired))
        }
    };

    let (late0, fired) = make_late(&ctx).await?;
    println!("LateGather swap fired: {fired}");
    if dump || !fired {
        let stock0 = fresh_stock(&ctx).await?;
        println!(
            "\n===== STOCK =====\n{}",
            displayable(stock0.as_ref()).indent(true)
        );
        println!(
            "\n===== LATE-MAT =====\n{}",
            displayable(late0.as_ref()).indent(true)
        );
    }
    if !fired {
        println!("swap did NOT fire — matcher needs adjustment.");
        return Ok(());
    }

    // Correctness gate. Skipped in single-arm ISOLATED mode (ARM=stock|late) — a
    // late-mat collect here retains a 15M build in mimalloc that starves page cache
    // and pollutes the subsequent isolated stock timing (the SF100 box artifact).
    let arm_early = std::env::var("ARM").unwrap_or_else(|_| "ab".into());
    if arm_early == "ab" {
        let stock_out = collect(fresh_stock(&ctx).await?, ctx.task_ctx()).await?;
        let late_out = collect(make_late(&ctx).await?.0, ctx.task_ctx()).await?;
        let (sr, ss) = checksum(&stock_out);
        let (kr, ks) = checksum(&late_out);
        let correct = sr == kr && (ss - ks).abs() < (ss.abs() * 1e-9 + 1e-6);
        println!(
            "correctness: stock=({sr} rows, sum {ss:.2})  late=({kr} rows, sum {ks:.2})  => {}",
            if correct { "MATCH ✓" } else { "MISMATCH ✗" }
        );
        if !correct {
            println!("ABORT: late-mat plan differs from stock — the spike shape is wrong.");
            return Ok(());
        }
    } // end correctness gate (ab-mode only)

    // EMAT_EXPLAIN_ANALYZE: collect the late plan once (warm) then dump per-node
    // metrics bottom-up to localize where the late-mat CPU goes.
    if std::env::var("EMAT_EXPLAIN_ANALYZE").is_ok() {
        let p = make_late(&ctx).await?.0;
        let _ = collect(p.clone(), ctx.task_ctx()).await?;
        let p = make_late(&ctx).await?.0;
        let _ = collect(p.clone(), ctx.task_ctx()).await?;
        fn dump(node: &Arc<dyn ExecutionPlan>, depth: usize) {
            let name = node.name();
            // Full metric set (so EmatixHashJoinExec's build_time/probe_time show).
            let m = node
                .metrics()
                .map(|s| s.aggregate_by_name().to_string())
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "metrics=[]".into());
            println!("{}{name}  {m}", "  ".repeat(depth));
            for c in node.children() {
                dump(c, depth + 1);
            }
        }
        let p = make_late(&ctx).await?.0;
        let _ = collect(p.clone(), ctx.task_ctx()).await?;
        println!("\n== late-mat per-node metrics (warm) ==");
        dump(&p, 0);
        return Ok(());
    }

    // ARM=stock|late runs ONE arm's trials CONSECUTIVELY (isolated, no A/B
    // interleave) — the interleaved A/B thrashes the 36GB box's page cache
    // (memory's box-artifact). Use two separate invocations for clean floors.
    let arm = std::env::var("ARM").unwrap_or_else(|_| "ab".into());
    if arm == "stock" || arm == "late" {
        for _ in 0..warmups {
            let p = if arm == "stock" {
                fresh_stock(&ctx).await?
            } else {
                make_late(&ctx).await?.0
            };
            let _ = collect(p, ctx.task_ctx()).await?;
        }
        let (mut w, mut c) = (vec![], vec![]);
        for _ in 0..trials {
            let p = if arm == "stock" {
                fresh_stock(&ctx).await?
            } else {
                make_late(&ctx).await?.0
            };
            let c0 = cpu_secs();
            let t = Instant::now();
            let _ = collect(p, ctx.task_ctx()).await?;
            w.push(t.elapsed().as_secs_f64() * 1000.0);
            c.push(cpu_secs() - c0);
        }
        let (wm, cm) = (median(&mut w), median(&mut c));
        println!(
            "\nARM={arm} (isolated)  wall={wm:.1}ms  cpu={cm:.2}s  eff={:.1}",
            cm / (wm / 1000.0)
        );
        println!("DuckDB SF=100 floor: ~1950ms / ~20.5 CPU-s.");
        return Ok(());
    }

    for _ in 0..warmups {
        let _ = collect(fresh_stock(&ctx).await?, ctx.task_ctx()).await?;
        let _ = collect(make_late(&ctx).await?.0, ctx.task_ctx()).await?;
    }
    let (mut ws, mut cs, mut wl, mut cl) = (vec![], vec![], vec![], vec![]);
    for _ in 0..trials {
        let p = fresh_stock(&ctx).await?;
        let c0 = cpu_secs();
        let t = Instant::now();
        let _ = collect(p, ctx.task_ctx()).await?;
        ws.push(t.elapsed().as_secs_f64() * 1000.0);
        cs.push(cpu_secs() - c0);

        let p = make_late(&ctx).await?.0;
        let c0 = cpu_secs();
        let t = Instant::now();
        let _ = collect(p, ctx.task_ctx()).await?;
        wl.push(t.elapsed().as_secs_f64() * 1000.0);
        cl.push(cpu_secs() - c0);
    }
    let (wsm, csm, wlm, clm) = (
        median(&mut ws),
        median(&mut cs),
        median(&mut wl),
        median(&mut cl),
    );
    println!(
        "\n{:<18} {:>9} {:>8} {:>6}",
        "arm", "wall_ms", "cpu_s", "eff"
    );
    println!(
        "{:<18} {wsm:>9.1} {csm:>8.2} {:>6.1}",
        "stock",
        csm / (wsm / 1000.0)
    );
    println!(
        "{:<18} {wlm:>9.1} {clm:>8.2} {:>6.1}",
        "late-mat",
        clm / (wlm / 1000.0)
    );
    println!(
        "\nQ10: stock {wsm:.0}ms vs late-mat {wlm:.0}ms => {:.3}x wall ({:.3}x CPU)",
        wsm / wlm,
        csm / clm
    );
    println!("DuckDB SF=100 floor: ~1950ms / ~20.5 CPU-s.");
    Ok(())
}
