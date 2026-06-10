//! Q10.WS.0 de-risk (2026-06-09): is the Q10 SF=100 wide-string hash-shuffle
//! WALL-recoverable, and does any narrow-key re-attach beat canonical?
//!
//! Q10.RC proved the wide customer strings (c_name/c_address/c_phone/c_comment/
//! c_acctbal) cost ~41% of CPU self-time (take_byte_view in the post-join
//! RepartitionExec + Arc::drop_slow). But CPU self-time != wall (those cores may
//! otherwise idle). This A/B measures the WALL sign before building any walker.
//!
//! Four interleaved arms, SF=100, production preset:
//!   A canonical       — GROUP BY 7 wide cols (baseline; the shuffle waste).
//!   B narrow-ceiling  — GROUP BY c_custkey, return c_custkey+revenue only.
//!                       NO wide strings anywhere. (A - B) = the WALL PRIZE: the
//!                       max recoverable if wide-string handling were free. If
//!                       B ~= A, the lever is DEAD (shuffle CPU isn't wall cost).
//!   C aggfetch        — FD-min shape (re-join customer+nation AFTER agg). Known
//!                       -20.6% loser at SF100.2; reference for re-attach tax.
//!   D aggfetch-v2     — drop the redundant customer from the heavy join (GROUP
//!                       BY o_custkey on orders|x|lineitem only), re-attach after.
//!                       Best-case re-attach. If D loses to A, lever ~dead.
//!
//! Decision: lever lives only if (A - B) is large AND D < A (a re-attach exists
//! cheaper than the prize). Then build the partial-agg-boundary walker.
//!
//!   caffeinate -i env TPCH_DATA_DIR=examples/tpch/data/sf100 TRIALS=7 \
//!     cargo run --release -p ematix-flow-core --example q10_ws_derisk --features triangulation

use std::path::Path;
use std::sync::Arc;

use datafusion::datasource::MemTable;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TPCH_TABLES: &[&str] = &[
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];

// A — canonical Q10 (no-LIMIT, matches examples/tpch/queries/q10.sql).
const A_CANON: &str = r#"
select c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) as revenue,
       c_acctbal, n_name, c_address, c_phone, c_comment
from customer, orders, lineitem, nation
where c_custkey = o_custkey and l_orderkey = o_orderkey
  and o_orderdate >= date '1993-10-01' and o_orderdate < date '1994-01-01'
  and l_returnflag = 'R' and c_nationkey = n_nationkey
group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
order by revenue desc
"#;

// B — narrow-ceiling: keep customer in the join (so the ONLY delta vs canonical
// is the wide strings), but GROUP BY c_custkey and return no wide cols. The
// optimizer projects customer to just c_custkey. (A - B) = total wide-string
// wall cost = the max the WS lever could ever recover.
const B_NARROW: &str = r#"
select c_custkey, sum(l_extendedprice * (1 - l_discount)) as revenue
from customer, orders, lineitem
where c_custkey = o_custkey and l_orderkey = o_orderkey
  and o_orderdate >= date '1993-10-01' and o_orderdate < date '1994-01-01'
  and l_returnflag = 'R'
group by c_custkey
order by revenue desc
"#;

// C — aggfetch (FD-min): GROUP BY c_custkey carrying keys through the heavy
// join, then re-join customer+nation for the wide cols. SF100.2 = -20.6%.
const C_AGGFETCH: &str = r#"
with agg as (
  select c_custkey, sum(l_extendedprice * (1 - l_discount)) as revenue
  from customer, orders, lineitem
  where c_custkey = o_custkey and l_orderkey = o_orderkey
    and o_orderdate >= date '1993-10-01' and o_orderdate < date '1994-01-01'
    and l_returnflag = 'R'
  group by c_custkey
)
select a.c_custkey, c.c_name, a.revenue, c.c_acctbal, n.n_name,
       c.c_address, c.c_phone, c.c_comment
from agg a, customer c, nation n
where a.c_custkey = c.c_custkey and c.c_nationkey = n.n_nationkey
order by a.revenue desc
"#;

// D — aggfetch-v2: the heavy join is orders|x|lineitem ONLY (customer dropped;
// c_custkey == o_custkey), GROUP BY o_custkey, then re-attach customer+nation.
// Best-case "narrow keys through heavy join + re-attach" shape.
const D_AGGFETCH2: &str = r#"
with agg as (
  select o_custkey, sum(l_extendedprice * (1 - l_discount)) as revenue
  from orders, lineitem
  where l_orderkey = o_orderkey
    and o_orderdate >= date '1993-10-01' and o_orderdate < date '1994-01-01'
    and l_returnflag = 'R'
  group by o_custkey
)
select a.o_custkey as c_custkey, c.c_name, a.revenue, c.c_acctbal, n.n_name,
       c.c_address, c.c_phone, c.c_comment
from agg a, customer c, nation n
where a.o_custkey = c.c_custkey and c.c_nationkey = n.n_nationkey
order by a.revenue desc
"#;

// F-part-1: the agg alone (heavy orders|x|lineitem, GROUP BY o_custkey) ->
// (o_custkey, revenue). Materialized to a MemTable so the re-attach planner
// SEES it as small and broadcasts it (CollectLeft) — customer streams as the
// probe and its wide strings are NEVER hash-shuffled. This is the lever's true
// premise; arms C/D fail it (Partitioned re-join shuffles 15M customer wide).
const F_AGG_ONLY: &str = r#"
select o_custkey, sum(l_extendedprice * (1 - l_discount)) as revenue
from orders, lineitem
where l_orderkey = o_orderkey
  and o_orderdate >= date '1993-10-01' and o_orderdate < date '1994-01-01'
  and l_returnflag = 'R'
group by o_custkey
"#;

// F-part-2: re-attach over the materialized agg MemTable (broadcast build,
// customer as streaming probe). Timed separately; total = build + re-attach.
const F_REATTACH: &str = r#"
select a.o_custkey as c_custkey, c.c_name, a.revenue, c.c_acctbal, n.n_name,
       c.c_address, c.c_phone, c.c_comment
from agg_mt a, customer c, nation n
where a.o_custkey = c.c_custkey and c.c_nationkey = n.n_nationkey
order by a.revenue desc
"#;

fn build_ctx(
    data_dir: &Path,
    broadcast: bool,
) -> Result<SessionContext, Box<dyn std::error::Error>> {
    let mut config = SessionConfig::new().with_target_partitions(14);
    if broadcast {
        // Raise the CollectLeft thresholds above the ~62MB / 3.88M-row agg so
        // the planner collects+broadcasts it instead of hash-partitioning it
        // (and customer with it). Heavy orders|x|lineitem stays well over.
        config
            .options_mut()
            .optimizer
            .hash_join_single_partition_threshold = 512 * 1024 * 1024;
        config
            .options_mut()
            .optimizer
            .hash_join_single_partition_threshold_rows = 16_000_000;
    }
    let builder = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features();
    let builder = ematix_flow_core::preset::with_optimizer_rules(builder);
    let ctx = SessionContext::new_with_state(builder.build());
    register_tables(&ctx, data_dir)?;
    Ok(ctx)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = std::env::var("TPCH_DATA_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("examples/tpch/data/sf100"));
    let trials: usize = std::env::var("TRIALS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(7);

    let ctx = build_ctx(&data_dir, false)?;
    let ctx_b = build_ctx(&data_dir, true)?;

    // (name, ctx, sql)
    let arms: Vec<(&str, &SessionContext, &str)> = vec![
        ("A canonical (GROUP BY 7 wide)", &ctx, A_CANON),
        ("B narrow-ceiling (GROUP BY ck)", &ctx, B_NARROW),
        ("C aggfetch (FD-min)", &ctx, C_AGGFETCH),
        ("D aggfetch-v2 (o_custkey)", &ctx, D_AGGFETCH2),
        ("E aggfetch-v2 + bcast cfg", &ctx_b, D_AGGFETCH2),
    ];

    // correctness: all produce 3,884,218 rows.
    for (name, c, sql) in &arms {
        let n = run_once(c, sql).await?;
        eprintln!("rows: {name:<32} = {n}");
    }
    // warm
    for (_, c, sql) in &arms {
        let _ = run_once(c, sql).await?;
    }

    let med = |v: &mut Vec<f64>| {
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        v[v.len() / 2]
    };

    let mut times: Vec<Vec<f64>> = vec![Vec::new(); arms.len()];
    for _ in 0..trials {
        for (i, (_, c, sql)) in arms.iter().enumerate() {
            times[i].push(time_once(c, sql).await?);
        }
    }
    let meds: Vec<f64> = times.iter_mut().map(|v| med(v)).collect();

    println!("\n=== Q10 SF=100 wide-string de-risk ({trials} interleaved trials, median) ===");
    let base = meds[0];
    for (i, (name, _, _)) in arms.iter().enumerate() {
        let m = meds[i];
        let delta = (m / base - 1.0) * 100.0;
        let tag = if i == 0 {
            String::new()
        } else {
            format!("  ({delta:+.1}% vs A)")
        };
        println!("  {name:<32} {m:>8.1} ms{tag}");
    }

    // Arm F: the broadcast ceiling. Materialize the agg to a MemTable (guaranteed
    // small stats -> broadcast) and time the re-attach (customer streams as probe;
    // its wide strings never hash-shuffle). total = build + re-attach. UPPER bound
    // on a no-new-shuffle physical re-attach (a fused plan would pipeline these).
    let agg_batches = ctx.sql(F_AGG_ONLY).await?.collect().await?;
    let agg_schema = agg_batches.first().expect("agg non-empty").schema();
    let ctx_f = build_ctx(&data_dir, true)?;
    let mem = MemTable::try_new(agg_schema, vec![agg_batches])?;
    ctx_f.register_table("agg_mt", Arc::new(mem))?;
    let _ = run_once(&ctx_f, F_REATTACH).await?; // warm
    let mut build_t = Vec::new();
    let mut reattach_t = Vec::new();
    for _ in 0..trials {
        build_t.push(time_once(&ctx, F_AGG_ONLY).await?);
        reattach_t.push(time_once(&ctx_f, F_REATTACH).await?);
    }
    let bmed = med(&mut build_t);
    let rmed = med(&mut reattach_t);
    let ftot = bmed + rmed;
    println!(
        "  F MemTable bcast ceiling: build {bmed:.1} + reattach {rmed:.1} = {ftot:>8.1} ms  ({:+.1}% vs A)",
        (ftot / base - 1.0) * 100.0
    );

    let prize = base - meds[1];
    let best_reattach = meds[2].min(meds[3]).min(meds[4]).min(ftot);
    println!(
        "\n  PRIZE (A - B narrow-ceiling) = {prize:.1} ms  ({:.1}% of canonical)",
        prize / base * 100.0
    );
    println!(
        "  best re-attach (min C,D,E,F) = {best_reattach:.1} ms  ({:+.1}% vs A)",
        (best_reattach / base - 1.0) * 100.0
    );
    println!("\nVERDICT GUIDE:");
    println!("  - if B ~= A (small prize): wide-string shuffle is NOT wall cost → lever DEAD.");
    println!(
        "  - if prize large but ALL re-attaches > A: capture costs more than the prize → DEAD."
    );
    println!("  - if best re-attach (esp. E/F broadcast) < A: sign confirmed → build the walker.");
    Ok(())
}

async fn run_once(ctx: &SessionContext, sql: &str) -> Result<usize, Box<dyn std::error::Error>> {
    let batches = ctx.sql(sql).await?.collect().await?;
    Ok(batches.iter().map(|b| b.num_rows()).sum())
}

async fn time_once(ctx: &SessionContext, sql: &str) -> Result<f64, Box<dyn std::error::Error>> {
    let t = std::time::Instant::now();
    let _ = ctx.sql(sql).await?.collect().await?;
    Ok(t.elapsed().as_secs_f64() * 1000.0)
}

fn register_tables(
    ctx: &SessionContext,
    data_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    for t in TPCH_TABLES {
        let path = data_dir.join(format!("{t}.parquet"));
        let use_emat = *t == "lineitem" || *t == "orders";
        if use_emat {
            ctx.register_table(
                *t,
                Arc::new(EmatixFastParquetTableProvider::try_new(
                    path.to_string_lossy(),
                )?),
            )?;
        } else {
            ctx.register_table(
                *t,
                Arc::new(FastParquetTableProvider::try_new(path.to_string_lossy())?),
            )?;
        }
    }
    Ok(())
}
