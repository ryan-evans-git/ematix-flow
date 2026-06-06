//! SF100.2 Phase-1 de-risk: does the "aggregate-then-fetch" rewrite of Q10 win?
//!
//! Canonical Q10 GROUP BYs 7 customer columns (3 wide strings) and carries them
//! through the 11.46M-row customer⋈orders⋈lineitem join → the samply hotspot is
//! `take_byte_view` (~25k) materializing those wide strings through the join +
//! the agg-key hashing (~7k). The rewrite carries ONLY keys through the join+agg
//! (group by c_custkey, sum revenue), then fetches the descriptive columns by
//! joining the small 3.88M agg result back to customer + nation. Semantically
//! identical (c_custkey is the customer PK → FDs the other cols). This measures
//! the CEILING of the group-by-key-minimization lever BEFORE building the rule.
//!
//!   TPCH_DATA_DIR=examples/tpch/data/sf100 TRIALS=7 \
//!     cargo run --release -p ematix-flow-core --example q10_aggfetch_ab --features triangulation

use std::path::Path;
use std::sync::Arc;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TPCH_TABLES: &[&str] = &[
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];

// Canonical Q10 (no LIMIT variant, matches examples/tpch/queries/q10.sql).
const Q10_CANON: &str = r#"
select c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) as revenue,
       c_acctbal, n_name, c_address, c_phone, c_comment
from customer, orders, lineitem, nation
where c_custkey = o_custkey and l_orderkey = o_orderkey
  and o_orderdate >= date '1993-10-01' and o_orderdate < date '1994-01-01'
  and l_returnflag = 'R' and c_nationkey = n_nationkey
group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
order by revenue desc
"#;

// Aggregate-then-fetch: group by c_custkey only (carry just keys through the
// heavy join + agg), then fetch the 7 descriptive columns by joining the small
// agg result back to customer + nation.
const Q10_AGGFETCH: &str = r#"
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

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = std::env::var("TPCH_DATA_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("examples/tpch/data/sf100"));
    let trials: usize = std::env::var("TRIALS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(7);

    let builder = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(14))
        .with_default_features();
    let builder = ematix_flow_core::preset::with_optimizer_rules(builder);
    let ctx = SessionContext::new_with_state(builder.build());
    register_tables(&ctx, &data_dir)?;

    // correctness: both produce the same row count + same top revenue
    let canon_rows = run_once(&ctx, Q10_CANON).await?;
    let aggf_rows = run_once(&ctx, Q10_AGGFETCH).await?;
    eprintln!("rows: canon={canon_rows} aggfetch={aggf_rows} (should match)");

    // warm
    let _ = run_once(&ctx, Q10_CANON).await?;
    let _ = run_once(&ctx, Q10_AGGFETCH).await?;

    let mut canon = Vec::new();
    let mut aggf = Vec::new();
    for _ in 0..trials {
        canon.push(time_once(&ctx, Q10_CANON).await?);
        aggf.push(time_once(&ctx, Q10_AGGFETCH).await?);
    }
    let med = |v: &mut Vec<f64>| {
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        v[v.len() / 2]
    };
    let c = med(&mut canon);
    let a = med(&mut aggf);
    println!("\n=== Q10 SF=100: canonical vs aggregate-then-fetch ({trials} trials) ===");
    println!("canonical (GROUP BY 7 cols):     {c:.1} ms");
    println!("aggregate-then-fetch (GROUP BY 1): {a:.1} ms");
    println!("speedup: {:.3}x  ({:+.1}%)", c / a, (c / a - 1.0) * 100.0);
    println!("\nDuckDB SF=100 Q10 = ~2899 ms. If aggfetch lands near/below that, the");
    println!("group-by-key-minimization rule is worth building. If it ties canonical,");
    println!("the lever is dead (the strings aren't the recoverable cost).");
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
