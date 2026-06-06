//! PV.3b Phase-0 — de-risk the (c) snowflake-collapse WIN through the
//! GENERALIZED operator + a REAL fragment split (not PV.2's semantic rewrite).
//!
//! Architect cut (c): fuse only the fact-probing i64 reductions into the
//! lineitem scan —
//!   - `part`   → DenseSet membership on l_partkey  (p_type filter)
//!   - `orders'`→ i64 payload o_orderkey→o_year, REDUCED to AMERICA+date
//!                (the dim group {orders,customer,nation,region} planned as
//!                one subquery keyed by o_orderkey)
//! — emitting a NARROW `(l_suppkey, volume, o_year)` (~0.06% of 60M ≈ 36K
//! rows). Then STOCK DataFusion finishes `⋈ supplier ⋈ nation(n2)` + the
//! mkt_share agg's `CASE WHEN n_name='BRAZIL'`. The string carry, the n1/n2
//! name ambiguity and extract(year) all stay OUT of the fused fragment; emit
//! stays {ProbeColumn, ProbeRevenue, BuildPayload-i64}.
//!
//! Go/no-go: FUSED moves the −16% direction vs full stock Q08 and ideally
//! beats DuckDB (SF=10 167.4 ms). Correctness: fused mkt_share == stock.
//!
//! Usage:
//!   TPCH_DATA_DIR=examples/tpch/data/sf10 TRIALS=11 \
//!     cargo run --release -p ematix-flow-core --example pv3b_q08_ab

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::{Float64Array, Int32Array, Int64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::MemTable;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::emat_push_pipeline_exec::{
    BuildBinding, BuildSource, EmatPushPipelineExec, EmitCol,
};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;
use ematix_flow_push::{ProbeStructure, choose};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TPCH_TABLES: &[&str] = &[
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];

/// The narrow fused-output schema feeding the stock supplier-join + agg (cut c).
fn fused_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("l_suppkey", DataType::Int64, false),
        Field::new("volume", DataType::Float64, false),
        Field::new("o_year", DataType::Int64, false),
    ]))
}

/// The all-3-dims fused-output schema (cut c': supplier' fused too, carrying
/// is_brazil as an i64 payload) → a SIMPLE `CASE WHEN is_brazil=1` agg.
fn fused3_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("o_year", DataType::Int64, false),
        Field::new("volume", DataType::Float64, false),
        Field::new("is_brazil", DataType::Int64, false),
    ]))
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = std::env::var("TPCH_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("examples/tpch/data/sf10"));
    let trials: usize = std::env::var("TRIALS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(11);

    // Production ctx for the stock Q08 baseline; plain ctx for the fused
    // path (PV.2 note: production custom rules mis-project the helper dim
    // queries, so dims + fused scan + stock-top run through a plain ctx).
    let builder = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(14))
        .with_default_features();
    let ctx_prod = SessionContext::new_with_state(
        ematix_flow_core::preset::with_optimizer_rules(builder).build(),
    );
    register(&ctx_prod, &data_dir)?;
    let ctx_plain =
        SessionContext::new_with_config(SessionConfig::new().with_target_partitions(14));
    register(&ctx_plain, &data_dir)?;

    let q08 = std::fs::read_to_string(data_dir.join("../../queries/q08.sql"))
        .or_else(|_| std::fs::read_to_string("examples/tpch/queries/q08.sql"))?;

    eprintln!(
        "PV.3b Q08 A/B — data={}, trials={trials}",
        data_dir.display()
    );

    // Correctness first.
    let stock_share = stock_q08_share(&ctx_prod, &q08).await?;
    let fused_share = fused_q08_share(&ctx_plain).await?;
    let mut keys: Vec<_> = stock_share.keys().copied().collect();
    keys.sort();
    for y in &keys {
        let s = stock_share[y];
        let f = fused_share.get(y).copied().unwrap_or(0.0);
        eprintln!("  o_year={y}: stock={s:.6} fused={f:.6}");
        assert!((s - f).abs() < 1e-6, "fused must match stock for {y}");
    }
    let all3_share = fused_all3_share(&ctx_plain).await?;
    for y in &keys {
        let s = stock_share[y];
        let f = all3_share.get(y).copied().unwrap_or(0.0);
        assert!((s - f).abs() < 1e-6, "fused-all3 must match stock for {y}");
    }
    eprintln!("correctness OK — both fused variants match stock");

    let mut stock = Vec::new();
    let mut fused = Vec::new();
    let mut all3 = Vec::new();
    for _ in 0..trials {
        let t = Instant::now();
        let _ = ctx_prod.sql(&q08).await?.collect().await?;
        stock.push(t.elapsed().as_secs_f64() * 1000.0);

        let t = Instant::now();
        let _ = fused_q08_share(&ctx_plain).await?;
        fused.push(t.elapsed().as_secs_f64() * 1000.0);

        let t = Instant::now();
        let _ = fused_all3_share(&ctx_plain).await?;
        all3.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    let med = |v: &mut Vec<f64>| {
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        v[v.len() / 2]
    };
    let s = med(&mut stock);
    let f = med(&mut fused);
    let a = med(&mut all3);
    println!("\n=== PV.3b Q08 A/B ({trials} trials, 14 partitions) ===");
    println!("STOCK production Q08:                      {s:7.1} ms");
    println!(
        "(c)  fuse part+orders' → stock supplier:   {f:7.1} ms   ({:+.1}%)",
        (f / s - 1.0) * 100.0
    );
    println!(
        "(c') fuse ALL 3 dims → simple agg:         {a:7.1} ms   ({:+.1}%)",
        (a / s - 1.0) * 100.0
    );
    println!("DuckDB Q08 SF=10 reference:                  167.4 ms");
    let dc = (1.0 - f / s) * 100.0;
    let dc2 = (1.0 - a / s) * 100.0;
    println!("\nPV.3b: (c)={dc:.1}%  (c')={dc2:.1}% vs stock.");
    println!(
        "  → supplier-fusion contributes {:.1} pts. {}",
        dc2 - dc,
        if dc2 >= 12.0 {
            "GO with all-3 fusion (needs agg-CASE rewrite); (c) rejected."
        } else {
            "neither banks the target; rethink."
        }
    );
    Ok(())
}

/// Build `part` (membership) + `orders'` (o_orderkey→o_year, AMERICA+date)
/// probe structures from the small dim subqueries, via `choose`.
async fn build_probes(
    ctx: &SessionContext,
) -> Result<(Arc<ProbeStructure>, Arc<ProbeStructure>), Box<dyn std::error::Error>> {
    let mut part_keys = Vec::new();
    for b in &ctx
        .sql("SELECT p_partkey FROM part WHERE p_type = 'ECONOMY ANODIZED STEEL'")
        .await?
        .collect()
        .await?
    {
        let k = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        (0..k.len()).for_each(|i| part_keys.push(k.value(i)));
    }
    let (mut ok_keys, mut ok_year) = (Vec::new(), Vec::new());
    for b in &ctx
        .sql(
            "SELECT o.o_orderkey, extract(year FROM o.o_orderdate) AS y \
             FROM orders o JOIN customer c ON o.o_custkey = c.c_custkey \
             JOIN nation n ON c.c_nationkey = n.n_nationkey \
             JOIN region r ON n.n_regionkey = r.r_regionkey \
             WHERE r.r_name = 'AMERICA' \
               AND o.o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'",
        )
        .await?
        .collect()
        .await?
    {
        let ok = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let y32 = b.column(1).as_any().downcast_ref::<Int32Array>();
        let y64 = b.column(1).as_any().downcast_ref::<Int64Array>();
        for i in 0..ok.len() {
            let y = match (y32, y64) {
                (Some(a), _) => a.value(i) as i64,
                (_, Some(a)) => a.value(i),
                _ => continue,
            };
            ok_keys.push(ok.value(i));
            ok_year.push(y);
        }
    }
    Ok((
        Arc::new(choose(&part_keys, None)),
        Arc::new(choose(&ok_keys, Some(&ok_year))),
    ))
}

/// Execute the fused (c) plan → stock supplier-join + agg → mkt_share/year.
async fn fused_q08_share(
    ctx: &SessionContext,
) -> Result<HashMap<i64, f64>, Box<dyn std::error::Error>> {
    let (part_ps, orders_ps) = build_probes(ctx).await?;

    // probe = lineitem projected to [l_partkey, l_orderkey, l_suppkey,
    // l_extendedprice, l_discount] (cols 0..4).
    let scan = ctx
        .sql("SELECT l_partkey, l_orderkey, l_suppkey, l_extendedprice, l_discount FROM lineitem")
        .await?
        .create_physical_plan()
        .await?;
    let fused: Arc<dyn ExecutionPlan> = Arc::new(EmatPushPipelineExec::new(
        scan,
        vec![
            BuildBinding {
                source: BuildSource::Prebuilt(part_ps),
                probe_fk_col: 0, // l_partkey
                require_unique: false,
            },
            BuildBinding {
                source: BuildSource::Prebuilt(orders_ps),
                probe_fk_col: 1, // l_orderkey
                require_unique: false,
            },
        ],
        vec![
            EmitCol::ProbeColumn { col: 2 }, // l_suppkey
            EmitCol::ProbeRevenue {
                price_col: 3,
                disc_col: 4,
            }, // volume
            EmitCol::BuildPayload { build_idx: 1 }, // o_year
        ],
        fused_schema(),
    ));
    let batches = collect(fused, ctx.task_ctx()).await?;

    // STOCK remainder: ⋈ supplier ⋈ nation(n2) + the mkt_share agg.
    let mem = MemTable::try_new(fused_schema(), vec![batches])?;
    ctx.register_table("fused_li", Arc::new(mem))?;
    let res = ctx
        .sql(
            "SELECT o_year, \
               sum(CASE WHEN n.n_name = 'BRAZIL' THEN volume ELSE 0 END) / sum(volume) AS mkt_share \
             FROM fused_li f \
             JOIN supplier s ON f.l_suppkey = s.s_suppkey \
             JOIN nation n ON s.s_nationkey = n.n_nationkey \
             GROUP BY o_year ORDER BY o_year",
        )
        .await?
        .collect()
        .await?;
    ctx.deregister_table("fused_li")?;

    let mut out = HashMap::new();
    for b in &res {
        let y = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let m = b.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
        for i in 0..b.num_rows() {
            out.insert(y.value(i), m.value(i));
        }
    }
    Ok(out)
}

/// Cut c': fuse ALL THREE dims (supplier' carries is_brazil 1/0 as an i64
/// payload), emit `(o_year, volume, is_brazil)`, feed a SIMPLE
/// `sum(CASE WHEN is_brazil=1 …)` agg. This is the DuckDB-style plan +
/// PV.2's shape, but measured here on the SAME run as cut (c) for an
/// apples-to-apples comparison.
async fn fused_all3_share(
    ctx: &SessionContext,
) -> Result<HashMap<i64, f64>, Box<dyn std::error::Error>> {
    let (part_ps, orders_ps) = build_probes(ctx).await?;
    // supplier' : s_suppkey → is_brazil (1/0), computed in the dim build.
    let (mut sk, mut isbr) = (Vec::new(), Vec::new());
    for b in &ctx
        .sql(
            "SELECT s.s_suppkey, CASE WHEN n.n_name = 'BRAZIL' THEN 1 ELSE 0 END AS is_br \
             FROM supplier s JOIN nation n ON s.s_nationkey = n.n_nationkey",
        )
        .await?
        .collect()
        .await?
    {
        let k = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let v32 = b.column(1).as_any().downcast_ref::<Int32Array>();
        let v64 = b.column(1).as_any().downcast_ref::<Int64Array>();
        for i in 0..k.len() {
            sk.push(k.value(i));
            isbr.push(match (v32, v64) {
                (Some(a), _) => a.value(i) as i64,
                (_, Some(a)) => a.value(i),
                _ => 0,
            });
        }
    }
    let supplier_ps = Arc::new(choose(&sk, Some(&isbr)));

    let scan = ctx
        .sql("SELECT l_partkey, l_orderkey, l_suppkey, l_extendedprice, l_discount FROM lineitem")
        .await?
        .create_physical_plan()
        .await?;
    let fused: Arc<dyn ExecutionPlan> = Arc::new(EmatPushPipelineExec::new(
        scan,
        vec![
            BuildBinding {
                source: BuildSource::Prebuilt(part_ps),
                probe_fk_col: 0,
                require_unique: false,
            },
            BuildBinding {
                source: BuildSource::Prebuilt(orders_ps),
                probe_fk_col: 1,
                require_unique: false,
            },
            BuildBinding {
                source: BuildSource::Prebuilt(supplier_ps),
                probe_fk_col: 2,
                require_unique: false,
            },
        ],
        vec![
            EmitCol::BuildPayload { build_idx: 1 }, // o_year
            EmitCol::ProbeRevenue {
                price_col: 3,
                disc_col: 4,
            }, // volume
            EmitCol::BuildPayload { build_idx: 2 }, // is_brazil
        ],
        fused3_schema(),
    ));
    let batches = collect(fused, ctx.task_ctx()).await?;
    let mem = MemTable::try_new(fused3_schema(), vec![batches])?;
    ctx.register_table("fused3", Arc::new(mem))?;
    let res = ctx
        .sql(
            "SELECT o_year, \
               sum(CASE WHEN is_brazil = 1 THEN volume ELSE 0 END) / sum(volume) AS mkt_share \
             FROM fused3 GROUP BY o_year ORDER BY o_year",
        )
        .await?
        .collect()
        .await?;
    ctx.deregister_table("fused3")?;
    let mut out = HashMap::new();
    for b in &res {
        let y = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let m = b.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
        for i in 0..b.num_rows() {
            out.insert(y.value(i), m.value(i));
        }
    }
    Ok(out)
}

/// Stock Q08 (o_year -> mkt_share); o_year is Int32 (extract(year)) in DF 53.
async fn stock_q08_share(
    ctx: &SessionContext,
    q08: &str,
) -> Result<HashMap<i64, f64>, Box<dyn std::error::Error>> {
    let batches = ctx.sql(q08).await?.collect().await?;
    let mut out = HashMap::new();
    for b in &batches {
        let y64 = b.column(0).as_any().downcast_ref::<Int64Array>();
        let y32 = b.column(0).as_any().downcast_ref::<Int32Array>();
        let m = b.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
        for i in 0..b.num_rows() {
            let year = match (y64, y32) {
                (Some(a), _) => a.value(i),
                (_, Some(a)) => a.value(i) as i64,
                _ => panic!("o_year neither Int64 nor Int32"),
            };
            out.insert(year, m.value(i));
        }
    }
    Ok(out)
}

fn register(ctx: &SessionContext, dir: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    for t in TPCH_TABLES {
        let p = dir.join(format!("{t}.parquet"));
        if *t == "lineitem" || *t == "orders" {
            ctx.register_table(
                *t,
                Arc::new(EmatixFastParquetTableProvider::try_new(
                    p.to_string_lossy(),
                )?),
            )?;
        } else {
            ctx.register_table(
                *t,
                Arc::new(FastParquetTableProvider::try_new(p.to_string_lossy())?),
            )?;
        }
    }
    Ok(())
}
