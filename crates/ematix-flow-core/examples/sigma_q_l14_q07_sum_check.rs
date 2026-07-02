//! Σ.Q.L14 slice 1 — verify Q07 SF=10's actual SUM values match DuckDB
//! when Inner-join L9 is ON vs OFF. The bench only checks row counts;
//! if Inner-L9 was silently corrupting sums on Q07 (the way it
//! corrupted Q21's row count), we need to know before re-enabling it.

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::dedupe_aggregate_rule::DedupeAggregateForFloatDeterminism;
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;
use ematix_flow_core::runtime_bloom_sideband_rule::EnableRuntimeBloomSidebandRule;

const TPCH_TABLES: &[&str] = &[
    "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
];

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir =
        std::env::var("TPCH_DATA_DIR").unwrap_or_else(|_| "examples/tpch/data/sf10".to_string());
    let sql = std::fs::read_to_string("examples/tpch/queries/q07.sql")?;

    println!("===== DuckDB Q07 SF=10 result =====");
    {
        let conn = duckdb::Connection::open_in_memory()?;
        conn.execute_batch("PRAGMA threads=14")?;
        for t in TPCH_TABLES {
            let path = PathBuf::from(&dir).join(format!("{t}.parquet"));
            let stmt = format!(
                "CREATE VIEW {t} AS SELECT * FROM read_parquet('{}')",
                path.display()
            );
            conn.execute_batch(&stmt)?;
        }
        let mut stmt = conn.prepare(&sql)?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let supp: String = row.get(0)?;
            let cust: String = row.get(1)?;
            // DuckDB's `extract(year from l_shipdate)` returns BIGINT.
            let year: i64 = row.get(2)?;
            let rev: f64 = row.get(3)?;
            println!("  {supp:8} | {cust:8} | {year} | {rev:.4}");
        }
    }

    async fn run_emat(
        dir: &str,
        sql: &str,
        allow_inner: bool,
        label: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        println!();
        println!("===== ematix-flow Q07 SF=10 result ({label}) =====");
        let mut builder = SessionStateBuilder::new()
            .with_config(SessionConfig::new().with_target_partitions(14))
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(DedupeAggregateForFloatDeterminism::default()));
        builder = builder.with_physical_optimizer_rule(Arc::new(EnableRuntimeBloomSidebandRule {
            min_probe_to_build_ratio: 64,
            allow_inner_join: allow_inner,
            require_filtered_build: false,
            max_expected_keys_per_partition: 0,
            min_probe_proj_cols: 0,
            // Σ.AH.2: env-resolved NDV ceiling (EMAT_L9_NDV_MAX_ROWS /
            // EMAT_L9_PARTITIONED) + any future fields track the default.
            ..EnableRuntimeBloomSidebandRule::default()
        }));
        let state = builder.build();
        let ctx = SessionContext::new_with_state(state);
        for t in TPCH_TABLES {
            let path = PathBuf::from(dir).join(format!("{t}.parquet"));
            let use_emat = *t == "lineitem" || *t == "orders";
            if use_emat {
                let prov = EmatixFastParquetTableProvider::try_new(path.to_string_lossy())?;
                ctx.register_table(*t, Arc::new(prov))?;
            } else {
                let prov = FastParquetTableProvider::try_new(path.to_string_lossy())?;
                ctx.register_table(*t, Arc::new(prov))?;
            }
        }
        let _ = ctx.sql(sql).await?.collect().await?;
        let batches = ctx.sql(sql).await?.collect().await?;
        let formatted = pretty_format_batches(&batches)?;
        println!("{formatted}");
        Ok(())
    }

    run_emat(&dir, &sql, false, "Inner-L9 OFF (current default)").await?;
    run_emat(&dir, &sql, true, "Inner-L9 ON (previously broken default)").await?;

    Ok(())
}
