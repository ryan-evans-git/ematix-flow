//! Q06.b — A/B Q06 SF=10 across ematix-flow + DuckDB + Polars on a
//! caller-specified `lineitem` parquet file. Lets us compare the
//! Snappy default vs. the LZ4_RAW sibling produced by
//! `rewrite_lineitem_lz4.rs`.
//!
//! Q06 is single-table (no joins on other TPC-H files), so this
//! sidesteps the rest of the triangulation bench's harness. Reads
//! `examples/tpch/queries/q06.sql` and substitutes `lineitem` with the
//! file referenced by `--file` (or `TPCH_LINEITEM_FILE`).
//!
//! Usage:
//! ```bash
//! # Snappy baseline:
//! cargo run --release --features triangulation -p ematix-flow-core \
//!   --example q06_lz4_ab -- lineitem.parquet
//!
//! # LZ4_RAW sibling:
//! cargo run --release --features triangulation -p ematix-flow-core \
//!   --example q06_lz4_ab -- lineitem_lz4.parquet
//! ```

#[cfg(not(feature = "triangulation"))]
fn main() {
    eprintln!("Build with --features triangulation to enable Polars; aborting.");
    std::process::exit(2);
}

#[cfg(feature = "triangulation")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Instant;

    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion::prelude::{SessionConfig, SessionContext};

    let args: Vec<String> = std::env::args().collect();
    let filename = if args.len() > 1 {
        args[1].clone()
    } else {
        std::env::var("TPCH_LINEITEM_FILE").unwrap_or_else(|_| "lineitem.parquet".to_string())
    };
    let dir =
        std::env::var("TPCH_DATA_DIR").unwrap_or_else(|_| "examples/tpch/data/sf10".to_string());
    let trials: usize = std::env::var("TPCH_TRIALS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);
    let warmups: usize = std::env::var("TPCH_WARMUPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2);

    let path = PathBuf::from(&dir).join(&filename);
    if !path.exists() {
        return Err(format!("lineitem file not found: {}", path.display()).into());
    }
    let q06_sql = std::fs::read_to_string("examples/tpch/queries/q06.sql")?;

    println!("=== Q06 SF=10 A/B — single-table ===");
    println!("  file:    {}", path.display());
    println!("  trials:  {trials} (after {warmups} warmups)");
    println!();

    // Build a tokio runtime — Polars uses tokio internally; DuckDB +
    // ematix are run on this same runtime via spawn_blocking where
    // needed.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(14)
        .enable_all()
        .build()?;
    let path_str = path.to_string_lossy().to_string();

    // ---------- ematix-flow ----------
    let path_em = path_str.clone();
    let sql_em = q06_sql.clone();
    let em_result: Result<(f64, f64, usize), String> = rt.block_on(async move {
        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new().with_target_partitions(14))
            .with_default_features()
            .build();
        let ctx = SessionContext::new_with_state(state);
        let prov = EmatixFastParquetTableProvider::try_new(&path_em)
            .map_err(|e| format!("provider: {e}"))?;
        ctx.register_table("lineitem", Arc::new(prov))
            .map_err(|e| format!("register: {e}"))?;
        let mut samples = Vec::with_capacity(trials);
        let mut last_rows = 0usize;
        for trial_idx in 0..(trials + warmups) {
            let t = Instant::now();
            let df = ctx.sql(&sql_em).await.map_err(|e| format!("sql: {e}"))?;
            let batches = df.collect().await.map_err(|e| format!("collect: {e}"))?;
            let ms = t.elapsed().as_secs_f64() * 1000.0;
            if trial_idx >= warmups {
                samples.push(ms);
            }
            last_rows = batches.iter().map(|b| b.num_rows()).sum();
        }
        let mut sorted = samples.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let median = sorted[sorted.len() / 2];
        let mean: f64 = samples.iter().sum::<f64>() / samples.len() as f64;
        let var: f64 =
            samples.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / samples.len() as f64;
        Ok((median, var.sqrt(), last_rows))
    });
    match &em_result {
        Ok((med, sd, rows)) => {
            println!("  ematix-flow  {med:7.2} ms ± {sd:.2}   ({rows} rows)");
        }
        Err(e) => {
            println!("  ematix-flow  FAIL: {e}");
        }
    }

    // ---------- DuckDB ----------
    let (med_dd, sd_dd, rows_dd) = {
        let conn = duckdb::Connection::open_in_memory()?;
        conn.execute_batch("PRAGMA threads=14")?;
        let view_sql = format!(
            "CREATE VIEW lineitem AS SELECT * FROM read_parquet('{path_str}');"
        );
        conn.execute_batch(&view_sql)?;
        let mut samples = Vec::with_capacity(trials);
        let mut last_rows = 0usize;
        for trial_idx in 0..(trials + warmups) {
            let t = Instant::now();
            let mut stmt = conn.prepare(&q06_sql)?;
            let mut rows = stmt.query([])?;
            let mut n = 0usize;
            while let Some(_) = rows.next()? {
                n += 1;
            }
            let ms = t.elapsed().as_secs_f64() * 1000.0;
            if trial_idx >= warmups {
                samples.push(ms);
            }
            last_rows = n;
        }
        let mut sorted = samples.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let median = sorted[sorted.len() / 2];
        let mean: f64 = samples.iter().sum::<f64>() / samples.len() as f64;
        let var: f64 =
            samples.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / samples.len() as f64;
        (median, var.sqrt(), last_rows)
    };
    println!("  DuckDB       {med_dd:7.2} ms ± {sd_dd:.2}   ({rows_dd} rows)");

    // ---------- Polars ---------- (run on dedicated thread to avoid
    // the "outer multi-thread runtime" panic seen in the triangulation
    // bench).
    let (med_pl, sd_pl, rows_pl) = {
        let path_pl = path_str.clone();
        let sql_pl = q06_sql.clone();
        std::thread::spawn(move || {
            use polars::prelude::*;
            use polars::sql::SQLContext;
            let mut ctx = SQLContext::new();
            let pl_path = polars::prelude::PlPath::new(&path_pl);
            let lf =
                LazyFrame::scan_parquet(pl_path, ScanArgsParquet::default()).unwrap();
            ctx.register("lineitem", lf);
            let mut samples = Vec::with_capacity(trials);
            let mut last_rows = 0usize;
            for trial_idx in 0..(trials + warmups) {
                let t = Instant::now();
                let df = ctx.execute(&sql_pl).unwrap().collect().unwrap();
                let ms = t.elapsed().as_secs_f64() * 1000.0;
                if trial_idx >= warmups {
                    samples.push(ms);
                }
                last_rows = df.height();
            }
            let mut sorted = samples.clone();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let median = sorted[sorted.len() / 2];
            let mean: f64 = samples.iter().sum::<f64>() / samples.len() as f64;
            let var: f64 =
                samples.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / samples.len() as f64;
            (median, var.sqrt(), last_rows)
        })
        .join()
        .unwrap()
    };
    println!("  Polars       {med_pl:7.2} ms ± {sd_pl:.2}   ({rows_pl} rows)");

    println!();
    if let Ok((med_em, _, _)) = em_result {
        let best = med_em.min(med_dd).min(med_pl);
        let leader = if best == med_em {
            "ematix-flow"
        } else if best == med_dd {
            "DuckDB"
        } else {
            "Polars"
        };
        println!("Leader: {leader} ({best:.2} ms)");
        println!(
            "ematix vs DuckDB:  {:+.1}%   ematix vs Polars: {:+.1}%",
            (med_em - med_dd) / med_dd * 100.0,
            (med_em - med_pl) / med_pl * 100.0,
        );
    } else {
        println!("ematix-flow failed — DuckDB vs Polars:");
        println!("  DuckDB/Polars ratio: {:+.1}%", (med_dd - med_pl) / med_pl * 100.0);
    }

    Ok(())
}
