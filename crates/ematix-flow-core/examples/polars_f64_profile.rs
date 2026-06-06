//! PV.M.3 — profile POLARS decoding the Q15 SCALAR f64 stage. The whole Q15
//! investigation has only ever profiled ematix and INFERRED what Polars does.
//! Polars' 4.9 ms (LZ4) / 17.4 ms (Snappy) on this stage is a constructive
//! proof the floor is far below ours — so measure Polars directly: where does
//! its time go (decompress vs PLAIN-f64 decode vs masked-sum), and in what
//! functions? Diff vs the ematix leaf profile (f64_decode_profile) to name the
//! lever.
//!
//! Runs the SCALAR query in a tight loop on a dedicated std thread (Polars
//! starts its own runtime) so `sample <pid> N` captures the decode hot path.
//! SQLContext built once; only collect() is looped/sampled.
//!
//! Usage (then `sample <pid> 15 1 -file out.txt`):
//!   TPCH_DATA_DIR=examples/tpch/data/sf10 REPS=2000 \
//!     cargo run --release -p ematix-flow-core --features triangulation \
//!     --example polars_f64_profile

use std::path::PathBuf;
use std::time::Instant;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TPCH_TABLES: &[&str] = &[
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];

const SCALAR_SQL: &str = "select sum(l_extendedprice * (1 - l_discount)) from lineitem \
     where l_shipdate >= date '1996-01-01' and l_shipdate < date '1996-04-01'";

fn med(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = std::env::var("TPCH_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("examples/tpch/data/sf10"));
    let which = std::env::var("F64_PROF").unwrap_or_else(|_| "scalar".to_string());
    let reps: usize = std::env::var("REPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2000);

    let sql = match which.as_str() {
        "count" => "select count(*) from lineitem where l_shipdate >= date '1996-01-01' and l_shipdate < date '1996-04-01'".to_string(),
        _ => SCALAR_SQL.to_string(),
    };

    println!(
        "PV.M.3 Polars f64 profile — which={which} reps={reps}\n  sql: {sql}\n  PID for sample: {}",
        std::process::id()
    );

    // Polars on a dedicated std thread (its collect() spins its own runtime).
    let handle = std::thread::spawn(move || -> Result<(), String> {
        use polars::prelude::*;
        use polars::sql::SQLContext;
        let mut ctx = SQLContext::new();
        for t in TPCH_TABLES {
            let path = data_dir.join(format!("{t}.parquet"));
            let pl_path = polars::prelude::PlPath::new(path.to_str().unwrap_or_default());
            let lf = LazyFrame::scan_parquet(pl_path, ScanArgsParquet::default())
                .map_err(|e| e.to_string())?;
            ctx.register(t, lf);
        }
        // Warmup (prime OS cache).
        for _ in 0..5 {
            let _ = ctx.execute(&sql).map_err(|e| e.to_string())?.collect();
        }
        let mut ms = Vec::with_capacity(reps);
        for _ in 0..reps {
            let t = Instant::now();
            let _ = ctx
                .execute(&sql)
                .map_err(|e| e.to_string())?
                .collect()
                .map_err(|e| e.to_string())?;
            ms.push(t.elapsed().as_secs_f64() * 1e3);
        }
        let m = med(ms.clone());
        let mn = ms.iter().cloned().fold(f64::INFINITY, f64::min);
        println!("done: polars median {m:.1} ms  min {mn:.1} ms  ({reps} reps)");
        Ok(())
    });
    handle.join().map_err(|_| "polars thread panicked")??;
    Ok(())
}
