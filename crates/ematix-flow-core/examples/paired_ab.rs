//! Paired, interleaved ematix-vs-DuckDB A/B with a sign test.
//!
//! WHY THIS EXISTS: the triangulation bench runs all N trials of one
//! engine, then all N of the next. That is an *unpaired* comparison —
//! slow common-mode drift (thermal throttling, background load, OS
//! scheduler) inflates each engine's per-series σ and can swamp a
//! small-but-real difference. If the gap is ~5 ms but each series has
//! σ≈15 ms, an unpaired mean comparison calls it "noise" — wrongly.
//!
//! This harness instead measures the two engines BACK TO BACK within
//! each trial (alternating which runs first to cancel order bias) and
//! analyses the per-trial DIFFERENCE d_i = t_emat,i − t_duck,i. Shared
//! common-mode noise cancels in the pair, so σ(d) << σ(each series),
//! and a SIGN TEST (in how many of N trials was ematix slower?) detects
//! a consistently-signed gap regardless of absolute σ. A gap present
//! with the same sign in ~every trial is, by definition, NOT noise.
//!
//!   TPCH_DATA_DIR=examples/tpch/data/sf10 TPCH_QUERIES=8,15 \
//!     TPCH_TRIALS=21 TPCH_WARMUPS=3 \
//!     cargo run --release -p ematix-flow-core --example paired_ab \
//!       --features triangulation
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::dedupe_aggregate_rule::DedupeAggregateForFloatDeterminism;
use ematix_flow_core::dict_aggregate_rule::EnableDictGroupCountRule;
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;
use ematix_flow_core::fused_aggregate_filter_multi_agg_rule::InjectFilterMultiAggRule;
use ematix_flow_core::fused_aggregate_filter_sum_rule::InjectFilterSumRule;
use ematix_flow_core::push_down_left_semi_rule::PushDownLeftSemiRule;
use ematix_flow_core::runtime_bloom_sideband_rule::EnableRuntimeBloomSidebandRule;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TPCH_TABLES: &[&str] = &[
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = std::env::var("TPCH_DATA_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("examples/tpch/data/sf10"));
    let queries: Vec<u8> = std::env::var("TPCH_QUERIES")
        .unwrap_or_else(|_| "8,15".into())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    let trials: usize = std::env::var("TPCH_TRIALS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(21);
    let warmups: usize = std::env::var("TPCH_WARMUPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3);

    eprintln!(
        "paired A/B: data={} queries={:?} trials={} warmups={}",
        data_dir.display(),
        queries,
        trials,
        warmups
    );

    // NOTE: the ematix SessionContext is built FRESH per trial (below),
    // untimed. Reusing one ctx across trials lets session-scoped caches —
    // notably the Σ.P SharedSubtreeExec subquery-CSE registry — replay
    // cached batches on trials 2..N, so a query like Q15 (which references
    // its revenue CTE twice) would "run" in microseconds by replaying, not
    // recomputing. Fresh-ctx-per-trial matches the canonical triangulation
    // protocol and measures real single-query latency. The process-global
    // rg-decode-cache (production default-on) intentionally DOES persist
    // across trials, since that's production-representative for repeated
    // queries; disable it with EMAT_RG_DECODE_CACHE=0 for a cold scan.

    // ---- duckdb connection (mirrors duckdb_profile_dump.rs) ----
    let conn = duckdb::Connection::open_in_memory()?;
    conn.execute_batch("PRAGMA threads=14")?;
    for t in TPCH_TABLES {
        let path = data_dir.join(format!("{t}.parquet"));
        conn.execute_batch(&format!(
            "CREATE VIEW {t} AS SELECT * FROM read_parquet('{}')",
            path.display()
        ))?;
    }

    println!(
        "\n{:<5} {:>10} {:>10}   {:>10} {:>9}   {:>14}",
        "Q", "emat med", "duck med", "Δ med", "ratio", "sign(emat slow)"
    );
    println!("{}", "-".repeat(72));

    for &q in &queries {
        let sql = std::fs::read_to_string(format!("examples/tpch/queries/q{q:02}.sql"))?;

        // Warm both engines (OS page cache + any process-global caches +
        // DuckDB's parquet metadata cache). Default config = production.
        for _ in 0..warmups {
            let ctx = build_ematix_ctx(&data_dir)?;
            let _ = ctx.sql(&sql).await?.collect().await?;
            let _ = run_duckdb(&conn, &sql)?;
        }

        let mut emat = Vec::with_capacity(trials);
        let mut duck = Vec::with_capacity(trials);
        let mut diffs = Vec::with_capacity(trials);
        let mut emat_slower = 0usize;
        // Sanity: confirm both engines return the same row count.
        let mut emat_rows = 0usize;
        let mut duck_rows = 0usize;

        for t in 0..trials {
            // Fresh ctx per trial (untimed) — resets session-scoped caches
            // (SharedSubtreeExec registry, plan cache) so each trial does
            // real work. Provider construction / table registration is the
            // only cost here and it is outside the timed region.
            let ctx = build_ematix_ctx(&data_dir)?;
            // Alternate order each trial to cancel any first-mover bias.
            let (e_ms, e_rows, d_ms, d_rows) = if t % 2 == 0 {
                let (e_ms, e_rows) = time_ematix(&ctx, &sql).await?;
                let (d_ms, d_rows) = run_duckdb(&conn, &sql)?;
                (e_ms, e_rows, d_ms, d_rows)
            } else {
                let (d_ms, d_rows) = run_duckdb(&conn, &sql)?;
                let (e_ms, e_rows) = time_ematix(&ctx, &sql).await?;
                (e_ms, e_rows, d_ms, d_rows)
            };
            emat_rows = e_rows;
            duck_rows = d_rows;
            let d = e_ms - d_ms;
            if d > 0.0 {
                emat_slower += 1;
            }
            emat.push(e_ms);
            duck.push(d_ms);
            diffs.push(d);
        }

        let em = median(&mut emat.clone());
        let dm = median(&mut duck.clone());
        let (e_mean, e_std) = mean_std(&emat);
        let (d_mean, d_std) = mean_std(&duck);
        let (diff_mean, diff_std) = mean_std(&diffs);
        // Paired t-statistic and sign-test z (normal approx of Binomial(N,0.5)).
        let t_stat = if diff_std > 0.0 {
            diff_mean / (diff_std / (trials as f64).sqrt())
        } else {
            f64::INFINITY
        };
        let sign_z = (emat_slower as f64 - trials as f64 / 2.0) / (trials as f64 / 4.0).sqrt();

        println!(
            "Q{q:02}  {:>9.2} {:>10.2}   {:>+9.2} {:>8.3}x   {:>4}/{:<4}",
            em,
            dm,
            em - dm,
            em / dm,
            emat_slower,
            trials,
        );
        println!(
            "       emat {e_mean:.2}±{e_std:.2}  duck {d_mean:.2}±{d_std:.2}  | \
             paired Δ {diff_mean:+.2}±{diff_std:.2} ms  t={t_stat:+.1}  sign-z={sign_z:+.1}  \
             rows emat={emat_rows} duck={duck_rows}{}",
            if emat_rows == duck_rows {
                ""
            } else {
                "  *** ROW MISMATCH ***"
            }
        );
    }

    Ok(())
}

async fn time_ematix(
    ctx: &SessionContext,
    sql: &str,
) -> Result<(f64, usize), Box<dyn std::error::Error>> {
    let t0 = Instant::now();
    let batches = ctx.sql(sql).await?.collect().await?;
    let ms = t0.elapsed().as_secs_f64() * 1000.0;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    Ok((ms, rows))
}

fn run_duckdb(
    conn: &duckdb::Connection,
    sql: &str,
) -> Result<(f64, usize), Box<dyn std::error::Error>> {
    let t0 = Instant::now();
    let mut stmt = conn.prepare(sql)?;
    let mut rows = stmt.query([])?;
    let mut n = 0usize;
    while let Some(_r) = rows.next()? {
        n += 1;
    }
    let ms = t0.elapsed().as_secs_f64() * 1000.0;
    Ok((ms, n))
}

fn median(v: &mut [f64]) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = v.len();
    if n == 0 {
        return f64::NAN;
    }
    if n % 2 == 1 {
        v[n / 2]
    } else {
        (v[n / 2 - 1] + v[n / 2]) / 2.0
    }
}

fn mean_std(v: &[f64]) -> (f64, f64) {
    let n = v.len() as f64;
    if n == 0.0 {
        return (f64::NAN, f64::NAN);
    }
    let mean = v.iter().sum::<f64>() / n;
    let var = v.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n;
    (mean, var.sqrt())
}

fn build_ematix_ctx(data_dir: &Path) -> Result<SessionContext, Box<dyn std::error::Error>> {
    let mut builder = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(14))
        .with_default_features()
        .with_physical_optimizer_rule(Arc::new(DedupeAggregateForFloatDeterminism::default()))
        .with_physical_optimizer_rule(Arc::new(EnableDictGroupCountRule))
        .with_physical_optimizer_rule(Arc::new(InjectFilterMultiAggRule))
        .with_physical_optimizer_rule(Arc::new(InjectFilterSumRule));
    builder = builder.with_optimizer_rule(Arc::new(PushDownLeftSemiRule));
    builder = builder.with_physical_optimizer_rule(Arc::new(
        ematix_flow_core::swap_semi_join_build_rule::SwapSemiJoinBuildSideRule,
    ));
    builder = builder.with_physical_optimizer_rule(Arc::new(
        ematix_flow_core::force_collect_left_semi_build_rule::ForceCollectLeftForSemiBoundedBuildRule::default(),
    ));
    // ::default() reads the production env gates: EMAT_RT_BLOOM_INNER_JOIN,
    // EMAT_L9_REQUIRE_FILTERED_BUILD, EMAT_L9_MAX_EXPECTED_KEYS, EMAT_RT_BLOOM_RATIO.
    builder =
        builder.with_physical_optimizer_rule(Arc::new(EnableRuntimeBloomSidebandRule::default()));
    // HJ.3: swap rule runs last; no-op unless EMAT_HASH_JOIN=1.
    builder = builder.with_physical_optimizer_rule(Arc::new(
        ematix_flow_core::swap_emat_hash_join_rule::SwapEmatixHashJoinRule,
    ));
    let ctx = SessionContext::new_with_state(builder.build());
    let all_emat = std::env::var("EMAT_ALL_TABLES_EMAT")
        .ok()
        .map(|v| v == "1")
        .unwrap_or(false);
    for t in TPCH_TABLES {
        let path = data_dir.join(format!("{t}.parquet"));
        let use_emat = all_emat || *t == "lineitem" || *t == "orders";
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
    Ok(ctx)
}
