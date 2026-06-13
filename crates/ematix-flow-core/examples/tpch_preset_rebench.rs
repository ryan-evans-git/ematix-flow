//! FAITHFUL TPC-H re-bench (2026-06-09): measures ematix on the EXACT production
//! path — `preset::with_optimizer_rules` + `SessionConfig::new()` (default
//! partitions) + ALL tables via `EmatixFastParquetTableProvider` — i.e. exactly
//! what `ematix-flow-cli/src/run_shard.rs` (the runtime) and the library do.
//!
//! WHY: the `tpch_triangulation_bench` builds its ematix ctx by hand
//! (`build_ematix_ctx`) and applies the walkers manually — it does NOT use the
//! production preset / FlowQueryPlanner. That under-represents production (Q10
//! SF=100: bench 2925-3012 vs preset ~2330). The ematix.dev numbers came from
//! the bench, so they're a non-production config. This re-measures ematix the
//! way users actually run it, head-to-head vs DuckDB (same `duckdb` crate path
//! the published numbers used) in one run.
//!
//!   caffeinate -i env TPCH_DATA_DIR=examples/tpch/data/sf100 TRIALS=5 WARMUPS=2 \
//!     cargo run --release -p ematix-flow-core --example tpch_preset_rebench --features triangulation
//!
//! Env: TPCH_DATA_DIR (default sf100), TRIALS (5), WARMUPS (2),
//!      TPCH_QUERIES (csv, default 1..=22), SKIP_DUCKDB=1.

use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::preset;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TPCH_TABLES: &[&str] = &[
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];

/// EXACT production context (mirrors run_shard.rs lines 93-110).
fn build_ematix_ctx(data_dir: &Path) -> Result<SessionContext, Box<dyn std::error::Error>> {
    let mut builder = preset::with_optimizer_rules(
        SessionStateBuilder::new()
            .with_config(SessionConfig::new())
            .with_default_features(),
    );
    // EMAT_TEST_HJ_SWAP=1: install the (NOT-in-preset) EmatixHashJoin swap rule so
    // we can MEASURE whether HJ.4 actually helps Q08 when the swap fires (it never
    // fires in production today). Test-only; gated, off by default.
    if std::env::var_os("EMAT_TEST_HJ_SWAP").is_some() {
        builder = builder.with_physical_optimizer_rule(Arc::new(
            ematix_flow_core::swap_emat_hash_join_rule::SwapEmatixHashJoinRule,
        ));
    }
    let state = builder.build();
    let ctx = SessionContext::new_with_state(state);
    for t in TPCH_TABLES {
        let path = data_dir.join(format!("{t}.parquet"));
        let provider = EmatixFastParquetTableProvider::try_new(path.to_string_lossy().to_string())?;
        ctx.register_table(*t, Arc::new(provider))?;
    }
    Ok(ctx)
}

async fn time_ematix(ctx: &SessionContext, sql: &str) -> Result<(f64, usize), String> {
    let t = Instant::now();
    let df = ctx.sql(sql).await.map_err(|e| e.to_string())?;
    let batches = df.collect().await.map_err(|e| e.to_string())?;
    let ms = t.elapsed().as_secs_f64() * 1000.0;
    Ok((ms, batches.iter().map(|b| b.num_rows()).sum()))
}

/// DuckDB via the duckdb crate — same shape as the triangulation bench's
/// `run_duckdb` (fresh in-memory conn + lazy read_parquet views per call;
/// only the prepare+query+row-drain is timed).
fn time_duckdb(data_dir: &Path, sql: &str) -> Result<(f64, usize), String> {
    use duckdb::Connection;
    let conn = Connection::open_in_memory().map_err(|e| e.to_string())?;
    for t in TPCH_TABLES {
        let path = data_dir.join(format!("{t}.parquet"));
        let stmt = format!(
            "CREATE VIEW {t} AS SELECT * FROM read_parquet('{}')",
            path.display()
        );
        conn.execute_batch(&stmt).map_err(|e| e.to_string())?;
    }
    let t0 = Instant::now();
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
    let mut n = 0usize;
    while rows.next().map_err(|e| e.to_string())?.is_some() {
        n += 1;
    }
    Ok((t0.elapsed().as_secs_f64() * 1000.0, n))
}

/// DuckDB EXPLAIN ANALYZE — same lazy read_parquet view setup as `time_duckdb`,
/// returns the profiled operator tree. For ematix-vs-DuckDB operator diffs
/// ("profile the competitor — don't infer a floor"). Warms once so the ANALYZE
/// reflects warm-cache timing, matching the timed bench.
fn duckdb_explain(data_dir: &Path, sql: &str) -> Result<String, String> {
    use duckdb::Connection;
    let conn = Connection::open_in_memory().map_err(|e| e.to_string())?;
    for t in TPCH_TABLES {
        let path = data_dir.join(format!("{t}.parquet"));
        conn.execute_batch(&format!(
            "CREATE VIEW {t} AS SELECT * FROM read_parquet('{}')",
            path.display()
        ))
        .map_err(|e| e.to_string())?;
    }
    {
        let mut s = conn.prepare(sql).map_err(|e| e.to_string())?;
        let mut r = s.query([]).map_err(|e| e.to_string())?;
        while r.next().map_err(|e| e.to_string())?.is_some() {}
    }
    let mut stmt = conn
        .prepare(&format!("EXPLAIN ANALYZE {sql}"))
        .map_err(|e| e.to_string())?;
    let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
    let mut out = String::new();
    while let Some(r) = rows.next().map_err(|e| e.to_string())? {
        if let Ok(v) = r.get::<_, String>(1) {
            out.push_str(&v);
            out.push('\n');
        } else if let Ok(v) = r.get::<_, String>(0) {
            out.push_str(&v);
            out.push('\n');
        }
    }
    Ok(out)
}

/// WIN.SF100.SWEEP — current process RSS in MB (via `ps`, ~10 ms fork;
/// called OUTSIDE the timed regions only). Lets per-trial stderr
/// markers carry the allocator-retention curve across the sweep.
fn rss_mb() -> f64 {
    std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .and_then(|s| s.trim().parse::<f64>().ok())
        .map(|kb| kb / 1024.0)
        .unwrap_or(f64::NAN)
}

/// WIN.SF100.SWEEP — return mimalloc-retained heap to the OS after
/// each measurement (outside the timed region), DEFAULT ON (opt-out
/// `EMAT_MI_COLLECT=0`), mirroring the production worker's identical
/// hook in `run_shard.rs::execute_work_unit`. The instrumented
/// 2026-06-11 sweep showed this process retaining 10-17GB of freed
/// heap across the SF=100 pass, starving the page cache: every big
/// query re-read its full column set from disk EVERY trial (60-68GB
/// pageins/pass) while DuckDB's pass (same files, own allocator,
/// conn-per-call) paged in ~2-4GB. Measured −5% geomean at SF=100.
/// MI.GATE (2026-06-12): the unconditional default measured a +6.1%
/// geomean TAX at SF=10 (interleaved A/B/A/B; up to +25% on sub-60ms
/// queries) — with no page-cache pressure to relieve, every collect
/// tears down the warm heap and the next trial re-faults it.
/// MI.GATE.2/3: the peak-RSS-only gate self-latched (a 20×3 SF=10
/// sweep ratchets ru_maxrss past 6144MB inside the warmups, so every
/// measured trial collected anyway), and ru_majflt is a dead signal
/// for read()-based file IO. Gate v3 (shared `heap_pressure` helper,
/// same behavior as the production worker) gates on CURRENT RSS vs
/// max(6144MB, RAM/4): 0=never, 1=always, unset=auto. Returns whether
/// it collected, for the [trial] log.
fn mi_collect_if_requested() -> bool {
    let collect = ematix_flow_core::heap_pressure::should_mi_collect();
    if collect {
        unsafe { libmimalloc_sys::mi_collect(true) };
    }
    collect
}

fn median(v: &mut [f64]) -> f64 {
    if v.is_empty() {
        return f64::NAN;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = std::env::var("TPCH_DATA_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("examples/tpch/data/sf100"));
    let trials: usize = std::env::var("TRIALS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);
    let warmups: usize = std::env::var("WARMUPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2);
    let skip_duckdb = std::env::var("SKIP_DUCKDB").ok().as_deref() == Some("1");
    // SKIP_EMATIX=1: measure DuckDB ISOLATED (no ematix interleaving). Pair a
    // SKIP_DUCKDB run (ematix isolated) with a SKIP_EMATIX run (DuckDB isolated)
    // to compare each engine production-faithfully — neither runs alongside a
    // competitor saturating the cores (which production never does either).
    let skip_ematix = std::env::var("SKIP_EMATIX").ok().as_deref() == Some("1");
    let qids: Vec<u8> = match std::env::var("TPCH_QUERIES") {
        Ok(s) => s.split(',').filter_map(|x| x.trim().parse().ok()).collect(),
        Err(_) => (1u8..=22).collect(),
    };
    let scale = data_dir
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("?")
        .to_string();
    eprintln!(
        "FAITHFUL preset re-bench: scale={scale} trials={trials} warmups={warmups} duckdb={} queries={:?}",
        !skip_duckdb, qids
    );

    // LEVER-MATCH GUARD: ctx is built via preset::with_optimizer_rules (== run_shard.rs
    // production). A publish-grade run must use PRODUCTION DEFAULTS — no EMAT_* lever
    // override — or we'd bench a config we don't ship. Warn loudly if any is set.
    let emat_overrides: Vec<String> = std::env::vars()
        .filter(|(k, _)| k.starts_with("EMAT_"))
        .map(|(k, v)| format!("{k}={v}"))
        .collect();
    if emat_overrides.is_empty() {
        eprintln!("levers: PRODUCTION DEFAULTS (no EMAT_* override) — matches run_shard.rs");
    } else {
        eprintln!(
            "⚠ NON-DEFAULT LEVERS SET (bench may differ from production): {}",
            emat_overrides.join(" ")
        );
    }

    let ctx = build_ematix_ctx(&data_dir)?;

    // Load SQL (strip trailing ';' for both engines).
    let mut sqls: Vec<(u8, String)> = Vec::new();
    for q in &qids {
        let p = format!("examples/tpch/queries/q{q:02}.sql");
        match std::fs::read_to_string(&p) {
            Ok(s) => sqls.push((*q, s.trim().trim_end_matches(';').to_string())),
            Err(e) => eprintln!("skip Q{q:02}: {e}"),
        }
    }

    // EMAT_CLEAR_REGISTRY=1: reuse ONE ctx but clear the SharedSubtreeRegistry
    // before each timed query. If reused-ctx then matches fresh-ctx, the registry
    // accumulation IS the degradation cause and clear()-per-query is the fix.
    if std::env::var_os("EMAT_CLEAR_REGISTRY").is_some() {
        let mut config = SessionConfig::new();
        let _ = &mut config;
        let (builder, registry) = preset::with_optimizer_rules_and_registry(
            SessionStateBuilder::new()
                .with_config(SessionConfig::new())
                .with_default_features(),
        );
        let rctx = SessionContext::new_with_state(builder.build());
        for t in TPCH_TABLES {
            let path = data_dir.join(format!("{t}.parquet"));
            rctx.register_table(
                *t,
                Arc::new(EmatixFastParquetTableProvider::try_new(
                    path.to_string_lossy().to_string(),
                )?),
            )?;
        }
        for (q, sql) in &sqls {
            for _ in 0..warmups {
                registry.clear();
                let _ = time_ematix(&rctx, sql).await;
            }
            let mut t = Vec::new();
            for _ in 0..8 {
                registry.clear();
                if let Ok((ms, _)) = time_ematix(&rctx, sql).await {
                    t.push(ms);
                }
            }
            println!("Q{q:02} cleared_ms={:.1}", median(&mut t));
        }
        return Ok(());
    }

    // EMAT_FRESH_CTX=1: rebuild a FRESH ctx (empty SharedSubtree registry) per
    // query, like the bench does, to test long-lived-ctx registry accumulation.
    if std::env::var_os("EMAT_FRESH_CTX").is_some() {
        for (q, sql) in &sqls {
            let qctx = build_ematix_ctx(&data_dir)?;
            for _ in 0..warmups {
                let _ = time_ematix(&qctx, sql).await;
            }
            let mut t = Vec::new();
            for _ in 0..8 {
                // fresh ctx each timed run = bench parity (empty registry)
                let fctx = build_ematix_ctx(&data_dir)?;
                if let Ok((ms, _)) = time_ematix(&fctx, sql).await {
                    t.push(ms);
                }
            }
            println!("Q{q:02} fresh_ctx_ms={:.1}", median(&mut t));
        }
        return Ok(());
    }

    // EMAT_PLANTIME=1: time PLANNING only (ctx.state().create_physical_plan =
    // optimize -> FlowQueryPlanner -> physical planning), excluding execution.
    if std::env::var_os("EMAT_PLANTIME").is_some() {
        for (q, sql) in &sqls {
            for _ in 0..3 {
                if let Ok(df) = ctx.sql(sql).await {
                    let lp = df.logical_plan().clone();
                    let _ = ctx.state().create_physical_plan(&lp).await;
                }
            }
            let mut pt = Vec::new();
            let mut plt = Vec::new();
            for _ in 0..15 {
                let s1 = Instant::now();
                let df = ctx.sql(sql).await?;
                let lp = df.logical_plan().clone();
                pt.push(s1.elapsed().as_secs_f64() * 1000.0);
                let s2 = Instant::now();
                let _ = ctx.state().create_physical_plan(&lp).await?;
                plt.push(s2.elapsed().as_secs_f64() * 1000.0);
            }
            println!(
                "Q{q:02} parse_ms={:.2} plan_ms={:.2}",
                median(&mut pt),
                median(&mut plt)
            );
        }
        return Ok(());
    }

    // EMAT_EXPLAIN=1: dump the PRESET physical plan. EMAT_EXPLAIN=analyze: run
    // EXPLAIN ANALYZE (real per-operator output_rows) through the production path.
    if let Ok(mode) = std::env::var("EMAT_EXPLAIN") {
        // EMAT_EXPLAIN=duckdb: dump DuckDB's profiled operator tree (competitor profile).
        if mode == "duckdb" {
            for (q, sql) in &sqls {
                println!("===== Q{q:02} DUCKDB EXPLAIN ANALYZE =====");
                match duckdb_explain(&data_dir, sql) {
                    Ok(s) => println!("{s}"),
                    Err(e) => println!("duckdb explain err: {e}"),
                }
            }
            return Ok(());
        }
        use datafusion::arrow::array::{Array, StringArray};
        use datafusion::physical_plan::displayable;
        for (q, sql) in &sqls {
            println!(
                "===== Q{q:02} PRESET {} =====",
                if mode == "analyze" {
                    "EXPLAIN ANALYZE"
                } else {
                    "PHYSICAL PLAN"
                }
            );
            if mode == "analyze" {
                match ctx.sql(&format!("EXPLAIN ANALYZE {sql}")).await {
                    Ok(df) => match df.collect().await {
                        Ok(batches) => {
                            for b in &batches {
                                let col = b.column(b.num_columns() - 1);
                                if let Some(s) = col.as_any().downcast_ref::<StringArray>() {
                                    for i in 0..s.len() {
                                        println!("{}", s.value(i));
                                    }
                                }
                            }
                        }
                        Err(e) => println!("analyze collect err: {e}"),
                    },
                    Err(e) => println!("analyze sql err: {e}"),
                }
            } else {
                match ctx.sql(sql).await {
                    Ok(df) => {
                        let lp = df.logical_plan().clone();
                        match ctx.state().create_physical_plan(&lp).await {
                            Ok(p) => println!("{}", displayable(p.as_ref()).indent(true)),
                            Err(e) => println!("create_physical_plan err: {e}"),
                        }
                    }
                    Err(e) => println!("sql err: {e}"),
                }
            }
        }
        return Ok(());
    }

    // Warmup (untimed): warm OS page cache + ematix internals for every query.
    for wi in 0..warmups {
        for (q, sql) in &sqls {
            if !skip_ematix {
                let res = time_ematix(&ctx, sql).await;
                let collected = mi_collect_if_requested();
                match res {
                    Ok((ms, _)) => eprintln!(
                        "[trial] eng=ematix q={q:02} warm{wi} ms={ms:.1} rss_mb={:.0} cur_mb={:.0} collect={}",
                        rss_mb(),
                        ematix_flow_core::heap_pressure::current_rss_mb().unwrap_or(0.0),
                        collected as u8
                    ),
                    Err(e) => eprintln!("warm ematix Q{q:02} ERR: {e}"),
                }
            }
            if !skip_duckdb {
                match time_duckdb(&data_dir, sql) {
                    Ok((ms, _)) => eprintln!(
                        "[trial] eng=duckdb q={q:02} warm{wi} ms={ms:.1} rss_mb={:.0}",
                        rss_mb()
                    ),
                    Err(e) => eprintln!("warm duckdb Q{q:02} ERR: {e}"),
                }
            }
        }
    }

    // Timed, interleaved per query per trial.
    let mut e_times: std::collections::BTreeMap<u8, Vec<f64>> = Default::default();
    let mut d_times: std::collections::BTreeMap<u8, Vec<f64>> = Default::default();
    let mut e_rows: std::collections::BTreeMap<u8, usize> = Default::default();
    let mut d_rows: std::collections::BTreeMap<u8, usize> = Default::default();
    for ti in 0..trials {
        for (q, sql) in &sqls {
            // PRODUCTION FIDELITY: build a FRESH ctx per measurement, exactly like
            // run_shard.rs (one ctx per query). The ctx build is OUTSIDE the timed
            // region (time_ematix only times sql+collect), so it doesn't pollute the
            // number — it just makes every measurement a cold production query,
            // immune to the #340 cross-query ctx accumulator that inflates small
            // queries on a reused ctx. Same levers as production by construction
            // (build_ematix_ctx == preset::with_optimizer_rules == run_shard.rs).
            if !skip_ematix {
                let q_ctx = build_ematix_ctx(&data_dir)?;
                let res = time_ematix(&q_ctx, sql).await;
                let collected = mi_collect_if_requested();
                match res {
                    Ok((ms, r)) => {
                        eprintln!(
                            "[trial] eng=ematix q={q:02} ti={ti} ms={ms:.1} rss_mb={:.0} cur_mb={:.0} collect={}",
                            rss_mb(),
                            ematix_flow_core::heap_pressure::current_rss_mb().unwrap_or(0.0),
                            collected as u8
                        );
                        e_times.entry(*q).or_default().push(ms);
                        e_rows.insert(*q, r);
                    }
                    Err(e) => eprintln!("ematix Q{q:02} ERR: {e}"),
                }
            }
            if !skip_duckdb {
                match time_duckdb(&data_dir, sql) {
                    Ok((ms, r)) => {
                        eprintln!(
                            "[trial] eng=duckdb q={q:02} ti={ti} ms={ms:.1} rss_mb={:.0}",
                            rss_mb()
                        );
                        d_times.entry(*q).or_default().push(ms);
                        d_rows.insert(*q, r);
                    }
                    Err(e) => eprintln!("duckdb Q{q:02} ERR: {e}"),
                }
            }
        }
    }

    // Report.
    println!(
        "\n=== FAITHFUL preset re-bench: {scale} (ematix=production preset, {trials} trials median) ==="
    );
    println!(
        "{:<5} {:>11} {:>11} {:>9}  {:>10} {:>10}",
        "Q", "ematix_ms", "DuckDB_ms", "ratio", "rows_e", "rows_d"
    );
    let mut e_geo = 0.0f64;
    let mut d_geo = 0.0f64;
    let mut n_geo = 0usize;
    let mut wins = 0usize;
    let mut losses = 0usize;
    for (q, _) in &sqls {
        let em = e_times
            .get(q)
            .cloned()
            .map(|mut v| median(&mut v))
            .unwrap_or(f64::NAN);
        let dm = d_times
            .get(q)
            .cloned()
            .map(|mut v| median(&mut v))
            .unwrap_or(f64::NAN);
        let ratio = if dm.is_finite() && em.is_finite() && em > 0.0 {
            dm / em
        } else {
            f64::NAN
        };
        let er = e_rows.get(q).copied().unwrap_or(0);
        let dr = d_rows.get(q).copied().unwrap_or(0);
        let flag = if dr != 0 && er != dr { " ROWS!" } else { "" };
        println!("Q{q:02}  {em:>11.1} {dm:>11.1} {ratio:>8.3}x  {er:>10} {dr:>10}{flag}");
        println!(
            "RESULT scale={scale} q={q} ematix={em:.1} duckdb={dm:.1} rows_e={er} rows_d={dr}"
        );
        if em.is_finite() && dm.is_finite() {
            e_geo += em.ln();
            d_geo += dm.ln();
            n_geo += 1;
            if em < dm { wins += 1 } else { losses += 1 }
        }
    }
    if n_geo > 0 {
        let eg = (e_geo / n_geo as f64).exp();
        let dg = (d_geo / n_geo as f64).exp();
        println!(
            "\nGEOMEAN  ematix {eg:.1} ms  DuckDB {dg:.1} ms  -> ematix {:.3}x of DuckDB ({} wins / {} losses of {})",
            eg / dg,
            wins,
            losses,
            n_geo
        );
    }
    Ok(())
}
