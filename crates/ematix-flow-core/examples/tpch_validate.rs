//! Σ.Q.L14 follow-up — TPC-H value validation: ematix-flow vs DuckDB.
//!
//! The triangulation bench only verifies row counts. Σ.Q.L14 caught a
//! silent correctness bug (Q07 sums 94% off with Inner-L9 ON) because
//! same-row-count results passed unnoticed. This script catches that
//! class of bug by comparing actual cell values with FP-relative
//! tolerance for floats and exact match for everything else.
//!
//! Usage:
//!   cargo run --release -p ematix-flow-core --example tpch_validate \
//!     --features triangulation
//!
//! Env:
//!   TPCH_DATA_DIR (default examples/tpch/data/sf1)
//!   TPCH_QUERIES  (comma list, default 1..=22; e.g. "5,7,8,21")
//!   FP_RTOL       (default 1e-6 — relative tolerance for f64 compare)
//!   MAX_DIFF_ROWS (default 5)

use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;

const TPCH_TABLES: &[&str] = &[
    "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
];

#[derive(Debug, Clone, PartialEq)]
enum Cell {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    /// Date32 (days since epoch).
    Date(i32),
    Str(String),
}

impl Cell {
    /// Sort key — converts each cell to a String. NOT used for equality
    /// (we use `cells_equal` below with FP tolerance).
    fn sort_key(&self) -> String {
        match self {
            Cell::Null => "\x00NULL".to_string(),
            Cell::Bool(b) => format!("\x01{b}"),
            Cell::Int(i) => format!("\x02{i:020}"),
            // Sort floats with enough precision to be deterministic.
            Cell::Float(f) => format!("\x03{f:.15e}"),
            Cell::Date(d) => format!("\x04{d:010}"),
            Cell::Str(s) => format!("\x05{s}"),
        }
    }
}

fn cells_equal(a: &Cell, b: &Cell, fp_rtol: f64) -> bool {
    match (a, b) {
        (Cell::Null, Cell::Null) => true,
        (Cell::Bool(x), Cell::Bool(y)) => x == y,
        (Cell::Int(x), Cell::Int(y)) => x == y,
        (Cell::Date(x), Cell::Date(y)) => x == y,
        (Cell::Str(x), Cell::Str(y)) => x.trim() == y.trim(),
        (Cell::Float(x), Cell::Float(y)) => fp_approx_eq(*x, *y, fp_rtol),
        // Cross-type numeric: DuckDB may return Int where ematix
        // returns Float (e.g. ratios that happen to be integral).
        (Cell::Int(x), Cell::Float(y)) | (Cell::Float(y), Cell::Int(x)) => {
            fp_approx_eq(*x as f64, *y, fp_rtol)
        }
        _ => false,
    }
}

fn fp_approx_eq(a: f64, b: f64, rtol: f64) -> bool {
    if a == b {
        return true;
    }
    if a.is_nan() && b.is_nan() {
        return true;
    }
    let mag = a.abs().max(b.abs()).max(1.0);
    (a - b).abs() / mag <= rtol
}

fn row_lt(a: &[Cell], b: &[Cell]) -> std::cmp::Ordering {
    for (ca, cb) in a.iter().zip(b.iter()) {
        let cmp = ca.sort_key().cmp(&cb.sort_key());
        if cmp != std::cmp::Ordering::Equal {
            return cmp;
        }
    }
    a.len().cmp(&b.len())
}

fn fmt_cell(c: &Cell) -> String {
    match c {
        Cell::Null => "NULL".to_string(),
        Cell::Bool(b) => format!("{b}"),
        Cell::Int(i) => format!("{i}"),
        Cell::Float(f) => format!("{f}"),
        Cell::Date(d) => format!("date:{d}"),
        Cell::Str(s) => format!("\"{}\"", s.trim()),
    }
}

fn fmt_row(r: &[Cell]) -> String {
    r.iter().map(fmt_cell).collect::<Vec<_>>().join(" | ")
}

#[derive(Debug)]
enum Outcome {
    Pass {
        rows: usize,
    },
    Mismatch {
        emat_rows: usize,
        duck_rows: usize,
        first_diffs: Vec<(String, String)>,
    },
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Legacy lever-name compatibility (2026-07-02 hardening): the Q20
    // transitive semi used to be applied manually, gated on
    // `EMAT_Q20_TRANSITIVE_SEMI`; it now runs inside the production
    // `FlowQueryPlanner`, whose gate is spelled `EMAT_Q20_SEMI`.
    // SAFETY: first statement of main — nothing reads EMAT_* env yet.
    if let (Ok(v), Err(_)) = (
        std::env::var("EMAT_Q20_TRANSITIVE_SEMI"),
        std::env::var("EMAT_Q20_SEMI"),
    ) {
        unsafe { std::env::set_var("EMAT_Q20_SEMI", v) };
    }
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .ok_or("workspace root not found")?
        .to_path_buf();
    let data_dir = std::env::var("TPCH_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| workspace.join("examples/tpch/data/sf1"));
    let queries_dir = workspace.join("examples/tpch/queries");
    // #315: scale Q11's HAVING fraction (0.0001 / SF) so it isn't degenerate at
    // SF>=10. Derived from the data-dir name; both DuckDB and ematix below run
    // the SAME scaled SQL, keeping the comparison apples-to-apples.
    let scale_factor = ematix_flow_core::tpch_params::scale_factor_from_data_dir(&data_dir);

    let query_subset: Vec<u8> = std::env::var("TPCH_QUERIES")
        .ok()
        .map(|s| {
            s.split(',')
                .filter_map(|t| t.trim().parse().ok())
                .collect::<Vec<u8>>()
        })
        .unwrap_or_else(|| (1..=22u8).collect());

    let fp_rtol: f64 = std::env::var("FP_RTOL")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1e-6);
    let max_diff_rows: usize = std::env::var("MAX_DIFF_ROWS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);

    println!("=== TPC-H value validation: ematix-flow vs DuckDB ===");
    println!("data: {}", data_dir.display());
    println!("queries: {query_subset:?}");
    println!("FP relative tolerance: {fp_rtol:e}");
    println!();

    let mut pass_count = 0usize;
    let mut fail_count = 0usize;
    let mut skip_count = 0usize;

    for q in &query_subset {
        let sql_path = queries_dir.join(format!("q{q:02}.sql"));
        let sql = match std::fs::read_to_string(&sql_path) {
            Ok(s) => ematix_flow_core::tpch_params::apply_tpch_query_params(*q, &s, scale_factor),
            Err(_) => {
                println!("Q{q:02}: SKIP (no SQL file at {})", sql_path.display());
                skip_count += 1;
                continue;
            }
        };

        let duck = match run_duckdb(&data_dir, &sql) {
            Ok(r) => r,
            Err(e) => {
                println!("Q{q:02}: SKIP (DuckDB error: {})", short(&e.to_string()));
                skip_count += 1;
                continue;
            }
        };
        let emat = match run_ematix(&data_dir, &sql).await {
            Ok(r) => r,
            Err(e) => {
                println!("Q{q:02}: FAIL (ematix error: {})", short(&e.to_string()));
                fail_count += 1;
                continue;
            }
        };

        let outcome = compare(&emat, &duck, fp_rtol, max_diff_rows);
        match outcome {
            Outcome::Pass { rows } => {
                println!("Q{q:02}: PASS ({rows} rows match)");
                pass_count += 1;
            }
            Outcome::Mismatch {
                emat_rows,
                duck_rows,
                first_diffs,
            } => {
                println!("Q{q:02}: MISMATCH (emat={emat_rows} duck={duck_rows})");
                for (i, (e, d)) in first_diffs.iter().enumerate() {
                    println!("    diff {i}:");
                    println!("      emat: {e}");
                    println!("      duck: {d}");
                }
                fail_count += 1;
            }
        }
    }

    println!();
    println!("--- summary ---");
    println!("PASS:    {pass_count}");
    println!("FAIL:    {fail_count}");
    println!("SKIP:    {skip_count}");
    if fail_count > 0 {
        std::process::exit(1);
    }
    Ok(())
}

fn short(s: &str) -> String {
    let s = s.replace('\n', " ");
    if s.len() > 160 {
        format!("{}…", &s[..160])
    } else {
        s
    }
}

fn compare(emat: &[Vec<Cell>], duck: &[Vec<Cell>], fp_rtol: f64, max_diffs: usize) -> Outcome {
    if emat.len() != duck.len() {
        let diffs: Vec<(String, String)> = emat
            .iter()
            .zip(duck.iter())
            .filter(|(a, b)| !rows_equal(a, b, fp_rtol))
            .take(max_diffs)
            .map(|(a, b)| (fmt_row(a), fmt_row(b)))
            .collect();
        return Outcome::Mismatch {
            emat_rows: emat.len(),
            duck_rows: duck.len(),
            first_diffs: diffs,
        };
    }
    let diffs: Vec<(String, String)> = emat
        .iter()
        .zip(duck.iter())
        .filter(|(a, b)| !rows_equal(a, b, fp_rtol))
        .take(max_diffs)
        .map(|(a, b)| (fmt_row(a), fmt_row(b)))
        .collect();
    if diffs.is_empty() {
        Outcome::Pass { rows: emat.len() }
    } else {
        Outcome::Mismatch {
            emat_rows: emat.len(),
            duck_rows: duck.len(),
            first_diffs: diffs,
        }
    }
}

fn rows_equal(a: &[Cell], b: &[Cell], fp_rtol: f64) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b.iter())
            .all(|(x, y)| cells_equal(x, y, fp_rtol))
}

// --------- DuckDB ----------

fn run_duckdb(data_dir: &Path, sql: &str) -> Result<Vec<Vec<Cell>>, Box<dyn std::error::Error>> {
    let conn = duckdb::Connection::open_in_memory()?;
    conn.execute_batch("PRAGMA threads=14")?;
    for t in TPCH_TABLES {
        let path = data_dir.join(format!("{t}.parquet"));
        let stmt = format!(
            "CREATE VIEW {t} AS SELECT * FROM read_parquet('{}')",
            path.display()
        );
        conn.execute_batch(&stmt)?;
    }
    let mut stmt = conn.prepare(sql)?;
    let mut rows_iter = stmt.query([])?;
    let mut out: Vec<Vec<Cell>> = Vec::new();
    let mut n_cols: Option<usize> = None;
    while let Some(row) = rows_iter.next()? {
        let nc = *n_cols.get_or_insert_with(|| row.as_ref().column_count());
        let mut cells: Vec<Cell> = Vec::with_capacity(nc);
        for i in 0..nc {
            cells.push(duckdb_cell(row, i));
        }
        out.push(cells);
    }
    out.sort_by(|a, b| row_lt(a, b));
    Ok(out)
}

fn duckdb_cell(row: &duckdb::Row, idx: usize) -> Cell {
    use duckdb::types::ValueRef;
    let v = match row.get_ref(idx) {
        Ok(v) => v,
        Err(_) => return Cell::Str("ERR".to_string()),
    };
    match v {
        ValueRef::Null => Cell::Null,
        ValueRef::Boolean(b) => Cell::Bool(b),
        ValueRef::TinyInt(i) => Cell::Int(i as i64),
        ValueRef::SmallInt(i) => Cell::Int(i as i64),
        ValueRef::Int(i) => Cell::Int(i as i64),
        ValueRef::BigInt(i) => Cell::Int(i),
        ValueRef::HugeInt(i) => Cell::Int(i as i64),
        ValueRef::UTinyInt(i) => Cell::Int(i as i64),
        ValueRef::USmallInt(i) => Cell::Int(i as i64),
        ValueRef::UInt(i) => Cell::Int(i as i64),
        ValueRef::UBigInt(i) => Cell::Int(i as i64),
        ValueRef::Float(f) => Cell::Float(f as f64),
        ValueRef::Double(f) => Cell::Float(f),
        ValueRef::Text(b) => Cell::Str(String::from_utf8_lossy(b).to_string()),
        ValueRef::Date32(d) => Cell::Date(d),
        ValueRef::Decimal(d) => {
            // duckdb::types::Decimal — convert to f64 (precision loss
            // is acceptable at our 1e-6 rtol).
            Cell::Float(d.to_string().parse::<f64>().unwrap_or(0.0))
        }
        _ => Cell::Str(format!("{v:?}")),
    }
}

// --------- ematix-flow ----------

/// Build the value-gate session. 2026-07-02 hardening (docs/PERF_Q18.md
/// § Hardening): the chain is assembled by the PRODUCTION preset via
/// `preset::with_optimizer_rules_overridden` — this harness only maps
/// its documented levers onto explicit, named `HarnessOverrides`, so
/// value validation exercises the exact plans preset users run
/// (including the FlowQueryPlanner walkers: agg_semi, dim_push,
/// q20_semi, the Q05 transitive dim-semi, and the shape-gated reorder —
/// the first three of which used to be re-applied by hand here, and the
/// last of which was MISSING, so validate never checked production's
/// reordered plans). Parity is pinned by `validate_preset_parity_tests`.
fn build_ematix_session() -> SessionContext {
    // Concurrency-aware partitions (campaign-2026-07-01 §3): explicit
    // `PARTITIONS=N` keeps precedence; otherwise the library's
    // `EMAT_TARGET_PARTITIONS` tri-state resolves (`=N` force, `=0`
    // legacy full cores, unset = AUTO cross-process sensing — solo
    // stays at full cores). Resolved here, so the preset's auto hook
    // is disabled in the overrides below.
    let partitions: usize = std::env::var("PARTITIONS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n >= 1)
        .unwrap_or_else(ematix_flow_core::partition_registry::resolve_target_partitions);
    // Σ.Q.L15: Inner-L9 + tight ratio + all-Emat. **Default ON at
    // milestone config**; set `L15=0` to revert to pre-L15 (stock L9
    // defaults + FastParquet for non-fact tables, below).
    let l15 = l15_enabled();
    let overrides = ematix_flow_core::preset::HarnessOverrides {
        // Partitions already resolved above (PARTITIONS > tri-state);
        // the preset must not re-resolve at a different instant.
        auto_target_partitions: false,
        // Σ.Q.M: synthetic LeftSemi producer (precedes L10 by
        // registration order inside the unified constructor).
        synthetic_left_semi: std::env::var_os("EMAT_SYNTHETIC_LEFT_SEMI").is_some(),
        // REV.3/17.3 — DEFAULT-ON to value-validate the production
        // preset config; opt-out via EMAT_FORCE_COLLECT_LEFT=0.
        force_collect_left: std::env::var("EMAT_FORCE_COLLECT_LEFT")
            .ok()
            .map(|v| !(v == "0" || v.eq_ignore_ascii_case("false")))
            .unwrap_or(true),
        // Σ.S.B legacy stem-fanout cascade — EMAT_L9_CASCADE_STEM=1
        // swaps the base L9 rule out (Σ.Q05.CHAIN repurposed
        // EMAT_L9_CASCADE as the tri-state gate INSIDE the base rule).
        l9_cascade_stem_max_extras: ematix_flow_core::flags::opt_in("EMAT_L9_CASCADE_STEM").then(
            || {
                std::env::var("EMAT_L9_CASCADE_MAX")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(4)
            },
        ),
        // L15=0 legacy A/B: revert L9 to its pre-L15 env-resolved
        // defaults (ratio 64, Inner joins off).
        l9_stock_defaults: !l15,
        // HJ.4: SIMD-tag/RobinHood join-probe swap — registered last;
        // dormant unless EMAT_HASH_JOIN=1. No-op for default runs.
        swap_emat_hash_join: true,
        ..ematix_flow_core::preset::HarnessOverrides::default()
    };
    let (builder, _handles) = ematix_flow_core::preset::with_optimizer_rules_overridden(
        SessionStateBuilder::new()
            .with_config(SessionConfig::new().with_target_partitions(partitions))
            .with_default_features(),
        &overrides,
    );
    SessionContext::new_with_state(builder.build())
}

/// Σ.Q.L15 lever shared by the session builder and the provider loop.
fn l15_enabled() -> bool {
    std::env::var("L15")
        .ok()
        .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
        .unwrap_or(true)
}

async fn run_ematix(
    data_dir: &Path,
    sql: &str,
) -> Result<Vec<Vec<Cell>>, Box<dyn std::error::Error>> {
    use arrow_array::ArrayRef;

    let l15 = l15_enabled();
    let ctx = build_ematix_session();
    // Σ.AH.5: the FD group-by simplifier needs the PK-derived FDs to fire, so
    // validating it declares the TPC-H primary keys — harness scaffolding like
    // tpch_preset_rebench's EMAT_TPCH_PK (a real catalog declares PKs via DDL;
    // the shipped library has no TPC-H hardcoding).
    // Σ.AI.5: the lever is now scale-gated tri-state and this read happens
    // BEFORE registration (scale unknown), so declare PKs unless the lever is
    // FORCED off — PK declaration is plan-inert when the rule doesn't fire
    // (pinned by flow_query_planner's FD-on-catalog plan-diff test), and the
    // rule's own gate (resolved post-registration) makes the final call.
    let fd_groupby = ematix_flow_core::flags::tri_state("EMAT_FD_GROUPBY") != Some(false);
    let tpch_pk = |t: &str| -> Option<Vec<usize>> {
        Some(match t {
            "region" | "nation" | "supplier" | "customer" | "part" | "orders" => vec![0],
            "partsupp" => vec![0, 1], // (ps_partkey, ps_suppkey)
            "lineitem" => vec![0, 3], // (l_orderkey, l_linenumber)
            _ => return None,
        })
    };
    for t in TPCH_TABLES {
        let path = data_dir.join(format!("{t}.parquet"));
        let use_emat = l15 || *t == "lineitem" || *t == "orders";
        if use_emat {
            let mut prov = EmatixFastParquetTableProvider::try_new(path.to_string_lossy())?;
            if fd_groupby {
                if let Some(pk) = tpch_pk(t) {
                    prov = prov.with_primary_key(pk);
                }
            }
            ctx.register_table(*t, Arc::new(prov))?;
        } else {
            let prov = FastParquetTableProvider::try_new(path.to_string_lossy())?;
            ctx.register_table(*t, Arc::new(prov))?;
        }
    }
    if std::env::var_os("EMAT_DUMP_PLAN").is_some() {
        let df = ctx.sql(sql).await?;
        let plan = df.clone().into_optimized_plan()?;
        eprintln!("=== OPTIMIZED LOGICAL PLAN ===\n{plan}\n==============================");
        let mut semi_count = 0usize;
        use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
        use datafusion::logical_expr::{JoinType, LogicalPlan as LP};
        let _ = plan.apply(|node| {
            if let LP::Join(j) = node {
                if matches!(j.join_type, JoinType::LeftSemi | JoinType::LeftAnti) {
                    semi_count += 1;
                }
            }
            Ok(TreeNodeRecursion::Continue)
        });
        eprintln!("LeftSemi/Anti node count in optimized plan: {semi_count}");

        // Also dump the physical plan to inspect build/probe sides.
        let phys = ctx.state().create_physical_plan(&plan).await?;
        let formatted = datafusion::physical_plan::displayable(phys.as_ref())
            .indent(true)
            .to_string();
        eprintln!("=== PHYSICAL PLAN ===\n{formatted}=====================");
    }
    // 2026-07-02 hardening: the production pre-plan walkers — agg_semi,
    // dim_push, q20_semi (Σ.Q20), the Q05 transitive dim-semi (Σ.Q05 /
    // #352), fd_groupby (Σ.AH.5), the shape-gated reorder — all run
    // inside the session's `FlowQueryPlanner`, so this value gate now
    // validates the EXACT production plan pipeline (it previously
    // re-applied only q20_semi/q05_semi/fd_groupby by hand and never
    // exercised agg_semi/dim_push/reorder plans at all). Their opt-out
    // levers are the planner's env gates (EMAT_AGG_SEMI, EMAT_DIM_PUSH,
    // EMAT_Q20_SEMI — legacy spelling EMAT_Q20_TRANSITIVE_SEMI is
    // forwarded in main() — EMAT_TRANSITIVE_DIM_SEMI, EMAT_REORDER_QP,
    // EMAT_FD_GROUPBY).
    let batches = ctx.sql(sql).await?.collect().await?;
    let mut out: Vec<Vec<Cell>> = Vec::new();
    for batch in &batches {
        let n_rows = batch.num_rows();
        let cols: Vec<ArrayRef> = (0..batch.num_columns())
            .map(|i| batch.column(i).clone())
            .collect();
        for row in 0..n_rows {
            let mut cells: Vec<Cell> = Vec::with_capacity(cols.len());
            for arr in &cols {
                cells.push(arrow_cell(arr, row));
            }
            out.push(cells);
        }
    }
    out.sort_by(|a, b| row_lt(a, b));
    Ok(out)
}

/// Validate↔preset parity pins (2026-07-02 hardening — see the bench's
/// `bench_preset_parity_tests` for the drift history). Run:
/// `cargo test -p ematix-flow-core --example tpch_validate`.
#[cfg(test)]
mod validate_preset_parity_tests {
    use super::*;
    use ematix_flow_core::preset;

    /// Harness-only rules the value gate may carry on top of the
    /// production chain. `SwapEmatixHashJoinRule` is installed
    /// unconditionally (HJ.4 measurement hook) but is dormant unless
    /// `EMAT_HASH_JOIN=1`; the cascade-stem rule only appears under
    /// `EMAT_L9_CASCADE_STEM=1`.
    const HARNESS_ONLY_PHYSICAL_RULES: &[&str] = &[
        "SwapEmatixHashJoinRule",             // always installed, dormant
        "ematix_flow_enable_cascading_bloom", // EMAT_L9_CASCADE_STEM=1
    ];
    const HARNESS_ONLY_LOGICAL_RULES: &[&str] = &[
        "ematix_flow_synthetic_left_semi", // EMAT_SYNTHETIC_LEFT_SEMI=1
    ];

    /// The value gate's ematix rule chain must EQUAL the production
    /// preset's (registration order included), modulo the documented
    /// allowlist — so a rule added to the preset can never again be
    /// silently absent from value validation (the Q18 SF=100
    /// RANGE.AGG gap), and vice versa (Σ.V).
    #[tokio::test(flavor = "multi_thread")]
    async fn validate_rule_chain_name_set_equals_production_preset() {
        let ctx = build_ematix_session();
        let state = ctx.state();
        let preset_state = preset::with_optimizer_rules(
            SessionStateBuilder::new()
                .with_config(SessionConfig::new().with_target_partitions(4))
                .with_default_features(),
        )
        .build();

        let (mut phys, logi) = preset::ematix_rule_names(&state);
        phys.retain(|n| !HARNESS_ONLY_PHYSICAL_RULES.contains(&n.as_str()));
        let logi: Vec<String> = logi
            .into_iter()
            .filter(|n| !HARNESS_ONLY_LOGICAL_RULES.contains(&n.as_str()))
            .collect();
        let (preset_phys, preset_logi) = preset::ematix_rule_names(&preset_state);
        assert_eq!(
            phys, preset_phys,
            "tpch_validate PHYSICAL rule chain diverged from the production \
             preset (allowlist-filtered) — the value gate would be \
             validating plans production never runs"
        );
        assert_eq!(
            logi, preset_logi,
            "tpch_validate LOGICAL rule chain diverged from the production preset"
        );
        assert_eq!(preset_phys, preset::PRODUCTION_PHYSICAL_RULE_NAMES);
        assert_eq!(preset_logi, preset::PRODUCTION_LOGICAL_RULE_NAMES);
        // The production FlowQueryPlanner (agg_semi / dim_push /
        // q20_semi / Q05 transitive dim-semi / shape-gated reorder /
        // fd_groupby / scalar-agg boost) must be exercised by the
        // value gate.
        assert_eq!(
            format!("{:?}", state.query_planner()),
            format!("{:?}", preset_state.query_planner()),
            "value-gate session's query planner differs from the production preset's"
        );
    }
}

fn arrow_cell(arr: &arrow_array::ArrayRef, row: usize) -> Cell {
    use arrow_array::cast::AsArray;
    use arrow_array::types::*;
    use arrow_schema::DataType;

    if arr.is_null(row) {
        return Cell::Null;
    }
    match arr.data_type() {
        DataType::Boolean => Cell::Bool(arr.as_boolean().value(row)),
        DataType::Int8 => Cell::Int(arr.as_primitive::<Int8Type>().value(row) as i64),
        DataType::Int16 => Cell::Int(arr.as_primitive::<Int16Type>().value(row) as i64),
        DataType::Int32 => Cell::Int(arr.as_primitive::<Int32Type>().value(row) as i64),
        DataType::Int64 => Cell::Int(arr.as_primitive::<Int64Type>().value(row)),
        DataType::UInt8 => Cell::Int(arr.as_primitive::<UInt8Type>().value(row) as i64),
        DataType::UInt16 => Cell::Int(arr.as_primitive::<UInt16Type>().value(row) as i64),
        DataType::UInt32 => Cell::Int(arr.as_primitive::<UInt32Type>().value(row) as i64),
        DataType::UInt64 => Cell::Int(arr.as_primitive::<UInt64Type>().value(row) as i64),
        DataType::Float32 => Cell::Float(arr.as_primitive::<Float32Type>().value(row) as f64),
        DataType::Float64 => Cell::Float(arr.as_primitive::<Float64Type>().value(row)),
        DataType::Date32 => Cell::Date(arr.as_primitive::<Date32Type>().value(row)),
        DataType::Date64 => Cell::Int(arr.as_primitive::<Date64Type>().value(row)),
        DataType::Utf8 => Cell::Str(arr.as_string::<i32>().value(row).to_string()),
        DataType::LargeUtf8 => Cell::Str(arr.as_string::<i64>().value(row).to_string()),
        DataType::Utf8View => Cell::Str(arr.as_string_view().value(row).to_string()),
        DataType::Decimal128(_, scale) => {
            let v = arr.as_primitive::<Decimal128Type>().value(row);
            Cell::Float(v as f64 / 10f64.powi(*scale as i32))
        }
        other => Cell::Str(format!("?{other:?}?")),
    }
}
