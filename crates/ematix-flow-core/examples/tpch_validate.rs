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
use ematix_flow_core::dedupe_aggregate_rule::DedupeAggregateForFloatDeterminism;
use ematix_flow_core::dict_aggregate_rule::EnableDictGroupCountRule;
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;
use ematix_flow_core::fused_aggregate_filter_multi_agg_rule::InjectFilterMultiAggRule;
use ematix_flow_core::fused_aggregate_filter_sum_rule::InjectFilterSumRule;
use ematix_flow_core::push_down_left_semi_rule::PushDownLeftSemiRule;
use ematix_flow_core::robin_hood_sum_f64_exec::EnableRobinHoodSumF64Rule;
use ematix_flow_core::runtime_bloom_sideband_rule::EnableRuntimeBloomSidebandRule;
use ematix_flow_core::swap_semi_join_build_rule::SwapSemiJoinBuildSideRule;

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
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .ok_or("workspace root not found")?
        .to_path_buf();
    let data_dir = std::env::var("TPCH_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| workspace.join("examples/tpch/data/sf1"));
    let queries_dir = workspace.join("examples/tpch/queries");

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
            Ok(s) => s,
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
    if s.len() > 160 { format!("{}…", &s[..160]) } else { s }
}

fn compare(
    emat: &[Vec<Cell>],
    duck: &[Vec<Cell>],
    fp_rtol: f64,
    max_diffs: usize,
) -> Outcome {
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
    a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| cells_equal(x, y, fp_rtol))
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

async fn run_ematix(data_dir: &Path, sql: &str) -> Result<Vec<Vec<Cell>>, Box<dyn std::error::Error>> {
    use arrow_array::ArrayRef;

    let mut builder = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(14))
        .with_default_features()
        .with_physical_optimizer_rule(Arc::new(DedupeAggregateForFloatDeterminism::default()))
        .with_physical_optimizer_rule(Arc::new(EnableDictGroupCountRule))
        .with_physical_optimizer_rule(Arc::new(InjectFilterMultiAggRule))
        .with_physical_optimizer_rule(Arc::new(InjectFilterSumRule));
    // Σ.Q.M: synthetic LeftSemi producer. Must precede L10 in
    // registration order.
    if std::env::var_os("EMAT_SYNTHETIC_LEFT_SEMI").is_some() {
        builder = builder.with_optimizer_rule(Arc::new(
            ematix_flow_core::synthetic_left_semi_rule::SyntheticLeftSemiRule,
        ));
    }
    builder = builder.with_optimizer_rule(Arc::new(PushDownLeftSemiRule));
    builder = builder.with_physical_optimizer_rule(Arc::new(SwapSemiJoinBuildSideRule));
    builder = builder.with_physical_optimizer_rule(Arc::new(EnableRobinHoodSumF64Rule));
    // Σ.Q.L15: Inner-L9 + tight ratio + all-Emat enabled by env.
    let l15 = std::env::var_os("L15").is_some();
    let l9_rule = if l15 {
        EnableRuntimeBloomSidebandRule {
            min_probe_to_build_ratio: 1024,
            allow_inner_join: true,
        }
    } else {
        EnableRuntimeBloomSidebandRule::default()
    };
    builder = builder.with_physical_optimizer_rule(Arc::new(l9_rule));
    let state = builder.build();
    let ctx = SessionContext::new_with_state(state);
    for t in TPCH_TABLES {
        let path = data_dir.join(format!("{t}.parquet"));
        let use_emat = l15 || *t == "lineitem" || *t == "orders";
        if use_emat {
            let prov = EmatixFastParquetTableProvider::try_new(path.to_string_lossy())?;
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
    }
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
        DataType::Float32 => {
            Cell::Float(arr.as_primitive::<Float32Type>().value(row) as f64)
        }
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
