//! **General SQL-surface parity harness** — the durable robustness asset for
//! the breadth campaign. For an arbitrary SQL string it runs the native
//! clean-room engine AND in-process DuckDB over the *same* parquet, then
//! compares full result sets as a sorted multiset (FP tolerance, cross-type
//! numeric/date equivalence) — exactly the `tpcds_native_oracle` contract,
//! generalized to any query.
//!
//! Every new SQL feature lands a case here: if the native engine binds and
//! executes it, the answer must match DuckDB row-for-row. A feature the
//! binder still rejects fails LOUDLY (that's the TDD signal), not silently.
//!
//! Over TPC-H SF-1 `lineitem` (int / float / decimal / string / date columns,
//! plus the low-card `l_returnflag` / `l_linestatus`).

use std::path::PathBuf;

use ematix_flow_engine::bind::bind_sql;
use ematix_flow_engine::catalog::Catalog;
use ematix_flow_engine::expr::ScalarValue;
use ematix_flow_engine::plan::execute;

fn sf1(table: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join(format!("../../examples/tpch/data/sf1/{table}.parquet"))
}

fn lineitem() -> PathBuf {
    sf1("lineitem")
}

fn have_data() -> bool {
    lineitem().exists()
}

// ---- comparable cell (mirrors the tpcds_native_oracle contract) ----

#[derive(Debug, Clone)]
enum Cell {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Date(i32),
    Str(String),
}

impl Cell {
    fn sort_key(&self) -> String {
        match self {
            Cell::Null => "\x00".into(),
            Cell::Bool(b) => format!("\x01{b}"),
            Cell::Int(i) => format!("\x02{i:020}"),
            Cell::Float(f) => format!("\x03{f:.6e}"),
            Cell::Date(d) => format!("\x02{d:020}"),
            Cell::Str(s) => format!("\x05{}", s.trim()),
        }
    }
}

fn fp_eq(a: f64, b: f64) -> bool {
    if a == b || (a.is_nan() && b.is_nan()) {
        return true;
    }
    let mag = a.abs().max(b.abs()).max(1.0);
    (a - b).abs() / mag <= 1e-6
}

fn cell_eq(a: &Cell, b: &Cell) -> bool {
    use Cell::*;
    match (a, b) {
        (Null, Null) => true,
        (Bool(x), Bool(y)) => x == y,
        (Int(x), Int(y)) => x == y,
        (Date(x), Date(y)) => x == y,
        (Str(x), Str(y)) => x.trim() == y.trim(),
        (Float(x), Float(y)) => fp_eq(*x, *y),
        (Int(x), Float(y)) | (Float(y), Int(x)) => fp_eq(*x as f64, *y),
        (Int(x), Date(y)) | (Date(y), Int(x)) => *x == *y as i64,
        (Float(x), Date(y)) | (Date(y), Float(x)) => fp_eq(*x, *y as f64),
        _ => false,
    }
}

fn row_lt(a: &[Cell], b: &[Cell]) -> std::cmp::Ordering {
    for (ca, cb) in a.iter().zip(b.iter()) {
        let o = ca.sort_key().cmp(&cb.sort_key());
        if o != std::cmp::Ordering::Equal {
            return o;
        }
    }
    a.len().cmp(&b.len())
}

fn fmt_row(r: &[Cell]) -> String {
    r.iter()
        .map(|c| match c {
            Cell::Null => "NULL".into(),
            Cell::Bool(b) => b.to_string(),
            Cell::Int(i) => i.to_string(),
            Cell::Float(f) => format!("{f:.4}"),
            Cell::Date(d) => format!("d{d}"),
            Cell::Str(s) => format!("\"{}\"", s.trim()),
        })
        .collect::<Vec<_>>()
        .join(" | ")
}

fn native_cell(v: &ScalarValue) -> Cell {
    match v {
        ScalarValue::Null => Cell::Null,
        ScalarValue::Boolean(b) => Cell::Bool(*b),
        ScalarValue::Int32(i) => Cell::Int(*i as i64),
        ScalarValue::Int64(i) => Cell::Int(*i),
        ScalarValue::Date32(d) => Cell::Date(*d),
        ScalarValue::Float64(f) => Cell::Float(*f),
        ScalarValue::Utf8(s) => Cell::Str(s.to_string()),
    }
}

fn duck_cell(row: &duckdb::Row, idx: usize) -> Cell {
    use duckdb::types::ValueRef;
    match row.get_ref(idx) {
        Err(_) => Cell::Str("ERR".into()),
        Ok(v) => match v {
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
            ValueRef::Text(b) => Cell::Str(String::from_utf8_lossy(b).into_owned()),
            ValueRef::Date32(d) => Cell::Date(d),
            // DuckDB's date_trunc on a DATE returns a TIMESTAMP (micros);
            // a whole-day timestamp is the same instant as the engine's
            // Date32 day-number, so normalize it to Date for comparison.
            ValueRef::Timestamp(tu, t) => Cell::Date((tu.to_micros(t) / 86_400_000_000) as i32),
            ValueRef::Decimal(d) => Cell::Float(d.to_string().parse().unwrap_or(0.0)),
            other => Cell::Str(format!("{other:?}")),
        },
    }
}

fn duck_rows(conn: &duckdb::Connection, sql: &str) -> Result<Vec<Vec<Cell>>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let mut it = stmt.query([]).map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    while let Some(row) = it.next().map_err(|e| e.to_string())? {
        let nc = row.as_ref().column_count();
        out.push((0..nc).map(|i| duck_cell(row, i)).collect());
    }
    Ok(out)
}

/// A harness bound to one registered table set: `check(sql)` asserts native
/// == DuckDB, panicking with a readable diff (or the native bind/exec error)
/// on any divergence.
struct Parity {
    catalog: Catalog,
    duck: duckdb::Connection,
}

impl Parity {
    fn new() -> Self {
        Self::with_tables(&["lineitem"])
    }

    /// Register the named SF1 tables into BOTH the native catalog and DuckDB
    /// (as views over the same parquet), so join parity is over identical data.
    fn with_tables(tables: &[&str]) -> Self {
        let mut catalog = Catalog::new();
        let duck = duckdb::Connection::open_in_memory().expect("duckdb");
        for &t in tables {
            catalog
                .register_parquet(t, sf1(t))
                .unwrap_or_else(|e| panic!("register {t}: {e}"));
            duck.execute_batch(&format!(
                "CREATE VIEW {t} AS SELECT * FROM read_parquet('{}');",
                sf1(t).display()
            ))
            .unwrap_or_else(|e| panic!("duck view {t}: {e}"));
        }
        Self { catalog, duck }
    }

    #[track_caller]
    fn check(&self, sql: &str) {
        let bq = match bind_sql(sql, &self.catalog) {
            Ok(q) => q,
            Err(e) => panic!("NATIVE BIND FAILED for `{sql}`:\n  {e}"),
        };
        let native: Vec<Vec<Cell>> = match execute(&bq) {
            Ok(r) => r
                .rows
                .iter()
                .map(|r| r.iter().map(native_cell).collect())
                .collect(),
            Err(e) => panic!("NATIVE EXEC FAILED for `{sql}`:\n  {e}"),
        };
        let duck = duck_rows(&self.duck, sql)
            .unwrap_or_else(|e| panic!("DUCK FAILED for `{sql}`:\n  {e}"));

        let mut n = native.clone();
        let mut d = duck.clone();
        n.sort_by(|a, b| row_lt(a, b));
        d.sort_by(|a, b| row_lt(a, b));
        if n.len() != d.len() {
            panic!(
                "ROW COUNT DIFFERS for `{sql}`: native {} vs duck {}",
                n.len(),
                d.len()
            );
        }
        for (i, (nr, dr)) in n.iter().zip(&d).enumerate() {
            let same = nr.len() == dr.len() && nr.iter().zip(dr).all(|(a, b)| cell_eq(a, b));
            if !same {
                panic!(
                    "MISMATCH for `{sql}` at sorted row {i}:\n  native: {}\n  duck:   {}",
                    fmt_row(nr),
                    fmt_row(dr)
                );
            }
        }
    }

    /// Assert the native binder STILL rejects `sql` (documents an
    /// intentional gap so a later accidental half-support is noticed).
    #[track_caller]
    fn check_rejected(&self, sql: &str) {
        if bind_sql(sql, &self.catalog).is_ok() {
            panic!("expected `{sql}` to be rejected by the binder, but it bound");
        }
    }
}

// ---------------------------------------------------------------------------
// Tier-1 breadth features. Each asserts native == DuckDB over the same data.
// ---------------------------------------------------------------------------

/// EXTRACT beyond YEAR: month/day/quarter/dow/week/doy over a date column.
#[test]
fn extract_date_fields() {
    if !have_data() {
        eprintln!("SKIP extract_date_fields: SF1 lineitem absent");
        return;
    }
    let p = Parity::new();
    for field in ["year", "month", "day", "quarter", "dow", "doy", "week"] {
        p.check(&format!(
            "select extract({field} from l_shipdate) f, count(*) n \
             from lineitem group by extract({field} from l_shipdate)"
        ));
    }
}

/// Scalar functions: lower, floor, ceil, mod, trim, length, replace.
#[test]
fn scalar_functions() {
    if !have_data() {
        eprintln!("SKIP scalar_functions: SF1 lineitem absent");
        return;
    }
    let p = Parity::new();
    p.check("select lower(l_returnflag) f, count(*) n from lineitem group by lower(l_returnflag)");
    p.check("select floor(l_quantity) f, count(*) n from lineitem group by floor(l_quantity)");
    p.check(
        "select ceil(l_discount * 100) f, count(*) n from lineitem group by ceil(l_discount * 100)",
    );
    p.check("select mod(l_orderkey, 7) m, count(*) n from lineitem group by mod(l_orderkey, 7)");
    p.check(
        "select length(l_returnflag) f, count(*) n from lineitem group by length(l_returnflag)",
    );
    p.check("select trim(l_returnflag) f, count(*) n from lineitem group by trim(l_returnflag)");
    p.check(
        "select replace(l_shipinstruct, ' ', '_') f, count(*) n \
         from lineitem group by replace(l_shipinstruct, ' ', '_')",
    );
    // Scalar string fns in a WHERE comparison (the inline owned-string path).
    p.check("select count(*) n from lineitem where lower(l_returnflag) = 'n'");
    p.check("select count(*) n from lineitem where trim(l_returnflag) <> 'A'");
    // Numeric fns in a predicate.
    p.check("select count(*) n from lineitem where mod(l_orderkey, 100) = 0");
    p.check("select count(*) n from lineitem where floor(l_quantity) > 30");
}

/// Unary minus on an expression (not just a literal).
#[test]
fn unary_minus_on_expr() {
    if !have_data() {
        eprintln!("SKIP unary_minus_on_expr: SF1 lineitem absent");
        return;
    }
    let p = Parity::new();
    p.check("select sum(-l_extendedprice) s from lineitem where l_shipdate >= date '1998-01-01'");
    p.check("select -l_linenumber v, count(*) n from lineitem group by -l_linenumber");
}

/// Positional GROUP BY / ORDER BY (`GROUP BY 1`, `ORDER BY 2`).
#[test]
fn positional_group_order() {
    if !have_data() {
        eprintln!("SKIP positional_group_order: SF1 lineitem absent");
        return;
    }
    let p = Parity::new();
    p.check("select l_returnflag, count(*) n from lineitem group by 1 order by 1");
    p.check(
        "select l_returnflag, l_linestatus, sum(l_quantity) q \
         from lineitem group by 1, 2 order by 2, 1",
    );
    p.check("select l_linestatus, count(*) n from lineitem group by 1 order by 2 desc");
}

// ---------------------------------------------------------------------------
// Tier-2a: aggregate variants + date_trunc.
// ---------------------------------------------------------------------------

/// Population/sample variance + population stddev (all from sum/sumsq/count).
#[test]
fn variance_aggregates() {
    if !have_data() {
        eprintln!("SKIP variance_aggregates: SF1 lineitem absent");
        return;
    }
    let p = Parity::new();
    p.check(
        "select l_returnflag, \
                var_samp(l_quantity) vs, var_pop(l_quantity) vp, \
                stddev_pop(l_extendedprice) sp, stddev_samp(l_discount) ss, \
                variance(l_tax) v \
         from lineitem group by 1",
    );
    // A single-row group: var_pop = 0, var_samp = NULL.
    p.check(
        "select l_orderkey, var_pop(l_quantity) vp, var_samp(l_quantity) vs \
         from lineitem where l_orderkey = 1 group by 1",
    );
}

/// date_trunc to year/quarter/month/week/day.
#[test]
fn date_trunc_units() {
    if !have_data() {
        eprintln!("SKIP date_trunc_units: SF1 lineitem absent");
        return;
    }
    let p = Parity::new();
    for unit in ["year", "quarter", "month", "week", "day"] {
        p.check(&format!(
            "select date_trunc('{unit}', l_shipdate) d, count(*) n \
             from lineitem group by date_trunc('{unit}', l_shipdate)"
        ));
    }
}

// ---------------------------------------------------------------------------
// Tier-2b: navigation/positional window functions. A TOTAL order in each
// OVER (l_orderkey, l_linenumber unique per lineitem) keeps lead/lag/ntile
// deterministic so parity is meaningful. (last_value deferred — default
// RANGE-frame peer semantics is a separate careful unit.)
// ---------------------------------------------------------------------------

#[test]
fn window_lead_lag_first_value() {
    if !have_data() {
        eprintln!("SKIP window_lead_lag_first_value: SF1 lineitem absent");
        return;
    }
    let p = Parity::new();
    let over = "over (partition by l_returnflag order by l_orderkey, l_linenumber)";
    p.check(&format!(
        "select l_orderkey, l_linenumber, \
                lag(l_quantity) {over} lg, \
                lag(l_quantity, 2) {over} lg2, \
                lead(l_extendedprice) {over} ld, \
                first_value(l_discount) {over} fv \
         from lineitem where l_orderkey < 800"
    ));
}

#[test]
fn window_ntile() {
    if !have_data() {
        eprintln!("SKIP window_ntile: SF1 lineitem absent");
        return;
    }
    let p = Parity::new();
    p.check(
        "select l_orderkey, l_linenumber, \
                ntile(4) over (order by l_orderkey, l_linenumber) q, \
                ntile(7) over (partition by l_returnflag order by l_orderkey, l_linenumber) q7 \
         from lineitem where l_orderkey < 400",
    );
}

// ---------------------------------------------------------------------------
// Tier-3 join residuals: RIGHT [OUTER] JOIN + non-equi joins. Over
// orders ⋈ lineitem (a 1-to-many key) with a filtered ON so preserved rows
// go unmatched (NULL-extended). RIGHT is implemented as a mirrored LEFT
// (the joined table is preserved, the keyed OLD table becomes nullable), so
// every LEFT capability — grouped NULL-extension, matched-only COUNT, and
// WHERE-side demote-to-INNER — must hold for RIGHT too.
// ---------------------------------------------------------------------------

fn have_joins() -> bool {
    sf1("orders").exists() && sf1("customer").exists()
}

#[test]
fn right_outer_join() {
    if !have_joins() {
        eprintln!("SKIP right_outer_join: SF1 orders/customer absent");
        return;
    }
    let p = Parity::with_tables(&["lineitem", "orders", "customer"]);
    // RIGHT preserves orders; a filtered ON leaves many orders unmatched.
    let on = "on l.l_orderkey = o.o_orderkey and l.l_quantity > 49";
    // Plain count over the preserved side.
    p.check(&format!(
        "select count(*) n from lineitem l right join orders o {on} where o.o_orderkey < 3000"
    ));
    // Grouped, with matched-only COUNT(l.col) vs total COUNT(*): exercises the
    // NULL-extension projection and the CountMatched path together.
    p.check(&format!(
        "select o.o_orderpriority pr, count(*) n, count(l.l_orderkey) m \
         from lineitem l right join orders o {on} where o.o_orderkey < 3000 group by 1"
    ));
    // A WHERE predicate on the (nullable) lineitem side that rejects NULLs
    // must demote RIGHT → INNER.
    p.check(
        "select count(*) n from lineitem l right join orders o \
         on l.l_orderkey = o.o_orderkey where o.o_orderkey < 3000 and l.l_quantity > 40",
    );
    // RIGHT join first, then an inner cross-linked third table.
    p.check(&format!(
        "select count(*) n from lineitem l right join orders o {on}, customer c \
         where c.c_custkey = o.o_custkey and o.o_orderkey < 3000"
    ));
    // RIGHT/RIGHT OUTER are the same operator.
    p.check(&format!(
        "select count(*) n from lineitem l right outer join orders o {on} where o.o_orderkey < 3000"
    ));
    // Boundary: a RIGHT join whose joined table is keyed to TWO different old
    // tables would need two preserved-root tables — intentionally rejected.
    p.check_rejected(
        "select count(*) n from customer c, orders o right join lineitem l \
         on l.l_orderkey = o.o_orderkey and l.l_suppkey = c.c_custkey",
    );
}

#[test]
fn non_equi_join() {
    if !have_joins() {
        eprintln!("SKIP non_equi_join: SF1 orders/customer absent");
        return;
    }
    let p = Parity::with_tables(&["lineitem", "orders"]);
    // A cross-table inequality alongside the equi key (post-join filter).
    p.check(
        "select count(*) n from orders o join lineitem l \
         on l.l_orderkey = o.o_orderkey and o.o_orderdate < l.l_shipdate \
         where o.o_orderkey < 3000",
    );
    // A pure non-equi ON (no equi key at all) — a filtered cross join whose
    // ON inequality prunes post-expansion.
    p.check(
        "select count(*) n from orders o join lineitem l \
         on o.o_totalprice < l.l_extendedprice where o.o_orderkey < 6 and l.l_orderkey < 6",
    );
    // Non-equi with an extra BETWEEN-style range and a grouped result.
    p.check(
        "select o.o_orderstatus s, count(*) n from orders o join lineitem l \
         on l.l_orderkey = o.o_orderkey and l.l_extendedprice > o.o_totalprice * 0.1 \
         where o.o_orderkey < 3000 group by 1",
    );
}

// ---------------------------------------------------------------------------
// Tier-3 fan-out payload materialization: an outer join whose preserved side
// fans out (orders → many lineitems on a filtered ON) must materialize —
// each match expands, each unmatched preserved row survives ONCE with
// NULL-filled nullable columns. Unblocks FULL OUTER on real 1-to-many keys
// and fan-out anti-joins (previously "LEFT duplicate-key payload" errors).
// ---------------------------------------------------------------------------

#[test]
fn outer_join_fanout_materialization() {
    if !have_joins() {
        eprintln!("SKIP outer_join_fanout_materialization: SF1 orders/customer absent");
        return;
    }
    let p = Parity::with_tables(&["lineitem", "orders", "customer"]);
    let on = "on l.l_orderkey = o.o_orderkey and l.l_quantity > 45";
    // Project the nullable side's columns across a fan-out — the core
    // materialization path (matched rows fan, unmatched go NULL).
    p.check(&format!(
        "select o.o_orderkey, l.l_partkey, l.l_quantity \
         from orders o left join lineitem l {on} where o.o_orderkey < 500"
    ));
    // Aggregate over the fan-out: COUNT(*) (all preserved rows) vs matched
    // COUNT(l.col) vs SUM over the NULL-extended column.
    p.check(&format!(
        "select o.o_orderpriority pr, count(*) n, count(l.l_orderkey) m, sum(l.l_quantity) q \
         from orders o left join lineitem l {on} where o.o_orderkey < 4000 group by 1"
    ));
    // Fan-out anti-join: keep only the unmatched preserved rows (LEFT and its
    // RIGHT mirror).
    p.check(&format!(
        "select count(*) n from orders o left join lineitem l {on} \
         where o.o_orderkey < 4000 and l.l_orderkey is null"
    ));
    p.check(&format!(
        "select count(*) n from lineitem l right join orders o {on} \
         where o.o_orderkey < 4000 and l.l_orderkey is null"
    ));
    // FULL OUTER on a 1-to-many key: count, and a column projection.
    p.check("select count(*) n from orders o full outer join lineitem l on l.l_orderkey = o.o_orderkey where o.o_orderkey < 2000");
    p.check("select o.o_orderkey, l.l_partkey from orders o full outer join lineitem l on l.l_orderkey = o.o_orderkey where o.o_orderkey < 400");
}

// ---------------------------------------------------------------------------
// Tier-3 GROUPING SETS / CUBE: the general grouping-set form (ROLLUP only
// does prefix cascades). Excluded columns render NULL (a subtotal); l_returnflag
// / l_linestatus have no genuine NULLs, so a NULL is unambiguously a subtotal.
// ---------------------------------------------------------------------------

#[test]
fn grouping_sets_and_cube() {
    if !have_data() {
        eprintln!("SKIP grouping_sets_and_cube: SF1 lineitem absent");
        return;
    }
    let p = Parity::new();
    // CUBE over two low-card dims: {rf,ls}, {rf}, {ls}, {} — 4 sets.
    p.check(
        "select l_returnflag, l_linestatus, count(*) n, sum(l_quantity) q \
         from lineitem group by cube(l_returnflag, l_linestatus)",
    );
    // GROUPING SETS with an explicit list incl the grand total ().
    p.check(
        "select l_returnflag, l_linestatus, count(*) n \
         from lineitem group by grouping sets ((l_returnflag, l_linestatus), (l_returnflag), ())",
    );
    // A single grouping column repeated across sets + a disjoint set.
    p.check(
        "select l_returnflag, l_linestatus, sum(l_extendedprice) e \
         from lineitem group by grouping sets ((l_returnflag), (l_linestatus), (l_returnflag))",
    );
    // Grand total only.
    p.check("select count(*) n, avg(l_discount) d from lineitem group by grouping sets (())");
    // CUBE over three dims (8 sets), with a WHERE to keep it small.
    p.check(
        "select l_returnflag, l_linestatus, l_shipmode, count(*) n \
         from lineitem where l_shipmode in ('AIR', 'RAIL') \
         group by cube(l_returnflag, l_linestatus, l_shipmode)",
    );
    // GROUPING(col) flags alongside CUBE (1 = the column is a subtotal here).
    p.check(
        "select l_returnflag, l_linestatus, \
                grouping(l_returnflag) gr, grouping(l_linestatus) gl, count(*) n \
         from lineitem group by cube(l_returnflag, l_linestatus)",
    );
    // A multi-column CUBE term: CUBE((rf,ls), sm) → 4 sets over 2 terms.
    p.check(
        "select l_returnflag, l_linestatus, l_shipmode, count(*) n \
         from lineitem where l_shipmode = 'AIR' \
         group by cube((l_returnflag, l_linestatus), l_shipmode)",
    );
}

// ---------------------------------------------------------------------------
// Tier-3: NOT + three-valued NULL logic. NULLs are injected with
// nullif(l_linenumber, 1) (NULL when linenumber == 1) since SF1 lineitem
// has no NULL columns. The point is that NOT over a NULL is NULL (drops at
// WHERE), NOT over AND/OR follows De Morgan under 3VL, and projected NOT
// values are NULL (not false) where the operand is unknown.
// ---------------------------------------------------------------------------

#[test]
fn not_and_three_valued_null_logic() {
    if !have_data() {
        eprintln!("SKIP not_and_three_valued_null_logic: SF1 lineitem absent");
        return;
    }
    let p = Parity::new();
    // NOT over comparisons / IS NULL / IN / LIKE (row counts must match).
    p.check("select count(*) n from lineitem where not (l_discount > 0.05)");
    p.check("select count(*) n from lineitem where not (nullif(l_linenumber, 1) > 3)");
    p.check("select count(*) n from lineitem where not (l_returnflag in ('A', 'N'))");
    p.check("select count(*) n from lineitem where not (l_shipinstruct like 'DELIVER%')");
    p.check("select count(*) n from lineitem where not (nullif(l_linenumber, 1) is null)");
    // NOT over AND / OR with a NULL-bearing operand (De Morgan under 3VL).
    p.check(
        "select count(*) n from lineitem \
         where not (nullif(l_linenumber, 1) > 3 and l_discount > 0.05)",
    );
    p.check(
        "select count(*) n from lineitem \
         where not (nullif(l_linenumber, 1) > 3 or l_discount > 0.09)",
    );
    // Double negation and nested.
    p.check("select count(*) n from lineitem where not (not (l_quantity > 25))");
    p.check(
        "select count(*) n from lineitem \
         where l_tax > 0.04 and not (l_returnflag = 'A' or nullif(l_linenumber,1) = 2)",
    );
    // Projected boolean: NOT over a NULL operand must render NULL, not false.
    p.check(
        "select l_orderkey, l_linenumber, \
                not (nullif(l_linenumber, 1) > 3) f, \
                not (l_linenumber > 2 and nullif(l_linenumber, 1) > 3) g \
         from lineitem where l_orderkey < 60",
    );
}

// ---------------------------------------------------------------------------
// Tier-2 aggregate tail: median / percentile_cont (buffered, continuous
// interpolation → Float64) and bool_and / bool_or (foldable → BOOLEAN, NULL
// over an empty/all-NULL group). All plain GROUP-BY aggregates — no window
// frame — so parity is deterministic. (last_value needs an UNBOUNDED FOLLOWING
// frame and string_agg needs an ordered string buffer; both are separate
// sub-units.)
// ---------------------------------------------------------------------------

#[test]
fn tier2_aggregate_tail() {
    if !have_data() {
        eprintln!("SKIP tier2_aggregate_tail: SF1 lineitem absent");
        return;
    }
    let p = Parity::new();
    // median — grouped and scalar (continuous interpolation over even/odd n).
    p.check("select l_returnflag, median(l_quantity) m from lineitem group by l_returnflag");
    p.check("select median(l_extendedprice) m from lineitem");
    // percentile_cont(p) WITHIN GROUP (ORDER BY x) — the classic quantiles.
    p.check(
        "select l_returnflag, \
                percentile_cont(0.25) within group (order by l_extendedprice) p25, \
                percentile_cont(0.5)  within group (order by l_extendedprice) p50, \
                percentile_cont(0.9)  within group (order by l_extendedprice) p90 \
         from lineitem group by l_returnflag",
    );
    p.check("select percentile_cont(0.5) within group (order by l_discount) med from lineitem");
    // bool_and / bool_or — BOOLEAN output, grouped and scalar.
    p.check(
        "select l_returnflag, \
                bool_and(l_quantity > 0)  allpos, \
                bool_and(l_quantity > 30) allbig, \
                bool_or(l_quantity > 45)  anyhuge, \
                bool_or(l_discount > 0.9) anydeep \
         from lineitem group by l_returnflag",
    );
    p.check("select bool_and(l_tax >= 0) b1, bool_or(l_tax > 0.07) b2 from lineitem");
    // Empty group: median → NULL, bool_and/bool_or → NULL (no non-NULL inputs).
    p.check(
        "select median(l_quantity) m, bool_and(l_quantity > 0) b, bool_or(l_quantity > 0) o \
         from lineitem where l_orderkey < 0",
    );
    // bool aggregate reused in HAVING (boolean predicate over the group).
    p.check(
        "select l_returnflag from lineitem group by l_returnflag having bool_or(l_quantity > 49)",
    );
}

// ---------------------------------------------------------------------------
// Window-frame breadth: UNBOUNDED FOLLOWING frames (whole-partition, distinct
// from the cumulative default) and last_value. The default RANGE ..CURRENT ROW
// frame keeps its cumulative meaning; UNBOUNDED..UNBOUNDED means whole
// partition for both the aggregate windows and last_value.
// ---------------------------------------------------------------------------

#[test]
fn window_last_value_and_frames() {
    if !have_data() {
        eprintln!("SKIP window_last_value_and_frames: SF1 lineitem absent");
        return;
    }
    let p = Parity::new();
    let part = "partition by l_returnflag order by l_orderkey, l_linenumber";
    let whole = "rows between unbounded preceding and unbounded following";
    // last_value over the whole partition (the deterministic, common form),
    // beside first_value over the default frame.
    p.check(&format!(
        "select l_orderkey, l_linenumber, \
                last_value(l_discount) over ({part} {whole}) lv, \
                first_value(l_discount) over ({part}) fv \
         from lineitem where l_orderkey < 800"
    ));
    // UNBOUNDED FOLLOWING makes an aggregate window WHOLE-partition (every row
    // gets the partition total), distinct from the cumulative default `run`.
    p.check(&format!(
        "select l_orderkey, l_linenumber, \
                sum(l_quantity) over ({part} {whole}) tot, \
                max(l_extendedprice) over ({part} {whole}) mx, \
                sum(l_quantity) over ({part}) run \
         from lineitem where l_orderkey < 500"
    ));
    // RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING = whole partition.
    p.check(&format!(
        "select l_orderkey, last_value(l_quantity) over \
                ({part} range between unbounded preceding and unbounded following) lv \
         from lineitem where l_orderkey < 300"
    ));
    // last_value with the DEFAULT frame (RANGE ..CURRENT ROW): a unique order
    // key makes peer groups singletons, so it equals the current row's value.
    p.check(&format!(
        "select l_orderkey, l_linenumber, last_value(l_discount) over ({part}) lv_def \
         from lineitem where l_orderkey < 200"
    ));
}

// ---------------------------------------------------------------------------
// string_agg / group_concat: ordered concatenation with a delimiter. Values
// are trim()ed so CHAR padding can't diverge inside a joined cell; ordering is
// pinned (value == key, or a unique key) so both engines emit the same string.
// ---------------------------------------------------------------------------

#[test]
fn string_agg_ordered() {
    if !have_data() {
        eprintln!("SKIP string_agg_ordered: SF1 lineitem absent");
        return;
    }
    let p = Parity::new();
    // Grouped, value == order key (ties are value-identical, so order is moot).
    p.check(
        "select l_returnflag, string_agg(trim(l_shipmode), ',' order by l_shipmode) s \
         from lineitem where l_orderkey < 200 group by l_returnflag",
    );
    // value != key with a UNIQUE order key (l_orderkey, l_linenumber).
    p.check(
        "select l_returnflag, \
                string_agg(trim(l_linestatus), '|' order by l_orderkey, l_linenumber) s \
         from lineitem where l_orderkey < 120 group by l_returnflag",
    );
    // DESC ordering + multi-char delimiter, scalar (single group).
    p.check(
        "select string_agg(trim(l_shipmode), '; ' order by l_shipmode desc) s \
         from lineitem where l_orderkey < 60",
    );
    // group_concat alias.
    p.check(
        "select group_concat(trim(l_shipmode), ',' order by l_shipmode) s \
         from lineitem where l_orderkey < 30",
    );
    // Empty group → NULL.
    p.check("select string_agg(trim(l_shipmode), ',' order by l_shipmode) s from lineitem where l_orderkey < 0");
}

// ---------------------------------------------------------------------------
// WITH RECURSIVE: seed (anchor) + iterate-to-fixpoint step. The anchor carries
// a FROM (the engine requires one), seeding from lineitem; the step reads the
// working set. Covers UNION ALL and UNION (dedup), multi-column accumulation,
// and outer aggregation over the recursive result.
// ---------------------------------------------------------------------------

#[test]
fn recursive_cte() {
    if !have_data() {
        eprintln!("SKIP recursive_cte: SF1 lineitem absent");
        return;
    }
    let p = Parity::new();
    // Integer series 1..12, UNION ALL.
    p.check(
        "with recursive seq(n) as ( \
             select l_linenumber from lineitem where l_orderkey = 1 and l_linenumber = 1 \
             union all \
             select n + 1 from seq where n < 12 \
         ) select n from seq order by n",
    );
    // Same series via UNION (dedup path) + outer aggregation.
    p.check(
        "with recursive seq(n) as ( \
             select l_linenumber from lineitem where l_orderkey = 1 and l_linenumber = 1 \
             union \
             select n + 1 from seq where n < 12 \
         ) select sum(n) s, count(*) c, max(n) m from seq",
    );
    // Two-column accumulation (factorials), UNION ALL.
    p.check(
        "with recursive t(n, acc) as ( \
             select 1, 1 from lineitem where l_orderkey = 1 and l_linenumber = 1 \
             union all \
             select n + 1, acc * (n + 1) from t where n < 8 \
         ) select n, acc from t order by n",
    );
    // Outer query joins/filters the recursive result.
    p.check(
        "with recursive seq(n) as ( \
             select l_linenumber from lineitem where l_orderkey = 1 and l_linenumber = 1 \
             union all \
             select n + 1 from seq where n < 20 \
         ) select n from seq where mod(n, 2) = 0 order by n",
    );
    // A non-recursive CTE alongside RECURSIVE (only the self-referencing one
    // iterates).
    p.check(
        "with recursive base as (select count(*) c from lineitem where l_orderkey < 50), \
              seq(n) as ( \
                  select l_linenumber from lineitem where l_orderkey = 1 and l_linenumber = 1 \
                  union all \
                  select n + 1 from seq where n < 6 \
              ) \
         select (select c from base) bc, sum(n) sn from seq",
    );
}

// ---------------------------------------------------------------------------
// Silent-correctness fixes (surfaced by the breadth sweep): features the
// binder accepted but whose semantics it dropped, silently returning wrong
// answers — aggregate FILTER, QUALIFY, and lag/lead's DEFAULT argument.
// ---------------------------------------------------------------------------

#[test]
fn aggregate_filter_qualify_lag_default() {
    if !have_data() {
        eprintln!("SKIP aggregate_filter_qualify_lag_default: SF1 lineitem absent");
        return;
    }
    let p = Parity::new();
    // FILTER (WHERE …) on aggregates — scalar and grouped.
    p.check(
        "select count(*) filter (where l_discount > 0.05) a, \
                sum(l_quantity) filter (where l_returnflag = 'N') b, \
                avg(l_extendedprice) filter (where l_quantity > 30) c, \
                count(distinct l_linenumber) filter (where l_discount > 0.04) d \
         from lineitem where l_orderkey < 2000",
    );
    p.check(
        "select l_returnflag, \
                count(*) filter (where l_discount > 0.05) a, \
                sum(l_quantity) filter (where l_linestatus = 'F') b \
         from lineitem where l_orderkey < 2000 group by l_returnflag",
    );
    // QUALIFY over a window (row_number keeps the first line per order).
    p.check(
        "select l_orderkey, l_linenumber from lineitem where l_orderkey < 40 \
         qualify row_number() over (partition by l_orderkey order by l_linenumber) = 1",
    );
    p.check(
        "select l_orderkey, l_linenumber from lineitem where l_orderkey < 40 \
         qualify rank() over (partition by l_orderkey order by l_quantity desc) <= 2",
    );
    // lag / lead with an explicit DEFAULT past the partition edge.
    p.check(
        "select l_orderkey, l_linenumber, \
                lag(l_quantity, 1, -1.0) over (partition by l_returnflag order by l_orderkey, l_linenumber) lg, \
                lead(l_quantity, 2, -9.0) over (partition by l_returnflag order by l_orderkey, l_linenumber) ld \
         from lineitem where l_orderkey < 80",
    );
}

// ---------------------------------------------------------------------------
// High-value operators & scalars bundle (from the breadth sweep): LIMIT OFFSET,
// the || and % operators, boolean literals, CROSS JOIN, IS [NOT] DISTINCT FROM,
// ILIKE, scalar math functions, greatest/least, and current_date.
// ---------------------------------------------------------------------------

#[test]
fn operators_and_scalars_bundle() {
    if !have_data() {
        eprintln!("SKIP operators_and_scalars_bundle: SF1 lineitem absent");
        return;
    }
    let p = Parity::new();
    // LIMIT ... OFFSET.
    p.check("select l_orderkey, l_linenumber from lineitem order by l_orderkey, l_linenumber limit 5 offset 3");
    p.check("select l_orderkey from lineitem order by l_orderkey desc, l_linenumber offset 10");
    // || string concat, % modulo.
    p.check(
        "select trim(l_shipmode) || '/' || trim(l_returnflag) s from lineitem where l_orderkey < 4",
    );
    p.check("select count(*) c from lineitem where l_orderkey % 3 = 0 and l_orderkey < 500");
    // Boolean literals (parenthesized IS-comparison to dodge sqlparser's
    // `IS DISTINCT FROM x AND y` precedence quirk).
    p.check(
        "select count(*) c from lineitem where (l_discount > 0.05) = true and l_orderkey < 200",
    );
    p.check("select count(*) c from lineitem where true and l_orderkey < 50");
    // CROSS JOIN (cartesian product with a 1-row derived).
    p.check("select count(*) c from lineitem l cross join (select max(l_quantity) mq from lineitem) t where l.l_orderkey < 10 and l.l_quantity < t.mq");
    // IS [NOT] DISTINCT FROM (NULL-safe compare), NULLs injected via nullif.
    p.check("select count(*) c from lineitem where (nullif(l_linenumber, 1) is distinct from 2) and l_orderkey < 200");
    p.check("select count(*) c from lineitem where (nullif(l_linenumber, 1) is not distinct from 3) and l_orderkey < 200");
    // ILIKE.
    p.check("select count(*) c from lineitem where l_shipmode ilike '%ai%'");
    // Scalar math functions.
    p.check(
        "select sqrt(l_quantity) a, ln(l_quantity) b, exp(l_discount) c, power(l_quantity, 2) d, \
                sign(l_discount - 0.05) e, trunc(l_extendedprice) f, abs(l_discount - 0.05) g \
         from lineitem where l_orderkey < 4",
    );
    // greatest / least.
    p.check("select greatest(l_quantity, l_extendedprice, 100) a, least(l_discount, l_tax, 0.02) b from lineitem where l_orderkey < 4");
    // current_date (all shipdates precede today, so the count is data-stable).
    p.check("select count(*) c from lineitem where l_shipdate < current_date");
}

// ---------------------------------------------------------------------------
// count(DISTINCT <string>) — the distinct-key path was i64-only and panicked
// on strings (surfaced by the breadth sweep). Now exact via a string set.
// ---------------------------------------------------------------------------

#[test]
fn count_distinct_string() {
    if !have_data() {
        eprintln!("SKIP count_distinct_string: SF1 lineitem absent");
        return;
    }
    let p = Parity::new();
    p.check("select count(distinct l_shipmode) c from lineitem where l_orderkey < 3000");
    p.check(
        "select l_returnflag, count(distinct l_shipmode) c \
         from lineitem where l_orderkey < 3000 group by l_returnflag",
    );
    // A string EXPRESSION, and mixed string + numeric distinct counts.
    p.check("select count(distinct trim(l_comment)) c from lineitem where l_orderkey < 1000");
    p.check(
        "select count(distinct l_shipmode) a, count(distinct l_linenumber) b \
         from lineitem where l_orderkey < 1000",
    );
    // Empty group → 0 (COUNT is never NULL).
    p.check("select count(distinct l_shipmode) c from lineitem where l_orderkey < 0");
}

// ---------------------------------------------------------------------------
// CAST(x AS VARCHAR/CHAR/TEXT) — the target arm returned the operand
// UNCHANGED, so a numeric/date operand stayed numeric instead of being
// rendered to text (silent wrong answer vs DuckDB; surfaced by breadth
// sweep #2). Now stringifies: integers/floats as decimal text, dates as
// ISO `YYYY-MM-DD`, strings identity.
// ---------------------------------------------------------------------------

#[test]
fn cast_as_varchar() {
    if !have_data() {
        eprintln!("SKIP cast_as_varchar: SF1 lineitem absent");
        return;
    }
    let p = Parity::new();
    // Integer column → text.
    p.check("select cast(l_linenumber as varchar) k from lineitem where l_orderkey < 200");
    // char(n) and text spellings.
    p.check("select cast(l_orderkey as char(20)) k from lineitem where l_orderkey < 50");
    p.check("select cast(l_linenumber as text) k from lineitem where l_orderkey < 50");
    // A string operand is identity.
    p.check("select cast(l_shipmode as varchar) k from lineitem where l_orderkey < 50");
    // A DATE renders ISO (native previously exposed the raw day-number).
    p.check("select distinct cast(l_shipdate as varchar) k from lineitem where l_orderkey < 100");
    // An integer arithmetic expression under the cast.
    p.check(
        "select cast(l_orderkey + l_linenumber as varchar) k \
         from lineitem where l_orderkey < 50",
    );
    // Cast result usable as a GROUP BY key and in string concatenation.
    p.check(
        "select cast(l_linenumber as varchar) k, count(*) c \
         from lineitem where l_orderkey < 2000 group by cast(l_linenumber as varchar)",
    );
    p.check(
        "select cast(l_linenumber as varchar) || 'x' k \
         from lineitem where l_orderkey < 50",
    );
}

// ---------------------------------------------------------------------------
// date_part / datediff / extract(epoch) — the function spellings of the date
// machinery. `date_part('u', d)` is EXTRACT(u FROM d); `datediff('u', a, b)`
// lowers to Extract + arithmetic (b - a in the given unit); epoch = days *
// 86400. Previously all three were honest bind-rejections (breadth sweep).
// ---------------------------------------------------------------------------

#[test]
fn date_part_and_datediff() {
    if !have_data() {
        eprintln!("SKIP date_part_and_datediff: SF1 lineitem absent");
        return;
    }
    let p = Parity::new();
    // date_part is EXTRACT by another name — every supported field.
    p.check(
        "select date_part('year', l_shipdate) y, date_part('month', l_shipdate) m, \
         date_part('quarter', l_shipdate) q, date_part('day', l_shipdate) d \
         from lineitem where l_orderkey < 100",
    );
    p.check(
        "select date_part('dow', l_shipdate) a, date_part('isodow', l_shipdate) b, \
         date_part('doy', l_shipdate) c, date_part('week', l_shipdate) w \
         from lineitem where l_orderkey < 100",
    );
    // datediff over day/year/month/quarter (receipt >= ship >= commit-ish).
    p.check(
        "select datediff('day', l_shipdate, l_receiptdate) d \
         from lineitem where l_orderkey < 200",
    );
    p.check(
        "select datediff('year', l_commitdate, l_receiptdate) y, \
         datediff('month', l_commitdate, l_receiptdate) m, \
         datediff('quarter', l_commitdate, l_receiptdate) q \
         from lineitem where l_orderkey < 200",
    );
    // date_diff spelling + use in an aggregate / predicate.
    p.check(
        "select avg(date_diff('day', l_shipdate, l_receiptdate)) avg_days \
         from lineitem where l_orderkey < 5000",
    );
    p.check(
        "select count(*) c from lineitem \
         where datediff('day', l_shipdate, l_receiptdate) > 15 and l_orderkey < 5000",
    );
    // extract(epoch) and date_part('epoch', …).
    p.check("select distinct extract(epoch from l_shipdate) e from lineitem where l_orderkey < 50");
    p.check("select distinct date_part('epoch', l_shipdate) e from lineitem where l_orderkey < 50");
}

// ---------------------------------------------------------------------------
// Bounded ROWS window frames (`n PRECEDING`/`n FOLLOWING`) — a sliding
// window, the one genuine frame-capability gap (previously the frame binder
// accepted only UNBOUNDED PRECEDING starts). Aggregate windows only.
// ---------------------------------------------------------------------------

#[test]
fn window_bounded_rows_frames() {
    if !have_data() {
        eprintln!("SKIP window_bounded_rows_frames: SF1 lineitem absent");
        return;
    }
    let p = Parity::new();
    let base = "from lineitem where l_orderkey < 400";
    // Trailing moving sum/avg (n PRECEDING .. CURRENT ROW).
    p.check(&format!(
        "select l_orderkey, l_linenumber, \
         sum(l_quantity) over (partition by l_orderkey order by l_linenumber \
           rows between 2 preceding and current row) s \
         {base}"
    ));
    // Centered window (n PRECEDING .. m FOLLOWING).
    p.check(&format!(
        "select l_orderkey, l_linenumber, \
         avg(l_extendedprice) over (partition by l_orderkey order by l_linenumber \
           rows between 1 preceding and 1 following) a, \
         min(l_quantity) over (partition by l_orderkey order by l_linenumber \
           rows between 1 preceding and 1 following) mn, \
         max(l_quantity) over (partition by l_orderkey order by l_linenumber \
           rows between 1 preceding and 1 following) mx \
         {base}"
    ));
    // Leading window (CURRENT ROW .. m FOLLOWING) and count over a frame.
    p.check(&format!(
        "select l_orderkey, l_linenumber, \
         count(l_quantity) over (partition by l_orderkey order by l_linenumber \
           rows between current row and 2 following) c \
         {base}"
    ));
    // One unbounded side + a finite offset; and the shorthand `rows n preceding`.
    p.check(&format!(
        "select l_orderkey, l_linenumber, \
         sum(l_quantity) over (partition by l_orderkey order by l_linenumber \
           rows between unbounded preceding and 1 following) s1, \
         sum(l_quantity) over (partition by l_orderkey order by l_linenumber \
           rows 1 preceding) s2 \
         {base}"
    ));
    // No PARTITION BY — the frame slides over the whole ordered set.
    p.check(&format!(
        "select l_orderkey, l_linenumber, \
         sum(l_quantity) over (order by l_orderkey, l_linenumber \
           rows between 3 preceding and current row) s \
         {base}"
    ));
    // Non-aggregate window under a bounded frame must be rejected (no
    // silent wrong answer).
    p.check_rejected(
        "select first_value(l_quantity) over (partition by l_orderkey \
         order by l_linenumber rows between 1 preceding and 1 following) f \
         from lineitem where l_orderkey < 10",
    );
}

// ---------------------------------------------------------------------------
// `<op> ANY/ALL (subquery)` — rewrites to existing machinery: `= ANY` ≡ IN,
// `<> ALL` ≡ NOT IN, and the ordered comparisons reduce to a min/max scalar
// subquery. Previously all were honest bind-rejections.
// ---------------------------------------------------------------------------

#[test]
fn quantified_subquery_any_all() {
    if !have_data() {
        eprintln!("SKIP quantified_subquery_any_all: SF1 lineitem absent");
        return;
    }
    let p = Parity::with_tables(&["lineitem", "orders"]);
    // = ANY / = SOME ≡ IN.
    p.check(
        "select count(*) c from orders where o_orderkey = any \
         (select l_orderkey from lineitem where l_quantity > 48)",
    );
    p.check(
        "select count(*) c from orders where o_orderkey = some \
         (select l_orderkey from lineitem where l_quantity > 48)",
    );
    // <> ALL ≡ NOT IN.
    p.check(
        "select count(*) c from orders where o_orderkey <> all \
         (select l_orderkey from lineitem where l_quantity > 48)",
    );
    // Ordered comparisons → min/max scalar subquery. The subquery is the
    // line quantities of order 1 (a small, non-empty, non-NULL set).
    let sub = "(select l_quantity from lineitem where l_orderkey = 1)";
    p.check(&format!(
        "select count(*) c from lineitem where l_quantity > all {sub} and l_orderkey < 5000"
    ));
    p.check(&format!(
        "select count(*) c from lineitem where l_quantity < all {sub} and l_orderkey < 5000"
    ));
    p.check(&format!(
        "select count(*) c from lineitem where l_quantity > any {sub} and l_orderkey < 5000"
    ));
    p.check(&format!(
        "select count(*) c from lineitem where l_quantity <= any {sub} and l_orderkey < 5000"
    ));
    p.check(&format!(
        "select count(*) c from lineitem where l_quantity >= all {sub} and l_orderkey < 5000"
    ));
    // Rare quantifier/operator combos reject loudly (no silent wrong answer).
    p.check_rejected(&format!("select count(*) from lineitem where l_quantity = all {sub}"));
    p.check_rejected(&format!("select count(*) from lineitem where l_quantity <> any {sub}"));
}

// ---------------------------------------------------------------------------
// Sweep-#3 silent-bug fixes: (1) `string_agg(DISTINCT …)` dropped the
// DISTINCT on the floor; (2) `CAST(<float> AS INT)` rounded .5 ties away
// from zero where DuckDB rounds half-to-even.
// ---------------------------------------------------------------------------

#[test]
fn string_agg_distinct_and_cast_ties() {
    if !have_data() {
        eprintln!("SKIP string_agg_distinct_and_cast_ties: SF1 lineitem absent");
        return;
    }
    let p = Parity::new();
    // DISTINCT + ORDER BY the aggregated value: sorted distinct values.
    p.check(
        "select string_agg(distinct l_returnflag, ',' order by l_returnflag) s \
         from lineitem where l_orderkey < 500",
    );
    p.check(
        "select l_returnflag, string_agg(distinct l_shipmode, '|' order by l_shipmode) s \
         from lineitem where l_orderkey < 2000 group by l_returnflag",
    );
    // DISTINCT without ORDER BY over a single-valued group (deterministic).
    p.check(
        "select string_agg(distinct l_linestatus, ',') s \
         from lineitem where l_returnflag = 'R' and l_orderkey < 2000",
    );
    // DISTINCT with an ORDER BY that is NOT the aggregated value is
    // ambiguous (which instance's sort position?) — reject loudly.
    p.check_rejected(
        "select string_agg(distinct l_shipmode, ',' order by l_orderkey) s \
         from lineitem where l_orderkey < 100",
    );
    // CAST float→int ties round half-to-even, matching DuckDB
    // (2.5→2, 3.5→4, −2.5→−2). Built from a real column so the native
    // binder sees a column expression, not a foldable literal.
    let one = "(select l_extendedprice * 0 + 1 as one from lineitem \
                where l_orderkey = 1 and l_linenumber = 1) t";
    p.check(&format!("select cast(one * 2.5 as int) v from {one}"));
    p.check(&format!("select cast(one * 3.5 as int) v from {one}"));
    p.check(&format!("select cast(0 - one * 2.5 as int) v from {one}"));
    p.check(&format!("select cast(one * 73426.5 as int) v from {one}"));
    // A DECIMAL literal takes the decimal rule instead: half AWAY from zero
    // (duck cast(2.5 as int) = 3, probe-verified) — the fold matches.
    p.check(&format!("select cast(2.5 as int) + one v from {one}"));
    // Non-tie values are unaffected.
    p.check("select cast(l_extendedprice as int) v from lineitem where l_orderkey < 200");
}


// ---------------------------------------------------------------------------
// String/predicate long-tail bundle: NOT BETWEEN, the string-function family
// (left/right/lpad/rpad/repeat/reverse/initcap, ltrim/rtrim with a char set,
// trim(BOTH/LEADING/TRAILING … FROM …)), and strpos/instr/POSITION.
// Previously all honest bind-rejections (breadth sweep #3).
// ---------------------------------------------------------------------------

#[test]
fn string_longtail_bundle() {
    if !have_data() {
        eprintln!("SKIP string_longtail_bundle: SF1 lineitem absent");
        return;
    }
    let p = Parity::new();
    // NOT BETWEEN (and its 3VL behavior through a nullable expression).
    p.check("select count(*) c from lineitem where l_quantity not between 10 and 20 and l_orderkey < 5000");
    p.check("select count(*) c from lineitem where nullif(l_quantity, 30) not between 10 and 20 and l_orderkey < 5000");
    // left/right, including DuckDB's negative-count form.
    p.check("select left(l_shipinstruct, 4) a, right(l_shipinstruct, 4) b from lineitem where l_orderkey < 50");
    p.check("select left(l_shipmode, -1) a, right(l_shipmode, -1) b from lineitem where l_orderkey < 50");
    p.check("select left(l_shipmode, 0) a, left(l_shipmode, 99) b from lineitem where l_orderkey < 50");
    // lpad/rpad: pad, cycle the fill, and truncate when already longer.
    p.check("select lpad(l_returnflag, 5, '*-') a, rpad(l_returnflag, 5, '*-') b from lineitem where l_orderkey < 50");
    p.check("select lpad(l_shipinstruct, 6, 'x') a, rpad(l_shipinstruct, 6, 'x') b from lineitem where l_orderkey < 50");
    // (2-arg lpad/rpad is the Postgres space-fill form; DuckDB has no 2-arg
    // overload to oracle against, so the explicit-space form gates it.)
    p.check("select lpad(l_returnflag, 3, ' ') a, rpad(l_returnflag, 3, ' ') b from lineitem where l_orderkey < 50");
    // repeat / reverse / initcap.
    p.check("select repeat(l_returnflag, 3) a, repeat(l_returnflag, 0) b from lineitem where l_orderkey < 50");
    p.check("select reverse(l_shipmode) s from lineitem where l_orderkey < 50");
    // (initcap ships with Postgres semantics; DuckDB has no initcap to
    // oracle against.)
    // ltrim/rtrim with a char set (and the default whitespace form).
    p.check("select ltrim(l_shipmode, 'MAIL') a, rtrim(l_shipmode, 'LIAM') b from lineitem where l_orderkey < 50");
    p.check("select ltrim(l_shipinstruct) a, rtrim(l_shipinstruct) b from lineitem where l_orderkey < 50");
    // trim(BOTH/LEADING/TRAILING <chars> FROM <expr>).
    p.check("select trim(both 'N' from l_shipmode) s from lineitem where l_orderkey < 50");
    p.check("select trim(leading 'RA' from l_shipmode) s from lineitem where l_orderkey < 50");
    p.check("select trim(trailing 'BLAIR' from l_shipmode) s from lineitem where l_orderkey < 50");
    // strpos / instr / POSITION — 1-based, 0 when absent; usable in WHERE.
    p.check("select strpos(l_shipinstruct, 'IN') a, instr(l_shipinstruct, 'zzz') b from lineitem where l_orderkey < 50");
    p.check("select position('AI' in l_shipmode) v from lineitem where l_orderkey < 50");
    p.check("select count(*) c from lineitem where strpos(l_shipmode, 'AI') = 2 and l_orderkey < 2000");
    // Composition: string fns as group keys and inside aggregates.
    p.check(
        "select left(l_shipmode, 2) k, count(*) c from lineitem \
         where l_orderkey < 2000 group by left(l_shipmode, 2)",
    );
}
