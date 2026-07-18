//! v2 S1.1 — grouping-set semantic contract (RED-first for Phase GS).
//!
//! Locks the *observable behaviour* of `GROUP BY ROLLUP / CUBE /
//! GROUPING SETS` and the `GROUPING()` / `GROUPING_ID()` functions on the
//! **shared v2 session** (`preset::session_context`, dogfooding S0.1),
//! before S1.2 swaps DataFusion's generic hash aggregate for the native
//! `FusedGroupingSetAggregateExec`. See `docs/PHASE_V2_S1_GROUPING_SETS.md`.
//!
//! These run on stock DataFusion today (correctness is already there —
//! the S1 gap is *native/fused execution*, not results), so they are
//! GREEN now and pin the exact result set the fused operator must
//! reproduce byte-for-byte. The single most important contract here is
//! the one most likely to catch a wrong implementation
//! (`PHASE_V2_S1_GROUPING_SETS.md` §7): a **rolled-up NULL** (a column
//! aggregated away) must be distinguishable from a **genuine data NULL**,
//! via the grouping id — never by "is the value NULL?".
//!
//! Hermetic: a tiny in-memory table, no TPC-DS data, so it runs in CI.

use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use ematix_flow_core::preset;

/// `t(g: Utf8, h: Utf8, v: i64)` on the shared v2 session, with a
/// **genuine data NULL** in `h` (row `("a", NULL, 2)`) — the fixture that
/// makes the rolled-up-NULL vs data-NULL distinction testable.
async fn ctx_with_t() -> SessionContext {
    let ctx = preset::session_context();
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Utf8, true),
        Field::new("h", DataType::Utf8, true),
        Field::new("v", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![
                Some("a"),
                Some("a"),
                Some("b"),
                Some("b"),
            ])),
            Arc::new(StringArray::from(vec![
                Some("x"),
                None, // genuine data NULL in a group column
                Some("x"),
                Some("y"),
            ])),
            Arc::new(Int64Array::from(vec![1_i64, 2, 4, 8])),
        ],
    )
    .unwrap();
    let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("t", Arc::new(table)).unwrap();
    ctx
}

/// Run `sql` and render each row as a canonical `key=val|…` string so the
/// assertions read like the expected result set. String columns render
/// NULL as `∅`; every numeric output column is `CAST(... AS BIGINT)` in
/// the query so it reads back uniformly as an `Int64Array`. Returns the
/// rows SORTED, so tests are order-independent.
async fn rows(ctx: &SessionContext, sql: &str) -> Vec<String> {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let mut out = Vec::new();
    for b in &batches {
        let schema = b.schema();
        for r in 0..b.num_rows() {
            let mut cells = Vec::with_capacity(schema.fields().len());
            for (c, field) in schema.fields().iter().enumerate() {
                let col = b.column(c);
                let val = if let Some(s) = col.as_any().downcast_ref::<StringArray>() {
                    if s.is_null(r) {
                        "∅".to_string()
                    } else {
                        s.value(r).to_string()
                    }
                } else if let Some(i) = col.as_any().downcast_ref::<Int64Array>() {
                    if i.is_null(r) {
                        "∅".to_string()
                    } else {
                        i.value(r).to_string()
                    }
                } else {
                    panic!(
                        "unexpected arrow type {:?} for column {} — cast numerics to BIGINT",
                        col.data_type(),
                        field.name()
                    );
                };
                cells.push(format!("{}={}", field.name(), val));
            }
            out.push(cells.join("|"));
        }
    }
    out.sort();
    out
}

/// **The gate (§7).** `ROLLUP(g, h)` over a table whose `h` has a genuine
/// data NULL must produce two *distinct* `g="a"` rows with `h` NULL:
///   * the `(g, h)` set row where `h` is the DATA null → `gh (GROUPING(h)) = 0`,
///     `SUM = 2`;
///   * the `(g)` set row where `h` is ROLLED UP → `gh = 1`, `SUM = 3`.
/// If an implementation derived `GROUPING(h)` from "is h NULL?" instead of
/// the grouping id, these two rows would collapse / mislabel — this test
/// catches exactly that.
#[tokio::test]
async fn rollup_distinguishes_data_null_from_rolled_up_null() {
    let ctx = ctx_with_t().await;
    let got = rows(
        &ctx,
        "SELECT g, h, \
                CAST(GROUPING(g) AS BIGINT) gg, \
                CAST(GROUPING(h) AS BIGINT) gh, \
                CAST(SUM(v) AS BIGINT) s \
         FROM t GROUP BY ROLLUP(g, h)",
    )
    .await;

    // sets {(g,h), (g), ()} — 7 rows.
    let mut expect = vec![
        // set (g,h): both present, gg=0 gh=0
        "g=a|h=x|gg=0|gh=0|s=1",
        "g=a|h=∅|gg=0|gh=0|s=2", // h is the DATA null here → gh=0
        "g=b|h=x|gg=0|gh=0|s=4",
        "g=b|h=y|gg=0|gh=0|s=8",
        // set (g): h rolled up → gh=1
        "g=a|h=∅|gg=0|gh=1|s=3", // same g="a", h NULL, but ROLLED UP → gh=1, sum=1+2
        "g=b|h=∅|gg=0|gh=1|s=12",
        // set (): grand total, both rolled up
        "g=∅|h=∅|gg=1|gh=1|s=15",
    ]
    .into_iter()
    .map(String::from)
    .collect::<Vec<_>>();
    expect.sort();

    assert_eq!(
        got, expect,
        "ROLLUP(g,h) result set / GROUPING() labels wrong"
    );
}

/// `CUBE(g, h)` emits all four sets, and the composed grouping id must
/// equal the set's mask with **g as the high bit, h as the low bit**
/// (`PHASE_V2_S1_GROUPING_SETS.md` §4.0): (g,h)→0, (g)→1, (h)→2, ()→3.
///
/// DataFusion 53 exposes `GROUPING()` but not the `GROUPING_ID(...)`
/// function, so — exactly as TPC-DS Q36 does (`grouping(a) + grouping(b)`)
/// — the id is composed from the per-column `GROUPING()` bits:
/// `GROUPING(g) * 2 + GROUPING(h)`.
#[tokio::test]
async fn cube_grouping_id_matches_each_set_mask() {
    let ctx = ctx_with_t().await;
    let got = rows(
        &ctx,
        "SELECT g, h, \
                CAST(GROUPING(g) AS BIGINT) * 2 + CAST(GROUPING(h) AS BIGINT) AS gid, \
                CAST(SUM(v) AS BIGINT) s \
         FROM t GROUP BY CUBE(g, h)",
    )
    .await;

    // sets {(g,h)=0, (g)=1, (h)=2, ()=3}.
    let mut expect = vec![
        // (g,h): gid=0
        "g=a|h=x|gid=0|s=1",
        "g=a|h=∅|gid=0|s=2", // data null, still gid=0 (both present)
        "g=b|h=x|gid=0|s=4",
        "g=b|h=y|gid=0|s=8",
        // (g): h rolled up → gid=1
        "g=a|h=∅|gid=1|s=3",
        "g=b|h=∅|gid=1|s=12",
        // (h): g rolled up → gid=2 (note the data-null h forms its own h-group)
        "g=∅|h=x|gid=2|s=5",
        "g=∅|h=y|gid=2|s=8",
        "g=∅|h=∅|gid=2|s=2", // h = the DATA null, g rolled up
        // (): grand total → gid=3
        "g=∅|h=∅|gid=3|s=15",
    ]
    .into_iter()
    .map(String::from)
    .collect::<Vec<_>>();
    expect.sort();

    assert_eq!(got, expect, "CUBE(g,h) GROUPING_ID / set membership wrong");
}

/// Explicit `GROUPING SETS` desugars to exactly the listed sets — no more,
/// no fewer — and the grand-total `()` set yields a single all-rolled-up
/// row carrying the full `SUM`.
#[tokio::test]
async fn explicit_grouping_sets_and_grand_total() {
    let ctx = ctx_with_t().await;
    let got = rows(
        &ctx,
        "SELECT g, CAST(SUM(v) AS BIGINT) s \
         FROM t GROUP BY GROUPING SETS ((g), ())",
    )
    .await;

    let mut expect = vec!["g=a|s=3", "g=b|s=12", "g=∅|s=15"]
        .into_iter()
        .map(String::from)
        .collect::<Vec<_>>();
    expect.sort();

    assert_eq!(got, expect, "explicit GROUPING SETS ((g),()) wrong");
}
