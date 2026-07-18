//! v2 S2 SETOP.1 + SETOP.3 — set-operator semantic contract + plan pin.
//!
//! Locks the *observable behaviour* of the set operations TPC-DS uses
//! (audit 2026-07-18, `PHASE_V2_S2_SET_OPERATORS.md`): `INTERSECT`,
//! `EXCEPT` (both **DISTINCT** — TPC-DS has no `ALL` variants), and a
//! large literal `IN (…)` — on the **shared v2 session**
//! (`preset::session_context()`, dogfooding S0.1).
//!
//! **Standing regression guard.** The set-op probe found no dedicated
//! set-op operator: `INTERSECT` lowers to a semi-join + a DISTINCT
//! aggregate, `EXCEPT` to an anti-join + aggregate, both landing on
//! ematix's accelerated semi/anti-join path; a large literal `IN` lowers
//! to a single `InList` membership, not an OR-chain / not N joins. These
//! tests pin (1) the **DISTINCT dedup semantics** — the contract most
//! likely to catch a wrong lowering — and (2) the **physical shape**, so
//! a DF upgrade that regresses the lowering (drops join lowering, or
//! expands `IN` to ORs) is caught.
//!
//! Hermetic: tiny in-memory tables, no TPC-DS data, so it runs in CI.

use std::sync::Arc;

use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::physical_plan::displayable;
use datafusion::prelude::SessionContext;
use ematix_flow_core::preset;

/// Registers `a(x)` and `b(x)` on the shared v2 session, both with
/// **duplicate** rows so the DISTINCT dedup contract is testable:
/// `a = {1,1,2,3,3,4}`, `b = {3,3,4,5}`. So `a INTERSECT b = {3,4}`
/// (distinct), `a EXCEPT b = {1,2}` (distinct).
async fn ctx_with_ab() -> SessionContext {
    let ctx = preset::session_context();
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
    let mk = |vals: Vec<i64>| {
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vals))]).unwrap();
        Arc::new(MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap())
    };
    ctx.register_table("a", mk(vec![1, 1, 2, 3, 3, 4])).unwrap();
    ctx.register_table("b", mk(vec![3, 3, 4, 5])).unwrap();
    ctx
}

/// Run `sql` (single `x` Int64 column) → sorted `Vec<i64>`.
async fn ints(ctx: &SessionContext, sql: &str) -> Vec<i64> {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let mut out = Vec::new();
    for b in &batches {
        let col = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("x should be Int64");
        for r in 0..b.num_rows() {
            out.push(col.value(r));
        }
    }
    out.sort();
    out
}

/// Physical-plan display string for `sql` on the shared v2 session.
async fn physical_display(ctx: &SessionContext, sql: &str) -> String {
    let plan = ctx
        .sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    format!("{}", displayable(plan.as_ref()).indent(false))
}

// ── SETOP.1: semantic contract ───────────────────────────────────────

/// **`INTERSECT` = distinct rows present in BOTH inputs.** Despite `a`
/// having two 3s and `b` having two 3s, the result carries a **single**
/// 3 (dedup). Result: `{3,4}`. An implementation that kept duplicates
/// (i.e. did `INTERSECT ALL`) or that used the wrong join side would fail.
#[tokio::test]
async fn intersect_is_distinct_and_dedups() {
    let ctx = ctx_with_ab().await;
    let got = ints(&ctx, "SELECT x FROM a INTERSECT SELECT x FROM b").await;
    assert_eq!(got, vec![3, 4], "INTERSECT DISTINCT semantics wrong");
}

/// **`EXCEPT` = distinct rows in the first input but NOT the second.**
/// `a EXCEPT b` = `{1,2}` (the two 1s in `a` collapse to a single 1; 3
/// and 4 are removed because they appear in `b`).
#[tokio::test]
async fn except_is_distinct_difference() {
    let ctx = ctx_with_ab().await;
    let got = ints(&ctx, "SELECT x FROM a EXCEPT SELECT x FROM b").await;
    assert_eq!(got, vec![1, 2], "EXCEPT DISTINCT semantics wrong");
}

/// **Large literal `IN (…)` is set membership, NOT dedup.** Unlike the
/// set operators, an `IN`-list filter keeps duplicate matching rows:
/// `a`'s rows with `x ∈ {2,3,4,…}` are `{2,3,3,4}` (both 3s kept). The
/// list is deliberately long to exercise the `InList` path (SETOP.3).
#[tokio::test]
async fn in_list_is_membership_keeps_duplicates() {
    let ctx = ctx_with_ab().await;
    let got = ints(
        &ctx,
        "SELECT x FROM a WHERE x IN (2, 3, 4, 7, 8, 9, 10, 11, 12, 13, 14, 15)",
    )
    .await;
    assert_eq!(got, vec![2, 3, 3, 4], "IN-list membership wrong");
}

// ── SETOP.3: plan-shape pin ──────────────────────────────────────────

/// `INTERSECT` lowers to a **semi-join + aggregate** (the DISTINCT
/// dedup) — no dedicated `IntersectExec`. Pins that the work lands on
/// ematix's accelerated join/agg path.
#[tokio::test]
async fn intersect_lowers_to_semi_join_and_aggregate() {
    let ctx = ctx_with_ab().await;
    let plan = physical_display(&ctx, "SELECT x FROM a INTERSECT SELECT x FROM b").await;
    assert!(
        plan.contains("Semi"),
        "INTERSECT should lower to a semi-join; plan was:\n{plan}"
    );
    assert!(
        plan.contains("AggregateExec"),
        "INTERSECT should include a DISTINCT aggregate; plan was:\n{plan}"
    );
    assert!(
        !plan.contains("IntersectExec"),
        "there should be no dedicated set-op operator; plan was:\n{plan}"
    );
}

/// `EXCEPT` lowers to an **anti-join + aggregate** — no `ExceptExec`.
#[tokio::test]
async fn except_lowers_to_anti_join_and_aggregate() {
    let ctx = ctx_with_ab().await;
    let plan = physical_display(&ctx, "SELECT x FROM a EXCEPT SELECT x FROM b").await;
    assert!(
        plan.contains("Anti"),
        "EXCEPT should lower to an anti-join; plan was:\n{plan}"
    );
    assert!(
        plan.contains("AggregateExec"),
        "EXCEPT should include a DISTINCT aggregate; plan was:\n{plan}"
    );
}

/// A literal `IN`-list stays a single filter — it does **not** expand
/// into an OR-chain of joins or a `UNION`. Pins "no blowup": the plan has
/// no `HashJoinExec` and no `UnionExec` for a pure membership filter.
#[tokio::test]
async fn in_list_does_not_expand_to_joins_or_union() {
    let ctx = ctx_with_ab().await;
    let plan = physical_display(
        &ctx,
        "SELECT x FROM a WHERE x IN (2, 3, 4, 7, 8, 9, 10, 11, 12, 13, 14, 15)",
    )
    .await;
    assert!(
        !plan.contains("HashJoinExec"),
        "IN-list must not expand to joins; plan was:\n{plan}"
    );
    assert!(
        !plan.contains("UnionExec"),
        "IN-list must not expand to a UNION; plan was:\n{plan}"
    );
}
