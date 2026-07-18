//! v2 S2 WIN.1 — window-function semantic contract.
//!
//! Locks the *observable behaviour* of the SQL window functions TPC-DS
//! actually uses (audit 2026-07-18, `PHASE_V2_S2_WINDOW_FUNCTIONS.md` §1):
//! `RANK()`, whole-partition `AVG`, and cumulative `SUM`
//! (`ROWS UNBOUNDED PRECEDING AND CURRENT ROW`) — on the **shared v2
//! session** (`preset::session_context()`, dogfooding S0.1).
//!
//! **Standing regression guard.** The S2.0 gate found DF-native window
//! execution competitive for 8 of 9 window queries (q51 the lone
//! candidate — see the Gate Verdict in the phase doc). These tests guard
//! the *correctness* of that DF-native path on the ematix session, and
//! would equally guard any future native operator. The contracts most
//! likely to catch a wrong implementation:
//!   * `RANK` ties produce **equal rank + a gap** (1,2,2,4 — never
//!     1,2,2,3), and rank **resets per partition**;
//!   * a whole-partition `AVG` (no `ORDER BY`) **broadcasts** the
//!     partition aggregate to every row;
//!   * a cumulative `SUM` **resets at the partition boundary** and is a
//!     true running total in `ORDER BY` order.
//!
//! Hermetic: a tiny in-memory table, no TPC-DS data, so it runs in CI.

use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::physical_plan::displayable;
use datafusion::prelude::SessionContext;
use ematix_flow_core::preset;

/// `w(p: Utf8, o: i64, v: i64, r: i64)` on the shared v2 session.
///   * `p` — partition key (two partitions, "a" with 4 rows, "b" with 2).
///   * `o` — the intra-partition order key (distinct per partition).
///   * `v` — the measure, chosen so each partition's `AVG(v)` is integral
///     (a=25, b=10) — so results read back cleanly as `Int64` after a
///     `CAST(... AS BIGINT)`.
///   * `r` — the rank key, with a **tie** in partition "a" (two rows at
///     20) so the tie→gap contract is testable.
async fn ctx_with_w() -> SessionContext {
    let ctx = preset::session_context();
    let schema = Arc::new(Schema::new(vec![
        Field::new("p", DataType::Utf8, false),
        Field::new("o", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new("r", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a", "a", "a", "a", "b", "b"])),
            Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4, 1, 2])),
            Arc::new(Int64Array::from(vec![10_i64, 20, 30, 40, 5, 15])),
            Arc::new(Int64Array::from(vec![10_i64, 20, 20, 40, 5, 8])),
        ],
    )
    .unwrap();
    let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("w", Arc::new(table)).unwrap();
    ctx
}

/// Run `sql` and render each row as `col=val|…`, SORTED for
/// order-independence. Every output column is `Int64` (queries
/// `CAST(... AS BIGINT)`), so this stays simple.
async fn rows(ctx: &SessionContext, sql: &str) -> Vec<String> {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let mut out = Vec::new();
    for b in &batches {
        let schema = b.schema();
        for row in 0..b.num_rows() {
            let mut cells = Vec::with_capacity(schema.fields().len());
            for (c, field) in schema.fields().iter().enumerate() {
                let col = b.column(c);
                let val = if let Some(s) = col.as_any().downcast_ref::<StringArray>() {
                    s.value(row).to_string()
                } else if let Some(i) = col.as_any().downcast_ref::<Int64Array>() {
                    i.value(row).to_string()
                } else {
                    panic!(
                        "unexpected arrow type {:?} for column {} — cast to BIGINT",
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

/// **RANK ties → equal rank + gap, and resets per partition.**
/// `rank() OVER (PARTITION BY p ORDER BY r)`: partition "a" has r =
/// {10,20,20,40} → ranks 1,2,2,**4** (the tie consumes rank 3 — there is
/// no rank-3 row). Partition "b" starts over at 1. An implementation that
/// emitted dense ranks (1,2,2,3) or that failed to reset per partition
/// would fail here.
#[tokio::test]
async fn rank_ties_gap_and_partition_reset() {
    let ctx = ctx_with_w().await;
    let got = rows(
        &ctx,
        "SELECT p, r, CAST(rank() OVER (PARTITION BY p ORDER BY r) AS BIGINT) rk \
         FROM w",
    )
    .await;

    let mut expect = vec![
        "p=a|r=10|rk=1",
        "p=a|r=20|rk=2",
        "p=a|r=20|rk=2", // tie → same rank as the row above
        "p=a|r=40|rk=4", // …and the next rank is 4, not 3 (the gap)
        "p=b|r=5|rk=1",  // partition "b" resets to 1
        "p=b|r=8|rk=2",
    ]
    .into_iter()
    .map(String::from)
    .collect::<Vec<_>>();
    expect.sort();

    assert_eq!(got, expect, "RANK tie/gap or partition-reset wrong");
}

/// **Global RANK (no PARTITION).** Over the whole table by `r`
/// {5,8,10,20,20,40} → 1,2,3,4,4,**6** — one ranking, ties still gap.
#[tokio::test]
async fn rank_global_no_partition() {
    let ctx = ctx_with_w().await;
    let got = rows(
        &ctx,
        "SELECT r, CAST(rank() OVER (ORDER BY r) AS BIGINT) rk FROM w",
    )
    .await;

    // Two rows share r=20 → both rank 4, then r=40 is rank 6.
    let mut expect = vec![
        "r=5|rk=1",
        "r=8|rk=2",
        "r=10|rk=3",
        "r=20|rk=4",
        "r=20|rk=4",
        "r=40|rk=6",
    ]
    .into_iter()
    .map(String::from)
    .collect::<Vec<_>>();
    expect.sort();

    assert_eq!(got, expect, "global RANK tie/gap wrong");
}

/// **Whole-partition AVG broadcasts (the q53/q63 shape).**
/// `avg(v) OVER (PARTITION BY p)` with no `ORDER BY` → the partition's
/// average is attached to *every* row of that partition: a=25 (×4),
/// b=10 (×2). The frame is the whole partition, not a running frame.
#[tokio::test]
async fn whole_partition_avg_broadcasts() {
    let ctx = ctx_with_w().await;
    let got = rows(
        &ctx,
        "SELECT p, o, CAST(avg(v) OVER (PARTITION BY p) AS BIGINT) a FROM w",
    )
    .await;

    let mut expect = vec![
        "p=a|o=1|a=25",
        "p=a|o=2|a=25",
        "p=a|o=3|a=25",
        "p=a|o=4|a=25",
        "p=b|o=1|a=10",
        "p=b|o=2|a=10",
    ]
    .into_iter()
    .map(String::from)
    .collect::<Vec<_>>();
    expect.sort();

    assert_eq!(got, expect, "whole-partition AVG broadcast wrong");
}

/// **Cumulative SUM resets at the partition boundary (the q51 shape).**
/// `sum(v) OVER (PARTITION BY p ORDER BY o ROWS UNBOUNDED PRECEDING AND
/// CURRENT ROW)` → a running total in `o` order that **restarts** in
/// partition "b": a → 10,30,60,100; b → 5,20. This is exactly the frame
/// the S2.0 gate flagged as q51's hotspot; the contract pins its meaning
/// regardless of whether DF or a future native operator executes it.
#[tokio::test]
async fn cumulative_sum_running_and_partition_reset() {
    let ctx = ctx_with_w().await;
    let got = rows(
        &ctx,
        "SELECT p, o, CAST(sum(v) OVER ( \
             PARTITION BY p ORDER BY o \
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS BIGINT) cume \
         FROM w",
    )
    .await;

    let mut expect = vec![
        "p=a|o=1|cume=10",
        "p=a|o=2|cume=30",
        "p=a|o=3|cume=60",
        "p=a|o=4|cume=100",
        "p=b|o=1|cume=5", // running total restarts in partition "b"
        "p=b|o=2|cume=20",
    ]
    .into_iter()
    .map(String::from)
    .collect::<Vec<_>>();
    expect.sort();

    assert_eq!(got, expect, "cumulative SUM running/partition-reset wrong");
}

// ── WIN.3: plan-shape pin ────────────────────────────────────────────
// Pins *which* DataFusion physical operator each window frame lowers to,
// so a DF upgrade that changes the operator (and thus the S2.0 gate's
// premises, or a future native operator's interception point) is caught.
// The shapes were recorded by `win_gate_probe` over real data; these
// hermetic assertions reproduce them without needing TPC-DS.

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

/// A bounded/running frame (`ROWS UNBOUNDED PRECEDING AND CURRENT ROW`,
/// the q51 shape) lowers to **`BoundedWindowAggExec`** in `Sorted` mode —
/// the operator the S2.0 gate measured as q51's hotspot.
#[tokio::test]
async fn cumulative_frame_lowers_to_bounded_window_agg() {
    let ctx = ctx_with_w().await;
    let plan = physical_display(
        &ctx,
        "SELECT p, sum(v) OVER ( \
             PARTITION BY p ORDER BY o \
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM w",
    )
    .await;
    assert!(
        plan.contains("BoundedWindowAggExec"),
        "cumulative ROWS frame should lower to BoundedWindowAggExec; plan was:\n{plan}"
    );
    assert!(
        plan.contains("mode=[Sorted]"),
        "cumulative window should run in Sorted mode; plan was:\n{plan}"
    );
}

/// A whole-partition aggregate window (no `ORDER BY`, the q53/q63 shape)
/// lowers to the unbounded **`WindowAggExec`**, not the bounded operator.
#[tokio::test]
async fn whole_partition_frame_lowers_to_window_agg() {
    let ctx = ctx_with_w().await;
    let plan = physical_display(&ctx, "SELECT p, avg(v) OVER (PARTITION BY p) FROM w").await;
    assert!(
        plan.contains("WindowAggExec"),
        "whole-partition window should lower to WindowAggExec; plan was:\n{plan}"
    );
}
