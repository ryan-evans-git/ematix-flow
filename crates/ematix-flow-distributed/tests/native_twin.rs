//! Σ.TW.1 — native-twin routing tests (see docs/ADR_NATIVE_TWIN_ROUTING.md).
//!
//! The SF100 4-leg A/B (STAMP 20260715T113833Z) proved a distributed
//! session's LOCAL commits lose ~1.6 s on Q21 / ~0.4 s on Q15 to the
//! native single-node session, because the localize path cannot carry
//! planning-time schema levers (KEYS.2 i32-key downcast). The twin
//! fixes that by re-planning join queries in a REAL native session.
//!
//! These tests pin the three claims the fix stands on:
//! 1. the twin re-registers stock parquet tables through the NATIVE
//!    fast provider (file → single, dir → multi) — that is where the
//!    downcast auto-gate lives;
//! 2. the twin's optimizer chain IS the production single-node preset
//!    (the anti-#219 pin: green-in-isolation configs are worthless);
//! 3. routing = local commit ∧ has-join; answers are identical to
//!    stock DataFusion.

use std::sync::Arc;

use datafusion::arrow::array::{Float64Array, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::prelude::SessionContext;
use ematix_flow_distributed::native_twin::{
    native_twin_ctx, plan_has_join, plan_is_mesh, should_route_to_twin,
};

/// One parquet file of `n` rows (k Int64 in 0..modulo, v Float64
/// integral — integral so float aggregates are order-independent and
/// exactly comparable across sessions). Column names deliberately do
/// NOT end in "key": the KEYS.2 downcast keys on that suffix and would
/// make the fixture schema env/scale-dependent.
fn write_parquet(path: &std::path::Path, n: i64, modulo: i64, offset: i64) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(
                (0..n).map(|i| (offset + i) % modulo).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                (0..n).map(|i| (offset + i) as f64).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("batch");
    let file = std::fs::File::create(path).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer");
    writer.write(&batch).expect("write");
    writer.close().expect("close");
}

/// Fixture layout: `probe_t.parquet` (500 rows), `build_t.parquet`
/// (8 rows), and `parted_t/` (a directory of 2 part files — the
/// multi-file layout the SF100 campaign uses for every table).
fn write_fixture() -> tempfile::TempDir {
    let tmp = tempfile::tempdir().expect("tempdir");
    write_parquet(&tmp.path().join("probe_t.parquet"), 500, 16, 0);
    write_parquet(&tmp.path().join("build_t.parquet"), 8, 8, 0);
    let dir = tmp.path().join("parted_t");
    std::fs::create_dir(&dir).expect("mkdir");
    write_parquet(&dir.join("parted_t-0001.parquet"), 250, 16, 0);
    write_parquet(&dir.join("parted_t-0002.parquet"), 250, 16, 250);
    tmp
}

/// A "distributed-session-shaped" ctx: stock DataFusion with the
/// tables registered via `register_parquet` (arrow-rs ListingTable) —
/// exactly how the campaign registers tables when peers exist, because
/// distributed stages must stay codec-serializable.
async fn stock_ctx(tmp: &tempfile::TempDir) -> SessionContext {
    let ctx = SessionContext::new();
    ctx.register_parquet(
        "probe_t",
        tmp.path().join("probe_t.parquet").to_str().unwrap(),
        Default::default(),
    )
    .await
    .expect("register probe_t");
    ctx.register_parquet(
        "build_t",
        tmp.path().join("build_t.parquet").to_str().unwrap(),
        Default::default(),
    )
    .await
    .expect("register build_t");
    ctx.register_parquet(
        "parted_t",
        tmp.path().join("parted_t").to_str().unwrap(),
        Default::default(),
    )
    .await
    .expect("register parted_t");
    ctx
}

const JOIN_SQL: &str = "SELECT b.k AS bk, count(*) AS c, sum(p.v) AS sv \
     FROM probe_t p JOIN build_t b ON p.k = b.k \
     GROUP BY b.k ORDER BY bk";

const SCAN_AGG_SQL: &str = "SELECT count(*) AS c, sum(v) AS sv FROM probe_t";

/// Claim 1: stock ListingTables come back as NATIVE fast providers —
/// single-file → `EmatixFastParquetTableProvider`, directory →
/// `EmatixFastParquetMultiTableProvider`.
#[tokio::test]
async fn twin_reregisters_stock_tables_through_the_native_fast_provider() {
    use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
    use ematix_flow_core::ematix_fast_parquet_multi::EmatixFastParquetMultiTableProvider;

    let tmp = write_fixture();
    let ctx = stock_ctx(&tmp).await;
    let twin = native_twin_ctx(&ctx).await.expect("twin");

    async fn provider_of(
        ctx: &SessionContext,
        name: &str,
    ) -> Arc<dyn datafusion::catalog::TableProvider> {
        ctx.catalog("datafusion")
            .expect("catalog")
            .schema("public")
            .expect("schema")
            .table(name)
            .await
            .expect("lookup")
            .unwrap_or_else(|| panic!("{name} not registered on twin"))
    }

    let probe = provider_of(&twin, "probe_t").await;
    assert!(
        probe.as_any().is::<EmatixFastParquetTableProvider>(),
        "single-file table must re-register through the native single-file fast provider"
    );
    let parted = provider_of(&twin, "parted_t").await;
    assert!(
        parted.as_any().is::<EmatixFastParquetMultiTableProvider>(),
        "parted directory table must re-register through the native multi-file fast provider"
    );
}

/// Claim 2 (the anti-#219 pin): the twin's rule chain is byte-for-byte
/// the production single-node preset — so every planning-time lever
/// (downcast, runtime blooms, grace) is the NATIVE configuration, not
/// a hand-assembled approximation of it.
#[tokio::test]
async fn twin_rule_chain_equals_production_single_node_preset() {
    let tmp = write_fixture();
    let ctx = stock_ctx(&tmp).await;
    let twin = native_twin_ctx(&ctx).await.expect("twin");

    let (physical, logical) = ematix_flow_core::preset::ematix_rule_names(&twin.state());
    assert_eq!(
        physical,
        ematix_flow_core::preset::PRODUCTION_PHYSICAL_RULE_NAMES,
        "twin physical chain must equal the production single-node preset"
    );
    assert_eq!(
        logical,
        ematix_flow_core::preset::PRODUCTION_LOGICAL_RULE_NAMES,
        "twin logical chain must equal the production single-node preset"
    );
}

/// Claim 3a: routing predicate — a locally-committed plan WITH a join
/// routes to the twin; a scan-aggregate does not (localize is measured
/// FASTER there: Q01 −706 ms / Q06 −262 ms vs native at SF100).
#[tokio::test]
async fn local_join_commits_route_to_twin_scan_aggs_do_not() {
    let tmp = write_fixture();
    let ctx = stock_ctx(&tmp).await;

    let join_plan = ctx
        .sql(JOIN_SQL)
        .await
        .expect("sql")
        .create_physical_plan()
        .await
        .expect("plan");
    assert!(plan_has_join(&join_plan), "join plan must report a join");
    assert!(
        !plan_is_mesh(&join_plan),
        "no workers → the plan must be a local commit"
    );
    assert!(
        should_route_to_twin(&join_plan),
        "local commit with a join must route to the native twin"
    );

    let agg_plan = ctx
        .sql(SCAN_AGG_SQL)
        .await
        .expect("sql")
        .create_physical_plan()
        .await
        .expect("plan");
    assert!(
        !plan_has_join(&agg_plan),
        "scan-aggregate must not report a join"
    );
    assert!(
        !should_route_to_twin(&agg_plan),
        "scan-aggregate must stay on the localize path"
    );
}

/// Claim 3b: answers are identical — the twin re-plans the SAME SQL in
/// a different session, so this is the whole-fix answer-preservation
/// gate (stock DataFusion is the parity oracle).
#[tokio::test]
async fn twin_join_answers_match_stock_datafusion() {
    let tmp = write_fixture();
    let ctx = stock_ctx(&tmp).await;
    let twin = native_twin_ctx(&ctx).await.expect("twin");

    for sql in [JOIN_SQL, SCAN_AGG_SQL] {
        let stock = ctx
            .sql(sql)
            .await
            .expect("sql")
            .collect()
            .await
            .expect("collect");
        let native = twin
            .sql(sql)
            .await
            .expect("sql")
            .collect()
            .await
            .expect("collect");
        let stock_rows: usize = stock.iter().map(|b| b.num_rows()).sum();
        let native_rows: usize = native.iter().map(|b| b.num_rows()).sum();
        assert_eq!(stock_rows, native_rows, "row count parity for {sql}");
        assert_eq!(
            pretty_format_batches(&stock).expect("fmt").to_string(),
            pretty_format_batches(&native).expect("fmt").to_string(),
            "value parity for {sql}"
        );
    }
}
