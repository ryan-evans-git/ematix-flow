//! Phase 39.1 acceptance benchmarks for the SQL transform layer.
//!
//! These benches validate the targets documented in
//! `docs/SQL_TRANSFORMS_PLAN.md`:
//!
//! | Workload                                         | Target                    |
//! | ------------------------------------------------ | ------------------------- |
//! | 1000-row batch, identity (`SELECT *`)            | <5% overhead vs zero      |
//! | 1000-row batch, single-column filter             | <10% overhead vs raw      |
//! | Plan compilation (single-source SQL)             | <100ms                    |
//! | MemTable load (10K rows, 10 columns)             | <100ms                    |
//!
//! The end-to-end "≥80% throughput of zero-transform baseline"
//! target requires real Kafka + Postgres infrastructure and lives
//! in the integration-test track, not here.
//!
//! Run with `cargo bench -p ematix-flow-core --bench transform`.

use std::sync::Arc;

use arrow_array::{BooleanArray, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use arrow_select::filter::filter_record_batch;
use criterion::{Criterion, criterion_group, criterion_main};
use ematix_flow_core::join::{JoinConfig, JoinKind, JoinLateDataPolicy, TimeWindowedJoinTransform};
use ematix_flow_core::transform::{BatchContext, BatchTransform, DataFusionTransform, LookupTable};
use ematix_flow_core::windowed::{
    AggKind, AggregationSpec, LateDataPolicy, WindowConfig, WindowKind, WindowedAggregateTransform,
};
use tokio::runtime::Runtime;

fn schema_id_value() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
    ]))
}

/// Build a 1000-row batch with `id ∈ [0, 1000)` and `value ∈ [0, 100)`.
/// 100 distinct values mean a `WHERE value = 0` filter keeps ~1% of
/// rows — enough to exercise the predicate, not so many that DataFusion
/// short-circuits.
fn build_thousand_row_batch() -> RecordBatch {
    let ids: Vec<i32> = (0..1000).collect();
    let values: Vec<i32> = (0..1000).map(|i| i % 100).collect();
    RecordBatch::try_new(
        schema_id_value(),
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(Int32Array::from(values)),
        ],
    )
    .unwrap()
}

/// 10K-row, 10-column lookup table — used by both the lookup-join
/// bench and the MemTable-load bench.
fn build_ten_thousand_row_lookup() -> LookupTable {
    let n: usize = 10_000;
    let mut fields: Vec<Field> = Vec::with_capacity(10);
    fields.push(Field::new("id", DataType::Int32, false));
    for i in 1..10 {
        fields.push(Field::new(format!("col{i}"), DataType::Utf8, false));
    }
    let schema: SchemaRef = Arc::new(Schema::new(fields));

    let ids: Vec<i32> = (0..n as i32).collect();
    let mut columns: Vec<Arc<dyn arrow_array::Array>> = Vec::with_capacity(10);
    columns.push(Arc::new(Int32Array::from(ids)));
    for i in 1..10 {
        let strs: Vec<String> = (0..n).map(|r| format!("col{i}-row{r}")).collect();
        columns.push(Arc::new(StringArray::from(strs)));
    }
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    LookupTable::new("dim", schema, vec![batch])
}

fn bench_baseline_clone(c: &mut Criterion) {
    let batch = build_thousand_row_batch();
    c.bench_function("baseline_clone_1000_rows", |b| {
        b.iter(|| {
            // Approximates the zero-transform path: forwarding a
            // RecordBatch unchanged. Measures the cost of cloning
            // an Arc-shared columnar batch (essentially refcount
            // bumps + a Vec clone — the columns themselves are
            // refcounted).
            std::hint::black_box(batch.clone());
        })
    });
}

fn bench_identity_transform(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let schema = schema_id_value();
    let batch = build_thousand_row_batch();
    // SELECT * triggers the trivial-projection bypass — i.e. a
    // RecordBatch::project(...) over the full column index list,
    // no DataFusion involved.
    let transform = rt.block_on(async {
        DataFusionTransform::new("SELECT * FROM source", schema)
            .await
            .expect("construct identity transform")
    });
    assert!(
        transform.is_trivial(),
        "SELECT * must hit the trivial bypass"
    );

    c.bench_function("identity_transform_1000_rows", |b| {
        b.to_async(&rt).iter(|| {
            let batch = batch.clone();
            let transform = &transform;
            async move {
                let out = transform
                    .transform(batch, &BatchContext::default())
                    .await
                    .unwrap();
                std::hint::black_box(out);
            }
        })
    });
}

/// Hand-written filter baseline for the WHERE-clause comparator
/// target: a developer who decides not to take the DataFusion
/// dependency would write essentially this code.
fn raw_filter_value_eq_zero(batch: &RecordBatch) -> RecordBatch {
    let col = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("column 1 is Int32Array");
    let mask = BooleanArray::from_iter((0..col.len()).map(|i| Some(col.value(i) == 0)));
    filter_record_batch(batch, &mask).expect("filter_record_batch")
}

fn bench_raw_filter_baseline(c: &mut Criterion) {
    let batch = build_thousand_row_batch();
    c.bench_function("raw_filter_1000_rows_value_eq_zero", |b| {
        b.iter(|| {
            let out = raw_filter_value_eq_zero(&batch);
            std::hint::black_box(out);
        })
    });
}

fn bench_filter_transform(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let schema = schema_id_value();
    let batch = build_thousand_row_batch();
    let transform = rt.block_on(async {
        DataFusionTransform::new("SELECT id, value FROM source WHERE value = 0", schema)
            .await
            .expect("construct filter transform")
    });
    assert!(
        !transform.is_trivial(),
        "WHERE clause must use the DataFusion path"
    );

    c.bench_function("filter_transform_1000_rows", |b| {
        b.to_async(&rt).iter(|| {
            let batch = batch.clone();
            let transform = &transform;
            async move {
                let out = transform
                    .transform(batch, &BatchContext::default())
                    .await
                    .unwrap();
                std::hint::black_box(out);
            }
        })
    });
}

fn bench_lookup_join_transform(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let schema = schema_id_value();
    let batch = build_thousand_row_batch();
    // 10K-row lookup, joined per batch. Stresses the per-batch hot
    // path with the most expensive realistic lookup shape.
    let lookup = build_ten_thousand_row_lookup();
    let transform = rt.block_on(async {
        DataFusionTransform::new_with_lookups(
            "SELECT s.id, s.value, d.col1 \
             FROM source s INNER JOIN dim d ON s.id = d.id",
            schema,
            vec![lookup],
        )
        .await
        .expect("construct lookup-join transform")
    });

    c.bench_function("lookup_join_1000_rows_into_10k_dim", |b| {
        b.to_async(&rt).iter(|| {
            let batch = batch.clone();
            let transform = &transform;
            async move {
                let out = transform
                    .transform(batch, &BatchContext::default())
                    .await
                    .unwrap();
                std::hint::black_box(out);
            }
        })
    });
}

fn bench_plan_compilation(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let schema = schema_id_value();

    // Plan compile is a one-time cost per pipeline lifetime — we
    // amortize over hours of streaming. The target is <100ms; the
    // bench surfaces drift if a future DataFusion bump or a more
    // complex SQL surface starts blowing past that.
    c.bench_function("plan_compile_filter_sql", |b| {
        b.to_async(&rt).iter(|| {
            let schema = schema.clone();
            async move {
                let t = DataFusionTransform::new(
                    "SELECT id, value FROM source WHERE value = 0",
                    schema,
                )
                .await
                .unwrap();
                std::hint::black_box(t);
            }
        })
    });
}

fn bench_lookup_load_construction(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let schema = schema_id_value();
    // The MemTable-load target measures construction of a 10K x 10
    // lookup, registered into the SessionContext + planned. We
    // build a fresh transform every iteration — that's the
    // point-in-time work the pipeline does at startup.
    c.bench_function("lookup_load_10k_x_10cols_register_and_plan", |b| {
        b.to_async(&rt)
            .iter_with_setup(build_ten_thousand_row_lookup, |lookup| {
                let schema = schema.clone();
                async move {
                    let t = DataFusionTransform::new_with_lookups(
                        "SELECT s.id FROM source s INNER JOIN dim d ON s.id = d.id",
                        schema,
                        vec![lookup],
                    )
                    .await
                    .unwrap();
                    std::hint::black_box(t);
                }
            })
    });
}

/// Build a 1000-row event batch with `_event_ts` spread across one
/// minute (windows `[0, 60s)` boundary at 60_000_000 µs). Distinct
/// `user_id` cardinality = 100, so the windowed bench exercises a
/// realistic group-by ratio.
fn build_event_batch_for_windowed() -> RecordBatch {
    use arrow_array::TimestampMicrosecondArray;
    let n: usize = 1000;
    let user_ids: Vec<i64> = (0..n as i64).map(|i| i % 100).collect();
    let amounts: Vec<i64> = (0..n as i64).map(|i| 10 + (i % 50)).collect();
    // ts spread evenly across 0..60_000_000 µs (one full window).
    let step = 60_000_000_i64 / n as i64;
    let ts: Vec<i64> = (0..n as i64).map(|i| i * step).collect();
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Int64, false),
        Field::new("amount", DataType::Int64, false),
        Field::new(
            "_event_ts",
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arrow_array::Int64Array::from(user_ids)),
            Arc::new(arrow_array::Int64Array::from(amounts)),
            Arc::new(TimestampMicrosecondArray::from(ts).with_timezone("UTC")),
        ],
    )
    .unwrap()
}

fn bench_windowed_tumbling_ingest(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("windowed_tumbling_1000_rows_100_keys_ingest", |b| {
        b.to_async(&rt).iter_with_setup(
            || {
                // Fresh transform per iteration so state doesn't
                // accumulate. Construction itself is cheap (no SQL
                // compile). The bench measures ingest of a
                // 1000-row batch into a 1-minute window.
                let cfg = WindowConfig {
                    kind: WindowKind::Tumbling,
                    duration_ms: 60_000,
                    hop_ms: 60_000,
                    gap_ms: None,
                    max_session_duration_ms: None,
                    event_time_column: "_event_ts".into(),
                    group_by: vec!["user_id".into()],
                    aggregations: vec![
                        AggregationSpec::new(AggKind::CountStar, None, "n"),
                        AggregationSpec::new(AggKind::Sum, Some("amount".into()), "amount_sum"),
                    ],
                    late_data: LateDataPolicy::Drop,
                    max_groups_per_window: 100_000,
                    window_start_column: "window_start".into(),
                    window_end_column: "window_end".into(),
                    session_id_column: "session_id".into(),
                };
                let t = WindowedAggregateTransform::new(cfg, None).unwrap();
                (t, build_event_batch_for_windowed())
            },
            |(t, batch)| async move {
                // global_wm before window end → ingest only, no emit.
                let ctx = BatchContext {
                    global_wm: Some(30_000_000),
                    source_id: None,
                };
                let out = t.transform(batch, &ctx).await.unwrap();
                std::hint::black_box(out);
            },
        )
    });
}

fn bench_windowed_tumbling_emit(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function(
        "windowed_tumbling_1000_rows_100_keys_ingest_and_emit",
        |b| {
            b.to_async(&rt).iter_with_setup(
                || {
                    let cfg = WindowConfig {
                        kind: WindowKind::Tumbling,
                        duration_ms: 60_000,
                        hop_ms: 60_000,
                        gap_ms: None,
                        max_session_duration_ms: None,
                        event_time_column: "_event_ts".into(),
                        group_by: vec!["user_id".into()],
                        aggregations: vec![AggregationSpec::new(
                            AggKind::Sum,
                            Some("amount".into()),
                            "amount_sum",
                        )],
                        late_data: LateDataPolicy::Drop,
                        max_groups_per_window: 100_000,
                        window_start_column: "window_start".into(),
                        window_end_column: "window_end".into(),
                        session_id_column: "session_id".into(),
                    };
                    let t = WindowedAggregateTransform::new(cfg, None).unwrap();
                    (t, build_event_batch_for_windowed())
                },
                |(t, batch)| async move {
                    // global_wm past window end → ingest, then emit one window.
                    let ctx = BatchContext {
                        global_wm: Some(60_000_001),
                        source_id: None,
                    };
                    let out = t.transform(batch, &ctx).await.unwrap();
                    std::hint::black_box(out);
                },
            )
        },
    );
}

/// P3 #22 baseline bench: stream-stream `TimeWindowedJoinTransform`.
///
/// The roadmap item ("columnar buffer storage for joins") proposes
/// replacing the per-row `BufferedRow { values: Vec<ScalarValue> }`
/// shape with `RecordBatch` references + row indices. The roadmap
/// explicitly says "profile first" — these benches give us the
/// before number to decide whether the refactor's worth doing.
///
/// Schema matches a realistic event-join: an Int64 join key, a
/// Float64 metric, a 32-byte Utf8 column (per-row `String` heap
/// alloc on extract — the most expensive `extract_scalar` branch),
/// plus the required `_event_ts` column. Both sides are 1000 rows.
fn build_join_batch(prefix: &'static str, key_offset: i64) -> RecordBatch {
    use arrow_array::{Float64Array, Int64Array, TimestampMicrosecondArray};
    let n: usize = 1000;
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("metric", DataType::Float64, false),
        Field::new("note", DataType::Utf8, false),
        Field::new(
            "_event_ts",
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
    ]));
    let keys: Vec<i64> = (0..n as i64).map(|i| key_offset + (i % 100)).collect();
    let metrics: Vec<f64> = (0..n).map(|i| (i as f64) * 1.5).collect();
    let notes: Vec<String> = (0..n)
        .map(|i| format!("{prefix}-row-{i:020}-padding"))
        .collect();
    // Spread timestamps across the 1s join window.
    let ts: Vec<i64> = (0..n as i64).map(|i| i * 1000).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(Float64Array::from(metrics)),
            Arc::new(StringArray::from(notes)),
            Arc::new(TimestampMicrosecondArray::from(ts).with_timezone("UTC")),
        ],
    )
    .unwrap()
}

fn build_join_config() -> JoinConfig {
    JoinConfig {
        kind: JoinKind::Inner,
        left_source: "left".into(),
        right_source: "right".into(),
        left_keys: vec!["key".into()],
        right_keys: vec!["key".into()],
        time_window_ms: 60_000,
        min_delta_ms: None,
        max_delta_ms: None,
        event_time_column: "_event_ts".into(),
        late_data: JoinLateDataPolicy::Drop,
        left_column_prefix: "left_".into(),
        right_column_prefix: "right_".into(),
    }
}

/// 1000-row left batch ingested into an empty buffer. No emit (right
/// side is empty). Stresses `extract_scalar` × 4 columns + the
/// `BufferedRow { values: Vec<ScalarValue> }` allocation path —
/// this is what the columnar refactor would target.
fn bench_join_ingest_no_match(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    c.bench_function("join_ingest_1000_rows_no_match", |b| {
        b.to_async(&rt).iter_with_setup(
            || {
                let t = TimeWindowedJoinTransform::new(build_join_config()).unwrap();
                (t, build_join_batch("L", 0))
            },
            |(t, batch)| async move {
                let ctx = BatchContext {
                    global_wm: None,
                    source_id: Some("left".into()),
                };
                let out = t.transform(batch, &ctx).await.unwrap();
                std::hint::black_box(out);
            },
        )
    });
}

/// 1000 left + 1000 right with 100 distinct keys per side → each
/// arriving right row matches every queued left row of the same key
/// (10 matches/key). Exercises `BufferedRow` ingest, `values.clone()`
/// per match, `EmittedRow` accumulation, and `build_emit_batch`'s
/// per-column scalars-to-array materialisation.
fn bench_join_ingest_high_match_rate(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    c.bench_function("join_steady_state_1000x1000_match_rate", |b| {
        b.to_async(&rt).iter_with_setup(
            || {
                let t = TimeWindowedJoinTransform::new(build_join_config()).unwrap();
                let l = build_join_batch("L", 0);
                let r = build_join_batch("R", 0);
                (t, l, r)
            },
            |(t, l, r)| async move {
                let ctx_l = BatchContext {
                    global_wm: None,
                    source_id: Some("left".into()),
                };
                let ctx_r = BatchContext {
                    global_wm: None,
                    source_id: Some("right".into()),
                };
                // Left first → buffered (no match yet, right is empty).
                let _ = t.transform(l, &ctx_l).await.unwrap();
                // Right next → every row matches every queued left
                // with the same key.
                let out = t.transform(r, &ctx_r).await.unwrap();
                std::hint::black_box(out);
            },
        )
    });
}

criterion_group!(
    benches,
    bench_baseline_clone,
    bench_identity_transform,
    bench_raw_filter_baseline,
    bench_filter_transform,
    bench_lookup_join_transform,
    bench_plan_compilation,
    bench_lookup_load_construction,
    bench_windowed_tumbling_ingest,
    bench_windowed_tumbling_emit,
    bench_join_ingest_no_match,
    bench_join_ingest_high_match_rate,
);
criterion_main!(benches);
