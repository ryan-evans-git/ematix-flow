//! KEYS.4.e — end-to-end proof that a UINT_64 column is processed UNSIGNED
//! through ematix-flow's `EmatixFastParquetTableProvider` after the
//! `arrow_type_for` flip (KEYS.4.a), decode (4.b), and stats (4.c).
//!
//! Fixture `tests/data/u64_keys.parquet` (written by ematix-parquet-codec's
//! `emit_u64_fixture`, `ColumnData::U64`) has:
//!   ukey:    u64 = [1, 100, 2^63, u64::MAX, 5]
//!   payload: i64 = [10, 20,  30,      40,  50]
//!
//! The u64 values straddle 2^63, so if the column were (buggily) read as
//! i64, both 2^63 and u64::MAX would be negative and would sort/compare
//! FIRST. Every assertion below fails in that case and passes only for
//! true unsigned semantics.

use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, RecordBatch, UInt64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::prelude::SessionContext;
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;

fn fixture() -> String {
    format!("{}/tests/data/u64_keys.parquet", env!("CARGO_MANIFEST_DIR"))
}

async fn ctx() -> SessionContext {
    let ctx = SessionContext::new();
    let provider = EmatixFastParquetTableProvider::try_new(fixture())
        .expect("EmatixFastParquetTableProvider must accept the UINT_64 fixture");
    ctx.register_table("u", Arc::new(provider)).unwrap();
    ctx
}

/// Flatten a single-i64-column result into a Vec<i64>.
fn i64_col(batches: &[RecordBatch]) -> Vec<i64> {
    let mut out = Vec::new();
    for b in batches {
        let a = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("payload column should be Int64");
        out.extend(a.values().iter().copied());
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uint64_column_decodes_as_unsigned() {
    let ctx = ctx().await;
    let batches = ctx
        .sql("SELECT ukey FROM u")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert!(!batches.is_empty());
    // The flip (4.a) makes the column genuinely UInt64, not Int64.
    assert_eq!(batches[0].schema().field(0).data_type(), &DataType::UInt64);
    let mut got: Vec<u64> = Vec::new();
    for b in &batches {
        let a = b
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("ukey should decode to a UInt64Array (4.b bitcast)");
        got.extend(a.values().iter().copied());
    }
    got.sort_unstable();
    // 2^63 and u64::MAX survive as their true unsigned values.
    assert_eq!(got, vec![1, 5, 100, 1u64 << 63, u64::MAX]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_by_uint64_is_unsigned() {
    let ctx = ctx().await;
    let batches = ctx
        .sql("SELECT payload FROM u ORDER BY ukey ASC")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    // Unsigned ascending of ukey [1,100,2^63,u64::MAX,5] = 1,5,100,2^63,u64::MAX
    // → payloads 10,50,20,30,40. (The i64-buggy order would be 30,40,10,50,20.)
    assert_eq!(i64_col(&batches), vec![10, 50, 20, 30, 40]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn range_filter_on_uint64_is_unsigned() {
    let ctx = ctx().await;
    // ukey > 2^62 (unsigned) matches only 2^63 and u64::MAX → payloads 30,40.
    // arrow_cast pins the literal to UInt64 so the comparison is unsigned;
    // read as i64, 2^63 and u64::MAX are negative and would NOT pass.
    let batches = ctx
        .sql(
            "SELECT payload FROM u \
             WHERE ukey > arrow_cast(4611686018427387904, 'UInt64') \
             ORDER BY payload",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(i64_col(&batches), vec![30, 40]);
}
