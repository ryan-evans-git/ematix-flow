//! Gate for the general parallel hash-aggregate breaker.
//!
//! `HashAggregateSink<N, B>` is the reusable `GROUP BY key → SUM(measures)`
//! breaker the driver runs per worker and the caller merges — the general
//! machinery that retires per-query aggregation sinks (e.g. Q08's). The
//! query supplies only an [`AggBinding`] that emits `(key, measures)` per
//! live row (and simply doesn't emit for inner-join misses).
//!
//! This drives several sinks over disjoint chunk slices (as the parallel
//! driver would), merges them, and checks the grouped sums against an
//! oracle. Measure values are exact integers as f64, so the merge order
//! can't change the result and the check is exact.

use std::collections::HashMap;
use std::sync::Arc;

use ematix_flow_engine::agg::{AggBinding, HashAggregateSink};
use ematix_flow_engine::chunk::DataChunk;
use ematix_flow_engine::exec::Sink;
use ematix_flow_engine::vector::Vector;

/// Group by i64 col 0, SUM f64 col 1; drop rows with a negative key (the
/// inner-join-miss analog — the binding just doesn't emit them).
struct SumByKey;
impl AggBinding<1> for SumByKey {
    fn for_each_group(&self, chunk: &DataChunk, mut emit: impl FnMut(i64, [f64; 1])) {
        let k = chunk.col(0).as_i64();
        let v = chunk.col(1).as_f64();
        chunk.sel.for_each(|i| {
            let i = i as usize;
            if k[i] >= 0 {
                emit(k[i], [v[i]]);
            }
        });
    }
}

/// Two measures: [value, value doubled] — exercises the `[f64; N]` array.
struct SumTwo;
impl AggBinding<2> for SumTwo {
    fn for_each_group(&self, chunk: &DataChunk, mut emit: impl FnMut(i64, [f64; 2])) {
        let k = chunk.col(0).as_i64();
        let v = chunk.col(1).as_f64();
        chunk.sel.for_each(|i| {
            let i = i as usize;
            emit(k[i], [v[i], 2.0 * v[i]]);
        });
    }
}

fn chunk(keys: Vec<i64>, vals: Vec<f64>) -> DataChunk {
    DataChunk::new(vec![Vector::i64(keys), Vector::f64(vals)])
}

#[test]
fn parallel_merge_and_inner_drop() {
    let b = Arc::new(SumByKey);
    // Three disjoint slices, one per simulated worker.
    let slices = [
        chunk(vec![1, 2, 1, -1], vec![10.0, 20.0, 1.0, 99.0]), // -1 dropped
        chunk(vec![2, 3], vec![2.0, 30.0]),
        chunk(vec![1, 3, 3], vec![100.0, 3.0, 3.0]),
    ];
    let mut sinks = Vec::new();
    for c in &slices {
        let mut s = HashAggregateSink::<1, SumByKey>::new(Arc::clone(&b));
        s.consume(c);
        sinks.push(s);
    }
    let merged = HashAggregateSink::merge(sinks);

    // key1: 10+1+100=111; key2: 20+2=22; key3: 30+3+3=36; -1 never emitted.
    let expect: HashMap<i64, [f64; 1]> = HashMap::from([(1, [111.0]), (2, [22.0]), (3, [36.0])]);
    assert_eq!(merged, expect);
}

#[test]
fn multi_measure_sums_each_column() {
    let b = Arc::new(SumTwo);
    let mut s = HashAggregateSink::<2, SumTwo>::new(Arc::clone(&b));
    s.consume(&chunk(vec![7, 7, 8], vec![5.0, 6.0, 9.0]));
    let merged = HashAggregateSink::merge(vec![s]);
    // key7: [5+6, 2*(5+6)] = [11,22]; key8: [9,18].
    let expect: HashMap<i64, [f64; 2]> = HashMap::from([(7, [11.0, 22.0]), (8, [9.0, 18.0])]);
    assert_eq!(merged, expect);
}

#[test]
fn empty_and_single_sink_merge() {
    let b = Arc::new(SumByKey);
    // No sinks → empty result.
    assert!(HashAggregateSink::<1, SumByKey>::merge(Vec::new()).is_empty());
    // A sink that consumed nothing → empty result.
    let s = HashAggregateSink::<1, SumByKey>::new(Arc::clone(&b));
    assert!(HashAggregateSink::merge(vec![s]).is_empty());
}
