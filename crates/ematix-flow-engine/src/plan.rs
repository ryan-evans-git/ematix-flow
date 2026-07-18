//! P3 physical planner + executor: walk a bound [`LogicalPlan`] into an
//! engine pipeline and run it — the last leg of `SQL → AST → bind → plan →
//! execute`. With this, a query runs on the engine with **zero
//! hand-assembly**: `Scan` decodes via the native ematix-parquet path by
//! leaf index, `Filter` narrows the deferred selection with the general
//! expression evaluator ([`crate::expr::filter_expr`] — no materialization),
//! and a scalar `Aggregate` sums with [`crate::expr::sum_expr_f64`].
//!
//! Sequential and interpreted, on purpose: this slice's job is *the planned
//! path computing exactly what the hand-built path computes* (the Q6 gate
//! asserts bit-equality, so partials accumulate per chunk in the same
//! association as the hand kernels). Parallelizing planned queries through
//! the morsel driver ([`crate::exec::run_scan_pipeline`]) and grouped
//! aggregation ([`crate::agg::HashAggregateSink`]) are the next slices —
//! the operators already exist; the planner grows into them.

use std::collections::BTreeMap;

use crate::chunk::DataChunk;
use crate::expr::{ScalarValue, filter_expr, sum_expr_f64};
use crate::logical::{AggFunc, LogicalPlan};
use crate::scan_native::{NativeColKind, scan_row_groups};
use crate::vector::LogicalType;

/// A query result: named columns, row-major values. Tiny by construction
/// today (aggregate outputs); a chunked/columnar result surface arrives with
/// non-aggregate queries.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<ScalarValue>>,
}

/// Execute a bound plan on the engine.
pub fn execute(plan: &LogicalPlan) -> Result<QueryResult, String> {
    match plan {
        LogicalPlan::Aggregate { input, group, aggs } => {
            let (chunks, sels) = run_input(input)?;

            let agg_names = |offset: usize| {
                aggs.iter().enumerate().map(move |(j, a)| {
                    a.alias
                        .clone()
                        .unwrap_or_else(|| format!("col{}", offset + j))
                })
            };

            if group.is_empty() {
                // Scalar aggregate: one accumulator per agg, per-chunk
                // partials added in chunk order (the hand-kernel
                // association — what makes the Q6 gate bit-exact).
                let mut acc = vec![0.0_f64; aggs.len()];
                for (chunk, sel) in chunks.iter().zip(&sels) {
                    for (j, agg) in aggs.iter().enumerate() {
                        match agg.func {
                            AggFunc::Sum => acc[j] += sum_expr_f64(chunk, sel, &agg.arg),
                        }
                    }
                }
                return Ok(QueryResult {
                    columns: agg_names(0).collect(),
                    rows: vec![acc.into_iter().map(ScalarValue::Float64).collect()],
                });
            }

            // Grouped aggregate: hash-accumulate per key tuple. A BTreeMap
            // keys the groups AND yields deterministic key-sorted output.
            let mut map: BTreeMap<Vec<i64>, Vec<f64>> = BTreeMap::new();
            for (chunk, sel) in chunks.iter().zip(&sels) {
                sel.for_each(|i| {
                    let i = i as usize;
                    let key: Vec<i64> = group.iter().map(|g| g.expr.eval_i64(chunk, i)).collect();
                    let acc = map.entry(key).or_insert_with(|| vec![0.0; aggs.len()]);
                    for (j, agg) in aggs.iter().enumerate() {
                        match agg.func {
                            AggFunc::Sum => acc[j] += agg.arg.eval_f64(chunk, i),
                        }
                    }
                });
            }

            let columns = group
                .iter()
                .map(|g| g.name.clone())
                .chain(agg_names(group.len()))
                .collect();
            let rows = map
                .into_iter()
                .map(|(key, acc)| {
                    key.into_iter()
                        .map(ScalarValue::Int64)
                        .chain(acc.into_iter().map(ScalarValue::Float64))
                        .collect()
                })
                .collect();
            Ok(QueryResult { columns, rows })
        }
        LogicalPlan::Filter { .. } | LogicalPlan::Scan { .. } => {
            Err("top-level non-aggregate queries are not yet supported (P3)".into())
        }
    }
}

/// Run the input side of a breaker: decode the scan and apply any filters,
/// yielding each chunk with its live selection (columns never compacted).
fn run_input(plan: &LogicalPlan) -> Result<(Vec<DataChunk>, Vec<crate::chunk::Selection>), String> {
    match plan {
        LogicalPlan::Scan {
            path, projection, ..
        } => {
            let columns: Vec<(usize, NativeColKind)> = projection
                .iter()
                .map(|c| Ok((c.leaf, native_kind(c.ty)?)))
                .collect::<Result<_, String>>()?;
            let chunks = scan_row_groups(path, &columns)?;
            let sels = chunks.iter().map(|c| c.sel.clone()).collect();
            Ok((chunks, sels))
        }
        LogicalPlan::Filter { input, predicate } => {
            let (chunks, sels) = run_input(input)?;
            let sels = chunks
                .iter()
                .zip(sels)
                .map(|(chunk, sel)| {
                    // Narrow the incoming selection (chunks arrive all-live
                    // from the scan; stacked filters compose).
                    let scoped = DataChunk {
                        cols: chunk.cols.clone(),
                        sel,
                    };
                    filter_expr(&scoped, predicate)
                })
                .collect();
            Ok((chunks, sels))
        }
        LogicalPlan::Aggregate { .. } => Err("nested aggregates are not supported".into()),
    }
}

/// Map a catalog logical type to its native-scan decode kind.
fn native_kind(ty: LogicalType) -> Result<NativeColKind, String> {
    Ok(match ty {
        LogicalType::Int32 | LogicalType::Date32 => NativeColKind::I32(ty),
        LogicalType::Int64 => NativeColKind::I64,
        LogicalType::Float64 => NativeColKind::F64,
        LogicalType::Utf8 => {
            return Err("Utf8 columns are not yet supported in the native scan".into());
        }
    })
}
