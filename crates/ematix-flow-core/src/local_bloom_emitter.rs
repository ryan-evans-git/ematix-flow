//! Σ.Q.L4′ slice 3 — single-node bloom emitter. Walks a LogicalPlan,
//! finds Inner-equijoin candidates, pre-executes each small build
//! side, hashes the join key into a [`BloomFilter`], returns a map
//! keyed by `column_uuid(probe_table, probe_col)`.
//!
//! Wraps the same algorithm as `ematix-flow-distributed`'s
//! `emit_build_side_blooms` but without the Flight passthrough wiring,
//! so it can be used from the local triangulation bench (and any other
//! caller that wants in-scan bloom pushdown without involving the
//! distributed crate).
//!
//! The returned map plugs into [`ContextBlooms::new`], which the
//! [`crate::inbloom_scan_pushdown_rule::EnableInBloomScanPushdownRule`]
//! consumes during physical optimization.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, Int32Array, Int64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Column, DataFusionError, Result as DfResult};
use datafusion::logical_expr::{Expr, JoinType, LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::SessionContext;

use crate::bloom::{BloomFilter, column_uuid};

/// Σ.Q.L4′ — knobs for [`emit_build_side_blooms_local`].
#[derive(Clone, Debug)]
pub struct LocalBloomOptions {
    /// Discard build sides exceeding this row count. Default 50_000.
    pub max_build_rows: usize,
}

impl Default for LocalBloomOptions {
    fn default() -> Self {
        Self {
            max_build_rows: 50_000,
        }
    }
}

/// Σ.Q.L4′ — walk the plan, pre-execute small build sides, return a
/// `column_uuid(probe_table, probe_col) → BloomFilter` map. Errors on
/// any individual candidate are logged via `tracing::warn` and never
/// fatal — the emitter is opportunistic.
pub async fn emit_build_side_blooms_local(
    ctx: &SessionContext,
    plan: &LogicalPlan,
    opts: &LocalBloomOptions,
) -> DfResult<HashMap<String, Arc<BloomFilter>>> {
    let mut candidates: Vec<JoinCandidate> = Vec::new();
    collect_join_candidates(plan, &mut candidates);

    let mut out: HashMap<String, Arc<BloomFilter>> = HashMap::new();
    for cand in candidates {
        match build_bloom_for_candidate(ctx, &cand, opts).await {
            Ok(Some((uuid, bloom))) => {
                out.insert(uuid, Arc::new(bloom));
            }
            Ok(None) => {}
            Err(e) => {
                tracing::warn!(
                    "Σ.Q.L4′: skipped bloom for {}: {}",
                    cand.probe_uuid_hint,
                    e
                );
            }
        }
    }
    Ok(out)
}

#[derive(Debug, Clone)]
struct JoinCandidate {
    build_plan: LogicalPlan,
    build_col: Column,
    probe_table: String,
    probe_col: String,
    probe_uuid_hint: String,
}

fn collect_join_candidates(plan: &LogicalPlan, out: &mut Vec<JoinCandidate>) {
    if let LogicalPlan::Join(join) = plan {
        if matches!(join.join_type, JoinType::Inner) {
            for (left_expr, right_expr) in &join.on {
                if let Some(cand) = build_candidate(join, left_expr, right_expr) {
                    out.push(cand);
                }
            }
        }
    }
    for input in plan.inputs() {
        collect_join_candidates(input, out);
    }
}

fn build_candidate(
    join: &datafusion::logical_expr::Join,
    left_expr: &Expr,
    right_expr: &Expr,
) -> Option<JoinCandidate> {
    let left_col = match left_expr {
        Expr::Column(c) => c.clone(),
        _ => return None,
    };
    let right_col = match right_expr {
        Expr::Column(c) => c.clone(),
        _ => return None,
    };
    let (probe_table, probe_col_name) = find_probe_table_col(&join.left, &left_col)?;
    let probe_schema = join.left.schema();
    let probe_field = probe_schema
        .field_with_unqualified_name(&probe_col_name)
        .ok()?;
    if !matches!(probe_field.data_type(), DataType::Int64 | DataType::Int32) {
        return None;
    }
    let build_schema = join.right.schema();
    let build_field = build_schema
        .field_with_unqualified_name(&right_col.name)
        .ok()?;
    if !matches!(build_field.data_type(), DataType::Int64 | DataType::Int32) {
        return None;
    }
    let probe_uuid_hint = format!("{probe_table}.{probe_col_name}");
    Some(JoinCandidate {
        build_plan: (*join.right).clone(),
        build_col: right_col,
        probe_table,
        probe_col: probe_col_name,
        probe_uuid_hint,
    })
}

fn find_probe_table_col(plan: &LogicalPlan, target_col: &Column) -> Option<(String, String)> {
    match plan {
        LogicalPlan::TableScan(scan) => {
            if scan
                .projected_schema
                .field_with_unqualified_name(&target_col.name)
                .is_ok()
            {
                Some((scan.table_name.table().to_string(), target_col.name.clone()))
            } else {
                None
            }
        }
        LogicalPlan::SubqueryAlias(alias) => find_probe_table_col(&alias.input, target_col),
        LogicalPlan::Filter(f) => find_probe_table_col(&f.input, target_col),
        LogicalPlan::Limit(l) => find_probe_table_col(&l.input, target_col),
        LogicalPlan::Sort(s) => find_probe_table_col(&s.input, target_col),
        LogicalPlan::Distinct(d) => find_probe_table_col(d.input(), target_col),
        LogicalPlan::Projection(p) => {
            for (idx, e) in p.expr.iter().enumerate() {
                let out_name = p.schema.field(idx).name();
                if out_name != &target_col.name {
                    continue;
                }
                match e {
                    Expr::Column(c) => return find_probe_table_col(&p.input, c),
                    Expr::Alias(alias) => {
                        if let Expr::Column(c) = alias.expr.as_ref() {
                            return find_probe_table_col(&p.input, c);
                        }
                        return None;
                    }
                    _ => return None,
                }
            }
            None
        }
        _ => None,
    }
}

async fn build_bloom_for_candidate(
    ctx: &SessionContext,
    cand: &JoinCandidate,
    opts: &LocalBloomOptions,
) -> DfResult<Option<(String, BloomFilter)>> {
    let cap = opts.max_build_rows + 1;
    let key_only_plan = LogicalPlanBuilder::from(cand.build_plan.clone())
        .project(vec![Expr::Column(cand.build_col.clone())])?
        .limit(0, Some(cap))?
        .build()?;
    let df = ctx.execute_logical_plan(key_only_plan).await?;
    let batches = df.collect().await?;

    let mut n_rows = 0usize;
    let mut keys: Vec<i64> = Vec::with_capacity(opts.max_build_rows.min(8192));
    for b in &batches {
        if b.num_columns() != 1 {
            return Err(DataFusionError::Internal(
                "local_bloom_emitter: projected batch has ≠1 columns".into(),
            ));
        }
        let col = b.column(0);
        n_rows += col.len();
        if n_rows > opts.max_build_rows {
            return Ok(None);
        }
        if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
            for i in 0..a.len() {
                if !a.is_null(i) {
                    keys.push(a.value(i));
                }
            }
        } else if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
            for i in 0..a.len() {
                if !a.is_null(i) {
                    keys.push(a.value(i) as i64);
                }
            }
        } else {
            return Err(DataFusionError::Internal(format!(
                "local_bloom_emitter: unexpected build-key type {:?}",
                col.data_type()
            )));
        }
    }

    if keys.is_empty() {
        return Ok(None);
    }

    let mut bloom = BloomFilter::for_keys(keys.len().max(64));
    for k in &keys {
        bloom.insert_i64(*k);
    }
    let uuid = column_uuid(&cand.probe_table, &cand.probe_col);
    Ok(Some((uuid, bloom)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bloom::ContextBlooms;
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{Field, Schema};
    use datafusion::datasource::MemTable;

    fn register(ctx: &SessionContext, name: &str, keys: &[i64]) -> DfResult<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let rb = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(keys.to_vec()))],
        )?;
        let mt = MemTable::try_new(schema, vec![vec![rb]])?;
        ctx.register_table(name, Arc::new(mt))?;
        Ok(())
    }

    #[tokio::test]
    async fn emits_bloom_for_small_build_side() {
        let ctx = SessionContext::new();
        register(&ctx, "big", &(0..1_000i64).collect::<Vec<_>>()).unwrap();
        register(&ctx, "small", &[1, 5, 9, 13]).unwrap();
        let plan = ctx
            .sql("SELECT big.k FROM big JOIN small ON big.k = small.k")
            .await
            .unwrap()
            .into_optimized_plan()
            .unwrap();
        let map =
            emit_build_side_blooms_local(&ctx, &plan, &LocalBloomOptions::default())
                .await
                .unwrap();
        // The probe side is "big" + col "k" → uuid("big", "k").
        let uuid = column_uuid("big", "k");
        let bloom = map.get(&uuid).expect("expected a bloom for big.k");
        for k in &[1, 5, 9, 13] {
            assert!(bloom.might_contain_i64(*k), "missing key {k}");
        }
        // A key not in the small build set should miss (with the 1%
        // FPR caveat — 4 keys is well below the floor, FPs are
        // extremely unlikely).
        assert!(!bloom.might_contain_i64(999_999));
        // Sanity: the map keys plug directly into ContextBlooms.
        let _ = ContextBlooms::new(map);
    }

    #[tokio::test]
    async fn skips_large_build_side() {
        let ctx = SessionContext::new();
        register(&ctx, "big", &(0..1_000i64).collect::<Vec<_>>()).unwrap();
        register(&ctx, "huge", &(0..200_000i64).collect::<Vec<_>>()).unwrap();
        let plan = ctx
            .sql("SELECT big.k FROM big JOIN huge ON big.k = huge.k")
            .await
            .unwrap()
            .into_optimized_plan()
            .unwrap();
        let opts = LocalBloomOptions {
            max_build_rows: 1_000,
        };
        let map = emit_build_side_blooms_local(&ctx, &plan, &opts).await.unwrap();
        assert!(map.is_empty(), "huge build side should have been skipped");
    }
}
