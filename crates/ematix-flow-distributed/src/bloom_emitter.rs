//! Σ.J.2.b.vii — automatic build-side bloom emitter.
//!
//! Walks a [`LogicalPlan`], finds Inner equijoins on Int64 columns
//! where the probe (left) side resolves to a single base [`TableScan`],
//! pre-executes the build (right) side locally on the coordinator,
//! and builds a [`BloomFilter`] per (probe_table, probe_col) pair.
//!
//! Output is a `HashMap<String, Arc<BloomFilter>>` keyed on the
//! canonical [`column_uuid`] = `<probe_table>.<probe_col>` (both
//! lowercase). The caller pipes this into
//! [`crate::bloom_flight::blooms_to_header_map`] and then
//! [`datafusion_distributed::DistributedExt::set_distributed_passthrough_headers`]
//! to ship the blooms to probe-side workers, where
//! [`ematix_flow_core::context_bloom_rule::EnableContextBloomRule`]
//! consumes them and wraps matching scans in `BloomFilterExec`.
//!
//! ## Decision: pre-execute, don't intercept
//!
//! Properly intercepting `HashJoinExec`'s build stream would need
//! either an upstream patch into datafusion-distributed's
//! `prepare_for_send` path or our own `BloomEmittingHashJoinExec`
//! replacement Exec. Both are non-trivial.
//!
//! Pre-execution is cheap on the *very thing the bloom is for* — a
//! small build side. If the build side exceeds `max_build_rows`, we
//! discard the bloom (the heuristic for "blooms help" already says
//! "small build side"). The extra cost on the hot path is bounded.
//!
//! ## What's eligible
//!
//! - `JoinType::Inner` only — other join types have different row-
//!   preserving semantics and need separate analysis
//! - Both sides of the equijoin pair are `Expr::Column`
//! - The columns are Int64 / Int32 (we promote Int32 to i64) — the
//!   bloom kernel is Int64-only today (see [`BloomFilter::insert_i64`])
//! - The probe (left) side resolves to a single base `TableScan`
//!   when walked through `Filter`, `Projection`, `SubqueryAlias`,
//!   `Limit`, `Sort`, `Distinct`. Multi-table sub-joins on the
//!   probe side are skipped (Σ.J.2.b.x territory)
//! - Build-side execution produces ≤ `max_build_rows` rows
//!
//! ## What's NOT eligible (intentional)
//!
//! - String / float / dictionary join keys — bloom kernel is i64-only
//! - Outer / Semi / Anti joins — those need null-aware membership
//! - Subqueries with row counts unknown until execution — we use
//!   `LIMIT max_build_rows + 1` as a clamp; if the clamped result
//!   has > max_build_rows rows, the bloom is discarded
//!
//! ## Caller-supplied table-name override
//!
//! In distributed mode, the same logical table may be registered
//! under different table names on different workers (e.g., a
//! coordinator alias). The bloom uuid uses the **probe-side table's
//! local name on the coordinator** by default. If workers register
//! under different aliases, the caller can override the table-name
//! resolution via [`BloomEmitterOptions::table_uuid_for`].

use datafusion::arrow::array::{Array, Int32Array, Int64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DfResult;
use datafusion::common::tree_node::TreeNode;
use datafusion::common::{Column, DataFusionError};
use datafusion::logical_expr::{Expr, JoinType, LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::SessionContext;
use datafusion_distributed::DistributedExt;
use ematix_flow_core::bloom::{BloomFilter, column_uuid};
use std::collections::HashMap;
use std::sync::Arc;

/// Σ.MG.2 hang fix: a SINGLE-NODE twin of `ctx` for bloom emission.
/// Pre-executing build sides through a distributed session routes
/// them into the stage splitter and the Flight mesh — the 2026-07-12
/// 00:28Z mesh leg deadlocked exactly there (coordinator parked on
/// futexes, workers idle). The twin shares the same table providers
/// (Arc clones out of the catalog) but carries only the single-node
/// preset rules, so emission executes locally, always.
pub async fn single_node_emission_ctx(ctx: &SessionContext) -> DfResult<SessionContext> {
    use datafusion::execution::session_state::SessionStateBuilder;
    let builder = ematix_flow_core::preset::with_optimizer_rules(
        SessionStateBuilder::new().with_default_features(),
    );
    let twin = SessionContext::new_with_state(builder.build());
    for cat_name in ctx.catalog_names() {
        let Some(cat) = ctx.catalog(&cat_name) else {
            continue;
        };
        for schema_name in cat.schema_names() {
            let Some(schema) = cat.schema(&schema_name) else {
                continue;
            };
            for table_name in schema.table_names() {
                if let Ok(Some(provider)) = schema.table(&table_name).await {
                    // Same-name collisions across catalogs are fine —
                    // last registration wins, matching lookup order.
                    let _ = twin.register_table(&table_name, provider);
                }
            }
        }
    }
    Ok(twin)
}

/// Σ.J.2.b.vii — knobs for [`emit_build_side_blooms`].
#[derive(Clone)]
pub struct BloomEmitterOptions {
    /// Discard any build side that produces more rows than this. The
    /// emitter clamps with `LIMIT max_build_rows + 1`, so the worst
    /// case is reading one extra row to discover the cap was hit.
    /// Default: 50_000 — fits in a 6 KiB header at ~12 bits/key.
    pub max_build_rows: usize,
    /// Optional callback to override the table-name component of the
    /// uuid. Called once per probe-side base table. Returning `None`
    /// uses the table's local DataFusion name unchanged. Default
    /// passes through.
    #[allow(clippy::type_complexity)]
    pub table_uuid_for: Option<Arc<dyn Fn(&str) -> Option<String> + Send + Sync>>,
}

impl std::fmt::Debug for BloomEmitterOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BloomEmitterOptions")
            .field("max_build_rows", &self.max_build_rows)
            .field("table_uuid_for", &self.table_uuid_for.is_some())
            .finish()
    }
}

impl Default for BloomEmitterOptions {
    fn default() -> Self {
        Self {
            max_build_rows: 50_000,
            table_uuid_for: None,
        }
    }
}

/// Σ.J.2.b.vii — walk the plan, pre-execute build sides, return a
/// map of `<probe_table>.<probe_col>` → `BloomFilter` ready to ship
/// via [`crate::bloom_flight::blooms_to_header_map`].
///
/// Errors short-circuit only on planner-level failures (e.g., the
/// supplied LogicalPlan is malformed). Build-side execution failures
/// on one join are logged via `tracing::warn` and skipped — never
/// fatal, the bloom is just not emitted for that pair.
pub async fn emit_build_side_blooms(
    ctx: &SessionContext,
    plan: &LogicalPlan,
    opts: &BloomEmitterOptions,
) -> DfResult<HashMap<String, Arc<BloomFilter>>> {
    let mut candidates: Vec<JoinCandidate> = Vec::new();
    collect_join_candidates(plan, &mut candidates);

    let mut out: HashMap<String, Arc<BloomFilter>> = HashMap::new();
    for cand in candidates {
        match build_bloom_for_candidate(ctx, &cand, opts).await {
            Ok(Some((uuid, bloom))) => {
                out.insert(uuid, Arc::new(bloom));
            }
            Ok(None) => {
                // Build side too big, or empty — silently skip.
            }
            Err(e) => {
                tracing::warn!(
                    "Σ.J.2.b.vii: skipped bloom for {}: {}",
                    cand.probe_uuid_hint,
                    e
                );
            }
        }
    }
    Ok(out)
}

/// Σ.J.2.b.viii — one-call coordinator-side: emit blooms for `plan`,
/// marshall to a `HeaderMap`, install via
/// `set_distributed_passthrough_headers`. The passthrough mechanism
/// flows the headers across every Flight stage; probe-side workers
/// with the [`crate::bloom_flight::BloomSessionBuilder`] installed
/// pick them up automatically.
///
/// Pass the *same* `SessionContext` you'll use for the actual query.
/// The headers are set on its internal SessionState; subsequent
/// `ctx.sql(...).collect()` calls propagate them across all Flight
/// requests for this query.
///
/// Returns the number of blooms attached (0 if nothing was eligible
/// or if every candidate's build side overshot the row cap).
pub async fn attach_blooms_for_plan(
    ctx: &mut SessionContext,
    plan: &LogicalPlan,
    opts: &BloomEmitterOptions,
) -> DfResult<usize> {
    let blooms = emit_build_side_blooms(ctx, plan, opts).await?;
    if blooms.is_empty() {
        return Ok(0);
    }
    let refs: Vec<(String, &BloomFilter)> = blooms
        .iter()
        .map(|(uuid, b)| (uuid.clone(), b.as_ref()))
        .collect();
    let map = crate::bloom_flight::blooms_to_header_map(&refs);
    let attached = map.len();
    ctx.set_distributed_passthrough_headers(map)?;
    Ok(attached)
}

#[derive(Debug, Clone)]
struct JoinCandidate {
    /// Build (right) side sub-plan that produces the join key column.
    /// We wrap this in a Projection so executing it yields a single
    /// Int64 column.
    build_plan: LogicalPlan,
    /// Build-side join key column name (used to project from build_plan).
    build_col: Column,
    /// Probe (left) side base table — table name + column name to
    /// form the uuid.
    probe_table: String,
    probe_col: String,
    /// For tracing/error messages.
    probe_uuid_hint: String,
}

fn collect_join_candidates(plan: &LogicalPlan, out: &mut Vec<JoinCandidate>) {
    // Visit this node first, then recurse into all inputs.
    if let LogicalPlan::Join(join) = plan {
        if matches!(join.join_type, JoinType::Inner) {
            for (left_expr, right_expr) in &join.on {
                // Σ.MG.2 eligibility: BOTH orientations. Each side's
                // keys can bloom the other's base scan; the build-side
                // pre-execution is LIMIT-clamped, so trying the big
                // side as a "build" costs one bounded read that
                // discards at the cap.
                if let Some(cand) = build_candidate(&join.left, &join.right, left_expr, right_expr)
                {
                    out.push(cand);
                }
                if let Some(cand) = build_candidate(&join.right, &join.left, right_expr, left_expr)
                {
                    out.push(cand);
                }
            }
        }
    }
    // Recurse — multi-level join trees produce multiple candidates.
    let _ = plan.apply(|node| {
        if !std::ptr::eq(node, plan) {
            // Avoid double-visiting `plan` itself; `apply` visits the
            // root then children. We want to start from `plan`'s
            // children for the recursion.
        }
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    });
    // Simpler explicit recursion to avoid TreeNode subtleties:
    for input in plan.inputs() {
        collect_join_candidates(input, out);
    }
}

fn build_candidate(
    probe: &LogicalPlan,
    build: &LogicalPlan,
    probe_expr: &Expr,
    build_expr: &Expr,
) -> Option<JoinCandidate> {
    let probe_col_expr = match probe_expr {
        Expr::Column(c) => c.clone(),
        _ => return None,
    };
    let build_col = match build_expr {
        Expr::Column(c) => c.clone(),
        _ => return None,
    };

    // Find a base TableScan on the probe side that has the column,
    // walking through Filter/Projection/Alias — and (Σ.MG.2) through
    // Inner joins / LeftSemi left sides, where the key column passes
    // through unchanged and pruning the base scan stays conservative
    // (those joins only ever DROP probe rows).
    let (probe_table, probe_col_name) = find_probe_table_col(probe, &probe_col_expr)?;

    // Type guard: the probe column must be Int64 or Int32 on the
    // probe-side schema (the schema the worker will see).
    let probe_schema = probe.schema();
    let probe_field = probe_schema
        .field_with_unqualified_name(&probe_col_name)
        .ok()?;
    if !matches!(probe_field.data_type(), DataType::Int64 | DataType::Int32) {
        return None;
    }
    // And on the build side too — we need to produce i64 keys to
    // insert.
    let build_schema = build.schema();
    let build_field = build_schema
        .field_with_unqualified_name(&build_col.name)
        .ok()?;
    if !matches!(build_field.data_type(), DataType::Int64 | DataType::Int32) {
        return None;
    }

    let probe_uuid_hint = format!("{probe_table}.{probe_col_name}");
    Some(JoinCandidate {
        build_plan: build.clone(),
        build_col,
        probe_table,
        probe_col: probe_col_name,
        probe_uuid_hint,
    })
}

/// Walk a probe-side LogicalPlan looking for a base [`TableScan`]
/// that produces `target_col`. Descends through row-preserving wrappers
/// (Filter, Projection, SubqueryAlias, Limit, Sort, Distinct, Aggregate
/// passing the col through). Returns `(table_name, col_name)` where
/// `col_name` may differ from `target_col.name` if there's an alias
/// rewrite — we follow the projection back to the underlying scan's
/// field.
fn find_probe_table_col(plan: &LogicalPlan, target_col: &Column) -> Option<(String, String)> {
    match plan {
        LogicalPlan::TableScan(scan) => {
            // The col we're after should exist on this scan's
            // projected schema. If the scan has a projection, the
            // field must be in it.
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
        LogicalPlan::SubqueryAlias(alias) => {
            // Alias renames the table; the col name inside is what we
            // need to find. The bloom uuid uses the underlying table
            // name (so build + probe sides agree regardless of alias).
            find_probe_table_col(&alias.input, target_col)
        }
        LogicalPlan::Filter(f) => find_probe_table_col(&f.input, target_col),
        LogicalPlan::Limit(l) => find_probe_table_col(&l.input, target_col),
        LogicalPlan::Sort(s) => find_probe_table_col(&s.input, target_col),
        LogicalPlan::Distinct(d) => find_probe_table_col(d.input(), target_col),
        LogicalPlan::Projection(p) => {
            // Find the projection expression that yields target_col.
            // If it's a direct column ref, follow it; if it's an
            // expression (e.g. col + 1), the bloom on the underlying
            // column doesn't apply.
            for (idx, e) in p.expr.iter().enumerate() {
                let out_name = p.schema.field(idx).name();
                if out_name != &target_col.name {
                    continue;
                }
                match e {
                    Expr::Column(c) => {
                        return find_probe_table_col(&p.input, c);
                    }
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
        // Σ.MG.2: descend through joins that only ever DROP rows of
        // the side we descend into — Inner (either side) and the left
        // (preserved-then-filtered) side of LeftSemi. The join key
        // column passes through unchanged, so a bloom on the base
        // scan prunes only rows that could never survive the upper
        // join: conservative. The column must resolve on exactly ONE
        // side (same-name-on-both is ambiguous → skip).
        LogicalPlan::Join(j) if matches!(j.join_type, JoinType::Inner | JoinType::LeftSemi) => {
            let sides: &[&LogicalPlan] = match j.join_type {
                JoinType::Inner => &[&j.left, &j.right],
                _ => &[&j.left],
            };
            let mut found = None;
            for side in sides {
                if let Some(hit) = find_probe_table_col(side, target_col) {
                    if found.is_some() {
                        return None; // ambiguous
                    }
                    found = Some(hit);
                }
            }
            found
        }
        // Aggregates, unions — the column may exist on the output,
        // but the scan-level mapping is ambiguous (post-aggregate the
        // key has post-GROUP BY semantics). Skip.
        _ => None,
    }
}

/// Execute the build sub-plan, project the build-side key, collect
/// distinct i64 values up to `max_build_rows + 1`, build a bloom.
/// Returns `None` if the build side was empty or exceeded the cap.
async fn build_bloom_for_candidate(
    ctx: &SessionContext,
    cand: &JoinCandidate,
    opts: &BloomEmitterOptions,
) -> DfResult<Option<(String, BloomFilter)>> {
    // Project just the join-key column from the build sub-plan.
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
                "bloom_emitter: projected batch has ≠1 columns".into(),
            ));
        }
        let col = b.column(0);
        n_rows += col.len();
        // Bail early if we already overshot.
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
                "bloom_emitter: unexpected build-key type {:?}",
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

    let table_name = opts
        .table_uuid_for
        .as_ref()
        .and_then(|f| f(&cand.probe_table))
        .unwrap_or_else(|| cand.probe_table.clone());
    let uuid = column_uuid(&table_name, &cand.probe_col);
    Ok(Some((uuid, bloom)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{Field, Schema};
    use datafusion::arrow::array::RecordBatch;
    use datafusion::datasource::MemTable;

    fn register_orders(ctx: &SessionContext, name: &str) -> DfResult<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("o_orderkey", DataType::Int64, false),
            Field::new("o_custkey", DataType::Int64, false),
        ]));
        // 1000 orders, custkey in [0, 100)
        let okeys: Vec<i64> = (0..1000).collect();
        let ckeys: Vec<i64> = (0..1000).map(|i| i % 100).collect();
        let rb = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(okeys)),
                Arc::new(Int64Array::from(ckeys)),
            ],
        )?;
        let mt = MemTable::try_new(schema, vec![vec![rb]])?;
        ctx.register_table(name, Arc::new(mt))?;
        Ok(())
    }

    fn register_customer(ctx: &SessionContext, name: &str) -> DfResult<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("c_custkey", DataType::Int64, false),
            Field::new("c_name", DataType::Utf8, false),
        ]));
        // Customers 0..50 named "A", 50..100 named "B"
        let ckeys: Vec<i64> = (0..100).collect();
        let names: Vec<&str> = ckeys
            .iter()
            .map(|i| if *i < 50 { "A" } else { "B" })
            .collect();
        let rb = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ckeys)),
                Arc::new(StringArray::from(names)),
            ],
        )?;
        let mt = MemTable::try_new(schema, vec![vec![rb]])?;
        ctx.register_table(name, Arc::new(mt))?;
        Ok(())
    }

    #[tokio::test]
    async fn small_build_side_emits_bloom() -> DfResult<()> {
        let ctx = SessionContext::new();
        register_orders(&ctx, "orders")?;
        register_customer(&ctx, "customer")?;
        let df = ctx
            .sql(
                "SELECT o_orderkey FROM orders \
                 INNER JOIN customer ON o_custkey = c_custkey \
                 WHERE c_name = 'A'",
            )
            .await?;
        let plan = df.into_optimized_plan()?;

        let blooms = emit_build_side_blooms(&ctx, &plan, &BloomEmitterOptions::default()).await?;

        // Customer is the build side (right), filtered to A → 50 rows.
        // Should bloom under `orders.o_custkey`.
        let bloom = blooms
            .get("orders.o_custkey")
            .expect("expected orders.o_custkey bloom");
        // Spot-check: keys 0..50 should be present, 50..100 should not
        // (modulo false positives).
        for k in 0..50i64 {
            assert!(
                bloom.might_contain_i64(k),
                "key {k} should be in bloom (was inserted)"
            );
        }
        // For keys 50..100 (definitely not inserted), at least most
        // should miss. With ~50 inserts in a 100-key-sized bloom the
        // FPR is well below 50%.
        let misses = (50..100i64)
            .filter(|k| !bloom.might_contain_i64(*k))
            .count();
        assert!(
            misses > 25,
            "expected most non-inserted keys to miss, got {misses}/50"
        );
        Ok(())
    }

    #[tokio::test]
    async fn build_side_over_cap_is_skipped() -> DfResult<()> {
        let ctx = SessionContext::new();
        register_orders(&ctx, "orders")?;
        register_customer(&ctx, "customer")?;
        let df = ctx
            .sql(
                "SELECT o_orderkey FROM orders \
                 INNER JOIN customer ON o_custkey = c_custkey",
            )
            .await?;
        let plan = df.into_optimized_plan()?;

        let opts = BloomEmitterOptions {
            max_build_rows: 10, // cap below customer's 100 rows
            ..Default::default()
        };
        let blooms = emit_build_side_blooms(&ctx, &plan, &opts).await?;
        // Build side exceeds cap → no bloom emitted.
        assert!(
            !blooms.contains_key("orders.o_custkey"),
            "build side > cap should skip emission"
        );
        Ok(())
    }

    #[tokio::test]
    async fn no_inner_join_means_no_blooms() -> DfResult<()> {
        let ctx = SessionContext::new();
        register_orders(&ctx, "orders")?;
        let df = ctx.sql("SELECT o_orderkey FROM orders").await?;
        let plan = df.into_optimized_plan()?;
        let blooms = emit_build_side_blooms(&ctx, &plan, &BloomEmitterOptions::default()).await?;
        assert!(blooms.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn non_int_key_is_skipped() -> DfResult<()> {
        let ctx = SessionContext::new();
        // String-keyed join: should not be emitted (bloom is i64-only).
        let schema_a = Arc::new(Schema::new(vec![
            Field::new("a_k", DataType::Utf8, false),
            Field::new("a_v", DataType::Int64, false),
        ]));
        let schema_b = Arc::new(Schema::new(vec![
            Field::new("b_k", DataType::Utf8, false),
            Field::new("b_v", DataType::Int64, false),
        ]));
        let rb_a = RecordBatch::try_new(
            schema_a.clone(),
            vec![
                Arc::new(StringArray::from(vec!["x", "y"])),
                Arc::new(Int64Array::from(vec![1, 2])),
            ],
        )?;
        let rb_b = RecordBatch::try_new(
            schema_b.clone(),
            vec![
                Arc::new(StringArray::from(vec!["x"])),
                Arc::new(Int64Array::from(vec![10])),
            ],
        )?;
        ctx.register_table(
            "a",
            Arc::new(MemTable::try_new(schema_a, vec![vec![rb_a]])?),
        )?;
        ctx.register_table(
            "b",
            Arc::new(MemTable::try_new(schema_b, vec![vec![rb_b]])?),
        )?;
        let df = ctx
            .sql("SELECT a_v FROM a INNER JOIN b ON a_k = b_k")
            .await?;
        let plan = df.into_optimized_plan()?;
        let blooms = emit_build_side_blooms(&ctx, &plan, &BloomEmitterOptions::default()).await?;
        assert!(
            blooms.is_empty(),
            "string-keyed join should not emit blooms"
        );
        Ok(())
    }

    /// Σ.MG.2 eligibility: the filtered dim on the LEFT (fact right)
    /// must still produce a bloom — both orientations are tried.
    #[tokio::test]
    async fn flipped_orientation_emits_bloom() -> DfResult<()> {
        let ctx = SessionContext::new();
        register_orders(&ctx, "orders")?;
        register_customer(&ctx, "customer")?;
        let df = ctx
            .sql(
                "SELECT o_orderkey FROM customer \
                 INNER JOIN orders ON c_custkey = o_custkey \
                 WHERE c_name = 'A'",
            )
            .await?;
        let plan = df.into_optimized_plan()?;
        let blooms = emit_build_side_blooms(&ctx, &plan, &BloomEmitterOptions::default()).await?;
        assert!(
            blooms.keys().any(|k| k.contains("orders.o_custkey")),
            "flipped join order must still bloom the fact scan; got {:?}",
            blooms.keys().collect::<Vec<_>>()
        );
        Ok(())
    }

    /// Σ.MG.2 eligibility: a MULTI-TABLE probe side (the Q07 shape —
    /// the fact scan sits under an intermediate inner join) resolves
    /// through the join to the base scan.
    #[tokio::test]
    async fn multi_table_probe_side_emits_bloom() -> DfResult<()> {
        let ctx = SessionContext::new();
        register_orders(&ctx, "orders")?;
        register_customer(&ctx, "customer")?;
        register_customer(&ctx, "supplier_like")?; // second small dim
        let df = ctx
            .sql(
                "SELECT o.o_orderkey FROM orders o \
                 INNER JOIN supplier_like s ON o.o_custkey = s.c_custkey \
                 INNER JOIN customer c ON o.o_custkey = c.c_custkey \
                 WHERE c.c_name = 'A'",
            )
            .await?;
        let plan = df.into_optimized_plan()?;
        let blooms = emit_build_side_blooms(&ctx, &plan, &BloomEmitterOptions::default()).await?;
        assert!(
            blooms.keys().any(|k| k.contains("orders.o_custkey")),
            "probe side behind an intermediate inner join must bloom \
             the base fact scan; got {:?}",
            blooms.keys().collect::<Vec<_>>()
        );
        Ok(())
    }

    #[tokio::test]
    async fn attach_blooms_for_plan_sets_headers() -> DfResult<()> {
        // Σ.J.2.b.viii — confirm the one-call coordinator API mutates
        // the SessionContext so subsequent ctx.sql(...) calls
        // propagate the bloom headers.
        let mut ctx = SessionContext::new();
        // Register tables + create the plan ON THE SAME ctx so the
        // emitter can execute the build side against them.
        register_orders(&ctx, "orders")?;
        register_customer(&ctx, "customer")?;
        let df = ctx
            .sql(
                "SELECT o_orderkey FROM orders \
                 INNER JOIN customer ON o_custkey = c_custkey \
                 WHERE c_name = 'A'",
            )
            .await?;
        let plan = df.into_optimized_plan()?;
        let n = attach_blooms_for_plan(&mut ctx, &plan, &BloomEmitterOptions::default()).await?;
        assert!(n >= 1, "expected at least one bloom attached, got {n}");
        Ok(())
    }

    #[tokio::test]
    async fn end_to_end_emit_then_consume() -> DfResult<()> {
        // Round-trip the bloom through the Σ.J.2.b.v transport: emit,
        // marshall to headers, decode back, look up by uuid. Proves
        // the build- and probe-side halves agree on the uuid scheme.
        use crate::bloom_flight::{blooms_to_header_map, context_blooms_from_headers};

        let ctx = SessionContext::new();
        register_orders(&ctx, "orders")?;
        register_customer(&ctx, "customer")?;
        let df = ctx
            .sql(
                "SELECT o_orderkey FROM orders \
                 INNER JOIN customer ON o_custkey = c_custkey \
                 WHERE c_name = 'A'",
            )
            .await?;
        let plan = df.into_optimized_plan()?;

        // 1. Build side: emit blooms.
        let blooms = emit_build_side_blooms(&ctx, &plan, &BloomEmitterOptions::default()).await?;
        assert!(!blooms.is_empty(), "expected at least one bloom");

        // 2. Build side: marshall to HeaderMap.
        let arc_refs: Vec<(String, &BloomFilter)> = blooms
            .iter()
            .map(|(uuid, b)| (uuid.clone(), b.as_ref()))
            .collect();
        let header_map = blooms_to_header_map(&arc_refs);
        assert!(header_map.contains_key("x-ematix-bloom-orders.o_custkey"));

        // 3. Probe side: decode headers → ContextBlooms.
        let ctx_blooms = context_blooms_from_headers(&header_map);
        assert_eq!(ctx_blooms.len(), blooms.len());
        let probe_bloom = ctx_blooms.get("orders.o_custkey").unwrap();
        // Verify some build-side keys made it through.
        for k in 0..50i64 {
            assert!(probe_bloom.might_contain_i64(k));
        }
        Ok(())
    }
}
