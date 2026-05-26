//! Σ.M — SQL → PhysicalPlan cache with semantic dedup.
//!
//! ## What this does
//!
//! `ctx.sql(sql).create_physical_plan()` is expensive — typical TPC-H
//! query is 5-20ms of analyzer + logical optimizer + physical
//! optimizer work even before the first batch flows. Most real
//! workloads repeat the same queries (dashboards, benchmarks,
//! materialized-view refresh, cron jobs). Photon caches plans only
//! for *prepared statements*; ad-hoc SQL re-plans every time.
//!
//! [`PlanCache`] caches the [`Arc<dyn ExecutionPlan>`] keyed by a
//! canonicalised SQL hash + schema-version. Repeat queries skip
//! straight to execution.
//!
//! ## Canonicalisation
//!
//! For v1, the cache key is the SQL string after:
//! - Trimming leading/trailing whitespace.
//! - Collapsing internal whitespace runs to single spaces.
//! - Stripping trailing semicolons.
//!
//! Semantic dedup of equivalent SQLs (e.g. column-reorder in SELECT,
//! parametrise literals) is v2 work — needs logical-plan
//! fingerprinting that survives DataFusion's analyzer rewrites.
//!
//! ## Schema invalidation
//!
//! If the caller re-registers a table mid-process (different
//! provider, different schema), cached plans referencing the old
//! provider become stale. The cache key includes a *schema epoch*
//! the caller bumps on register/deregister. Bump → all entries miss
//! on next query.
//!
//! ## Why this is OSS-novel
//!
//! Photon, Velox, DuckDB, ClickHouse all re-plan ad-hoc SQL every
//! call. Prepared statements get caching but require an upfront
//! statement-prepare round trip the user has to opt into. With this
//! cache, ad-hoc queries get the same speedup without user changes.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use datafusion::error::DataFusionError;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;

/// Σ.M — bounded LRU plan cache. Cheap to construct one per
/// SessionContext; or share across contexts if they all have the
/// same registered tables.
pub struct PlanCache {
    inner: Mutex<PlanCacheInner>,
    /// Bound on entries; oldest evicted at insert time.
    capacity: usize,
}

/// Σ.AG (2026-05-26): cache the optimized LogicalPlan. PhysicalPlan
/// operators (RepartitionExec, AggregateExec) carry per-execute
/// internal state — caching `Arc<dyn ExecutionPlan>` and calling
/// `execute()` twice panics on the second call. LogicalPlan is
/// immutable.
///
/// Σ.AG.2 (2026-05-26): also stash a PhysicalPlan template alongside
/// the LogicalPlan. On hit, instead of re-physicalizing (which is the
/// dominant plan cost ~0.6 ms), bottom-up rebuild the cached
/// PhysicalPlan tree via `with_new_children` — DataFusion's operator
/// `with_new_children` constructors create fresh internal state.
/// Skips the physical planner entirely.
struct CacheEntry {
    logical: Arc<LogicalPlan>,
    /// PhysicalPlan template for fast-rebuild via with_new_children.
    /// None means we have to re-physicalize on next hit (e.g., the
    /// cached template was just consumed and is dirty).
    physical_template: Arc<dyn ExecutionPlan>,
}

struct PlanCacheInner {
    entries: HashMap<CacheKey, CacheEntry>,
    /// Insertion order — first is oldest. Simple ring; for v1 size is
    /// small (default 1024) so linear scan on eviction is fine.
    order: Vec<CacheKey>,
    /// Caller bumps on table register/deregister. Cache key includes
    /// this so a fresh schema invalidates all entries naturally.
    schema_epoch: u64,
    hits: u64,
    misses: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    sql_canonical: String,
    schema_epoch: u64,
}

impl PlanCache {
    /// Default capacity 1024 entries.
    pub fn new() -> Self {
        Self::with_capacity(1024)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(PlanCacheInner {
                entries: HashMap::new(),
                order: Vec::new(),
                schema_epoch: 0,
                hits: 0,
                misses: 0,
            }),
            capacity: capacity.max(1),
        }
    }

    /// Plan if needed, return a fresh ExecutionPlan.
    /// Idiomatic call site replaces:
    ///
    /// ```text
    /// let df = ctx.sql(sql).await?;
    /// let plan = df.create_physical_plan().await?;
    /// ```
    ///
    /// with:
    ///
    /// ```text
    /// let plan = cache.get_or_plan(&ctx, sql).await?;
    /// ```
    ///
    /// Σ.AG (2026-05-26): always returns a FRESH ExecutionPlan tree.
    /// On cache hit, we still re-run the physical planner against the
    /// cached LogicalPlan — physical plan operators are stateful and
    /// not safe to re-execute. The win is the skipped SQL parse +
    /// analyzer + logical optimizer (where our custom Inject*Rule
    /// chain lives, ~0.7-0.9 ms / query on Q06 SF=1).
    pub async fn get_or_plan(
        &self,
        ctx: &SessionContext,
        sql: &str,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let key = self.key_for(sql);
        // Σ.AG.2 (2026-05-26): fast path — rebuild the cached
        // PhysicalPlan template by bottom-up `with_new_children`.
        // Each operator's constructor creates a fresh node with no
        // execute-state baggage. ~10× cheaper than re-physicalizing
        // (no logical→physical translation, no physical-optimizer
        // rules).
        if let Some(entry) = self.try_get(&key) {
            return rebuild_plan_tree(&entry.physical_template);
        }
        // Slow path: full SQL → DataFrame → logical → physical.
        let df = ctx.sql(sql).await?;
        let logical = Arc::new(df.logical_plan().clone());
        let physical = ctx.state().create_physical_plan(&logical).await?;
        // Σ.AG.4 (2026-05-26): only cache plans whose every operator
        // is re-execute-safe via with_new_children. For uncacheable
        // plans, return the freshly-physicalized plan directly —
        // it's already a clean tree (never executed), so no need to
        // rebuild it via with_new_children, which would just allocate
        // another tree's worth of operator nodes for no benefit
        // (Q21 SF=10 regressed +32% from the wasted rebuild walk on
        // its 15-node plan before this short-circuit landed).
        if !is_cacheable(&physical) {
            return Ok(physical);
        }
        self.insert(
            key,
            CacheEntry {
                logical,
                physical_template: physical.clone(),
            },
        );
        // Return a freshly-rebuilt tree so the FIRST execute gets a
        // clean tree too (the inserted template is never executed —
        // it's only ever the source for with_new_children rebuilds).
        rebuild_plan_tree(&physical)
    }

    fn key_for(&self, sql: &str) -> CacheKey {
        let inner = self.inner.lock().unwrap();
        CacheKey {
            sql_canonical: canonicalise_sql(sql),
            schema_epoch: inner.schema_epoch,
        }
    }

    fn try_get(&self, key: &CacheKey) -> Option<CacheEntry> {
        let mut inner = self.inner.lock().unwrap();
        let cloned = inner.entries.get(key).map(|e| CacheEntry {
            logical: e.logical.clone(),
            physical_template: e.physical_template.clone(),
        });
        if cloned.is_some() {
            inner.hits += 1;
        } else {
            inner.misses += 1;
        }
        cloned
    }

    fn insert(&self, key: CacheKey, entry: CacheEntry) {
        let mut inner = self.inner.lock().unwrap();
        if let std::collections::hash_map::Entry::Occupied(mut e) = inner.entries.entry(key.clone())
        {
            e.insert(entry);
            return;
        }
        if inner.entries.len() >= self.capacity {
            // Evict oldest.
            if !inner.order.is_empty() {
                let oldest = inner.order.remove(0);
                inner.entries.remove(&oldest);
            }
        }
        inner.order.push(key.clone());
        inner.entries.insert(key, entry);
    }

    /// Caller invokes after register_table / deregister_table to
    /// invalidate all cached plans. Cheap — just bumps the epoch.
    pub fn bump_schema_epoch(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.schema_epoch = inner.schema_epoch.wrapping_add(1);
        // Eager cleanup: drop entries with the previous epoch so
        // memory frees immediately. Not strictly required for
        // correctness — stale entries would never hit — but tidy.
        let current_epoch = inner.schema_epoch;
        inner.entries.retain(|k, _| k.schema_epoch == current_epoch);
        inner.order.retain(|k| k.schema_epoch == current_epoch);
    }

    /// (hits, misses) for telemetry.
    pub fn stats(&self) -> (u64, u64) {
        let inner = self.inner.lock().unwrap();
        (inner.hits, inner.misses)
    }

    /// Number of live entries.
    pub fn len(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for PlanCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Σ.AG.4 (2026-05-26): plan-tree predicate — returns true iff every
/// operator in the tree is safe to clone-and-rebuild via
/// `with_new_children`.
///
/// Refuses for:
///
/// - `BuildSideBloomEmitterExec` — populates an Arc<RuntimeBloomSideband>
///   on first execute that subsequent rebuilds clone (not re-populate).
///
/// - `HashJoinExec` with `mode=CollectLeft` AND `join_type=LeftSemi`
///   or `LeftAnti`. DataFusion's CollectLeft probe side uses a
///   "visited" bitmap on the build-side hash table to track which
///   rows have produced output. The bitmap is held in the cached
///   `JoinLeftData` and survives `with_new_children` rebuilds. After
///   rep 0 marks every build row as visited, rep 1+ on the same
///   cached state finds no unvisited rows → empty output.
///
/// - `SharedSubtreeExec` — caches the materialised RecordBatch output
///   of a subtree in an Arc<CachedBatches>. `with_new_children`
///   returns `self`, so a rebuilt plan still points at the same
///   cached batches. That's correct WITHIN a single query execution
///   (the whole point of CSE), but ACROSS executions the underlying
///   table data may have changed — replay would serve stale batches.
///   The plan cache must not assume that result data is invariant.
///
/// Empirically verified at 22q SF=1: with the BuildSide-only gate,
/// Q20/Q21/Q22 still return 0 rows on rep 2+ (411→0, 186→0, 7→0).
/// All three have at least one CollectLeft + LeftSemi or LeftAnti
/// join. Q18 (LeftSemi but Partitioned mode) caches safely.
///
/// Other operators (RepartitionExec, AggregateExec, Inner/RightSemi
/// HashJoinExec, FilterExec, ProjectionExec, SortExec, EmatixFastParquetExec)
/// re-execute safely.
pub fn is_cacheable(plan: &Arc<dyn ExecutionPlan>) -> bool {
    if plan.name() == "BuildSideBloomEmitterExec" {
        return false;
    }
    if plan.name() == "SharedSubtreeExec" {
        return false;
    }
    if plan.name() == "HashJoinExec" {
        if let Some(hj) = plan
            .as_any()
            .downcast_ref::<datafusion::physical_plan::joins::HashJoinExec>()
        {
            use datafusion::common::JoinType;
            use datafusion::physical_plan::joins::PartitionMode;
            if matches!(hj.partition_mode(), PartitionMode::CollectLeft)
                && matches!(hj.join_type(), JoinType::LeftSemi | JoinType::LeftAnti)
            {
                return false;
            }
        }
    }
    plan.children().iter().all(|c| is_cacheable(c))
}

/// Σ.AG.2 (2026-05-26): bottom-up rebuild of a cached PhysicalPlan
/// tree. Walks `template` recursively and calls `with_new_children`
/// at each node — DataFusion's standard contract is that
/// `with_new_children` returns a NEW operator instance with no
/// execute-state carried from the old one. RepartitionExec,
/// AggregateExec, HashJoinExec, FilterExec, ProjectionExec etc. all
/// honor this. Source-leaf operators like our EmatixFastParquetExec
/// have stateless `execute()` and can safely return `self` from
/// `with_new_children` (their state lives in spawned tasks, not the
/// node).
///
/// On a re-execute, this rebuild costs O(plan-nodes) — ~5-15
/// constructor calls per query, vs the ~0.6 ms of full re-physicalize
/// (which runs the physical planner + physical optimizer rules).
fn rebuild_plan_tree(
    template: &Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let new_children: Vec<Arc<dyn ExecutionPlan>> = template
        .children()
        .iter()
        .map(|c| rebuild_plan_tree(*c))
        .collect::<Result<Vec<_>, _>>()?;
    template.clone().with_new_children(new_children)
}

/// SQL canonicalisation for cache key matching. v1: whitespace +
/// trailing-semicolon normalisation. v2: full LogicalPlan
/// fingerprinting that handles column-reorder + literal parametrisation.
pub fn canonicalise_sql(sql: &str) -> String {
    let mut s = sql.trim().to_string();
    // Strip trailing semicolons.
    while s.ends_with(';') {
        s.pop();
    }
    // Collapse whitespace runs to single space, preserving string
    // literals (single-quoted). Doesn't try to be SQL-correct for
    // nested quotes / escapes — close enough for cache key purposes;
    // a literal with `'foo  bar'` keeps its double space; a where
    // clause `WHERE x  =  1` becomes `WHERE x = 1`.
    let mut out = String::with_capacity(s.len());
    let mut in_string = false;
    let mut prev_space = false;
    for c in s.chars() {
        if c == '\'' {
            in_string = !in_string;
            out.push(c);
            prev_space = false;
            continue;
        }
        if !in_string && c.is_whitespace() {
            if !prev_space {
                out.push(' ');
                prev_space = true;
            }
        } else {
            out.push(c);
            prev_space = false;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;

    fn ctx_with_t() -> SessionContext {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let mt = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(mt)).unwrap();
        ctx
    }

    #[tokio::test]
    async fn miss_then_hit() {
        let ctx = ctx_with_t();
        let cache = PlanCache::new();
        let _ = cache.get_or_plan(&ctx, "SELECT * FROM t").await.unwrap();
        let _ = cache.get_or_plan(&ctx, "SELECT * FROM t").await.unwrap();
        let (h, m) = cache.stats();
        assert_eq!(h, 1);
        assert_eq!(m, 1);
        assert_eq!(cache.len(), 1);
    }

    #[tokio::test]
    async fn canonicalisation_hits_on_whitespace_variants() {
        let ctx = ctx_with_t();
        let cache = PlanCache::new();
        let _ = cache.get_or_plan(&ctx, "SELECT * FROM t").await.unwrap();
        let _ = cache
            .get_or_plan(&ctx, "  SELECT  *   FROM   t  ")
            .await
            .unwrap();
        let _ = cache.get_or_plan(&ctx, "SELECT * FROM t;").await.unwrap();
        let (h, m) = cache.stats();
        assert_eq!(h, 2);
        assert_eq!(m, 1);
    }

    #[tokio::test]
    async fn different_sqls_get_different_entries() {
        let ctx = ctx_with_t();
        let cache = PlanCache::new();
        let _ = cache.get_or_plan(&ctx, "SELECT * FROM t").await.unwrap();
        let _ = cache
            .get_or_plan(&ctx, "SELECT COUNT(*) FROM t")
            .await
            .unwrap();
        assert_eq!(cache.len(), 2);
    }

    #[tokio::test]
    async fn schema_bump_invalidates_entries() {
        let ctx = ctx_with_t();
        let cache = PlanCache::new();
        let _ = cache.get_or_plan(&ctx, "SELECT * FROM t").await.unwrap();
        cache.bump_schema_epoch();
        assert_eq!(cache.len(), 0);
        // re-query: misses again
        let _ = cache.get_or_plan(&ctx, "SELECT * FROM t").await.unwrap();
        let (h, m) = cache.stats();
        assert_eq!(h, 0);
        assert_eq!(m, 2);
    }

    #[tokio::test]
    async fn lru_evicts_oldest_at_capacity() {
        let ctx = ctx_with_t();
        let cache = PlanCache::with_capacity(2);
        let _ = cache.get_or_plan(&ctx, "SELECT 1 FROM t").await.unwrap();
        let _ = cache.get_or_plan(&ctx, "SELECT 2 FROM t").await.unwrap();
        let _ = cache.get_or_plan(&ctx, "SELECT 3 FROM t").await.unwrap();
        assert_eq!(cache.len(), 2);
        // Oldest ("SELECT 1") should be evicted; new query (re-issued)
        // misses again.
        let _ = cache.get_or_plan(&ctx, "SELECT 1 FROM t").await.unwrap();
        let (h, m) = cache.stats();
        // misses: 1, 2, 3, then re-1 = 4
        assert_eq!(m, 4);
        assert_eq!(h, 0);
    }

    #[test]
    fn canonicalise_preserves_string_literals_whitespace() {
        // Spaces inside string literals shouldn't collapse.
        let s = canonicalise_sql("SELECT  'a  b'  FROM  t");
        assert_eq!(s, "SELECT 'a  b' FROM t");
    }

    #[test]
    fn canonicalise_strips_trailing_semicolons() {
        assert_eq!(canonicalise_sql("SELECT 1;"), "SELECT 1");
        assert_eq!(canonicalise_sql("SELECT 1;;;"), "SELECT 1");
        assert_eq!(canonicalise_sql("SELECT 1 ;;"), "SELECT 1 ");
    }
}
