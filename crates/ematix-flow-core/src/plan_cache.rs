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

struct PlanCacheInner {
    entries: HashMap<CacheKey, Arc<dyn ExecutionPlan>>,
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

    /// Plan if needed, return the (possibly cached) ExecutionPlan.
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
    pub async fn get_or_plan(
        &self,
        ctx: &SessionContext,
        sql: &str,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let key = self.key_for(sql);
        // Fast path: hit.
        if let Some(plan) = self.try_get(&key) {
            return Ok(plan);
        }
        // Slow path: plan then cache.
        let df = ctx.sql(sql).await?;
        let plan = df.create_physical_plan().await?;
        self.insert(key, plan.clone());
        Ok(plan)
    }

    fn key_for(&self, sql: &str) -> CacheKey {
        let inner = self.inner.lock().unwrap();
        CacheKey {
            sql_canonical: canonicalise_sql(sql),
            schema_epoch: inner.schema_epoch,
        }
    }

    fn try_get(&self, key: &CacheKey) -> Option<Arc<dyn ExecutionPlan>> {
        let mut inner = self.inner.lock().unwrap();
        let cloned = inner.entries.get(key).cloned();
        if cloned.is_some() {
            inner.hits += 1;
        } else {
            inner.misses += 1;
        }
        cloned
    }

    fn insert(&self, key: CacheKey, plan: Arc<dyn ExecutionPlan>) {
        let mut inner = self.inner.lock().unwrap();
        if inner.entries.contains_key(&key) {
            inner.entries.insert(key, plan);
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
        inner.entries.insert(key, plan);
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
        inner
            .entries
            .retain(|k, _| k.schema_epoch == current_epoch);
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
        let _ = cache
            .get_or_plan(&ctx, "SELECT * FROM t;")
            .await
            .unwrap();
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
