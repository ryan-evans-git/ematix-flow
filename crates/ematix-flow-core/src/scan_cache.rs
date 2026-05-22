//! Σ.L.4 — cross-query scan-cache framing.
//!
//! ## Vision
//!
//! Two concurrent queries hit `lineitem.parquet` with overlapping
//! filters (e.g. one runs Q01 `l_shipdate <= '1998-09-02'`, the
//! other Q06 `l_shipdate >= '1994-01-01' AND ...`). Both scan the
//! same row groups and apply mostly the same column projections. A
//! traditional engine decodes both independently. We can share the
//! decoded `RecordBatch`es when the cache key matches.
//!
//! Materialize does this for views (`Differential Dataflow`). DuckDB
//! does NOT (single-process design). Photon doesn't either (per-query
//! plan boundaries). **No ad-hoc-SQL engine ships cross-query work
//! sharing in OSS.** Hence Σ.L.4.
//!
//! ## Status — scaffolding only
//!
//! Tonight: the `ScanCacheKey` + cache struct + registration API.
//! Real integration with `EmatArrowBatchReader` / `EmatixFastParquetExec`
//! requires:
//!
//! 1. Plumbing the cache through DataFusion's `ExecutionPlan::execute`.
//! 2. Lifecycle management (last-reader-drops-it semantics).
//! 3. Filter-fingerprint computation that's stable across plan
//!    rewrites but specific enough to avoid false sharing.
//! 4. Cancellation handling when one query of the shared pair errors.
//!
//! All four are non-trivial; this is multi-day work. The framing here
//! is enough that Σ.L.4 follow-up bites can plug into it.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, Weak};
use std::time::Instant;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

/// Σ.L.4 cache key. Equal keys → shareable scan output.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ScanCacheKey {
    pub file_path: String,
    pub row_group_idx: u32,
    /// Sorted projection column names — order-independent equality.
    pub projection: Vec<String>,
    /// Stable fingerprint of pushdown filters. Computed by the caller
    /// — typically a hash of canonicalised filter expressions.
    pub filter_fingerprint: u64,
}

/// Σ.L.4 — shared decoded scan output. The producer (first query) fills
/// it; subscribers (concurrent queries with the same key) clone Arc'd
/// batches out. Drop-on-last-ref.
pub struct CachedScan {
    pub schema: SchemaRef,
    /// Filled in once the producer finishes the row group. Subscribers
    /// block on this until ready (currently via `Mutex` poll loop;
    /// can switch to a `Notify` once the executor wires real async).
    pub batches: Mutex<Option<Vec<Arc<RecordBatch>>>>,
    pub created_at: Instant,
}

impl CachedScan {
    pub fn empty(schema: SchemaRef) -> Self {
        Self {
            schema,
            batches: Mutex::new(None),
            created_at: Instant::now(),
        }
    }

    /// Producer side — called when decode finishes.
    pub fn publish(&self, batches: Vec<Arc<RecordBatch>>) {
        *self.batches.lock().unwrap() = Some(batches);
    }

    /// Subscriber side — pulls the published batches (None if producer
    /// hasn't finished yet).
    pub fn try_take(&self) -> Option<Vec<Arc<RecordBatch>>> {
        self.batches.lock().unwrap().clone()
    }
}

/// Σ.L.4 — process-local scan cache. Concurrent queries register their
/// scan intent; the first one becomes the producer, the rest become
/// subscribers waiting for the producer's output.
#[derive(Default)]
pub struct ScanCache {
    entries: Mutex<HashMap<ScanCacheKey, Weak<CachedScan>>>,
}

impl ScanCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Lookup-or-create. Returns:
    /// - `(handle, true)` — caller is the producer, must call
    ///   `handle.publish(...)` when decode finishes.
    /// - `(handle, false)` — caller is a subscriber, should poll
    ///   `handle.try_take()` until it returns Some.
    pub fn lookup_or_create(
        &self,
        key: ScanCacheKey,
        schema: SchemaRef,
    ) -> (Arc<CachedScan>, bool) {
        let mut e = self.entries.lock().unwrap();
        if let Some(weak) = e.get(&key) {
            if let Some(strong) = weak.upgrade() {
                return (strong, false);
            }
        }
        let scan = Arc::new(CachedScan::empty(schema));
        e.insert(key, Arc::downgrade(&scan));
        (scan, true)
    }

    /// Drop stale weak refs whose strong count is zero. Call
    /// occasionally — the cache is bounded by concurrent in-flight
    /// queries but a long-running process should periodically reap.
    pub fn reap(&self) {
        let mut e = self.entries.lock().unwrap();
        e.retain(|_, weak| weak.strong_count() > 0);
    }

    /// Number of live entries (for telemetry).
    pub fn live_entries(&self) -> usize {
        let e = self.entries.lock().unwrap();
        e.values().filter(|w| w.strong_count() > 0).count()
    }
}

/// Compute a 64-bit fingerprint of a list of filter expression strings.
/// Stable for the same canonicalised inputs. Caller is responsible for
/// canonicalising (e.g. sort literals, normalise whitespace).
pub fn filter_fingerprint(canonicalised_exprs: &[&str]) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    let mut sorted: Vec<&&str> = canonicalised_exprs.iter().collect();
    sorted.sort();
    for s in sorted {
        s.hash(&mut h);
    }
    h.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]))
    }

    fn key(rg: u32) -> ScanCacheKey {
        ScanCacheKey {
            file_path: "data/lineitem.parquet".to_string(),
            row_group_idx: rg,
            projection: vec!["x".to_string()],
            filter_fingerprint: 0,
        }
    }

    #[test]
    fn first_caller_is_producer_second_is_subscriber() {
        let cache = ScanCache::new();
        let (h1, is_producer1) = cache.lookup_or_create(key(0), schema());
        assert!(is_producer1);
        let (h2, is_producer2) = cache.lookup_or_create(key(0), schema());
        assert!(!is_producer2);
        // Both handles point at the same scan.
        assert!(Arc::ptr_eq(&h1, &h2));
    }

    #[test]
    fn subscriber_sees_published_batches() {
        let cache = ScanCache::new();
        let (h_prod, _) = cache.lookup_or_create(key(0), schema());
        let (h_sub, _) = cache.lookup_or_create(key(0), schema());
        // Initially nothing.
        assert!(h_sub.try_take().is_none());
        let batch = Arc::new(
            RecordBatch::try_new(
                schema(),
                vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
            )
            .unwrap(),
        );
        h_prod.publish(vec![batch.clone()]);
        let pulled = h_sub.try_take().unwrap();
        assert_eq!(pulled.len(), 1);
        assert_eq!(pulled[0].num_rows(), 3);
    }

    #[test]
    fn different_keys_produce_different_handles() {
        let cache = ScanCache::new();
        let (h1, _) = cache.lookup_or_create(key(0), schema());
        let (h2, _) = cache.lookup_or_create(key(1), schema());
        assert!(!Arc::ptr_eq(&h1, &h2));
    }

    #[test]
    fn dropped_handles_freed_by_reap() {
        let cache = ScanCache::new();
        {
            let (_h, _) = cache.lookup_or_create(key(42), schema());
        }
        // Strong ref dropped; weak still in map until reap.
        cache.reap();
        assert_eq!(cache.live_entries(), 0);
    }

    #[test]
    fn filter_fingerprint_stable_under_reorder() {
        let a = filter_fingerprint(&["a = 1", "b > 2"]);
        let b = filter_fingerprint(&["b > 2", "a = 1"]);
        assert_eq!(a, b);
        let c = filter_fingerprint(&["a = 2", "b > 2"]);
        assert_ne!(a, c);
    }
}
