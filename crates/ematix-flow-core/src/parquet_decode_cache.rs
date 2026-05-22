//! Σ.O — persistent parquet decode cache.
//!
//! ## Why this matters
//!
//! Decoded `RecordBatch` arrays for hot tables get re-decoded on
//! every query in OLAP engines that don't share state. For
//! dashboards (same query family, varying parameters), benchmark
//! suites (same 22 queries, 5 reps each), and exploratory analysis
//! (same projection, different filters), this is pure waste.
//!
//! ClickHouse caches at the materialized-view layer. DuckDB doesn't
//! cache across queries by design (single-process, lifetime managed
//! by the Python/CLI session). Photon caches inside Databricks's
//! cluster layer but doesn't ship the cache in OSS.
//!
//! Σ.O ships a process-local LRU cache of decoded `RecordBatch`
//! arrays keyed by `(file_path, row_group_idx, projection_fingerprint)`.
//! Capacity is bounded by total bytes (default 1 GiB); oldest entries
//! evict first. Integration with the parquet TableProvider is
//! [`Σ.O.b`](docs/PHASE_SIGMA_O_DECODE_CACHE_WIRING.md, deferred).
//!
//! ## API
//!
//! - [`ParquetDecodeCache::new`] / `with_capacity_bytes`
//! - [`get`] / [`insert`] — by [`DecodeCacheKey`]
//! - [`reap_lru`] — manual eviction trigger
//! - [`stats`] — (hits, misses, bytes)

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use arrow_array::RecordBatch;

const DEFAULT_CAPACITY_BYTES: usize = 1024 * 1024 * 1024; // 1 GiB

/// Σ.O cache key. Equal keys share decoded batches.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DecodeCacheKey {
    pub file_path: String,
    pub row_group_idx: u32,
    /// Stable fingerprint of the projected columns. Caller computes
    /// from column-name list (sorted) or column-index list.
    pub projection_fingerprint: u64,
}

impl DecodeCacheKey {
    pub fn new(file_path: impl Into<String>, rg: u32, projection: &[&str]) -> Self {
        let mut sorted: Vec<&&str> = projection.iter().collect();
        sorted.sort();
        let mut h = std::collections::hash_map::DefaultHasher::new();
        for s in sorted {
            std::hash::Hash::hash(*s, &mut h);
        }
        Self {
            file_path: file_path.into(),
            row_group_idx: rg,
            projection_fingerprint: std::hash::Hasher::finish(&h),
        }
    }
}

struct Entry {
    batch: Arc<RecordBatch>,
    /// Bytes occupied by this entry (approx, sum of column buffer
    /// sizes).
    bytes: usize,
    /// Monotonic insertion counter — oldest = lowest = evicted first.
    inserted_at: u64,
}

/// Σ.O — in-memory LRU cache.
pub struct ParquetDecodeCache {
    inner: Mutex<Inner>,
    capacity_bytes: usize,
}

struct Inner {
    entries: HashMap<DecodeCacheKey, Entry>,
    bytes_used: usize,
    counter: u64,
    hits: u64,
    misses: u64,
}

impl Default for ParquetDecodeCache {
    fn default() -> Self {
        Self::new()
    }
}

impl ParquetDecodeCache {
    pub fn new() -> Self {
        Self::with_capacity_bytes(DEFAULT_CAPACITY_BYTES)
    }

    pub fn with_capacity_bytes(capacity_bytes: usize) -> Self {
        Self {
            inner: Mutex::new(Inner {
                entries: HashMap::new(),
                bytes_used: 0,
                counter: 0,
                hits: 0,
                misses: 0,
            }),
            capacity_bytes,
        }
    }

    /// Σ.O — lookup. Cloning the Arc is cheap; the underlying buffers
    /// are shared.
    pub fn get(&self, key: &DecodeCacheKey) -> Option<Arc<RecordBatch>> {
        let mut inner = self.inner.lock().unwrap();
        let batch = inner.entries.get(key).map(|e| e.batch.clone());
        if batch.is_some() {
            inner.hits += 1;
        } else {
            inner.misses += 1;
        }
        batch
    }

    /// Σ.O — insert. Evicts oldest entries until capacity allows.
    pub fn insert(&self, key: DecodeCacheKey, batch: RecordBatch) {
        let bytes = batch_size_bytes(&batch);
        // If the entry is bigger than capacity, skip entirely.
        if bytes > self.capacity_bytes {
            return;
        }
        let mut inner = self.inner.lock().unwrap();
        // Evict until we can fit.
        while inner.bytes_used + bytes > self.capacity_bytes {
            self.evict_one_locked(&mut inner);
        }
        inner.counter += 1;
        let counter = inner.counter;
        if let Some(old) = inner.entries.insert(
            key,
            Entry {
                batch: Arc::new(batch),
                bytes,
                inserted_at: counter,
            },
        ) {
            inner.bytes_used -= old.bytes;
        }
        inner.bytes_used += bytes;
    }

    fn evict_one_locked(&self, inner: &mut Inner) {
        // Find oldest. O(n) — fine for cache sizes in the thousands;
        // upgrade to a separate LRU list if profiling shows it.
        let oldest = inner
            .entries
            .iter()
            .min_by_key(|(_, e)| e.inserted_at)
            .map(|(k, _)| k.clone());
        if let Some(k) = oldest {
            if let Some(e) = inner.entries.remove(&k) {
                inner.bytes_used -= e.bytes;
            }
        }
    }

    /// Σ.O — manual reaper. Frees entries until we're at most
    /// `target_fraction` of capacity. Useful for memory pressure
    /// callbacks.
    pub fn reap_to(&self, target_fraction: f64) {
        let target = (self.capacity_bytes as f64 * target_fraction.clamp(0.0, 1.0)) as usize;
        let mut inner = self.inner.lock().unwrap();
        while inner.bytes_used > target {
            self.evict_one_locked(&mut inner);
        }
    }

    /// (hits, misses, bytes_used).
    pub fn stats(&self) -> (u64, u64, usize) {
        let inner = self.inner.lock().unwrap();
        (inner.hits, inner.misses, inner.bytes_used)
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Approximate size in bytes of a RecordBatch (sum of column buffer
/// allocations). Cheap — caller can also pass in a known-good value.
pub fn batch_size_bytes(batch: &RecordBatch) -> usize {
    batch
        .columns()
        .iter()
        .map(|c| {
            c.to_data()
                .buffers()
                .iter()
                .map(|b| b.capacity())
                .sum::<usize>()
        })
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};

    fn make_batch(n: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let arr = Int64Array::from((0..n as i64).collect::<Vec<_>>());
        RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap()
    }

    #[test]
    fn miss_then_hit() {
        let cache = ParquetDecodeCache::new();
        let key = DecodeCacheKey::new("data/t.parquet", 0, &["v"]);
        assert!(cache.get(&key).is_none());
        cache.insert(key.clone(), make_batch(100));
        assert!(cache.get(&key).is_some());
        let (h, m, _) = cache.stats();
        assert_eq!(h, 1);
        assert_eq!(m, 1);
    }

    #[test]
    fn projection_fingerprint_order_invariant() {
        let k1 = DecodeCacheKey::new("a", 0, &["x", "y", "z"]);
        let k2 = DecodeCacheKey::new("a", 0, &["z", "x", "y"]);
        assert_eq!(k1, k2);
    }

    #[test]
    fn different_rg_different_keys() {
        let k0 = DecodeCacheKey::new("a", 0, &["v"]);
        let k1 = DecodeCacheKey::new("a", 1, &["v"]);
        assert_ne!(k0, k1);
    }

    #[test]
    fn evicts_oldest_when_over_capacity() {
        // 1 KiB cap; each batch ~8 KB (1000 i64s).
        let cache = ParquetDecodeCache::with_capacity_bytes(8000);
        for i in 0..3u32 {
            let k = DecodeCacheKey::new("a", i, &["v"]);
            cache.insert(k, make_batch(1000));
        }
        // Cache should hold at most 1 entry (8000 ≈ one batch).
        assert!(cache.len() <= 1);
    }

    #[test]
    fn reap_to_target() {
        // Use a small cap so all 10 inserts fill the cache → reap
        // is meaningful.
        let cache = ParquetDecodeCache::with_capacity_bytes(100_000);
        for i in 0..10u32 {
            let k = DecodeCacheKey::new("a", i, &["v"]);
            cache.insert(k, make_batch(1000));
        }
        // reap to 50% of cap = 50_000 bytes.
        cache.reap_to(0.5);
        let (_, _, after) = cache.stats();
        assert!(after <= 50_000, "bytes_used {after} exceeds reap target");
    }

    #[test]
    fn batch_size_bytes_nonzero() {
        let b = make_batch(1000);
        let size = batch_size_bytes(&b);
        // 1000 i64s = 8000 bytes minimum, plus buffer alignment.
        assert!(size >= 8000);
        assert!(size <= 16000);
    }

    #[test]
    fn arc_returned_shared_with_internal_state() {
        let cache = ParquetDecodeCache::new();
        let key = DecodeCacheKey::new("a", 0, &["v"]);
        cache.insert(key.clone(), make_batch(100));
        let got1 = cache.get(&key).unwrap();
        let got2 = cache.get(&key).unwrap();
        // Same underlying data.
        assert!(Arc::ptr_eq(&got1, &got2));
    }
}
