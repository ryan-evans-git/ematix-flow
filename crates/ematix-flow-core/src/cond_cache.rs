//! Σ.CC — query condition cache (ClickHouse 25.x's condition cache
//! mapped onto ematix-flow's row-group granularity).
//!
//! [`BridgeFilter::build_bitmap`](crate::ematix_fast_parquet::BridgeFilter)
//! is the per-(file, row-group) cost of a pushed predicate set: it
//! decodes every predicate COLUMN to produce the combined row bitmap.
//! For repeated predicates — dashboards, retried queries, bench
//! trials 2..N — that work is identical every time. This module
//! caches the bitmap keyed by
//!
//!   (file path, mtime_ns, len, row group, predicate fingerprint)
//!
//! so a repeat skips every predicate-column decode for that row
//! group. Correctness properties:
//!
//! - **Exact key, exact value.** The fingerprint covers the full
//!   semantic content of every predicate (variant + fields; f64s via
//!   `to_bits`, both hashed AND length-prefix-concatenated through a
//!   64-bit hasher). File identity (mtime_ns, len) is part of the
//!   key, so a rewritten file simply never hits — stale entries decay
//!   out of the LRU.
//! - **Runtime blooms are never cached.** `I64InBloom` predicates are
//!   per-query artifacts (a join build side); [`BridgeFilterExt::
//!   cond_fingerprint`] returns `None` when any is present and the
//!   whole predicate set bypasses the cache.
//! - **Bounded.** `EMAT_COND_CACHE_BYTES` (default 256 MiB, 0
//!   disables) budgets the bitmap bytes; least-recently-used entries
//!   evict on overflow.
//!
//! Bench methodology note (site copy carries this): the cache
//! accelerates REPEATED predicates, so trials 2..N and `med(3-5)`
//! benefit while `first_trial_ms` stays cold-path honest — the same
//! class of effect as a warm page cache, and it ships default-ON so
//! benched defaults remain shipped defaults.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

/// Cache key: file identity + row group + predicate fingerprint.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CondKey {
    path: PathBuf,
    mtime_ns: u128,
    len: u64,
    rg: u32,
    pred_fp: u64,
}

/// Build a key for `path`/`rg`/`pred_fp`, stat-ing the file for its
/// identity. `None` when the stat fails (vanished file — the compute
/// path will surface the real error) or the cache is disabled.
pub fn key_for(path: &Path, rg: usize, pred_fp: u64) -> Option<CondKey> {
    if budget_bytes() == 0 {
        return None;
    }
    let md = std::fs::metadata(path).ok()?;
    let mtime_ns = md
        .modified()
        .ok()?
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_nanos();
    Some(CondKey {
        path: path.to_path_buf(),
        mtime_ns,
        len: md.len(),
        rg: rg as u32,
        pred_fp,
    })
}

struct Entry {
    bitmap: Arc<Vec<u8>>,
    total: usize,
    stamp: u64,
}

/// Byte-budgeted LRU over `(CondKey → bitmap)`. Recency is a
/// monotonic stamp; eviction scans for the minimum — entry counts are
/// thousands at most (8 KiB per 64Ki-row RG bitmap), so O(n) eviction
/// is noise next to one avoided column decode.
pub(crate) struct CondCacheInner {
    map: HashMap<CondKey, Entry>,
    bytes: usize,
    budget: usize,
    tick: u64,
}

impl CondCacheInner {
    pub(crate) fn new(budget: usize) -> Self {
        Self {
            map: HashMap::new(),
            bytes: 0,
            budget,
            tick: 0,
        }
    }

    pub(crate) fn lookup(&mut self, key: &CondKey) -> Option<(Arc<Vec<u8>>, usize)> {
        self.tick += 1;
        let tick = self.tick;
        let e = self.map.get_mut(key)?;
        e.stamp = tick;
        Some((Arc::clone(&e.bitmap), e.total))
    }

    pub(crate) fn insert(&mut self, key: CondKey, bitmap: Arc<Vec<u8>>, total: usize) -> u64 {
        let sz = bitmap.len();
        if sz > self.budget {
            return 0; // one bitmap bigger than the whole budget — skip.
        }
        self.tick += 1;
        if let Some(old) = self.map.insert(
            key,
            Entry {
                bitmap,
                total,
                stamp: self.tick,
            },
        ) {
            self.bytes -= old.bitmap.len();
        }
        self.bytes += sz;
        let mut evicted = 0u64;
        while self.bytes > self.budget {
            let Some(victim) = self
                .map
                .iter()
                .min_by_key(|(_, e)| e.stamp)
                .map(|(k, _)| k.clone())
            else {
                break;
            };
            if let Some(e) = self.map.remove(&victim) {
                self.bytes -= e.bitmap.len();
                evicted += 1;
            }
        }
        evicted
    }

    #[cfg(test)]
    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }
}

static CACHE: OnceLock<Mutex<CondCacheInner>> = OnceLock::new();
static HITS: AtomicU64 = AtomicU64::new(0);
static MISSES: AtomicU64 = AtomicU64::new(0);
static EVICTIONS: AtomicU64 = AtomicU64::new(0);

/// `EMAT_COND_CACHE_BYTES` — bitmap byte budget, default 256 MiB,
/// `0` disables the cache entirely. Read once per process.
pub fn budget_bytes() -> usize {
    static BUDGET: OnceLock<usize> = OnceLock::new();
    *BUDGET.get_or_init(|| {
        std::env::var("EMAT_COND_CACHE_BYTES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(256 << 20)
    })
}

fn cache() -> &'static Mutex<CondCacheInner> {
    CACHE.get_or_init(|| Mutex::new(CondCacheInner::new(budget_bytes())))
}

/// Cache lookup. Counts a hit/miss.
pub fn lookup(key: &CondKey) -> Option<(Arc<Vec<u8>>, usize)> {
    let got = cache().lock().expect("cond cache poisoned").lookup(key);
    match &got {
        Some(_) => HITS.fetch_add(1, Ordering::Relaxed),
        None => MISSES.fetch_add(1, Ordering::Relaxed),
    };
    got
}

/// Cache insert (post-compute).
pub fn insert(key: CondKey, bitmap: Arc<Vec<u8>>, total: usize) {
    let evicted = cache()
        .lock()
        .expect("cond cache poisoned")
        .insert(key, bitmap, total);
    if evicted > 0 {
        EVICTIONS.fetch_add(evicted, Ordering::Relaxed);
    }
}

/// Monotonic process-wide counters (probes + tests read deltas).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CondCacheMetrics {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
}

/// Snapshot the counters (relaxed).
pub fn cond_cache_metrics() -> CondCacheMetrics {
    CondCacheMetrics {
        hits: HITS.load(Ordering::Relaxed),
        misses: MISSES.load(Ordering::Relaxed),
        evictions: EVICTIONS.load(Ordering::Relaxed),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(n: u64) -> CondKey {
        CondKey {
            path: PathBuf::from(format!("/t/{n}.parquet")),
            mtime_ns: 1,
            len: 1,
            rg: 0,
            pred_fp: n,
        }
    }

    /// LRU eviction under a byte budget: oldest-touched entries leave
    /// first; a re-touched entry survives.
    #[test]
    fn lru_evicts_least_recent_within_budget() {
        let mut c = CondCacheInner::new(3 * 8); // room for 3 × 8-byte bitmaps
        let bm = |b: u8| Arc::new(vec![b; 8]);
        assert_eq!(c.insert(key(1), bm(1), 10), 0);
        assert_eq!(c.insert(key(2), bm(2), 10), 0);
        assert_eq!(c.insert(key(3), bm(3), 10), 0);
        // Touch 1 → 2 becomes least recent.
        assert!(c.lookup(&key(1)).is_some());
        assert_eq!(c.insert(key(4), bm(4), 10), 1);
        assert!(c.lookup(&key(2)).is_none(), "LRU victim must be 2");
        assert!(c.lookup(&key(1)).is_some());
        assert!(c.lookup(&key(4)).is_some());
        assert_eq!(c.bytes(), 3 * 8);
        // An entry over the whole budget is refused outright.
        assert_eq!(c.insert(key(5), Arc::new(vec![0; 64]), 10), 0);
        assert!(c.lookup(&key(5)).is_none());
    }

    /// Same-key reinsert replaces without double-counting bytes.
    #[test]
    fn reinsert_replaces_bytes_exactly() {
        let mut c = CondCacheInner::new(64);
        c.insert(key(1), Arc::new(vec![0; 16]), 5);
        c.insert(key(1), Arc::new(vec![1; 24]), 5);
        assert_eq!(c.bytes(), 24);
        let (bm, _) = c.lookup(&key(1)).unwrap();
        assert_eq!(bm.len(), 24);
    }
}
