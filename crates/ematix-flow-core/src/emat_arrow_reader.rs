//! Σ.E5.1 — Streaming Arrow `RecordBatch` reader over `ematix-parquet`.
//!
//! Reads parquet column chunks via the sibling-repo `ematix-parquet-io`
//! `PageWalker` + `ematix-parquet-codec` decoders, and emits Arrow
//! `RecordBatch`es of a caller-supplied schema. Replaces the whole-
//! row-group emission shape used by `ematix_parquet_bridge`'s typed
//! decoders for the Q1-shape workload — downstream operators see
//! 65 536-row batches (the FastParquet shape) instead of one mega-
//! batch per row group.
//!
//! ## API contract (locked by Σ.E5.2)
//!
//! The supplied Arrow schema carries the caller's column-type intent.
//! In particular:
//!   - `Utf8View`  → `StringViewArray`, materialised once per RG by
//!     pushing the parquet dictionary bytes as one backing block and
//!     building per-row views over it (no per-row `StringBuilder`).
//!   - `Dictionary(UInt32, Utf8)` → `DictionaryArray<UInt32Type>`
//!     preserving the parquet dict end-to-end.
//!   - `Int32` / `Date32` / `Int64` / `Float64` → primitive Arrays
//!     sliced from per-RG buffers.
//!   - `Utf8` (slow path) → `StringArray` materialised row-by-row.
//!
//! Anything else returns `DataFusionError::NotImplemented` from
//! `build()` — extend later when a workload needs it.
//!
//! ## Streaming semantics
//!
//! Per row group:
//!  1. Decode every projected column once (per-RG dict reuse).
//!  2. Slice the per-RG arrays into `batch_size`-row windows.
//!  3. Emit one `RecordBatch` per window.
//!  4. Cross RG boundaries between windows — never within. (Dict
//!     codes change at the RG boundary; mixing them in a single
//!     `DictionaryArray` would mis-index.)
//!
//! See `docs/PHASE_SIGMA_E5_PARQUET_RS_ELIMINATION.md` §E5.1.
//!
//! ## Scope (Σ.E5.1)
//!
//! Build + test + bench. No provider/exec wire-up — that's Σ.E5.1.b.
//! Async sibling + writer are Σ.E5.3/E5.4.

use std::sync::Arc;

use arrow_array::builder::{ArrayBuilder, StringBuilder, make_view};
use arrow_array::types::UInt32Type;
use arrow_array::{
    Array, ArrayRef, Date32Array, DictionaryArray, Float64Array, Int32Array, Int64Array,
    RecordBatch, StringArray, StringViewArray, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, SchemaRef};
// Buffer types come in via datafusion's arrow re-export — keeps this
// module's direct dep list aligned with the rest of the crate.
use datafusion::arrow::buffer::{Buffer, NullBuffer, ScalarBuffer};
use datafusion::error::{DataFusionError, Result as DfResult};

use crate::ematix_fast_parquet::BridgeFilter;
use crate::ematix_parquet_bridge::{
    masked_decode_byte_array, masked_decode_f64, masked_decode_i32, masked_decode_i64,
};
use ematix_parquet_codec::compression::{
    decompress_lz4_raw_into, decompress_lz4_raw_into_sized, decompress_snappy_into,
    decompress_zstd_into,
};
use ematix_parquet_codec::dict::decode_rle_dictionary_into;
use ematix_parquet_codec::plain::{
    decode_plain_byte_array, decode_plain_f64, decode_plain_i32, decode_plain_i64,
};
use ematix_parquet_codec::read::read_column_byte_array_dict_preserved;
use ematix_parquet_codec::read::read_column_byte_array_dict_preserved_into;
use ematix_parquet_format::types::{CompressionCodec, Encoding, ParquetType};
use ematix_parquet_io::{PageWalker, ParquetFile};

/// Default batch size — matches `FastParquetTableProvider`'s
/// `DEFAULT_BATCH_SIZE` and DataFusion's pipelining sweet spot.
pub const DEFAULT_BATCH_SIZE: usize = 65_536;

/// NARROW.DEC fire counter (observability, mirrors `TAG_BUILDS` /
/// `RH_BUILDS`): number of column chunks decoded through the
/// downcast-on-read narrow path (`Int32` target over physical INT64).
/// Read by tests and the A/B harness to prove which decode path ran.
pub static NARROW_KEY_DECODES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

// ---------------------------------------------------------------------
// Σ.O.c.1 — Private row-group decode cache.
//
// Stores `Vec<DecodedColumn>` per (file_path, row_group_idx, projection)
// so repeated scans across queries skip parquet decode work. Uses Arc-
// shared Arrow Buffers internally — clone is O(projection_count)
// pointer copies, not a data copy.
//
// Σ.O.c bench data (project-3 cols, fresh ctx per rep): first-rep 45ms
// → rep 2-5 36ms (warm OS cache). With this cache: rep 2-5 → ≪1ms
// (no decode at all).
//
// Filter mode (BridgeFilter set) BYPASSES the cache because the decode
// output is row-mask-specific.
// ---------------------------------------------------------------------

/// Σ.Q.L6′ — per-column cache key. Two scans of the same parquet file
/// that ask for overlapping column subsets (Q17: cols [1,4,5] then
/// cols [1,4]) share the decoded columns 1 and 4. Earlier
/// projection-bound key (`RgCacheKey { ..., projection: Vec<usize> }`)
/// keyed on the *set* of projected columns, so different projections
/// missed the cache entirely — bench-confirmed neutral on Q17 SF=10.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct RgCacheKey {
    pub(crate) file_path: std::path::PathBuf,
    pub(crate) row_group_idx: usize,
    /// Single parquet leaf-column index. Each row-group × column pair
    /// gets its own cache entry so overlapping projections share
    /// decoded columns.
    pub(crate) leaf_idx: usize,
}

/// Σ.O.c.1 — process-shared cache of decoded row-group columns.
/// Thread-safe; share an `Arc<RowGroupDecodeCache>` across reader
/// instances to amortise decode across queries.
///
/// Σ.AI.6e — two eviction policies, selected at construction:
///
/// - **retention = false** (legacy): pure FIFO on insertion order.
///   `get` never reorders, so a scan whose insert traffic exceeds
///   capacity evicts the seeded working set in insertion order even
///   while it is being hit — the Q09 SF=100 trial-3 collapse (6.5 s
///   cache-served → 16–50 s decode-bound once eviction starts).
/// - **retention = true** (`EMAT_RG_CACHE_RETENTION`, AUTO = ON):
///   segmented LRU with admission-on-second-touch. New entries land
///   in a *probationary* segment; only a second touch promotes to the
///   *protected* segment (capped at [`RG_PROTECTED_NUM`]/[`RG_PROTECTED_DEN`]
///   of capacity, LRU overflow demotes back to probation). Eviction
///   takes probation-LRU victims first and touches protected only
///   when probation is dry — a sequential one-pass flood dies in
///   probation and cannot displace the re-touched working set.
///
/// Σ.AI.6f — ghost-assisted adaptive demotion (ARC-lite), retention
/// mode only. Field counters (AWS TPC-H SF=100) proved Σ.AI.6e
/// deadlocks once the cache fills: protected only demotes on
/// promotion overflow, promotion needs a probation second touch, and
/// effective probation (capacity − protected ≈ 1/5) is smaller than a
/// later query's re-touch distance — so no second touches, no
/// promotions, no demotions: `retention(admit+0 … prot_evict+0)` on
/// every subsequent query while rejects count tens of thousands.
/// Whoever filled protected first owns it forever (order-lucky
/// poisoning). Fix: keys evicted from probation WITHOUT promotion go
/// to a bounded keys-only *ghost* list; a miss on a ghosted key is
/// proof a live working set re-touches at a distance probation can't
/// hold, so it demotes *stale* protected-LRU entries (untouched since
/// the ghost key was evicted) to probation-MRU, making room for the
/// live set to earn promotion. One-touch floods never re-request
/// their keys → no ghost hits → protected untouched (scan resistance
/// preserved); a still-hitting protected set has fresh stamps → the
/// staleness check refuses to demote it (loop-over-capacity streams
/// cannot cannibalise a live seed).
pub struct RowGroupDecodeCache {
    inner: std::sync::Mutex<RgInner>,
    capacity_bytes: usize,
    retention: bool,
}

/// Σ.AI.6e — which segment an entry lives in (retention mode only;
/// legacy FIFO keeps every entry in `Probation` and never promotes).
#[derive(Clone, Copy, PartialEq, Eq)]
enum RgSegment {
    Probation,
    Protected,
}

/// Σ.AI.6e — protected-segment cap as a fraction of `capacity_bytes`
/// (4/5). Leaves ≥1/5 of the budget as probation flow-through so
/// fresh entries can always demonstrate a second touch; a protected
/// set larger than the cap demotes LRU-first back to probation.
const RG_PROTECTED_NUM: usize = 4;
const RG_PROTECTED_DEN: usize = 5;

/// Σ.AI.6f — ghost-list floor (live-key count). The cap is
/// `max(entries.len(), RG_GHOST_MIN_ENTRIES)` — ARC sizes its ghost
/// lists to the cache's entry count so a re-touch distance up to ~2×
/// capacity is still observable; the floor keeps tiny/near-empty
/// caches from truncating ghost history to nothing.
const RG_GHOST_MIN_ENTRIES: usize = 64;

/// Σ.AI.6f — bytes of stale protected demoted per ghost hit, as a
/// multiple of the incoming entry: admit the entry itself plus equal
/// headroom so probation grows a little faster than the ghost-hitting
/// working set streams in (convergence in a bounded number of passes
/// instead of asymptotically).
const RG_GHOST_DEMOTE_HEADROOM: usize = 2;

struct RgInner {
    entries: std::collections::HashMap<RgCacheKey, RgEntry>,
    /// Σ.Q.L6′: `VecDeque` so eviction is O(1) (pop_front) instead of
    /// O(n) (Vec::remove(0)). With per-column keys the entry count
    /// grows 5-10× vs the old per-projection cache, so the linear-
    /// scan eviction became visible on SF=1 single-scan queries
    /// (Q06 +37%).
    ///
    /// Σ.AI.6e — legacy mode: FIFO insertion order (exact pre-Σ.AI.6e
    /// behaviour). Retention mode: probationary LRU queue with lazy
    /// (stamp-validated) entries — see `RgEntry::stamp`.
    q_probation: std::collections::VecDeque<(u64, RgCacheKey)>,
    /// Σ.AI.6e — protected LRU queue (retention mode only; stays
    /// empty in legacy mode). Same lazy-stamp scheme as probation.
    q_protected: std::collections::VecDeque<(u64, RgCacheKey)>,
    bytes_used: usize,
    /// Σ.AI.6e — bytes currently in the protected segment.
    protected_bytes: usize,
    /// Σ.AI.6e — monotone stamp for lazy queue entries: a queued
    /// `(stamp, key)` is live iff `entries[key].stamp == stamp` and
    /// the segment matches the queue. Re-touch pushes a fresh pair
    /// instead of an O(n) mid-queue removal; stale pairs are skipped
    /// on pop and swept by `maybe_compact` — O(1) amortised per op.
    next_stamp: u64,
    hits: u64,
    misses: u64,
    /// Σ.AI.6e probe counters (survive `clear()`, like hits/misses):
    /// probation→protected promotions (second touch) …
    retention_admits: u64,
    /// … probation evictions (one-touch entries denied admission) …
    retention_rejects: u64,
    /// … and protected-segment evictions (probation ran dry).
    protected_evictions: u64,
    /// Σ.AI.6f — ghost list: keys (no payloads) recently evicted from
    /// probation WITHOUT promotion, mapped to their eviction stamp.
    /// A later miss on one of these keys ("ghost hit") is proof that
    /// probation is too small for a live, re-touching working set —
    /// the trigger for stale-protected demotion. Bounded: live-key
    /// cap = `max(entries.len(), RG_GHOST_MIN_ENTRIES)`.
    ghost: std::collections::HashMap<RgCacheKey, u64>,
    /// Σ.AI.6f — FIFO order for ghost trimming. Same lazy scheme as
    /// the segment queues: a pair is live iff `ghost[key] == stamp`
    /// (consumed ghost hits leave stale pairs, swept when the queue
    /// outgrows the live set 2:1).
    ghost_q: std::collections::VecDeque<(u64, RgCacheKey)>,
    /// Σ.AI.6f probe counters (survive `clear()`, like hits/misses):
    /// inserts whose key was found in the ghost list …
    ghost_hits: u64,
    /// … and stale protected→probation demotions those hits forced.
    ghost_demotions: u64,
}

struct RgEntry {
    column: std::sync::Arc<DecodedColumn>,
    bytes: usize,
    /// Σ.AI.6e — validity stamp; see `RgInner::next_stamp`.
    stamp: u64,
    seg: RgSegment,
}

impl RowGroupDecodeCache {
    /// Default cap 1 GiB; retention policy per `EMAT_RG_CACHE_RETENTION`
    /// (AUTO = ON, Σ.AI.6e).
    pub fn new() -> Self {
        Self::with_capacity_bytes(1024 * 1024 * 1024)
    }

    /// Retention policy resolved from the environment
    /// ([`rg_cache_retention_resolved`]); use
    /// [`Self::with_capacity_bytes_and_retention`] for an explicit,
    /// env-independent policy (tests / A/B harnesses).
    pub fn with_capacity_bytes(capacity_bytes: usize) -> Self {
        Self::with_capacity_bytes_and_retention(capacity_bytes, rg_cache_retention_resolved())
    }

    /// Σ.AI.6e — explicit-policy constructor: `retention = false` is
    /// the exact pre-Σ.AI.6e FIFO; `true` is segmented LRU with
    /// admission-on-second-touch.
    pub fn with_capacity_bytes_and_retention(capacity_bytes: usize, retention: bool) -> Self {
        Self {
            inner: std::sync::Mutex::new(RgInner {
                entries: std::collections::HashMap::new(),
                q_probation: std::collections::VecDeque::new(),
                q_protected: std::collections::VecDeque::new(),
                bytes_used: 0,
                protected_bytes: 0,
                next_stamp: 0,
                hits: 0,
                misses: 0,
                retention_admits: 0,
                retention_rejects: 0,
                protected_evictions: 0,
                ghost: std::collections::HashMap::new(),
                ghost_q: std::collections::VecDeque::new(),
                ghost_hits: 0,
                ghost_demotions: 0,
            }),
            capacity_bytes,
            retention,
        }
    }

    /// Σ.AI.6e — protected-segment byte cap (4/5 of capacity).
    fn protected_cap(&self) -> usize {
        self.capacity_bytes / RG_PROTECTED_DEN * RG_PROTECTED_NUM
    }

    pub(crate) fn get(&self, key: &RgCacheKey) -> Option<std::sync::Arc<DecodedColumn>> {
        let mut guard = self.inner.lock().unwrap();
        let inner = &mut *guard;
        let Some(e) = inner.entries.get_mut(key) else {
            inner.misses += 1;
            return None;
        };
        inner.hits += 1;
        let column = e.column.clone();
        if self.retention {
            // Σ.AI.6e — touch: probation hit is the second touch →
            // promote to protected; protected hit refreshes recency.
            inner.next_stamp += 1;
            e.stamp = inner.next_stamp;
            if e.seg == RgSegment::Probation {
                e.seg = RgSegment::Protected;
                inner.protected_bytes += e.bytes;
                inner.retention_admits += 1;
            }
            inner.q_protected.push_back((e.stamp, key.clone()));
            demote_protected_overflow(inner, self.protected_cap());
            maybe_compact(inner);
        }
        Some(column)
    }

    pub(crate) fn insert(&self, key: RgCacheKey, column: DecodedColumn) {
        let bytes = estimate_column_bytes(&column);
        if bytes > self.capacity_bytes {
            return; // entry alone exceeds cap; skip
        }
        let mut guard = self.inner.lock().unwrap();
        let inner = &mut *guard;
        if self.retention {
            // Σ.AI.6f — ghost hit: this key was recently evicted from
            // probation un-promoted and is being re-requested — its
            // re-touch distance exceeds what probation can hold. Demote
            // stale protected-LRU entries (untouched since this key was
            // evicted) so the live working set can earn promotion.
            // Consumes the ghost entry (a key proves the point once).
            if let Some(ghost_stamp) = inner.ghost.remove(&key) {
                inner.ghost_hits += 1;
                demote_stale_protected_on_ghost_hit(inner, ghost_stamp, bytes);
            }
            // Σ.AI.6e — probation-first eviction: one-touch flood
            // traffic evicts itself; the protected working set pays
            // only when probation runs dry.
            evict_retention(inner, bytes, self.capacity_bytes);
        } else {
            while inner.bytes_used + bytes > self.capacity_bytes && !inner.q_probation.is_empty() {
                // O(1) FIFO eviction — was O(n) when using Vec::remove(0)
                // and the per-column cache has 5-10× more entries than
                // the old per-projection one.
                let (_, oldest) = inner.q_probation.pop_front().unwrap();
                if let Some(e) = inner.entries.remove(&oldest) {
                    inner.bytes_used -= e.bytes;
                }
            }
        }
        let arc = std::sync::Arc::new(column);
        inner.next_stamp += 1;
        let stamp = inner.next_stamp;
        if let Some(old) = inner.entries.insert(
            key.clone(),
            RgEntry {
                column: arc,
                bytes,
                stamp,
                seg: RgSegment::Probation,
            },
        ) {
            inner.bytes_used -= old.bytes;
            if old.seg == RgSegment::Protected {
                // Σ.AI.6e — replacement (racing decoders) restarts the
                // entry in probation; the stale protected queue pair is
                // skipped on pop via its stamp.
                inner.protected_bytes -= old.bytes;
            }
            if self.retention {
                inner.q_probation.push_back((stamp, key));
            }
            // legacy FIFO: already in q_probation, no need to re-add
            // (eviction removes by key, stamps are not checked).
        } else {
            inner.q_probation.push_back((stamp, key));
        }
        inner.bytes_used += bytes;
        if self.retention {
            maybe_compact(inner);
        }
    }

    pub fn stats(&self) -> (u64, u64, usize) {
        let inner = self.inner.lock().unwrap();
        (inner.hits, inner.misses, inner.bytes_used)
    }

    /// Σ.AI.6e probe — `(retention_admits, retention_rejects,
    /// protected_evictions)`: probation→protected promotions, probation
    /// evictions (one-touch entries denied admission), and protected
    /// evictions (probation ran dry). All zero in legacy-FIFO mode.
    pub fn retention_stats(&self) -> (u64, u64, u64) {
        let inner = self.inner.lock().unwrap();
        (
            inner.retention_admits,
            inner.retention_rejects,
            inner.protected_evictions,
        )
    }

    /// Σ.AI.6f probe — `(ghost_hits, ghost_demotions)`: inserts whose
    /// key was found in the ghost list (a live working set re-touching
    /// beyond probation's reach), and the stale protected→probation
    /// demotions those hits forced. Both zero in legacy-FIFO mode, and
    /// both zero under pure one-touch floods (flood keys are never
    /// re-requested). Additive probe — `retention_stats()` unchanged.
    pub fn retention_ghost_stats(&self) -> (u64, u64) {
        let inner = self.inner.lock().unwrap();
        (inner.ghost_hits, inner.ghost_demotions)
    }

    /// Σ.AI.6e — which policy this cache instance runs.
    pub fn retention_enabled(&self) -> bool {
        self.retention
    }

    /// Σ.AI.6f — test probe: `(live ghost keys, ghost queue pairs)`.
    /// Both must stay bounded (cap + lazy-pair slack) under any
    /// traffic; see `rg_cache_retention_ghost_list_is_bounded`.
    #[cfg(test)]
    fn ghost_len(&self) -> (usize, usize) {
        let inner = self.inner.lock().unwrap();
        (inner.ghost.len(), inner.ghost_q.len())
    }

    /// Σ.AI.6d — drop every cached column and return the bytes to the
    /// allocator (cache-shed action of `mem_pressure::DecodeShedGate`:
    /// under memory pressure the cache's up-to-1-GiB is page-cache the
    /// scan working set needs more). Hit/miss counters survive — they
    /// describe lookup history, not current contents.
    pub fn clear(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.entries.clear();
        inner.q_probation.clear();
        inner.q_protected.clear();
        // Σ.AI.6f — ghost history describes evicted *contents*; after a
        // shed it would demote a protected set that no longer exists
        // (harmless) or, worse, one rebuilt from scratch — drop it.
        inner.ghost.clear();
        inner.ghost_q.clear();
        inner.bytes_used = 0;
        inner.protected_bytes = 0;
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Σ.AI.6e — retention-mode eviction: pop probation-LRU victims
/// (skipping stale stamp pairs) until `needed` fits; fall back to
/// protected-LRU only when probation is dry. Amortised O(1): every
/// queue pair is popped at most once.
fn evict_retention(inner: &mut RgInner, needed: usize, capacity: usize) {
    while inner.bytes_used + needed > capacity {
        let victim = loop {
            match inner.q_probation.pop_front() {
                Some((stamp, k)) => {
                    let live = inner
                        .entries
                        .get(&k)
                        .is_some_and(|e| e.stamp == stamp && e.seg == RgSegment::Probation);
                    if live {
                        break Some((k, RgSegment::Probation));
                    }
                }
                None => {
                    break loop {
                        match inner.q_protected.pop_front() {
                            Some((stamp, k)) => {
                                let live = inner.entries.get(&k).is_some_and(|e| {
                                    e.stamp == stamp && e.seg == RgSegment::Protected
                                });
                                if live {
                                    break Some((k, RgSegment::Protected));
                                }
                            }
                            None => break None,
                        }
                    };
                }
            }
        };
        let Some((k, seg)) = victim else {
            break; // cache empty; caller's entry fits by the cap pre-check
        };
        if let Some(e) = inner.entries.remove(&k) {
            inner.bytes_used -= e.bytes;
            if seg == RgSegment::Protected {
                inner.protected_bytes -= e.bytes;
                inner.protected_evictions += 1;
            } else {
                inner.retention_rejects += 1;
                // Σ.AI.6f — evicted from probation without ever earning
                // promotion: remember the key so a re-request can prove
                // probation was too small (ghost hit → stale-protected
                // demotion). Protected evictions are NOT ghosted — they
                // already had their tenure.
                rg_ghost_push(inner, k);
            }
        }
    }
}

/// Σ.AI.6f — record a promotion-less probation eviction in the ghost
/// list and trim to cap. Keys only (a `RgCacheKey` is a path + two
/// indices — no decoded payload is retained), FIFO trim, lazy stale
/// pairs swept 2:1 — O(1) amortised, bounded by construction.
fn rg_ghost_push(inner: &mut RgInner, key: RgCacheKey) {
    inner.next_stamp += 1;
    let stamp = inner.next_stamp;
    // A key enters the ghost only when evicted live, and re-insertion
    // consumes its ghost entry — so this insert never truly collides;
    // if it ever did, the map keeps the newer stamp and the older
    // queue pair goes stale (skipped by the `== Some(&s)` checks).
    inner.ghost.insert(key.clone(), stamp);
    inner.ghost_q.push_back((stamp, key));
    let cap = inner.entries.len().max(RG_GHOST_MIN_ENTRIES);
    while inner.ghost.len() > cap {
        let Some((s, k)) = inner.ghost_q.pop_front() else {
            break; // unreachable: map is populated only via the queue
        };
        if inner.ghost.get(&k) == Some(&s) {
            inner.ghost.remove(&k);
        }
    }
    // Consumed ghost hits leave stale queue pairs behind (the map entry
    // is removed, the pair is not) — sweep once they outgrow the live
    // set 2:1, same policy as `maybe_compact`.
    if inner.ghost_q.len() > 2 * inner.ghost.len() + 64 {
        let ghost = &inner.ghost;
        inner.ghost_q.retain(|(s, k)| ghost.get(k) == Some(s));
    }
}

/// Σ.AI.6f — the freeze breaker: on a ghost hit, demote protected-LRU
/// entries back to probation-MRU until the incoming entry has
/// [`RG_GHOST_DEMOTE_HEADROOM`]× its bytes of reclaimed room — but
/// ONLY entries whose last touch predates the ghost key's eviction
/// (`stamp < ghost_stamp`). That staleness gate is what preserves the
/// Σ.AI.6e guarantees: a protected set that is still being hit carries
/// stamps newer than any concurrent probation eviction, so a
/// loop-over-capacity stream (whose ghost hits are legitimate) cannot
/// cannibalise a live seed; only a set that stopped being touched
/// before the ghost key's whole probation lifetime is demotable.
/// Demotion keeps bytes cached (probation-MRU, fresh stamp) — a
/// demoted entry that is in fact still wanted earns its way back with
/// one touch. Amortised O(1): live pops are byte-bounded, stale pops
/// are paid for by the push that created them.
fn demote_stale_protected_on_ghost_hit(inner: &mut RgInner, ghost_stamp: u64, needed: usize) {
    let target = needed.saturating_mul(RG_GHOST_DEMOTE_HEADROOM);
    let mut freed = 0usize;
    while freed < target {
        let Some((stamp, k)) = inner.q_protected.pop_front() else {
            break; // protected empty — nothing left to reclaim
        };
        let live = inner
            .entries
            .get(&k)
            .is_some_and(|e| e.stamp == stamp && e.seg == RgSegment::Protected);
        if !live {
            continue; // stale pair, skip (lazy-queue invariant)
        }
        if stamp >= ghost_stamp {
            // Protected-LRU was touched after the ghost key fell out of
            // probation → the entire protected set is fresher still.
            // Put the pair back and refuse: this ghost hit is capacity
            // pressure between two LIVE sets, not a stale freeze.
            inner.q_protected.push_front((stamp, k));
            break;
        }
        inner.next_stamp += 1;
        let fresh = inner.next_stamp;
        let e = inner.entries.get_mut(&k).expect("liveness checked above");
        let bytes = e.bytes;
        e.stamp = fresh;
        e.seg = RgSegment::Probation;
        inner.protected_bytes -= bytes;
        inner.q_probation.push_back((fresh, k));
        inner.ghost_demotions += 1;
        freed += bytes;
    }
}

/// Σ.AI.6e — demote protected-LRU entries back to probation while the
/// protected segment exceeds its cap. Demotion keeps the bytes cached
/// (total budget unchanged) but exposes them to probation eviction —
/// a demoted entry earns its way back with another touch.
fn demote_protected_overflow(inner: &mut RgInner, protected_cap: usize) {
    while inner.protected_bytes > protected_cap {
        let Some((stamp, k)) = inner.q_protected.pop_front() else {
            break;
        };
        let live = inner
            .entries
            .get(&k)
            .is_some_and(|e| e.stamp == stamp && e.seg == RgSegment::Protected);
        if !live {
            continue;
        }
        inner.next_stamp += 1;
        let fresh = inner.next_stamp;
        let e = inner.entries.get_mut(&k).expect("liveness checked above");
        e.stamp = fresh;
        e.seg = RgSegment::Probation;
        inner.protected_bytes -= e.bytes;
        inner.q_probation.push_back((fresh, k));
    }
}

/// Σ.AI.6e — sweep stale (stamp-superseded) queue pairs once the
/// queues outgrow the live entry set 2:1. `VecDeque::retain` keeps
/// order and allocates nothing; amortised O(1) per cache op.
fn maybe_compact(inner: &mut RgInner) {
    let live = inner.entries.len();
    if inner.q_probation.len() + inner.q_protected.len() <= 2 * live + 64 {
        return;
    }
    let entries = &inner.entries;
    inner.q_probation.retain(|(s, k)| {
        entries
            .get(k)
            .is_some_and(|e| e.stamp == *s && e.seg == RgSegment::Probation)
    });
    inner.q_protected.retain(|(s, k)| {
        entries
            .get(k)
            .is_some_and(|e| e.stamp == *s && e.seg == RgSegment::Protected)
    });
}

impl Default for RowGroupDecodeCache {
    fn default() -> Self {
        Self::new()
    }
}

// Σ.O.c.2 — process-wide row-group decode cache slot. Settable at
// runtime via `set_process_rg_decode_cache`. **Default ON at the
// 2026-05-24 milestone config** (closes Q13 −58 ms, Q21 −41 ms,
// Q18 −35 ms on TPC-H SF=10). Override via `EMAT_RG_DECODE_CACHE=0`
// to disable for A/B benching; `EMAT_RG_DECODE_CACHE_BYTES=<n>`
// overrides the default 1 GiB cap.
//
// `RwLock` is used so the hot-path lookup (in provider wire-up) is
// shared-read; only install/uninstall takes the write lock.
static PROCESS_RG_DECODE_CACHE: std::sync::OnceLock<
    std::sync::RwLock<Option<std::sync::Arc<RowGroupDecodeCache>>>,
> = std::sync::OnceLock::new();

fn process_rg_decode_cache_slot()
-> &'static std::sync::RwLock<Option<std::sync::Arc<RowGroupDecodeCache>>> {
    PROCESS_RG_DECODE_CACHE.get_or_init(|| {
        let initial = {
            let enabled = std::env::var("EMAT_RG_DECODE_CACHE")
                .ok()
                .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
                .unwrap_or(true);
            if enabled {
                let cap = std::env::var("EMAT_RG_DECODE_CACHE_BYTES")
                    .ok()
                    .and_then(|s| s.parse::<usize>().ok())
                    .unwrap_or(1024 * 1024 * 1024);
                Some(std::sync::Arc::new(
                    RowGroupDecodeCache::with_capacity_bytes(cap),
                ))
            } else {
                None
            }
        };
        std::sync::RwLock::new(initial)
    })
}

/// Σ.O.c.2 — read the current process-wide RG decode cache, if one is
/// installed. Returned as `Option<Arc<...>>` so callers wire it into
/// builders without branching on env each call.
pub fn process_rg_decode_cache() -> Option<std::sync::Arc<RowGroupDecodeCache>> {
    process_rg_decode_cache_slot().read().unwrap().clone()
}

/// Σ.O.c.2 — install (or uninstall, with `None`) the process-wide RG
/// decode cache. Bench-friendly: lets a single process compare
/// rep-progression with cache off vs on without re-execing.
pub fn set_process_rg_decode_cache(cache: Option<std::sync::Arc<RowGroupDecodeCache>>) {
    *process_rg_decode_cache_slot().write().unwrap() = cache;
}

/// Σ.AI.6e — AUTO arm of `EMAT_RG_CACHE_RETENTION`: **ON**. The unit
/// benches (`rg_cache_retention_*` below) show segmented-LRU retention
/// never loses to the legacy FIFO on any measured pattern (one-pass
/// flood, LRU-friendly loop, Q09-shaped seed+re-stream) — parity where
/// FIFO is fine, 0% → 100% where FIFO thrashes. Final call = the AWS
/// 32 GB-box FULL-SUITE A/B (isolated memory-lever A/Bs don't
/// transfer — proven twice, docs/plans/MEMORY_BUDGET.md).
const RG_CACHE_RETENTION_AUTO_ON: bool = true;

/// Σ.AI.6e — resolve `EMAT_RG_CACHE_RETENTION` (tri-state): `=1`
/// force retention, `=0` force legacy FIFO, unset/unrecognized =
/// AUTO = ON. Read at cache construction (`with_capacity_bytes` /
/// the process-slot init), not per lookup.
pub fn rg_cache_retention_resolved() -> bool {
    crate::flags::tri_state("EMAT_RG_CACHE_RETENTION").unwrap_or(RG_CACHE_RETENTION_AUTO_ON)
}

fn estimate_column_bytes(c: &DecodedColumn) -> usize {
    match c {
        DecodedColumn::Int32 { data, .. } => data.len(),
        DecodedColumn::Int64 { data, .. } => data.len(),
        DecodedColumn::Float64 { data, .. } => data.len(),
        DecodedColumn::StringView {
            views,
            data_buffers,
            ..
        } => views.len() + data_buffers.iter().map(|b| b.len()).sum::<usize>(),
        DecodedColumn::DictUtf8 {
            values, indices, ..
        } => values.value_data().len() + indices.len(),
        DecodedColumn::Utf8(s) => s.value_data().len(),
    }
}

/// `EMAT_BATCH_SIZE` env override (decimal). Σ.E5 diagnostic for the
/// Q16 HashJoin-probe gap: split the masked-decode output into more
/// batches when the post-filter row count is large relative to the
/// reader's batch_size. Default = `DEFAULT_BATCH_SIZE`.
fn env_batch_size() -> usize {
    std::env::var("EMAT_BATCH_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_BATCH_SIZE)
}

// ============================================================
// Cached metadata — Σ.E5.6 profile-driven optimization
// ============================================================
//
// Profiling Q19 (200-iteration loop, ~7s) showed ~10% of CPU spent
// in `read_file_metadata` + `read_row_group` + `read_column_chunk`
// + `read_column_metadata` (1014 samples across metadata-parsing
// frames). Root cause: `ParquetFile::metadata()` re-decodes the
// thrift footer on every call, and we call it once per
// (decode_column × RG × partition) — for Q19 that's 6×6×6 = 216
// calls per query × 200 queries = 43,200 parses.
//
// Fix: snapshot the metadata fields we actually use into an owned
// struct (primitives only — no borrows from the footer bytes), cache
// it on the reader, and share read-only across scoped threads.

/// Per-column-chunk metadata we need for decode. Owned (primitives
/// only) so it can live as long as the reader without lifetime gymnastics.
#[derive(Debug, Clone)]
pub(crate) struct CachedColumnChunk {
    pub num_values: i64,
    pub codec: CompressionCodec,
    pub dictionary_page_offset: Option<i64>,
    pub data_page_offset: i64,
    pub total_compressed_size: i64,
    pub total_uncompressed_size: i64,
    #[allow(dead_code)]
    pub column_type: ParquetType,
}

/// Per-row-group cached metadata.
#[derive(Debug, Clone)]
pub(crate) struct CachedRowGroup {
    pub num_rows: i64,
    pub columns: Vec<CachedColumnChunk>,
}

/// File-level cached metadata — built once at reader construction,
/// shared (via Arc) across all scoped column-decode threads.
#[derive(Debug, Clone)]
pub(crate) struct CachedFileMetadata {
    pub row_groups: Vec<CachedRowGroup>,
}

impl CachedFileMetadata {
    pub fn from_file(file: &ParquetFile) -> DfResult<Self> {
        let md = file.metadata().map_err(|e| ext(format!("metadata: {e}")))?;
        let row_groups = md
            .row_groups
            .iter()
            .map(|rg| {
                let columns = rg
                    .columns
                    .iter()
                    .map(|col| {
                        let cm = col
                            .meta_data
                            .as_ref()
                            .ok_or_else(|| ext("column missing meta_data"))?;
                        Ok(CachedColumnChunk {
                            num_values: cm.num_values,
                            codec: cm.codec,
                            dictionary_page_offset: cm.dictionary_page_offset,
                            data_page_offset: cm.data_page_offset,
                            total_compressed_size: cm.total_compressed_size,
                            total_uncompressed_size: cm.total_uncompressed_size,
                            column_type: cm.column_type,
                        })
                    })
                    .collect::<DfResult<Vec<_>>>()?;
                Ok(CachedRowGroup {
                    num_rows: rg.num_rows,
                    columns,
                })
            })
            .collect::<DfResult<Vec<_>>>()?;
        Ok(Self { row_groups })
    }
}

// ============================================================
// Builder
// ============================================================

/// Builder for [`EmatArrowBatchReader`].
///
/// See module docs for semantics. `arrow_schema` is REQUIRED and drives
/// type promotion (Utf8View / Dictionary). `projection` carries leaf
/// indices in the parquet column order; the output `RecordBatch`
/// Morsel-engine P2 spike: a shared, lock-free work queue of row-group
/// indices that every partition decode stream pulls from, replacing the
/// static round-robin `rg % N` assignment. Idle decode threads grab the
/// next RG, so the per-RG cost imbalance P1 measured (filter pruning →
/// heavy RGs aliasing onto a few partitions) self-balances. Created once
/// per physical-plan node and shared across its partition streams; the
/// bench drives a fresh plan per trial, so the single-drain lifetime is
/// correct (re-executing the same node would find it empty — spike-only,
/// gated `EMAT_MORSEL_STEAL=1`). See docs/plans/MORSEL_ENGINE.md.
#[derive(Debug)]
pub struct SharedRgCursor {
    rgs: Vec<usize>,
    next: std::sync::atomic::AtomicUsize,
}

impl SharedRgCursor {
    pub fn new(rgs: Vec<usize>) -> Self {
        Self {
            rgs,
            next: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Claim the next row group, or `None` when the queue is drained.
    #[inline]
    pub fn pop(&self) -> Option<usize> {
        let i = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.rgs.get(i).copied()
    }

    pub fn len(&self) -> usize {
        self.rgs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rgs.is_empty()
    }
}

/// schema has the same column order, with one Arrow field per
/// projected leaf taken from `arrow_schema` (or all fields if no
/// projection is supplied).
pub struct EmatArrowBatchReaderBuilder {
    file: ParquetFile,
    arrow_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    row_groups: Option<Vec<usize>>,
    batch_size: usize,
    /// Σ.E5.1.c: per-RG column-decode parallelism budget. `None` means
    /// "default to `available_parallelism()`" — the original behaviour.
    /// `Some(n)` caps the per-RG scoped-thread count at `n` (still
    /// further capped by `n_projected_columns`). Callers that know how
    /// much outer parallelism is already in play (e.g. a per-partition
    /// `ExecutionPlan`) pass `total_threads / outer_partitions` so the
    /// total concurrent thread count tracks the core count instead of
    /// the product.
    parallelism_budget: Option<usize>,
    /// Σ.E5 (#516): when set, the reader runs in late-materialisation
    /// mode — decode the filter column first, build a row bitmap, then
    /// masked-decode each projected column. Lets the streaming reader
    /// skip ~99% of decode work on selective queries (Q14, Q06) while
    /// preserving the per-batch slicing shape.
    filter: Option<BridgeFilter>,
    /// File path. Needed for `filter_i32_column_to_bitmap` which opens
    /// the file itself. Cached here at builder time to avoid threading
    /// it through `load_row_group`.
    path: Option<std::path::PathBuf>,
    /// Σ.O.b — optional shared parquet decode cache. When present,
    /// load_row_group consults the cache keyed by (path,
    /// row_group_idx, projection_fingerprint) before decoding; on
    /// hit, returns the cached Arc<RecordBatch> directly. On miss,
    /// decodes and inserts.
    ///
    /// Today this field accepts the cache; the load_row_group hot-
    /// path lookup is **wired but conservative**: lookup on entry,
    /// insert on successful decode. Filter-mode decodes (`with_filter`)
    /// bypass the cache (the masked output is row-mask-specific and
    /// not safely shareable across queries with different filters).
    decode_cache: Option<std::sync::Arc<crate::parquet_decode_cache::ParquetDecodeCache>>,
    /// Σ.O.c.1 — private row-group decode cache (`Vec<DecodedColumn>`
    /// keyed by file/rg/projection). Bypassed when `filter` is set.
    rg_decode_cache: Option<std::sync::Arc<RowGroupDecodeCache>>,
    /// L9.ADAPT LATE-ARM — a tight-rescued wrap's sideband whose build
    /// hadn't published when the scan started. `load_row_group` peeks
    /// it at each row-group boundary and merges the published
    /// predicates into `self.filter` once they land — the probe never
    /// stalls waiting for the build (the wait-based design cost Q17
    /// +31% in-sweep when a cold build blew through its 60ms budget).
    late_arm: Option<crate::bridge_filter_sideband::BridgeFilterSideband>,
    /// NARROW.DEC — file-leaf indices of KEYS.2-narrowed INT64 key
    /// columns that should decode DIRECTLY to `Int32` (downcast-on-
    /// read) instead of decode-wide + boundary-cast. `build()` rewrites
    /// the matching projected fields Int64→Int32 so the reader emits
    /// `Int32Array` natively. Empty by default (bit-identical to the
    /// pre-NARROW.DEC reader).
    narrow_i64_leaves: Vec<usize>,
    /// Morsel-engine P2 spike: when set, the reader ignores its static
    /// `row_groups` and pulls RGs from this shared work queue (dynamic
    /// work-stealing across partition streams). See [`SharedRgCursor`].
    /// Mutually exclusive with the L9 sideband path per scan (morsel
    /// call sites pass `None` for `late_arm` and vice versa).
    shared_cursor: Option<std::sync::Arc<SharedRgCursor>>,
}

impl EmatArrowBatchReaderBuilder {
    pub fn new(file: ParquetFile, arrow_schema: SchemaRef) -> Self {
        Self {
            file,
            arrow_schema,
            projection: None,
            row_groups: None,
            batch_size: env_batch_size(),
            parallelism_budget: None,
            filter: None,
            path: None,
            decode_cache: None,
            rg_decode_cache: None,
            late_arm: None,
            narrow_i64_leaves: Vec::new(),
            shared_cursor: None,
        }
    }

    /// NARROW.DEC — declare the file-leaf indices of stats-proven
    /// narrowed INT64 keys (KEYS.2's `narrowed_leaves`). Projected
    /// leaves in this set whose schema field is `Int64` over a
    /// physical INT64 column are rewritten to `Int32` at `build()`
    /// and decode through `read_column_i64_downcast` — emitting
    /// `Int32Array` directly, no transient `Vec<i64>`, no boundary
    /// cast. Leaves not projected (or not Int64) are ignored.
    pub fn with_narrow_i64_leaves(mut self, leaves: Vec<usize>) -> Self {
        self.narrow_i64_leaves = leaves;
        self
    }

    /// Morsel-engine P2 spike: pull row groups from a shared work queue
    /// instead of this reader's static `row_groups` assignment.
    pub fn with_shared_rg_cursor(mut self, cursor: std::sync::Arc<SharedRgCursor>) -> Self {
        self.shared_cursor = Some(cursor);
        self
    }

    /// Σ.O.b — install a process-shared parquet decode cache. Across
    /// queries that scan the same (file, row_group, projection),
    /// the second read returns the Arc<RecordBatch> from cache
    /// instead of re-decoding.
    pub fn with_decode_cache(
        mut self,
        cache: std::sync::Arc<crate::parquet_decode_cache::ParquetDecodeCache>,
    ) -> Self {
        self.decode_cache = Some(cache);
        self
    }

    /// Σ.O.c.1 — install a private (per-process or per-context)
    /// row-group decode cache. Stores `Vec<DecodedColumn>` per (file,
    /// rg, projection) so repeated scans skip parquet decode entirely.
    /// Wired into the dense (no-filter) `load_row_group_dense` path
    /// only — filter paths produce row-mask-specific output and aren't
    /// safely shareable.
    pub fn with_rg_decode_cache(mut self, cache: std::sync::Arc<RowGroupDecodeCache>) -> Self {
        self.rg_decode_cache = Some(cache);
        self
    }

    /// Σ.O.c.2 — set the source file path so the RG decode cache key
    /// can include it. `with_filter` already sets this; callers using
    /// only `with_rg_decode_cache` (no filter) need to set it
    /// explicitly so cache hits are scoped to the right file.
    pub fn with_path(mut self, path: std::path::PathBuf) -> Self {
        self.path = Some(path);
        self
    }

    /// Σ.E5 (#516): enable late-materialisation with the given filter.
    /// The filter column is decoded first to build a row bitmap, then
    /// projected columns are masked-decoded — pages with zero
    /// bitmap-popcount are skipped entirely. Requires `with_path` so
    /// the bitmap-build kernel can open the file.
    pub fn with_filter(mut self, filter: BridgeFilter, path: std::path::PathBuf) -> Self {
        self.filter = Some(filter);
        self.path = Some(path);
        self
    }

    /// L9.ADAPT LATE-ARM — attach a not-yet-published sideband. The
    /// reader adopts its predicates mid-scan at a row-group boundary.
    /// Sets `path` (required by the masked-decode the armed filter
    /// triggers) without requiring a static filter.
    pub fn with_late_arm(
        mut self,
        sb: crate::bridge_filter_sideband::BridgeFilterSideband,
        path: std::path::PathBuf,
    ) -> Self {
        self.late_arm = Some(sb);
        if self.path.is_none() {
            self.path = Some(path);
        }
        self
    }

    pub fn with_projection(mut self, leaf_indices: Vec<usize>) -> Self {
        self.projection = Some(leaf_indices);
        self
    }

    pub fn with_row_groups(mut self, rgs: Vec<usize>) -> Self {
        self.row_groups = Some(rgs);
        self
    }

    pub fn with_batch_size(mut self, n: usize) -> Self {
        self.batch_size = n.max(1);
        self
    }

    /// Σ.E5.1.c: cap the per-RG column-decode thread count at `n`.
    ///
    /// Default behaviour (no call / `None`) is "saturate
    /// `available_parallelism()`" — every column gets a thread until
    /// core count is hit. Callers that spawn N concurrent readers (one
    /// per outer DataFusion partition) should pass `total_threads / N`
    /// to keep the global thread count anchored to the core count
    /// rather than the product, which oversubscribes the scheduler and
    /// inflates wall-clock variance on the streaming path.
    ///
    /// Setting `n = 1` forces sequential per-RG column decode and is
    /// the confirmation knob for "is per-column parallelism the
    /// dominant cost when outer parallelism already saturates cores?".
    pub fn with_parallelism_budget(mut self, n: usize) -> Self {
        self.parallelism_budget = Some(n.max(1));
        self
    }

    pub fn build(self) -> DfResult<EmatArrowBatchReader> {
        // Σ.E5.6: decode and own the metadata snapshot once. Avoids
        // re-parsing the thrift footer per column-decode call.
        let cached_md = Arc::new(CachedFileMetadata::from_file(&self.file)?);

        let md = self
            .file
            .metadata()
            .map_err(|e| ext(format!("metadata: {e}")))?;

        // Determine projection (defaults to all leaves in column order).
        let num_cols = if let Some(rg0) = md.row_groups.first() {
            rg0.columns.len()
        } else {
            0
        };
        let projection = self.projection.unwrap_or_else(|| (0..num_cols).collect());

        // Cross-check schema length with projection.
        if self.arrow_schema.fields().len() != projection.len() {
            return Err(ext(format!(
                "arrow_schema has {} fields but projection has {} columns; \
                 supply one Arrow field per projected leaf",
                self.arrow_schema.fields().len(),
                projection.len(),
            )));
        }

        // Bounds-check projection.
        for &c in &projection {
            if c >= num_cols {
                return Err(ext(format!(
                    "projection: leaf {c} out of range (num_cols = {num_cols})"
                )));
            }
        }

        // Resolve row group order.
        let total_rgs = md.row_groups.len();
        let row_groups = match self.row_groups {
            Some(rgs) => {
                for &rg in &rgs {
                    if rg >= total_rgs {
                        return Err(ext(format!(
                            "row_groups: rg {rg} out of range (total = {total_rgs})"
                        )));
                    }
                }
                rgs
            }
            None => (0..total_rgs).collect(),
        };

        // Pre-validate each projected column's Arrow target against
        // its parquet physical type — fail-fast at build() so callers
        // get a clean error before they start iterating.
        // NARROW.DEC — rewrite narrowed key leaves to their advertised
        // Int32 BEFORE validation, so the reader emits `Int32Array`
        // directly (downcast-on-read) and validation sees the final
        // (target, phys) pairs. Only an `Int64` field over a physical
        // INT64 leaf listed in `narrow_i64_leaves` is rewritten;
        // everything else — including the empty default — is untouched.
        let arrow_schema = if self.narrow_i64_leaves.is_empty() {
            self.arrow_schema
        } else {
            let mut fields: Vec<Arc<arrow_schema::Field>> = Vec::with_capacity(projection.len());
            for (proj_idx, &leaf) in projection.iter().enumerate() {
                let f = self.arrow_schema.field(proj_idx);
                if self.narrow_i64_leaves.contains(&leaf)
                    && matches!(f.data_type(), DataType::Int64)
                    && column_physical_type(&md, leaf)? == ParquetType::Int64
                {
                    fields.push(Arc::new(
                        arrow_schema::Field::new(f.name(), DataType::Int32, f.is_nullable())
                            .with_metadata(f.metadata().clone()),
                    ));
                } else {
                    fields.push(Arc::new(f.clone()));
                }
            }
            Arc::new(arrow_schema::Schema::new_with_metadata(
                fields,
                self.arrow_schema.metadata().clone(),
            ))
        };
        for (proj_idx, &leaf) in projection.iter().enumerate() {
            let phys = column_physical_type(&md, leaf)?;
            let target = arrow_schema.field(proj_idx).data_type();
            validate_type_pair(arrow_schema.field(proj_idx).name(), phys, target)?;
        }

        Ok(EmatArrowBatchReader {
            file: self.file,
            arrow_schema,
            projection,
            row_groups,
            batch_size: self.batch_size,
            parallelism_budget: self.parallelism_budget,
            cached_md,
            filter: self.filter,
            path: self.path,
            decode_cache: self.decode_cache,
            rg_decode_cache: self.rg_decode_cache,
            late_arm: self.late_arm,
            shared_cursor: self.shared_cursor,
            cur_rg_idx: 0,
            cur_rg_columns: None,
            cur_rg_filter_bitmap: None,
            cur_rg_row: 0,
            cur_rg_total: 0,
        })
    }
}

#[inline]
fn column_physical_type(
    md: &ematix_parquet_format::metadata::FileMetaData<'_>,
    leaf: usize,
) -> DfResult<ParquetType> {
    let rg0 = md
        .row_groups
        .first()
        .ok_or_else(|| ext("file has no row groups"))?;
    let cm = rg0.columns[leaf]
        .meta_data
        .as_ref()
        .ok_or_else(|| ext(format!("leaf {leaf}: missing meta_data")))?;
    Ok(cm.column_type)
}

fn validate_type_pair(name: &str, phys: ParquetType, target: &DataType) -> DfResult<()> {
    let ok = match target {
        // NARROW.DEC: an Int32 target over a physical INT64 column is
        // the downcast-on-read shape installed by
        // `with_narrow_i64_leaves` (KEYS.2 stats-proven key narrowing).
        // `decode_one_column` / `masked_decode_one_column` self-route
        // on the physical type and check-narrow, so the pair is safe.
        // Date32 must stay INT32-only — no narrowing decode exists.
        DataType::Int32 => phys == ParquetType::Int32 || phys == ParquetType::Int64,
        DataType::Date32 => phys == ParquetType::Int32,
        // KEYS.4.b: UInt64 is physically stored as INT64 (parquet's only
        // 64-bit integer width); surfaced via a zero-copy bit reinterpret.
        DataType::Int64 | DataType::UInt64 => phys == ParquetType::Int64,
        DataType::Float64 => phys == ParquetType::Double,
        DataType::Utf8 | DataType::Utf8View => phys == ParquetType::ByteArray,
        DataType::Dictionary(k, v) => {
            matches!(k.as_ref(), DataType::UInt32)
                && matches!(v.as_ref(), DataType::Utf8)
                && phys == ParquetType::ByteArray
        }
        _ => false,
    };
    if !ok {
        return Err(DataFusionError::NotImplemented(format!(
            "EmatArrowBatchReader: column `{name}`: parquet physical type {phys:?} not yet \
             supported with target Arrow type {target:?} \
             (supported: Int32/Date32←INT32, Int64/UInt64←INT64, Float64←DOUBLE, \
              Utf8/Utf8View/Dictionary(UInt32,Utf8)←BYTE_ARRAY)"
        )));
    }
    Ok(())
}

// ============================================================
// Reader
// ============================================================

/// Per-RG decoded buffers for one projected column. Each variant is a
/// fully-materialised representation of the *entire* RG for that
/// column; the reader slices these into `batch_size`-row windows
/// without re-decoding.
///
/// Σ.E5.1.c: primitive variants hold `Buffer` instead of `Vec` so that
/// per-batch slicing is zero-copy (`Buffer::slice_with_length` = Arc
/// bump + offset/length change). Previously `Buffer::from_slice_ref`
/// inside `slice_batch` did a fresh memcpy of every batch — for Q1
/// (3 numeric cols × 65K rows × 8 bytes ≈ 1.5 MB per batch × ~20
/// batches/partition × 6 partitions ≈ 200 MB of copying per query).
#[derive(Clone)]
pub(crate) enum DecodedColumn {
    /// 4-byte primitives (i32/Date32). `n_rows` is the logical row
    /// count; `data.len() == n_rows * 4`.
    Int32 { data: Buffer, n_rows: usize },
    /// 8-byte primitives (i64). `data.len() == n_rows * 8`.
    Int64 { data: Buffer, n_rows: usize },
    /// 8-byte primitives (f64). `data.len() == n_rows * 8`.
    Float64 { data: Buffer, n_rows: usize },
    /// (views as `Buffer` of `u128`, backing data blocks). The views
    /// buffer is sliced zero-copy per batch (Σ.E5.1.c); the backing
    /// data buffers are shared across every batch in the RG via Arc.
    ///
    /// Σ.E5 (2026-05-18): widened from a single `data: Buffer` to
    /// `data_buffers: Vec<Buffer>` — one buffer per decompressed
    /// page, so we can take ownership of each page's decompressed
    /// scratch directly instead of memcpy-per-row from scratch into
    /// a single accumulator. Skips ~45 MB of memory traffic per RG
    /// on the Q13-shape PLAIN byte_array decode. Views encode the
    /// `block_id` (= index into `data_buffers`) of their source page.
    StringView {
        views: Buffer,
        n_rows: usize,
        data_buffers: Vec<Buffer>,
    },
    /// Dict-preserved BYTE_ARRAY → DictionaryArray<UInt32, Utf8>
    /// values + indices. Indices are RG-local — never spans RGs.
    /// `indices` is a `Buffer` so per-batch key slicing is zero-copy
    /// (Σ.E5.1.c).
    DictUtf8 {
        values: Arc<StringArray>,
        indices: Buffer,
        n_rows: usize,
    },
    /// Slow path: materialised UTF-8 (per-row copy).
    Utf8(Arc<StringArray>),
}

impl DecodedColumn {
    fn len(&self) -> usize {
        match self {
            DecodedColumn::Int32 { n_rows, .. } => *n_rows,
            DecodedColumn::Int64 { n_rows, .. } => *n_rows,
            DecodedColumn::Float64 { n_rows, .. } => *n_rows,
            DecodedColumn::StringView { n_rows, .. } => *n_rows,
            DecodedColumn::DictUtf8 { n_rows, .. } => *n_rows,
            DecodedColumn::Utf8(s) => s.len(),
        }
    }
}

/// Σ.E5 (2026-05-19): compact a densely-decoded column down to only
/// the rows where `bitmap` bit = 1. Output row count = `popcount`.
///
/// Intended for a future high-sel dense+bitmap-apply fallback path:
/// when popcount is too high for masked decode to win, the reader
/// would dense-decode and THEN compact via this helper. The first
/// integration (calling this from the selectivity-gate fallback)
/// regressed Q03 -36% → +21% and Q21 -10% → +30% because it ran
/// alongside the FilterExec (Inexact pushdown), duplicating the
/// per-row predicate work.
///
/// Kept as dead code so it's ready when we add per-filter Exact
/// pushdown for predicate shapes we fully handle (string LIKE via
/// `LikeMatcher`, etc.) — that drops the FilterExec and unblocks the
/// compact path as a real win.
///
/// Σ.E5 Phase 1.5 (2026-05-19): SIMD-accelerated compact via Arrow's
/// `filter` kernel. Converts the DecodedColumn to an ArrayRef, runs
/// `arrow_select::filter::filter` (which uses SIMD per-type kernels),
/// and stores the result back. Same contract as
/// `compact_decoded_column` but ~3-5× faster on wide projections.
///
/// `predicate` is a precompiled `FilterPredicate` so we don't rebuild
/// the index list per column. Use `arrow_select::filter::FilterBuilder`
/// to build one from a BooleanArray once per RG.
#[allow(dead_code)]
fn compact_decoded_column_via_arrow(
    col: &DecodedColumn,
    predicate: &datafusion::arrow::compute::FilterPredicate,
    target_type: &DataType,
) -> DfResult<DecodedColumn> {
    // Convert DecodedColumn -> ArrayRef via slice_decoded with full RG.
    let n_rows = col.len();
    let array_ref = slice_decoded(col, 0, n_rows, target_type);
    let filtered = predicate
        .filter(array_ref.as_ref())
        .map_err(|e| ext(format!("arrow filter: {e}")))?;

    // Convert ArrayRef -> DecodedColumn for downstream slice_batch.
    let popcount = filtered.len();
    match (col, filtered.data_type()) {
        (DecodedColumn::Int32 { .. }, DataType::Int32 | DataType::Date32) => {
            let arr = filtered.as_any();
            let buf = if let Some(a) = arr.downcast_ref::<Int32Array>() {
                a.values().inner().clone()
            } else if let Some(a) = arr.downcast_ref::<Date32Array>() {
                a.values().inner().clone()
            } else {
                return Err(ext("compact_via_arrow: i32 downcast failed"));
            };
            Ok(DecodedColumn::Int32 {
                data: buf,
                n_rows: popcount,
            })
        }
        (DecodedColumn::Int64 { .. }, DataType::Int64) => {
            let a = filtered
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| ext("compact_via_arrow: i64 downcast failed"))?;
            Ok(DecodedColumn::Int64 {
                data: a.values().inner().clone(),
                n_rows: popcount,
            })
        }
        (DecodedColumn::Float64 { .. }, DataType::Float64) => {
            let a = filtered
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| ext("compact_via_arrow: f64 downcast failed"))?;
            Ok(DecodedColumn::Float64 {
                data: a.values().inner().clone(),
                n_rows: popcount,
            })
        }
        (DecodedColumn::StringView { .. }, DataType::Utf8View) => {
            let a = filtered
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| ext("compact_via_arrow: StringView downcast failed"))?;
            Ok(DecodedColumn::StringView {
                views: a.views().inner().clone(),
                n_rows: popcount,
                data_buffers: a.data_buffers().to_vec(),
            })
        }
        (DecodedColumn::DictUtf8 { values, .. }, DataType::Dictionary(_, _)) => {
            // Dict array's keys are filtered; values stay shared.
            let a = filtered
                .as_any()
                .downcast_ref::<DictionaryArray<UInt32Type>>()
                .ok_or_else(|| ext("compact_via_arrow: Dictionary<U32,Utf8> downcast failed"))?;
            Ok(DecodedColumn::DictUtf8 {
                values: values.clone(),
                indices: a.keys().values().inner().clone(),
                n_rows: popcount,
            })
        }
        (DecodedColumn::Utf8(_), _) => {
            // Slow path — keep as Utf8.
            let a = filtered
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| ext("compact_via_arrow: Utf8 downcast failed"))?;
            Ok(DecodedColumn::Utf8(Arc::new(a.clone())))
        }
        (other, dt) => Err(ext(format!(
            "compact_via_arrow: type mismatch — DecodedColumn {:?} got Arrow {dt:?}",
            std::mem::discriminant(other)
        ))),
    }
}

/// Σ.E5 (2026-05-19): compact a densely-decoded column down to only
/// the rows where `bitmap` bit = 1. Output row count = `popcount`.
///
/// Intended for a future high-sel dense+bitmap-apply fallback path:
/// when popcount is too high for masked decode to win, the reader
/// would dense-decode and THEN compact via this helper. The first
/// integration (calling this from the selectivity-gate fallback)
/// regressed Q03 -36% → +21% and Q21 -10% → +30% because it ran
/// alongside the FilterExec (Inexact pushdown), duplicating the
/// per-row predicate work.
///
/// Kept as dead code so it's ready when we add per-filter Exact
/// pushdown for predicate shapes we fully handle (string LIKE via
/// `LikeMatcher`, etc.) — that drops the FilterExec and unblocks the
/// compact path as a real win.
#[allow(dead_code)]
fn compact_decoded_column(col: &DecodedColumn, bitmap: &[u8], popcount: usize) -> DecodedColumn {
    match col {
        DecodedColumn::Int32 { data, n_rows } => {
            let src = unsafe { std::slice::from_raw_parts(data.as_ptr() as *const i32, *n_rows) };
            let mut out: Vec<i32> = Vec::with_capacity(popcount);
            for row in 0..*n_rows {
                if (bitmap[row >> 3] >> (row & 7)) & 1 == 1 {
                    out.push(src[row]);
                }
            }
            DecodedColumn::Int32 {
                data: Buffer::from_vec(out),
                n_rows: popcount,
            }
        }
        DecodedColumn::Int64 { data, n_rows } => {
            let src = unsafe { std::slice::from_raw_parts(data.as_ptr() as *const i64, *n_rows) };
            let mut out: Vec<i64> = Vec::with_capacity(popcount);
            for row in 0..*n_rows {
                if (bitmap[row >> 3] >> (row & 7)) & 1 == 1 {
                    out.push(src[row]);
                }
            }
            DecodedColumn::Int64 {
                data: Buffer::from_vec(out),
                n_rows: popcount,
            }
        }
        DecodedColumn::Float64 { data, n_rows } => {
            let src = unsafe { std::slice::from_raw_parts(data.as_ptr() as *const f64, *n_rows) };
            let mut out: Vec<f64> = Vec::with_capacity(popcount);
            for row in 0..*n_rows {
                if (bitmap[row >> 3] >> (row & 7)) & 1 == 1 {
                    out.push(src[row]);
                }
            }
            DecodedColumn::Float64 {
                data: Buffer::from_vec(out),
                n_rows: popcount,
            }
        }
        DecodedColumn::StringView {
            views,
            n_rows,
            data_buffers,
        } => {
            let src = unsafe { std::slice::from_raw_parts(views.as_ptr() as *const u128, *n_rows) };
            let mut out: Vec<u128> = Vec::with_capacity(popcount);
            for row in 0..*n_rows {
                if (bitmap[row >> 3] >> (row & 7)) & 1 == 1 {
                    out.push(src[row]);
                }
            }
            DecodedColumn::StringView {
                views: Buffer::from_vec(out),
                n_rows: popcount,
                data_buffers: data_buffers.clone(),
            }
        }
        DecodedColumn::DictUtf8 {
            values,
            indices,
            n_rows,
        } => {
            let src =
                unsafe { std::slice::from_raw_parts(indices.as_ptr() as *const u32, *n_rows) };
            let mut out: Vec<u32> = Vec::with_capacity(popcount);
            for row in 0..*n_rows {
                if (bitmap[row >> 3] >> (row & 7)) & 1 == 1 {
                    out.push(src[row]);
                }
            }
            DecodedColumn::DictUtf8 {
                values: values.clone(),
                indices: Buffer::from_vec(out),
                n_rows: popcount,
            }
        }
        DecodedColumn::Utf8(s) => {
            // Slow path — build a new StringArray with only matching
            // rows. Rarely hit (Utf8 is the slow fallback).
            let mut b =
                arrow_array::builder::StringBuilder::with_capacity(popcount, s.value_data().len());
            for row in 0..s.len() {
                if (bitmap[row >> 3] >> (row & 7)) & 1 == 1 {
                    b.append_value(s.value(row));
                }
            }
            DecodedColumn::Utf8(Arc::new(b.finish()))
        }
    }
}

pub struct EmatArrowBatchReader {
    file: ParquetFile,
    arrow_schema: SchemaRef,
    /// Leaf indices into the parquet column order. `projection[i]`
    /// gives the parquet leaf for the i-th Arrow field.
    projection: Vec<usize>,
    /// Row groups to scan, in order.
    row_groups: Vec<usize>,
    batch_size: usize,
    /// Σ.E5.1.c: caller-supplied cap on per-RG column-decode threads.
    /// See [`EmatArrowBatchReaderBuilder::with_parallelism_budget`].
    parallelism_budget: Option<usize>,
    /// Σ.E5.6: cached parquet metadata. Decoded once at builder
    /// time; shared (Arc) across all scoped column-decode threads.
    /// Profile-driven: ~10% of Q19 CPU was re-parsing the thrift
    /// footer on every `decode_one_column` call.
    cached_md: Arc<CachedFileMetadata>,
    /// Σ.E5 (#516): late-mat filter. When set, `load_row_group`
    /// branches into the masked-decode path.
    filter: Option<BridgeFilter>,
    /// File path. Set when `filter` is set — needed for the
    /// path-based `filter_i32_column_to_bitmap` kernel.
    path: Option<std::path::PathBuf>,
    /// Σ.O.b — process-shared decode cache. When present, hot-path
    /// hooks consult before decoding a row group and insert after.
    /// Bypassed entirely when `filter` is set (filter outputs are
    /// row-mask-specific and not safely shareable across queries
    /// with different masks). See `Σ.O.c` follow-up for the load_
    /// row_group integration.
    pub(crate) decode_cache:
        Option<std::sync::Arc<crate::parquet_decode_cache::ParquetDecodeCache>>,
    /// Σ.O.c.1 — private decode-column cache. Lookup at the top of
    /// `load_row_group_dense`; on hit, restores `cur_rg_columns` from
    /// the shared Arc<Vec<DecodedColumn>> with no parquet I/O. On miss,
    /// decodes as usual and inserts. Bypassed when `filter` is set.
    pub(crate) rg_decode_cache: Option<std::sync::Arc<RowGroupDecodeCache>>,
    /// Morsel-engine P2 spike: shared work queue. When `Some`, `next()`
    /// pulls the next RG from here (dynamic work-stealing) instead of
    /// walking the static `row_groups`. See [`SharedRgCursor`].
    shared_cursor: Option<std::sync::Arc<SharedRgCursor>>,

    // ---- iteration state ----
    /// L9.ADAPT LATE-ARM — pending sideband; see the builder field.
    /// `Some` until the publish lands (then merged into `filter` and
    /// taken), or for the whole scan if the build never publishes.
    late_arm: Option<crate::bridge_filter_sideband::BridgeFilterSideband>,
    /// Index into `row_groups`; `cur_rg_idx == row_groups.len()`
    /// signals end-of-stream.
    cur_rg_idx: usize,
    /// Per-projected-column decoded buffers for the current RG, or
    /// `None` before the first batch / after the RG is exhausted.
    cur_rg_columns: Option<Vec<DecodedColumn>>,
    /// Σ.E5 Phase 1.6: row bitmap from the late-mat selectivity gate
    /// fallback. When present, `slice_batch` filters each emitted
    /// batch via Arrow's SIMD `filter` kernel after the zero-copy
    /// slice of `cur_rg_columns`. Cleared on RG boundary by
    /// `load_row_group` / `load_row_group_dense`.
    /// Σ.AE.3 (2026-05-26): stored as Arc-shared `Buffer` (not raw
    /// `Vec<u8>`) so `slice_batch` clones it cheaply on each batch
    /// rather than copying ~125 KB via `Buffer::from_slice_ref` per
    /// 65k-row batch (the Vec<u8> form regressed Q03 SF=10 +5% Exact
    /// mode through cumulative per-batch buffer allocation).
    cur_rg_filter_bitmap: Option<datafusion::arrow::buffer::Buffer>,
    /// Next row index within the current RG.
    cur_rg_row: usize,
    /// Total rows in the current RG.
    cur_rg_total: usize,
}

impl EmatArrowBatchReader {
    pub fn schema(&self) -> &SchemaRef {
        &self.arrow_schema
    }

    /// Σ.O.b — read-only accessor for the installed decode cache.
    /// Returns None if no cache was installed via the builder.
    pub fn decode_cache(
        &self,
    ) -> Option<&std::sync::Arc<crate::parquet_decode_cache::ParquetDecodeCache>> {
        self.decode_cache.as_ref()
    }

    /// Σ.O.b — compute the cache key for the row group at the given
    /// index. Used by Σ.O.c's load_row_group integration. Exposed
    /// for tests + by-hand integration probing.
    pub fn cache_key_for_row_group(
        &self,
        rg_idx: usize,
    ) -> Option<crate::parquet_decode_cache::DecodeCacheKey> {
        let path = self.path.as_ref()?;
        let path_str = path.to_string_lossy().into_owned();
        // Build projection fingerprint from leaf-index list (stable
        // across runs with same schema + projection).
        let proj_names: Vec<String> = self
            .arrow_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let proj_refs: Vec<&str> = proj_names.iter().map(|s| s.as_str()).collect();
        Some(crate::parquet_decode_cache::DecodeCacheKey::new(
            path_str,
            rg_idx as u32,
            &proj_refs,
        ))
    }

    /// Decode every projected column of `rg` into `cur_rg_columns`.
    ///
    /// Parallel-decode shape (Σ.E5.1 follow-up):
    ///
    /// Each projected column's decode is independent — distinct
    /// `PageWalker` over a disjoint byte range from a shared
    /// `ParquetFile` whose `read_range` is lock-free (`pread(2)` on
    /// Unix, positional `ReadFile` on Windows). `ParquetFile` is `Sync`
    /// — borrowing it across `std::thread::scope` works without
    /// re-opening per thread. The `Arc<Schema>` is also `Sync` so each
    /// thread can index its own `target` `DataType`.
    ///
    /// Threadpool sizing: we spawn `min(num_projected_columns,
    /// available_parallelism())` threads. For the Q1 shape (7 columns
    /// on a 14-core machine) every column gets its own thread; for a
    /// 200-column wide table the spawn count is bounded by core count
    /// and threads pull from a shared work queue.
    ///
    /// Memory note: doing N columns concurrently raises per-RG peak
    /// memory by ~Nx vs the sequential path. At SF=1 a numeric RG
    /// column is ~8 MB and 7 concurrent columns peak at ~50-100 MB —
    /// well inside budget. At SF=10+ this is worth revisiting (chunked
    /// or page-streaming decode is the next lever).
    /// Σ.E5 (#516): late-mat variant of `load_row_group`. Decodes the
    /// filter column first to build a row bitmap, then masked-decodes
    /// each projected column in parallel. Sets `cur_rg_total` to the
    /// bitmap popcount so the subsequent batch slicing emits only
    /// surviving rows. Pages whose bitmap-popcount is zero are skipped
    /// inside `masked_decode_*`.
    fn load_row_group_masked(
        &mut self,
        rg: usize,
        filter: BridgeFilter,
        path: std::path::PathBuf,
    ) -> DfResult<()> {
        // Σ.AH.2 Story 1'.4 Stage 1 (2026-05-26): fused bloom-probe
        // path. For runtime-injected i64-only filters (I64InBloom,
        // I64InSet, I64Range from the L9 sideband), the filter
        // column is ALWAYS in the projection (it's the HashJoin's
        // join key, which the projection above us already selects).
        // So we can:
        //   1. Dense-decode all projected columns (existing path)
        //   2. Walk the already-decoded i64 buffer for the filter
        //      column to build the bitmap (no second decode)
        //   3. Stash the bitmap; slice_batch applies it per-batch
        //      via Arrow's SIMD filter (existing Σ.AE.2 hook).
        //
        // Saves: one full decode + one Snappy decompress of the
        // filter column (~6 ms/partition on SF=10 lineitem.l_partkey;
        // saving ~25-30 ms wall after parallel-overlap).
        //
        // **Default ON** as of 2026-05-27 (Σ.AI.2). Two independent
        // interleaved A/B/A/B strict benches (22q SF=10, 4 invocations
        // each side) showed net **-1.26% and -2.27% faster** with the
        // fused-probe path, driven primarily by Q21 (-40 to -54 ms
        // / -10 to -14%). No clear regressions above the 2σ noise bar
        // in either run (Q10 +3.6% appeared in one run but not the
        // other; treated as edge-of-noise). Earlier "loose" benches
        // (single-invocation, no caffeinate/taskpolicy discipline)
        // reported net regressions that turned out to be sequential-
        // block-order artifact — see `[[sigma-ai-1-strict-bench-landed]]`
        // and the interleaved A/B harness at `scripts/bench/strict_ab.sh`.
        //
        // Opt-OUT via `EMAT_L9_FUSED_PROBE=0` (or `false`) for ad-hoc
        // A/B work or if a future workload exposes a real regression.
        let fused_probe_enabled = std::env::var("EMAT_L9_FUSED_PROBE")
            .ok()
            .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
            .unwrap_or(true);
        // L9.PROBEORDER (2026-06-10): the fused arm now covers ANY
        // filter that carries a runtime membership probe (set/bloom)
        // — including ones bundled with static predicates (Q20's
        // shipdate range + forest-partkey set; Q08's p_type + part
        // set). `eval_on_decoded_views` evaluates statics first and
        // masks the probe to survivors, so the expensive per-row
        // probe no longer runs on rows a 2-comparison range already
        // killed (Q20 SF=100: 600M probes → 91M). The previous
        // runtime-i64-ONLY gate sent the bundled shape to the legacy
        // path, which re-decodes every predicate column from the file
        // on top of the dense decode. Static-only pushdowns (Q12/Q19
        // user filters, no probe) keep the legacy masked path — at
        // their ≤4.3% pass rates the masked gather beats dense
        // materialize (REV.23).
        if fused_probe_enabled && (filter.is_runtime_i64_only() || filter.has_runtime_probe()) {
            self.cur_rg_total = self.cached_md.row_groups[rg].num_rows as usize;
            self.load_row_group_dense(rg)?;
            let cols = self.cur_rg_columns.as_ref().ok_or_else(|| {
                ext("load_row_group_dense returned without cur_rg_columns".to_string())
            })?;
            // Predicate col_idx values are FILE-schema indices; map
            // each through self.projection into the decoded set. Any
            // unresolvable column (not projected / string repr) makes
            // eval return None → legacy fallback below.
            let projection = &self.projection;
            let eval = filter.eval_on_decoded_views(|file_col| {
                let proj_idx = projection.iter().position(|&l| l == file_col)?;
                match &cols[proj_idx] {
                    DecodedColumn::Int64 { data, .. } => Some(
                        crate::ematix_fast_parquet::DecodedView::I64(data.typed_data::<i64>()),
                    ),
                    DecodedColumn::Int32 { data, .. } => Some(
                        crate::ematix_fast_parquet::DecodedView::I32(data.typed_data::<i32>()),
                    ),
                    DecodedColumn::Float64 { data, .. } => Some(
                        crate::ematix_fast_parquet::DecodedView::F64(data.typed_data::<f64>()),
                    ),
                    _ => None,
                }
            });
            if let Some((bitmap, total)) = eval {
                // Route by ACTUAL popcount, not a plan-time estimate:
                // stash the bitmap (per-batch Arrow gather) only when
                // the surviving fraction is low; above the threshold,
                // DISCARD it and emit dense — the downstream join /
                // FilterExec re-applies every one of these inexact-by-
                // design predicates, so discarding is always correct
                // and the only sunk cost is the (statics-masked,
                // cheap) probe. Measured SF=100: Q10 2676 vs 2836 and
                // Q18 3019 vs 3304 with always-stash — the ~4-15%-pass
                // bloom gathers of 24-91M rows cost more than the
                // relieved probes. (An always-stash arm was briefly
                // adopted off a Q05 +9% reading that turned out to be
                // post-build thermal drift — Q05 carries no wrap at
                // all; the trace shows every candidate gate-rejected.)
                // Same REV.23 threshold: gather wins ≤4.3%, dense wins
                // ≥15%, 0.10 sits in the gap.
                let popcount: usize = bitmap.iter().map(|b| b.count_ones() as usize).sum();
                // Σ.Q05.CHAIN — a chain-intermediate filter's bitmap
                // must APPLY regardless of the pass-rate routing: the
                // next chain link samples this scan's output, and
                // intermediate targets are dim-sized (per-batch filter
                // is negligible). Everything else keeps REV.23.
                if filter.apply_when_dense()
                    || total == 0
                    || (popcount as f64 / total as f64) <= masked_dense_passrate_threshold()
                {
                    self.cur_rg_filter_bitmap =
                        Some(datafusion::arrow::buffer::Buffer::from_vec(bitmap));
                }
                return Ok(());
            }
            // Some predicate column isn't decodable from the dense
            // projection (string predicate, col not projected). Drop
            // the dense decode and use the legacy bitmap path.
            self.cur_rg_columns = None;
            return self.load_row_group_masked_legacy(rg, filter, path);
        }
        self.load_row_group_masked_legacy(rg, filter, path)
    }

    /// Σ.AH.2 Story 1'.4 — original masked path, kept for the
    /// non-fused dispatch (user-pushed filters, opt-out).
    fn load_row_group_masked_legacy(
        &mut self,
        rg: usize,
        filter: BridgeFilter,
        path: std::path::PathBuf,
    ) -> DfResult<()> {
        // Σ.Q.L13 (2026-05-23): parallel-bitmap+dense path is opt-IN
        // via `EMAT_FORCE_PARALLEL_BITMAP=1`. The path was previously
        // default-ON when `predicted_pass_rate > 0.33`, citing an SF=1
        // 22q geomean win (0.89 → 0.856), but the Σ.Q.L13 scan-only
        // A/B at SF=10 showed catastrophic regression on date-filter
        // workloads — T2 (lineitem + l_shipdate BETWEEN) ran at 7318ms
        // vs 168ms with this path disabled (43× regression). The
        // earlier SF=1 win likely fell inside [[optimizer-codegen-sensitivity]]
        // noise. Default-off restores T2/T3 parity with DataFusion's
        // native parquet reader (both ~1.45× DuckDB on scan-only).
        // Opt-in via EMAT_FORCE_PARALLEL_BITMAP=1 for cases where the
        // work-stealing parallel decode is empirically faster.
        let force_parallel = std::env::var_os("EMAT_FORCE_PARALLEL_BITMAP").is_some();
        if force_parallel && filter.predicted_pass_rate() > 0.33 {
            return self.load_row_group_parallel_bitmap_dense(rg, filter, path);
        }

        // 1. Build the combined multi-column AND bitmap.
        let (bitmap, total) = filter.build_bitmap(&path, rg)?;
        let popcount: usize = bitmap.iter().map(|b| b.count_ones() as usize).sum();

        // Σ.E5 #517-518: selectivity gate. Masked decode is a win
        // only when the filter is selective enough that the skipped
        // decode work outweighs the bitmap-construction + per-row
        // gather overhead. Phase 1.8 may misfire (stats inaccurate)
        // so keep the actual-popcount fallback as a safety net.
        //
        // REV.23 (2026-05-31): threshold LOWERED 1/3 → 1/10. The
        // per-column masked gather is memory-LATENCY-bound (strided
        // scatter of survivors) and plateaus at ~7 cores; dense decode
        // is bandwidth-bound and scales to all cores. For an
        // UNCLUSTERED filter (e.g. l_shipdate, uniform across pages)
        // masked skips zero pages — it decompresses everything dense
        // does, then adds the non-scaling gather, so it is pure
        // overhead. Calibrated on TPC-H SF=10: routing the masked scan
        // to dense is a clear win at every pass rate ≥15% (Q07 30% −25%,
        // Q08 30%, Q10 25%, Q20/Q16 15%) and a loss only at ≤4.3%
        // (Q12 4.3% +49%, Q19 3.6% +23%). A clean gap in (0.043, 0.148);
        // 0.10 sits in it. Env-tunable via EMAT_MASKED_DENSE_PASSRATE.
        // See [[rev20-q07-q08-decode-bound]] REV.23.
        //
        // Σ.AE.2 (2026-05-26): under Exact pushdown, FilterExec is
        // dropped from the plan, so the BridgeFilter is the ONLY
        // row filter. If we fall back to dense here without saving
        // the bitmap, rows that should be filtered out are emitted.
        // Q01 SF=10 leaked ~2.9% rows on boundary RGs (l_shipdate
        // spans the cutoff); Q03 leaked ~26× rows (two filtered
        // tables × Cartesian explosion). Stash the bitmap before
        // the dense decode so slice_batch applies it per-batch via
        // Arrow's SIMD filter (same shape as
        // load_row_group_parallel_bitmap_dense at line 1355).
        //
        // Gate on EMAT_EXACT_PUSHDOWN: under Inexact (default),
        // FilterExec is still in the plan and the per-batch SIMD
        // filter is redundant work (~+9% Q01 SF=1 baseline regression
        // when ungated). With the gate the default-mode path is
        // bit-identical to pre-fix behavior; only Exact mode (which
        // would otherwise emit wrong rows) gains the bitmap apply.
        if should_route_masked_to_dense(popcount, total, masked_dense_passrate_threshold()) {
            self.cur_rg_total = self.cached_md.row_groups[rg].num_rows as usize;
            self.load_row_group_dense(rg)?;
            // Σ.Q05.CHAIN — chain-intermediate filters must apply even
            // when routed dense (see site above); Σ.AE.2 Exact mode
            // stashes for correctness as before.
            if filter.apply_when_dense() || std::env::var_os("EMAT_EXACT_PUSHDOWN").is_some() {
                self.cur_rg_filter_bitmap =
                    Some(datafusion::arrow::buffer::Buffer::from_vec(bitmap));
            }
            return Ok(());
        }

        // 2. Parallel masked-decode of each projected column. Same
        //    scoped-thread shape as the dense path — distinct
        //    PageWalker per thread over shared ParquetFile (Sync).
        let projection = &self.projection;
        let schema = &self.arrow_schema;
        let file = &self.file;
        let cached_md = &self.cached_md;
        let n_cols = projection.len();
        let cap = self.parallelism_budget.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or(1)
        });
        let max_threads = cap.max(1).min(n_cols.max(1));

        let cols: Vec<DecodedColumn> = if max_threads <= 1 || n_cols <= 1 {
            let mut out = Vec::with_capacity(n_cols);
            for (proj_idx, &leaf) in projection.iter().enumerate() {
                let target = schema.field(proj_idx).data_type();
                let phys = cached_md.row_groups[rg].columns[leaf].column_type;
                out.push(masked_decode_one_column(
                    file, rg, leaf, &bitmap, popcount, target, phys,
                )?);
            }
            out
        } else {
            use std::sync::atomic::{AtomicUsize, Ordering};
            let next = AtomicUsize::new(0);
            let bitmap_ref = &bitmap;
            let merged: Vec<(usize, DfResult<DecodedColumn>)> = std::thread::scope(|s| {
                let mut handles = Vec::with_capacity(max_threads);
                for _ in 0..max_threads {
                    let next = &next;
                    handles.push(s.spawn(move || -> Vec<(usize, DfResult<DecodedColumn>)> {
                        let mut local: Vec<(usize, DfResult<DecodedColumn>)> = Vec::new();
                        loop {
                            let i = next.fetch_add(1, Ordering::Relaxed);
                            if i >= n_cols {
                                break;
                            }
                            let leaf = projection[i];
                            let target = schema.field(i).data_type();
                            let phys = cached_md.row_groups[rg].columns[leaf].column_type;
                            let r = masked_decode_one_column(
                                file, rg, leaf, bitmap_ref, popcount, target, phys,
                            );
                            local.push((i, r));
                        }
                        local
                    }));
                }
                let mut all = Vec::with_capacity(n_cols);
                for h in handles {
                    all.extend(h.join().expect("masked decode thread panic"));
                }
                all
            });

            let mut slots: Vec<Option<DfResult<DecodedColumn>>> =
                (0..n_cols).map(|_| None).collect();
            for (i, r) in merged {
                slots[i] = Some(r);
            }
            let mut out = Vec::with_capacity(n_cols);
            for (i, slot) in slots.into_iter().enumerate() {
                let r =
                    slot.ok_or_else(|| ext(format!("column {i} masked-decode slot never filled")))?;
                out.push(r?);
            }
            out
        };

        for (i, c) in cols.iter().enumerate() {
            if c.len() != popcount {
                return Err(ext(format!(
                    "RG {rg} masked: column {i} (leaf {}) decoded {} rows but bitmap popcount = {popcount}",
                    self.projection[i],
                    c.len(),
                )));
            }
        }

        self.cur_rg_total = popcount;
        self.cur_rg_columns = Some(cols);
        self.cur_rg_row = 0;
        Ok(())
    }

    fn load_row_group(&mut self, rg: usize) -> DfResult<()> {
        // Σ.AI.6d — decode-pressure gate (opt-in `EMAT_DECODE_SHED=1`).
        // The eager reader decodes the WHOLE row group synchronously
        // inside this call, so the permit brackets exactly one RG's
        // decode-buffer lifetime; it drops before `slice_batch`/send,
        // so a decode slot is never held while blocked on a downstream
        // consumer (no cross-scan wait cycles). Disabled (default) this
        // is a single OnceLock load.
        let _shed_permit = crate::mem_pressure::decode_gate_enter();
        // L9.ADAPT LATE-ARM — adopt a tight-rescued wrap's published
        // predicates at the row-group boundary. One `is_ready()` read
        // per RG until the build lands; arming merges the predicates
        // into `self.filter` (attaching the shared Guard-2 disarm
        // counters) so this and all later RGs filter. Row groups
        // decoded before the publish simply flow unfiltered — the
        // join re-applies every inexact-by-design predicate, so
        // mid-scan adoption is correct by construction.
        if let Some(sb) = &self.late_arm {
            if sb.is_ready() {
                if let Some(extras) = sb.peek() {
                    if !extras.is_empty() {
                        let mut bf = self
                            .filter
                            .take()
                            .unwrap_or_else(|| BridgeFilter::new(Vec::new()));
                        bf.extend(extras);
                        if sb.tight_admitted() {
                            bf.set_probe_disarm(sb.probe_disarm_handle());
                        }
                        self.filter = Some(bf);
                    }
                }
                self.late_arm = None;
            }
        }
        // Σ.E5.6: use the cached metadata snapshot — no thrift re-parse.
        self.cur_rg_total = self.cached_md.row_groups[rg].num_rows as usize;
        // Σ.E5 Phase 1.6: clear any per-RG filter bitmap left from a
        // previous RG's selectivity-gate fallback.
        self.cur_rg_filter_bitmap = None;

        // Morsel-engine P1: time the whole per-RG decode (the single
        // decode work-unit) regardless of which sub-path fires, so the
        // trace reconstructs a per-core busy/idle timeline. No-op unless
        // EMAT_MORSEL_TRACE=1.
        let _span = crate::morsel_trace::start_span();

        // Σ.E5 (#516): late-mat path — when a filter is set, decode the
        // filter column to a bitmap, then masked-decode each projected
        // column. Pages with zero bitmap-popcount are skipped entirely.
        let masked = match (&self.filter, &self.path) {
            (Some(filter), Some(path)) => Some((filter.clone(), path.clone())),
            _ => None,
        };
        let res = match masked {
            Some((filter, path)) => self.load_row_group_masked(rg, filter, path),
            None => self.load_row_group_dense(rg),
        };
        crate::morsel_trace::end_span(_span, rg, self.cur_rg_total, self.projection.len());
        res
    }

    /// Σ.E5 Phase 1.8: parallel bitmap+dense path. Spawns one thread
    /// for `filter.build_bitmap` alongside the existing parallel-
    /// projection decode pool. Stores both `cur_rg_columns` and
    /// `cur_rg_filter_bitmap` so `slice_batch` applies the bitmap
    /// per-batch via Arrow's SIMD filter.
    ///
    /// Reused across-the-board thread budget: of `cap` threads
    /// available, 1 goes to bitmap, `cap - 1` to projection cols
    /// (with at least 1 minimum). Avoids the +1 oversubscription
    /// that broke Phase 1.7.
    fn load_row_group_parallel_bitmap_dense(
        &mut self,
        rg: usize,
        filter: BridgeFilter,
        path: std::path::PathBuf,
    ) -> DfResult<()> {
        // Σ.E5 timing probe (`EMAT_TIMING=1`) — per-RG wall times for
        // bitmap thread vs projection-thread max vs scope total. Helps
        // diagnose why the parallel path doesn't beat no-pushdown on
        // Q01-shape queries.
        let timing = std::env::var_os("EMAT_TIMING").is_some();
        let t_fn_start = if timing {
            Some(std::time::Instant::now())
        } else {
            None
        };

        let total_rows = self.cached_md.row_groups[rg].num_rows as usize;
        let projection = &self.projection;
        let schema = &self.arrow_schema;
        let file = &self.file;
        let cached_md = &self.cached_md;
        let n_cols = projection.len();
        let cap = self.parallelism_budget.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or(1)
        });
        // Σ.E5 Phase 1.8 (post-profile): use a unified work-stealing
        // pool of `cap` threads across (n_cols projection + 1 bitmap)
        // tasks. The earlier "1 dedicated bitmap thread + cap-1
        // projection threads" structure starved projection on small
        // caps (Q01: cap=2 → 1 projection thread → 28 ms wall while
        // the bitmap thread idled after its 1 ms task). Treating
        // bitmap as just another task lets the bitmap-doing thread
        // pick up a projection col next.
        let total_threads = cap.max(1).min(n_cols + 1);

        use std::sync::atomic::{AtomicUsize, Ordering};
        // Task indices: 0..n_cols = projection col i, n_cols = bitmap.
        let next = AtomicUsize::new(0);
        let filter_ref = &filter;
        let path_ref = &path;

        let t_scope_start = if timing {
            Some(std::time::Instant::now())
        } else {
            None
        };
        let mut bitmap_ms: f64 = 0.0;
        let mut proj_max_ms: f64 = 0.0;
        let bitmap_ms_ref = &mut bitmap_ms;
        let proj_max_ms_ref = &mut proj_max_ms;
        #[allow(clippy::type_complexity)]
        let bitmap_slot: std::sync::Mutex<Option<DfResult<(Vec<u8>, usize)>>> =
            std::sync::Mutex::new(None);
        let bitmap_slot_ref = &bitmap_slot;

        let projection_results: Vec<(usize, DfResult<DecodedColumn>)> = std::thread::scope(|s| {
            let mut handles = Vec::with_capacity(total_threads);
            let n_tasks = n_cols + 1;
            for _ in 0..total_threads {
                let next = &next;
                handles.push(s.spawn(
                    move || -> (Vec<(usize, DfResult<DecodedColumn>)>, f64, f64) {
                        let t_outer = std::time::Instant::now();
                        let mut local: Vec<(usize, DfResult<DecodedColumn>)> = Vec::new();
                        let mut chunk_buf: Vec<u8> = Vec::new();
                        let mut bitmap_self_ms: f64 = 0.0;
                        loop {
                            let i = next.fetch_add(1, Ordering::Relaxed);
                            if i >= n_tasks {
                                break;
                            }
                            if i == n_cols {
                                // Bitmap task — lives in the same
                                // pool. Whichever thread grabs it
                                // will then drop back to picking
                                // up projection cols.
                                let t_bm = std::time::Instant::now();
                                let r = filter_ref.build_bitmap(path_ref, rg);
                                bitmap_self_ms = t_bm.elapsed().as_secs_f64() * 1000.0;
                                *bitmap_slot_ref.lock().unwrap() = Some(r);
                            } else {
                                let leaf = projection[i];
                                let target = schema.field(i).data_type();
                                local.push((
                                    i,
                                    decode_one_column(
                                        file,
                                        cached_md,
                                        &mut chunk_buf,
                                        rg,
                                        leaf,
                                        target,
                                    ),
                                ));
                            }
                        }
                        let outer_ms = t_outer.elapsed().as_secs_f64() * 1000.0;
                        (local, outer_ms, bitmap_self_ms)
                    },
                ));
            }
            let mut all = Vec::with_capacity(n_cols);
            for h in handles {
                let (partial, outer_ms, bitmap_self_ms) = h.join().expect("decode thread panicked");
                if timing {
                    if outer_ms > *proj_max_ms_ref {
                        *proj_max_ms_ref = outer_ms;
                    }
                    if bitmap_self_ms > 0.0 {
                        *bitmap_ms_ref = bitmap_self_ms;
                    }
                }
                all.extend(partial);
            }
            all
        });

        let bitmap_res = bitmap_slot
            .into_inner()
            .map_err(|e| ext(format!("bitmap slot poisoned: {e}")))?
            .ok_or_else(|| ext("bitmap task never ran"))?;

        let scope_ms = t_scope_start
            .map(|t| t.elapsed().as_secs_f64() * 1000.0)
            .unwrap_or(0.0);

        let (bitmap, total) = bitmap_res?;
        if total != total_rows {
            return Err(ext(format!(
                "Phase 1.8: bitmap total {total} != RG rows {total_rows}"
            )));
        }
        let mut slots: Vec<Option<DfResult<DecodedColumn>>> = (0..n_cols).map(|_| None).collect();
        for (i, r) in projection_results {
            slots[i] = Some(r);
        }
        let mut cols = Vec::with_capacity(n_cols);
        for (i, slot) in slots.into_iter().enumerate() {
            let r = slot.ok_or_else(|| ext(format!("column {i} decode slot never filled")))?;
            cols.push(r?);
        }
        for (i, c) in cols.iter().enumerate() {
            if c.len() != total {
                return Err(ext(format!(
                    "RG {rg}: column {i} (leaf {}) decoded {} rows but RG declares {total}",
                    projection[i],
                    c.len(),
                )));
            }
        }

        self.cur_rg_total = total;
        self.cur_rg_columns = Some(cols);
        self.cur_rg_filter_bitmap = Some(datafusion::arrow::buffer::Buffer::from_vec(bitmap));
        self.cur_rg_row = 0;

        if timing {
            let total_fn_ms = t_fn_start
                .map(|t| t.elapsed().as_secs_f64() * 1000.0)
                .unwrap_or(0.0);
            eprintln!(
                "[emat.parallel] rg={rg} n_cols={n_cols} pool={total_threads} \
                 bitmap={bitmap_ms:.2}ms proj_max={proj_max_ms:.2}ms scope={scope_ms:.2}ms \
                 fn_total={total_fn_ms:.2}ms"
            );
        }
        Ok(())
    }

    fn load_row_group_dense(&mut self, rg: usize) -> DfResult<()> {
        // Restore total to the RG's actual row count in case a masked
        // call fell back here mid-execution.
        self.cur_rg_total = self.cached_md.row_groups[rg].num_rows as usize;

        // Σ.Q.L6′ — per-column cache. Each projected leaf has its own
        // entry: two scans of the same RG with overlapping but not
        // identical projections share decoded columns (Q17: scan A
        // wants [1,4,5], scan B wants [1,4] → 1 and 4 reuse, only 5
        // is decoded once).
        //
        // Stage 1: probe cache, fill `cached_cols[i]` for hits, push i
        // onto `miss_indices` for misses.
        let projection = &self.projection;
        let schema = &self.arrow_schema;
        let file = &self.file;
        let cached_md = &self.cached_md;
        let n_cols = projection.len();
        // NARROW.DEC — `RgCacheKey` is type-blind (path/rg/leaf), so a
        // narrow-decoded Int32 column must never be shared with a wide
        // (Int64-schema) consumer of the same leaf or vice versa.
        // Narrowed leaves bypass the cache entirely (get AND insert).
        let is_narrowed = |i: usize, leaf: usize| -> bool {
            matches!(schema.field(i).data_type(), DataType::Int32)
                && cached_md.row_groups[rg].columns[leaf].column_type == ParquetType::Int64
        };
        let mut cached_cols: Vec<Option<DecodedColumn>> = vec![None; n_cols];
        let mut miss_indices: Vec<usize> = Vec::new();
        if let (Some(cache), Some(path)) = (self.rg_decode_cache.as_ref(), self.path.as_ref()) {
            for (i, &leaf) in projection.iter().enumerate() {
                if is_narrowed(i, leaf) {
                    miss_indices.push(i);
                    continue;
                }
                let key = RgCacheKey {
                    file_path: path.clone(),
                    row_group_idx: rg,
                    leaf_idx: leaf,
                };
                if let Some(arc) = cache.get(&key) {
                    cached_cols[i] = Some((*arc).clone());
                } else {
                    miss_indices.push(i);
                }
            }
        } else {
            miss_indices.extend(0..n_cols);
        }

        // Fast-path: full hit, no decode work.
        if miss_indices.is_empty() {
            let cols: Vec<DecodedColumn> = cached_cols.into_iter().map(Option::unwrap).collect();
            self.cur_rg_columns = Some(cols);
            self.cur_rg_row = 0;
            return Ok(());
        }

        // Cap on spawned threads. Default: never exceed available
        // cores, even for wide projections (Q1 = 7 cols on a 14-core
        // box → 7 threads). When the caller supplied a parallelism
        // budget, honour it instead — this is how the
        // `EmatixFastParquetExec` partition wrapper avoids
        // oversubscribing the scheduler with `N_partitions × N_cols`
        // scoped threads on top of its own outer parallelism.
        let cap = self.parallelism_budget.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or(1)
        });
        // Σ.Q.L6′: decode only the cache-miss subset, then merge with
        // cached hits at their original projection positions.
        let n_miss = miss_indices.len();
        let max_threads = cap.max(1).min(n_miss.max(1));

        // Sequential fast path: skip scoped-thread overhead when
        // there's nothing to parallelise (single-column miss or
        // single-core machine).
        let mut decoded_misses: Vec<DecodedColumn> = if max_threads <= 1 || n_miss <= 1 {
            let mut chunk_buf: Vec<u8> = Vec::new();
            let mut out = Vec::with_capacity(n_miss);
            for &proj_idx in &miss_indices {
                let leaf = projection[proj_idx];
                let target = schema.field(proj_idx).data_type();
                out.push(decode_one_column(
                    file,
                    cached_md,
                    &mut chunk_buf,
                    rg,
                    leaf,
                    target,
                )?);
            }
            out
        } else {
            // Pre-allocate result slots so we can scatter into them
            // by miss-index position without a final sort step.
            let mut slots: Vec<Option<DfResult<DecodedColumn>>> =
                (0..n_miss).map(|_| None).collect();

            // Shared work queue over the miss subset only.
            use std::sync::atomic::{AtomicUsize, Ordering};
            let next = AtomicUsize::new(0);
            let miss_indices_ref = &miss_indices;

            std::thread::scope(|s| {
                let mut handles = Vec::with_capacity(max_threads);
                for _ in 0..max_threads {
                    let next = &next;
                    handles.push(s.spawn(move || -> Vec<(usize, DfResult<DecodedColumn>)> {
                        let mut local: Vec<(usize, DfResult<DecodedColumn>)> = Vec::new();
                        let mut chunk_buf: Vec<u8> = Vec::new();
                        loop {
                            let m = next.fetch_add(1, Ordering::Relaxed);
                            if m >= n_miss {
                                break;
                            }
                            let proj_idx = miss_indices_ref[m];
                            let leaf = projection[proj_idx];
                            let target = schema.field(proj_idx).data_type();
                            local.push((
                                m,
                                decode_one_column(
                                    file,
                                    cached_md,
                                    &mut chunk_buf,
                                    rg,
                                    leaf,
                                    target,
                                ),
                            ));
                        }
                        local
                    }));
                }
                for h in handles {
                    let partial = h.join().expect("emat_arrow_reader decode thread panicked");
                    for (m, r) in partial {
                        slots[m] = Some(r);
                    }
                }
            });

            // Fail-fast on the first column error.
            let mut out = Vec::with_capacity(n_miss);
            for (m, slot) in slots.into_iter().enumerate() {
                let r = slot.ok_or_else(|| ext(format!("column miss-slot {m} never filled")))?;
                out.push(r?);
            }
            out
        };

        // Σ.Q.L6′: insert each newly-decoded column into the per-column
        // cache, then merge into the projection-ordered cols vec.
        if let (Some(cache), Some(path)) = (self.rg_decode_cache.as_ref(), self.path.as_ref()) {
            for (m, &proj_idx) in miss_indices.iter().enumerate() {
                let leaf = projection[proj_idx];
                // NARROW.DEC — see the probe loop: narrowed leaves are
                // never inserted (type-blind key).
                if is_narrowed(proj_idx, leaf) {
                    continue;
                }
                let key = RgCacheKey {
                    file_path: path.clone(),
                    row_group_idx: rg,
                    leaf_idx: leaf,
                };
                cache.insert(key, decoded_misses[m].clone());
            }
        }

        // Drain `decoded_misses` in order; for each projection slot,
        // pull either the cached column (if any) or the next decoded
        // miss. Misses appear in the same order as `miss_indices`.
        let mut miss_iter = decoded_misses.drain(..);
        let cols: Vec<DecodedColumn> = (0..n_cols)
            .map(|i| match cached_cols[i].take() {
                Some(c) => c,
                None => miss_iter
                    .next()
                    .expect("missed column with no decode result"),
            })
            .collect();

        // Sanity: every column has the same length as the RG.
        for (i, c) in cols.iter().enumerate() {
            if c.len() != self.cur_rg_total {
                return Err(ext(format!(
                    "RG {rg}: column {i} (leaf {}) decoded {} rows but RG declares {}",
                    self.projection[i],
                    c.len(),
                    self.cur_rg_total,
                )));
            }
        }

        // Σ.Q.L6′: cache inserts happened above per-column on the
        // miss path — nothing to do here.

        self.cur_rg_columns = Some(cols);
        self.cur_rg_row = 0;
        Ok(())
    }

    /// Slice `[start, start+n)` from each per-RG buffer and assemble
    /// the `RecordBatch`. Caller guarantees the window stays inside
    /// the current RG.
    fn slice_batch(&self, start: usize, n: usize) -> DfResult<RecordBatch> {
        let cols = self
            .cur_rg_columns
            .as_ref()
            .expect("slice_batch called with no current RG");
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(cols.len());
        for (i, c) in cols.iter().enumerate() {
            let target = self.arrow_schema.field(i).data_type();
            arrays.push(slice_decoded(c, start, n, target));
        }
        let batch = RecordBatch::try_new(self.arrow_schema.clone(), arrays)
            .map_err(|e| ext(format!("RecordBatch::try_new: {e}")))?;

        // Σ.E5 Phase 1.6: if a per-RG filter bitmap is present (set by
        // the selectivity-gate fallback in load_row_group_masked),
        // build a BooleanArray over the batch's window of the bitmap
        // and apply Arrow's SIMD filter to the batch. Matches the
        // pipeline shape FilterExec would have provided if pushdown
        // were Inexact; with Exact pushdown, this is the only filter
        // step in the plan for the pushed predicate.
        if let Some(buf) = self.cur_rg_filter_bitmap.as_ref() {
            let timing = std::env::var_os("EMAT_TIMING").is_some();
            let t_filter = if timing {
                Some(std::time::Instant::now())
            } else {
                None
            };
            // Σ.AE.3 (2026-05-26): clone the cached Buffer (Arc bump,
            // not a memcpy) and window into it. Previously did
            // `Buffer::from_slice_ref(&Vec<u8>)` per slice_batch call,
            // which copied the whole bitmap each time (~125 KB × 15
            // batches/RG → ~7.5 MB of copies per Q03 partition).
            let bool_buf = datafusion::arrow::buffer::BooleanBuffer::new(buf.clone(), start, n);
            let predicate_arr = arrow_array::BooleanArray::new(bool_buf, None);
            let filtered = datafusion::arrow::compute::filter_record_batch(&batch, &predicate_arr)
                .map_err(|e| ext(format!("filter_record_batch: {e}")))?;
            if let Some(t) = t_filter {
                eprintln!(
                    "[emat.batch_filter] start={start} n={n} out={} elapsed={:.3}ms",
                    filtered.num_rows(),
                    t.elapsed().as_secs_f64() * 1000.0
                );
            }
            return Ok(filtered);
        }
        Ok(batch)
    }
}

impl Iterator for EmatArrowBatchReader {
    type Item = DfResult<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // If the current RG is exhausted (or unset), advance.
            let need_new_rg = self.cur_rg_columns.is_none() || self.cur_rg_row >= self.cur_rg_total;
            if need_new_rg {
                // Morsel-engine P2 spike: pull the next RG dynamically
                // from the shared work queue if one is attached, else
                // walk the static per-partition assignment.
                let rg = match &self.shared_cursor {
                    Some(cursor) => match cursor.pop() {
                        Some(rg) => rg,
                        None => return None,
                    },
                    None => {
                        if self.cur_rg_idx >= self.row_groups.len() {
                            return None;
                        }
                        let rg = self.row_groups[self.cur_rg_idx];
                        self.cur_rg_idx += 1;
                        rg
                    }
                };
                if let Err(e) = self.load_row_group(rg) {
                    return Some(Err(e));
                }
                if self.cur_rg_total == 0 {
                    // Empty RG — try the next one.
                    continue;
                }
            }

            // Σ.E5 (2026-05-19): when BridgeFilter pushdown is
            // active and the post-filter RG fits in a single batch,
            // sub-divide so downstream HashJoinExec sees multiple
            // smaller probe batches. FastParquet emits ~4 batches in
            // these cases (because filter+repartition splits a single
            // 65k batch), and HashJoin probe with a 1-big-batch build
            // side is slower than with 4-smaller-batch build (cache
            // locality on the build-side hash table lookup). Q16
            // wins ~5pp; doesn't regress Q01 (no filter pushdown).
            let remaining = self.cur_rg_total - self.cur_rg_row;
            let effective_batch_size = if self.filter.is_some()
                && self.cur_rg_total < self.batch_size
                && self.cur_rg_total >= 4 * 1024
            {
                // Target ~4 sub-batches when there's a filter and the
                // post-filter RG is under one normal batch.
                (self.cur_rg_total / 4).max(1024)
            } else {
                self.batch_size
            };
            let n = remaining.min(effective_batch_size);
            let start = self.cur_rg_row;
            self.cur_rg_row += n;
            return Some(self.slice_batch(start, n));
        }
    }
}

// ============================================================
// Per-column masked decode (Σ.E5 #516 late-mat path)
// ============================================================

/// REV.23 — pass-rate above which a user-filtered scan routes to dense
/// decode (`load_row_group_dense` + FilterExec/SIMD compaction) instead of
/// the per-column masked gather. Default 0.10, calibrated on TPC-H SF=10
/// (dense-winners all ≥0.15 pass, masked-winners all ≤0.043 — clean gap;
/// 0.10 sits in it). Env-tunable via `EMAT_MASKED_DENSE_PASSRATE` for A/B
/// sweeps and for workloads with clustered selective filters that page-skip.
pub(crate) fn masked_dense_passrate_threshold() -> f64 {
    std::env::var("EMAT_MASKED_DENSE_PASSRATE")
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .filter(|v| v.is_finite() && (0.0..=1.0).contains(v))
        .unwrap_or(0.10)
}

/// REV.23 — true iff a masked scan with this `popcount`/`total` should fall
/// back to dense decode. Pure helper so the gate decision is unit-testable
/// without a reader + parquet file. Strictly-greater so a 0.0 threshold
/// keeps an all-pass RG on dense (matches the prior `popcount*3 > total`
/// strict-comparison semantics).
pub(crate) fn should_route_masked_to_dense(popcount: usize, total: usize, threshold: f64) -> bool {
    total > 0 && (popcount as f64) > threshold * (total as f64)
}

/// Decode one projected column applying the row bitmap. Pages whose
/// bitmap-popcount is zero are skipped inside the underlying
/// `masked_decode_*` helpers (Π.10). Returns a `DecodedColumn` with
/// exactly `popcount` rows — same shape as the dense decode so the
/// downstream `slice_batch` path works unchanged.
fn masked_decode_one_column(
    file: &ParquetFile,
    rg: usize,
    leaf: usize,
    bitmap: &[u8],
    popcount: usize,
    target: &DataType,
    // NARROW.DEC — the leaf's parquet physical type, so an Int32
    // target over physical INT64 (downcast-on-read narrowed key) can
    // route to the wide masked decode + checked narrow of the (small)
    // survivor set instead of mis-decoding INT64 pages as i32.
    phys: ParquetType,
) -> DfResult<DecodedColumn> {
    match target {
        DataType::Int32 | DataType::Date32 => {
            if phys == ParquetType::Int64 {
                // NARROW.DEC masked arm: decode wide at the mask (the
                // masked kernel skips zero-popcount pages, so the wide
                // transient is only the survivor rows), then checked-
                // narrow. `validate_type_pair` restricts this pair to
                // Int32 targets.
                let v = masked_decode_i64(file, rg, leaf, bitmap)
                    .map_err(|e| ext(format!("masked i64→i32 leaf {leaf}: {e}")))?;
                if v.len() != popcount {
                    return Err(ext(format!(
                        "masked i64→i32 leaf {leaf}: got {} rows, expected {popcount}",
                        v.len()
                    )));
                }
                let v = crate::ematix_parquet_bridge::i64s_into_i32_checked(v, rg, leaf)?;
                NARROW_KEY_DECODES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return Ok(DecodedColumn::Int32 {
                    data: Buffer::from_vec(v),
                    n_rows: popcount,
                });
            }
            let v = masked_decode_i32(file, rg, leaf, bitmap)
                .map_err(|e| ext(format!("masked i32 leaf {leaf}: {e}")))?;
            if v.len() != popcount {
                return Err(ext(format!(
                    "masked i32 leaf {leaf}: got {} rows, expected {popcount}",
                    v.len()
                )));
            }
            Ok(DecodedColumn::Int32 {
                data: Buffer::from_vec(v),
                n_rows: popcount,
            })
        }
        // KEYS.4.b: UInt64 shares the physical INT64 decode; stored as a
        // DecodedColumn::Int64 buffer and reinterpreted in `slice_decoded`.
        DataType::Int64 | DataType::UInt64 => {
            let v = masked_decode_i64(file, rg, leaf, bitmap)
                .map_err(|e| ext(format!("masked i64 leaf {leaf}: {e}")))?;
            if v.len() != popcount {
                return Err(ext(format!(
                    "masked i64 leaf {leaf}: got {} rows, expected {popcount}",
                    v.len()
                )));
            }
            Ok(DecodedColumn::Int64 {
                data: Buffer::from_vec(v),
                n_rows: popcount,
            })
        }
        DataType::Float64 => {
            let v = masked_decode_f64(file, rg, leaf, bitmap)
                .map_err(|e| ext(format!("masked f64 leaf {leaf}: {e}")))?;
            if v.len() != popcount {
                return Err(ext(format!(
                    "masked f64 leaf {leaf}: got {} rows, expected {popcount}",
                    v.len()
                )));
            }
            Ok(DecodedColumn::Float64 {
                data: Buffer::from_vec(v),
                n_rows: popcount,
            })
        }
        DataType::Utf8View => {
            // Σ.E5 #517: dict-preserved masked decode. Decode the
            // chunk via the dict-preserved fast path (same as the
            // dense `decode_byte_array_to_string_view_dict_preserved`
            // shape), then build a per-dict-entry views cache and
            // gather only bitmap-matching indices. Net: ~same decode
            // CPU as the dense path + a cheap u128 gather per
            // surviving row.
            //
            // Build dict_views once over the whole dict-page slice;
            // per surviving row emit dict_views[idx] (16-byte gather).
            // No per-row `make_view` call; matches the dense fast
            // path's emission cost.
            //
            // Σ.E5 (2026-05-19): falls back to PLAIN masked decode
            // when the column is non-dict-encoded (Q20's p_name —
            // writer fell back to PLAIN). The PLAIN path packs bytes
            // and per-row make_view; slower but correct.
            let mut dict_bytes: Vec<u8> = Vec::new();
            let mut dict_offsets: Vec<u32> = Vec::new();
            let mut all_indices: Vec<u32> = Vec::new();
            if let Err(_e) = read_column_byte_array_dict_preserved_into(
                file,
                rg,
                leaf,
                &mut dict_bytes,
                &mut dict_offsets,
                &mut all_indices,
            ) {
                let vals = masked_decode_byte_array(file, rg, leaf, bitmap)
                    .map_err(|e| ext(format!("plain masked byte_array leaf {leaf}: {e}")))?;
                if vals.len() != popcount {
                    return Err(ext(format!(
                        "plain masked byte_array leaf {leaf}: got {} rows, expected {popcount}",
                        vals.len()
                    )));
                }
                let total_bytes: usize = vals.iter().map(|v| v.len()).sum();
                let mut packed: Vec<u8> = Vec::with_capacity(total_bytes);
                let mut views: Vec<u128> = Vec::with_capacity(popcount);
                let block_id: u32 = 0;
                for v in &vals {
                    let off = packed.len() as u32;
                    packed.extend_from_slice(v);
                    views.push(make_view(v, block_id, off));
                }
                return Ok(DecodedColumn::StringView {
                    views: Buffer::from_vec(views),
                    n_rows: popcount,
                    data_buffers: vec![Buffer::from_vec(packed)],
                });
            }

            // Bytes land in a single buffer; block_id 0.
            let data_buffer = Buffer::from_vec(dict_bytes);
            let dict_len = dict_offsets.len().saturating_sub(1);
            // SAFETY: data_buffer is the canonical block 0 store.
            let base = data_buffer.as_ptr() as usize;
            let mut dict_views: Vec<u128> = Vec::with_capacity(dict_len);
            for i in 0..dict_len {
                let off = dict_offsets[i] as usize;
                let len = (dict_offsets[i + 1] - dict_offsets[i]) as usize;
                let bytes_ptr = base + off;
                // Build the bytes slice for make_view; it inspects
                // the prefix only, no aliasing concerns with the
                // owning Buffer.
                let bytes = unsafe { std::slice::from_raw_parts(bytes_ptr as *const u8, len) };
                dict_views.push(make_view(bytes, 0, off as u32));
            }

            // Gather views for bitmap-matching rows in order.
            let mut views: Vec<u128> = Vec::with_capacity(popcount);
            for (row, &idx) in all_indices.iter().enumerate() {
                let byte = bitmap[row >> 3];
                if (byte >> (row & 7)) & 1 != 0 {
                    let i = idx as usize;
                    if i >= dict_len {
                        return Err(ext(format!(
                            "dict-preserved masked leaf {leaf}: idx {idx} out of range {dict_len}"
                        )));
                    }
                    views.push(dict_views[i]);
                }
            }
            if views.len() != popcount {
                return Err(ext(format!(
                    "dict-preserved masked leaf {leaf}: emitted {} rows, expected {popcount}",
                    views.len()
                )));
            }
            Ok(DecodedColumn::StringView {
                views: Buffer::from_vec(views),
                n_rows: popcount,
                data_buffers: vec![data_buffer],
            })
        }
        DataType::Utf8 => {
            let vals = masked_decode_byte_array(file, rg, leaf, bitmap)
                .map_err(|e| ext(format!("masked byte_array leaf {leaf}: {e}")))?;
            if vals.len() != popcount {
                return Err(ext(format!(
                    "masked byte_array leaf {leaf}: got {} rows, expected {popcount}",
                    vals.len()
                )));
            }
            let total_bytes: usize = vals.iter().map(|v| v.len()).sum();
            let mut sb = StringBuilder::with_capacity(popcount, total_bytes);
            for v in &vals {
                let s = std::str::from_utf8(v)
                    .map_err(|e| ext(format!("masked Utf8 leaf {leaf}: invalid UTF-8: {e}")))?;
                sb.append_value(s);
            }
            Ok(DecodedColumn::Utf8(Arc::new(sb.finish())))
        }
        other => Err(ext(format!(
            "masked decode: unsupported target type {other:?} for leaf {leaf}"
        ))),
    }
}

// ============================================================
// Per-column decode
// ============================================================

fn decode_one_column(
    file: &ParquetFile,
    cached_md: &CachedFileMetadata,
    chunk_buf: &mut Vec<u8>,
    rg: usize,
    leaf: usize,
    target: &DataType,
) -> DfResult<DecodedColumn> {
    let cm = &cached_md.row_groups[rg].columns[leaf];
    match target {
        DataType::Int32 | DataType::Date32 => {
            // NARROW.DEC: Int32 target over a physical INT64 chunk =
            // the downcast-on-read shape (KEYS.2 narrowed key +
            // EMAT_NARROW_KEY_DECODE). Decode directly at the
            // narrowest stats-proven width — no transient Vec<i64>,
            // no boundary cast (`validate_type_pair` restricts the
            // pair to Int32; Date32 can never land here).
            if cm.column_type == ParquetType::Int64 {
                let v = crate::ematix_parquet_bridge::decode_column_chunk_i64_downcast_to_i32(
                    file, rg, leaf,
                )?;
                NARROW_KEY_DECODES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let n_rows = v.len();
                return Ok(DecodedColumn::Int32 {
                    data: Buffer::from_vec(v),
                    n_rows,
                });
            }
            let v = decode_dict_chunk_typed::<i32>(file, chunk_buf, cm, |b| {
                decode_plain_i32(b).map_err(|e| ext(format!("plain i32: {e}")))
            })?;
            let n_rows = v.len();
            Ok(DecodedColumn::Int32 {
                data: Buffer::from_vec(v),
                n_rows,
            })
        }
        // KEYS.4.b: UInt64 shares the physical INT64 decode (see
        // `masked_decode_one_column`); interpretation deferred to `slice_decoded`.
        DataType::Int64 | DataType::UInt64 => {
            let v = decode_dict_chunk_typed::<i64>(file, chunk_buf, cm, |b| {
                decode_plain_i64(b).map_err(|e| ext(format!("plain i64: {e}")))
            })?;
            let n_rows = v.len();
            Ok(DecodedColumn::Int64 {
                data: Buffer::from_vec(v),
                n_rows,
            })
        }
        DataType::Float64 => {
            let v = decode_dict_chunk_typed::<f64>(file, chunk_buf, cm, |b| {
                decode_plain_f64(b).map_err(|e| ext(format!("plain f64: {e}")))
            })?;
            let n_rows = v.len();
            Ok(DecodedColumn::Float64 {
                data: Buffer::from_vec(v),
                n_rows,
            })
        }
        DataType::Utf8View => decode_byte_array_to_string_view(file, chunk_buf, cm, rg, leaf),
        DataType::Dictionary(_, _) => decode_byte_array_dict_preserved(file, rg, leaf),
        DataType::Utf8 => decode_byte_array_to_utf8(file, chunk_buf, cm),
        other => Err(DataFusionError::NotImplemented(format!(
            "EmatArrowBatchReader: target Arrow type {other:?} not yet supported"
        ))),
    }
}

/// PR-2-style generic dict-or-PLAIN decoder for fixed-size primitives.
/// Mirrors `ematix_parquet_bridge::decode_dict_chunk_generic`.
///
/// Σ.E5.6: takes the cached `CachedColumnChunk` directly instead of
/// re-parsing the thrift footer per call, AND takes a reusable
/// `chunk_buf` so consecutive column-chunk reads on the same thread
/// share one allocation (eliminates per-call ~1 MB Vec alloc +
/// `madvise(MADV_DONTNEED)` on drop).
fn decode_dict_chunk_typed<T: Copy>(
    file: &ParquetFile,
    chunk_buf: &mut Vec<u8>,
    cm: &CachedColumnChunk,
    decode_plain: impl Fn(&[u8]) -> DfResult<Vec<T>>,
) -> DfResult<Vec<T>> {
    let total = cm.num_values as usize;
    let codec = cm.codec;
    let start = cm
        .dictionary_page_offset
        .filter(|&d| d < cm.data_page_offset)
        .unwrap_or(cm.data_page_offset) as u64;
    let length = cm.total_compressed_size as u64;
    file.read_range_into(chunk_buf, start, length)
        .map_err(|e| ext(format!("read_range_into: {e}")))?;
    let chunk = &chunk_buf[..];

    let mut walker = PageWalker::new(chunk);
    let mut scratch: Vec<u8> = Vec::with_capacity(128 * 1024);
    let mut out: Vec<T> = Vec::with_capacity(total);

    let (first_hdr, first_body) = walker
        .next_page()
        .map_err(|e| ext(format!("next_page (first): {e}")))?
        .ok_or_else(|| ext("empty chunk"))?;
    decompress_into_sized(
        codec,
        first_body,
        first_hdr.uncompressed_page_size as usize,
        &mut scratch,
    )?;

    let dict: Vec<T> = if first_hdr.dictionary_page_header.is_some() {
        decode_plain(&scratch)?
    } else {
        let dph = first_hdr
            .data_page_header
            .as_ref()
            .ok_or_else(|| ext("first page neither dict nor v1 data"))?;
        let n = dph.num_values as usize;
        match dph.encoding {
            Encoding::Plain => {
                let mut vals = decode_plain(&scratch)?;
                vals.truncate(n);
                out.extend(vals);
            }
            other => {
                return Err(ext(format!(
                    "first page is data but encoding {other:?} (need dict context)"
                )));
            }
        }
        Vec::new()
    };

    while out.len() < total {
        let (hdr, body) = walker
            .next_page()
            .map_err(|e| ext(format!("next_page: {e}")))?
            .ok_or_else(|| ext("chunk ended before num_values consumed"))?;
        let dph = hdr
            .data_page_header
            .as_ref()
            .ok_or_else(|| ext("v2 pages not yet supported"))?;
        let n = dph.num_values as usize;
        decompress_into_sized(
            codec,
            body,
            hdr.uncompressed_page_size as usize,
            &mut scratch,
        )?;
        match dph.encoding {
            Encoding::RleDictionary | Encoding::PlainDictionary => {
                decode_rle_dictionary_into(&scratch, &dict, n, &mut out)
                    .map_err(|e| ext(format!("rle_dict: {e}")))?;
            }
            Encoding::Plain => {
                let mut vals = decode_plain(&scratch)?;
                vals.truncate(n);
                out.extend(vals);
            }
            other => {
                return Err(ext(format!("unexpected data page encoding {other:?}")));
            }
        }
    }
    Ok(out)
}

/// BYTE_ARRAY → `StringViewArray`.
///
/// Σ.E5.1.d: the overwhelmingly common case is a dict-encoded chunk
/// (parquet's default writer behaviour for low-cardinality byte
/// arrays — `l_returnflag` / `l_linestatus` on TPC-H, every enum-ish
/// column in practice). For that case we hand off to
/// `decode_dict_byte_array_to_string_view` which:
///   * decodes the dict + indices once via
///     `read_column_byte_array_dict_preserved` (zero per-row UTF-8
///     validation; reuses the codec's tuned dict-preservation path),
///   * builds a `Vec<u128>` of *per-dict-entry* views — `make_view`
///     (which is `#[inline(never)]` — a real call) runs `dict_len`
///     times instead of `n_rows` times, and
///   * fills the `n_rows` view buffer with a tight gather
///     (`views[r] = dict_views[indices[r] as usize]`) — branch-free,
///     no `extend_from_slice`, no per-row inline/buffer split.
///
/// We use `dict_bytes` straight from the dict-preserved column as the
/// backing data block (no copy). For inline (≤12B) views the data
/// block is unused but it's still attached for >12B-prefix arrays
/// that might land here in future workloads.
///
/// Σ.E5 (2026-05-18) bench-only entry point. Exposes the per-RG
/// StringView decode for direct micro-benchmarks against parquet-rs;
/// returns just `(row_count, total_bytes_decoded)` so we don't have
/// to widen `DecodedColumn`'s visibility.
pub fn decode_byte_array_to_string_view_for_bench(
    file: &ParquetFile,
    rg: usize,
    col: usize,
) -> DfResult<(usize, usize)> {
    let cached_md = CachedFileMetadata::from_file(file)?;
    let cm = &cached_md.row_groups[rg].columns[col];
    let mut chunk_buf: Vec<u8> = Vec::new();
    let dc = decode_byte_array_to_string_view(file, &mut chunk_buf, cm, rg, col)?;
    let rows = dc.len();
    // Total decoded byte size = views buffer (16 bytes/row) + sum of
    // backing data buffers (one per page in the page-streaming layout).
    let bytes = match &dc {
        DecodedColumn::StringView {
            views,
            data_buffers,
            ..
        } => views.len() + data_buffers.iter().map(|b| b.len()).sum::<usize>(),
        _ => 0,
    };
    Ok((rows, bytes))
}

/// Σ.E5 (2026-05-18) bench-only entry point: decode an arbitrary
/// column (numeric or string) for one RG, return `(rows, bytes)`. Used
/// by `sigma_e5_column_decode_diff` to time decode by Arrow type
/// without going through DataFusion / mpsc orchestration.
///
/// `target` must be one of the Arrow types `decode_one_column`
/// recognises (Int32/Date32, Int64, Float64, Utf8View, Dictionary,
/// Utf8). Bench callers pick the type by inspecting parquet-rs's
/// promoted Arrow schema.
pub fn decode_one_column_for_bench(
    file: &ParquetFile,
    rg: usize,
    leaf: usize,
    target: &DataType,
) -> DfResult<(usize, usize)> {
    let cached_md = CachedFileMetadata::from_file(file)?;
    let mut chunk_buf: Vec<u8> = Vec::new();
    let dc = decode_one_column(file, &cached_md, &mut chunk_buf, rg, leaf, target)?;
    let rows = dc.len();
    let bytes = match &dc {
        DecodedColumn::Int32 { data, .. } => data.len(),
        DecodedColumn::Int64 { data, .. } => data.len(),
        DecodedColumn::Float64 { data, .. } => data.len(),
        DecodedColumn::StringView {
            views,
            data_buffers,
            ..
        } => views.len() + data_buffers.iter().map(|b| b.len()).sum::<usize>(),
        DecodedColumn::DictUtf8 {
            values, indices, ..
        } => indices.len() + values.get_array_memory_size(),
        DecodedColumn::Utf8(a) => a.get_array_memory_size(),
    };
    Ok((rows, bytes))
}

/// Σ.JS.1 (2026-07-09) plan-time sampling entry point: decode the
/// FIRST row group of one column and return it as an `ArrayRef` of
/// `target` type, so `join_side_rule` can evaluate a string-pattern
/// predicate against REAL data and measure its true selectivity
/// (instead of DataFusion's flat 20% default). One RG decode is tens
/// of ms and the caller caches the measured rate process-wide per
/// (file, predicate), so this is a one-shot plan-time cost. The
/// sample size is bounded by the writer's row-group size (even a
/// 131072+-row first RG is fine at plan time).
pub(crate) fn decode_first_rg_column_for_sampling(
    path: &str,
    leaf: usize,
    target: &DataType,
) -> DfResult<ArrayRef> {
    let file = ParquetFile::open(path).map_err(|e| ext(format!("sampling open {path}: {e}")))?;
    let cached_md = CachedFileMetadata::from_file(&file)?;
    let Some(rg0) = cached_md.row_groups.first() else {
        return Err(ext(format!("sampling {path}: file has no row groups")));
    };
    if leaf >= rg0.columns.len() {
        return Err(ext(format!(
            "sampling {path}: leaf {leaf} out of range ({} columns)",
            rg0.columns.len()
        )));
    }
    let mut chunk_buf: Vec<u8> = Vec::new();
    let dc = decode_one_column(&file, &cached_md, &mut chunk_buf, 0, leaf, target)?;
    let n = dc.len();
    Ok(slice_decoded(&dc, 0, n, target))
}

/// For the rare PLAIN-only (no dict) case — extremely unusual in
/// real parquet — we fall back to the previous row-by-row path so we
/// still produce a correct result.
fn decode_byte_array_to_string_view(
    file: &ParquetFile,
    chunk_buf: &mut Vec<u8>,
    cm: &CachedColumnChunk,
    rg: usize,
    col: usize,
) -> DfResult<DecodedColumn> {
    // Fast path: try the dict-preserved reader first. It fails only
    // when the column has no DictionaryPage or has a PLAIN-fallback
    // data page (writer wrote some pages dict, some PLAIN).
    //
    // NOTE: `read_column_byte_array_dict_preserved` is from
    // ematix-parquet-codec — it parses its own metadata AND allocates
    // its own chunk buffer. The CachedColumnChunk + chunk_buf reuse
    // only benefit the slow path here. Upstream API changes to take
    // pre-cached metadata + reusable scratch would close the
    // remaining gap.
    match read_column_byte_array_dict_preserved(file, rg, col) {
        Ok(raw) => Ok(build_string_view_from_dict_preserved(raw)),
        Err(_) => decode_byte_array_to_string_view_slow(file, chunk_buf, cm),
    }
}

/// Σ.E5.1.d hot path — collapses to a tight gather:
///   1. Build `dict_views: Vec<u128>` once (size = `dict_len`,
///      typically 3–100 for low-card columns).
///   2. For each `idx` in `indices`, `views.push(dict_views[idx])`.
///      No `make_view` call; no inline-vs-buffered branch in the loop.
///   3. Reuse `dict_bytes` as the backing block — zero copy.
fn build_string_view_from_dict_preserved(
    raw: ematix_parquet_codec::read::DictPreservedColumn,
) -> DecodedColumn {
    let dict_len = raw.dict_offsets.len() - 1;
    let n_rows = raw.indices.len();

    // (1) Build one view per dict entry. `make_view` is
    // `#[inline(never)]` so we want this to run dict_len times, not
    // n_rows times.
    let mut dict_views: Vec<u128> = Vec::with_capacity(dict_len);
    for i in 0..dict_len {
        let start = raw.dict_offsets[i] as usize;
        let end = raw.dict_offsets[i + 1] as usize;
        let bytes = &raw.dict_bytes[start..end];
        // block_id = 0 (we attach `dict_bytes` as buffer 0); offset
        // = start (`make_view` ignores it for ≤12B inline strings,
        // uses it for the long path). `make_view` already
        // jump-tables by length to avoid `ptr::copy_nonoverlapping`
        // for inline strings — see godbolt link in arrow-array.
        dict_views.push(make_view(bytes, 0, raw.dict_offsets[i]));
    }

    // (2) Tight gather. Bounds were validated by the dict-preserved
    // reader (it rejects any idx >= dict_len up front), so unchecked
    // indexing is sound — but the bounds check usually elides under
    // the optimiser here anyway. Keep it safe; the read is the cost.
    let mut views: Vec<u128> = Vec::with_capacity(n_rows);
    for &idx in &raw.indices {
        // SAFETY (logical, not unsafe): the dict-preserved reader
        // validates every idx < dict_len before returning.
        views.push(dict_views[idx as usize]);
    }
    debug_assert_eq!(views.len(), n_rows);

    // (3) `dict_bytes` is exactly the backing block our views point
    // at (for >12B strings; ≤12B strings are inlined in the view).
    // Dict-preserved path: single backing buffer (block_id=0) since
    // all values come from the one DictionaryPage.
    DecodedColumn::StringView {
        views: Buffer::from_vec(views),
        n_rows,
        data_buffers: vec![Buffer::from_vec(raw.dict_bytes)],
    }
}

/// Slow path: PLAIN-only or mixed-encoding BYTE_ARRAY → StringView,
/// row-by-row. Pre-Σ.E5.1.d behaviour; kept for correctness when the
/// fast dict-preserved reader can't claim the chunk.
fn decode_byte_array_to_string_view_slow(
    file: &ParquetFile,
    chunk_buf: &mut Vec<u8>,
    cm: &CachedColumnChunk,
) -> DfResult<DecodedColumn> {
    // Σ.E5 (2026-05-19): EMAT_DECODE_TIMING=1 dumps per-stage
    // breakdown (read / decompress / view-build) for the slow path.
    // Σ.E5 (2026-05-19): Q13 profile showed Snappy decompress is
    // 86% of column-decode time (~42ms cumulative for o_comment
    // across both RGs, sequential). Refactored to pre-walk pages,
    // then rayon-parallelise the decompress+view-build of remaining
    // data pages with pre-assigned block_ids. EMAT_DECODE_SERIAL=1
    // forces back to the legacy sequential path for A/B testing.
    let timing = std::env::var_os("EMAT_DECODE_TIMING").is_some();
    let force_serial = std::env::var_os("EMAT_DECODE_SERIAL").is_some();
    let t0 = std::time::Instant::now();

    let total = cm.num_values as usize;
    let codec = cm.codec;
    let start = cm
        .dictionary_page_offset
        .filter(|&d| d < cm.data_page_offset)
        .unwrap_or(cm.data_page_offset) as u64;
    let length = cm.total_compressed_size as u64;
    file.read_range_into(chunk_buf, start, length)
        .map_err(|e| ext(format!("read_range_into: {e}")))?;
    let chunk = &chunk_buf[..];
    let read_ns = t0.elapsed().as_nanos();
    let mut decompress_ns: u128 = 0;
    let mut viewbuild_ns: u128 = 0;
    let mut n_pages_dict: usize = 0;
    let mut n_pages_plain: usize = 0;
    let mut n_pages_rle: usize = 0;

    let mut walker = PageWalker::new(chunk);
    let mut views: Vec<u128> = Vec::with_capacity(total);

    // Σ.E5 (2026-05-18): page-streaming layout. Each data page's
    // decompressed bytes become an owned `Vec<u8>` that we hand off
    // as a distinct `Buffer` (`data_buffers[block_id]`). Views into
    // that page use `block_id = data_buffers.len() - 1` so no per-row
    // memcpy into a coalesced backing buffer is needed.
    //
    // The dictionary page (if any) is `data_buffers[0]`; dict-encoded
    // data pages emit views referencing block 0. PLAIN data pages emit
    // views referencing their own (newly-pushed) block.
    let mut data_buffers: Vec<Buffer> = Vec::new();

    // Dict offsets/lengths within `data_buffers[0]`, if a dict page is
    // present.
    let mut dict_offsets: Vec<u32> = Vec::new();
    let mut dict_lengths: Vec<u32> = Vec::new();

    let (first_hdr, first_body) = walker
        .next_page()
        .map_err(|e| ext(format!("next_page (first): {e}")))?
        .ok_or_else(|| ext("empty chunk"))?;

    if first_hdr.dictionary_page_header.is_some() {
        // Decompress the dict page into a fresh owned buffer and
        // record per-entry (offset, length) within it. The decoded
        // bytes are *exactly* what we want as the backing block.
        let mut dict_scratch: Vec<u8> = Vec::with_capacity(first_body.len() * 2);
        let td = std::time::Instant::now();
        decompress_into(codec, first_body, &mut dict_scratch)?;
        decompress_ns += td.elapsed().as_nanos();
        n_pages_dict += 1;
        let entries = decode_plain_byte_array(&dict_scratch)
            .map_err(|e| ext(format!("plain byte_array dict: {e}")))?;
        // Compute offsets directly into `dict_scratch` by pointer math:
        // every slice from `decode_plain_byte_array` is a view into
        // `dict_scratch`, so its offset = ptr - dict_scratch.as_ptr().
        let base = dict_scratch.as_ptr() as usize;
        for s in &entries {
            let off = (s.as_ptr() as usize - base) as u32;
            dict_offsets.push(off);
            dict_lengths.push(s.len() as u32);
        }
        data_buffers.push(Buffer::from_vec(dict_scratch));
    } else {
        // First page IS a data page; handle it inline (PLAIN-only column).
        let dph = first_hdr
            .data_page_header
            .as_ref()
            .ok_or_else(|| ext("first page neither dict nor v1 data"))?;
        if !matches!(dph.encoding, Encoding::Plain) {
            return Err(ext(format!(
                "first page is data but encoding {:?} (need dict context)",
                dph.encoding
            )));
        }
        let n = dph.num_values as usize;
        let mut page_buf: Vec<u8> = Vec::with_capacity(first_body.len() * 2);
        let td = std::time::Instant::now();
        decompress_into(codec, first_body, &mut page_buf)?;
        decompress_ns += td.elapsed().as_nanos();
        let block_id = data_buffers.len() as u32;
        let tv = std::time::Instant::now();
        plain_byte_array_to_views_in_place(&page_buf, &mut views, n, block_id)?;
        viewbuild_ns += tv.elapsed().as_nanos();
        n_pages_plain += 1;
        data_buffers.push(Buffer::from_vec(page_buf));
    }

    // Σ.E5 (2026-05-19): pre-walk remaining data pages so we can
    // either keep the serial inline loop OR fan-out via rayon. The
    // walk itself is cheap (just thrift header reads + slice math)
    // but the per-page decompress + view-build is the hot work.
    struct PendingPage<'a> {
        encoding: Encoding,
        body: &'a [u8],
        n_values: usize,
        // Block_id of the data_buffers slot this page writes into
        // (for PLAIN pages; RleDict pages reuse block 0 from dict).
        block_id: u32,
    }
    let mut pending: Vec<PendingPage<'_>> = Vec::new();
    let mut next_block_id = data_buffers.len() as u32;
    let mut rows_seen = views.len();
    while rows_seen < total {
        let (hdr, body) = walker
            .next_page()
            .map_err(|e| ext(format!("next_page: {e}")))?
            .ok_or_else(|| ext("chunk ended before num_values"))?;
        let dph = hdr
            .data_page_header
            .as_ref()
            .ok_or_else(|| ext("v2 pages not yet supported"))?;
        let n = dph.num_values as usize;
        let block_id = if matches!(dph.encoding, Encoding::Plain) {
            let bid = next_block_id;
            next_block_id += 1;
            bid
        } else {
            0
        };
        pending.push(PendingPage {
            encoding: dph.encoding,
            body,
            n_values: n,
            block_id,
        });
        rows_seen += n;
    }

    // Parallel branch: decompress + build views for each pending
    // page in rayon. Each task returns (page_views, optional page_buf
    // for PLAIN). Block IDs were pre-assigned above so each task's
    // views reference the correct data_buffers slot once we
    // sequentially append.
    //
    // Threshold: only parallelise when there are enough pages to
    // amortise rayon dispatch (~50-100µs). On Q16 (supplier ~2 pages,
    // ~10k rows) going parallel was adding ~300µs of overhead. On
    // Q13 (o_comment 22-51 pages) parallel saves 15-25ms — clear
    // win above ~4 pages.
    let parallel_threshold = std::env::var("EMAT_DECODE_PARALLEL_THRESHOLD")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4usize);
    if !force_serial && pending.len() > parallel_threshold {
        use rayon::prelude::*;
        let dict_offsets_ref = &dict_offsets;
        let dict_lengths_ref = &dict_lengths;
        let dict_bytes_ref: &[u8] = if data_buffers.is_empty() {
            &[]
        } else {
            data_buffers[0].as_slice()
        };
        let codec_copy = codec;

        let td = std::time::Instant::now();
        #[allow(clippy::type_complexity)]
        let results: Vec<DfResult<(Vec<u128>, Option<Vec<u8>>)>> = pending
            .par_iter()
            .map(|p| match p.encoding {
                Encoding::RleDictionary | Encoding::PlainDictionary => {
                    let mut idx_scratch: Vec<u8> = Vec::new();
                    decompress_into(codec_copy, p.body, &mut idx_scratch)?;
                    let mut idx_buf: Vec<u32> = Vec::with_capacity(p.n_values);
                    ematix_parquet_codec::dict::decode_rle_dictionary_indices_into(
                        &idx_scratch,
                        p.n_values,
                        &mut idx_buf,
                    )
                    .map_err(|e| ext(format!("rle_dict_indices byte_array: {e}")))?;
                    let dict_len = dict_offsets_ref.len();
                    let mut page_views: Vec<u128> = Vec::with_capacity(p.n_values);
                    for &i in &idx_buf {
                        let i = i as usize;
                        if i >= dict_len {
                            return Err(ext(format!("dict idx {i} out of range {dict_len}")));
                        }
                        let off = dict_offsets_ref[i];
                        let len = dict_lengths_ref[i];
                        let bytes = &dict_bytes_ref[off as usize..(off + len) as usize];
                        page_views.push(make_view(bytes, 0u32, off));
                    }
                    Ok((page_views, None))
                }
                Encoding::Plain => {
                    let mut page_buf: Vec<u8> = Vec::with_capacity(p.body.len() * 2);
                    decompress_into(codec_copy, p.body, &mut page_buf)?;
                    let mut page_views: Vec<u128> = Vec::with_capacity(p.n_values);
                    plain_byte_array_to_views_in_place(
                        &page_buf,
                        &mut page_views,
                        p.n_values,
                        p.block_id,
                    )?;
                    Ok((page_views, Some(page_buf)))
                }
                other => Err(ext(format!(
                    "unexpected byte_array data page encoding {other:?}"
                ))),
            })
            .collect();
        let par_ns = td.elapsed().as_nanos();
        // Account the parallel-section time under decompress (it's
        // mostly Snappy) so timing breakdown stays comparable to the
        // serial path.
        decompress_ns += par_ns;

        // Sequentially append in page order — preserves block_id
        // contract (data_buffers[block_id] = this PLAIN page's bytes).
        for (page, res) in pending.iter().zip(results) {
            let (mut page_views, page_buf_opt) = res?;
            views.append(&mut page_views);
            match page.encoding {
                Encoding::Plain => {
                    debug_assert!(page_buf_opt.is_some());
                    if let Some(buf) = page_buf_opt {
                        data_buffers.push(Buffer::from_vec(buf));
                    }
                    n_pages_plain += 1;
                }
                _ => {
                    n_pages_rle += 1;
                }
            }
        }
    } else {
        // Serial fallback (EMAT_DECODE_SERIAL=1 or single-page chunks).
        let mut idx_scratch: Vec<u8> = Vec::new();
        let mut idx_buf: Vec<u32> = Vec::new();
        for p in &pending {
            let n = p.n_values;
            let body = p.body;
            match p.encoding {
                Encoding::RleDictionary | Encoding::PlainDictionary => {
                    let td = std::time::Instant::now();
                    decompress_into(codec, body, &mut idx_scratch)?;
                    decompress_ns += td.elapsed().as_nanos();
                    idx_buf.clear();
                    let tv = std::time::Instant::now();
                    ematix_parquet_codec::dict::decode_rle_dictionary_indices_into(
                        &idx_scratch,
                        n,
                        &mut idx_buf,
                    )
                    .map_err(|e| ext(format!("rle_dict_indices byte_array: {e}")))?;
                    let dict_len = dict_offsets.len();
                    // Dict pages always reside in `data_buffers[0]`.
                    let dict_block = 0u32;
                    // SAFETY: data_buffers[0] is the dict page; established
                    // above. Slicing is sound since dict_offsets/lengths
                    // were computed against its full contents.
                    let dict_bytes: &[u8] = data_buffers[0].as_slice();
                    for &i in &idx_buf {
                        let i = i as usize;
                        if i >= dict_len {
                            return Err(ext(format!("dict idx {i} out of range {dict_len}")));
                        }
                        let off = dict_offsets[i];
                        let len = dict_lengths[i];
                        let bytes = &dict_bytes[off as usize..(off + len) as usize];
                        views.push(make_view(bytes, dict_block, off));
                    }
                    viewbuild_ns += tv.elapsed().as_nanos();
                    n_pages_rle += 1;
                }
                Encoding::Plain => {
                    let mut page_buf: Vec<u8> = Vec::with_capacity(body.len() * 2);
                    let td = std::time::Instant::now();
                    decompress_into(codec, body, &mut page_buf)?;
                    decompress_ns += td.elapsed().as_nanos();
                    let block_id = data_buffers.len() as u32;
                    let tv = std::time::Instant::now();
                    plain_byte_array_to_views_in_place(&page_buf, &mut views, n, block_id)?;
                    viewbuild_ns += tv.elapsed().as_nanos();
                    n_pages_plain += 1;
                    data_buffers.push(Buffer::from_vec(page_buf));
                }
                other => {
                    return Err(ext(format!(
                        "unexpected byte_array data page encoding {other:?}"
                    )));
                }
            }
        }
    }

    debug_assert_eq!(views.len(), total);

    if timing {
        let total_ms = t0.elapsed().as_secs_f64() * 1000.0;
        let read_ms = read_ns as f64 / 1_000_000.0;
        let dec_ms = decompress_ns as f64 / 1_000_000.0;
        let view_ms = viewbuild_ns as f64 / 1_000_000.0;
        let other_ms = total_ms - read_ms - dec_ms - view_ms;
        eprintln!(
            "[emat.decode_byte_slow] total={total_ms:.2}ms read={read_ms:.2}ms \
             decompress={dec_ms:.2}ms view-build={view_ms:.2}ms other={other_ms:.2}ms \
             rows={total} pages_dict={n_pages_dict} pages_plain={n_pages_plain} pages_rle={n_pages_rle}"
        );
    }

    let n_rows = views.len();
    let views_buffer = Buffer::from_vec(views);
    Ok(DecodedColumn::StringView {
        views: views_buffer,
        n_rows,
        data_buffers,
    })
}

/// Σ.E5 (2026-05-18): PLAIN-encoded BYTE_ARRAY → views buffer in a
/// single pass, pointing views at the decompressed page bytes
/// directly. No coalescing copy.
///
/// `page_buf` is the decompressed page (which the caller will hand
/// off as `data_buffers[block_id]`). Each emitted view encodes
/// `(block_id, offset_in_page)` — Arrow's StringViewArray dereferences
/// it against `data_buffers[block_id]` at access time.
///
/// On Q13's `o_comment` (1.5M rows × ~30 B/value PLAIN) this
/// eliminates ~45 MB of `extend_from_slice` memcpy that the old
/// coalescing path did.
///
/// Format: parquet BYTE_ARRAY PLAIN is `[u32 len][bytes]` repeated.
///
/// Σ.E5 (2026-05-19): inlined the long-string (>12 B) view construction.
/// Arrow's `make_view` is `#[inline(never)]` and dominates Q13 decode
/// (~30 ns/row × 1.5 M = 45 ms). For values > 12 bytes the view is
/// `[len:u32 | prefix:u32 | block_id:u32 | offset:u32]` little-endian,
/// which we can splice directly into a `u128`. Short strings (≤ 12 B)
/// fall back to `make_view` for the per-length inline specialization.
#[inline(always)]
fn plain_byte_array_to_views_in_place(
    page_buf: &[u8],
    views: &mut Vec<u128>,
    n: usize,
    block_id: u32,
) -> DfResult<()> {
    let page_len = page_buf.len();
    let bytes_ptr = page_buf.as_ptr();
    let mut off = 0usize;
    let block_hi = (block_id as u128) << 64;
    views.reserve(n);

    for i in 0..n {
        if off + 4 > page_len {
            return Err(ext(format!(
                "plain byte_array: truncated length prefix at value {i}/{n}, offset {off}/{page_len}"
            )));
        }
        // Unaligned u32 read of the length prefix.
        let len = unsafe { std::ptr::read_unaligned(bytes_ptr.add(off) as *const u32) } as usize;
        off += 4;
        if off + len > page_len {
            return Err(ext(format!(
                "plain byte_array: value {i}/{n} length {len} overruns page at offset {off}"
            )));
        }

        let view: u128 = if len > 12 {
            // Inlined ByteView u128 layout (LE):
            //   bytes 0..4  = length
            //   bytes 4..8  = first-4-byte prefix
            //   bytes 8..12 = buffer_index (= block_id)
            //   bytes 12..16 = offset (= off as u32)
            let prefix = unsafe { std::ptr::read_unaligned(bytes_ptr.add(off) as *const u32) };
            (len as u128) | ((prefix as u128) << 32) | block_hi | ((off as u128) << 96)
        } else {
            // Short strings need byte-by-byte inlining into the u128
            // body — `make_view` jump-tables on length for this.
            let bytes = &page_buf[off..off + len];
            make_view(bytes, block_id, off as u32)
        };
        views.push(view);
        off += len;
    }
    Ok(())
}

/// BYTE_ARRAY → `DictionaryArray<UInt32, Utf8>` — same shape as
/// `ematix_parquet_bridge::decode_column_chunk_byte_array_dict_preserved`
/// but stashed in the `DecodedColumn::DictUtf8` variant so per-batch
/// slicing produces a `DictionaryArray` with the original `values`
/// shared across slices.
fn decode_byte_array_dict_preserved(
    file: &ParquetFile,
    rg: usize,
    col: usize,
) -> DfResult<DecodedColumn> {
    let raw = read_column_byte_array_dict_preserved(file, rg, col)
        .map_err(|e| ext(format!("dict_preserved (rg={rg}, col={col}): {e}")))?;

    let dict_len = raw.dict_offsets.len() - 1;
    let mut dict_strings: Vec<&str> = Vec::with_capacity(dict_len);
    for i in 0..dict_len {
        let s = raw.dict_offsets[i] as usize;
        let e = raw.dict_offsets[i + 1] as usize;
        let bytes = &raw.dict_bytes[s..e];
        let txt = std::str::from_utf8(bytes)
            .map_err(|err| ext(format!("dict entry {i} not valid UTF-8: {err}")))?;
        dict_strings.push(txt);
    }
    let values = Arc::new(StringArray::from(dict_strings));
    let n_rows = raw.indices.len();
    let indices_buf = Buffer::from_vec(raw.indices);
    Ok(DecodedColumn::DictUtf8 {
        values,
        indices: indices_buf,
        n_rows,
    })
}

/// Slow path: BYTE_ARRAY → `StringArray` row-by-row, using
/// `StringBuilder`. Kept for completeness; callers should prefer
/// `Utf8View` or `Dictionary(UInt32, Utf8)` on the hot path.
fn decode_byte_array_to_utf8(
    file: &ParquetFile,
    chunk_buf: &mut Vec<u8>,
    cm: &CachedColumnChunk,
) -> DfResult<DecodedColumn> {
    let total = cm.num_values as usize;
    let codec = cm.codec;
    let start = cm
        .dictionary_page_offset
        .filter(|&d| d < cm.data_page_offset)
        .unwrap_or(cm.data_page_offset) as u64;
    let length = cm.total_compressed_size as u64;
    file.read_range_into(chunk_buf, start, length)
        .map_err(|e| ext(format!("read_range_into: {e}")))?;
    let chunk = &chunk_buf[..];

    let mut walker = PageWalker::new(chunk);
    let mut scratch: Vec<u8> = Vec::with_capacity(128 * 1024);
    let mut builder = StringBuilder::with_capacity(total, cm.total_uncompressed_size as usize);

    let (first_hdr, first_body) = walker
        .next_page()
        .map_err(|e| ext(format!("next_page (first): {e}")))?
        .ok_or_else(|| ext("empty chunk"))?;
    decompress_into(codec, first_body, &mut scratch)?;

    let dict: Vec<Vec<u8>> = if first_hdr.dictionary_page_header.is_some() {
        let slices = decode_plain_byte_array(&scratch)
            .map_err(|e| ext(format!("plain byte_array dict: {e}")))?;
        slices.iter().map(|s| s.to_vec()).collect()
    } else {
        let dph = first_hdr
            .data_page_header
            .as_ref()
            .ok_or_else(|| ext("first page neither dict nor v1 data"))?;
        let n = dph.num_values as usize;
        let slices =
            decode_plain_byte_array(&scratch).map_err(|e| ext(format!("plain byte_array: {e}")))?;
        for s in slices.iter().take(n) {
            append_utf8(&mut builder, s)?;
        }
        Vec::new()
    };

    // Σ.E5 (2026-05-19): reusable per-RG idx buffer eliminates the
    // per-page `Vec<u32>` alloc churn.
    let mut idx_buf: Vec<u32> = Vec::new();
    while builder.len() < total {
        let (hdr, body) = walker
            .next_page()
            .map_err(|e| ext(format!("next_page: {e}")))?
            .ok_or_else(|| ext("chunk ended before num_values"))?;
        let dph = hdr
            .data_page_header
            .as_ref()
            .ok_or_else(|| ext("v2 pages not yet supported"))?;
        let n = dph.num_values as usize;
        decompress_into(codec, body, &mut scratch)?;
        match dph.encoding {
            Encoding::RleDictionary | Encoding::PlainDictionary => {
                idx_buf.clear();
                ematix_parquet_codec::dict::decode_rle_dictionary_indices_into(
                    &scratch,
                    n,
                    &mut idx_buf,
                )
                .map_err(|e| ext(format!("rle_dict_indices: {e}")))?;
                for &i in &idx_buf {
                    let s = dict
                        .get(i as usize)
                        .ok_or_else(|| ext(format!("dict idx {i} out of range {}", dict.len())))?;
                    append_utf8(&mut builder, s)?;
                }
            }
            Encoding::Plain => {
                let slices = decode_plain_byte_array(&scratch)
                    .map_err(|e| ext(format!("plain byte_array: {e}")))?;
                for s in slices.iter().take(n) {
                    append_utf8(&mut builder, s)?;
                }
            }
            other => {
                return Err(ext(format!(
                    "unexpected byte_array data page encoding {other:?}"
                )));
            }
        }
    }
    debug_assert_eq!(builder.len(), total);
    Ok(DecodedColumn::Utf8(Arc::new(builder.finish())))
}

#[inline]
fn append_utf8(builder: &mut StringBuilder, bytes: &[u8]) -> DfResult<()> {
    let s = std::str::from_utf8(bytes).map_err(|e| ext(format!("byte_array not UTF-8: {e}")))?;
    builder.append_value(s);
    Ok(())
}

// ============================================================
// Batch slicing
// ============================================================

/// Build an `ArrayRef` covering `[start, start+n)` of the decoded
/// per-RG buffer. For primitives this is a single Buffer slice
/// (zero-copy). For `StringView` we share the backing data buffer
/// across all slices and emit a fresh views array per slice.
/// For `DictUtf8` we share the `values` and slice the indices.
/// KEYS.4.b — reinterpret an INT64 physical value buffer as a
/// `UInt64Array`, zero-copy and bit-for-bit. Parquet's only ≥5-byte
/// integer width is INT64, so an unsigned 64-bit column is stored as
/// INT64 on disk and must surface as UInt64 without altering the bits:
/// values ≥ 2^63 differ from their i64 reading only in interpretation,
/// not representation. `i64` and `u64` share layout and 8-byte
/// alignment, so the value buffer is reused as-is — no copy, no decode.
/// Dormant until KEYS.4.a flips `arrow_type_for` to present such columns
/// as UInt64; this is the single bitcast surface both readers funnel
/// through (see `ematix_fast_parquet`'s decode arms).
#[inline]
pub(crate) fn u64_array_from_i64_buffer(buf: Buffer, len: usize) -> UInt64Array {
    let scalar = ScalarBuffer::<u64>::new(buf, 0, len);
    UInt64Array::new(scalar, None)
}

fn slice_decoded(c: &DecodedColumn, start: usize, n: usize, target: &DataType) -> ArrayRef {
    match c {
        DecodedColumn::Int32 { data, .. } => {
            // Σ.E5.1.c: zero-copy slice via `Buffer::slice_with_length`
            // — Arc bump + offset/length update, no memcpy. The
            // previous `Buffer::from_slice_ref(&v[start..start+n])`
            // copied every batch's worth of i32 (4 bytes × 65K rows ×
            // many batches) for nothing.
            let sliced = data.slice_with_length(start * 4, n * 4);
            let scalar = ScalarBuffer::<i32>::new(sliced, 0, n);
            match target {
                DataType::Date32 => Arc::new(Date32Array::new(scalar, None)),
                _ => Arc::new(Int32Array::new(scalar, None)),
            }
        }
        DecodedColumn::Int64 { data, .. } => {
            let sliced = data.slice_with_length(start * 8, n * 8);
            match target {
                // KEYS.4.b: a UInt64 column is physically INT64; the same
                // 8-byte buffer is reinterpreted bit-for-bit (zero-copy).
                DataType::UInt64 => Arc::new(u64_array_from_i64_buffer(sliced, n)),
                _ => {
                    let scalar = ScalarBuffer::<i64>::new(sliced, 0, n);
                    Arc::new(Int64Array::new(scalar, None))
                }
            }
        }
        DecodedColumn::Float64 { data, .. } => {
            let sliced = data.slice_with_length(start * 8, n * 8);
            let scalar = ScalarBuffer::<f64>::new(sliced, 0, n);
            Arc::new(Float64Array::new(scalar, None))
        }
        DecodedColumn::StringView {
            views,
            data_buffers,
            ..
        } => {
            // Σ.E5.1.c: zero-copy slice for the views buffer too. Each
            // view is 16 bytes — at 65K rows × ~20 batches × 2 string
            // cols on Q1 that's ~40 MB of u128 copying eliminated.
            // Backing data buffers are shared via Arc bump (one clone
            // per page; cheap — typically 1–6 pages per RG).
            //
            // Σ.E5 (2026-05-19, verified NEG): per-batch coalesce was
            // tested as a fix for Q13's `output_bytes=2.1GB` accounting
            // (vs fast's 152MB). Result: Q13 regressed +29% → +57%.
            // The 14× buffer-size inflation is a reporting artifact
            // (Arc<Buffer> ref-counts; operators don't iterate the
            // backing bytes during repartition). The per-batch memcpy
            // cost (~30MB for o_comment) far exceeds any downstream
            // saving. Don't coalesce; share the page buffers.
            let sliced_views = views.slice_with_length(start * 16, n * 16);
            let views_buf = ScalarBuffer::<u128>::new(sliced_views, 0, n);
            // SAFETY: we built every view ourselves with `make_view`
            // against the corresponding `data_buffers[block_id]` so
            // the (block_id, offset) coordinates are valid and the
            // bytes are valid UTF-8 (parquet Utf8 logical type).
            // `new_unchecked` saves ~9% of self-time on Q01 SF=10
            // (`validate_string_view` + `core::str::from_utf8` in the
            // 2026-05 stage profile); `force_validate` feature still
            // enables validation in debug.
            let arr = unsafe {
                StringViewArray::new_unchecked(views_buf, data_buffers.clone(), None::<NullBuffer>)
            };
            Arc::new(arr)
        }
        DecodedColumn::DictUtf8 {
            values, indices, ..
        } => {
            // Σ.E5.1.c: zero-copy slice on dict keys.
            let sliced = indices.slice_with_length(start * 4, n * 4);
            let scalar = ScalarBuffer::<u32>::new(sliced, 0, n);
            let keys = UInt32Array::new(scalar, None);
            let arr = DictionaryArray::<UInt32Type>::try_new(keys, values.clone() as ArrayRef)
                .expect("DictionaryArray::try_new on internally-built dict");
            Arc::new(arr)
        }
        DecodedColumn::Utf8(s) => {
            // Arrow array slice is O(1) — shares the offsets/values
            // buffers, just bumps the offset.
            Arc::new(StringArray::from(s.slice(start, n).to_data()))
        }
    }
}

// ============================================================
// Helpers
// ============================================================

#[inline]
fn ext<S: Into<String>>(msg: S) -> DataFusionError {
    DataFusionError::External(format!("emat_arrow_reader: {}", msg.into()).into())
}

/// Codec-aware decompress helper — same shape as the bridge's.
fn decompress_into(codec: CompressionCodec, body: &[u8], out: &mut Vec<u8>) -> DfResult<()> {
    match codec {
        CompressionCodec::Uncompressed => {
            out.clear();
            out.extend_from_slice(body);
            Ok(())
        }
        CompressionCodec::Snappy => {
            // Re-confirmed 2026-05-19: opting into
            // `decompress_snappy_fast_into` via `EMAT_FAST_SNAPPY=1`
            // regresses the 22-query geomean from 0.92 → 0.98 (Q14
            // -36% → -24%, Q01 -5% → +3%). Microbench wins on
            // random data don't transfer to TPC-H. `snap` crate
            // stays the default; see [[hand-rolled-snappy-neg]].
            decompress_snappy_into(body, out).map_err(|e| ext(format!("snappy: {e}")))
        }
        CompressionCodec::Zstd => {
            decompress_zstd_into(body, out).map_err(|e| ext(format!("zstd: {e}")))
        }
        CompressionCodec::Lz4Raw => {
            // LZ4_RAW page bodies (parquet's frameless LZ4 variant). This
            // size-less entry scans the block tags to recover the
            // uncompressed length; the primitive decode path calls
            // `decompress_into_sized` with the page header's declared size to
            // skip that scan on high-entropy columns. Shared kernel lives in
            // the sibling `ematix-parquet-codec`.
            decompress_lz4_raw_into(body, out).map_err(|e| ext(format!("lz4_raw: {e}")))
        }
        other => Err(ext(format!(
            "codec {other:?} not yet supported in emat_arrow_reader"
        ))),
    }
}

/// Like `decompress_into` but uses the page header's declared uncompressed
/// size for LZ4_RAW — the fast sized path that skips the block-tag scan the
/// size-less variant pays (the scan dominates on high-entropy columns like
/// foreign keys). Falls back to size-less if the declared size is wrong.
/// Non-LZ4 codecs ignore the hint and delegate to `decompress_into`.
fn decompress_into_sized(
    codec: CompressionCodec,
    body: &[u8],
    uncompressed_size: usize,
    out: &mut Vec<u8>,
) -> DfResult<()> {
    match codec {
        CompressionCodec::Lz4Raw => decompress_lz4_raw_into_sized(body, uncompressed_size, out)
            .or_else(|_| decompress_lz4_raw_into(body, out))
            .map_err(|e| ext(format!("lz4_raw: {e}"))),
        _ => decompress_into(codec, body, out),
    }
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::cast::AsArray;
    use arrow_schema::{Field, Schema};
    use ematix_parquet_codec::write::ColumnData;
    use ematix_parquet_codec::write::{
        write_table_to_path, write_table_to_path_with_row_group_size, write_table_with_dict_to_path,
    };
    use ematix_parquet_format::types::CompressionCodec;

    fn tmp_parquet(name: &str) -> std::path::PathBuf {
        // Unique per CALL — see 01e6a50: PID+name-keyed fixture paths race
        // under the parallel test runner (interleaved re-write vs read).
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "emat_arrow_reader_test_{}_{}_{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed),
            name
        ));
        let _ = std::fs::create_dir_all(&dir);
        dir.join(format!("{name}.parquet"))
    }

    // KEYS.4.b — a UInt64 column is physically stored as INT64 (parquet's
    // only 64-bit integer width), so it must surface as a UInt64Array
    // bit-for-bit. The value 2^63 (= i64::MIN's bit pattern) is a normal
    // u64, not a wrapped negative — this is the latent ORDER BY / range
    // correctness bug that type-faithful u64 fixes. The bitcast is
    // zero-copy: i64 and u64 share layout, so the physical buffer is
    // reinterpreted, never re-decoded.
    #[test]
    fn slice_decoded_uint64_bitcasts_from_i64_buffer() {
        use arrow_array::UInt64Array;
        let vals: Vec<i64> = vec![0, 1, -1, i64::MIN, 42];
        let n = vals.len();
        let col = DecodedColumn::Int64 {
            data: Buffer::from_vec(vals),
            n_rows: n,
        };
        let arr = slice_decoded(&col, 0, n, &DataType::UInt64);
        let u = arr
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("slice_decoded should yield a UInt64Array for a UInt64 target");
        assert_eq!(u.values(), &[0u64, 1, u64::MAX, 1u64 << 63, 42]);

        // Int64 target unchanged — no regression for the common path.
        let arr_i = slice_decoded(&col, 0, n, &DataType::Int64);
        assert!(arr_i.as_any().downcast_ref::<Int64Array>().is_some());

        // Mid-RG slice still bitcasts correctly (zero-copy offset).
        let mid = slice_decoded(&col, 2, 2, &DataType::UInt64);
        let m = mid.as_any().downcast_ref::<UInt64Array>().unwrap();
        assert_eq!(m.values(), &[u64::MAX, 1u64 << 63]);
    }

    #[test]
    fn validate_type_pair_accepts_uint64_over_int64() {
        assert!(validate_type_pair("k", ParquetType::Int64, &DataType::UInt64).is_ok());
        // still rejected over a non-INT64 physical type
        assert!(validate_type_pair("k", ParquetType::Double, &DataType::UInt64).is_err());
    }

    /// NARROW.DEC — the downcast-on-read pair (Int32 target over
    /// physical INT64) validates; Date32 must NOT inherit it.
    #[test]
    fn validate_type_pair_accepts_int32_over_int64_but_not_date32() {
        assert!(validate_type_pair("k", ParquetType::Int64, &DataType::Int32).is_ok());
        assert!(validate_type_pair("k", ParquetType::Int32, &DataType::Int32).is_ok());
        assert!(validate_type_pair("d", ParquetType::Int64, &DataType::Date32).is_err());
    }

    // ---------- NARROW.DEC — downcast-on-read reader tests ----------

    /// Fixture: 3-RG file with an i32-fitting INT64 key (negatives +
    /// both i32 extremes on the edge rows) and a payload column.
    fn write_narrow_fixture(path: &std::path::Path, n: usize) -> (Vec<i64>, Vec<f64>) {
        let mut keys: Vec<i64> = (0..n as i64).map(|x| x * 31 - 500_000).collect();
        keys[0] = i32::MIN as i64;
        keys[n - 1] = i32::MAX as i64;
        let payload: Vec<f64> = (0..n).map(|x| x as f64 * 0.25).collect();
        write_table_to_path_with_row_group_size(
            path,
            &[
                ("k_key", ColumnData::I64(&keys)),
                ("v", ColumnData::F64(&payload)),
            ],
            CompressionCodec::Uncompressed,
            (n / 3).max(1), // → 3 row groups
        )
        .unwrap();
        (keys, payload)
    }

    fn narrow_fixture_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k_key", DataType::Int64, false),
            Field::new("v", DataType::Float64, false),
        ]))
    }

    /// Result identity on the dense path: a narrowed leaf must emit an
    /// `Int32Array` whose values equal the wide (`Int64Array`) path's,
    /// on data with negatives and the i32 min/max edges; the untouched
    /// payload column stays byte-identical. The fire counter proves the
    /// downcast decode ran (the flag/leaf-set toggles the path).
    #[test]
    fn narrow_dec_dense_reader_matches_wide_path() {
        let path = tmp_parquet("narrow_dense");
        let n = 999;
        let (keys, payload) = write_narrow_fixture(&path, n);

        // Wide (control): no narrow leaves → Int64Array, the default.
        let file = ParquetFile::open(&path).unwrap();
        let wide = EmatArrowBatchReaderBuilder::new(file, narrow_fixture_schema())
            .build()
            .unwrap();
        let mut wide_keys: Vec<i64> = Vec::new();
        for b in wide {
            let b = b.unwrap();
            let k = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("wide path must stay Int64");
            wide_keys.extend(k.values());
        }
        assert_eq!(wide_keys, keys);

        // Narrow: leaf 0 declared narrowed → Int32Array, same values.
        let before = NARROW_KEY_DECODES.load(std::sync::atomic::Ordering::Relaxed);
        let file = ParquetFile::open(&path).unwrap();
        let narrow = EmatArrowBatchReaderBuilder::new(file, narrow_fixture_schema())
            .with_narrow_i64_leaves(vec![0])
            .build()
            .unwrap();
        assert_eq!(
            narrow.schema().field(0).data_type(),
            &DataType::Int32,
            "narrowed leaf must advertise Int32 on the reader schema"
        );
        let mut narrow_keys: Vec<i64> = Vec::new();
        let mut narrow_payload: Vec<f64> = Vec::new();
        for b in narrow {
            let b = b.unwrap();
            let k = b
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("narrowed leaf must emit Int32Array");
            narrow_keys.extend(k.values().iter().map(|&x| x as i64));
            let v = b
                .column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("payload must stay Float64");
            narrow_payload.extend(v.values());
        }
        assert_eq!(narrow_keys, keys, "narrow path must equal wide path");
        assert_eq!(narrow_payload, payload, "payload untouched");
        let after = NARROW_KEY_DECODES.load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            after >= before + 3,
            "downcast decode must have fired once per RG (3 RGs): {before} → {after}"
        );
    }

    /// Result identity on the masked (filter) path: same narrowed leaf,
    /// with a low-pass-rate filter so the masked-gather branch runs.
    #[test]
    fn narrow_dec_masked_reader_matches_wide_path() {
        use crate::ematix_fast_parquet::{ColumnPredicate, F64RangeClause};
        use datafusion::logical_expr::Operator;

        let path = tmp_parquet("narrow_masked");
        let n = 999;
        let (keys, _) = write_narrow_fixture(&path, n);
        // Filter on the PAYLOAD column: v < 5.0 → 20 of RG0's 333 rows
        // (6%) pass — under the 10% masked/dense threshold, so the
        // masked-gather branch (not the inexact dense fallback) runs —
        // while the narrowed KEY is a projected column. RG1/RG2 have
        // zero matches (bitmap short-circuit).
        let mk_filter = || {
            BridgeFilter::new(vec![ColumnPredicate::F64Range {
                col_idx: 1,
                clauses: vec![F64RangeClause {
                    op: Operator::Lt,
                    literal_f64: 5.0,
                }],
            }])
        };
        let expected: Vec<i64> = keys
            .iter()
            .enumerate()
            .filter(|&(i, _)| (i as f64 * 0.25) < 5.0)
            .map(|(_, &k)| k)
            .collect();

        // Wide masked (control).
        let file = ParquetFile::open(&path).unwrap();
        let wide = EmatArrowBatchReaderBuilder::new(file, narrow_fixture_schema())
            .with_filter(mk_filter(), path.clone())
            .build()
            .unwrap();
        let mut wide_keys: Vec<i64> = Vec::new();
        for b in wide {
            let b = b.unwrap();
            let k = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            wide_keys.extend(k.values());
        }
        assert_eq!(wide_keys, expected, "wide masked control");

        // Narrow masked.
        let file = ParquetFile::open(&path).unwrap();
        let narrow = EmatArrowBatchReaderBuilder::new(file, narrow_fixture_schema())
            .with_filter(mk_filter(), path.clone())
            .with_narrow_i64_leaves(vec![0])
            .build()
            .unwrap();
        let mut narrow_keys: Vec<i64> = Vec::new();
        for b in narrow {
            let b = b.unwrap();
            let k = b
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("narrowed leaf must emit Int32Array on the masked path");
            narrow_keys.extend(k.values().iter().map(|&x| x as i64));
        }
        assert_eq!(narrow_keys, expected, "narrow masked must equal wide");
    }

    /// A leaf declared narrow whose data does NOT fit i32 must error
    /// loudly (checked narrow), never wrap.
    #[test]
    fn narrow_dec_out_of_range_leaf_errors() {
        let path = tmp_parquet("narrow_overflow");
        let big: Vec<i64> = vec![0, 1, i32::MAX as i64 + 10, 3];
        write_table_to_path(
            &path,
            &[("k_key", ColumnData::I64(&big))],
            CompressionCodec::Uncompressed,
        )
        .unwrap();
        let file = ParquetFile::open(&path).unwrap();
        let reader = EmatArrowBatchReaderBuilder::new(
            file,
            Arc::new(Schema::new(vec![Field::new(
                "k_key",
                DataType::Int64,
                false,
            )])),
        )
        .with_narrow_i64_leaves(vec![0])
        .build()
        .unwrap();
        let results: Vec<DfResult<RecordBatch>> = reader.collect();
        assert!(
            results.iter().any(|r| r.is_err()),
            "out-of-i32 data under a narrow-declared leaf must error, not wrap"
        );
    }

    fn write_three_primitives(path: &std::path::Path, n: usize) {
        let i32s: Vec<i32> = (0..n as i32).collect();
        let i64s: Vec<i64> = (0..n as i64).map(|x| x * 7).collect();
        let f64s: Vec<f64> = (0..n).map(|x| x as f64 * 0.5).collect();
        write_table_to_path(
            path,
            &[
                ("c_i32", ColumnData::I32(&i32s)),
                ("c_i64", ColumnData::I64(&i64s)),
                ("c_f64", ColumnData::F64(&f64s)),
            ],
            CompressionCodec::Uncompressed,
        )
        .unwrap();
    }

    fn schema_three_primitives() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("c_i32", DataType::Int32, false),
            Field::new("c_i64", DataType::Int64, false),
            Field::new("c_f64", DataType::Float64, false),
        ]))
    }

    #[test]
    fn rev23_masked_dense_gate_calibration() {
        // Calibrated on TPC-H SF=10: dense-winners all ≥0.15 pass rate,
        // masked-winners all ≤0.043 — a clean gap. Default threshold 0.10
        // must route the winners to dense and keep the losers on the masked
        // gather. See [[rev20-q07-q08-decode-bound]] REV.23.
        let t = 0.10;
        // dense-winners (route to dense):
        assert!(should_route_masked_to_dense(304, 1000, t), "Q07/Q08 ~30%");
        assert!(should_route_masked_to_dense(250, 1000, t), "Q10 ~25%");
        assert!(should_route_masked_to_dense(150, 1000, t), "Q16/Q20 ~15%");
        assert!(should_route_masked_to_dense(486, 1000, t), "Q03 ~49%");
        // masked-winners (keep masked):
        assert!(!should_route_masked_to_dense(43, 1000, t), "Q12 ~4.3%");
        assert!(!should_route_masked_to_dense(36, 1000, t), "Q19 ~3.6%");
        assert!(!should_route_masked_to_dense(1, 1000, t), "Q17 ~0.1%");
        // empty RG never routes
        assert!(!should_route_masked_to_dense(0, 0, t));
        // strict boundary: exactly 10% stays masked, just above flips
        assert!(!should_route_masked_to_dense(100, 1000, t));
        assert!(should_route_masked_to_dense(101, 1000, t));
        // the calibrated gap (0.043, 0.148) — the default threshold lands in it
        assert!(0.043 < t && t < 0.148);
    }

    #[test]
    fn rev23_threshold_default_is_calibrated() {
        // Only assert the default when the override env is unset (normal run).
        if std::env::var_os("EMAT_MASKED_DENSE_PASSRATE").is_none() {
            assert_eq!(masked_dense_passrate_threshold(), 0.10);
        }
    }

    #[test]
    fn roundtrip_i32_i64_f64() {
        let path = tmp_parquet("roundtrip");
        let n = 1024;
        write_three_primitives(&path, n);

        let file = ParquetFile::open(&path).unwrap();
        let reader = EmatArrowBatchReaderBuilder::new(file, schema_three_primitives())
            .build()
            .unwrap();
        let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, n);
        assert_eq!(batches[0].num_columns(), 3);
        // Spot-check first batch values.
        let i32_arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..i32_arr.len() {
            assert_eq!(i32_arr.value(i), i as i32);
        }
        let f64_arr = batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(f64_arr.value(0), 0.0);
        assert_eq!(f64_arr.value(1), 0.5);
    }

    #[test]
    fn roundtrip_lz4_raw() {
        // Guards the production reader's LZ4_RAW decode (the `Lz4Raw` arm +
        // `decompress_into_sized`). Before this the reader hard-errored on
        // LZ4_RAW parquet ("codec Lz4Raw not yet supported"). Multiple row
        // groups exercise both the first-page and subsequent-page decompress
        // callsites; every value is verified (not just a spot-check).
        let path = tmp_parquet("roundtrip_lz4");
        let n = 100_000usize;
        let i32s: Vec<i32> = (0..n as i32).collect();
        let i64s: Vec<i64> = (0..n as i64).map(|x| x * 7).collect();
        let f64s: Vec<f64> = (0..n).map(|x| x as f64 * 0.5).collect();
        write_table_to_path_with_row_group_size(
            &path,
            &[
                ("c_i32", ColumnData::I32(&i32s)),
                ("c_i64", ColumnData::I64(&i64s)),
                ("c_f64", ColumnData::F64(&f64s)),
            ],
            CompressionCodec::Lz4Raw,
            40_000, // → 3 row groups
        )
        .unwrap();

        let file = ParquetFile::open(&path).unwrap();
        let reader = EmatArrowBatchReaderBuilder::new(file, schema_three_primitives())
            .build()
            .unwrap();
        let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, n, "LZ4_RAW decode must yield every row");

        let mut gi = 0usize;
        for b in &batches {
            let c0 = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            let c1 = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
            let c2 = b.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
            for r in 0..b.num_rows() {
                assert_eq!(c0.value(r), i32s[gi]);
                assert_eq!(c1.value(r), i64s[gi]);
                assert_eq!(c2.value(r), f64s[gi]);
                gi += 1;
            }
        }
        assert_eq!(gi, n, "LZ4_RAW decode must yield every value in order");
    }

    #[test]
    fn streaming_emits_65k_chunks() {
        let path = tmp_parquet("streaming");
        // > 65_536 rows in one RG. Write a single-column file (lighter).
        let n: usize = 200_000;
        let xs: Vec<i32> = (0..n as i32).collect();
        write_table_to_path(
            &path,
            &[("c", ColumnData::I32(&xs))],
            CompressionCodec::Uncompressed,
        )
        .unwrap();

        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("c", DataType::Int32, false)]));
        let file = ParquetFile::open(&path).unwrap();
        let reader = EmatArrowBatchReaderBuilder::new(file, schema)
            .build()
            .unwrap();
        let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
        assert!(
            batches.len() > 1,
            "expected > 1 batch, got {}",
            batches.len()
        );
        // All but the last should be exactly 65_536 rows.
        for b in &batches[..batches.len() - 1] {
            assert_eq!(b.num_rows(), DEFAULT_BATCH_SIZE);
        }
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), n);
    }

    #[test]
    fn utf8view_promotion() {
        let path = tmp_parquet("utf8view");
        // 3 distinct strings, repeated; dict-encoded.
        let raw = [b"alpha".as_slice(), b"beta".as_slice(), b"gamma".as_slice()];
        let mut rows: Vec<&[u8]> = Vec::with_capacity(300);
        for i in 0..300 {
            rows.push(raw[i % 3]);
        }
        write_table_with_dict_to_path(
            &path,
            &[("s", ColumnData::ByteArray(&rows))],
            CompressionCodec::Uncompressed,
            usize::MAX,
            &[true],
        )
        .unwrap();

        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Utf8View,
            false,
        )]));
        let file = ParquetFile::open(&path).unwrap();
        let reader = EmatArrowBatchReaderBuilder::new(file, schema)
            .build()
            .unwrap();
        let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 300);

        // Concatenate values and compare row-wise.
        let mut got: Vec<String> = Vec::with_capacity(300);
        for b in &batches {
            let arr = b
                .column(0)
                .as_any()
                .downcast_ref::<StringViewArray>()
                .expect("expected StringViewArray for Utf8View target");
            for i in 0..arr.len() {
                got.push(arr.value(i).to_string());
            }
        }
        for (i, g) in got.iter().enumerate() {
            assert_eq!(g.as_bytes(), raw[i % 3], "mismatch at row {i}");
        }
    }

    /// Σ.E5.1.d: dict-encoded BYTE_ARRAY → StringView must match an
    /// independently-computed expected `StringViewArray` value-for-
    /// value. Exercises the new gather-from-dict-views fast path
    /// (Q1's `l_returnflag` / `l_linestatus` shape — short strings
    /// inlined in the view).
    #[test]
    fn utf8view_dict_encoded_decode_matches_plain() {
        let path = tmp_parquet("utf8view_dict");
        // Mix of inline (≤12B) and long (>12B) entries to exercise
        // both `make_view` branches inside the dict-views build loop.
        // Q1's returnflag/linestatus are 1-byte; we add longer
        // strings too so the data buffer path is also tested.
        let raw: [&[u8]; 4] = [
            b"R",
            b"AB",
            b"long_string_more_than_twelve_bytes_yo",
            b"another_long_one",
        ];
        let n = 5_000usize;
        let rows: Vec<&[u8]> = (0..n).map(|i| raw[i % raw.len()]).collect();
        // Repeat values heavily → writer emits RLE_DICTIONARY data
        // pages, exercising the fast path.
        write_table_with_dict_to_path(
            &path,
            &[("s", ColumnData::ByteArray(&rows))],
            CompressionCodec::Uncompressed,
            usize::MAX,
            &[true],
        )
        .unwrap();

        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Utf8View,
            false,
        )]));
        let file = ParquetFile::open(&path).unwrap();
        let reader = EmatArrowBatchReaderBuilder::new(file, schema)
            .build()
            .unwrap();
        let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();

        // Expected StringViewArray from the row sequence the writer
        // saw. Built via the standard arrow-array path so we know
        // it's correct regardless of how we built ours.
        let expected_strs: Vec<&str> = rows
            .iter()
            .map(|s| std::str::from_utf8(s).unwrap())
            .collect();
        let expected = StringViewArray::from(expected_strs);

        // Concatenate every batch row-by-row and compare against
        // `expected`. We can't use array-level equality because we
        // emit one StringViewArray per batch (one per RG slice) and
        // `expected` is one array — but the row sequence is locked.
        let mut got_idx = 0usize;
        for b in &batches {
            let arr = b
                .column(0)
                .as_any()
                .downcast_ref::<StringViewArray>()
                .unwrap();
            for i in 0..arr.len() {
                assert_eq!(
                    arr.value(i),
                    expected.value(got_idx),
                    "row {got_idx}: got {:?}, expected {:?}",
                    arr.value(i),
                    expected.value(got_idx),
                );
                got_idx += 1;
            }
        }
        assert_eq!(got_idx, n);
    }

    #[test]
    fn dictionary_preservation() {
        let path = tmp_parquet("dict");
        let raw = [b"R".as_slice(), b"A".as_slice(), b"N".as_slice()];
        let mut rows: Vec<&[u8]> = Vec::with_capacity(900);
        for i in 0..900 {
            rows.push(raw[i % 3]);
        }
        write_table_with_dict_to_path(
            &path,
            &[("flag", ColumnData::ByteArray(&rows))],
            CompressionCodec::Uncompressed,
            usize::MAX,
            &[true],
        )
        .unwrap();

        let dict_ty = DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8));
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("flag", dict_ty, false)]));
        let file = ParquetFile::open(&path).unwrap();
        let reader = EmatArrowBatchReaderBuilder::new(file, schema)
            .build()
            .unwrap();

        let mut total = 0usize;
        let mut row_idx = 0usize;
        for b in reader {
            let b = b.unwrap();
            let dict_arr = b
                .column(0)
                .as_any()
                .downcast_ref::<DictionaryArray<UInt32Type>>()
                .expect("expected DictionaryArray<UInt32Type>");
            let values = dict_arr.values().as_string::<i32>();
            // Sanity: dict values should be small (3 distinct).
            assert!(values.len() <= 8);
            let keys = dict_arr.keys();
            for i in 0..keys.len() {
                let k = keys.value(i) as usize;
                let s = values.value(k);
                assert_eq!(s.as_bytes(), raw[row_idx % 3]);
                row_idx += 1;
            }
            total += b.num_rows();
        }
        assert_eq!(total, 900);
    }

    #[test]
    fn projection_subset() {
        // 5 columns, project 2 (indices 1 + 3).
        let path = tmp_parquet("projection");
        let n = 128usize;
        let a: Vec<i32> = (0..n as i32).collect();
        let b: Vec<i32> = (0..n as i32).map(|x| x + 1000).collect();
        let c: Vec<i64> = (0..n as i64).collect();
        let d: Vec<f64> = (0..n).map(|x| x as f64).collect();
        let e: Vec<i32> = (0..n as i32).map(|x| x - 50).collect();
        write_table_to_path(
            &path,
            &[
                ("a", ColumnData::I32(&a)),
                ("b", ColumnData::I32(&b)),
                ("c", ColumnData::I64(&c)),
                ("d", ColumnData::F64(&d)),
                ("e", ColumnData::I32(&e)),
            ],
            CompressionCodec::Uncompressed,
        )
        .unwrap();

        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Int32, false),
            Field::new("d", DataType::Float64, false),
        ]));
        let file = ParquetFile::open(&path).unwrap();
        let reader = EmatArrowBatchReaderBuilder::new(file, schema)
            .with_projection(vec![1, 3])
            .build()
            .unwrap();
        let batches: Vec<RecordBatch> = reader.map(|x| x.unwrap()).collect();
        assert_eq!(batches.len(), 1);
        let rb = &batches[0];
        assert_eq!(rb.num_columns(), 2);
        let b_arr = rb.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let d_arr = rb
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(b_arr.value(0), 1000);
        assert_eq!(d_arr.value(10), 10.0);
    }

    #[test]
    fn row_group_selection() {
        let path = tmp_parquet("rg_select");
        // 3 RGs of 100 rows each.
        let n_per_rg = 100usize;
        let n = n_per_rg * 3;
        let xs: Vec<i32> = (0..n as i32).collect();
        write_table_to_path_with_row_group_size(
            &path,
            &[("c", ColumnData::I32(&xs))],
            CompressionCodec::Uncompressed,
            n_per_rg,
        )
        .unwrap();

        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("c", DataType::Int32, false)]));
        let file = ParquetFile::open(&path).unwrap();
        let reader = EmatArrowBatchReaderBuilder::new(file, schema)
            .with_row_groups(vec![1])
            .build()
            .unwrap();
        let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, n_per_rg);
        // RG 1 should hold values 100..200.
        let arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(arr.value(0), 100);
        assert_eq!(arr.value(arr.len() - 1), 199);
    }

    #[test]
    fn crosses_row_group_boundary_safely() {
        let path = tmp_parquet("rg_boundary");
        // 2 RGs of 100_000 rows each. batch_size = 65_536 < RG size.
        let n_per_rg = 100_000usize;
        let n = n_per_rg * 2;
        let xs: Vec<i32> = (0..n as i32).collect();
        write_table_to_path_with_row_group_size(
            &path,
            &[("c", ColumnData::I32(&xs))],
            CompressionCodec::Uncompressed,
            n_per_rg,
        )
        .unwrap();

        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("c", DataType::Int32, false)]));
        let file = ParquetFile::open(&path).unwrap();
        let reader = EmatArrowBatchReaderBuilder::new(file, schema)
            .build()
            .unwrap();
        let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, n);
        // Each batch must come from a single RG, i.e. its first and
        // last values must lie in the same `[k * n_per_rg, (k+1)*
        // n_per_rg)` window. With the row-id pattern xs[i] = i this
        // means `first / n_per_rg == last / n_per_rg`.
        for b in &batches {
            let arr = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            let first = arr.value(0) as usize;
            let last = arr.value(arr.len() - 1) as usize;
            assert_eq!(
                first / n_per_rg,
                last / n_per_rg,
                "batch crosses RG boundary: first={first} last={last}"
            );
        }
    }

    #[test]
    fn utf8view_zero_copy_or_efficient() {
        // For N rows of avg length L, the backing buffer size should
        // be bounded by ~dict_unique_bytes + a small fixed overhead.
        // We assert it's at most N * L (the worst case of *every* row
        // copied into the buffer). For a dict-encoded column with K
        // distinct values, it should be far less.
        let path = tmp_parquet("utf8view_efficient");
        let n = 50_000usize;
        // Use long strings (> 12 bytes) so views point at the backing
        // buffer rather than inlining. Without dict reuse, copying
        // every row would burn n * 20 bytes ≈ 1 MB; with dict reuse
        // it should be 3 * 20 + change.
        let raw = [
            b"alpha_quite_long_string".as_slice(),
            b"beta_quite_long_string".as_slice(),
            b"gamma_quite_long_string".as_slice(),
        ];
        let avg_len: usize = raw.iter().map(|s| s.len()).sum::<usize>() / raw.len();
        let mut rows: Vec<&[u8]> = Vec::with_capacity(n);
        for i in 0..n {
            rows.push(raw[i % raw.len()]);
        }
        write_table_with_dict_to_path(
            &path,
            &[("s", ColumnData::ByteArray(&rows))],
            CompressionCodec::Uncompressed,
            usize::MAX,
            &[true],
        )
        .unwrap();

        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Utf8View,
            false,
        )]));
        let file = ParquetFile::open(&path).unwrap();
        let reader = EmatArrowBatchReaderBuilder::new(file, schema)
            .build()
            .unwrap();
        let mut total_backing_bytes = 0usize;
        let mut total_rows = 0usize;
        for b in reader {
            let b = b.unwrap();
            total_rows += b.num_rows();
            let arr = b
                .column(0)
                .as_any()
                .downcast_ref::<StringViewArray>()
                .unwrap();
            // data_buffers() returns the backing blocks; each slice
            // shares the same Arc<Buffer> so we count unique ones
            // by pointer identity.
            for buf in arr.data_buffers() {
                total_backing_bytes += buf.len();
            }
        }
        assert_eq!(total_rows, n);
        // We share one data block per RG across all slices of that
        // RG, but each slice clones the Buffer (`Arc` bump) into its
        // own array — so the sum here counts the block once per
        // *slice*, not once per *RG*. The relevant upper bound is
        // `(slice_count) * dict_unique_bytes`, comfortably <
        // `n * avg_len` for any K << n.
        let dict_unique_bytes: usize = raw.iter().map(|s| s.len()).sum();
        let upper_bound_per_slice = dict_unique_bytes + 64; // overhead slack
        let n_slices = n.div_ceil(DEFAULT_BATCH_SIZE);
        let upper_bound = upper_bound_per_slice * n_slices;
        assert!(
            total_backing_bytes <= upper_bound,
            "total backing bytes {total_backing_bytes} > upper bound {upper_bound} \
             (dict_unique={dict_unique_bytes}, slices={n_slices}, avg_len={avg_len}, n={n})"
        );
        // Crisper sanity: total backing bytes per row is « avg_len.
        assert!(
            total_backing_bytes < n * avg_len,
            "backing bytes {total_backing_bytes} reached row-by-row materialisation bound"
        );
    }

    /// Σ.E5.1 follow-up: parallel per-column decode must produce
    /// identical output to its own previous run (no race in the
    /// scoped-thread implementation) and identical output across
    /// batch-size variants.
    ///
    /// Fixture mixes every supported `DecodedColumn` variant —
    /// Int32, Int64, Float64, Utf8View, Dictionary(UInt32, Utf8) —
    /// so each decode path runs concurrently with the others.
    #[test]
    fn parallel_decode_equivalence() {
        let path = tmp_parquet("parallel_eq");
        let n = 12_000usize;
        let c_i32: Vec<i32> = (0..n as i32).collect();
        let c_i64: Vec<i64> = (0..n as i64).map(|x| x * 11).collect();
        let c_f64: Vec<f64> = (0..n).map(|x| x as f64 * 0.25).collect();
        let s_pool = [
            b"alpha_long_string".as_slice(),
            b"beta_long_string".as_slice(),
            b"gamma_long_string".as_slice(),
            b"delta_long_string".as_slice(),
        ];
        let s_view: Vec<&[u8]> = (0..n).map(|i| s_pool[i % s_pool.len()]).collect();
        let d_pool = [b"R".as_slice(), b"A".as_slice(), b"N".as_slice()];
        let s_dict: Vec<&[u8]> = (0..n).map(|i| d_pool[i % d_pool.len()]).collect();

        write_table_with_dict_to_path(
            &path,
            &[
                ("c_i32", ColumnData::I32(&c_i32)),
                ("c_i64", ColumnData::I64(&c_i64)),
                ("c_f64", ColumnData::F64(&c_f64)),
                ("s_view", ColumnData::ByteArray(&s_view)),
                ("s_dict", ColumnData::ByteArray(&s_dict)),
            ],
            CompressionCodec::Uncompressed,
            usize::MAX,
            // Force dict encoding on both byte_array columns so the
            // DictUtf8 + StringView decode paths both run.
            &[false, false, false, true, true],
        )
        .unwrap();

        let dict_ty = DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8));
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("c_i32", DataType::Int32, false),
            Field::new("c_i64", DataType::Int64, false),
            Field::new("c_f64", DataType::Float64, false),
            Field::new("s_view", DataType::Utf8View, false),
            Field::new("s_dict", dict_ty, false),
        ]));

        // Snapshot the rows of every column into plain Vecs for a
        // batch-size-independent equality check.
        type Snapshot = (Vec<i32>, Vec<i64>, Vec<f64>, Vec<String>, Vec<String>);
        fn collect_rows(batches: &[RecordBatch]) -> Snapshot {
            let mut a = Vec::new();
            let mut b = Vec::new();
            let mut c = Vec::new();
            let mut d = Vec::new();
            let mut e = Vec::new();
            for rb in batches {
                let i32_arr = rb.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
                let i64_arr = rb.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
                let f64_arr = rb
                    .column(2)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                let sv = rb
                    .column(3)
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .unwrap();
                let dict = rb
                    .column(4)
                    .as_any()
                    .downcast_ref::<DictionaryArray<UInt32Type>>()
                    .unwrap();
                let dict_vals = dict.values().as_string::<i32>();
                let keys = dict.keys();
                for i in 0..rb.num_rows() {
                    a.push(i32_arr.value(i));
                    b.push(i64_arr.value(i));
                    c.push(f64_arr.value(i));
                    d.push(sv.value(i).to_string());
                    e.push(dict_vals.value(keys.value(i) as usize).to_string());
                }
            }
            (a, b, c, d, e)
        }

        fn read_with_batch_size(
            path: &std::path::Path,
            schema: SchemaRef,
            batch_size: usize,
        ) -> Vec<RecordBatch> {
            let file = ParquetFile::open(path).unwrap();
            let reader = EmatArrowBatchReaderBuilder::new(file, schema)
                .with_batch_size(batch_size)
                .build()
                .unwrap();
            reader.map(|b| b.unwrap()).collect()
        }

        // Read twice with the default 65_536 batch size to catch any
        // race in the scoped-thread path.
        let r1 = read_with_batch_size(&path, schema.clone(), DEFAULT_BATCH_SIZE);
        let r2 = read_with_batch_size(&path, schema.clone(), DEFAULT_BATCH_SIZE);
        assert_eq!(collect_rows(&r1), collect_rows(&r2));

        // And cross-batch-size: the row sequence must be identical
        // regardless of slicing.
        let r_small = read_with_batch_size(&path, schema.clone(), 1024);
        let r_large = read_with_batch_size(&path, schema.clone(), 65_536);
        let g = collect_rows(&r1);
        assert_eq!(g, collect_rows(&r_small));
        assert_eq!(g, collect_rows(&r_large));

        // Spot-check the actual values too — guards against a subtler
        // bug where every read produces the same wrong answer.
        assert_eq!(g.0.len(), n);
        assert_eq!(g.0[0], 0);
        assert_eq!(g.0[n - 1], (n - 1) as i32);
        assert_eq!(g.1[7], 7 * 11);
        assert_eq!(g.2[4], 1.0);
        assert_eq!(g.3[0].as_bytes(), s_pool[0]);
        assert_eq!(g.3[5].as_bytes(), s_pool[5 % s_pool.len()]);
        assert_eq!(g.4[0].as_bytes(), d_pool[0]);
        assert_eq!(g.4[8].as_bytes(), d_pool[8 % d_pool.len()]);
    }

    /// Σ.E5.1.c: with `parallelism_budget = 1` (sequential per-RG
    /// column decode), every variant must produce byte-identical
    /// output to the default `available_parallelism()`-saturating
    /// path. Same fixture as `parallel_decode_equivalence`.
    #[test]
    fn parallelism_budget_one_matches_default() {
        let path = tmp_parquet("budget_one_eq");
        let n = 8_000usize;
        let c_i32: Vec<i32> = (0..n as i32).collect();
        let c_i64: Vec<i64> = (0..n as i64).map(|x| x * 13).collect();
        let c_f64: Vec<f64> = (0..n).map(|x| x as f64 * 0.125).collect();
        let s_pool = [
            b"alpha_long_string".as_slice(),
            b"beta_long_string".as_slice(),
            b"gamma_long_string".as_slice(),
        ];
        let s_view: Vec<&[u8]> = (0..n).map(|i| s_pool[i % s_pool.len()]).collect();

        write_table_with_dict_to_path(
            &path,
            &[
                ("c_i32", ColumnData::I32(&c_i32)),
                ("c_i64", ColumnData::I64(&c_i64)),
                ("c_f64", ColumnData::F64(&c_f64)),
                ("s_view", ColumnData::ByteArray(&s_view)),
            ],
            CompressionCodec::Uncompressed,
            usize::MAX,
            &[false, false, false, true],
        )
        .unwrap();

        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("c_i32", DataType::Int32, false),
            Field::new("c_i64", DataType::Int64, false),
            Field::new("c_f64", DataType::Float64, false),
            Field::new("s_view", DataType::Utf8View, false),
        ]));

        let read = |budget: Option<usize>| -> Vec<RecordBatch> {
            let file = ParquetFile::open(&path).unwrap();
            let mut b = EmatArrowBatchReaderBuilder::new(file, schema.clone());
            if let Some(n) = budget {
                b = b.with_parallelism_budget(n);
            }
            b.build().unwrap().map(|x| x.unwrap()).collect()
        };

        fn snap(batches: &[RecordBatch]) -> (Vec<i32>, Vec<i64>, Vec<f64>, Vec<String>) {
            let mut a = Vec::new();
            let mut b = Vec::new();
            let mut c = Vec::new();
            let mut d = Vec::new();
            for rb in batches {
                let i32_arr = rb.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
                let i64_arr = rb.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
                let f64_arr = rb
                    .column(2)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                let sv = rb
                    .column(3)
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .unwrap();
                for i in 0..rb.num_rows() {
                    a.push(i32_arr.value(i));
                    b.push(i64_arr.value(i));
                    c.push(f64_arr.value(i));
                    d.push(sv.value(i).to_string());
                }
            }
            (a, b, c, d)
        }

        let default = snap(&read(None));
        let budget_1 = snap(&read(Some(1)));
        let budget_2 = snap(&read(Some(2)));
        assert_eq!(default, budget_1, "budget=1 must match default output");
        assert_eq!(default, budget_2, "budget=2 must match default output");
    }

    // ----- Σ.O.c.1 — RowGroupDecodeCache wire-in -----

    fn read_all_with_rg_cache(
        path: &std::path::Path,
        schema: SchemaRef,
        cache: std::sync::Arc<RowGroupDecodeCache>,
    ) -> Vec<RecordBatch> {
        let file = ParquetFile::open(path).unwrap();
        let mut rdr = EmatArrowBatchReaderBuilder::new(file, schema)
            .with_rg_decode_cache(cache)
            .build()
            .unwrap();
        // Set path so `rg_cache_key` is populated (mirrors how the
        // provider wires the reader). The reader uses `path` for the
        // RG cache key.
        rdr.path = Some(path.to_path_buf());
        let mut out = Vec::new();
        while let Some(b) = rdr.next().transpose().unwrap() {
            out.push(b);
        }
        out
    }

    #[test]
    fn rg_decode_cache_returns_identical_rows_on_hit() {
        let path = tmp_parquet("rg_cache_identity");
        let n = 4096;
        write_three_primitives(&path, n);

        let cache = std::sync::Arc::new(RowGroupDecodeCache::new());

        // First read: cache miss → decode + insert.
        let first = read_all_with_rg_cache(&path, schema_three_primitives(), cache.clone());
        let (h1, m1, _) = cache.stats();

        // Second read: should hit the cache for every RG.
        let second = read_all_with_rg_cache(&path, schema_three_primitives(), cache.clone());
        let (h2, m2, _) = cache.stats();

        // Sanity: first run inserted at least one entry; second run hit.
        assert!(m1 >= 1, "first read should miss + insert (misses={m1})");
        assert!(h2 > h1, "second read should produce hits (h1={h1} h2={h2})");
        // Misses shouldn't grow on the second read (same key set).
        assert_eq!(m1, m2, "second read should produce no new misses");

        // Output equivalence: collect (i32, i64, f64) tuples and compare.
        fn collect(rbs: &[RecordBatch]) -> Vec<(i32, i64, f64)> {
            let mut out = Vec::new();
            for rb in rbs {
                let a = rb.column(0).as_primitive::<arrow_array::types::Int32Type>();
                let b = rb.column(1).as_primitive::<arrow_array::types::Int64Type>();
                let c = rb
                    .column(2)
                    .as_primitive::<arrow_array::types::Float64Type>();
                for i in 0..rb.num_rows() {
                    out.push((a.value(i), b.value(i), c.value(i)));
                }
            }
            out
        }
        assert_eq!(collect(&first), collect(&second));
    }

    #[test]
    fn rg_decode_cache_default_is_inactive() {
        // No cache installed → reader behaves like before; this just
        // confirms the new field is optional and doesn't perturb the
        // dense-decode path.
        let path = tmp_parquet("rg_cache_none");
        write_three_primitives(&path, 256);
        let file = ParquetFile::open(&path).unwrap();
        let mut rdr = EmatArrowBatchReaderBuilder::new(file, schema_three_primitives())
            .build()
            .unwrap();
        assert!(rdr.rg_decode_cache.is_none());
        while rdr.next().transpose().unwrap().is_some() {}
    }

    #[test]
    fn rg_decode_cache_evicts_when_capacity_exceeded() {
        // Tiny cap → entry should be skipped or evicted.
        let path = tmp_parquet("rg_cache_evict");
        write_three_primitives(&path, 256);
        let cache = std::sync::Arc::new(RowGroupDecodeCache::with_capacity_bytes(8));
        let _ = read_all_with_rg_cache(&path, schema_three_primitives(), cache.clone());
        // Entry is larger than cap → skipped, cache stays empty.
        assert_eq!(cache.len(), 0);
    }

    /// Σ.AI.6d — `clear()` (the mem_pressure cache-shed action) empties
    /// the cache and zeroes bytes_used, and a subsequent read repopulates
    /// with identical rows (the cache is a pure accelerator).
    #[test]
    fn rg_decode_cache_clear_empties_and_repopulates_identically() {
        let path = tmp_parquet("rg_cache_clear");
        write_three_primitives(&path, 4096);
        let cache = std::sync::Arc::new(RowGroupDecodeCache::new());

        let before = read_all_with_rg_cache(&path, schema_three_primitives(), cache.clone());
        assert!(!cache.is_empty(), "warm read populates the cache");

        cache.clear();
        assert_eq!(cache.len(), 0, "clear() empties the entries");
        let (_, _, bytes) = cache.stats();
        assert_eq!(bytes, 0, "clear() returns every byte");

        let (_, m_before, _) = cache.stats();
        let after = read_all_with_rg_cache(&path, schema_three_primitives(), cache.clone());
        let (_, m_after, _) = cache.stats();
        assert!(m_after > m_before, "post-clear read re-decodes (misses)");
        assert!(!cache.is_empty(), "post-clear read repopulates");
        assert_eq!(
            format!("{before:?}"),
            format!("{after:?}"),
            "cleared-and-repopulated rows are identical"
        );
    }

    // ----- Σ.AI.6e — RG cache retention (segmented LRU, second-touch
    // admission). Deterministic unit benches: synthetic keys/columns,
    // no wall clock — the asserts are HIT RATES, not times. -----

    /// One synthetic entry = 1 KiB Int64 column (Arc'd Buffer, same
    /// shape `estimate_column_bytes` sees in production).
    const RG_SYNTH_ENTRY_BYTES: usize = 1024;

    fn rg_synth_col() -> DecodedColumn {
        DecodedColumn::Int64 {
            data: Buffer::from(vec![0u8; RG_SYNTH_ENTRY_BYTES]),
            n_rows: RG_SYNTH_ENTRY_BYTES / 8,
        }
    }

    fn rg_synth_id(id: usize) -> RgCacheKey {
        RgCacheKey {
            file_path: std::path::PathBuf::from("/synthetic/rg-retention-bench"),
            row_group_idx: id,
            leaf_idx: 0,
        }
    }

    /// Touch entry `id`: `true` = hit, `false` = miss (then insert —
    /// exactly what `load_row_group_dense` does per projected leaf).
    fn rg_touch(cache: &RowGroupDecodeCache, id: usize) -> bool {
        let key = rg_synth_id(id);
        if cache.get(&key).is_some() {
            true
        } else {
            cache.insert(key, rg_synth_col());
            false
        }
    }

    /// 64-entry policy cache (64 × 1 KiB), explicit env-independent policy.
    fn rg_policy_cache(retention: bool) -> RowGroupDecodeCache {
        RowGroupDecodeCache::with_capacity_bytes_and_retention(64 * RG_SYNTH_ENTRY_BYTES, retention)
    }

    /// Σ.AI.6e step-2 reproduction — the settled Q09 SF=100 mechanism:
    /// seed K ≤ capacity, then stream 2×capacity sequential entries
    /// repeatedly. Legacy FIFO: every repeat pass yields ZERO hits (a
    /// scan whose insert traffic exceeds capacity evicts the seeded
    /// set in insertion order even while it is being hit). Retention:
    /// the twice-touched seed is protected and keeps hitting.
    #[test]
    fn rg_cache_sequential_overcap_stream_fifo_thrash_vs_retention() {
        // Legacy FIFO — pins the pathology.
        let fifo = rg_policy_cache(false);
        for id in 0..32 {
            rg_touch(&fifo, id); // seed K=32 ≤ capacity 64
        }
        let mut pass_hits = [0usize; 3];
        for hits in &mut pass_hits {
            for id in 0..128 {
                // 2× capacity, sequential
                if rg_touch(&fifo, id) {
                    *hits += 1;
                }
            }
        }
        assert_eq!(
            pass_hits[2], 0,
            "FIFO repeat pass must yield zero hits (thrash): {pass_hits:?}"
        );
        let (admits, rejects, prot) = fifo.retention_stats();
        assert_eq!(
            (admits, rejects, prot),
            (0, 0, 0),
            "legacy mode: retention probes stay zero"
        );

        // Retention — pass 1 re-touches the seed (second touch →
        // protected); the flood churns probation only. Steady state:
        // the full seed keeps hitting every pass.
        let ret = rg_policy_cache(true);
        for id in 0..32 {
            rg_touch(&ret, id);
        }
        let mut final_seed_hits = 0usize;
        for pass in 0..3 {
            for id in 0..128 {
                let hit = rg_touch(&ret, id);
                if pass == 2 && id < 32 && hit {
                    final_seed_hits += 1;
                }
            }
        }
        assert!(
            final_seed_hits * 10 >= 32 * 9,
            "retention must keep ≥90% of the seeded set hitting: {final_seed_hits}/32"
        );
    }

    /// Unit bench (a) — one-pass sequential flood over 2× capacity
    /// with a hot set touched twice beforehand: the hot set must
    /// survive with ≥90% hits on re-touch (FIFO: 0%).
    #[test]
    fn rg_cache_retention_hot_set_survives_one_pass_flood() {
        let run = |cache: &RowGroupDecodeCache| -> usize {
            for id in 0..16 {
                rg_touch(cache, id); // touch 1: insert
            }
            for id in 0..16 {
                assert!(rg_touch(cache, id), "hot set fits: touch 2 hits");
            }
            for id in 1_000..1_128 {
                rg_touch(cache, id); // one-pass flood, 2× capacity
            }
            (0..16).filter(|&id| rg_touch(cache, id)).count()
        };
        let fifo_hits = run(&rg_policy_cache(false));
        assert_eq!(fifo_hits, 0, "FIFO: flood evicts the twice-touched hot set");
        let ret = rg_policy_cache(true);
        let ret_hits = run(&ret);
        assert!(
            ret_hits * 10 >= 16 * 9,
            "retention: hot set survives the flood ≥90%: {ret_hits}/16"
        );
        let (admits, rejects, prot) = ret.retention_stats();
        assert!(admits >= 16, "hot set promoted on second touch");
        assert!(rejects > 0, "flood one-touch entries die in probation");
        assert_eq!(prot, 0, "the flood never reaches the protected segment");
    }

    /// Unit bench (b) — LRU-friendly pattern (repeated small loop <
    /// capacity): retention must MATCH the legacy policy's hit count
    /// exactly (no regression where FIFO was already fine).
    #[test]
    fn rg_cache_retention_matches_legacy_on_lru_friendly_loop() {
        let run = |cache: &RowGroupDecodeCache| -> usize {
            let mut hits = 0;
            for _pass in 0..5 {
                for id in 0..32 {
                    if rg_touch(cache, id) {
                        hits += 1;
                    }
                }
            }
            hits
        };
        let fifo_hits = run(&rg_policy_cache(false));
        let ret_hits = run(&rg_policy_cache(true));
        assert_eq!(
            fifo_hits,
            4 * 32,
            "loop < capacity: all passes after the first hit"
        );
        assert_eq!(
            ret_hits, fifo_hits,
            "retention must not regress the LRU-friendly loop"
        );
    }

    /// Unit bench (c) — Q09-shaped: seed W (the warmup), re-stream W
    /// repeatedly with distinct one-touch noise interleaved — light in
    /// trials 1–2 (the seeding window), heavier once cross-trial
    /// traffic accumulates. Reproduces the observed SF=100 curve:
    /// FIFO's trials 1–2 are fast (warmup-seeded hits), trial 3+
    /// collapse once eviction starts. Retention must hold W at ≥90%
    /// steady-state.
    #[test]
    fn rg_cache_q09_shaped_fifo_collapses_retention_holds() {
        const W: usize = 40;
        // Returns per-trial hit counts on W across 6 trials.
        let run = |cache: &RowGroupDecodeCache| -> Vec<usize> {
            for id in 0..W {
                rg_touch(cache, id); // warmup seed
            }
            let mut noise_id = 10_000;
            let mut trials = Vec::new();
            for trial in 0..6 {
                let per_group = if trial < 2 { 2 } else { 4 };
                let mut hits = 0;
                for id in 0..W {
                    if rg_touch(cache, id) {
                        hits += 1;
                    }
                    if id % 5 == 4 {
                        for _ in 0..per_group {
                            rg_touch(cache, noise_id); // distinct one-touch noise
                            noise_id += 1;
                        }
                    }
                }
                trials.push(hits);
            }
            trials
        };
        let fifo = run(&rg_policy_cache(false));
        let ret_cache = rg_policy_cache(true);
        let ret = run(&ret_cache);
        // FIFO reproduces the observed fast-fast-collapse: trials 1–2
        // ride the seed, steady state goes near-zero.
        assert_eq!(fifo[0], W, "trial 1 rides the warmup seed");
        assert_eq!(fifo[1], W, "trial 2 still rides the seed (pre-eviction)");
        assert!(
            fifo[5] * 10 < W,
            "FIFO steady state must collapse (<10% of W): {fifo:?}"
        );
        // Retention: W stays protected across every trial.
        assert!(
            ret[5] * 10 >= W * 9,
            "retention steady state must hold ≥90% of W: {ret:?}"
        );
        let (admits, rejects, prot) = ret_cache.retention_stats();
        assert!(admits >= W as u64, "W promoted on trial-1 re-touch");
        assert!(rejects > 0, "noise rejected in probation");
        assert_eq!(prot, 0, "noise never evicts protected entries");
    }

    /// Σ.AI.6f RED repro — the AWS SF=100 field freeze
    /// (`retention(admit+0 … prot_evict+0)` + tens of thousands of
    /// rejects on every query after the cache fills): whoever fills
    /// protected FIRST owns it forever. Mechanism: protected only
    /// demotes on promotion overflow; promotion needs a probation
    /// second touch; effective probation (capacity − protected ≈ 1/5)
    /// is smaller than a later query's re-touch distance → no second
    /// touches → no promotions → no demotions → freeze.
    ///
    /// Junk set: 48 entries second-touched into protected (48 KiB of
    /// the 52 KiB protected cap — no overflow demotion, order-lucky).
    /// New live working set: 32 entries looped — re-touch distance 32
    /// exceeds the 16-entry effective probation, so under the frozen policy
    /// it NEVER achieves residency (0 hits on every pass, forever).
    /// The fix (ghost-assisted demotion) must let it reach ≥80% hits
    /// within 8 passes while the never-again-touched junk drains.
    /// Pre-fix per-pass hits: [0,0,0,0,0,0,0,0]; post-fix:
    /// [0, 0, 25, 32, 32, 32, 32, 32].
    #[test]
    fn rg_cache_retention_ghost_breaks_order_lucky_protected_freeze() {
        let cache = rg_policy_cache(true);
        for id in 0..48 {
            rg_touch(&cache, id); // junk touch 1: insert (probation)
        }
        for id in 0..48 {
            assert!(rg_touch(&cache, id), "junk touch 2: promote"); // → protected
        }
        let (admits, _, _) = cache.retention_stats();
        assert_eq!(admits, 48, "junk owns protected before the new set arrives");

        // New working set (ids 100..132) — the only traffic from here
        // on; the junk set is NEVER touched again (it is stale).
        let mut per_pass = Vec::new();
        for _pass in 0..8 {
            let mut hits = 0usize;
            for id in 100..132 {
                if rg_touch(&cache, id) {
                    hits += 1;
                }
            }
            per_pass.push(hits);
        }
        let last = *per_pass.last().unwrap();
        assert!(
            last * 10 >= 32 * 8,
            "a re-touching working set must reclaim protected from a stale \
             one: want ≥80% hits/pass at steady state, got {per_pass:?} \
             (all-zero = the field freeze: no promotions, no demotions, \
             junk owns protected forever)"
        );
        // Drain proof: every junk entry left protected via ghost-forced
        // demotion (48 = the whole stale set), and the new set then
        // earned promotion the normal way (second touch).
        let (ghost_hits, ghost_demotions) = cache.retention_ghost_stats();
        assert!(ghost_hits > 0, "the freeze breaker fired");
        assert!(
            ghost_demotions >= 48,
            "the stale protected set fully drained: {ghost_demotions}"
        );
        let (admits_after, _, _) = cache.retention_stats();
        assert!(
            admits_after >= 48 + 32,
            "the new working set achieved protected residency: {admits_after}"
        );
    }

    /// Σ.AI.6f — scan resistance survives the ghost mechanism: a pure
    /// one-touch flood (every key seen exactly ONCE) can fill the
    /// ghost list but can never *hit* it — no ghost hits, no forced
    /// demotions, and the twice-touched hot set keeps 100% residency.
    /// This is the guard that the freeze breaker only responds to
    /// PROOF of a live re-touching working set, not to volume.
    #[test]
    fn rg_cache_retention_ghost_ignores_one_touch_flood() {
        let cache = rg_policy_cache(true);
        for id in 0..16 {
            rg_touch(&cache, id); // hot touch 1: insert
        }
        for id in 0..16 {
            assert!(rg_touch(&cache, id), "hot touch 2: promote");
        }
        for id in 1_000..1_400 {
            rg_touch(&cache, id); // 6×-capacity one-touch flood
        }
        let survivors = (0..16).filter(|&id| rg_touch(&cache, id)).count();
        assert_eq!(
            survivors, 16,
            "flood must not displace the protected hot set"
        );
        let (ghost_hits, ghost_demotions) = cache.retention_ghost_stats();
        assert_eq!(ghost_hits, 0, "flood keys are never re-requested");
        assert_eq!(ghost_demotions, 0, "no ghost hit → no demotion");
        let (_, _, prot) = cache.retention_stats();
        assert_eq!(prot, 0, "the flood never reaches the protected segment");
    }

    /// Σ.AI.6f — the ghost list stays bounded under (a) an unbounded
    /// stream of distinct keys (pure growth pressure) and (b) a
    /// loop-over-capacity pattern (every pass consumes ghost entries
    /// and refills them — maximal stale-pair churn). Keys only, cap =
    /// max(live entries, 64); queue pairs swept at 2:1.
    #[test]
    fn rg_cache_retention_ghost_list_is_bounded() {
        let cache = rg_policy_cache(true);
        for id in 0..10_000 {
            rg_touch(&cache, id); // 10 000 distinct one-touch keys
        }
        let (live, pairs) = cache.ghost_len();
        assert!(live <= 64, "ghost live keys ≤ cap after flood: {live}");
        assert!(pairs <= 2 * 64 + 64, "ghost queue swept 2:1: {pairs}");
        for _pass in 0..50 {
            for id in 20_000..20_100 {
                rg_touch(&cache, id); // 100-key loop > 64-entry capacity
            }
        }
        let (live, pairs) = cache.ghost_len();
        assert!(live <= 64, "ghost live keys ≤ cap after loop churn: {live}");
        assert!(
            pairs <= 2 * 64 + 64,
            "ghost queue bounded after churn: {pairs}"
        );
        cache.clear();
        let (live, pairs) = cache.ghost_len();
        assert_eq!((live, pairs), (0, 0), "clear() drops ghost history");
    }

    /// Σ.AI.6e — correctness oracle: retention forced ON (injectable
    /// policy — no env mutation, no parallel-runner race), scans return
    /// byte-identical rows on hit, and clear() + repopulate is identical.
    #[test]
    fn rg_decode_cache_retention_forced_on_identical_rows_and_clear() {
        let path = tmp_parquet("rg_cache_retention_identity");
        write_three_primitives(&path, 4096);
        let cache = std::sync::Arc::new(RowGroupDecodeCache::with_capacity_bytes_and_retention(
            1 << 30,
            true,
        ));
        assert!(cache.retention_enabled());

        let first = read_all_with_rg_cache(&path, schema_three_primitives(), cache.clone());
        let second = read_all_with_rg_cache(&path, schema_three_primitives(), cache.clone());
        let (h, m, _) = cache.stats();
        assert!(m >= 1, "first read misses + inserts");
        assert!(h >= 1, "second read hits");
        let (admits, _, _) = cache.retention_stats();
        assert!(admits >= 1, "second read promotes to protected");
        assert_eq!(
            format!("{first:?}"),
            format!("{second:?}"),
            "cache-served rows are identical (it's a cache — wrong results disqualify)"
        );

        cache.clear();
        assert!(cache.is_empty(), "clear() empties both segments");
        let (_, _, bytes) = cache.stats();
        assert_eq!(bytes, 0, "clear() returns every byte incl. protected");
        let third = read_all_with_rg_cache(&path, schema_three_primitives(), cache.clone());
        assert_eq!(format!("{first:?}"), format!("{third:?}"));
    }

    /// Σ.AI.6e — `EMAT_RG_CACHE_RETENTION` tri-state resolution: `=0`
    /// forces legacy FIFO, `=1` forces retention, unset = AUTO = ON.
    /// One combined test; mutates a production `EMAT_*` var → takes
    /// `EMAT_ENV_TEST_LOCK` (repo convention) and restores on exit.
    #[test]
    fn rg_cache_retention_env_tri_state_resolution() {
        let _guard = crate::flags::EMAT_ENV_TEST_LOCK.blocking_lock();
        let k = "EMAT_RG_CACHE_RETENTION";
        let prev = std::env::var(k).ok();

        unsafe { std::env::remove_var(k) };
        assert!(rg_cache_retention_resolved(), "unset = AUTO = ON");
        unsafe { std::env::set_var(k, "0") };
        assert!(!rg_cache_retention_resolved(), "=0 forces legacy FIFO");
        assert!(
            !RowGroupDecodeCache::new().retention_enabled(),
            "env-resolved constructor honours the force-OFF"
        );
        unsafe { std::env::set_var(k, "1") };
        assert!(rg_cache_retention_resolved(), "=1 forces retention");
        assert!(RowGroupDecodeCache::new().retention_enabled());
        unsafe { std::env::set_var(k, "auto") };
        assert!(
            rg_cache_retention_resolved(),
            "unrecognized value = AUTO = ON (tri_state convention)"
        );

        match prev {
            Some(v) => unsafe { std::env::set_var(k, v) },
            None => unsafe { std::env::remove_var(k) },
        }
    }

    /// L9.ADAPT LATE-ARM — a tight-rescued wrap's sideband published
    /// MID-SCAN must be adopted at the next row-group boundary: RG0
    /// (decoded before the publish) flows dense; RG1 (after) is
    /// filtered by the published set. Rows skipped by the late filter
    /// are a superset-of-matches concern only — the join re-applies
    /// the predicate — but the test pins that the reader (a) does not
    /// stall, (b) arms exactly once, and (c) attaches the Guard-2
    /// disarm counters on arm.
    #[test]
    fn late_arm_adopts_published_predicates_at_rg_boundary() {
        use crate::bridge_filter_sideband::BridgeFilterSideband;
        use crate::ematix_fast_parquet::ColumnPredicate;
        use crate::i64_set::I64Set;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use ematix_parquet_codec::write::{ColumnData, write_table_to_path_with_row_group_size};

        let path = tmp_parquet("late_arm_two_rgs");
        // 2 RGs × 64 rows of i64 keys 0..128.
        let keys: Vec<i64> = (0..128).collect();
        write_table_to_path_with_row_group_size(
            &path,
            &[("k", ColumnData::I64(&keys))],
            CompressionCodec::Uncompressed,
            64,
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));

        let sb = BridgeFilterSideband::new().mark_tight_admitted();
        let file = ParquetFile::open(&path).unwrap();
        let mut rdr = EmatArrowBatchReaderBuilder::new(file, schema)
            .with_projection(vec![0])
            .with_late_arm(sb.clone(), path.clone())
            .build()
            .unwrap();

        // RG0: sideband unpublished → dense, all 64 rows.
        let rg0_rows: usize = {
            let b = rdr.next().unwrap().unwrap();
            b.num_rows()
        };
        assert_eq!(rg0_rows, 64, "pre-publish RG flows dense (no stall)");
        assert!(rdr.late_arm.is_some(), "still armed-pending before publish");

        // Publish a 2-key set between RG boundaries.
        let mut set = I64Set::with_keys(2);
        set.insert(70);
        set.insert(99);
        sb.publish(vec![ColumnPredicate::I64InSet {
            col_idx: 0,
            set: std::sync::Arc::new(set),
        }]);

        // RG1: filter adopted → only keys {70, 99} survive.
        let mut post_rows = 0usize;
        while let Some(b) = rdr.next().transpose().unwrap() {
            post_rows += b.num_rows();
        }
        assert_eq!(post_rows, 2, "post-publish RG is filtered by the set");
        assert!(rdr.late_arm.is_none(), "armed exactly once, handle taken");
        let f = rdr.filter.as_ref().expect("filter installed on arm");
        assert!(
            f.probe_disarm().is_some(),
            "Guard-2 disarm counters attached on arm (tight sideband)"
        );
    }
}
