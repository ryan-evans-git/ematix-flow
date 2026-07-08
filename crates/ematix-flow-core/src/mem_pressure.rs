//! Σ.AI.6d — decode-pressure shedding (2026-07-08): the scan-side link
//! between system `MemAvailable` and the UNTRACKED decode memory the
//! DataFusion pool cannot see. Option 3 of
//! [`docs/plans/MEMORY_BUDGET.md`](../../../docs/plans/MEMORY_BUDGET.md).
//!
//! ## Why (run6 evidence)
//!
//! The Σ.AI.6c `ElasticFloorPool` guards TRACKED consumers (joins /
//! sorts / aggregates): `try_grow` refuses before the kernel OOM-kills.
//! But the parted SF100 suite tar-pitted the 32 GB box anyway — the
//! pressure comes from allocations the pool never sees: ematix decode
//! buffers and the rayon page fan-out under the 8-part union scan.
//! `try_grow` is never asked, so the floor has nothing to hook. And on
//! flat SF100, Q09's scan+join working set evicts the OS page cache →
//! ~40 s thrash where a fixed plan runs 6.6 s. This module is the
//! missing hook: when `MemAvailable` sinks, *the decode side itself*
//! backs off.
//!
//! ## What (v1, opt-in `EMAT_DECODE_SHED=1`, default OFF)
//!
//! One process-global gate consulted at every row-group-decode entry
//! point (`EmatArrowBatchReader::load_row_group`, the two streaming
//! readers' `open_row_group`, and the legacy per-RG bridge loop).
//! While sensed `MemAvailable` < `EMAT_SHED_AVAILABLE_FRACTION`
//! (default 0.10) × RAM the gate is in [`Pressure::Shed`] and:
//!
//! 1. **Concurrency shed** — a semaphore of `max(1, cores/4)` permits
//!    bounds concurrent row-group decodes, draining decode-buffer
//!    pressure instead of fanning it out (`shed_gate_entries`).
//! 2. **Cache shed** — the Normal→Shed transition clears the process
//!    RG decode cache once per transition (`shed_cache_drops`),
//!    handing its up-to-1-GiB back to the page cache.
//!
//! **The healthy-path cost is the contract.** The refuted Σ.AI.6
//! blanket cap taxed healthy suites (flat SF100 82.5 → 140.2 s); this
//! gate must not repeat that. Disabled (the default): one `OnceLock`
//! atomic load → `None`. Enabled + Normal pressure: the ~25 ms-cached
//! sensor read plus one relaxed atomic load — no locks, no syscalls.
//! Permits exist only while Shed persists; Normal bypasses the
//! semaphore entirely.
//!
//! The default stays OFF until the 32 GB-box FULL-SUITE A/B decides it:
//! isolated memory-lever A/Bs do not transfer (proven twice — the 0.7
//! cap and the pool-fraction Q09 win both refuted in-suite).

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};

/// Machine-level memory pressure as seen by the decode side.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pressure {
    /// Healthy: decode runs exactly as it always has (zero overhead).
    Normal,
    /// `MemAvailable` has sunk below the shed fraction of RAM: bound
    /// decode concurrency and drop the RG decode cache.
    Shed,
}

/// Default for `EMAT_SHED_AVAILABLE_FRACTION`: shed when less than 10%
/// of RAM is available. On the 32 GB campaign box that is ~3.2 GB —
/// comfortably above the 1 GiB ElasticFloorPool floor, so decode backs
/// off BEFORE the tracked-consumer guard starts refusing queries.
pub const DEFAULT_SHED_AVAILABLE_FRACTION: f64 = 0.10;

/// Resolved `EMAT_SHED_AVAILABLE_FRACTION` (numeric tunable,
/// [`crate::flags::f64_or`] convention).
pub fn shed_available_fraction() -> f64 {
    crate::flags::f64_or(
        "EMAT_SHED_AVAILABLE_FRACTION",
        DEFAULT_SHED_AVAILABLE_FRACTION,
    )
}

/// Pure pressure classifier: `Shed` iff both sensors are known AND
/// `available < shed_fraction × ram`. Any unknown value → `Normal`
/// (never shed on a platform we cannot sense — macOS dev boxes run
/// exactly as before).
pub fn pressure_of(available: Option<usize>, ram: Option<usize>, shed_fraction: f64) -> Pressure {
    match (available, ram) {
        (Some(avail), Some(ram)) if (avail as f64) < shed_fraction * ram as f64 => Pressure::Shed,
        _ => Pressure::Normal,
    }
}

/// Live pressure from the shared Σ.AI.6c sensors — reuses
/// [`crate::mem_pool::sensed_available_bytes`] (~25 ms TTL cache, so
/// this is relaxed-atomic cheap between refreshes) and
/// [`crate::mem_pool::system_ram_bytes`] (memoized once per process).
pub fn current_pressure() -> Pressure {
    pressure_of(
        crate::mem_pool::sensed_available_bytes(),
        crate::mem_pool::system_ram_bytes(),
        shed_available_fraction(),
    )
}

/// Pure resolver for the Shed-mode decode-concurrency bound:
/// `max(1, cores/4)`.
pub fn default_shed_limit(cores: usize) -> usize {
    (cores / 4).max(1)
}

/// Snapshot of the shed counters ([`crate::sidecar_index::sidecar_metrics`]
/// precedent: cumulative per gate; probes and tests read deltas).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MemPressureMetrics {
    /// Shed-mode gate entries (semaphore acquisitions). Normal-pressure
    /// passes are deliberately NOT counted — counting them would put an
    /// extra atomic RMW on the healthy path.
    pub shed_gate_entries: u64,
    /// RG-decode-cache drops fired on Normal→Shed transitions.
    pub shed_cache_drops: u64,
}

/// Injected pressure source — production uses [`current_pressure`];
/// tests inject deterministic sources (never env mutation).
pub type PressureSensor = dyn Fn() -> Pressure + Send + Sync;

/// Injected Normal→Shed transition action — production drops the
/// process RG decode cache; tests inject counters.
pub type ShedTransitionHook = dyn Fn() + Send + Sync;

/// The decode-pressure gate: a pressure-aware semaphore plus a one-shot
/// per-transition cache-drop latch. Constructed once per process from
/// env (see [`decode_gate_enter`]); tests build their own instances
/// with injected hooks.
pub struct DecodeShedGate {
    pressure: Box<PressureSensor>,
    on_shed_transition: Box<ShedTransitionHook>,
    /// Max concurrent Shed-mode decode permits.
    limit: usize,
    /// Available permits. Only touched while Shed persists — Normal
    /// pressure never takes this lock.
    permits: Mutex<usize>,
    released: Condvar,
    /// Transition latch: `true` while the last observed pressure was
    /// Shed. Re-armed on the first Normal observation.
    in_shed: AtomicBool,
    shed_gate_entries: AtomicU64,
    shed_cache_drops: AtomicU64,
}

impl std::fmt::Debug for DecodeShedGate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DecodeShedGate")
            .field("limit", &self.limit)
            .field("in_shed", &self.in_shed.load(Ordering::Relaxed))
            .finish()
    }
}

/// RAII permit for one Shed-mode row-group decode. Dropping releases
/// the semaphore slot.
pub struct ShedPermit {
    gate: Arc<DecodeShedGate>,
}

impl Drop for ShedPermit {
    fn drop(&mut self) {
        let mut avail = self.gate.permits.lock().unwrap();
        *avail += 1;
        self.gate.released.notify_one();
    }
}

impl DecodeShedGate {
    /// Build a gate with injected pressure source + transition hook.
    /// Production wiring lives in [`decode_gate_enter`]; tests call
    /// this directly (no process-global state, no env mutation).
    pub fn new(
        limit: usize,
        pressure: Box<PressureSensor>,
        on_shed_transition: Box<ShedTransitionHook>,
    ) -> Self {
        let limit = limit.max(1);
        Self {
            pressure,
            on_shed_transition,
            limit,
            permits: Mutex::new(limit),
            released: Condvar::new(),
            in_shed: AtomicBool::new(false),
            shed_gate_entries: AtomicU64::new(0),
            shed_cache_drops: AtomicU64::new(0),
        }
    }

    /// Consult pressure and enter the gate. `None` = Normal: proceed
    /// exactly as before (one sensor call + one relaxed load — no lock,
    /// no permit). `Some(permit)` = Shed: the caller holds one of the
    /// `limit` decode slots until the permit drops; the first Shed
    /// observation after a Normal one fires the transition hook once.
    pub fn enter(self: &Arc<Self>) -> Option<ShedPermit> {
        match (self.pressure)() {
            Pressure::Normal => {
                // Shed→Normal: re-arm the one-shot transition latch.
                // Load-then-store (not an unconditional store) keeps
                // the steady healthy path to a single relaxed LOAD.
                if self.in_shed.load(Ordering::Relaxed) {
                    self.in_shed.store(false, Ordering::Relaxed);
                }
                None
            }
            Pressure::Shed => {
                if !self.in_shed.swap(true, Ordering::Relaxed) {
                    // Normal→Shed transition (the swap admits exactly
                    // one thread per transition): drop the decode cache
                    // once — its bytes go back to the page cache.
                    (self.on_shed_transition)();
                    self.shed_cache_drops.fetch_add(1, Ordering::Relaxed);
                }
                self.shed_gate_entries.fetch_add(1, Ordering::Relaxed);
                let mut avail = self.permits.lock().unwrap();
                while *avail == 0 {
                    avail = self.released.wait(avail).unwrap();
                }
                *avail -= 1;
                Some(ShedPermit {
                    gate: Arc::clone(self),
                })
            }
        }
    }

    /// Cumulative counters for THIS gate instance (relaxed — probes
    /// want cheap, not fenced).
    pub fn metrics(&self) -> MemPressureMetrics {
        MemPressureMetrics {
            shed_gate_entries: self.shed_gate_entries.load(Ordering::Relaxed),
            shed_cache_drops: self.shed_cache_drops.load(Ordering::Relaxed),
        }
    }

    /// The Shed-mode concurrency bound this gate enforces.
    pub fn limit(&self) -> usize {
        self.limit
    }
}

/// Process-global gate, built once from env. `None` (the default —
/// `EMAT_DECODE_SHED` unset) keeps every decode path on the historical
/// fast path at the cost of a single `OnceLock` load.
fn process_gate() -> Option<&'static Arc<DecodeShedGate>> {
    static GATE: OnceLock<Option<Arc<DecodeShedGate>>> = OnceLock::new();
    GATE.get_or_init(|| {
        if !crate::flags::opt_in("EMAT_DECODE_SHED") {
            return None;
        }
        let cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        Some(Arc::new(DecodeShedGate::new(
            default_shed_limit(cores),
            Box::new(current_pressure),
            Box::new(|| {
                // Cache shed: return the RG decode cache's bytes to the
                // OS/page cache. The cache is a pure accelerator —
                // dropping entries is always correct.
                if let Some(cache) = crate::emat_arrow_reader::process_rg_decode_cache() {
                    cache.clear();
                }
            }),
        )))
    })
    .as_ref()
}

/// Test-only override slot so integration tests can force a gate
/// (e.g. always-Shed) through the REAL scan choke points without
/// mutating process-global env vars. Tests touching this slot must
/// hold [`crate::flags::EMAT_ENV_TEST_LOCK`] and restore `None`.
#[cfg(test)]
pub(crate) fn test_gate_slot() -> &'static std::sync::RwLock<Option<Arc<DecodeShedGate>>> {
    static SLOT: OnceLock<std::sync::RwLock<Option<Arc<DecodeShedGate>>>> = OnceLock::new();
    SLOT.get_or_init(|| std::sync::RwLock::new(None))
}

/// Resolve the active gate: test override (test builds only), else the
/// env-built process gate.
fn resolved_gate() -> Option<Arc<DecodeShedGate>> {
    #[cfg(test)]
    if let Some(g) = test_gate_slot().read().unwrap().clone() {
        return Some(g);
    }
    process_gate().cloned()
}

/// THE decode choke-point hook. Every row-group-decode entry point
/// calls this and holds the returned permit for the duration of the
/// decode:
///
/// ```ignore
/// let _shed_permit = crate::mem_pressure::decode_gate_enter();
/// ```
///
/// Gate disabled (default): one `OnceLock` atomic load → `None`.
/// Enabled + Normal: cached sensor read + one relaxed load → `None`.
/// Enabled + Shed: counted semaphore entry → `Some(permit)`.
pub fn decode_gate_enter() -> Option<ShedPermit> {
    let gate = resolved_gate()?;
    gate.enter()
}

/// Probe for the ACTIVE gate's counters — zeros when the gate is
/// disabled (the default), which the zero-overhead pin test asserts
/// stays true across real scans.
pub fn mem_pressure_metrics() -> MemPressureMetrics {
    resolved_gate().map(|g| g.metrics()).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pure pressure table: Shed strictly below fraction×RAM, Normal
    /// at/above the line, and any unknown sensor → Normal (never shed
    /// blind).
    #[test]
    fn pressure_of_table() {
        let gib = 1usize << 30;
        let ram = Some(32 * gib);
        // Well below 10% of 32 GiB (3.2 GiB) → Shed.
        assert_eq!(pressure_of(Some(gib), ram, 0.10), Pressure::Shed);
        // Comfortably above → Normal.
        assert_eq!(pressure_of(Some(8 * gib), ram, 0.10), Pressure::Normal);
        // Exactly AT the line → Normal (strict <). Use an exactly-
        // representable line (0.25 × 32 GiB = 8 GiB): a truncated
        // `0.10 × RAM` sits epsilon below the float line and would
        // test the wrong side of the boundary.
        assert_eq!(pressure_of(Some(8 * gib), ram, 0.25), Pressure::Normal);
        assert_eq!(pressure_of(Some(8 * gib - 1), ram, 0.25), Pressure::Shed);
        // Fraction edges: 0.0 never sheds; 1.0 sheds whenever any RAM
        // is in use.
        assert_eq!(pressure_of(Some(0), ram, 0.0), Pressure::Normal);
        assert_eq!(pressure_of(Some(31 * gib), ram, 1.0), Pressure::Shed);
        // Unknown sensors → Normal, in every combination.
        assert_eq!(pressure_of(None, ram, 0.10), Pressure::Normal);
        assert_eq!(pressure_of(Some(gib), None, 0.10), Pressure::Normal);
        assert_eq!(pressure_of(None, None, 0.10), Pressure::Normal);
    }

    /// Shed-limit resolver: `max(1, cores/4)`.
    #[test]
    fn default_shed_limit_table() {
        assert_eq!(default_shed_limit(1), 1);
        assert_eq!(default_shed_limit(3), 1, "cores/4 == 0 clamps to 1");
        assert_eq!(default_shed_limit(4), 1);
        assert_eq!(default_shed_limit(8), 2);
        assert_eq!(default_shed_limit(16), 4);
        assert_eq!(default_shed_limit(64), 16);
    }

    /// Under an injected always-Shed source, N threads entering the
    /// gate never exceed the shed limit concurrently.
    #[test]
    fn shed_semaphore_bounds_concurrency() {
        use std::sync::atomic::AtomicUsize;

        const LIMIT: usize = 2;
        const THREADS: usize = 8;
        let gate = Arc::new(DecodeShedGate::new(
            LIMIT,
            Box::new(|| Pressure::Shed),
            Box::new(|| {}),
        ));
        let active = AtomicUsize::new(0);
        let max_seen = AtomicUsize::new(0);

        std::thread::scope(|s| {
            for _ in 0..THREADS {
                let gate = &gate;
                let active = &active;
                let max_seen = &max_seen;
                s.spawn(move || {
                    let permit = gate.enter();
                    assert!(permit.is_some(), "always-Shed source must gate");
                    let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                    max_seen.fetch_max(now, Ordering::SeqCst);
                    // Widen the overlap window so a broken semaphore
                    // actually exhibits > LIMIT concurrency (timing
                    // mechanics only — no data derived from the clock).
                    std::thread::sleep(std::time::Duration::from_millis(20));
                    active.fetch_sub(1, Ordering::SeqCst);
                    drop(permit);
                });
            }
        });

        let max = max_seen.load(Ordering::SeqCst);
        assert!(
            max <= LIMIT,
            "max concurrent Shed decodes {max} exceeded limit {LIMIT}"
        );
        assert!(max >= 1, "at least one decode must have run");
        assert_eq!(
            gate.metrics().shed_gate_entries,
            THREADS as u64,
            "every Shed entry is counted"
        );
    }

    /// Normal pressure bypasses the semaphore entirely (no permit) and
    /// counts nothing.
    #[test]
    fn normal_pressure_bypasses_gate() {
        let gate = Arc::new(DecodeShedGate::new(
            1,
            Box::new(|| Pressure::Normal),
            Box::new(|| panic!("transition hook must not fire under Normal")),
        ));
        for _ in 0..100 {
            assert!(gate.enter().is_none(), "Normal → no permit, no blocking");
        }
        assert_eq!(gate.metrics(), MemPressureMetrics::default());
    }

    /// The cache drop fires exactly once per Normal→Shed transition:
    /// Normal→Shed→Normal→Shed drops exactly twice, however many gate
    /// entries happen inside each Shed episode.
    #[test]
    fn cache_drop_once_per_transition() {
        let shed_now = Arc::new(AtomicBool::new(false));
        let drops = Arc::new(AtomicU64::new(0));
        let sensor_state = Arc::clone(&shed_now);
        let drop_counter = Arc::clone(&drops);
        let gate = Arc::new(DecodeShedGate::new(
            4,
            Box::new(move || {
                if sensor_state.load(Ordering::SeqCst) {
                    Pressure::Shed
                } else {
                    Pressure::Normal
                }
            }),
            Box::new(move || {
                drop_counter.fetch_add(1, Ordering::SeqCst);
            }),
        ));

        // Episode 0: Normal — nothing fires.
        assert!(gate.enter().is_none());
        assert_eq!(drops.load(Ordering::SeqCst), 0);

        // Episode 1: Shed — first entry drops, further entries don't.
        shed_now.store(true, Ordering::SeqCst);
        let p1 = gate.enter();
        let p2 = gate.enter();
        assert!(p1.is_some() && p2.is_some());
        assert_eq!(drops.load(Ordering::SeqCst), 1, "one drop per transition");
        drop(p1);
        drop(p2);

        // Back to Normal: re-arms the latch, no drop.
        shed_now.store(false, Ordering::SeqCst);
        assert!(gate.enter().is_none());
        assert_eq!(drops.load(Ordering::SeqCst), 1);

        // Episode 2: Shed again — exactly one more drop.
        shed_now.store(true, Ordering::SeqCst);
        let p3 = gate.enter();
        assert!(p3.is_some());
        drop(p3);
        assert_eq!(drops.load(Ordering::SeqCst), 2, "Normal→Shed×2 = 2 drops");
        assert_eq!(gate.metrics().shed_cache_drops, 2);
        assert_eq!(gate.metrics().shed_gate_entries, 3);
    }

    /// End-to-end through the REAL scan choke points, two arms under
    /// one test (they share the process-global test-gate slot, so they
    /// must not interleave — the `default_pool_bounds_and_caller_env_wins`
    /// convention):
    ///
    /// **Arm 1 (zero-overhead pin):** gate disabled (the shipped
    /// default), a real multi-RG parquet scan leaves every shed counter
    /// at 0 — the choke point resolves to `None` and touches nothing.
    ///
    /// **Arm 2 (forced-Shed smoke):** an injected always-Shed gate
    /// (limit 1) through the SAME scan returns bit-identical rows
    /// (oracle = arm 1), actually engages the gate (entries > 0), and
    /// fires the cache drop exactly once for the single Normal→Shed
    /// transition.
    #[tokio::test]
    async fn scan_disabled_is_untouched_and_forced_shed_is_correct() {
        use datafusion::prelude::SessionContext;
        use ematix_parquet_codec::write::{ColumnData, write_table_to_path_with_row_group_size};
        use ematix_parquet_format::types::CompressionCodec;

        // Serialize against any other test touching process-global
        // gate/env state; restore the slot before the guard drops.
        let _guard = crate::flags::EMAT_ENV_TEST_LOCK.lock().await;
        *test_gate_slot().write().unwrap() = None;

        let dir = std::env::temp_dir().join(format!("mem_pressure_e2e_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("t.parquet");
        let n = 4096usize;
        let ids: Vec<i64> = (0..n as i64).collect();
        let vals: Vec<f64> = (0..n).map(|i| i as f64 * 0.5).collect();
        // 4 row groups → several gated decode entries per scan.
        write_table_to_path_with_row_group_size(
            &path,
            &[
                ("v_id", ColumnData::I64(&ids)),
                ("v_val", ColumnData::F64(&vals)),
            ],
            CompressionCodec::Uncompressed,
            1024,
        )
        .unwrap();

        async fn run_scan(
            path: &std::path::Path,
        ) -> Vec<datafusion::arrow::record_batch::RecordBatch> {
            let ctx = SessionContext::new();
            let prov = crate::ematix_fast_parquet::EmatixFastParquetTableProvider::try_new(
                path.to_str().unwrap(),
            )
            .unwrap();
            ctx.register_table("t", Arc::new(prov)).unwrap();
            ctx.sql("select v_id, v_val from t order by v_id")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap()
        }

        // ---- Arm 1: disabled (default) — the zero-overhead pin. ----
        let oracle = run_scan(&path).await;
        let total: usize = oracle.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, n, "oracle scan returns every row");
        assert_eq!(
            mem_pressure_metrics(),
            MemPressureMetrics::default(),
            "disabled gate must leave every shed counter at 0 across a real scan"
        );

        // ---- Arm 2: forced Shed through the same choke points. ----
        let drops = Arc::new(AtomicU64::new(0));
        let drop_counter = Arc::clone(&drops);
        let gate = Arc::new(DecodeShedGate::new(
            1,
            Box::new(|| Pressure::Shed),
            Box::new(move || {
                drop_counter.fetch_add(1, Ordering::SeqCst);
            }),
        ));
        *test_gate_slot().write().unwrap() = Some(Arc::clone(&gate));
        let shed = run_scan(&path).await;
        *test_gate_slot().write().unwrap() = None;

        assert_eq!(
            format!("{oracle:?}"),
            format!("{shed:?}"),
            "forced-Shed scan must return exactly the oracle rows"
        );
        let m = gate.metrics();
        assert!(
            m.shed_gate_entries > 0,
            "the scan's RG decodes must actually flow through the gate \
             (entries={})",
            m.shed_gate_entries
        );
        assert_eq!(
            m.shed_cache_drops, 1,
            "one Normal→Shed transition = exactly one cache drop"
        );
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}
