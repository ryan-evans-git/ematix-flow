//! Σ.AI.6 — opt-in bounded DataFusion memory pool (2026-07-08).
//!
//! ## History: proposed default, refuted by the full-suite re-bench
//!
//! ematix builds sessions on DataFusion's default `UnboundedMemoryPool`.
//! On a box whose RAM is small relative to the working set that can
//! thrash (operator memory evicts the page cache under the scans) or
//! get the process kernel-OOM-killed. An ISOLATED Q09 SF100 A/B on the
//! 32 GB campaign box (`exp/q09-mem-ab`) made a 0.7 × RAM cap look like
//! the fix: 94.3 s → 6.58 s (DuckDB parity). It shipped as the default
//! (`b7ef44f3`) — and the mandatory zero-override FULL-SUITE re-bench
//! on the same box refuted it:
//!
//! | leg (22q, defaults)   | unbounded | capped 0.7×RAM              |
//! |-----------------------|-----------|-----------------------------|
//! | flat SF100 total      | 82.5 s    | **140.2 s** (Q10 3.2→57.5 s |
//! |                       |           | spilling to EBS; Q09 ~same) |
//! | parted SF100          | OOM-kill  | **LIVELOCK** (loadavg 26→0, |
//! |                       |           | alive, zero progress)       |
//!
//! A blanket cap helps a cold isolated query and taxes — or deadlocks —
//! a warm suite: DF 53's hash-join builds cannot spill, so concurrent
//! reservations under a Greedy cap can wait on each other forever.
//! Hence the default is **OFF** (unbounded, the historical behaviour).
//!
//! ## What remains (opt-in)
//!
//! `EMAT_MEM_POOL_FRACTION=<f in (0,1]>` attaches a `RuntimeEnv` with a
//! memory cap of `f × physical RAM` — for memory-tight deployments that
//! prefer per-query `ResourcesExhausted` / backpressure over a kernel
//! OOM-kill, accepting the spill-tax and the documented deadlock risk
//! on non-spillable join builds. Applied at the preset choke point
//! (`preset::with_optimizer_rules_overridden`) so production, bench,
//! and library sessions behave identically (bench == release). A
//! caller-installed `RuntimeEnv` always wins; unknown RAM (no
//! `/proc/meminfo`, no `sysctl`) stays unbounded.

use std::sync::OnceLock;

use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::session_state::SessionStateBuilder;

/// The fraction the isolated A/B measured as the sweet spot (see module
/// docs). NOT a default — the pool is opt-in — but the value to reach
/// for when opting a memory-tight deployment in.
pub const RECOMMENDED_FRACTION: f64 = 0.7;

/// Resolved `EMAT_MEM_POOL_FRACTION`: `None` = unbounded (the default
/// and the unset/invalid state), `Some(f)` = cap at `f × RAM`.
pub fn configured_fraction() -> Option<f64> {
    fraction_of(std::env::var("EMAT_MEM_POOL_FRACTION").ok().as_deref())
}

/// Pure parse core of [`configured_fraction`] so tests can pin the
/// table without racing on process-global env vars (the
/// `tri_state_of` convention in [`crate::flags`]).
fn fraction_of(val: Option<&str>) -> Option<f64> {
    match val {
        Some(v) if v == "0" || v.eq_ignore_ascii_case("false") || v.eq_ignore_ascii_case("off") => {
            None
        }
        Some(v) => match v.parse::<f64>() {
            Ok(f) if f > 0.0 && f <= 1.0 => Some(f),
            // Unparsable / out-of-range: stay OFF (the default) rather
            // than guessing a cap the operator didn't ask for.
            _ => None,
        },
        // Σ.AI.6b (2026-07-08): default **OFF**. The 0.7 default was
        // refuted by the full-suite zero-override re-bench on the same
        // box where the isolated A/B won: flat SF100 82.5 s → 140.2 s
        // (Q10's late-mat aggregate spilled to EBS, 3.2 s → 57.5 s;
        // Q09 unchanged ~40 s), and parted SF100 LIVELOCKED (loadavg
        // 26 → 0.00, process alive, zero progress — Greedy pool +
        // non-spillable hash-join builds deadlock under the cap).
        // A blanket cap helps a cold isolated query and taxes/deadlocks
        // a warm suite. Opt-in remains for memory-tight deployments
        // that prefer ResourcesExhausted/backpressure over a kernel
        // OOM-kill — with the deadlock caveat documented above.
        None => None,
    }
}

/// Physical RAM in bytes, memoized per process. Linux: `/proc/meminfo`
/// `MemTotal`. macOS: `sysctl -n hw.memsize` (dev machines; one
/// subprocess, once). `None` when neither works.
pub fn system_ram_bytes() -> Option<usize> {
    static RAM: OnceLock<Option<usize>> = OnceLock::new();
    *RAM.get_or_init(|| {
        if let Ok(meminfo) = std::fs::read_to_string("/proc/meminfo") {
            let kb: Option<usize> = meminfo
                .lines()
                .find_map(|l| l.strip_prefix("MemTotal:"))
                .and_then(|rest| rest.split_whitespace().next())
                .and_then(|n| n.parse().ok());
            if let Some(kb) = kb {
                return Some(kb * 1024);
            }
        }
        if cfg!(target_os = "macos") {
            if let Ok(out) = std::process::Command::new("sysctl")
                .args(["-n", "hw.memsize"])
                .output()
            {
                if out.status.success() {
                    if let Ok(bytes) = String::from_utf8_lossy(&out.stdout).trim().parse() {
                        return Some(bytes);
                    }
                }
            }
        }
        None
    })
}

/// The pool cap in bytes for the current env + machine, `None` when the
/// pool should stay unbounded (explicitly disabled, or RAM unknown).
pub fn effective_limit_bytes() -> Option<usize> {
    let frac = configured_fraction()?;
    let ram = system_ram_bytes()?;
    Some((ram as f64 * frac) as usize)
}

/// Attach the default memory policy to `builder`. Precedence:
/// 1. a caller-installed `RuntimeEnv` always wins (no-op);
/// 2. `EMAT_MEM_POOL_FRACTION` opt-in → hard Greedy cap at f × RAM;
/// 3. otherwise the Σ.AI.6c **elastic floor guard** (default ON where
///    MemAvailable is sensable, i.e. Linux) — unbounded until the
///    MACHINE is nearly out of memory, then per-query refusal instead
///    of a kernel OOM-kill. `EMAT_MEM_FLOOR_BYTES=0` disables;
/// 4. unknown platform / disabled → unbounded legacy.
pub fn apply_default_memory_pool(mut builder: SessionStateBuilder) -> SessionStateBuilder {
    if builder.runtime_env().is_some() {
        return builder;
    }
    // Opt-in hard cap (see module docs for why it is NOT the default).
    if let Some(limit) = effective_limit_bytes() {
        return match RuntimeEnvBuilder::new()
            .with_memory_limit(limit, 1.0)
            .build_arc()
        {
            Ok(renv) => builder.with_runtime_env(renv),
            // Construction can't realistically fail, but never let the
            // guard break session construction.
            Err(_) => builder,
        };
    }
    // Elastic floor guard: only where the sensor works (Linux) and the
    // floor resolves (RAM known, not disabled).
    if sensed_available_bytes().is_some() {
        if let Some(floor) = configured_floor(system_ram_bytes()) {
            let pool = std::sync::Arc::new(ElasticFloorPool::new(
                floor,
                Box::new(sensed_available_bytes),
            ));
            return match RuntimeEnvBuilder::new().with_memory_pool(pool).build_arc() {
                Ok(renv) => builder.with_runtime_env(renv),
                Err(_) => builder,
            };
        }
    }
    builder
}

// ---------------------------------------------------------------------------
// Σ.AI.6c — ElasticFloorPool: the OOM-guard that the refuted blanket cap
// could not be. Unbounded while the MACHINE is healthy (zero tax on every
// banked benchmark number), refusing allocations only when system
// MemAvailable would sink below a floor — so a runaway query gets a
// recoverable per-query `ResourcesExhausted` instead of the kernel
// OOM-killing the process (the parted-SF100 failure mode). Unlike the hard
// fraction cap there is no artificial mid-plenty ceiling: spillable
// consumers that DO spill free real RAM, MemAvailable recovers, and
// progress resumes — the cap's in-suite spill-tax/livelock shape can't
// form because the floor only binds when the machine is genuinely full.
// ---------------------------------------------------------------------------

use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::error::DataFusionError;
use datafusion::execution::memory_pool::{MemoryPool, MemoryReservation};

/// Sensor for "bytes the OS could still hand out without reclaim pain".
/// Injected so tests are deterministic; production uses
/// [`sensed_available_bytes`]. `None` = unknown → the pool never refuses.
pub type AvailableSensor = dyn Fn() -> Option<usize> + Send + Sync;

/// System MemAvailable in bytes. Linux: `/proc/meminfo` `MemAvailable`
/// (the kernel's own reclaim-aware estimate — the number the OOM killer
/// effectively works against). Other platforms: `None` (the floor guard
/// is a server-side feature; macOS dev boxes run unguarded, as before).
///
/// `try_grow` sits on the per-batch hot path, so the read is cached for
/// ~25 ms (a runaway allocation burns MemAvailable over hundreds of ms —
/// the guard still fires long before the OOM killer; the cache just
/// keeps the syscall+parse off every reservation).
pub fn sensed_available_bytes() -> Option<usize> {
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    const TTL_MS: u64 = 25;
    const UNKNOWN: usize = usize::MAX;
    static EPOCH: OnceLock<std::time::Instant> = OnceLock::new();
    static LAST_MS: AtomicU64 = AtomicU64::new(u64::MAX); // MAX = never read
    static CACHED: AtomicUsize = AtomicUsize::new(UNKNOWN);

    let now_ms = EPOCH
        .get_or_init(std::time::Instant::now)
        .elapsed()
        .as_millis() as u64;
    let last = LAST_MS.load(Ordering::Relaxed);
    if last != u64::MAX && now_ms.saturating_sub(last) < TTL_MS {
        let c = CACHED.load(Ordering::Relaxed);
        return if c == UNKNOWN { None } else { Some(c) };
    }
    let fresh: Option<usize> = (|| {
        let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
        let kb: usize = meminfo
            .lines()
            .find_map(|l| l.strip_prefix("MemAvailable:"))?
            .split_whitespace()
            .next()?
            .parse()
            .ok()?;
        Some(kb * 1024)
    })();
    CACHED.store(fresh.unwrap_or(UNKNOWN), Ordering::Relaxed);
    LAST_MS.store(now_ms, Ordering::Relaxed);
    fresh
}

/// Default floor: max(1 GiB, 3% of RAM). Big enough that the kernel +
/// page cache keep breathing room; small enough that it never binds on a
/// healthy run (the SF100 suite peaked with ~2 GB reclaimable when it
/// thrashed — the floor fires *before* the OOM killer would).
pub fn default_floor_bytes(ram: usize) -> usize {
    (1usize << 30).max(ram / 33)
}

/// Resolved `EMAT_MEM_FLOOR_BYTES`: `None` = guard disabled, `Some(b)` =
/// refuse allocations that would leave less than `b` bytes available.
/// Unset = AUTO (default floor from RAM). `=0` disables.
pub fn configured_floor(ram: Option<usize>) -> Option<usize> {
    floor_of(std::env::var("EMAT_MEM_FLOOR_BYTES").ok().as_deref(), ram)
}

/// Pure parse core (env-race-free tests, `fraction_of` convention).
fn floor_of(val: Option<&str>, ram: Option<usize>) -> Option<usize> {
    match val {
        Some(v) if v == "0" || v.eq_ignore_ascii_case("off") => None,
        Some(v) => match v.parse::<usize>() {
            Ok(b) if b > 0 => Some(b),
            _ => ram.map(default_floor_bytes),
        },
        None => ram.map(default_floor_bytes),
    }
}

/// A [`MemoryPool`] that tracks reservations but only *refuses* growth
/// when the sensed system MemAvailable, minus the requested bytes, would
/// drop below `floor_bytes`. Unknown availability never refuses.
pub struct ElasticFloorPool {
    reserved: AtomicUsize,
    floor_bytes: usize,
    sensor: Box<AvailableSensor>,
}

impl std::fmt::Debug for ElasticFloorPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ElasticFloorPool")
            .field("reserved", &self.reserved.load(Ordering::Relaxed))
            .field("floor_bytes", &self.floor_bytes)
            .finish()
    }
}

impl ElasticFloorPool {
    pub fn new(floor_bytes: usize, sensor: Box<AvailableSensor>) -> Self {
        Self {
            reserved: AtomicUsize::new(0),
            floor_bytes,
            sensor,
        }
    }
}

impl MemoryPool for ElasticFloorPool {
    fn grow(&self, _reservation: &MemoryReservation, additional: usize) {
        self.reserved.fetch_add(additional, Ordering::Relaxed);
    }

    fn shrink(&self, _reservation: &MemoryReservation, shrink: usize) {
        self.reserved.fetch_sub(shrink, Ordering::Relaxed);
    }

    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> datafusion::error::Result<()> {
        if let Some(avail) = (self.sensor)() {
            if avail.saturating_sub(additional) < self.floor_bytes {
                return Err(DataFusionError::ResourcesExhausted(format!(
                    "ElasticFloorPool: refusing {additional} B for {} — system \
                     MemAvailable {avail} B would sink below the {} B floor \
                     (EMAT_MEM_FLOOR_BYTES; =0 disables). The machine is out of \
                     memory; failing this query instead of risking a kernel \
                     OOM-kill.",
                    reservation.consumer().name(),
                    self.floor_bytes,
                )));
            }
        }
        self.grow(reservation, additional);
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.reserved.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::MemoryConsumer;

    /// Floor parse table (pure, no env): unset/garbage = AUTO default
    /// from RAM; `=0`/`off` disables; explicit bytes win.
    #[test]
    fn floor_parse_table() {
        let ram = Some(32usize << 30);
        let auto = Some(default_floor_bytes(32 << 30));
        assert_eq!(floor_of(None, ram), auto, "unset = AUTO floor");
        assert_eq!(floor_of(Some("0"), ram), None, "=0 disables");
        assert_eq!(floor_of(Some("off"), ram), None);
        assert_eq!(floor_of(Some("2147483648"), ram), Some(2 << 30));
        assert_eq!(floor_of(Some("banana"), ram), auto, "garbage = AUTO");
        assert_eq!(floor_of(None, None), None, "unknown RAM = no guard");
        // Default floor: max(1 GiB, ~3% RAM). 32 GiB box → 3% ≈ 0.97 GiB,
        // so the 1 GiB minimum rules; 128 GiB box → 3% ≈ 3.9 GiB wins.
        assert_eq!(default_floor_bytes(32 << 30), 1 << 30);
        assert_eq!(default_floor_bytes(128 << 30), (128usize << 30) / 33);
    }

    /// The elastic pool refuses growth only when sensed availability
    /// minus the request would sink below the floor; unknown
    /// availability never refuses; reserved() tracks grow/shrink.
    #[test]
    fn elastic_floor_refuses_only_under_real_pressure() {
        let gib = 1usize << 30;
        // Healthy machine: 8 GiB available, 1 GiB floor → grow fine.
        let pool: std::sync::Arc<dyn MemoryPool> = std::sync::Arc::new(ElasticFloorPool::new(
            gib,
            Box::new(move || Some(8 * (1 << 30))),
        ));
        let r = MemoryConsumer::new("healthy").register(&pool);
        assert!(r.try_grow(2 * gib).is_ok(), "plenty available → grow");
        assert_eq!(pool.reserved(), 2 * gib, "reserved tracks grow");
        r.free();
        assert_eq!(pool.reserved(), 0, "reserved tracks free");

        // Pressured machine: 1.5 GiB available, 1 GiB floor → a 1 GiB
        // request would leave 0.5 GiB < floor → REFUSED; a small request
        // that keeps us above the floor is fine.
        let pool: std::sync::Arc<dyn MemoryPool> = std::sync::Arc::new(ElasticFloorPool::new(
            gib,
            Box::new(move || Some(gib + gib / 2)),
        ));
        let r = MemoryConsumer::new("pressured").register(&pool);
        let err = r.try_grow(gib).unwrap_err().to_string();
        assert!(
            err.contains("ElasticFloorPool") && err.contains("OOM"),
            "refusal must be self-diagnosing, got: {err}"
        );
        assert_eq!(pool.reserved(), 0, "failed grow must not reserve");
        assert!(
            r.try_grow(gib / 4).is_ok(),
            "small request stays above floor"
        );
        r.free();

        // Unknown availability (sensor None, e.g. macOS): never refuses.
        let pool: std::sync::Arc<dyn MemoryPool> =
            std::sync::Arc::new(ElasticFloorPool::new(gib, Box::new(|| None)));
        let r = MemoryConsumer::new("unknown").register(&pool);
        assert!(
            r.try_grow(1 << 40).is_ok(),
            "unknown availability → unguarded"
        );
        r.free();
    }

    /// Parse table for `EMAT_MEM_POOL_FRACTION` (pure, no env). The
    /// pool is OPT-IN: only a valid fraction in (0,1] bounds it.
    #[test]
    fn fraction_parse_table() {
        assert_eq!(fraction_of(None), None, "unset = OFF (unbounded default)");
        assert_eq!(fraction_of(Some("0")), None, "=0 = OFF");
        assert_eq!(fraction_of(Some("false")), None);
        assert_eq!(fraction_of(Some("off")), None);
        assert_eq!(fraction_of(Some("0.5")), Some(0.5));
        assert_eq!(fraction_of(Some("0.7")), Some(RECOMMENDED_FRACTION));
        assert_eq!(fraction_of(Some("1")), Some(1.0));
        // Garbage / out-of-range stays OFF — never bind a cap the
        // operator didn't ask for.
        assert_eq!(fraction_of(Some("1.5")), None);
        assert_eq!(fraction_of(Some("-0.2")), None);
        assert_eq!(fraction_of(Some("banana")), None);
    }

    /// RAM sensing must work on the platforms we build/test on
    /// (Linux CI + macOS dev) — the default bound depends on it.
    #[test]
    fn system_ram_is_known_here() {
        let ram = system_ram_bytes().expect("RAM should be detectable on linux/macos");
        assert!(ram >= 1 << 30, "implausible RAM: {ram}");
    }

    /// The default-constructed session (no caller RuntimeEnv, env
    /// unset) must carry a pool that REFUSES an over-cap reservation
    /// and accepts a small one; a caller-provided RuntimeEnv must
    /// survive untouched. One test, not three: these arms mutate
    /// process-global env and must not interleave with each other.
    #[test]
    fn default_pool_bounds_and_caller_env_wins() {
        // Arm 1: UNSET = the shipped default, which is PLATFORM-
        // DEPENDENT by design: elastic floor guard where MemAvailable
        // is sensable (Linux), unbounded legacy elsewhere (macOS dev).
        // (The 0.7 blanket cap was refuted by the full-suite re-bench —
        // see module docs; the floor is the guard that replaced it.)
        // SAFETY: single-threaded within this test; restored before exit.
        unsafe { std::env::remove_var("EMAT_MEM_POOL_FRACTION") };
        unsafe { std::env::remove_var("EMAT_MEM_FLOOR_BYTES") };
        let state =
            apply_default_memory_pool(SessionStateBuilder::new().with_default_features()).build();
        let pool = state.runtime_env().memory_pool.clone();
        let r = MemoryConsumer::new("test-default").register(&pool);
        if sensed_available_bytes().is_some() {
            // Elastic floor: small requests succeed; an absurd 1 TiB
            // request that would blow past physical memory is refused —
            // the whole point: per-query error, not a kernel OOM-kill.
            assert!(r.try_grow(1 << 20).is_ok(), "1 MiB fits on a healthy box");
            r.free();
            let r2 = MemoryConsumer::new("test-default-absurd").register(&pool);
            let err = r2.try_grow(1 << 40).unwrap_err().to_string();
            assert!(
                err.contains("ElasticFloorPool"),
                "1 TiB must be refused by the floor guard, got: {err}"
            );
            r2.free();
        } else {
            assert!(
                r.try_grow(1 << 40).is_ok(),
                "sensor-less platform stays unbounded — accept 1 TiB"
            );
            r.free();
        }

        // Arm 2: explicit opt-in bounds the pool.
        unsafe { std::env::set_var("EMAT_MEM_POOL_FRACTION", "0.7") };
        let limit = effective_limit_bytes().expect("RAM known per test above");
        let state =
            apply_default_memory_pool(SessionStateBuilder::new().with_default_features()).build();
        let pool = state.runtime_env().memory_pool.clone();
        let r = MemoryConsumer::new("test-over-cap").register(&pool);
        assert!(
            r.try_grow(limit + (1 << 20)).is_err(),
            "over-cap reservation must be refused when opted in (limit={limit})"
        );
        r.free();
        let small = MemoryConsumer::new("test-small").register(&pool);
        assert!(small.try_grow(1 << 20).is_ok(), "1 MiB must fit");
        small.free();
        unsafe { std::env::remove_var("EMAT_MEM_POOL_FRACTION") };

        // Arm 3: caller-installed RuntimeEnv wins even when the env
        // asks for a (much larger) opt-in cap — set the env so the
        // no-clobber path is actually exercised.
        unsafe { std::env::set_var("EMAT_MEM_POOL_FRACTION", "0.7") };
        let tiny = RuntimeEnvBuilder::new()
            .with_memory_limit(1 << 20, 1.0)
            .build_arc()
            .unwrap();
        let state = apply_default_memory_pool(
            SessionStateBuilder::new()
                .with_default_features()
                .with_runtime_env(tiny),
        )
        .build();
        unsafe { std::env::remove_var("EMAT_MEM_POOL_FRACTION") };
        let pool = state.runtime_env().memory_pool.clone();
        let r = MemoryConsumer::new("test-caller-pool").register(&pool);
        assert!(
            r.try_grow(2 << 20).is_err(),
            "caller's 1 MiB pool must still be in effect (not replaced by the opt-in cap)"
        );
        r.free();
    }
}
