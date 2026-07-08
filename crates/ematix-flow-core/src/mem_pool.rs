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
        Some(v)
            if v == "0"
                || v.eq_ignore_ascii_case("false")
                || v.eq_ignore_ascii_case("off") =>
        {
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

/// Attach the bounded default pool to `builder` unless the caller
/// already installed a `RuntimeEnv` (theirs wins) or the pool is
/// disabled/unresolvable (unbounded legacy behaviour).
pub fn apply_default_memory_pool(mut builder: SessionStateBuilder) -> SessionStateBuilder {
    if builder.runtime_env().is_some() {
        return builder;
    }
    let Some(limit) = effective_limit_bytes() else {
        return builder;
    };
    match RuntimeEnvBuilder::new()
        .with_memory_limit(limit, 1.0)
        .build_arc()
    {
        Ok(renv) => builder.with_runtime_env(renv),
        // Construction can't realistically fail, but never let the
        // guard break session construction.
        Err(_) => builder,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::MemoryConsumer;

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
        // Arm 1: UNSET = unbounded (the shipped default — the 0.7
        // blanket cap was refuted by the full-suite re-bench, see
        // module docs).
        // SAFETY: single-threaded within this test; restored before exit.
        unsafe { std::env::remove_var("EMAT_MEM_POOL_FRACTION") };
        let state = apply_default_memory_pool(
            SessionStateBuilder::new().with_default_features(),
        )
        .build();
        let pool = state.runtime_env().memory_pool.clone();
        let r = MemoryConsumer::new("test-default-unbounded").register(&pool);
        assert!(
            r.try_grow(1 << 40).is_ok(),
            "default (unset) pool must be unbounded — accept 1 TiB"
        );
        r.free();

        // Arm 2: explicit opt-in bounds the pool.
        unsafe { std::env::set_var("EMAT_MEM_POOL_FRACTION", "0.7") };
        let limit = effective_limit_bytes().expect("RAM known per test above");
        let state = apply_default_memory_pool(
            SessionStateBuilder::new().with_default_features(),
        )
        .build();
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
