//! Σ.AI.6 — bounded-by-default DataFusion memory pool (2026-07-08).
//!
//! ## Why (the Q09 SF100 memory cliff)
//!
//! ematix historically built sessions on DataFusion's default
//! `UnboundedMemoryPool`. Memory-aware operators (repartition buffers,
//! aggregates, sorts) then balloon without backpressure; on a box whose
//! RAM is small relative to the working set, that anonymous memory
//! evicts the OS page cache under the scans and the whole pipeline
//! thrashes — or the kernel OOM-kills the process outright.
//!
//! Measured on the AWS single-node campaign box (c7i.4xlarge, 32 GB,
//! TPC-H SF=100 flat, Q09 in isolation, 2026-07-08 A/B
//! `exp/q09-mem-ab`):
//!
//! | pool                    | Q09 median      |
//! |-------------------------|-----------------|
//! | unbounded (old default) | 94.3 s (thrash) |
//! | bounded at 0.7 × RAM    | **6.58 s**      |
//! | DuckDB same box         | 6.37 s          |
//!
//! The same unbounded default is what let the parted-SF100 run get
//! kernel-OOM-killed (Q07) and a `prefer_hash_join=false` diagnostic
//! balloon to 31.7 GB anon RSS. A bounded pool converts both into
//! either graceful backpressure or a *recoverable* per-query
//! `ResourcesExhausted` error — never a dead process.
//!
//! ## What
//!
//! [`apply_default_memory_pool`] attaches a `RuntimeEnv` whose memory
//! pool is capped at [`DEFAULT_FRACTION`] of physical RAM to a
//! `SessionStateBuilder` — **only when the caller has not installed a
//! `RuntimeEnv` of their own** (an explicit `with_runtime_env` always
//! wins). It is called from `preset::with_optimizer_rules_overridden`,
//! the single session-construction choke point, so production
//! (`DistributedBackend::build_context`), every bench harness, and
//! library consumers all get the same bound — bench == release.
//!
//! ## Override
//!
//! `EMAT_MEM_POOL_FRACTION` — `0` (or `false`/`off`) restores the old
//! unbounded behaviour; a float in `(0, 1]` sets the fraction; unset or
//! unparsable = [`DEFAULT_FRACTION`]. Recorded by `flags::dump_active`
//! like every other `EMAT_*` override.
//!
//! Unknown RAM (exotic platform: no `/proc/meminfo`, no `sysctl`)
//! degrades to the old unbounded behaviour rather than guessing a cap.

use std::sync::OnceLock;

use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::session_state::SessionStateBuilder;

/// Default cap as a fraction of physical RAM. 0.7 is the measured
/// winner on the 32 GB campaign box (6.58 s Q09 vs 94.3 s unbounded);
/// a looser 0.85 diagnostic arm still thrashed (80.9 s) because too
/// little page cache survived under the 35 GB dataset.
pub const DEFAULT_FRACTION: f64 = 0.7;

/// Resolved `EMAT_MEM_POOL_FRACTION`: `None` = explicitly disabled
/// (unbounded), `Some(f)` = cap at `f × RAM`.
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
            // Unparsable / out-of-range: keep the shipped default
            // rather than silently unbinding the pool.
            _ => Some(DEFAULT_FRACTION),
        },
        None => Some(DEFAULT_FRACTION),
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

    /// Parse table for `EMAT_MEM_POOL_FRACTION` (pure, no env).
    #[test]
    fn fraction_parse_table() {
        assert_eq!(fraction_of(None), Some(DEFAULT_FRACTION), "unset = AUTO");
        assert_eq!(fraction_of(Some("0")), None, "=0 disables");
        assert_eq!(fraction_of(Some("false")), None);
        assert_eq!(fraction_of(Some("off")), None);
        assert_eq!(fraction_of(Some("0.5")), Some(0.5));
        assert_eq!(fraction_of(Some("1")), Some(1.0));
        // Garbage / out-of-range keeps the shipped default (never
        // silently unbinds).
        assert_eq!(fraction_of(Some("1.5")), Some(DEFAULT_FRACTION));
        assert_eq!(fraction_of(Some("-0.2")), Some(DEFAULT_FRACTION));
        assert_eq!(fraction_of(Some("banana")), Some(DEFAULT_FRACTION));
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
        // SAFETY: single-threaded within this test; restored before exit.
        unsafe { std::env::remove_var("EMAT_MEM_POOL_FRACTION") };

        // Arm 1: default bound engages.
        let state = apply_default_memory_pool(
            SessionStateBuilder::new().with_default_features(),
        )
        .build();
        let limit = effective_limit_bytes().expect("RAM known per test above");
        let pool = state.runtime_env().memory_pool.clone();
        let r = MemoryConsumer::new("test-over-cap").register(&pool);
        assert!(
            r.try_grow(limit + (1 << 20)).is_err(),
            "over-cap reservation must be refused (limit={limit})"
        );
        r.free();
        let small = MemoryConsumer::new("test-small").register(&pool);
        assert!(small.try_grow(1 << 20).is_ok(), "1 MiB must fit");
        small.free();

        // Arm 2: explicit `=0` restores unbounded.
        unsafe { std::env::set_var("EMAT_MEM_POOL_FRACTION", "0") };
        let state = apply_default_memory_pool(
            SessionStateBuilder::new().with_default_features(),
        )
        .build();
        let pool = state.runtime_env().memory_pool.clone();
        let r = MemoryConsumer::new("test-unbounded").register(&pool);
        assert!(
            r.try_grow(1 << 40).is_ok(),
            "disabled pool must accept 1 TiB (unbounded legacy)"
        );
        r.free();
        unsafe { std::env::remove_var("EMAT_MEM_POOL_FRACTION") };

        // Arm 3: caller-installed RuntimeEnv wins over the default.
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
        let pool = state.runtime_env().memory_pool.clone();
        let r = MemoryConsumer::new("test-caller-pool").register(&pool);
        assert!(
            r.try_grow(2 << 20).is_err(),
            "caller's 1 MiB pool must still be in effect (not replaced by the default)"
        );
        r.free();
    }
}
