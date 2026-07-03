//! Central accessors and an active-config dump for `EMAT_*` environment flags.
//!
//! This is the single place that defines **how** an `EMAT_*` flag is read.
//! Historically flags were read inline at ~40 call sites with three inconsistent
//! idioms, so you could not tell a flag's default-state from its name. The full
//! inventory (default-state, value, owner, purpose for every flag) lives in
//! [`docs/EMAT_FLAGS.md`](../../../docs/EMAT_FLAGS.md). The canonical read helpers
//! are:
//!
//! - [`enabled`]  — default-**ON**, opt out with `=0` / `=false`
//! - [`opt_in`]   — default-**OFF**, opt in with `=1` / `=true`
//! - [`present`]  — default-**OFF**, on if the var is set to any value
//! - [`usize_or`] / [`u64_or`] / [`f64_or`] — numeric tunables with an explicit default
//!
//! [`dump_active`] returns a one-line summary of the `EMAT_*` overrides currently
//! in the environment so any run (bench or production) can record exactly which
//! non-default flags were in effect — the "stop losing track of settings" win.

use std::env;

/// Default-**ON** gate: `true` unless the var is `0`/`false` (case-insensitive).
/// Opt out with `=0`.
pub fn enabled(var: &str) -> bool {
    env::var(var)
        .ok()
        .map(|v| !is_off_value(&v))
        .unwrap_or(true)
}

/// Opt-**in** gate: `true` only if the var is `1`/`true` (case-insensitive).
/// Default OFF.
pub fn opt_in(var: &str) -> bool {
    env::var(var)
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// Presence gate: `true` if the var is set to any value at all. Default OFF.
pub fn present(var: &str) -> bool {
    env::var(var).is_ok()
}

/// Tri-state gate (Σ.AI.5 scale-gated levers): explicit `=1`/`=true` →
/// `Some(true)` (force ON), explicit `=0`/`=false` → `Some(false)` (force
/// OFF), unset or unrecognized → `None` (AUTO — the caller decides, e.g.
/// from [`crate::scale_class`]).
pub fn tri_state(var: &str) -> Option<bool> {
    tri_state_of(env::var(var).ok().as_deref())
}

/// Pure core of [`tri_state`] so tests can pin the parse table without
/// racing on process-global env vars (the `resolve_l9_ndv_max_rows`
/// convention).
fn tri_state_of(val: Option<&str>) -> Option<bool> {
    match val {
        Some(v) if v == "1" || v.eq_ignore_ascii_case("true") => Some(true),
        Some(v) if v == "0" || v.eq_ignore_ascii_case("false") => Some(false),
        _ => None,
    }
}

/// Σ.AI.5 scale-gated lever (2026-07-02 campaign gating): explicit env
/// value wins in BOTH directions (`=1` force on, `=0` force off); unset =
/// AUTO — default **ON** only when the process has observed an SF≥100-class
/// dataset ([`crate::scale_class::large_scale_seen`]), default OFF below.
/// Used by the levers the 2026-07-01 campaign proved pay only at SF=100
/// scale: `EMAT_DOWNCAST_KEYS` + `EMAT_NARROW_KEY_DECODE` (Q09 −1075 ms,
/// but net +10% at SF=10), `EMAT_DATE_BUILD_SIDE` + `EMAT_NDV_BUILD_SIDE`
/// (Q10 −947 ms, neutral at SF=10), `EMAT_FD_GROUPBY` (Q10 −99 ms, no
/// regressions — gated consistently with its siblings).
pub fn scale_gated_large(var: &str) -> bool {
    tri_state(var).unwrap_or_else(crate::scale_class::large_scale_seen)
}

/// Numeric tunable: parse the var as `usize`, else fall back to `default`.
pub fn usize_or(var: &str, default: usize) -> usize {
    env::var(var)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Numeric tunable: parse the var as `u64`, else fall back to `default`.
pub fn u64_or(var: &str, default: u64) -> u64 {
    env::var(var)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Numeric tunable: parse the var as `f64`, else fall back to `default`.
pub fn f64_or(var: &str, default: f64) -> f64 {
    env::var(var)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn is_off_value(v: &str) -> bool {
    v == "0" || v.eq_ignore_ascii_case("false")
}

/// Flags that ship **ON** (read via [`enabled`]). Used only to annotate the dump:
/// one of these set to `0`/`false` is disabling a shipped default, which is
/// high-signal. Keep in sync with the "Production gate (default-ON)" section of
/// `docs/EMAT_FLAGS.md`. (The dump itself does not depend on this list being
/// complete — it scans the live environment.)
pub const DEFAULT_ON: &[&str] = &[
    "EMAT_AGG_SEMI",
    "EMAT_COLLECT_LEFT_SEMI_BROADCAST",
    "EMAT_CSE_FILTER_FUSION",
    "EMAT_CSE_PARALLEL",
    "EMAT_DIM_PUSH",
    "EMAT_L9_FUSED_PROBE",
    "EMAT_L9_REQUIRE_FILTERED_BUILD",
    "EMAT_L9_TIGHT_CARDINALITY",
    "EMAT_NO_PROVIDER_CACHE",
    "EMAT_Q20_SEMI",
    "EMAT_RANGE_AGG",
    "EMAT_REORDER_QP",
    "EMAT_REORDER_STATS_AWARE_GATE",
    "EMAT_RG_DECODE_CACHE",
    "EMAT_RH_AVG_F64_VEC",
    "EMAT_RH_SUM_F64_VEC",
    "EMAT_SCALAR_AGG_BOOST",
    "EMAT_TRANSITIVE_DIM_SEMI",
];

/// All `EMAT_*` variables currently set in the environment, sorted by name.
/// Maintenance-free: reads the live environment, so it never drifts from the code.
pub fn active_env_flags() -> Vec<(String, String)> {
    let mut v: Vec<(String, String)> = env::vars()
        .filter(|(k, _)| k.starts_with("EMAT_"))
        .collect();
    v.sort();
    v
}

/// Format one flag for the dump, annotating a disabled shipped-default.
fn fmt_flag(name: &str, val: &str) -> String {
    if DEFAULT_ON.contains(&name) && is_off_value(val) {
        format!("{name}={val} (disables default)")
    } else {
        format!("{name}={val}")
    }
}

/// One-line human summary of the `EMAT_*` overrides in effect, e.g.
/// `EMAT_HASH_JOIN=1, EMAT_DIM_PUSH=0 (disables default)`, or
/// `none (shipped defaults)` when no `EMAT_*` var is set. Print this once per run
/// to make "what's actually on right now" observable.
pub fn dump_active() -> String {
    let active = active_env_flags();
    if active.is_empty() {
        return "none (shipped defaults)".to_string();
    }
    active
        .iter()
        .map(|(k, v)| fmt_flag(k, v))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Serializes every test in this crate that mutates a PRODUCTION `EMAT_*`
/// environment variable. The environment is process-global and the parallel
/// test runner interleaves tests freely, so two tests toggling flags — even
/// different flags in different modules — can break each other's read-back
/// assertions (observed: `EMAT_NARROW_KEY_DECODE` fire-counter flakes in
/// `ematix_fast_parquet`, and `EMAT_LARGE_SCALE_MIN_ROWS` is mutated from two
/// modules, which module-local locks cannot serialize). Async tests hold the
/// guard across awaits — `tokio::sync::Mutex` makes that clippy-clean
/// (`await_holding_lock`); sync tests use `blocking_lock()`.
///
/// Rule: any test doing `set_var`/`remove_var` on a real flag takes this lock
/// first and restores the var before dropping the guard. Tests that only READ
/// flags don't take it (pre-existing exposure, unchanged). `flags::tests`
/// itself uses synthetic `EMAT_TEST_*` names and is exempt by design.
#[cfg(test)]
pub(crate) static EMAT_ENV_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[cfg(test)]
mod tests {
    use super::*;

    // Tests use UNIQUE var names so the parallel test runner doesn't race on the
    // process-global environment, and never mutate a real production flag.
    fn set(k: &str, v: &str) {
        unsafe { env::set_var(k, v) };
    }
    fn clear(k: &str) {
        unsafe { env::remove_var(k) };
    }

    #[test]
    fn enabled_defaults_on_and_opts_out() {
        let k = "EMAT_TEST_ENABLED_A";
        clear(k);
        assert!(enabled(k), "absent => ON");
        set(k, "0");
        assert!(!enabled(k), "0 => off");
        set(k, "false");
        assert!(!enabled(k), "false => off");
        set(k, "FALSE");
        assert!(!enabled(k), "FALSE => off (case-insensitive)");
        set(k, "1");
        assert!(enabled(k), "1 => on");
        set(k, "anything");
        assert!(enabled(k), "any other value => on");
        clear(k);
    }

    #[test]
    fn opt_in_defaults_off_and_opts_in() {
        let k = "EMAT_TEST_OPTIN_B";
        clear(k);
        assert!(!opt_in(k), "absent => off");
        set(k, "1");
        assert!(opt_in(k), "1 => on");
        set(k, "true");
        assert!(opt_in(k), "true => on");
        set(k, "TRUE");
        assert!(opt_in(k), "TRUE => on (case-insensitive)");
        set(k, "0");
        assert!(!opt_in(k), "0 => off");
        set(k, "yes");
        assert!(!opt_in(k), "only 1/true count as on");
        clear(k);
    }

    #[test]
    fn tri_state_parse_table() {
        // Pure parse table — no env mutation, no race.
        assert_eq!(tri_state_of(None), None, "unset => AUTO");
        assert_eq!(tri_state_of(Some("1")), Some(true), "1 => force ON");
        assert_eq!(tri_state_of(Some("true")), Some(true));
        assert_eq!(tri_state_of(Some("TRUE")), Some(true), "case-insensitive");
        assert_eq!(tri_state_of(Some("0")), Some(false), "0 => force OFF");
        assert_eq!(tri_state_of(Some("false")), Some(false));
        assert_eq!(tri_state_of(Some("FALSE")), Some(false));
        assert_eq!(
            tri_state_of(Some("yes")),
            None,
            "unrecognized value => AUTO (documented in EMAT_FLAGS.md)"
        );
        assert_eq!(tri_state_of(Some("")), None, "set-but-empty => AUTO");
    }

    #[test]
    fn scale_gated_large_env_overrides_beat_auto() {
        // Unique var name (parallel-runner convention above). The AUTO arm
        // is exercised end-to-end in scale_class + the plan-diff tests;
        // here we pin only that explicit values win in both directions.
        let k = "EMAT_TEST_SCALE_GATED_E";
        set(k, "1");
        assert!(scale_gated_large(k), "=1 forces ON at any scale");
        set(k, "0");
        assert!(!scale_gated_large(k), "=0 forces OFF at any scale");
        clear(k);
    }

    #[test]
    fn present_is_presence_activated() {
        let k = "EMAT_TEST_PRESENT_C";
        clear(k);
        assert!(!present(k), "absent => off");
        set(k, "");
        assert!(present(k), "set-but-empty => present");
        set(k, "x");
        assert!(present(k));
        clear(k);
    }

    #[test]
    fn numeric_tunables_parse_or_fall_back() {
        let k = "EMAT_TEST_NUM_D";
        clear(k);
        assert_eq!(usize_or(k, 7), 7, "absent => default");
        set(k, "42");
        assert_eq!(usize_or(k, 7), 42);
        set(k, "nope");
        assert_eq!(usize_or(k, 7), 7, "unparseable => default");
        set(k, "100");
        assert_eq!(u64_or(k, 9), 100);
        clear(k);
        assert!((f64_or(k, 1.25) - 1.25).abs() < 1e-9, "absent => default");
        set(k, "2.5");
        assert!((f64_or(k, 1.25) - 2.5).abs() < 1e-9);
        clear(k);
    }

    #[test]
    fn active_flags_lists_set_emat_vars_sorted() {
        let k = "EMAT_TEST_ZZ_ACTIVE_E";
        set(k, "1");
        let active = active_env_flags();
        assert!(
            active.iter().any(|(kk, vv)| kk == k && vv == "1"),
            "a set EMAT_ var must appear in the active list"
        );
        assert!(
            active.iter().all(|(kk, _)| kk.starts_with("EMAT_")),
            "only EMAT_ vars are listed"
        );
        let mut sorted = active.clone();
        sorted.sort();
        assert_eq!(active, sorted, "active list is sorted");
        clear(k);
    }

    #[test]
    fn dump_formats_and_annotates_disabled_defaults() {
        // fmt_flag is pure — exercise it directly so we never mutate a real flag.
        assert!(DEFAULT_ON.contains(&"EMAT_TRANSITIVE_DIM_SEMI"));
        assert!(DEFAULT_ON.contains(&"EMAT_DIM_PUSH"));
        assert_eq!(
            fmt_flag("EMAT_DIM_PUSH", "0"),
            "EMAT_DIM_PUSH=0 (disables default)"
        );
        assert_eq!(
            fmt_flag("EMAT_DIM_PUSH", "false"),
            "EMAT_DIM_PUSH=false (disables default)"
        );
        // opt-in flag set => shown verbatim, no annotation
        assert_eq!(fmt_flag("EMAT_HASH_JOIN", "1"), "EMAT_HASH_JOIN=1");
        // a default-on flag set to its on-value is not a disable
        assert_eq!(fmt_flag("EMAT_DIM_PUSH", "1"), "EMAT_DIM_PUSH=1");
    }
}
