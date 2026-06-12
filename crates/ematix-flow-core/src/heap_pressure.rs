//! MI.GATE (2026-06-12, #357 follow-up) — should the caller return
//! freed heap to the OS (`mi_collect(true)`) right now?
//!
//! Between-query collects are a measured −5% geomean win at SF=100:
//! mimalloc's per-thread segment retention ratcheted RSS to 10-17GB
//! across a 22-query sweep, starving the OS page cache below the
//! cyclic working set (60-68GB of pageins per pass vs DuckDB's 2-4GB
//! on identical files). But the SAME unconditional collect is a
//! measured **+6.1% geomean TAX at SF=10** (interleaved A/B/A/B,
//! 2026-06-12; up to +25% on sub-60ms queries): with no page-cache
//! pressure to relieve, every collect tears down the warm heap and
//! the next query re-faults it page by page.
//!
//! Gate: collect only when the process PEAK RSS (`ru_maxrss`) says the
//! heap is big enough to plausibly starve the page cache. Peak RSS is
//! monotone, so once a workload balloons past the threshold the
//! discipline stays on for the rest of the process — exactly the
//! SF=100 sweep shape — while small-working-set processes never pay.
//!
//! Env contract (shared by the bench harness and the production
//! worker so published numbers reflect shipped behavior):
//!   - `EMAT_MI_COLLECT=0`  never collect
//!   - `EMAT_MI_COLLECT=1`  always collect (legacy/diagnostic)
//!   - unset / other        auto: collect iff peak RSS ≥ threshold
//!   - `EMAT_MI_COLLECT_MIN_RSS_MB` auto threshold (default 6144)

/// Process peak RSS in MB via `getrusage` (no fork, ~ns). macOS
/// reports `ru_maxrss` in bytes; Linux in kilobytes.
pub fn peak_rss_mb() -> Option<f64> {
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) } != 0 {
        return None;
    }
    #[cfg(target_os = "macos")]
    let mb = usage.ru_maxrss as f64 / (1024.0 * 1024.0);
    #[cfg(not(target_os = "macos"))]
    let mb = usage.ru_maxrss as f64 / 1024.0;
    Some(mb)
}

/// Auto-mode threshold (MB). Default 6144: comfortably above any
/// SF≤10-class working set (peaks ~2-4GB) and below the 10-17GB
/// retention the SF=100 sweep measured.
pub fn min_collect_rss_mb() -> f64 {
    std::env::var("EMAT_MI_COLLECT_MIN_RSS_MB")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(6144.0)
}

/// The full gate: master switch + pressure threshold. Callers that
/// hold a mimalloc dependency do
/// `if should_mi_collect() { unsafe { mi_collect(true) } }`.
pub fn should_mi_collect() -> bool {
    match std::env::var("EMAT_MI_COLLECT").ok().as_deref() {
        Some("0") => false,
        Some("1") => true,
        _ => peak_rss_mb().is_some_and(|rss| rss >= min_collect_rss_mb()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peak_rss_reports_plausible_mb() {
        let rss = peak_rss_mb().expect("getrusage must succeed");
        assert!(
            rss > 1.0 && rss < 1_000_000.0,
            "peak RSS should be a plausible MB figure, got {rss}"
        );
    }

    /// The three-state contract: 0=never, 1=always, auto=threshold.
    /// (Env-var test: the only other reader is the gate itself.)
    #[test]
    fn gate_modes() {
        unsafe { std::env::set_var("EMAT_MI_COLLECT", "0") };
        assert!(!should_mi_collect(), "=0 must disable");
        unsafe { std::env::set_var("EMAT_MI_COLLECT", "1") };
        assert!(should_mi_collect(), "=1 must force-enable");
        unsafe { std::env::remove_var("EMAT_MI_COLLECT") };

        unsafe { std::env::set_var("EMAT_MI_COLLECT_MIN_RSS_MB", "0") };
        assert!(
            should_mi_collect(),
            "auto with threshold 0 must collect (any RSS qualifies)"
        );
        unsafe { std::env::set_var("EMAT_MI_COLLECT_MIN_RSS_MB", "999999999") };
        assert!(
            !should_mi_collect(),
            "auto with an unreachable threshold must skip (SF=10-class)"
        );
        unsafe { std::env::remove_var("EMAT_MI_COLLECT_MIN_RSS_MB") };
    }
}
