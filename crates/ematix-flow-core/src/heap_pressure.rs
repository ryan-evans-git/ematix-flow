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
//! Two failed gate designs, kept here so they are not retried:
//!   - v1, peak RSS (`ru_maxrss`) vs absolute threshold: SELF-LATCHES.
//!     Peak is monotone — a collect can never lower it — and a 20×3
//!     SF=10 sweep ratchets to 7.6GB, crossing 6144MB at execution
//!     39/506 (inside the warmups), so every measured trial collected
//!     and the published SF=10 column carried the full always-on tax.
//!   - v2, peak RSS AND major-fault delta: `ru_majflt` is a DEAD
//!     SIGNAL here. The engine reads parquet via `read()` syscalls,
//!     not mmap — page-cache file IO never major-faults the process
//!     (measured: mf=1 while reading GBs at SF=100). The 60-68GB
//!     "pageins" behind the SF=100 win are system-wide vm_stat
//!     counters, not process faults; v2 would never collect at SF=100
//!     and silently forfeit the −5% win.
//!
//! Gate v3: collect iff CURRENT RSS ≥ max(6144MB, 25% of physical
//! RAM). Current RSS is the mechanism-true, process-local signal —
//! mimalloc retention is the pressure we cause and exactly what a
//! collect releases — and unlike peak it falls when pressure does, so
//! the gate cannot latch. The RAM-relative bar separates the measured
//! classes on a 36GB box (SF=10 sweep ≤7.7GB < 9216MB ≤ SF=100's
//! 10-17GB retention) and scales to smaller boxes, where an SF=10
//! sweep genuinely is cache-tight and collecting is correct.
//!
//! Env contract (shared by the bench harness and the production
//! worker so published numbers reflect shipped behavior):
//!   - `EMAT_MI_COLLECT=0`  never collect
//!   - `EMAT_MI_COLLECT=1`  always collect (legacy/diagnostic)
//!   - unset / other        auto: current RSS ≥ threshold
//!   - `EMAT_MI_COLLECT_MIN_RSS_MB` explicit threshold override
//!     (default: max(6144, physical RAM / 4))

/// Process peak RSS in MB via `getrusage` (no fork, ~ns). macOS
/// reports `ru_maxrss` in bytes; Linux in kilobytes. Monotone —
/// diagnostic/logging only, NOT a gate input (see v1 above).
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

/// Process CURRENT resident set in MB.
#[cfg(target_os = "macos")]
pub fn current_rss_mb() -> Option<f64> {
    let mut info: libc::rusage_info_v4 = unsafe { std::mem::zeroed() };
    let ret = unsafe {
        libc::proc_pid_rusage(
            std::process::id() as libc::c_int,
            libc::RUSAGE_INFO_V4,
            &mut info as *mut _ as *mut libc::rusage_info_t,
        )
    };
    (ret == 0).then(|| info.ri_resident_size as f64 / (1024.0 * 1024.0))
}

/// Process CURRENT resident set in MB.
#[cfg(not(target_os = "macos"))]
pub fn current_rss_mb() -> Option<f64> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let pages: f64 = statm.split_whitespace().nth(1)?.parse().ok()?;
    let page_bytes = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    (page_bytes > 0).then(|| pages * page_bytes as f64 / (1024.0 * 1024.0))
}

/// Physical RAM in MB.
#[cfg(target_os = "macos")]
pub fn physical_ram_mb() -> Option<f64> {
    let mut size: u64 = 0;
    let mut len = std::mem::size_of::<u64>();
    let ret = unsafe {
        libc::sysctlbyname(
            c"hw.memsize".as_ptr(),
            &mut size as *mut _ as *mut libc::c_void,
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    (ret == 0 && size > 0).then(|| size as f64 / (1024.0 * 1024.0))
}

/// Physical RAM in MB.
#[cfg(not(target_os = "macos"))]
pub fn physical_ram_mb() -> Option<f64> {
    let pages = unsafe { libc::sysconf(libc::_SC_PHYS_PAGES) };
    let page_bytes = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    (pages > 0 && page_bytes > 0).then(|| pages as f64 * page_bytes as f64 / (1024.0 * 1024.0))
}

/// Auto-mode threshold (MB): explicit env override, else
/// max(6144, physical RAM / 4).
pub fn min_collect_rss_mb() -> f64 {
    if let Some(v) = std::env::var("EMAT_MI_COLLECT_MIN_RSS_MB")
        .ok()
        .and_then(|s| s.parse().ok())
    {
        return v;
    }
    let ram_quarter = physical_ram_mb().map(|r| r / 4.0).unwrap_or(0.0);
    ram_quarter.max(6144.0)
}

/// The full gate: master switch + pressure threshold. Callers that
/// hold a mimalloc dependency do
/// `if should_mi_collect() { unsafe { mi_collect(true) } }`.
pub fn should_mi_collect() -> bool {
    match std::env::var("EMAT_MI_COLLECT").ok().as_deref() {
        Some("0") => false,
        Some("1") => true,
        _ => current_rss_mb().is_some_and(|rss| rss >= min_collect_rss_mb()),
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

    /// MI.GATE.2 regression: the gate input must be CURRENT resident
    /// size, which is bounded by (and normally well under) the
    /// monotone peak — a current reading materially above peak means
    /// the platform call is returning the wrong quantity.
    #[test]
    fn current_rss_is_plausible_and_bounded_by_peak() {
        let cur = current_rss_mb().expect("current RSS must be readable");
        let peak = peak_rss_mb().expect("getrusage must succeed");
        assert!(cur > 1.0, "current RSS should be nonzero MB, got {cur}");
        assert!(
            cur <= peak * 1.05,
            "current RSS ({cur}MB) cannot exceed peak ({peak}MB)"
        );
    }

    /// The three-state contract: 0=never, 1=always, auto=threshold.
    /// (THE env-var test — keep all EMAT_MI_COLLECT* mutation in this
    /// one test; parallel test threads share the process environment.)
    #[test]
    fn gate_modes() {
        // Crate-wide env lock: serializes this test's EMAT_MI_COLLECT*
        // windows against every other env-mutating test (sync → blocking).
        let _env = crate::flags::EMAT_ENV_TEST_LOCK.blocking_lock();
        unsafe { std::env::remove_var("EMAT_MI_COLLECT_MIN_RSS_MB") };
        let ram = physical_ram_mb().expect("RAM size must be readable");
        assert_eq!(
            min_collect_rss_mb(),
            (ram / 4.0).max(6144.0),
            "default threshold must be max(6144, RAM/4)"
        );

        unsafe { std::env::set_var("EMAT_MI_COLLECT", "0") };
        assert!(!should_mi_collect(), "=0 must disable");
        unsafe { std::env::set_var("EMAT_MI_COLLECT", "1") };
        assert!(should_mi_collect(), "=1 must force-enable");
        unsafe { std::env::remove_var("EMAT_MI_COLLECT") };

        unsafe { std::env::set_var("EMAT_MI_COLLECT_MIN_RSS_MB", "0") };
        assert!(
            should_mi_collect(),
            "auto with threshold 0 must collect (any current RSS qualifies)"
        );
        unsafe { std::env::set_var("EMAT_MI_COLLECT_MIN_RSS_MB", "999999999") };
        assert!(
            !should_mi_collect(),
            "auto with an unreachable threshold must skip (warm-cache class)"
        );
        unsafe { std::env::remove_var("EMAT_MI_COLLECT_MIN_RSS_MB") };
    }
}
