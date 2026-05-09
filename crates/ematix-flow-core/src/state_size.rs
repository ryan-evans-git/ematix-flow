//! P3 #23: config-load warning for projected per-window state-blob
//! memory. Pairs with the existing fail-loud `max_groups_per_window`
//! cap — the cap stops a runaway pipeline once it's already running;
//! the warning fires upfront so users notice "this config will need
//! ~2 GiB of resident state under realistic load" before they ship.
//!
//! Threshold default is 1 GiB. Override with the
//! `EMATIX_FLOW_STATE_SIZE_WARN_BYTES` env var (parsed as `usize`).
//!
//! The projection deliberately errs on the side of *over*-estimating
//! per-group bytes — false positives (warn on a config that actually
//! fits) are cheap; false negatives (silent OOM at runtime) are not.
//! Source heuristic: `docs/PHASE_39_5_SESSIONS.md` "~200B/state_blob"
//! design-doc note.

use crate::windowed::{WindowConfig, WindowKind};

/// Default warn threshold: 1 GiB. Empirically the point where
/// per-pipeline RSS becomes worth flagging on a typical 8–16 GiB
/// runtime host.
pub const DEFAULT_THRESHOLD_BYTES: usize = 1 << 30;

/// Env var that overrides [`DEFAULT_THRESHOLD_BYTES`]. Value is a
/// decimal `usize` — set to `0` to disable the warning entirely.
pub const ENV_THRESHOLD_VAR: &str = "EMATIX_FLOW_STATE_SIZE_WARN_BYTES";

/// Resolve the active warn threshold. Honors [`ENV_THRESHOLD_VAR`] if
/// set + parseable; falls back to [`DEFAULT_THRESHOLD_BYTES`].
pub fn warn_threshold_bytes() -> usize {
    std::env::var(ENV_THRESHOLD_VAR)
        .ok()
        .and_then(|s| s.trim().parse::<usize>().ok())
        .unwrap_or(DEFAULT_THRESHOLD_BYTES)
}

/// Heuristic per-group state-blob size for a window config.
///
/// Calibrated against the postcard-encoded shapes that
/// `WindowedAggregateTransform::take_state_commit` produces:
///
/// - `~96 B` baseline: window-id discriminant + event_ts + accs[]
///   header.
/// - `~24 B` per running aggregator state slot.
/// - `~16 B + name length` per group-by column (the encoded key
///   carries the name).
/// - `+64 B` for session windows: session id (16 B), active-state
///   flags, and valid-from/valid-to range bookkeeping.
///
/// Real blobs vary with column types and accumulator kind; this is
/// a deliberately conservative estimate so a warning fires before
/// the pipeline actually OOMs.
pub fn estimated_window_blob_bytes(cfg: &WindowConfig) -> usize {
    let mut bytes: usize = 96;
    bytes = bytes.saturating_add(cfg.aggregations.len().saturating_mul(24));
    for col in &cfg.group_by {
        bytes = bytes.saturating_add(16usize.saturating_add(col.len()));
    }
    if matches!(cfg.kind, WindowKind::Session) {
        bytes = bytes.saturating_add(64);
    }
    bytes
}

/// Total state-blob memory projection for a window config:
/// `max_groups_per_window × estimated_window_blob_bytes`. Saturating
/// arithmetic, so a degenerately-large `max_groups_per_window`
/// pegs at `usize::MAX` instead of overflowing.
pub fn project_window_state_bytes(cfg: &WindowConfig) -> usize {
    cfg.max_groups_per_window
        .saturating_mul(estimated_window_blob_bytes(cfg))
}

/// Emit a `tracing::warn!` if `projected_bytes > threshold_bytes`.
/// Returns `true` when the warning fires.
///
/// Pure-function form — caller supplies the threshold so unit tests
/// don't have to touch the process env. Production callers use
/// [`warn_if_exceeds_default`] which resolves the threshold via
/// [`warn_threshold_bytes`].
///
/// `label` is the human-readable identifier (typically the pipeline
/// name) included in the warning so multi-pipeline deployments can
/// trace which config tripped the budget.
pub fn warn_if_exceeds(label: &str, projected_bytes: usize, threshold_bytes: usize) -> bool {
    if threshold_bytes == 0 || projected_bytes <= threshold_bytes {
        return false;
    }
    let projected_mib = projected_bytes as f64 / (1usize << 20) as f64;
    let threshold_mib = threshold_bytes as f64 / (1usize << 20) as f64;
    tracing::warn!(
        label = %label,
        projected_bytes = projected_bytes,
        threshold_bytes = threshold_bytes,
        "projected state-blob memory ({projected_mib:.1} MiB) exceeds the \
         warn threshold ({threshold_mib:.1} MiB). Lower max_groups_per_window, \
         or override via {ENV_THRESHOLD_VAR} (set to 0 to silence)."
    );
    true
}

/// Convenience wrapper: fire the warning against the env-resolved
/// default threshold ([`warn_threshold_bytes`]). The shape callers
/// reach for at config-load time.
pub fn warn_if_exceeds_default(label: &str, projected_bytes: usize) -> bool {
    warn_if_exceeds(label, projected_bytes, warn_threshold_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::windowed::{AggKind, AggregationSpec, LateDataPolicy, WindowConfig, WindowKind};

    fn tumbling_cfg(max_groups: usize, group_by: Vec<&str>) -> WindowConfig {
        WindowConfig {
            kind: WindowKind::Tumbling,
            duration_ms: 60_000,
            hop_ms: 60_000,
            gap_ms: None,
            max_session_duration_ms: None,
            event_time_column: "_event_ts".into(),
            group_by: group_by.into_iter().map(String::from).collect(),
            aggregations: vec![AggregationSpec::new(AggKind::CountStar, None, "n")],
            late_data: LateDataPolicy::Drop,
            max_groups_per_window: max_groups,
            window_start_column: "window_start".into(),
            window_end_column: "window_end".into(),
            session_id_column: "session_id".into(),
        }
    }

    fn session_cfg(max_groups: usize, group_by: Vec<&str>) -> WindowConfig {
        let mut c = tumbling_cfg(max_groups, group_by);
        c.kind = WindowKind::Session;
        c.duration_ms = 0;
        c.hop_ms = 0;
        c.gap_ms = Some(30_000);
        c.max_session_duration_ms = Some(120_000);
        c
    }

    #[test]
    fn project_scales_linearly_with_max_groups() {
        let small = project_window_state_bytes(&tumbling_cfg(1_000, vec!["k"]));
        let large = project_window_state_bytes(&tumbling_cfg(1_000_000, vec!["k"]));
        assert_eq!(large, small * 1_000);
    }

    #[test]
    fn session_baseline_higher_than_tumbling() {
        let t = estimated_window_blob_bytes(&tumbling_cfg(1, vec!["k"]));
        let s = estimated_window_blob_bytes(&session_cfg(1, vec!["k"]));
        assert!(
            s > t,
            "session ({s}) should carry more per-group bytes than tumbling ({t}) \
             because of session_id + active-state metadata"
        );
        // Specifically the +64 B session-only add-on.
        assert_eq!(s - t, 64);
    }

    #[test]
    fn group_by_column_name_lengths_contribute() {
        let short = estimated_window_blob_bytes(&tumbling_cfg(1, vec!["k"]));
        let long =
            estimated_window_blob_bytes(&tumbling_cfg(1, vec!["a_quite_long_user_id_column_name"]));
        // Difference = (16 + len("a_quite_long_user_id_column_name")) - (16 + 1)
        let expected_diff = "a_quite_long_user_id_column_name".len() - 1;
        assert_eq!(long - short, expected_diff);
    }

    #[test]
    fn warn_if_exceeds_fires_when_over_threshold() {
        let threshold = 1024;
        assert!(warn_if_exceeds("p", 2 * threshold, threshold));
        assert!(!warn_if_exceeds("p", threshold / 2, threshold));
        // Equal-to-threshold is not "exceeds" — strict >.
        assert!(!warn_if_exceeds("p", threshold, threshold));
    }

    #[test]
    fn warn_if_exceeds_zero_threshold_disables() {
        // Threshold = 0 is the "silence me" sentinel — no warning
        // even when projected pegs at usize::MAX (e.g. saturating
        // overflow on a degenerate config).
        assert!(!warn_if_exceeds("p", usize::MAX, 0));
    }

    /// Sanity-check the env-var override flow without racing other
    /// tests — uses a globally-scoped guard so this test runs
    /// serially when `cargo test` parallelizes the module.
    #[test]
    fn warn_threshold_env_override_parses() {
        use std::sync::Mutex;
        // OnceLock would suffice but Mutex<()> is the simplest
        // serializer for a one-off env-touching test.
        static GUARD: Mutex<()> = Mutex::new(());
        let _g = GUARD.lock().unwrap();
        let prev = std::env::var(ENV_THRESHOLD_VAR).ok();
        // SAFETY: GUARD ensures no other thread reads the env var
        // mid-mutation. Restore the prior value before unlock.
        unsafe { std::env::set_var(ENV_THRESHOLD_VAR, "12345") };
        assert_eq!(warn_threshold_bytes(), 12345);
        unsafe { std::env::set_var(ENV_THRESHOLD_VAR, "not-a-number") };
        // Unparseable falls back to default.
        assert_eq!(warn_threshold_bytes(), DEFAULT_THRESHOLD_BYTES);
        match prev {
            Some(v) => unsafe { std::env::set_var(ENV_THRESHOLD_VAR, v) },
            None => unsafe { std::env::remove_var(ENV_THRESHOLD_VAR) },
        }
    }
}
