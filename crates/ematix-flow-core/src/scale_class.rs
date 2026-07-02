//! Σ.AI.5 — dataset scale classification for scale-gated lever defaults.
//!
//! The 2026-07-01 campaign (bench-results/campaign-2026-07-01/REPORT.md §2/§4)
//! proved a set of levers that flip the SF=100 losses but are neutral-to-
//! harmful at SF≤10: narrow keys (`EMAT_DOWNCAST_KEYS` +
//! `EMAT_NARROW_KEY_DECODE`, Q09 −1075 ms at SF=100 vs net +10% at SF=10),
//! the build-side swaps (`EMAT_DATE_BUILD_SIDE` + `EMAT_NDV_BUILD_SIDE`,
//! Q10 −947 ms), and `EMAT_FD_GROUPBY` (Q10 −99 ms). Those levers default
//! ON only when the workload is SF≥100-class, following the PR #159
//! precedent (`agg_filter_pushdown::TRANSITIVE_SEMI_MAX_TARGET_ROWS`):
//! classify by **table row-count statistics**, never by data-dir naming.
//!
//! ## How scale is detected
//!
//! Every parquet table provider construction calls [`observe_file`], which
//! records the **maximum footer row count across the file's sibling
//! `*.parquet` files** (memoized per canonical parent directory — one
//! footer-only metadata read per sibling, once per process). A dataset is
//! "large-scale" when any observed table crosses
//! [`LARGE_SCALE_MIN_FACT_ROWS`] (300M — SF=100 lineitem is 600M, SF=10 is
//! 60M, so the threshold splits them with 2× margin on either side).
//!
//! Scanning **siblings** (not just the constructed file) makes the
//! classification registration-order independent: TPC-H harnesses register
//! `region` (5 rows) before `lineitem`, and the narrow-keys lever must
//! decide at provider-construction time whether to advertise Int32 keys —
//! consistently across every table of the dataset, or join key types
//! would diverge. The co-location assumption (facts and dims share a
//! directory) matches every TPC-H layout in this repo; datasets that split
//! tables across directories still classify correctly as soon as the
//! fact-table's own provider is constructed.
//!
//! ## Overrides
//!
//! - Per-lever: the lever's own env var stays authoritative in both
//!   directions (`=0` force off, `=1` force on, unset = auto) — see
//!   [`crate::flags::scale_gated_large`].
//! - Threshold: `EMAT_LARGE_SCALE_MIN_ROWS=N` (numeric tunable, read at
//!   classification time — tests use it to exercise the auto-ON arms
//!   against small fixtures without fabricating 300M-row files).
//!
//! ## Caveats (documented, accepted)
//!
//! - The high-water mark is process-global and only grows: registering a
//!   second, larger dataset mid-process flips AUTO levers ON for later
//!   queries against the small dataset too. Mixed-scale processes should
//!   pin the levers explicitly.
//! - The threshold env var is consulted at classification time, so the
//!   classification (not the recorded maximum) responds to env changes.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};

/// AUTO threshold: a dataset is SF≥100-class when any of its tables has at
/// least this many rows. 300M sits between SF=10 lineitem (60M) and SF=100
/// lineitem (600M) with 2× margin on either side, and above every TPC-H
/// dimension at SF ≤ 100 (orders at SF=100 is 150M).
pub const LARGE_SCALE_MIN_FACT_ROWS: usize = 300_000_000;

/// Process-global high-water mark: max footer row count observed across
/// all providers' dataset directories. Grows monotonically.
static MAX_TABLE_ROWS_SEEN: AtomicUsize = AtomicUsize::new(0);

/// Per-directory sibling-scan memo (canonical dir → max sibling rows).
static DIR_MAX_ROWS: OnceLock<Mutex<HashMap<PathBuf, usize>>> = OnceLock::new();

fn dir_memo() -> &'static Mutex<HashMap<PathBuf, usize>> {
    DIR_MAX_ROWS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// The AUTO threshold currently in effect (`EMAT_LARGE_SCALE_MIN_ROWS`
/// override, else [`LARGE_SCALE_MIN_FACT_ROWS`]).
pub fn threshold() -> usize {
    crate::flags::usize_or("EMAT_LARGE_SCALE_MIN_ROWS", LARGE_SCALE_MIN_FACT_ROWS)
}

/// Has this process observed an SF≥100-class table? (See module docs for
/// the exact semantics.) This is the AUTO arm of
/// [`crate::flags::scale_gated_large`].
pub fn large_scale_seen() -> bool {
    MAX_TABLE_ROWS_SEEN.load(Ordering::Relaxed) >= threshold()
}

/// Record a parquet file at provider-construction time: memoized
/// max-sibling-rows scan of its parent directory, folded into the process
/// high-water mark. Returns whether the dataset classifies large **right
/// now** (callers that must decide at construction time — the narrow-keys
/// schema advertisement — use the return value; rule-time callers use
/// [`large_scale_seen`]).
pub fn observe_file(path: &str) -> bool {
    let max = dir_max_parquet_rows(Path::new(path));
    if max > 0 {
        MAX_TABLE_ROWS_SEEN.fetch_max(max, Ordering::Relaxed);
    }
    large_scale_seen()
}

/// Max footer `num_rows` across `*.parquet` files in `path`'s parent
/// directory (including `path` itself), memoized per canonical directory.
/// Footer-only reads via `emat_parquet_metadata::num_rows` — no page I/O.
/// Unreadable siblings are skipped; a missing/unreadable directory falls
/// back to the file's own footer count (0 if that fails too — callers
/// treat 0 as "unknown", which never flips the classification).
fn dir_max_parquet_rows(path: &Path) -> usize {
    let file_rows = |p: &Path| -> usize {
        crate::emat_parquet_metadata::num_rows(p)
            .ok()
            .and_then(|n| usize::try_from(n).ok())
            .unwrap_or(0)
    };
    let Some(parent) = path.parent() else {
        return file_rows(path);
    };
    let Ok(canonical) = std::fs::canonicalize(parent) else {
        return file_rows(path);
    };
    if let Some(&cached) = dir_memo().lock().unwrap().get(&canonical) {
        return cached;
    }
    let mut max = 0usize;
    let Ok(entries) = std::fs::read_dir(&canonical) else {
        return file_rows(path);
    };
    for entry in entries.flatten() {
        let p = entry.path();
        if p.extension().and_then(|e| e.to_str()) == Some("parquet") {
            max = max.max(file_rows(&p));
        }
    }
    dir_memo().lock().unwrap().insert(canonical, max);
    max
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pure classification semantics (no env, no globals): the threshold
    /// splits SF=10 from SF=100 fact tables with margin.
    #[test]
    fn threshold_splits_sf10_from_sf100() {
        // `let` (not `const`) bindings — clippy::assertions_on_constants.
        let sf10_lineitem: usize = 59_986_052;
        let sf100_lineitem: usize = 600_037_902;
        let sf100_orders: usize = 150_000_000;
        assert!(sf10_lineitem < LARGE_SCALE_MIN_FACT_ROWS);
        assert!(
            sf100_orders < LARGE_SCALE_MIN_FACT_ROWS,
            "dims/mid facts stay under"
        );
        assert!(sf100_lineitem >= LARGE_SCALE_MIN_FACT_ROWS);
        // 2× margin on both sides — row-count drift across TPC-H
        // generators can't flip the class.
        assert!(sf10_lineitem * 2 < LARGE_SCALE_MIN_FACT_ROWS);
        assert!(LARGE_SCALE_MIN_FACT_ROWS * 2 <= sf100_lineitem);
    }

    /// Sibling scan is registration-order independent: observing the
    /// SMALLEST table of the mini TPC-H fixture must classify by the
    /// largest sibling (lineitem), not by the observed file itself.
    #[test]
    fn observe_file_scans_siblings_not_just_self() {
        let dir = std::path::PathBuf::from(crate::test_support::tpch_mini_dir());
        let region = dir.join("region.parquet");
        let lineitem = dir.join("lineitem.parquet");
        let via_region = dir_max_parquet_rows(&region);
        let via_lineitem = dir_max_parquet_rows(&lineitem);
        assert_eq!(
            via_region, via_lineitem,
            "classification must not depend on which table registers first"
        );
        let lineitem_rows = crate::emat_parquet_metadata::num_rows(&lineitem).unwrap() as usize;
        assert_eq!(via_region, lineitem_rows, "max sibling = lineitem");
        assert!(via_region > 0);
        // The mini fixture is nowhere near large-scale under the shipped
        // threshold — recording it must not flip the process class.
        assert!(via_region < LARGE_SCALE_MIN_FACT_ROWS);
    }

    /// The high-water mark folds observations monotonically; the shipped
    /// threshold keeps every test fixture in this repo small-scale.
    #[test]
    fn high_water_mark_grows_and_stays_small_for_fixtures() {
        let dir = std::path::PathBuf::from(crate::test_support::tpch_mini_dir());
        let before = MAX_TABLE_ROWS_SEEN.load(Ordering::Relaxed);
        observe_file(&dir.join("nation.parquet").to_string_lossy());
        let after = MAX_TABLE_ROWS_SEEN.load(Ordering::Relaxed);
        assert!(after >= before, "monotone");
        assert!(after > 0, "mini fixture recorded");
    }
}
