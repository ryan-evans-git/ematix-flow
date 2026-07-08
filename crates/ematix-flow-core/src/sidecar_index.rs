//! Read-side sidecar index lookups — Phase 1 of `feat/sidecar-indexes`.
//!
//! A sidecar (`<source>.parquet.idx`, produced by ematix-parquet's
//! [`IndexBuilder`](ematix_parquet_codec::index::IndexBuilder)) lets a selective
//! equality/range predicate skip-decompress whole row groups and masked-decode
//! only the matching rows — Postgres-style indexing over a Parquet file that was
//! never sorted on that column, without rewriting the file.
//!
//! This module is the flow-side entry point for the *read* path. It follows the
//! opt-in principle from `ematix-parquet/docs/ematix-flow-integration.md`: a
//! sidecar only ever adds capability. Every function here returns `Ok(None)`
//! (never an error) when there is no current sidecar covering the query, so the
//! caller keeps its existing full-scan path unchanged. A stale sidecar (the
//! source Parquet was rewritten) is treated as "no sidecar" and reported via
//! `Ok(None)` as well — it is recoverable, not fatal.
//!
//! Scope of this first cut: sorted-`INT64` equality lookup returning one target
//! column. Range, other physical types, multi-column projection, and the
//! `EmatixFastParquetTableProvider::scan` wiring build on this primitive in
//! subsequent commits (see `docs/SIDECAR_INDEXES_PLAN.md`).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use datafusion::common::ScalarValue;
use datafusion::common::stats::Precision;
use datafusion::error::{DataFusionError, Result as DfResult};

use crate::flags;

/// Conventional sidecar location for a source Parquet file:
/// `data.parquet` → `data.parquet.idx`.
///
/// `Path::with_extension` replaces the final component's extension, so for a
/// stem of `data` and current extension `parquet` this yields
/// `data.parquet.idx` — the path convention the builder writes to by default.
pub fn sidecar_path(source: &Path) -> PathBuf {
    source.with_extension("parquet.idx")
}

/// A sidecar open failed because the source was rewritten after the sidecar was
/// built. Recoverable — the caller should fall back to a full scan and, ideally,
/// schedule an offline rebuild. The codec surfaces this as
/// `InvalidInput("sidecar fingerprint mismatch: source file changed ...")`; we
/// match on the stable `"fingerprint mismatch"` substring of the error's debug
/// form so we do not depend on the codec's error enum being publicly
/// destructurable.
fn is_stale(err: &impl std::fmt::Debug) -> bool {
    format!("{err:?}").contains("fingerprint mismatch")
}

/// Answer `WHERE <indexed_col> = key` from a sorted-`INT64` sidecar index,
/// returning the matching values of `target_column` (a source-Parquet column
/// ordinal).
///
/// Returns:
/// - `Ok(Some(values))` when a current sidecar carries an index named
///   `index_name` — the masked-decoded `target_column` values for matching rows;
/// - `Ok(None)` when there is no sidecar, the sidecar is stale, or it has no
///   index by that name — the caller full-scans;
/// - `Err(_)` only on an unexpected I/O or codec failure.
pub fn indexed_i64_eq(
    source_path: &Path,
    index_name: &str,
    key: i64,
    target_column: usize,
) -> DfResult<Option<Vec<i64>>> {
    let idx_path = sidecar_path(source_path);
    if !idx_path.exists() {
        return Ok(None);
    }
    let source = ematix_parquet_io::ParquetFile::open(source_path).map_err(|e| {
        DataFusionError::Execution(format!("sidecar: open source {source_path:?}: {e:?}"))
    })?;
    let idx = match ematix_parquet_codec::index::ParquetIndex::open(&idx_path, &source) {
        Ok(idx) => idx,
        Err(e) if is_stale(&e) => {
            // Σ.SC P3: staleness is recoverable but worth watching — a
            // rising counter means sidecars are rotting behind rewrites.
            SIDECAR_STALE.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        }
        Err(e) => {
            return Err(DataFusionError::Execution(format!(
                "sidecar: open index {idx_path:?}: {e:?}"
            )));
        }
    };
    // The sidecar exists but does not carry the index we need — not covered.
    let covered = idx
        .manifest()
        .indexes
        .iter()
        .any(|entry| entry.name == index_name);
    if !covered {
        return Ok(None);
    }
    let values = idx
        .read_column_i64_where_eq(&source, index_name, key, target_column)
        .map_err(|e| {
            DataFusionError::Execution(format!("sidecar: eq lookup {index_name}: {e:?}"))
        })?;
    Ok(Some(values))
}

// ============================================================
// Σ.SC P3 — planner selectivity gate
// ============================================================

/// Process-wide sidecar decision counters — the `sidecar_{hit,miss,stale}`
/// metric planned in Phase 1, plus the P3 gate's skip counter. Monotonic
/// per process; probes (and tests) read deltas via [`sidecar_metrics`].
static SIDECAR_HIT: AtomicU64 = AtomicU64::new(0);
static SIDECAR_MISS: AtomicU64 = AtomicU64::new(0);
static SIDECAR_STALE: AtomicU64 = AtomicU64::new(0);
static SIDECAR_SKIPPED_SELECTIVITY: AtomicU64 = AtomicU64::new(0);

/// Snapshot of the sidecar decision counters. `hit` = index path taken and
/// answered; `miss` = no/uncovered sidecar (full scan); `stale` = fingerprint
/// mismatch (full scan + rebuild candidate); `skipped_selectivity` = a
/// covering sidecar existed but the P3 gate estimated the predicate too
/// unselective to win (full scan by choice).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SidecarMetrics {
    pub hit: u64,
    pub miss: u64,
    pub stale: u64,
    pub skipped_selectivity: u64,
}

/// Read the current counter values (relaxed — probes want cheap, not fenced).
pub fn sidecar_metrics() -> SidecarMetrics {
    SidecarMetrics {
        hit: SIDECAR_HIT.load(Ordering::Relaxed),
        miss: SIDECAR_MISS.load(Ordering::Relaxed),
        stale: SIDECAR_STALE.load(Ordering::Relaxed),
        skipped_selectivity: SIDECAR_SKIPPED_SELECTIVITY.load(Ordering::Relaxed),
    }
}

/// The gate's threshold: maximum estimated selectivity (matching fraction of
/// the file) for which the index path is still expected to beat the plain
/// scan. `EMAT_SIDECAR_MAX_SELECTIVITY`, default `0.05` — deliberately far
/// below the codec's measured ~60% crossover (`bench_indexed_lookup`):
/// the uniform-distribution estimate below is crude, and the cost of a wrong
/// "use index" call (per-row masked decode of most of the file) is much
/// worse than a wrong "use scan" call (a vectorized scan we know is
/// predictably fast). See `docs/EMAT_FLAGS.md`.
pub fn sidecar_max_selectivity() -> f64 {
    flags::f64_or("EMAT_SIDECAR_MAX_SELECTIVITY", 0.05)
}

/// Uniform-model selectivity estimate for `col = key` given the column's
/// footer `[min, max]`: `0.0` when `key` is provably outside the bounds,
/// else `1 / (max - min + 1)` — every value in the width is assumed equally
/// likely. The same estimate the Iceberg manifest prune would make from the
/// stamped `IndexSummary` bounds (`iceberg_stamp` derives them from the same
/// footer). i128 arithmetic so full-domain i64 bounds cannot overflow.
pub fn estimate_eq_selectivity(min: i64, max: i64, key: i64) -> f64 {
    if key < min || key > max {
        return 0.0;
    }
    let width = (max as i128 - min as i128) + 1;
    1.0 / width as f64
}

/// Σ.SC P3 front door for `WHERE <indexed_col> = key`: the selectivity-gated
/// index-vs-scan decision over the Phase 1 primitive, threshold from
/// [`sidecar_max_selectivity`].
///
/// Returns `Ok(None)` — caller full-scans — when the Phase 1 primitive would
/// (no/stale/uncovered sidecar) **or** when the gate estimates the predicate
/// matches more than the threshold fraction of rows. In the latter case the
/// index is deliberately left unused: masked per-row decode of a large match
/// set loses to the vectorized scan (codec crossover ~60%; see
/// [`sidecar_max_selectivity`] for why the default sits far below that).
pub fn sidecar_i64_eq(
    source_path: &Path,
    index_name: &str,
    key: i64,
    target_column: usize,
) -> DfResult<Option<Vec<i64>>> {
    sidecar_i64_eq_opt(
        source_path,
        index_name,
        key,
        target_column,
        sidecar_max_selectivity(),
    )
}

/// [`sidecar_i64_eq`] with an explicit threshold — the deterministic entry
/// point for tests and for callers that already resolved their own tunable
/// (process env is global; parameter-passing keeps concurrent scans honest).
pub fn sidecar_i64_eq_opt(
    source_path: &Path,
    index_name: &str,
    key: i64,
    target_column: usize,
    max_selectivity: f64,
) -> DfResult<Option<Vec<i64>>> {
    // Cheap pre-check so nothing below runs when there is no sidecar at all.
    let idx_path = sidecar_path(source_path);
    if !idx_path.exists() {
        SIDECAR_MISS.fetch_add(1, Ordering::Relaxed);
        return Ok(None);
    }

    // Open source + sidecar ONCE and share them across the gate and the
    // lookup — the P5 bench measured the sidecar open at ~140 ms on a
    // 10M-row fixture, so a gate that re-opened for its own bounds check
    // doubled the constant cost of every lookup.
    let source = ematix_parquet_io::ParquetFile::open(source_path).map_err(|e| {
        DataFusionError::Execution(format!("sidecar: open source {source_path:?}: {e:?}"))
    })?;
    let idx = match ematix_parquet_codec::index::ParquetIndex::open(&idx_path, &source) {
        Ok(idx) => idx,
        Err(e) if is_stale(&e) => {
            SIDECAR_STALE.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        }
        Err(e) => {
            return Err(DataFusionError::Execution(format!(
                "sidecar: open index {idx_path:?}: {e:?}"
            )));
        }
    };
    let Some(entry) = idx
        .manifest()
        .indexes
        .iter()
        .find(|entry| entry.name == index_name)
    else {
        // Sidecar exists but does not carry this index — not covered.
        SIDECAR_MISS.fetch_add(1, Ordering::Relaxed);
        return Ok(None);
    };

    // Gate on the indexed column's footer stats. The indexed column is named
    // by the sidecar manifest; resolving its stats costs one footer read (no
    // data pages). Missing/untyped bounds → no estimate → default to the
    // index path (pre-P3 behavior: someone built this index on purpose).
    if let ematix_parquet_codec::index::IndexKind::Sorted { source_column, .. } = &entry.kind {
        if let Some((min, max)) = footer_i64_bounds(source_path, source_column)? {
            let est = estimate_eq_selectivity(min, max, key);
            if est > max_selectivity {
                SIDECAR_SKIPPED_SELECTIVITY.fetch_add(1, Ordering::Relaxed);
                return Ok(None);
            }
        }
    }

    let values = idx
        .read_column_i64_where_eq(&source, index_name, key, target_column)
        .map_err(|e| {
            DataFusionError::Execution(format!("sidecar: eq lookup {index_name}: {e:?}"))
        })?;
    SIDECAR_HIT.fetch_add(1, Ordering::Relaxed);
    Ok(Some(values))
}

/// Footer `[min, max]` of `column` (by name), or `None` when it cannot be
/// determined (unknown column, non-i64 stats, footer without bounds) — every
/// `None` case is "no estimate", never an error, so the gate safely defaults
/// to the lookup path which has its own fallbacks.
fn footer_i64_bounds(source_path: &Path, column: &str) -> DfResult<Option<(i64, i64)>> {
    let md = crate::emat_parquet_metadata::load_provider_metadata(source_path).map_err(|e| {
        DataFusionError::Execution(format!("sidecar: read footer {source_path:?}: {e:?}"))
    })?;
    let Ok(ordinal) = md.schema.index_of(column) else {
        return Ok(None);
    };
    let stats = &md.column_stats[ordinal];
    match (&stats.min_value, &stats.max_value) {
        (
            Precision::Exact(ScalarValue::Int64(Some(min))),
            Precision::Exact(ScalarValue::Int64(Some(max))),
        ) => Ok(Some((*min, *max))),
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ematix_parquet_codec::index::IndexBuilder;
    use ematix_parquet_codec::write::{ColumnData, write_table_to_path};
    use ematix_parquet_format::types::CompressionCodec;

    /// Write a two-column `(key: i64, val: i64)` Parquet file and return its
    /// path inside `dir`.
    fn write_kv_fixture(dir: &Path, keys: &[i64], vals: &[i64]) -> PathBuf {
        let path = dir.join("data.parquet");
        write_table_to_path(
            &path,
            &[
                ("key", ColumnData::I64(keys)),
                ("val", ColumnData::I64(vals)),
            ],
            CompressionCodec::Uncompressed,
        )
        .expect("write fixture parquet");
        path
    }

    /// Full-scan oracle: values of `val` where `key == k`, order-independent.
    fn full_scan_eq(keys: &[i64], vals: &[i64], k: i64) -> Vec<i64> {
        let mut out: Vec<i64> = keys
            .iter()
            .zip(vals)
            .filter(|(key, _)| **key == k)
            .map(|(_, v)| *v)
            .collect();
        out.sort_unstable();
        out
    }

    #[test]
    fn eq_lookup_matches_full_scan() {
        let dir = tempfile::tempdir().unwrap();
        // 100 keys, each appearing a few times, values distinct.
        let keys: Vec<i64> = (0..600).map(|i| i % 100).collect();
        let vals: Vec<i64> = (0..600).collect();
        let src = write_kv_fixture(dir.path(), &keys, &vals);

        let source = ematix_parquet_io::ParquetFile::open(&src).unwrap();
        IndexBuilder::new(&source)
            .write_sorted_i64(sidecar_path(&src), "idx_key", 0)
            .expect("build sidecar");

        for k in [0_i64, 42, 99] {
            let mut got = indexed_i64_eq(&src, "idx_key", k, 1)
                .unwrap()
                .expect("sidecar present and covers idx_key");
            got.sort_unstable();
            assert_eq!(got, full_scan_eq(&keys, &vals, k), "key={k}");
        }
    }

    #[test]
    fn no_sidecar_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let src = write_kv_fixture(dir.path(), &[1, 2, 3], &[10, 20, 30]);
        // No sidecar built.
        assert!(indexed_i64_eq(&src, "idx_key", 2, 1).unwrap().is_none());
    }

    #[test]
    fn unknown_index_name_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let src = write_kv_fixture(dir.path(), &[1, 2, 3], &[10, 20, 30]);
        let source = ematix_parquet_io::ParquetFile::open(&src).unwrap();
        IndexBuilder::new(&source)
            .write_sorted_i64(sidecar_path(&src), "idx_key", 0)
            .unwrap();
        // Sidecar exists but has no "idx_other" — fall back, not an error.
        assert!(indexed_i64_eq(&src, "idx_other", 2, 1).unwrap().is_none());
    }

    #[test]
    fn stale_sidecar_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let src = write_kv_fixture(dir.path(), &[1, 2, 3], &[10, 20, 30]);
        let source = ematix_parquet_io::ParquetFile::open(&src).unwrap();
        IndexBuilder::new(&source)
            .write_sorted_i64(sidecar_path(&src), "idx_key", 0)
            .unwrap();
        // Rewrite the source with different data — the sidecar's footer
        // fingerprint no longer matches, so open must report staleness and we
        // recover to a full scan (None), never an error.
        write_kv_fixture(dir.path(), &[7, 8, 9, 10], &[70, 80, 90, 100]);
        assert!(indexed_i64_eq(&src, "idx_key", 2, 1).unwrap().is_none());
    }

    // ---- Σ.SC P3: selectivity gate ----

    /// Build a fixture + sidecar with the given id distribution and return
    /// its path. Uses distinct `val` per row so results are checkable.
    fn gated_fixture(dir: &Path, ids: &[i64]) -> PathBuf {
        let vals: Vec<i64> = (0..ids.len() as i64).collect();
        let src = write_kv_fixture(dir, ids, &vals);
        let source = ematix_parquet_io::ParquetFile::open(&src).unwrap();
        IndexBuilder::new(&source)
            .write_sorted_i64(sidecar_path(&src), "idx_key", 0)
            .unwrap();
        src
    }

    /// A highly selective eq (600 distinct ids, est. selectivity ≈ 1/600)
    /// stays on the INDEX path under the default 0.05 threshold — and the hit
    /// counter moves. Result must still match the full-scan oracle.
    #[test]
    fn gate_high_selectivity_uses_index() {
        let dir = tempfile::tempdir().unwrap();
        let ids: Vec<i64> = (0..600).collect();
        let src = gated_fixture(dir.path(), &ids);

        let hits_before = sidecar_metrics().hit;
        let got = sidecar_i64_eq_opt(&src, "idx_key", 42, 1, 0.05)
            .unwrap()
            .expect("selective eq must take the index path");
        assert_eq!(
            got,
            vec![42],
            "val == row ordinal for the unique-id fixture"
        );
        assert!(
            sidecar_metrics().hit > hits_before,
            "index path must count a sidecar hit"
        );
    }

    /// A low-selectivity eq (two distinct ids → est. selectivity ≈ 0.5) must
    /// FALL BACK to the plain scan (`None`) under the default threshold: the
    /// masked per-row decode loses to the vectorized scan when a large
    /// fraction of rows match. The skip counter is the probe.
    #[test]
    fn gate_low_selectivity_falls_back_to_scan() {
        let dir = tempfile::tempdir().unwrap();
        let ids: Vec<i64> = (0..600).map(|i| i % 2).collect();
        let src = gated_fixture(dir.path(), &ids);

        let skips_before = sidecar_metrics().skipped_selectivity;
        assert!(
            sidecar_i64_eq_opt(&src, "idx_key", 1, 1, 0.05)
                .unwrap()
                .is_none(),
            "half-the-file eq must fall back to the scan path"
        );
        assert!(
            sidecar_metrics().skipped_selectivity > skips_before,
            "gate fallback must count a selectivity skip"
        );
    }

    /// Raising the threshold flips the same low-selectivity predicate back
    /// onto the index path — the override is respected.
    #[test]
    fn gate_threshold_override_respected() {
        let dir = tempfile::tempdir().unwrap();
        let ids: Vec<i64> = (0..600).map(|i| i % 2).collect();
        let src = gated_fixture(dir.path(), &ids);

        let mut got = sidecar_i64_eq_opt(&src, "idx_key", 1, 1, 0.9)
            .unwrap()
            .expect("0.9 threshold admits a 0.5-selectivity eq");
        got.sort_unstable();
        let want: Vec<i64> = (0..600).filter(|i| i % 2 == 1).collect();
        assert_eq!(got, want);
    }

    /// The env-tunable front door reads `EMAT_SIDECAR_MAX_SELECTIVITY`
    /// (default 0.05). Plumbing-only test — the behavioral tests above go
    /// through the parameterized entry point so concurrent tests never race
    /// on process env (the ematix_fast_parquet_multi de-flake lesson).
    #[test]
    fn gate_threshold_flag_plumbing() {
        assert!(
            (sidecar_max_selectivity() - 0.05).abs() < 1e-12,
            "default threshold is 0.05"
        );
        unsafe { std::env::set_var("EMAT_SIDECAR_MAX_SELECTIVITY", "0.75") };
        let read = sidecar_max_selectivity();
        unsafe { std::env::remove_var("EMAT_SIDECAR_MAX_SELECTIVITY") };
        assert!((read - 0.75).abs() < 1e-12, "override read back: {read}");
    }

    /// The uniform-model estimator itself: outside-bounds keys are free
    /// (0.0), a degenerate single-value column is total (1.0), and the
    /// in-bounds estimate is 1/width.
    #[test]
    fn eq_selectivity_estimates() {
        assert_eq!(estimate_eq_selectivity(0, 599, 1_000), 0.0);
        assert_eq!(estimate_eq_selectivity(0, 599, -5), 0.0);
        assert_eq!(estimate_eq_selectivity(7, 7, 7), 1.0);
        let s = estimate_eq_selectivity(0, 599, 42);
        assert!((s - 1.0 / 600.0).abs() < 1e-12, "1/width: {s}");
        // i64 extremes must not overflow the width computation.
        let wide = estimate_eq_selectivity(i64::MIN, i64::MAX, 0);
        assert!(wide > 0.0 && wide < 1e-15, "full-domain width: {wide}");
    }
}
