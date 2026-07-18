//! Native dimension-table reductions — building Q08's probe inputs without
//! DataFusion.
//!
//! Q08's 60M-row hot path (the lineitem probe) is already engine-native;
//! the last DataFusion dependency is the small-table reductions that
//! produce the probe key/payload lists (`build_probes` in the harness).
//! These decode via the P0 stock-parquet low-level reader — the dimension
//! tables are tiny, so the fast ematix-parquet path (for the numeric hot
//! scans) isn't needed, and it keeps string decode off the sibling codec
//! for now.
//!
//! First reduction: the part semijoin key set — a scan plus one
//! string-equality filter, no joins. The supplier (a join + string flag)
//! and orders (a 4-way join + date window + year bucket) reductions build
//! on this string capability plus the engine's join operators, and are
//! follow-ons.

use std::path::Path;

use crate::scan::{ColKind, scan_columns};

/// `SELECT key_col FROM <path> WHERE str_col = needle`: collect the i64
/// `key_col` of every row whose Utf8 `str_col` equals `needle`. The
/// membership seed for a semijoin probe — e.g. Q08's part reduction,
/// `p_partkey WHERE p_type = 'ECONOMY ANODIZED STEEL'`.
pub fn collect_i64_keys_where_str_eq(
    path: &Path,
    key_col: &str,
    str_col: &str,
    needle: &str,
) -> Result<Vec<i64>, String> {
    let chunks = scan_columns(path, &[(key_col, ColKind::I64), (str_col, ColKind::Utf8)])?;
    let mut keys = Vec::new();
    for chunk in &chunks {
        let k = chunk.col(0).as_i64();
        let s = chunk.col(1).as_utf8();
        chunk.sel.for_each(|i| {
            let i = i as usize;
            if s.get(i) == needle {
                keys.push(k[i]);
            }
        });
    }
    Ok(keys)
}
