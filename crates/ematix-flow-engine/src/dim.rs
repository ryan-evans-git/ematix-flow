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

use crate::adaptive::AdaptiveHashJoin;
use crate::chunk::DataChunk;
use crate::scan::{ColKind, scan_columns};
use crate::vector::Vector;

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

/// Q08's supplier reduction: `(s_suppkey, flag)` for every supplier, where
/// `flag = 1` iff the supplier's nation name equals `nation_needle`
/// (`"BRAZIL"`). This is `supplier ⋈ nation` on nationkey with the nation's
/// name-match flag carried as the payload — built on the engine's **own**
/// [`AdaptiveHashJoin`], which keeps the tiny (25-row) nation build in
/// memory. Returns `(suppkeys, flags)` for a payload-carrying `choose`.
pub fn supplier_nation_flag(
    supplier_path: &Path,
    nation_path: &Path,
    nation_needle: &str,
) -> Result<(Vec<i64>, Vec<i64>), String> {
    // Build side = nation, projected to (n_nationkey, name == needle ? 1 : 0).
    let nation_chunks = scan_columns(
        nation_path,
        &[("n_nationkey", ColKind::I64), ("n_name", ColKind::Utf8)],
    )?;
    // Probe side = supplier: key = s_nationkey, payload = s_suppkey.
    let supplier_chunks = scan_columns(
        supplier_path,
        &[("s_nationkey", ColKind::I64), ("s_suppkey", ColKind::I64)],
    )?;

    // Tiny build (25 nations) ⇒ the adaptive join runs fully in memory.
    let mut join = AdaptiveHashJoin::new(16 * 1024 * 1024, 4, 0);
    for chunk in &nation_chunks {
        let nk = chunk.col(0).as_i64();
        let nm = chunk.col(1).as_utf8();
        let mut keys = Vec::with_capacity(chunk.sel.len());
        let mut flags = Vec::with_capacity(chunk.sel.len());
        chunk.sel.for_each(|i| {
            let i = i as usize;
            keys.push(nk[i]);
            flags.push(i64::from(nm.get(i) == nation_needle));
        });
        let build = DataChunk::new(vec![Vector::i64(keys), Vector::i64(flags)]);
        join.consume_build(&build, 0, 1)
            .map_err(|e| e.to_string())?;
    }
    for chunk in &supplier_chunks {
        join.consume_probe(chunk, 0, 1).map_err(|e| e.to_string())?;
    }

    // The join emits (nationkey, build_pay = flag, probe_pay = s_suppkey).
    let mut suppkeys = Vec::new();
    let mut flags = Vec::new();
    join.run(|_nationkey, flag, suppkey| {
        suppkeys.push(suppkey);
        flags.push(flag);
    })
    .map_err(|e| e.to_string())?;
    Ok((suppkeys, flags))
}
