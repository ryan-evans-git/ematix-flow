//! Native dimension-table reductions — building Q08's probe inputs without
//! DataFusion. With these, Q08 is **fully engine-native**: dims → scan →
//! join → aggregate, no DataFusion anywhere in the path.
//!
//! The dimension tables are tiny, so these decode via the P0 stock-parquet
//! low-level reader (the fast ematix-parquet path is for the numeric hot
//! scans, and stock parquet handles BYTE_ARRAY strings the sibling codec
//! doesn't yet). Three reductions, each composing the engine's own pieces:
//!
//! - **part** ([`collect_i64_keys_where_str_eq`]) — a scan + one
//!   string-equality filter (`p_partkey WHERE p_type = …`).
//! - **supplier** ([`supplier_nation_flag`]) — `supplier ⋈ nation` on the
//!   engine's [`AdaptiveHashJoin`], carrying the `n_name` flag.
//! - **orders** ([`collect_i64_where_i64_member`] ×2 +
//!   [`orders_semijoin_datebucket`]) — a region→nation→customer `∈`-membership
//!   semijoin chain, then a date-windowed filter with a year bucket.

use std::path::Path;

use crate::adaptive::AdaptiveHashJoin;
use crate::chunk::DataChunk;
use crate::join::choose;
use crate::scan::{ColKind, scan_columns};
use crate::vector::{LogicalType, Vector};

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

/// Semijoin reduction: `SELECT collect_col FROM <path> WHERE filter_col IN
/// members` — collect the i64 `collect_col` of every row whose i64
/// `filter_col` is a member of `members`. Membership uses the engine's
/// adaptive probe structure (dense byte-set vs hash). The chain link in a
/// dimension reduction — e.g. Q08's `n_nationkey WHERE n_regionkey ∈
/// {AMERICA}` and `c_custkey WHERE c_nationkey ∈ {AMERICA nations}`.
pub fn collect_i64_where_i64_member(
    path: &Path,
    collect_col: &str,
    filter_col: &str,
    members: &[i64],
) -> Result<Vec<i64>, String> {
    let probe = choose(members, None);
    let chunks = scan_columns(
        path,
        &[(collect_col, ColKind::I64), (filter_col, ColKind::I64)],
    )?;
    let mut out = Vec::new();
    for chunk in &chunks {
        let c = chunk.col(0).as_i64();
        let f = chunk.col(1).as_i64();
        chunk.sel.for_each(|i| {
            let i = i as usize;
            if probe.contains(f[i]) {
                out.push(c[i]);
            }
        });
    }
    Ok(out)
}

/// Q08's orders reduction: `(o_orderkey, year_bucket)` for every order whose
/// `cust_col` is in `member_custkeys` (the AMERICA-region customers) and
/// whose Date32 `date_col` is in `[date_lo, date_hi]` (days since epoch).
/// `year_bucket` is 0 below `split_day` and 1 at/above it — Q08's 1995 vs
/// 1996 split. Composes a custkey semijoin with a date-window filter;
/// returns `(orderkeys, buckets)` for a payload-carrying `choose`.
#[allow(clippy::too_many_arguments)]
pub fn orders_semijoin_datebucket(
    orders_path: &Path,
    member_custkeys: &[i64],
    key_col: &str,
    cust_col: &str,
    date_col: &str,
    date_lo: i32,
    date_hi: i32,
    split_day: i32,
) -> Result<(Vec<i64>, Vec<i64>), String> {
    let probe = choose(member_custkeys, None);
    let chunks = scan_columns(
        orders_path,
        &[
            (key_col, ColKind::I64),
            (cust_col, ColKind::I64),
            (date_col, ColKind::I32(LogicalType::Date32)),
        ],
    )?;
    let mut keys = Vec::new();
    let mut buckets = Vec::new();
    for chunk in &chunks {
        let ok = chunk.col(0).as_i64();
        let ck = chunk.col(1).as_i64();
        let od = chunk.col(2).as_i32();
        chunk.sel.for_each(|i| {
            let i = i as usize;
            let d = od[i];
            // Date window first (cheap), then the custkey semijoin.
            if (date_lo..=date_hi).contains(&d) && probe.contains(ck[i]) {
                keys.push(ok[i]);
                buckets.push(i64::from(d >= split_day));
            }
        });
    }
    Ok((keys, buckets))
}
