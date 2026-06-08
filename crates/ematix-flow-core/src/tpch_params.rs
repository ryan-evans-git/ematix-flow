//! TPC-H query-generation parameters that depend on the scale factor.
//!
//! The canonical query files in `examples/tpch/queries/` are the **SF=1**
//! variants (what `dbgen`/`qgen` emit at SF=1). One query — **Q11** — has a
//! HAVING threshold whose spec parameter is `FRACTION = 0.0001 / SF`. With the
//! hardcoded SF=1 value (`0.0001`), the threshold exceeds the largest group at
//! SF≥10, so **every** engine returns 0 rows — a degenerate Q11 that silently
//! "passes" cross-engine row-count checks (all engines agree on 0, masking the
//! issue). Scaling the fraction by SF restores a real Q11.
//!
//! This is a benchmark-fidelity fix (affects ematix AND DuckDB AND Spark
//! equally — they all consume the same query file), not an engine change. It
//! lives here so every harness (`tpch_validate`, `tpch_triangulation_bench`,
//! the distributed campaign) applies the same SF-aware parameters. See memory
//! `[[dist1-local-bench-findings]]` (#315 / DIST.1a).

use std::path::Path;

/// Derive the integer scale factor from a TPC-H data directory path by reading
/// a trailing `sf<N>` component (e.g. `.../examples/tpch/data/sf10` → 10).
///
/// Returns `1` when no `sf<N>` component is present (mini fixtures / unknown
/// layouts), so callers get a safe no-op default.
pub fn scale_factor_from_data_dir(path: &Path) -> u32 {
    path.components()
        .rev()
        .find_map(|c| {
            let s = c.as_os_str().to_str()?;
            s.strip_prefix("sf").and_then(|n| n.parse::<u32>().ok())
        })
        .unwrap_or(1)
}

/// Apply scale-factor-dependent TPC-H query parameters to `sql`.
///
/// Currently this scales **Q11**'s HAVING fraction (`FRACTION = 0.0001 / SF`).
/// It is a no-op for any other query, and for `scale_factor <= 1` (the file
/// already carries the SF=1 value). Q11 contains exactly one `0.0001` literal,
/// so the rewrite is unambiguous; only the first occurrence is replaced.
pub fn apply_tpch_query_params(query_number: u8, sql: &str, scale_factor: u32) -> String {
    if query_number == 11 && scale_factor > 1 {
        sql.replacen("0.0001", &format!("(0.0001 / {scale_factor})"), 1)
    } else {
        sql.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scale_factor_parsed_from_dir() {
        assert_eq!(
            scale_factor_from_data_dir(Path::new("/x/examples/tpch/data/sf10")),
            10
        );
        assert_eq!(scale_factor_from_data_dir(Path::new("/x/data/sf100")), 100);
        assert_eq!(scale_factor_from_data_dir(Path::new("/x/data/sf1")), 1);
        // No `sf<N>` component → safe default 1 (mini fixtures, ad-hoc dirs).
        assert_eq!(
            scale_factor_from_data_dir(Path::new("/x/data/tpch_mini")),
            1
        );
        assert_eq!(scale_factor_from_data_dir(Path::new("relative/sf10")), 10);
    }

    #[test]
    fn q11_fraction_scaled_by_sf() {
        let q11 = "group by ps_partkey having sum(ps_supplycost * ps_availqty) > (\
                   select sum(ps_supplycost * ps_availqty) * 0.0001 from partsupp)";
        // SF=10 → 0.0001 / 10
        let s10 = apply_tpch_query_params(11, q11, 10);
        assert!(
            s10.contains("(0.0001 / 10)"),
            "SF=10 scaling missing:\n{s10}"
        );
        assert!(
            !s10.contains("* 0.0001 from"),
            "raw fraction still present:\n{s10}"
        );
        // SF=100 → 0.0001 / 100
        assert!(apply_tpch_query_params(11, q11, 100).contains("(0.0001 / 100)"));
        // SF=1 → unchanged (file already holds the SF=1 value).
        assert_eq!(apply_tpch_query_params(11, q11, 1), q11);
    }

    #[test]
    fn non_q11_unchanged_even_with_literal() {
        // A hypothetical query that happens to contain 0.0001 must NOT be
        // rewritten — only Q11 carries the spec fraction.
        let q = "select x where rate = 0.0001";
        assert_eq!(apply_tpch_query_params(6, q, 10), q);
        assert_eq!(apply_tpch_query_params(1, q, 100), q);
    }
}
