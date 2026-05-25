//! Σ.S.B — plan-time FK-chain detection helper.
//!
//! Shared by the Σ.S.B cascading-L9 prototype and the general rule.
//! Two responsibilities:
//!
//! 1. [`fk_chain_stem`] — pure function that extracts the suffix
//!    after a column's first `_`. Two columns are in the same FK
//!    chain iff their stems match. Works for any schema using
//!    `<table_prefix>_<colname>` naming (TPC-H, TPC-DS, many in-house
//!    schemas) — not TPC-H-specific.
//!
//! 2. [`find_scans_by_fk_chain`] — walks a physical plan and returns
//!    every [`EmatixFastParquetExec`] whose projection includes a
//!    column whose stem matches a given target stem. Returns enough
//!    info (scan node + typed handle + file-schema col index +
//!    column name) for the caller to attach a sideband.
//!
//! ## Why a stem-match is safe for cascading
//!
//! The cascade rule attaches a bloom built on column `A.k` (values
//! of `A.k`) to a downstream scan column `B.k'` only when `A.k` and
//! `B.k'` are joined together transitively in the plan (i.e. each
//! `HashJoinExec` on the path equates two columns whose stems match
//! the target stem). The plan-walker here surfaces *candidates*; the
//! caller (the rule) is responsible for confirming a join path
//! connects the bloom source to each candidate scan before attaching.
//!
//! Treating stem-match alone as "same domain" would be wrong for
//! arbitrary schemas — two unrelated tables can both have `id`
//! columns. The walker is therefore intentionally narrow: it returns
//! candidates, not commitments.

use std::sync::Arc;

use datafusion::physical_plan::ExecutionPlan;

use crate::ematix_fast_parquet::EmatixFastParquetExec;

/// Extract the FK-chain stem from a column name.
///
/// Splits on the first `_` and returns the suffix. Columns without
/// an underscore return `None` and must be matched by exact name.
///
/// Examples:
/// ```text
/// fk_chain_stem("c_custkey")     == Some("custkey")
/// fk_chain_stem("o_custkey")     == Some("custkey")
/// fk_chain_stem("l_orderkey")    == Some("orderkey")
/// fk_chain_stem("ps_partkey")    == Some("partkey")   // two-letter prefix
/// fk_chain_stem("user_id_first") == Some("id_first")  // splits ONCE
/// fk_chain_stem("id")            == None
/// fk_chain_stem("")              == None
/// ```
pub fn fk_chain_stem(col_name: &str) -> Option<&str> {
    let (_prefix, stem) = col_name.split_once('_')?;
    if stem.is_empty() {
        return None;
    }
    Some(stem)
}

/// Returns true iff `a` and `b` share an FK-chain stem.
///
/// Falls back to exact-name equality for columns without underscores.
pub fn share_fk_chain(a: &str, b: &str) -> bool {
    match (fk_chain_stem(a), fk_chain_stem(b)) {
        (Some(sa), Some(sb)) => sa == sb,
        _ => a == b,
    }
}

/// A candidate scan for a cascading bloom attachment.
#[derive(Clone)]
pub struct FkChainScanMatch {
    /// The original Arc node — caller compares with `Arc::ptr_eq`
    /// when rewriting the plan, so the *same* Arc must be threaded
    /// through (mirrors the [[sigma_q_l9_landed]] Q21 fix).
    pub scan_node: Arc<dyn ExecutionPlan>,
    /// Typed handle to the same scan — pre-cloned with empty added
    /// predicates so the caller can directly call
    /// `with_runtime_sideband` without reaching back through `Any`.
    pub scan_typed: Arc<EmatixFastParquetExec>,
    /// File-schema column index for the matched column (not the
    /// projected-schema index — see Σ.Q.L14 col_idx bug).
    pub col_idx: usize,
    /// The actual column name that matched (for diagnostics / logs).
    pub col_name: String,
}

impl std::fmt::Debug for FkChainScanMatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FkChainScanMatch")
            .field("col_name", &self.col_name)
            .field("col_idx", &self.col_idx)
            .field("scan_path", &self.scan_typed.path())
            .finish()
    }
}

/// Walk `plan` and return every [`EmatixFastParquetExec`] whose
/// projected file-schema columns include at least one column whose
/// FK-chain stem matches `target_stem`.
///
/// Pre-order traversal. Each scan instance appears at most once in
/// the output (deduped by `Arc::as_ptr`). When a scan has multiple
/// columns sharing the target stem (unusual but possible), only the
/// FIRST match is returned — the caller is responsible for any
/// per-column iteration if needed.
///
/// Walks through every child of every plan node, so multi-child
/// plans (HashJoinExec, UnionExec, etc.) contribute scans from all
/// of their subtrees.
pub fn find_scans_by_fk_chain(
    plan: &Arc<dyn ExecutionPlan>,
    target_stem: &str,
) -> Vec<FkChainScanMatch> {
    let mut out: Vec<FkChainScanMatch> = Vec::new();
    let mut seen: Vec<*const ()> = Vec::new();
    walk(plan, target_stem, &mut out, &mut seen);
    out
}

fn walk(
    plan: &Arc<dyn ExecutionPlan>,
    target_stem: &str,
    out: &mut Vec<FkChainScanMatch>,
    seen: &mut Vec<*const ()>,
) {
    if let Some(scan) = plan.as_any().downcast_ref::<EmatixFastParquetExec>() {
        let ptr = Arc::as_ptr(plan) as *const ();
        if !seen.contains(&ptr) {
            seen.push(ptr);
            let file_sch = scan.file_schema();
            let projection = scan.projection();
            for (file_idx, field) in file_sch.fields().iter().enumerate() {
                if !projection.contains(&file_idx) {
                    continue;
                }
                let name = field.name();
                let matches = match fk_chain_stem(name) {
                    Some(stem) => stem == target_stem,
                    None => name == target_stem,
                };
                if matches {
                    if let Ok(fresh) = scan.with_added_predicates(Vec::new()) {
                        out.push(FkChainScanMatch {
                            scan_node: Arc::clone(plan),
                            scan_typed: fresh,
                            col_idx: file_idx,
                            col_name: name.to_string(),
                        });
                        break;
                    }
                }
            }
        }
        return;
    }
    for child in plan.children() {
        walk(child, target_stem, out, seen);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -------- fk_chain_stem --------

    #[test]
    fn stem_tpch_columns() {
        assert_eq!(fk_chain_stem("c_custkey"), Some("custkey"));
        assert_eq!(fk_chain_stem("o_custkey"), Some("custkey"));
        assert_eq!(fk_chain_stem("o_orderkey"), Some("orderkey"));
        assert_eq!(fk_chain_stem("l_orderkey"), Some("orderkey"));
        assert_eq!(fk_chain_stem("n_nationkey"), Some("nationkey"));
        assert_eq!(fk_chain_stem("s_nationkey"), Some("nationkey"));
        assert_eq!(fk_chain_stem("r_regionkey"), Some("regionkey"));
        assert_eq!(fk_chain_stem("ps_partkey"), Some("partkey"));
        assert_eq!(fk_chain_stem("ps_suppkey"), Some("suppkey"));
        assert_eq!(fk_chain_stem("p_partkey"), Some("partkey"));
    }

    #[test]
    fn stem_no_underscore_returns_none() {
        assert_eq!(fk_chain_stem("id"), None);
        assert_eq!(fk_chain_stem("name"), None);
        assert_eq!(fk_chain_stem(""), None);
    }

    #[test]
    fn stem_splits_only_on_first_underscore() {
        // user_id_first → prefix "user", stem "id_first". This is
        // intentional — two columns named e.g. "user_id_first" and
        // "admin_id_first" should share an FK chain.
        assert_eq!(fk_chain_stem("user_id_first"), Some("id_first"));
        assert_eq!(fk_chain_stem("admin_id_first"), Some("id_first"));
    }

    #[test]
    fn stem_empty_suffix_is_none() {
        // Defensive — `prefix_` has no real stem.
        assert_eq!(fk_chain_stem("c_"), None);
        assert_eq!(fk_chain_stem("_"), None);
    }

    #[test]
    fn stem_empty_prefix_returns_suffix() {
        // `_custkey` — empty prefix but non-empty stem. We accept
        // it; matches the spirit of "stem after the first underscore".
        assert_eq!(fk_chain_stem("_custkey"), Some("custkey"));
    }

    // -------- share_fk_chain --------

    #[test]
    fn share_fk_tpch_pairs() {
        assert!(share_fk_chain("c_custkey", "o_custkey"));
        assert!(share_fk_chain("o_orderkey", "l_orderkey"));
        assert!(share_fk_chain("n_nationkey", "c_nationkey"));
        assert!(share_fk_chain("n_nationkey", "s_nationkey"));
        assert!(share_fk_chain("ps_partkey", "l_partkey"));
        assert!(share_fk_chain("ps_partkey", "p_partkey"));
    }

    #[test]
    fn share_fk_unrelated_pairs() {
        assert!(!share_fk_chain("c_custkey", "l_orderkey"));
        assert!(!share_fk_chain("o_orderkey", "n_nationkey"));
        assert!(!share_fk_chain("p_partkey", "s_suppkey"));
    }

    #[test]
    fn share_fk_self_match() {
        assert!(share_fk_chain("o_orderkey", "o_orderkey"));
        assert!(share_fk_chain("id", "id"));
    }

    #[test]
    fn share_fk_no_underscore_falls_back_to_exact() {
        assert!(share_fk_chain("id", "id"));
        assert!(!share_fk_chain("id", "name"));
        // Mixed: one has underscore, one doesn't — never match.
        assert!(!share_fk_chain("id", "c_custkey"));
        assert!(!share_fk_chain("c_custkey", "id"));
    }

    // -------- find_scans_by_fk_chain --------
    //
    // The walker tests use real Emat parquet files because Emat
    // scans aren't trivially constructable in a unit test (they
    // need a file_schema, partitions, etc.). The runtime-bloom
    // tests use the same pattern.

    use crate::ematix_fast_parquet::EmatixFastParquetTableProvider;
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use ematix_parquet_codec::write::{ColumnData, write_table_to_path};
    use ematix_parquet_format::types::CompressionCodec;

    fn tmp_parquet(name: &str) -> std::path::PathBuf {
        let dir =
            std::env::temp_dir().join(format!("fk_chain_test_{}_{}", std::process::id(), name));
        let _ = std::fs::create_dir_all(&dir);
        dir.join(format!("{name}.parquet"))
    }

    fn write_orders(path: &std::path::Path) {
        let o_orderkey: Vec<i64> = (0..200i64).collect();
        let o_custkey: Vec<i64> = (0..200i64).map(|i| i % 50).collect();
        write_table_to_path(
            path,
            &[
                ("o_orderkey", ColumnData::I64(&o_orderkey)),
                ("o_custkey", ColumnData::I64(&o_custkey)),
            ],
            CompressionCodec::Uncompressed,
        )
        .unwrap();
    }

    fn write_lineitem(path: &std::path::Path) {
        let l_orderkey: Vec<i64> = (0..1000i64).map(|i| i % 200).collect();
        let l_suppkey: Vec<i64> = (0..1000i64).map(|i| i % 50).collect();
        write_table_to_path(
            path,
            &[
                ("l_orderkey", ColumnData::I64(&l_orderkey)),
                ("l_suppkey", ColumnData::I64(&l_suppkey)),
            ],
            CompressionCodec::Uncompressed,
        )
        .unwrap();
    }

    async fn build_join_plan(stem_for_join: &str) -> Arc<dyn ExecutionPlan> {
        let o = tmp_parquet(&format!("o_{stem_for_join}"));
        let li = tmp_parquet(&format!("li_{stem_for_join}"));
        write_orders(&o);
        write_lineitem(&li);

        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(SessionConfig::new().with_target_partitions(2))
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_table(
            "orders",
            Arc::new(EmatixFastParquetTableProvider::try_new(o.to_string_lossy()).unwrap()),
        )
        .unwrap();
        ctx.register_table(
            "lineitem",
            Arc::new(EmatixFastParquetTableProvider::try_new(li.to_string_lossy()).unwrap()),
        )
        .unwrap();
        let sql = "SELECT o_orderkey, l_orderkey, l_suppkey \
                   FROM orders JOIN lineitem ON o_orderkey = l_orderkey";
        ctx.sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn walker_finds_both_scans_for_orderkey_stem() {
        let plan = build_join_plan("walker_both").await;
        let matches = find_scans_by_fk_chain(&plan, "orderkey");
        let names: Vec<_> = matches.iter().map(|m| m.col_name.clone()).collect();
        assert!(
            names.iter().any(|n| n == "o_orderkey") && names.iter().any(|n| n == "l_orderkey"),
            "expected both orders.o_orderkey AND lineitem.l_orderkey to match \
             stem 'orderkey'; got {names:?}"
        );
    }

    #[tokio::test]
    async fn walker_finds_only_lineitem_for_suppkey_stem() {
        let plan = build_join_plan("walker_supp").await;
        let matches = find_scans_by_fk_chain(&plan, "suppkey");
        let names: Vec<_> = matches.iter().map(|m| m.col_name.clone()).collect();
        assert_eq!(
            names,
            vec!["l_suppkey".to_string()],
            "expected only lineitem.l_suppkey to match stem 'suppkey'"
        );
    }

    #[tokio::test]
    async fn walker_returns_empty_for_unrelated_stem() {
        let plan = build_join_plan("walker_none").await;
        let matches = find_scans_by_fk_chain(&plan, "partkey");
        assert!(
            matches.is_empty(),
            "expected no scans to match unused stem 'partkey'; got {} matches",
            matches.len()
        );
    }

    #[tokio::test]
    async fn walker_col_idx_is_file_schema_index() {
        // Σ.Q.L14 lesson: col_idx must be the FILE-schema index,
        // not the projected-schema index. orders.o_orderkey is
        // file col 0; lineitem.l_orderkey is file col 0 also.
        let plan = build_join_plan("walker_idx").await;
        let matches = find_scans_by_fk_chain(&plan, "orderkey");
        for m in &matches {
            let file_sch = m.scan_typed.file_schema();
            assert_eq!(
                file_sch.field(m.col_idx).name(),
                &m.col_name,
                "col_idx must refer to FILE-schema column with name {:?}",
                m.col_name,
            );
        }
    }
}
