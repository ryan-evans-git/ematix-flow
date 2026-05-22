//! Σ.L.5 — workload-aware parquet write tuner.
//!
//! ## Vision
//!
//! Read Σ.L.2's workload.db. For each table we have read profiling
//! data on, recommend write-side changes:
//!
//! - **Row-group size** — if Q12-shape (small post-filter result)
//!   dominates queries against this table, smaller row groups give
//!   finer pruning. If full-scan aggregates dominate (Q01), larger
//!   row groups reduce per-RG overhead.
//! - **Dict columns** — Σ.L.1 probe outcomes that consistently picked
//!   dict-on say "write this column dict-encoded with a generous
//!   threshold so dict pages aren't fallen back to PLAIN at high
//!   cardinality".
//! - **Sort key** — observed GROUP BY columns + range predicates point
//!   at the right sort order for `parquet --sort-by`.
//! - **Bloom columns** — frequently-Eq'd columns benefit from bloom
//!   filters at write time (DataFusion reads them at scan time).
//! - **Compression codec** — wall-time-vs-disk tradeoff per column.
//!
//! Photon reads parquet but doesn't write it. We do both (Π.2 series)
//! — the **read-profile → write-tune closed loop is unique to us**.
//!
//! ## Status — analysis API + recommendation struct only
//!
//! Tonight: the recommendation data structure + analyser that converts
//! a workload.db state into recommendations. CLI + actual rewrite are
//! follow-ups (Σ.L.5.1 = analysis, Σ.L.5.2 = `flow tune-parquet` CLI,
//! Σ.L.5.3 = in-place rewrite + verify-equivalent check).

use std::collections::HashMap;

use crate::workload_log::WorkloadLog;

/// Per-table write-side recommendation derived from observed read
/// patterns.
#[derive(Debug, Clone, PartialEq)]
pub struct WriteTuneRecommendation {
    pub table_name: String,
    /// Suggested row-group size in rows. `None` = no change.
    pub row_group_size: Option<u32>,
    /// Columns that benefit from dict encoding at write time.
    pub dict_columns: Vec<String>,
    /// Suggested ordering of column data — first key is the primary
    /// sort, secondary keys break ties.
    pub sort_key: Vec<String>,
    /// Columns to write bloom filters for.
    pub bloom_columns: Vec<String>,
    /// Per-column compression suggestions (column → codec name).
    /// Codec names: "snappy", "zstd", "lz4", "gzip".
    pub compression_per_column: HashMap<String, String>,
    /// Confidence score 0..1 based on observation count. Below 0.3
    /// means the workload log is too thin; callers can choose to
    /// skip the rewrite.
    pub confidence: f64,
    /// Human-readable rationale lines for the CLI / docs.
    pub rationale: Vec<String>,
}

impl WriteTuneRecommendation {
    pub fn no_changes(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            row_group_size: None,
            dict_columns: Vec::new(),
            sort_key: Vec::new(),
            bloom_columns: Vec::new(),
            compression_per_column: HashMap::new(),
            confidence: 0.0,
            rationale: vec!["no observations in workload log".to_string()],
        }
    }

    pub fn is_no_op(&self) -> bool {
        self.row_group_size.is_none()
            && self.dict_columns.is_empty()
            && self.sort_key.is_empty()
            && self.bloom_columns.is_empty()
            && self.compression_per_column.is_empty()
    }
}

/// Σ.L.5 — analyse the workload log and emit a per-table
/// recommendation. Calls into [`WorkloadLog`] for probe outcomes +
/// observed selectivity.
///
/// For tonight: implements the dict-column + bloom-column rules. Sort
/// key + row-group size + per-column compression are stubbed (return
/// empty/None) pending more observation types being logged.
pub fn recommend_for_table(
    log: &WorkloadLog,
    table_name: &str,
    candidate_columns: &[&str],
) -> Result<WriteTuneRecommendation, crate::workload_log::WorkloadLogError> {
    let mut rec = WriteTuneRecommendation::no_changes(table_name);
    let mut total_observations: i64 = 0;

    // Rule 1: dict-encode columns where the workload log says dict-on
    // wins. We use `consult_probe` with min_observations=2 to filter
    // out single-observation noise.
    for col in candidate_columns {
        if let Some(true) = log.consult_probe(table_name, col, 2)? {
            rec.dict_columns.push((*col).to_string());
            rec.rationale.push(format!(
                "dict-encode `{col}`: observed dict-arrival wins in workload log"
            ));
            total_observations += 1;
        }
    }

    // Rule 2: bloom-filter columns with observed Eq selectivity ≤ 1%.
    // These are the columns where bloom is most likely to skip rows
    // at read time (DataFusion will use parquet bloom indices for
    // pruning).
    for col in candidate_columns {
        if let Some(sel) = log.get_selectivity(table_name, col, "eq")? {
            if sel <= 0.01 {
                rec.bloom_columns.push((*col).to_string());
                rec.rationale.push(format!(
                    "bloom on `{col}`: Eq selectivity {:.4} ≤ 1%, bloom pruning will skip the bulk",
                    sel
                ));
                total_observations += 1;
            }
        }
    }

    // Rule 3 (stub): sort key — pick the most-frequently-GROUPed
    // string column. Workload log doesn't currently log per-GROUP-BY
    // counts; once Σ.L.2 adds that, this picks them out.
    // (Intentional no-op for now; the rationale records the gap.)
    if rec.dict_columns.is_empty() {
        rec.rationale
            .push("sort-key recommendation: requires Σ.L.2 group-by-frequency logging".to_string());
    }

    // Confidence — combine count + diversity. Rough heuristic.
    rec.confidence = (total_observations as f64 / 10.0).min(1.0);
    Ok(rec)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_observations_yields_no_op() {
        let log = WorkloadLog::open_in_memory().unwrap();
        let rec = recommend_for_table(&log, "lineitem", &["l_shipmode", "l_returnflag"]).unwrap();
        assert!(rec.is_no_op());
        assert_eq!(rec.confidence, 0.0);
    }

    #[test]
    fn probe_wins_become_dict_columns() {
        let log = WorkloadLog::open_in_memory().unwrap();
        // Two observations of dict-on winning on l_shipmode
        log.record_probe_outcome("lineitem", "l_shipmode", 8.0, 18.0)
            .unwrap();
        log.record_probe_outcome("lineitem", "l_shipmode", 9.0, 17.0)
            .unwrap();
        // One observation of dict-on losing on l_returnflag
        log.record_probe_outcome("lineitem", "l_returnflag", 25.0, 15.0)
            .unwrap();
        log.record_probe_outcome("lineitem", "l_returnflag", 24.0, 14.0)
            .unwrap();

        let rec = recommend_for_table(&log, "lineitem", &["l_shipmode", "l_returnflag"])
            .unwrap();
        assert_eq!(rec.dict_columns, vec!["l_shipmode"]);
        assert!(!rec.is_no_op());
        assert!(rec.confidence > 0.0);
    }

    #[test]
    fn low_selectivity_becomes_bloom_column() {
        let log = WorkloadLog::open_in_memory().unwrap();
        log.record_selectivity("orders", "o_orderkey", "eq", 0.0001)
            .unwrap();
        log.record_selectivity("orders", "o_status", "eq", 0.5)
            .unwrap();

        let rec = recommend_for_table(&log, "orders", &["o_orderkey", "o_status"]).unwrap();
        assert_eq!(rec.bloom_columns, vec!["o_orderkey"]);
        assert!(!rec
            .bloom_columns
            .iter()
            .any(|c| c == "o_status"));
    }

    #[test]
    fn full_adaptive_pipeline_l1_l2_l5_e2e() {
        // Simulate: Σ.L.1 probes a table several times → outcomes
        // land in Σ.L.2's log → Σ.L.5 reads it and recommends a
        // dict-column rewrite.
        let log = WorkloadLog::open_in_memory().unwrap();

        // Σ.L.1 records 5 probes against (orders, o_orderpriority);
        // dict-on consistently wins.
        for _ in 0..5 {
            log.record_probe_outcome("orders", "o_orderpriority", 8.0, 18.0)
                .unwrap();
        }
        // (orders, o_clerk) is consistently a dict-loser.
        for _ in 0..5 {
            log.record_probe_outcome("orders", "o_clerk", 22.0, 12.0)
                .unwrap();
        }
        // o_orderkey is heavily Eq-filtered.
        log.record_selectivity("orders", "o_orderkey", "eq", 0.0001)
            .unwrap();

        // Σ.L.5 emits recommendations.
        let rec = recommend_for_table(
            &log,
            "orders",
            &["o_orderpriority", "o_clerk", "o_orderkey"],
        )
        .unwrap();

        // Pipeline assertions:
        // - dict-rewrite recommended only for o_orderpriority
        assert_eq!(rec.dict_columns, vec!["o_orderpriority"]);
        // - bloom recommended on o_orderkey (low Eq selectivity)
        assert_eq!(rec.bloom_columns, vec!["o_orderkey"]);
        // - confidence > 0 since we have observations
        assert!(rec.confidence > 0.0);
        // - rationale lines reference both rules
        assert!(rec
            .rationale
            .iter()
            .any(|l| l.contains("dict-encode `o_orderpriority`")));
        assert!(rec
            .rationale
            .iter()
            .any(|l| l.contains("bloom on `o_orderkey`")));
    }

    #[test]
    fn confidence_saturates_at_one() {
        let log = WorkloadLog::open_in_memory().unwrap();
        // 15 dict-winning columns → confidence should max at 1.0
        for i in 0..15 {
            let c = format!("c{i}");
            log.record_probe_outcome("t", &c, 8.0, 18.0).unwrap();
            log.record_probe_outcome("t", &c, 8.0, 18.0).unwrap();
        }
        let cols: Vec<String> = (0..15).map(|i| format!("c{i}")).collect();
        let cols_refs: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
        let rec = recommend_for_table(&log, "t", &cols_refs).unwrap();
        assert_eq!(rec.confidence, 1.0);
    }
}
