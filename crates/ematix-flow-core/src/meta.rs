//! Phase 10: metadata helpers — watermarks + filter-wrapping.
//!
//! `ematix_flow.watermarks` stores the last value processed per pipeline.
//! `wrap_with_watermark_filter` wraps a user source query with a WHERE
//! clause that filters out rows already processed.

/// Phase 11: opt-in delete handling for merge and SCD2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteHandling {
    /// merge: DELETE rows whose keys are missing from the source.
    Hard,
    /// scd2: close out current versions whose keys are missing.
    Soft,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatermarkConfig {
    /// The column on the source side carrying the incremental value.
    pub column: String,
    /// Pre-cast SQL literal for the previous run's high-water mark, e.g.
    /// `'2026-04-30T00:00:00Z'::timestamptz` or `100::bigint`. `None` on
    /// the first run (treated as `-infinity`).
    pub last_value_literal: Option<String>,
}

pub fn wrap_with_watermark_filter(
    source_query: &str,
    watermark: Option<&WatermarkConfig>,
) -> String {
    match watermark {
        Some(WatermarkConfig {
            column,
            last_value_literal: Some(literal),
        }) => format!("SELECT * FROM ({source_query}) _wm WHERE {column} > {literal}"),
        _ => source_query.to_string(),
    }
}

/// Phase 11: hard-delete post-step for merge.
///
/// `source_keys_query` should produce the natural key columns for every
/// row in the desired target state (typically the source query). Any
/// existing target row whose key tuple is absent gets DELETEd.
pub fn build_hard_delete_sql(
    target_schema: &str,
    target_table: &str,
    keys: &[String],
    source_keys_query: &str,
) -> String {
    let key_tuple = keys.join(", ");
    format!(
        "DELETE FROM {schema}.{table} \
         WHERE ({key_tuple}) NOT IN (SELECT {key_tuple} FROM ({source}) _del)",
        schema = target_schema,
        table = target_table,
        key_tuple = key_tuple,
        source = source_keys_query,
    )
}

/// Phase 11: soft-delete (close-out) post-step for SCD2.
///
/// Closes out current versions whose natural key is absent from the
/// source (sets `valid_to = now()`, `is_current = false`). No new
/// version is inserted — the row is logically tombstoned.
pub fn build_scd2_close_missing_sql(
    target_schema: &str,
    target_table: &str,
    keys: &[String],
    source_keys_query: &str,
) -> String {
    let key_tuple = keys.join(", ");
    format!(
        "UPDATE {schema}.{table} \
         SET valid_to = now(), is_current = false \
         WHERE is_current \
           AND ({key_tuple}) NOT IN (SELECT {key_tuple} FROM ({source}) _del)",
        schema = target_schema,
        table = target_table,
        key_tuple = key_tuple,
        source = source_keys_query,
    )
}

#[cfg(test)]
mod tests {
    use crate::meta::{WatermarkConfig, wrap_with_watermark_filter};

    #[test]
    fn no_watermark_passes_query_through_unchanged() {
        let out = wrap_with_watermark_filter("SELECT * FROM events", None);
        assert_eq!(out, "SELECT * FROM events");
    }

    #[test]
    fn watermark_with_no_last_value_passes_through() {
        // First run: column known, but no prior watermark — no filter.
        let cfg = WatermarkConfig {
            column: "updated_at".into(),
            last_value_literal: None,
        };
        let out = wrap_with_watermark_filter("SELECT * FROM events", Some(&cfg));
        assert_eq!(out, "SELECT * FROM events");
    }

    #[test]
    fn watermark_with_last_value_wraps_in_subquery_with_strict_gt() {
        let cfg = WatermarkConfig {
            column: "updated_at".into(),
            last_value_literal: Some("'2026-04-30T00:00:00Z'::timestamptz".into()),
        };
        let out = wrap_with_watermark_filter("SELECT * FROM events", Some(&cfg));
        assert_eq!(
            out,
            "SELECT * FROM (SELECT * FROM events) _wm \
             WHERE updated_at > '2026-04-30T00:00:00Z'::timestamptz"
        );
    }

    use crate::meta::{build_hard_delete_sql, build_scd2_close_missing_sql};

    #[test]
    fn hard_delete_emits_not_in_clause_for_single_key() {
        let sql = build_hard_delete_sql(
            "warehouse",
            "customer_dim",
            &["customer_id".into()],
            "SELECT * FROM staging",
        );
        assert!(sql.contains("DELETE FROM warehouse.customer_dim"));
        assert!(sql.contains(
            "WHERE (customer_id) NOT IN (SELECT customer_id FROM (SELECT * FROM staging) _del)"
        ));
    }

    #[test]
    fn hard_delete_handles_composite_keys() {
        let sql = build_hard_delete_sql("s", "t", &["a".into(), "b".into()], "SELECT * FROM stage");
        assert!(sql.contains("(a, b) NOT IN (SELECT a, b FROM (SELECT * FROM stage) _del)"));
    }

    #[test]
    fn scd2_close_missing_filters_to_current_only() {
        let sql = build_scd2_close_missing_sql(
            "s",
            "t",
            &["customer_id".into()],
            "SELECT * FROM staging",
        );
        assert!(sql.contains("UPDATE s.t"));
        assert!(sql.contains("SET valid_to = now()"));
        assert!(sql.contains("is_current = false"));
        assert!(sql.contains("WHERE is_current"));
        assert!(sql.contains(
            "(customer_id) NOT IN (SELECT customer_id FROM (SELECT * FROM staging) _del)"
        ));
    }

    #[test]
    fn watermark_handles_complex_source_query() {
        // Joins, subqueries, etc. — wrapping in a subquery isolates them.
        let cfg = WatermarkConfig {
            column: "ts".into(),
            last_value_literal: Some("100::bigint".into()),
        };
        let source = "SELECT a.x, b.y AS ts FROM a JOIN b ON a.id = b.id";
        let out = wrap_with_watermark_filter(source, Some(&cfg));
        assert!(out.contains("FROM (SELECT a.x, b.y AS ts FROM a JOIN b ON a.id = b.id) _wm"));
        assert!(out.ends_with("WHERE ts > 100::bigint"));
    }
}
