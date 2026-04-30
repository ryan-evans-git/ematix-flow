//! Phase 10: metadata helpers — watermarks + filter-wrapping.
//!
//! `ematix_flow.watermarks` stores the last value processed per pipeline.
//! `wrap_with_watermark_filter` wraps a user source query with a WHERE
//! clause that filters out rows already processed.

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
