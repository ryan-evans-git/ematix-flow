//! Phase 1 `PipelineSpec`: the wire format Python ships to Rust.
//!
//! This is the smallest stable shape that downstream phases can extend
//! (Phase 2 adds column declarations, Phase 5+ adds strategy options,
//! Phase 10 adds incremental/watermark fields).

use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    Append,
    Truncate,
    Merge,
    Scd1,
    Scd2,
}

impl Mode {
    fn requires_keys(self) -> bool {
        matches!(self, Mode::Merge | Mode::Scd1 | Mode::Scd2)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceSpec {
    pub connection: String,
    pub query: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TargetSpec {
    pub connection: String,
    pub schema: String,
    pub table: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineSpec {
    pub name: String,
    pub source: SourceSpec,
    pub target: TargetSpec,
    pub mode: Mode,
    #[serde(default)]
    pub keys: Vec<String>,
}

#[derive(Debug, Error)]
pub enum SpecError {
    #[error("invalid spec JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid spec: {0}")]
    Validation(String),
}

impl PipelineSpec {
    pub fn from_json(s: &str) -> Result<Self, SpecError> {
        let mut spec: PipelineSpec = serde_json::from_str(s)?;
        spec.normalize();
        spec.validate()?;
        Ok(spec)
    }

    pub fn to_json(&self) -> Result<String, SpecError> {
        Ok(serde_json::to_string(self)?)
    }

    fn normalize(&mut self) {
        trim_in_place(&mut self.name);
        trim_in_place(&mut self.source.connection);
        trim_in_place(&mut self.source.query);
        trim_in_place(&mut self.target.connection);
        trim_in_place(&mut self.target.schema);
        trim_in_place(&mut self.target.table);
        for k in &mut self.keys {
            trim_in_place(k);
        }
        let mut seen: HashSet<String> = HashSet::new();
        self.keys.retain(|k| seen.insert(k.clone()));
    }

    fn validate(&self) -> Result<(), SpecError> {
        if self.name.is_empty() {
            return Err(SpecError::Validation("name must not be empty".into()));
        }
        if self.source.query.is_empty() {
            return Err(SpecError::Validation(
                "source.query must not be empty".into(),
            ));
        }
        if self.source.connection.is_empty() {
            return Err(SpecError::Validation(
                "source.connection must not be empty".into(),
            ));
        }
        if self.target.connection.is_empty() {
            return Err(SpecError::Validation(
                "target.connection must not be empty".into(),
            ));
        }
        if self.target.schema.is_empty() || self.target.table.is_empty() {
            return Err(SpecError::Validation(
                "target.schema and target.table must not be empty".into(),
            ));
        }
        if self.mode.requires_keys() && self.keys.is_empty() {
            return Err(SpecError::Validation(format!(
                "mode {:?} requires at least one key",
                self.mode
            )));
        }
        if self.keys.iter().any(|k| k.is_empty()) {
            return Err(SpecError::Validation(
                "keys must not contain empty strings".into(),
            ));
        }
        Ok(())
    }
}

fn trim_in_place(s: &mut String) {
    let trimmed = s.trim();
    if trimmed.len() != s.len() {
        *s = trimmed.to_string();
    }
}

/// Parse, normalize, validate, and re-serialize a spec. The bridge entry point.
pub fn normalize_json(s: &str) -> Result<String, SpecError> {
    PipelineSpec::from_json(s)?.to_json()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal_append() -> &'static str {
        r#"{
            "name": "customers",
            "source": {"connection": "postgres://src/db", "query": "SELECT * FROM customers"},
            "target": {"connection": "postgres://dst/db", "schema": "warehouse", "table": "customer_dim"},
            "mode": "append"
        }"#
    }

    #[test]
    fn round_trips_minimal() {
        let spec = PipelineSpec::from_json(minimal_append()).unwrap();
        assert_eq!(spec.name, "customers");
        assert_eq!(spec.mode, Mode::Append);
        assert!(spec.keys.is_empty());

        let again = PipelineSpec::from_json(&spec.to_json().unwrap()).unwrap();
        assert_eq!(spec, again);
    }

    #[test]
    fn trims_whitespace_and_dedupes_keys() {
        let raw = r#"{
            "name": "  c  ",
            "source": {"connection": " a ", "query": "  SELECT 1  "},
            "target": {"connection": "a", "schema": " s ", "table": " t "},
            "mode": "merge",
            "keys": ["id", " id ", "name"]
        }"#;
        let spec = PipelineSpec::from_json(raw).unwrap();
        assert_eq!(spec.name, "c");
        assert_eq!(spec.source.query, "SELECT 1");
        assert_eq!(spec.target.schema, "s");
        assert_eq!(spec.keys, vec!["id".to_string(), "name".to_string()]);
    }

    #[test]
    fn rejects_unknown_field() {
        let raw = r#"{
            "name": "x",
            "source": {"connection": "c", "query": "q"},
            "target": {"connection": "c", "schema": "s", "table": "t"},
            "mode": "append",
            "bogus": true
        }"#;
        let err = PipelineSpec::from_json(raw).unwrap_err();
        assert!(matches!(err, SpecError::Json(_)));
    }

    #[test]
    fn rejects_invalid_mode() {
        let raw = r#"{
            "name": "x",
            "source": {"connection": "c", "query": "q"},
            "target": {"connection": "c", "schema": "s", "table": "t"},
            "mode": "wat"
        }"#;
        assert!(matches!(
            PipelineSpec::from_json(raw).unwrap_err(),
            SpecError::Json(_)
        ));
    }

    #[test]
    fn merge_requires_keys() {
        let raw = r#"{
            "name": "x",
            "source": {"connection": "c", "query": "q"},
            "target": {"connection": "c", "schema": "s", "table": "t"},
            "mode": "merge"
        }"#;
        let err = PipelineSpec::from_json(raw).unwrap_err();
        assert!(matches!(err, SpecError::Validation(_)));
    }

    #[test]
    fn append_does_not_require_keys() {
        PipelineSpec::from_json(minimal_append()).unwrap();
    }

    #[test]
    fn empty_query_is_rejected_after_trim() {
        let raw = r#"{
            "name": "x",
            "source": {"connection": "c", "query": "   "},
            "target": {"connection": "c", "schema": "s", "table": "t"},
            "mode": "append"
        }"#;
        assert!(matches!(
            PipelineSpec::from_json(raw).unwrap_err(),
            SpecError::Validation(_)
        ));
    }

    #[test]
    fn normalize_json_is_idempotent() {
        let once = normalize_json(minimal_append()).unwrap();
        let twice = normalize_json(&once).unwrap();
        assert_eq!(once, twice);
    }
}
