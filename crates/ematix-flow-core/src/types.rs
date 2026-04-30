//! Phase 2: column type catalogue + `TableSpec`.
//!
//! Wire format Python ships when declaring a `ManagedTable`. Phase 4's DDL
//! planner consumes `TableSpec` to emit `CREATE TABLE` statements.

use std::collections::HashSet;
use std::fmt::Write as _;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::spec::SpecError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ColumnType {
    SmallInt,
    Integer,
    BigInt,
    Float,
    Double,
    Numeric { precision: u8, scale: u8 },
    Boolean,
    Text,
    String { length: u32 },
    Date,
    Timestamp,
    TimestampTz,
    Json,
    Jsonb,
    Uuid,
    Bytes,
}

impl ColumnType {
    pub fn to_postgres_sql(&self) -> String {
        match self {
            ColumnType::SmallInt => "SMALLINT".into(),
            ColumnType::Integer => "INTEGER".into(),
            ColumnType::BigInt => "BIGINT".into(),
            ColumnType::Float => "REAL".into(),
            ColumnType::Double => "DOUBLE PRECISION".into(),
            ColumnType::Numeric { precision, scale } => format!("NUMERIC({precision},{scale})"),
            ColumnType::Boolean => "BOOLEAN".into(),
            ColumnType::Text => "TEXT".into(),
            ColumnType::String { length } => format!("VARCHAR({length})"),
            ColumnType::Date => "DATE".into(),
            ColumnType::Timestamp => "TIMESTAMP".into(),
            ColumnType::TimestampTz => "TIMESTAMPTZ".into(),
            ColumnType::Json => "JSON".into(),
            ColumnType::Jsonb => "JSONB".into(),
            ColumnType::Uuid => "UUID".into(),
            ColumnType::Bytes => "BYTEA".into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnSpec {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: ColumnType,
    pub nullable: bool,
    pub primary_key: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableSpec {
    pub schema: String,
    pub name: String,
    pub columns: Vec<ColumnSpec>,
    /// Computed by Rust during normalize; ignored on input. 32-char hex
    /// (first 16 bytes of SHA-256 over a canonical encoding).
    #[serde(default)]
    pub fingerprint: String,
}

impl TableSpec {
    pub fn from_json(s: &str) -> Result<Self, SpecError> {
        let mut spec: TableSpec = serde_json::from_str(s)?;
        spec.normalize();
        spec.validate()?;
        spec.fingerprint = spec.compute_fingerprint();
        Ok(spec)
    }

    pub fn to_json(&self) -> Result<String, SpecError> {
        Ok(serde_json::to_string(self)?)
    }

    fn normalize(&mut self) {
        self.schema = self.schema.trim().to_string();
        self.name = self.name.trim().to_string();
        for col in &mut self.columns {
            col.name = col.name.trim().to_string();
        }
    }

    fn validate(&self) -> Result<(), SpecError> {
        if self.schema.is_empty() {
            return Err(SpecError::Validation("schema must not be empty".into()));
        }
        if self.name.is_empty() {
            return Err(SpecError::Validation("name must not be empty".into()));
        }
        if self.columns.is_empty() {
            return Err(SpecError::Validation(
                "table must declare at least one column".into(),
            ));
        }
        let mut seen: HashSet<&str> = HashSet::new();
        for col in &self.columns {
            if col.name.is_empty() {
                return Err(SpecError::Validation(
                    "column name must not be empty".into(),
                ));
            }
            if !seen.insert(col.name.as_str()) {
                return Err(SpecError::Validation(format!(
                    "duplicate column name: {}",
                    col.name
                )));
            }
        }
        if !self.columns.iter().any(|c| c.primary_key) {
            return Err(SpecError::Validation(
                "table must declare at least one primary key column".into(),
            ));
        }
        Ok(())
    }

    fn compute_fingerprint(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(self.schema.as_bytes());
        hasher.update(b".");
        hasher.update(self.name.as_bytes());
        for col in &self.columns {
            hasher.update(b"|");
            hasher.update(col.name.as_bytes());
            hasher.update(b":");
            hasher.update(col.ty.to_postgres_sql().as_bytes());
            hasher.update(if col.nullable { b":n" } else { b":N" });
            hasher.update(if col.primary_key { b":p" } else { b":P" });
        }
        let digest = hasher.finalize();
        hex_encode(&digest[..16])
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        write!(s, "{b:02x}").unwrap();
    }
    s
}

/// Parse, normalize, validate, fingerprint, and re-serialize a `TableSpec`.
pub fn normalize_table_json(s: &str) -> Result<String, SpecError> {
    TableSpec::from_json(s)?.to_json()
}

#[cfg(test)]
mod tests {
    use crate::types::{ColumnSpec, ColumnType, TableSpec, normalize_table_json};

    fn customer_dim_json() -> &'static str {
        r#"{
            "schema": "warehouse",
            "name": "customer_dim",
            "columns": [
                {"name": "customer_id", "type": {"kind": "big_int"}, "nullable": false, "primary_key": true},
                {"name": "email", "type": {"kind": "string", "length": 256}, "nullable": false, "primary_key": false},
                {"name": "balance", "type": {"kind": "numeric", "precision": 12, "scale": 2}, "nullable": true, "primary_key": false},
                {"name": "created_at", "type": {"kind": "timestamp_tz"}, "nullable": false, "primary_key": false}
            ]
        }"#
    }

    #[test]
    fn to_postgres_sql_covers_catalogue() {
        assert_eq!(ColumnType::SmallInt.to_postgres_sql(), "SMALLINT");
        assert_eq!(ColumnType::Integer.to_postgres_sql(), "INTEGER");
        assert_eq!(ColumnType::BigInt.to_postgres_sql(), "BIGINT");
        assert_eq!(ColumnType::Float.to_postgres_sql(), "REAL");
        assert_eq!(ColumnType::Double.to_postgres_sql(), "DOUBLE PRECISION");
        assert_eq!(
            ColumnType::Numeric {
                precision: 12,
                scale: 2,
            }
            .to_postgres_sql(),
            "NUMERIC(12,2)"
        );
        assert_eq!(ColumnType::Boolean.to_postgres_sql(), "BOOLEAN");
        assert_eq!(ColumnType::Text.to_postgres_sql(), "TEXT");
        assert_eq!(
            ColumnType::String { length: 64 }.to_postgres_sql(),
            "VARCHAR(64)"
        );
        assert_eq!(ColumnType::Date.to_postgres_sql(), "DATE");
        assert_eq!(ColumnType::Timestamp.to_postgres_sql(), "TIMESTAMP");
        assert_eq!(ColumnType::TimestampTz.to_postgres_sql(), "TIMESTAMPTZ");
        assert_eq!(ColumnType::Json.to_postgres_sql(), "JSON");
        assert_eq!(ColumnType::Jsonb.to_postgres_sql(), "JSONB");
        assert_eq!(ColumnType::Uuid.to_postgres_sql(), "UUID");
        assert_eq!(ColumnType::Bytes.to_postgres_sql(), "BYTEA");
    }

    #[test]
    fn table_spec_round_trips() {
        let spec = TableSpec::from_json(customer_dim_json()).unwrap();
        assert_eq!(spec.schema, "warehouse");
        assert_eq!(spec.name, "customer_dim");
        assert_eq!(spec.columns.len(), 4);
        assert_eq!(spec.columns[0].name, "customer_id");
        assert!(spec.columns[0].primary_key);
        assert!(!spec.columns[0].nullable);
        assert!(!spec.fingerprint.is_empty());

        let again = TableSpec::from_json(&spec.to_json().unwrap()).unwrap();
        assert_eq!(spec, again);
    }

    #[test]
    fn rejects_unknown_field_on_table_spec() {
        let raw = r#"{
            "schema": "s",
            "name": "t",
            "columns": [{"name": "id", "type": {"kind": "integer"}, "nullable": false, "primary_key": true}],
            "bogus": true
        }"#;
        assert!(TableSpec::from_json(raw).is_err());
    }

    #[test]
    fn rejects_unknown_field_on_column() {
        let raw = r#"{
            "schema": "s",
            "name": "t",
            "columns": [{"name": "id", "type": {"kind": "integer"}, "nullable": false, "primary_key": true, "extra": 1}]
        }"#;
        assert!(TableSpec::from_json(raw).is_err());
    }

    #[test]
    fn requires_at_least_one_column() {
        let raw = r#"{"schema": "s", "name": "t", "columns": []}"#;
        assert!(TableSpec::from_json(raw).is_err());
    }

    #[test]
    fn requires_at_least_one_primary_key() {
        let raw = r#"{
            "schema": "s",
            "name": "t",
            "columns": [{"name": "id", "type": {"kind": "integer"}, "nullable": true, "primary_key": false}]
        }"#;
        assert!(TableSpec::from_json(raw).is_err());
    }

    #[test]
    fn rejects_duplicate_column_names() {
        let raw = r#"{
            "schema": "s",
            "name": "t",
            "columns": [
                {"name": "id", "type": {"kind": "integer"}, "nullable": false, "primary_key": true},
                {"name": "id", "type": {"kind": "text"}, "nullable": false, "primary_key": false}
            ]
        }"#;
        assert!(TableSpec::from_json(raw).is_err());
    }

    #[test]
    fn fingerprint_is_stable_across_runs() {
        let a = TableSpec::from_json(customer_dim_json()).unwrap();
        let b = TableSpec::from_json(customer_dim_json()).unwrap();
        assert_eq!(a.fingerprint, b.fingerprint);
        assert_eq!(a.fingerprint.len(), 32); // 16 bytes hex
    }

    #[test]
    fn fingerprint_changes_when_columns_change() {
        let base = TableSpec::from_json(customer_dim_json()).unwrap();
        let altered = r#"{
            "schema": "warehouse",
            "name": "customer_dim",
            "columns": [
                {"name": "customer_id", "type": {"kind": "big_int"}, "nullable": false, "primary_key": true},
                {"name": "email", "type": {"kind": "string", "length": 256}, "nullable": false, "primary_key": false}
            ]
        }"#;
        let alt = TableSpec::from_json(altered).unwrap();
        assert_ne!(base.fingerprint, alt.fingerprint);
    }

    #[test]
    fn fingerprint_changes_when_type_changes() {
        let base = TableSpec::from_json(customer_dim_json()).unwrap();
        let altered = customer_dim_json().replace(r#""kind": "big_int""#, r#""kind": "integer""#);
        let alt = TableSpec::from_json(&altered).unwrap();
        assert_ne!(base.fingerprint, alt.fingerprint);
    }

    #[test]
    fn fingerprint_sensitive_to_column_order() {
        // Reordering columns is a real schema difference (physical order).
        let json = r#"{
            "schema": "s",
            "name": "t",
            "columns": [
                {"name": "a", "type": {"kind": "integer"}, "nullable": false, "primary_key": true},
                {"name": "b", "type": {"kind": "text"}, "nullable": true, "primary_key": false}
            ]
        }"#;
        let reversed = r#"{
            "schema": "s",
            "name": "t",
            "columns": [
                {"name": "b", "type": {"kind": "text"}, "nullable": true, "primary_key": false},
                {"name": "a", "type": {"kind": "integer"}, "nullable": false, "primary_key": true}
            ]
        }"#;
        let a = TableSpec::from_json(json).unwrap();
        let b = TableSpec::from_json(reversed).unwrap();
        assert_ne!(a.fingerprint, b.fingerprint);
    }

    #[test]
    fn normalize_table_json_is_idempotent() {
        let once = normalize_table_json(customer_dim_json()).unwrap();
        let twice = normalize_table_json(&once).unwrap();
        assert_eq!(once, twice);
    }

    #[test]
    fn columns_are_serializable_with_struct_variants() {
        // {"kind": "string", "length": 64} should round-trip
        let col = ColumnSpec {
            name: "x".into(),
            ty: ColumnType::String { length: 64 },
            nullable: false,
            primary_key: false,
        };
        let json = serde_json::to_string(&col).unwrap();
        let back: ColumnSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(col, back);
    }
}
