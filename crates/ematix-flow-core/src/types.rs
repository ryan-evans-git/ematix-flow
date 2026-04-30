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
    /// Phase 22: composite UNIQUE constraints in addition to the primary
    /// key. Each entry is an ordered list of column names; column order
    /// within a constraint is significant (Postgres treats `UNIQUE (a, b)`
    /// and `UNIQUE (b, a)` as distinct indexes). Order across constraints
    /// is not significant.
    #[serde(default)]
    pub unique_constraints: Vec<Vec<String>>,
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
        for uc in &mut self.unique_constraints {
            for col in uc {
                *col = col.trim().to_string();
            }
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
        // Phase 22: every UNIQUE constraint must be non-empty and reference
        // declared columns.
        let column_names: HashSet<&str> = self.columns.iter().map(|c| c.name.as_str()).collect();
        for (i, uc) in self.unique_constraints.iter().enumerate() {
            if uc.is_empty() {
                return Err(SpecError::Validation(format!(
                    "unique_constraints[{i}] must not be empty"
                )));
            }
            let mut local_seen: HashSet<&str> = HashSet::new();
            for col in uc {
                if col.is_empty() {
                    return Err(SpecError::Validation(format!(
                        "unique_constraints[{i}] contains an empty column name"
                    )));
                }
                if !column_names.contains(col.as_str()) {
                    return Err(SpecError::Validation(format!(
                        "unique_constraints[{i}] references unknown column {col}"
                    )));
                }
                if !local_seen.insert(col.as_str()) {
                    return Err(SpecError::Validation(format!(
                        "unique_constraints[{i}] lists column {col} twice"
                    )));
                }
            }
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
        // Sort the unique-constraint set so the fingerprint is invariant
        // to the declaration order across constraints (matches the
        // semantic equality compare_table uses).
        let mut sorted_unique: Vec<&[String]> = self
            .unique_constraints
            .iter()
            .map(|v| v.as_slice())
            .collect();
        sorted_unique.sort();
        for uc in sorted_unique {
            hasher.update(b"|U:");
            hasher.update(uc.join(",").as_bytes());
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

    // --- Phase 22: unique_constraints --------------------------------------

    #[test]
    fn unique_constraints_default_empty() {
        let spec = TableSpec::from_json(customer_dim_json()).unwrap();
        assert!(spec.unique_constraints.is_empty());
    }

    #[test]
    fn unique_constraints_round_trip_through_json() {
        let raw = r#"{
            "schema": "warehouse",
            "name": "customer_order",
            "columns": [
                {"name": "id", "type": {"kind": "big_int"}, "nullable": false, "primary_key": true},
                {"name": "customer_id", "type": {"kind": "big_int"}, "nullable": false, "primary_key": false},
                {"name": "order_date", "type": {"kind": "date"}, "nullable": false, "primary_key": false}
            ],
            "unique_constraints": [["customer_id", "order_date"]]
        }"#;
        let spec = TableSpec::from_json(raw).unwrap();
        assert_eq!(
            spec.unique_constraints,
            vec![vec!["customer_id".to_string(), "order_date".to_string()]]
        );
        let again = TableSpec::from_json(&spec.to_json().unwrap()).unwrap();
        assert_eq!(spec, again);
    }

    #[test]
    fn unique_constraint_referencing_unknown_column_is_rejected() {
        let raw = r#"{
            "schema": "s",
            "name": "t",
            "columns": [
                {"name": "id", "type": {"kind": "big_int"}, "nullable": false, "primary_key": true}
            ],
            "unique_constraints": [["nonexistent"]]
        }"#;
        let err = TableSpec::from_json(raw).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("nonexistent"));
    }

    #[test]
    fn empty_unique_constraint_is_rejected() {
        let raw = r#"{
            "schema": "s",
            "name": "t",
            "columns": [
                {"name": "id", "type": {"kind": "big_int"}, "nullable": false, "primary_key": true}
            ],
            "unique_constraints": [[]]
        }"#;
        assert!(TableSpec::from_json(raw).is_err());
    }

    #[test]
    fn duplicate_columns_within_unique_constraint_rejected() {
        let raw = r#"{
            "schema": "s",
            "name": "t",
            "columns": [
                {"name": "id", "type": {"kind": "big_int"}, "nullable": false, "primary_key": true},
                {"name": "x", "type": {"kind": "text"}, "nullable": true, "primary_key": false}
            ],
            "unique_constraints": [["x", "x"]]
        }"#;
        let err = TableSpec::from_json(raw).unwrap_err();
        assert!(err.to_string().contains("twice"));
    }

    #[test]
    fn fingerprint_changes_when_unique_constraints_change() {
        let base = r#"{
            "schema": "s",
            "name": "t",
            "columns": [
                {"name": "id", "type": {"kind": "big_int"}, "nullable": false, "primary_key": true},
                {"name": "x", "type": {"kind": "text"}, "nullable": true, "primary_key": false}
            ]
        }"#;
        let with_uc = r#"{
            "schema": "s",
            "name": "t",
            "columns": [
                {"name": "id", "type": {"kind": "big_int"}, "nullable": false, "primary_key": true},
                {"name": "x", "type": {"kind": "text"}, "nullable": true, "primary_key": false}
            ],
            "unique_constraints": [["x"]]
        }"#;
        let a = TableSpec::from_json(base).unwrap();
        let b = TableSpec::from_json(with_uc).unwrap();
        assert_ne!(a.fingerprint, b.fingerprint);
    }

    #[test]
    fn fingerprint_invariant_to_unique_constraint_declaration_order() {
        let order_a = r#"{
            "schema": "s",
            "name": "t",
            "columns": [
                {"name": "id", "type": {"kind": "big_int"}, "nullable": false, "primary_key": true},
                {"name": "x", "type": {"kind": "text"}, "nullable": true, "primary_key": false},
                {"name": "y", "type": {"kind": "text"}, "nullable": true, "primary_key": false}
            ],
            "unique_constraints": [["x"], ["y"]]
        }"#;
        let order_b = r#"{
            "schema": "s",
            "name": "t",
            "columns": [
                {"name": "id", "type": {"kind": "big_int"}, "nullable": false, "primary_key": true},
                {"name": "x", "type": {"kind": "text"}, "nullable": true, "primary_key": false},
                {"name": "y", "type": {"kind": "text"}, "nullable": true, "primary_key": false}
            ],
            "unique_constraints": [["y"], ["x"]]
        }"#;
        let a = TableSpec::from_json(order_a).unwrap();
        let b = TableSpec::from_json(order_b).unwrap();
        assert_eq!(a.fingerprint, b.fingerprint);
    }

    #[test]
    fn fingerprint_sensitive_to_column_order_within_unique_constraint() {
        // UNIQUE (a, b) and UNIQUE (b, a) are distinct indexes in Postgres.
        let ab = r#"{
            "schema": "s",
            "name": "t",
            "columns": [
                {"name": "id", "type": {"kind": "big_int"}, "nullable": false, "primary_key": true},
                {"name": "a", "type": {"kind": "text"}, "nullable": true, "primary_key": false},
                {"name": "b", "type": {"kind": "text"}, "nullable": true, "primary_key": false}
            ],
            "unique_constraints": [["a", "b"]]
        }"#;
        let ba = r#"{
            "schema": "s",
            "name": "t",
            "columns": [
                {"name": "id", "type": {"kind": "big_int"}, "nullable": false, "primary_key": true},
                {"name": "a", "type": {"kind": "text"}, "nullable": true, "primary_key": false},
                {"name": "b", "type": {"kind": "text"}, "nullable": true, "primary_key": false}
            ],
            "unique_constraints": [["b", "a"]]
        }"#;
        let a = TableSpec::from_json(ab).unwrap();
        let b = TableSpec::from_json(ba).unwrap();
        assert_ne!(a.fingerprint, b.fingerprint);
    }
}
