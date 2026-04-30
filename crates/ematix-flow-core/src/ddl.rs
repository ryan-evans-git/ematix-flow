//! Phase 4: DDL planner — emits `CREATE TABLE` SQL from a `TableSpec` and
//! compares a declared spec against a reflected one to detect drift.

use std::collections::HashMap;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::types::{ColumnType, TableSpec};

/// Reflected column read back from `information_schema`. Mirrors `ColumnSpec`
/// in shape, but built from the live database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReflectedColumn {
    pub name: String,
    pub ty: ColumnType,
    pub nullable: bool,
    pub primary_key: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum Difference {
    ColumnMissing {
        name: String,
    },
    ColumnExtra {
        name: String,
    },
    TypeMismatch {
        name: String,
        declared: String,
        reflected: String,
    },
    NullabilityMismatch {
        name: String,
        declared: bool,
        reflected: bool,
    },
    PrimaryKeyMismatch {
        name: String,
        declared: bool,
        reflected: bool,
    },
    UniqueConstraintMissing {
        columns: Vec<String>,
    },
    UniqueConstraintExtra {
        columns: Vec<String>,
    },
}

impl fmt::Display for Difference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Difference::ColumnMissing { name } => {
                write!(f, "column `{name}` is declared but missing in the table")
            }
            Difference::ColumnExtra { name } => {
                write!(f, "column `{name}` exists in the table but is not declared")
            }
            Difference::TypeMismatch {
                name,
                declared,
                reflected,
            } => write!(
                f,
                "column `{name}` type mismatch: declared {declared}, reflected {reflected}"
            ),
            Difference::NullabilityMismatch {
                name,
                declared,
                reflected,
            } => write!(
                f,
                "column `{name}` nullability mismatch: declared {}, reflected {}",
                if *declared { "NULL" } else { "NOT NULL" },
                if *reflected { "NULL" } else { "NOT NULL" },
            ),
            Difference::PrimaryKeyMismatch {
                name,
                declared,
                reflected,
            } => write!(
                f,
                "column `{name}` primary-key mismatch: declared {declared}, reflected {reflected}"
            ),
            Difference::UniqueConstraintMissing { columns } => write!(
                f,
                "unique constraint ({}) is declared but missing in the table",
                columns.join(", ")
            ),
            Difference::UniqueConstraintExtra { columns } => write!(
                f,
                "unique constraint ({}) exists in the table but is not declared",
                columns.join(", ")
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DriftResult {
    Match,
    Drift(Vec<Difference>),
}

pub fn create_table_sql(spec: &TableSpec) -> String {
    let mut out = String::new();
    out.push_str(&format!("CREATE TABLE {}.{} (\n", spec.schema, spec.name));
    let lines: Vec<String> = spec
        .columns
        .iter()
        .map(|c| {
            let mut line = format!("    {} {}", c.name, c.ty.to_postgres_sql());
            if !c.nullable {
                line.push_str(" NOT NULL");
            }
            line
        })
        .collect();
    out.push_str(&lines.join(",\n"));
    let pks: Vec<&str> = spec
        .columns
        .iter()
        .filter(|c| c.primary_key)
        .map(|c| c.name.as_str())
        .collect();
    if !pks.is_empty() {
        out.push_str(",\n    PRIMARY KEY (");
        out.push_str(&pks.join(", "));
        out.push(')');
    }
    for uc in &spec.unique_constraints {
        out.push_str(",\n    UNIQUE (");
        out.push_str(&uc.join(", "));
        out.push(')');
    }
    out.push_str("\n)");
    out
}

pub fn compare_table(declared: &TableSpec, reflected: &[ReflectedColumn]) -> DriftResult {
    compare_table_with_uniques(declared, reflected, &[])
}

/// Phase 22: extended `compare_table` that also flags differences in
/// UNIQUE constraints. The set comparison ignores constraint-list
/// ordering across constraints but preserves column order *within* each
/// constraint (Postgres treats `UNIQUE (a, b)` and `UNIQUE (b, a)` as
/// distinct indexes).
pub fn compare_table_with_uniques(
    declared: &TableSpec,
    reflected_columns: &[ReflectedColumn],
    reflected_uniques: &[Vec<String>],
) -> DriftResult {
    let mut diffs = Vec::new();
    let reflected_by_name: HashMap<&str, &ReflectedColumn> = reflected_columns
        .iter()
        .map(|c| (c.name.as_str(), c))
        .collect();
    let declared_names: std::collections::HashSet<&str> =
        declared.columns.iter().map(|c| c.name.as_str()).collect();
    let reflected = reflected_columns;

    for col in &declared.columns {
        match reflected_by_name.get(col.name.as_str()) {
            None => diffs.push(Difference::ColumnMissing {
                name: col.name.clone(),
            }),
            Some(r) => {
                if col.ty != r.ty {
                    diffs.push(Difference::TypeMismatch {
                        name: col.name.clone(),
                        declared: col.ty.to_postgres_sql(),
                        reflected: r.ty.to_postgres_sql(),
                    });
                }
                if col.nullable != r.nullable {
                    diffs.push(Difference::NullabilityMismatch {
                        name: col.name.clone(),
                        declared: col.nullable,
                        reflected: r.nullable,
                    });
                }
                if col.primary_key != r.primary_key {
                    diffs.push(Difference::PrimaryKeyMismatch {
                        name: col.name.clone(),
                        declared: col.primary_key,
                        reflected: r.primary_key,
                    });
                }
            }
        }
    }

    for r in reflected {
        if !declared_names.contains(r.name.as_str()) {
            diffs.push(Difference::ColumnExtra {
                name: r.name.clone(),
            });
        }
    }

    // Phase 22: compare UNIQUE constraints as a set (column order within
    // a constraint is significant; constraint declaration order is not).
    let declared_uniques: std::collections::HashSet<&[String]> = declared
        .unique_constraints
        .iter()
        .map(|v| v.as_slice())
        .collect();
    let reflected_uniques_set: std::collections::HashSet<&[String]> =
        reflected_uniques.iter().map(|v| v.as_slice()).collect();
    for uc in &declared.unique_constraints {
        if !reflected_uniques_set.contains(uc.as_slice()) {
            diffs.push(Difference::UniqueConstraintMissing {
                columns: uc.clone(),
            });
        }
    }
    for uc in reflected_uniques {
        if !declared_uniques.contains(uc.as_slice()) {
            diffs.push(Difference::UniqueConstraintExtra {
                columns: uc.clone(),
            });
        }
    }

    if diffs.is_empty() {
        DriftResult::Match
    } else {
        DriftResult::Drift(diffs)
    }
}

/// Convert raw `information_schema` column metadata into a `ColumnType`.
/// Returns `None` for types we don't model — caller decides whether to
/// surface that as drift or as an unsupported-type error.
pub fn canonicalize_reflected_type(
    data_type: &str,
    char_max: Option<i32>,
    num_precision: Option<i32>,
    num_scale: Option<i32>,
) -> Option<ColumnType> {
    match data_type {
        "smallint" => Some(ColumnType::SmallInt),
        "integer" => Some(ColumnType::Integer),
        "bigint" => Some(ColumnType::BigInt),
        "real" => Some(ColumnType::Float),
        "double precision" => Some(ColumnType::Double),
        "numeric" => match (num_precision, num_scale) {
            (Some(p), Some(s)) if p > 0 && (0..=u8::MAX as i32).contains(&p) => {
                Some(ColumnType::Numeric {
                    precision: p as u8,
                    scale: s.max(0) as u8,
                })
            }
            _ => None,
        },
        "boolean" => Some(ColumnType::Boolean),
        "text" => Some(ColumnType::Text),
        "character varying" => match char_max {
            Some(n) if n > 0 => Some(ColumnType::String { length: n as u32 }),
            // Unbounded VARCHAR is functionally TEXT; treat as TEXT for drift.
            _ => Some(ColumnType::Text),
        },
        "date" => Some(ColumnType::Date),
        "timestamp without time zone" => Some(ColumnType::Timestamp),
        "timestamp with time zone" => Some(ColumnType::TimestampTz),
        "json" => Some(ColumnType::Json),
        "jsonb" => Some(ColumnType::Jsonb),
        "uuid" => Some(ColumnType::Uuid),
        "bytea" => Some(ColumnType::Bytes),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use crate::ddl::{Difference, DriftResult, ReflectedColumn, compare_table, create_table_sql};
    use crate::types::{ColumnSpec, ColumnType, TableSpec};

    fn customer_dim() -> TableSpec {
        TableSpec {
            schema: "warehouse".into(),
            name: "customer_dim".into(),
            columns: vec![
                ColumnSpec {
                    name: "customer_id".into(),
                    ty: ColumnType::BigInt,
                    nullable: false,
                    primary_key: true,
                },
                ColumnSpec {
                    name: "email".into(),
                    ty: ColumnType::String { length: 256 },
                    nullable: false,
                    primary_key: false,
                },
                ColumnSpec {
                    name: "balance".into(),
                    ty: ColumnType::Numeric {
                        precision: 12,
                        scale: 2,
                    },
                    nullable: true,
                    primary_key: false,
                },
                ColumnSpec {
                    name: "created_at".into(),
                    ty: ColumnType::TimestampTz,
                    nullable: false,
                    primary_key: false,
                },
            ],
            unique_constraints: Vec::new(),
            fingerprint: String::new(),
        }
    }

    #[test]
    fn create_table_sql_emits_schema_qualified_ddl() {
        let sql = create_table_sql(&customer_dim());
        assert!(sql.contains("CREATE TABLE warehouse.customer_dim"));
        assert!(sql.contains("customer_id BIGINT NOT NULL"));
        assert!(sql.contains("email VARCHAR(256) NOT NULL"));
        assert!(sql.contains("balance NUMERIC(12,2)"));
        assert!(!sql.contains("balance NUMERIC(12,2) NOT NULL"));
        assert!(sql.contains("created_at TIMESTAMPTZ NOT NULL"));
        assert!(sql.contains("PRIMARY KEY (customer_id)"));
    }

    #[test]
    fn create_table_sql_emits_composite_primary_key() {
        let spec = TableSpec {
            schema: "s".into(),
            name: "t".into(),
            columns: vec![
                ColumnSpec {
                    name: "a".into(),
                    ty: ColumnType::Integer,
                    nullable: false,
                    primary_key: true,
                },
                ColumnSpec {
                    name: "b".into(),
                    ty: ColumnType::Integer,
                    nullable: false,
                    primary_key: true,
                },
                ColumnSpec {
                    name: "v".into(),
                    ty: ColumnType::Text,
                    nullable: true,
                    primary_key: false,
                },
            ],
            unique_constraints: Vec::new(),
            fingerprint: String::new(),
        };
        let sql = create_table_sql(&spec);
        assert!(sql.contains("PRIMARY KEY (a, b)"));
    }

    fn reflected_match() -> Vec<ReflectedColumn> {
        vec![
            ReflectedColumn {
                name: "customer_id".into(),
                ty: ColumnType::BigInt,
                nullable: false,
                primary_key: true,
            },
            ReflectedColumn {
                name: "email".into(),
                ty: ColumnType::String { length: 256 },
                nullable: false,
                primary_key: false,
            },
            ReflectedColumn {
                name: "balance".into(),
                ty: ColumnType::Numeric {
                    precision: 12,
                    scale: 2,
                },
                nullable: true,
                primary_key: false,
            },
            ReflectedColumn {
                name: "created_at".into(),
                ty: ColumnType::TimestampTz,
                nullable: false,
                primary_key: false,
            },
        ]
    }

    #[test]
    fn compare_table_match() {
        assert_eq!(
            compare_table(&customer_dim(), &reflected_match()),
            DriftResult::Match
        );
    }

    #[test]
    fn compare_table_detects_missing_column() {
        let mut reflected = reflected_match();
        reflected.retain(|c| c.name != "balance");
        let result = compare_table(&customer_dim(), &reflected);
        match result {
            DriftResult::Drift(diffs) => {
                assert!(
                    diffs.iter().any(
                        |d| matches!(d, Difference::ColumnMissing { name } if name == "balance"),
                    )
                );
            }
            other => panic!("expected Drift, got {other:?}"),
        }
    }

    #[test]
    fn compare_table_detects_extra_column() {
        let mut reflected = reflected_match();
        reflected.push(ReflectedColumn {
            name: "stowaway".into(),
            ty: ColumnType::Text,
            nullable: true,
            primary_key: false,
        });
        let result = compare_table(&customer_dim(), &reflected);
        match result {
            DriftResult::Drift(diffs) => {
                assert!(
                    diffs.iter().any(
                        |d| matches!(d, Difference::ColumnExtra { name } if name == "stowaway"),
                    )
                );
            }
            other => panic!("expected Drift, got {other:?}"),
        }
    }

    #[test]
    fn compare_table_detects_type_mismatch() {
        let mut reflected = reflected_match();
        reflected[0].ty = ColumnType::Integer;
        let result = compare_table(&customer_dim(), &reflected);
        match result {
            DriftResult::Drift(diffs) => {
                assert!(diffs.iter().any(|d| matches!(
                    d,
                    Difference::TypeMismatch { name, .. } if name == "customer_id"
                )));
            }
            other => panic!("expected Drift, got {other:?}"),
        }
    }

    #[test]
    fn compare_table_detects_nullability_change() {
        let mut reflected = reflected_match();
        reflected[3].nullable = true; // declared NOT NULL, reflected NULL
        let result = compare_table(&customer_dim(), &reflected);
        match result {
            DriftResult::Drift(diffs) => {
                assert!(diffs.iter().any(|d| matches!(
                    d,
                    Difference::NullabilityMismatch { name, .. } if name == "created_at"
                )));
            }
            other => panic!("expected Drift, got {other:?}"),
        }
    }

    #[test]
    fn compare_table_detects_primary_key_change() {
        let mut reflected = reflected_match();
        reflected[0].primary_key = false;
        let result = compare_table(&customer_dim(), &reflected);
        match result {
            DriftResult::Drift(diffs) => {
                assert!(diffs.iter().any(|d| matches!(
                    d,
                    Difference::PrimaryKeyMismatch { name, .. } if name == "customer_id"
                )));
            }
            other => panic!("expected Drift, got {other:?}"),
        }
    }

    #[test]
    fn compare_table_missing_when_reflected_empty() {
        // Empty reflected = table doesn't exist (caller's job to convey).
        // Here we test that compare_table on empty reflected reports every
        // declared column as missing.
        let result = compare_table(&customer_dim(), &[]);
        match result {
            DriftResult::Drift(diffs) => {
                assert_eq!(diffs.len(), 4);
                assert!(
                    diffs
                        .iter()
                        .all(|d| matches!(d, Difference::ColumnMissing { .. }))
                );
            }
            other => panic!("expected Drift, got {other:?}"),
        }
    }

    #[test]
    fn drift_messages_are_human_readable() {
        let mut reflected = reflected_match();
        reflected[0].ty = ColumnType::Integer;
        let result = compare_table(&customer_dim(), &reflected);
        match result {
            DriftResult::Drift(diffs) => {
                let msg = diffs[0].to_string();
                assert!(msg.contains("customer_id"));
                assert!(msg.contains("BIGINT"));
                assert!(msg.contains("INTEGER"));
            }
            other => panic!("expected Drift, got {other:?}"),
        }
    }

    // --- Phase 22: UNIQUE constraints ------------------------------------

    use crate::ddl::compare_table_with_uniques;

    fn customer_order_with_unique() -> TableSpec {
        TableSpec {
            schema: "warehouse".into(),
            name: "customer_order".into(),
            columns: vec![
                ColumnSpec {
                    name: "id".into(),
                    ty: ColumnType::BigInt,
                    nullable: false,
                    primary_key: true,
                },
                ColumnSpec {
                    name: "customer_id".into(),
                    ty: ColumnType::BigInt,
                    nullable: false,
                    primary_key: false,
                },
                ColumnSpec {
                    name: "order_date".into(),
                    ty: ColumnType::Date,
                    nullable: false,
                    primary_key: false,
                },
            ],
            unique_constraints: vec![vec!["customer_id".into(), "order_date".into()]],
            fingerprint: String::new(),
        }
    }

    #[test]
    fn create_table_sql_emits_single_unique_clause() {
        let sql = create_table_sql(&customer_order_with_unique());
        assert!(sql.contains("PRIMARY KEY (id)"));
        assert!(sql.contains("UNIQUE (customer_id, order_date)"));
    }

    #[test]
    fn create_table_sql_emits_multiple_unique_clauses() {
        let mut spec = customer_order_with_unique();
        spec.columns.push(ColumnSpec {
            name: "external_ref".into(),
            ty: ColumnType::Text,
            nullable: true,
            primary_key: false,
        });
        spec.unique_constraints.push(vec!["external_ref".into()]);
        let sql = create_table_sql(&spec);
        assert!(sql.contains("UNIQUE (customer_id, order_date)"));
        assert!(sql.contains("UNIQUE (external_ref)"));
    }

    #[test]
    fn compare_table_match_when_uniques_align() {
        let declared = customer_order_with_unique();
        let reflected_cols: Vec<ReflectedColumn> = declared
            .columns
            .iter()
            .map(|c| ReflectedColumn {
                name: c.name.clone(),
                ty: c.ty.clone(),
                nullable: c.nullable,
                primary_key: c.primary_key,
            })
            .collect();
        let reflected_uniques = vec![vec!["customer_id".into(), "order_date".into()]];
        assert_eq!(
            compare_table_with_uniques(&declared, &reflected_cols, &reflected_uniques),
            DriftResult::Match
        );
    }

    #[test]
    fn compare_table_detects_missing_unique_constraint() {
        let declared = customer_order_with_unique();
        let reflected_cols: Vec<ReflectedColumn> = declared
            .columns
            .iter()
            .map(|c| ReflectedColumn {
                name: c.name.clone(),
                ty: c.ty.clone(),
                nullable: c.nullable,
                primary_key: c.primary_key,
            })
            .collect();
        // Live DB has no UNIQUE constraint.
        let result = compare_table_with_uniques(&declared, &reflected_cols, &[]);
        match result {
            DriftResult::Drift(diffs) => {
                assert!(diffs.iter().any(|d| matches!(
                    d,
                    Difference::UniqueConstraintMissing { columns }
                        if columns == &vec!["customer_id".to_string(), "order_date".to_string()]
                )));
            }
            other => panic!("expected Drift, got {other:?}"),
        }
    }

    #[test]
    fn compare_table_detects_extra_unique_constraint() {
        let mut declared = customer_order_with_unique();
        declared.unique_constraints.clear();
        let reflected_cols: Vec<ReflectedColumn> = declared
            .columns
            .iter()
            .map(|c| ReflectedColumn {
                name: c.name.clone(),
                ty: c.ty.clone(),
                nullable: c.nullable,
                primary_key: c.primary_key,
            })
            .collect();
        let reflected_uniques = vec![vec!["customer_id".into()]];
        let result = compare_table_with_uniques(&declared, &reflected_cols, &reflected_uniques);
        match result {
            DriftResult::Drift(diffs) => {
                assert!(diffs.iter().any(|d| matches!(
                    d,
                    Difference::UniqueConstraintExtra { columns }
                        if columns == &vec!["customer_id".to_string()]
                )));
            }
            other => panic!("expected Drift, got {other:?}"),
        }
    }

    #[test]
    fn compare_table_unique_match_ignores_constraint_order() {
        let mut declared = customer_order_with_unique();
        declared.unique_constraints.push(vec!["id".into()]);
        let reflected_cols: Vec<ReflectedColumn> = declared
            .columns
            .iter()
            .map(|c| ReflectedColumn {
                name: c.name.clone(),
                ty: c.ty.clone(),
                nullable: c.nullable,
                primary_key: c.primary_key,
            })
            .collect();
        // Live DB has the same uniques but in reversed order.
        let reflected_uniques = vec![
            vec!["id".into()],
            vec!["customer_id".into(), "order_date".into()],
        ];
        assert_eq!(
            compare_table_with_uniques(&declared, &reflected_cols, &reflected_uniques),
            DriftResult::Match
        );
    }

    #[test]
    fn compare_table_unique_treats_column_order_as_significant() {
        let declared = customer_order_with_unique();
        let reflected_cols: Vec<ReflectedColumn> = declared
            .columns
            .iter()
            .map(|c| ReflectedColumn {
                name: c.name.clone(),
                ty: c.ty.clone(),
                nullable: c.nullable,
                primary_key: c.primary_key,
            })
            .collect();
        // Live DB has UNIQUE (order_date, customer_id) — reversed.
        let reflected_uniques = vec![vec!["order_date".into(), "customer_id".into()]];
        let result = compare_table_with_uniques(&declared, &reflected_cols, &reflected_uniques);
        match result {
            DriftResult::Drift(diffs) => {
                assert!(
                    diffs
                        .iter()
                        .any(|d| matches!(d, Difference::UniqueConstraintMissing { .. }))
                );
                assert!(
                    diffs
                        .iter()
                        .any(|d| matches!(d, Difference::UniqueConstraintExtra { .. }))
                );
            }
            other => panic!("expected Drift, got {other:?}"),
        }
    }
}
