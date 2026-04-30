//! Phase 5: AppendOnly strategy planner.
//!
//! Produces SQL for the same-DB path (one INSERT...SELECT) and the
//! column-list metadata the executor needs to drive the cross-DB path
//! (COPY into a staging temp table, then INSERT...SELECT FROM stage).

use crate::types::{ColumnSpec, ColumnType, TableSpec};

pub const LOADED_AT_COL: &str = "_loaded_at";
pub const BATCH_ID_COL: &str = "_batch_id";

/// Output of the same-DB planner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendPlan {
    /// The single INSERT...SELECT statement to execute.
    pub sql: String,
    /// User-declared column names (excluding metadata) — these must be
    /// present in the source SELECT result.
    pub user_columns: Vec<String>,
    /// True when the target carries the `_loaded_at` + `_batch_id` columns;
    /// the executor must bind a UUID parameter for `$1`.
    pub has_metadata: bool,
}

/// Insert `_loaded_at` (TIMESTAMPTZ NOT NULL) and `_batch_id` (UUID NOT NULL)
/// at the end of the column list if the user did not declare them.
pub fn augment_with_metadata(spec: &TableSpec) -> TableSpec {
    let mut out = spec.clone();
    if !out.columns.iter().any(|c| c.name == LOADED_AT_COL) {
        out.columns.push(ColumnSpec {
            name: LOADED_AT_COL.into(),
            ty: ColumnType::TimestampTz,
            nullable: false,
            primary_key: false,
        });
    }
    if !out.columns.iter().any(|c| c.name == BATCH_ID_COL) {
        out.columns.push(ColumnSpec {
            name: BATCH_ID_COL.into(),
            ty: ColumnType::Uuid,
            nullable: false,
            primary_key: false,
        });
    }
    out
}

fn is_metadata_col(name: &str) -> bool {
    name == LOADED_AT_COL || name == BATCH_ID_COL
}

pub fn plan_same_db_append(target: &TableSpec, source_query: &str) -> AppendPlan {
    let user_columns: Vec<String> = target
        .columns
        .iter()
        .filter(|c| !is_metadata_col(&c.name))
        .map(|c| c.name.clone())
        .collect();
    let has_metadata = target.columns.iter().any(|c| is_metadata_col(&c.name));

    let mut insert_cols: Vec<String> = user_columns.clone();
    let mut select_exprs: Vec<String> = user_columns.clone();
    if has_metadata {
        insert_cols.push(LOADED_AT_COL.into());
        insert_cols.push(BATCH_ID_COL.into());
        select_exprs.push("now()".into());
        select_exprs.push("$1::uuid".into());
    }

    let sql = format!(
        "INSERT INTO {schema}.{table} ({insert_cols}) SELECT {select_exprs} FROM ({source}) src",
        schema = target.schema,
        table = target.name,
        insert_cols = insert_cols.join(", "),
        select_exprs = select_exprs.join(", "),
        source = source_query,
    );

    AppendPlan {
        sql,
        user_columns,
        has_metadata,
    }
}

#[cfg(test)]
mod tests {
    use crate::strategy::append::{
        BATCH_ID_COL, LOADED_AT_COL, augment_with_metadata, plan_same_db_append,
    };
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
            ],
            fingerprint: String::new(),
        }
    }

    #[test]
    fn augment_adds_loaded_at_and_batch_id() {
        let augmented = augment_with_metadata(&customer_dim());
        let names: Vec<&str> = augmented.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&LOADED_AT_COL));
        assert!(names.contains(&BATCH_ID_COL));
        // Originals preserved.
        assert!(names.contains(&"customer_id"));
        assert!(names.contains(&"email"));
    }

    #[test]
    fn augment_preserves_existing_primary_key() {
        let augmented = augment_with_metadata(&customer_dim());
        let pk_cols: Vec<&str> = augmented
            .columns
            .iter()
            .filter(|c| c.primary_key)
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(pk_cols, vec!["customer_id"]);
    }

    #[test]
    fn augment_metadata_columns_are_not_null() {
        let augmented = augment_with_metadata(&customer_dim());
        let loaded_at = augmented
            .columns
            .iter()
            .find(|c| c.name == LOADED_AT_COL)
            .unwrap();
        let batch_id = augmented
            .columns
            .iter()
            .find(|c| c.name == BATCH_ID_COL)
            .unwrap();
        assert!(!loaded_at.nullable);
        assert!(!batch_id.nullable);
        assert_eq!(loaded_at.ty, ColumnType::TimestampTz);
        assert_eq!(batch_id.ty, ColumnType::Uuid);
    }

    #[test]
    fn augment_is_idempotent() {
        let once = augment_with_metadata(&customer_dim());
        let twice = augment_with_metadata(&once);
        assert_eq!(once, twice);
    }

    #[test]
    fn augment_skips_when_user_declared_metadata_columns() {
        let mut spec = customer_dim();
        spec.columns.push(ColumnSpec {
            name: LOADED_AT_COL.into(),
            ty: ColumnType::TimestampTz,
            nullable: false,
            primary_key: false,
        });
        let augmented = augment_with_metadata(&spec);
        let loaded_at_count = augmented
            .columns
            .iter()
            .filter(|c| c.name == LOADED_AT_COL)
            .count();
        assert_eq!(loaded_at_count, 1);
    }

    #[test]
    fn same_db_plan_has_full_column_list() {
        let augmented = augment_with_metadata(&customer_dim());
        let plan = plan_same_db_append(&augmented, "SELECT * FROM src.customers");
        assert!(plan.sql.contains(
            "INSERT INTO warehouse.customer_dim (customer_id, email, _loaded_at, _batch_id)"
        ));
    }

    #[test]
    fn same_db_plan_select_matches_user_columns_plus_metadata_literals() {
        let augmented = augment_with_metadata(&customer_dim());
        let plan = plan_same_db_append(&augmented, "SELECT * FROM src.customers");
        // The SELECT picks user columns from the source subquery, then
        // appends literal `now()` and the `$1` batch_id parameter.
        assert!(
            plan.sql
                .contains("SELECT customer_id, email, now(), $1::uuid")
        );
        assert!(plan.sql.contains("FROM (SELECT * FROM src.customers) src"));
    }

    #[test]
    fn same_db_plan_skips_metadata_when_target_omits_them() {
        // If a user has explicitly omitted metadata via no augmentation,
        // the planner should still produce a working INSERT.
        let plan = plan_same_db_append(&customer_dim(), "SELECT * FROM src.customers");
        assert!(
            plan.sql
                .contains("INSERT INTO warehouse.customer_dim (customer_id, email)")
        );
        assert!(plan.sql.contains("SELECT customer_id, email"));
        assert!(!plan.sql.contains("$1::uuid"));
    }

    #[test]
    fn same_db_plan_reports_user_columns_and_param_count() {
        let augmented = augment_with_metadata(&customer_dim());
        let plan = plan_same_db_append(&augmented, "SELECT * FROM src.customers");
        assert_eq!(plan.user_columns, vec!["customer_id", "email"]);
        assert!(plan.has_metadata);
    }

    #[test]
    fn same_db_plan_quotes_source_literally() {
        // We don't try to "fix" a user's source query; we wrap it as-is.
        let plan = plan_same_db_append(
            &augment_with_metadata(&customer_dim()),
            "SELECT id AS customer_id, lower(e) AS email FROM stale_customers WHERE id > 100",
        );
        assert!(plan.sql.contains(
            "FROM (SELECT id AS customer_id, lower(e) AS email FROM stale_customers WHERE id > 100) src"
        ));
    }
}
