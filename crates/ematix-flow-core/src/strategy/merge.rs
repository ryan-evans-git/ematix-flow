//! Phase 7: MergeUpsert / SCD1 planner.
//!
//! Same-DB plan: a single CTE-wrapped INSERT...ON CONFLICT DO UPDATE, with
//! a `WHERE target.* IS DISTINCT FROM EXCLUDED.*` so unchanged rows are
//! filtered out — that makes the affected-row counts (inserted/updated/
//! unchanged) meaningful. The outer SELECT returns three counts in one row.
//!
//! Cross-DB plan: identical SQL, but the source is a temp staging table
//! that the executor populates via COPY (binary).

use crate::strategy::append::{BATCH_ID_COL, LOADED_AT_COL};
use crate::types::TableSpec;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergePlan {
    /// Single CTE-wrapped query that returns one row of (inserted, updated,
    /// total) — caller computes unchanged = total − inserted − updated.
    pub sql: String,
    /// True when the target carries `_loaded_at`/`_batch_id`; the executor
    /// must bind a UUID parameter for `$1`.
    pub has_metadata: bool,
}

fn is_metadata_col(name: &str) -> bool {
    name == LOADED_AT_COL || name == BATCH_ID_COL
}

/// Build the merge upsert query.
///
/// `keys` are the natural-key columns (the `ON CONFLICT (...)` target).
/// `update_columns` is the list of non-key, non-metadata columns to compare
/// and update; pass `&[]` for an insert-only ON CONFLICT DO NOTHING plan.
pub fn plan_merge_upsert(
    target: &TableSpec,
    source_query: &str,
    keys: &[String],
    update_columns: &[String],
) -> MergePlan {
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

    // The src CTE projects only the user columns we care about.
    let src_cte = format!(
        "src AS MATERIALIZED (SELECT {cols} FROM ({source}) src_inner)",
        cols = user_columns.join(", "),
        source = source_query,
    );

    let on_conflict_clause = if update_columns.is_empty() {
        format!("ON CONFLICT ({}) DO NOTHING", keys.join(", "))
    } else {
        let mut set_pairs: Vec<String> = update_columns
            .iter()
            .map(|c| format!("{c} = EXCLUDED.{c}"))
            .collect();
        if has_metadata {
            set_pairs.push(format!("{LOADED_AT_COL} = EXCLUDED.{LOADED_AT_COL}"));
            set_pairs.push(format!("{BATCH_ID_COL} = EXCLUDED.{BATCH_ID_COL}"));
        }
        let target_tuple: String = update_columns
            .iter()
            .map(|c| format!("t.{c}"))
            .collect::<Vec<_>>()
            .join(", ");
        let excluded_tuple: String = update_columns
            .iter()
            .map(|c| format!("EXCLUDED.{c}"))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "ON CONFLICT ({keys}) DO UPDATE SET {sets} \
             WHERE ({target_tuple}) IS DISTINCT FROM ({excluded_tuple})",
            keys = keys.join(", "),
            sets = set_pairs.join(", "),
        )
    };

    let upsert_cte = format!(
        "upsert AS (\
             INSERT INTO {schema}.{table} AS t ({insert_cols}) \
             SELECT {select_exprs} FROM src \
             {on_conflict} \
             RETURNING (xmax = 0) AS was_inserted\
         )",
        schema = target.schema,
        table = target.name,
        insert_cols = insert_cols.join(", "),
        select_exprs = select_exprs.join(", "),
        on_conflict = on_conflict_clause,
    );

    let sql = format!(
        "WITH {src_cte}, {upsert_cte} \
         SELECT \
             count(*) FILTER (WHERE was_inserted)::bigint AS inserted, \
             count(*) FILTER (WHERE NOT was_inserted)::bigint AS updated, \
             (SELECT count(*) FROM src)::bigint AS total \
         FROM upsert"
    );

    MergePlan { sql, has_metadata }
}

#[cfg(test)]
mod tests {
    use crate::strategy::append::augment_with_metadata;
    use crate::strategy::merge::plan_merge_upsert;
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
                    name: "name".into(),
                    ty: ColumnType::Text,
                    nullable: false,
                    primary_key: false,
                },
            ],
            fingerprint: String::new(),
        }
    }

    #[test]
    fn merge_plan_emits_on_conflict_do_update() {
        let augmented = augment_with_metadata(&customer_dim());
        let plan = plan_merge_upsert(
            &augmented,
            "SELECT * FROM src.customers",
            &["customer_id".into()],
            &["email".into(), "name".into()],
        );
        assert!(plan.sql.contains("INSERT INTO warehouse.customer_dim AS t"));
        assert!(plan.sql.contains("ON CONFLICT (customer_id) DO UPDATE"));
        assert!(plan.sql.contains("email = EXCLUDED.email"));
        assert!(plan.sql.contains("name = EXCLUDED.name"));
    }

    #[test]
    fn merge_plan_filters_unchanged_with_is_distinct_from() {
        let augmented = augment_with_metadata(&customer_dim());
        let plan = plan_merge_upsert(
            &augmented,
            "SELECT * FROM src",
            &["customer_id".into()],
            &["email".into(), "name".into()],
        );
        // The WHERE clause compares target tuple vs EXCLUDED tuple over the
        // declared update columns only (NOT metadata, NOT keys).
        assert!(
            plan.sql.contains(
                "WHERE (t.email, t.name) IS DISTINCT FROM (EXCLUDED.email, EXCLUDED.name)"
            )
        );
    }

    #[test]
    fn merge_plan_updates_metadata_columns_too() {
        // Metadata cols update on a real change so the row reflects the
        // latest sync, but they are NOT in the WHERE comparison.
        let augmented = augment_with_metadata(&customer_dim());
        let plan = plan_merge_upsert(
            &augmented,
            "SELECT * FROM src",
            &["customer_id".into()],
            &["email".into(), "name".into()],
        );
        assert!(plan.sql.contains("_loaded_at = EXCLUDED._loaded_at"));
        assert!(plan.sql.contains("_batch_id = EXCLUDED._batch_id"));
        assert!(!plan.sql.contains("t._loaded_at"));
    }

    #[test]
    fn merge_plan_returns_three_counts() {
        let augmented = augment_with_metadata(&customer_dim());
        let plan = plan_merge_upsert(
            &augmented,
            "SELECT * FROM src",
            &["customer_id".into()],
            &["email".into(), "name".into()],
        );
        // The CTE-wrapped query returns inserted, updated, and total
        // (unchanged is computed by the executor as total - inserted - updated).
        assert!(plan.sql.contains("RETURNING (xmax = 0) AS was_inserted"));
        assert!(plan.sql.contains("count(*) FILTER (WHERE was_inserted)"));
        assert!(
            plan.sql
                .contains("count(*) FILTER (WHERE NOT was_inserted)")
        );
    }

    #[test]
    fn merge_plan_handles_composite_keys() {
        let mut spec = customer_dim();
        spec.columns[1].primary_key = true; // make email also a PK
        let augmented = augment_with_metadata(&spec);
        let plan = plan_merge_upsert(
            &augmented,
            "SELECT * FROM src",
            &["customer_id".into(), "email".into()],
            &["name".into()],
        );
        assert!(
            plan.sql
                .contains("ON CONFLICT (customer_id, email) DO UPDATE")
        );
    }

    #[test]
    fn merge_plan_falls_back_to_do_nothing_when_no_update_cols() {
        // When the user has no non-key non-metadata columns, there is
        // nothing to update; ON CONFLICT DO NOTHING is the right move.
        let spec = TableSpec {
            schema: "s".into(),
            name: "t".into(),
            columns: vec![ColumnSpec {
                name: "id".into(),
                ty: ColumnType::BigInt,
                nullable: false,
                primary_key: true,
            }],
            fingerprint: String::new(),
        };
        let augmented = augment_with_metadata(&spec);
        let plan = plan_merge_upsert(&augmented, "SELECT * FROM src", &["id".into()], &[]);
        assert!(plan.sql.contains("ON CONFLICT (id) DO NOTHING"));
        assert!(!plan.sql.contains("DO UPDATE"));
    }

    #[test]
    fn merge_plan_carries_metadata_flag() {
        let augmented = augment_with_metadata(&customer_dim());
        let plan = plan_merge_upsert(
            &augmented,
            "SELECT * FROM src",
            &["customer_id".into()],
            &["email".into()],
        );
        assert!(plan.has_metadata);
    }

    #[test]
    fn merge_plan_inserts_full_column_list() {
        let augmented = augment_with_metadata(&customer_dim());
        let plan = plan_merge_upsert(
            &augmented,
            "SELECT * FROM src",
            &["customer_id".into()],
            &["email".into(), "name".into()],
        );
        assert!(
            plan.sql
                .contains("(customer_id, email, name, _loaded_at, _batch_id)")
        );
    }
}
