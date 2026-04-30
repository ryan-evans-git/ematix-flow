//! Phase 8: SCD2 planner.
//!
//! Augments the user's TableSpec with `valid_from`, `valid_to`,
//! `is_current`, `row_hash` and standard run metadata. Emits three
//! statements run inside one transaction:
//!   1. CREATE TEMP TABLE `_scd2_changed_<run_id>` holding source rows
//!      whose row_hash differs from the current version's.
//!   2. UPDATE the existing current rows for those keys (close-out).
//!   3. INSERT new current rows from the temp table.
//!
//! The same plan works against a real-table source (same-DB) or a
//! `_ematix_stage_*` temp table (cross-DB).

use crate::hash::postgres_digest_expression;
use crate::strategy::append::{BATCH_ID_COL, LOADED_AT_COL, augment_with_metadata};
use crate::types::{ColumnSpec, ColumnType, TableSpec};

pub const VALID_FROM_COL: &str = "valid_from";
pub const VALID_TO_COL: &str = "valid_to";
pub const IS_CURRENT_COL: &str = "is_current";
pub const ROW_HASH_COL: &str = "row_hash";

pub fn is_scd2_metadata(name: &str) -> bool {
    matches!(
        name,
        VALID_FROM_COL | VALID_TO_COL | IS_CURRENT_COL | ROW_HASH_COL
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scd2Plan {
    /// Three statements run in one transaction:
    ///   0. CREATE TEMP TABLE _scd2_changed_<id> AS WITH src+digest ...
    ///   1. UPDATE target SET valid_to=now(), is_current=false WHERE keys IN ...
    ///   2. INSERT INTO target ... SELECT ... FROM _scd2_changed_<id>
    pub statements: Vec<String>,
    /// True when the target carries `_loaded_at`/`_batch_id`; the executor
    /// must bind a UUID parameter for `$1` to statement 2.
    pub has_metadata: bool,
}

pub fn augment_with_scd2(spec: &TableSpec) -> TableSpec {
    let mut out = spec.clone();
    if !out.columns.iter().any(|c| c.name == VALID_FROM_COL) {
        out.columns.push(ColumnSpec {
            name: VALID_FROM_COL.into(),
            ty: ColumnType::TimestampTz,
            nullable: false,
            // valid_from joins the natural keys to form the composite PK so
            // multiple versions per natural key can coexist.
            primary_key: true,
        });
    }
    if !out.columns.iter().any(|c| c.name == VALID_TO_COL) {
        out.columns.push(ColumnSpec {
            name: VALID_TO_COL.into(),
            ty: ColumnType::TimestampTz,
            nullable: true,
            primary_key: false,
        });
    }
    if !out.columns.iter().any(|c| c.name == IS_CURRENT_COL) {
        out.columns.push(ColumnSpec {
            name: IS_CURRENT_COL.into(),
            ty: ColumnType::Boolean,
            nullable: false,
            primary_key: false,
        });
    }
    if !out.columns.iter().any(|c| c.name == ROW_HASH_COL) {
        out.columns.push(ColumnSpec {
            name: ROW_HASH_COL.into(),
            ty: ColumnType::Bytes,
            nullable: false,
            primary_key: false,
        });
    }
    augment_with_metadata(&out)
}

fn is_user_data_col(name: &str) -> bool {
    !is_scd2_metadata(name) && name != LOADED_AT_COL && name != BATCH_ID_COL
}

/// Build the SCD2 plan against `source_query` (a SELECT or a staging temp
/// table). `keys` are the natural-key columns for the LEFT JOIN to current
/// versions. `compare_columns` participate in the row hash (changes here
/// produce new versions). `run_token` distinguishes the temp-table name
/// across concurrent runs in the same session.
///
/// Phase 15: when `event_ts_column = Some(col)`, the plan switches to
/// event-time semantics — `valid_from` is taken from that source column
/// instead of `now()`, and the close-out's `valid_to` is set to the new
/// version's event_ts (so a previous version's `valid_to` equals its
/// successor's `valid_from`). The src CTE uses `DISTINCT ON (keys) ORDER
/// BY event_ts DESC` so multiple events for the same key in one source
/// resolve to the latest. Out-of-order arrivals (event_ts older than the
/// existing current version's `valid_from`) are caught at execution time
/// in a separate guard query — not embedded in the plan.
pub fn plan_scd2(
    target: &TableSpec,
    source_query: &str,
    keys: &[String],
    compare_columns: &[String],
    run_token: &str,
    event_ts_column: Option<&str>,
) -> Scd2Plan {
    let user_columns: Vec<String> = target
        .columns
        .iter()
        .filter(|c| is_user_data_col(&c.name))
        .map(|c| c.name.clone())
        .collect();
    let has_metadata = target
        .columns
        .iter()
        .any(|c| c.name == LOADED_AT_COL || c.name == BATCH_ID_COL);

    let stage = format!("_scd2_changed_{run_token}");

    let digest_expr = postgres_digest_expression(compare_columns, "");
    let t_tuple: String = keys
        .iter()
        .map(|k| format!("t.{k}"))
        .collect::<Vec<_>>()
        .join(", ");
    let src_tuple: String = keys
        .iter()
        .map(|k| format!("src.{k}"))
        .collect::<Vec<_>>()
        .join(", ");
    let join_clause = format!("({t_tuple}) = ({src_tuple})");
    let key_tuple: String = keys.join(", ");

    let src_select = match event_ts_column {
        Some(ets) => format!(
            "SELECT DISTINCT ON ({keys}) {user_cols}, \
             {ets}::timestamptz AS _event_ts, \
             {digest_expr} AS _row_hash \
             FROM ({source}) q \
             ORDER BY {keys}, {ets} DESC",
            keys = key_tuple,
            user_cols = user_columns.join(", "),
            ets = ets,
            digest_expr = digest_expr,
            source = source_query,
        ),
        None => format!(
            "SELECT {user_cols}, {digest_expr} AS _row_hash FROM ({source}) q",
            user_cols = user_columns.join(", "),
            digest_expr = digest_expr,
            source = source_query,
        ),
    };

    let create_temp = format!(
        "CREATE TEMP TABLE {stage} ON COMMIT DROP AS \
         WITH src AS MATERIALIZED ( {src_select} ) \
         SELECT src.* FROM src \
         LEFT JOIN {schema}.{table} t \
             ON {join_clause} AND t.{is_current_col} \
         WHERE t.{row_hash_col} IS DISTINCT FROM src._row_hash",
        stage = stage,
        src_select = src_select,
        schema = target.schema,
        table = target.name,
        join_clause = join_clause,
        is_current_col = IS_CURRENT_COL,
        row_hash_col = ROW_HASH_COL,
    );

    let close_out = match event_ts_column {
        Some(_) => {
            // Correlated UPDATE so each row's valid_to picks up the matching
            // changed row's event_ts.
            let join: String = keys
                .iter()
                .map(|k| format!("t.{k} = c.{k}"))
                .collect::<Vec<_>>()
                .join(" AND ");
            format!(
                "UPDATE {schema}.{table} t \
                 SET {valid_to} = c._event_ts, {is_current} = false \
                 FROM {stage} c \
                 WHERE t.{is_current} AND {join}",
                schema = target.schema,
                table = target.name,
                valid_to = VALID_TO_COL,
                is_current = IS_CURRENT_COL,
                stage = stage,
                join = join,
            )
        }
        None => format!(
            "UPDATE {schema}.{table} \
             SET {valid_to} = now(), {is_current} = false \
             WHERE {is_current} AND ({keys}) IN (SELECT {keys} FROM {stage})",
            schema = target.schema,
            table = target.name,
            valid_to = VALID_TO_COL,
            is_current = IS_CURRENT_COL,
            keys = key_tuple,
            stage = stage,
        ),
    };

    // Insert new versions. Includes user cols + scd2 metadata + run metadata.
    let mut insert_cols: Vec<String> = user_columns.clone();
    insert_cols.push(VALID_FROM_COL.into());
    insert_cols.push(VALID_TO_COL.into());
    insert_cols.push(IS_CURRENT_COL.into());
    insert_cols.push(ROW_HASH_COL.into());
    let mut select_exprs: Vec<String> = user_columns.clone();
    let valid_from_expr: String = match event_ts_column {
        Some(_) => "_event_ts".into(),
        None => "now()".into(),
    };
    select_exprs.push(valid_from_expr); // valid_from
    select_exprs.push("NULL".into()); // valid_to
    select_exprs.push("true".into()); // is_current
    select_exprs.push("_row_hash".into()); // row_hash
    if has_metadata {
        insert_cols.push(LOADED_AT_COL.into());
        insert_cols.push(BATCH_ID_COL.into());
        select_exprs.push("now()".into());
        select_exprs.push("$1::uuid".into());
    }
    let insert_new = format!(
        "INSERT INTO {schema}.{table} ({insert_cols}) \
         SELECT {select_exprs} FROM {stage}",
        schema = target.schema,
        table = target.name,
        insert_cols = insert_cols.join(", "),
        select_exprs = select_exprs.join(", "),
        stage = stage,
    );

    Scd2Plan {
        statements: vec![create_temp, close_out, insert_new],
        has_metadata,
    }
}

/// Phase 15: build the SQL that detects out-of-order event arrivals.
/// Returns a query that yields a single row with `count` = number of
/// _scd2_changed rows whose event_ts is older than the existing current
/// version's valid_from. The executor runs this between the CREATE TEMP
/// and the UPDATE, and raises if count > 0.
pub fn build_out_of_order_check_sql(
    target: &TableSpec,
    keys: &[String],
    run_token: &str,
) -> String {
    let stage = format!("_scd2_changed_{run_token}");
    let join: String = keys
        .iter()
        .map(|k| format!("t.{k} = c.{k}"))
        .collect::<Vec<_>>()
        .join(" AND ");
    format!(
        "SELECT count(*)::bigint FROM {stage} c \
         JOIN {schema}.{table} t ON {join} AND t.{is_current} \
         WHERE t.{valid_from} > c._event_ts",
        stage = stage,
        schema = target.schema,
        table = target.name,
        join = join,
        is_current = IS_CURRENT_COL,
        valid_from = VALID_FROM_COL,
    )
}

#[cfg(test)]
mod tests {
    use crate::strategy::scd2::{
        IS_CURRENT_COL, ROW_HASH_COL, VALID_FROM_COL, VALID_TO_COL, augment_with_scd2, plan_scd2,
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
                ColumnSpec {
                    name: "name".into(),
                    ty: ColumnType::Text,
                    nullable: true,
                    primary_key: false,
                },
            ],
            unique_constraints: Vec::new(),
            fingerprint: String::new(),
        }
    }

    #[test]
    fn augment_adds_scd2_columns() {
        let augmented = augment_with_scd2(&customer_dim());
        let names: Vec<&str> = augmented.columns.iter().map(|c| c.name.as_str()).collect();
        for col in [VALID_FROM_COL, VALID_TO_COL, IS_CURRENT_COL, ROW_HASH_COL] {
            assert!(names.contains(&col), "missing scd2 col {col}");
        }
        // metadata cols also added
        assert!(names.contains(&"_loaded_at"));
        assert!(names.contains(&"_batch_id"));
    }

    #[test]
    fn augment_makes_valid_from_part_of_pk() {
        let augmented = augment_with_scd2(&customer_dim());
        let pks: Vec<&str> = augmented
            .columns
            .iter()
            .filter(|c| c.primary_key)
            .map(|c| c.name.as_str())
            .collect();
        assert!(pks.contains(&"customer_id"));
        assert!(pks.contains(&VALID_FROM_COL));
    }

    #[test]
    fn augment_is_idempotent() {
        let once = augment_with_scd2(&customer_dim());
        let twice = augment_with_scd2(&once);
        assert_eq!(once, twice);
    }

    #[test]
    fn augment_scd2_columns_have_correct_types() {
        let augmented = augment_with_scd2(&customer_dim());
        let by_name = |n: &str| augmented.columns.iter().find(|c| c.name == n).unwrap();
        assert_eq!(by_name(VALID_FROM_COL).ty, ColumnType::TimestampTz);
        assert!(!by_name(VALID_FROM_COL).nullable);
        assert_eq!(by_name(VALID_TO_COL).ty, ColumnType::TimestampTz);
        assert!(by_name(VALID_TO_COL).nullable);
        assert_eq!(by_name(IS_CURRENT_COL).ty, ColumnType::Boolean);
        assert!(!by_name(IS_CURRENT_COL).nullable);
        assert_eq!(by_name(ROW_HASH_COL).ty, ColumnType::Bytes);
        assert!(!by_name(ROW_HASH_COL).nullable);
    }

    #[test]
    fn plan_emits_three_statements() {
        let augmented = augment_with_scd2(&customer_dim());
        let plan = plan_scd2(
            &augmented,
            "SELECT * FROM src.customers",
            &["customer_id".into()],
            &["email".into(), "name".into()],
            "abc123",
            None,
        );
        assert_eq!(plan.statements.len(), 3);
    }

    #[test]
    fn plan_create_temp_uses_digest_with_compare_columns() {
        let augmented = augment_with_scd2(&customer_dim());
        let plan = plan_scd2(
            &augmented,
            "SELECT * FROM src",
            &["customer_id".into()],
            &["email".into(), "name".into()],
            "abc123",
            None,
        );
        let create_temp = &plan.statements[0];
        assert!(create_temp.contains("CREATE TEMP TABLE"));
        assert!(create_temp.contains("_scd2_changed_abc123"));
        assert!(create_temp.contains("digest("));
        assert!(create_temp.contains("'sha256'"));
        // Compare cols included in the digest expression.
        assert!(create_temp.contains("email"));
        assert!(create_temp.contains("name"));
        // Hash uses NULL marker + 0x01 separator.
        assert!(create_temp.contains("E'\\\\x00NULL\\\\x00'"));
        assert!(create_temp.contains("E'\\\\x01'"));
    }

    #[test]
    fn plan_join_filters_only_current_rows() {
        let augmented = augment_with_scd2(&customer_dim());
        let plan = plan_scd2(
            &augmented,
            "SELECT * FROM src",
            &["customer_id".into()],
            &["email".into(), "name".into()],
            "abc",
            None,
        );
        let create_temp = &plan.statements[0];
        assert!(create_temp.contains("LEFT JOIN warehouse.customer_dim t"));
        assert!(create_temp.contains("t.is_current"));
        assert!(create_temp.contains("t.row_hash IS DISTINCT FROM"));
    }

    #[test]
    fn plan_close_out_updates_current_rows_for_changed_keys() {
        let augmented = augment_with_scd2(&customer_dim());
        let plan = plan_scd2(
            &augmented,
            "SELECT * FROM src",
            &["customer_id".into()],
            &["email".into(), "name".into()],
            "abc",
            None,
        );
        let close_out = &plan.statements[1];
        assert!(close_out.contains("UPDATE warehouse.customer_dim"));
        assert!(close_out.contains("SET valid_to = now()"));
        assert!(close_out.contains("is_current = false"));
        assert!(close_out.contains("WHERE is_current"));
        assert!(close_out.contains("(customer_id) IN (SELECT customer_id FROM _scd2_changed_abc)"));
    }

    #[test]
    fn plan_insert_new_versions() {
        let augmented = augment_with_scd2(&customer_dim());
        let plan = plan_scd2(
            &augmented,
            "SELECT * FROM src",
            &["customer_id".into()],
            &["email".into(), "name".into()],
            "abc",
            None,
        );
        let insert = &plan.statements[2];
        assert!(insert.contains("INSERT INTO warehouse.customer_dim"));
        assert!(insert.contains("valid_from"));
        assert!(insert.contains("is_current"));
        assert!(insert.contains("row_hash"));
        assert!(insert.contains("now(), NULL, true"));
        assert!(insert.contains("$1::uuid"));
    }

    #[test]
    fn plan_handles_composite_keys() {
        let mut spec = customer_dim();
        spec.columns[1].primary_key = true;
        let augmented = augment_with_scd2(&spec);
        let plan = plan_scd2(
            &augmented,
            "SELECT * FROM src",
            &["customer_id".into(), "email".into()],
            &["name".into()],
            "abc",
            None,
        );
        let create_temp = &plan.statements[0];
        // JOIN on composite key.
        assert!(create_temp.contains("(t.customer_id, t.email) = (src.customer_id, src.email)"));
        let close_out = &plan.statements[1];
        assert!(close_out.contains("(customer_id, email) IN"));
    }

    // --- Phase 15: event-time SCD2 ----------------------------------------

    use crate::strategy::scd2::build_out_of_order_check_sql;

    #[test]
    fn event_time_create_temp_uses_distinct_on_and_event_ts_alias() {
        let augmented = augment_with_scd2(&customer_dim());
        let plan = plan_scd2(
            &augmented,
            "SELECT * FROM src",
            &["customer_id".into()],
            &["email".into(), "name".into()],
            "abc",
            Some("event_ts"),
        );
        let create_temp = &plan.statements[0];
        assert!(create_temp.contains("DISTINCT ON (customer_id)"));
        assert!(create_temp.contains("event_ts::timestamptz AS _event_ts"));
        assert!(create_temp.contains("ORDER BY customer_id, event_ts DESC"));
    }

    #[test]
    fn event_time_close_out_uses_correlated_update_with_event_ts() {
        let augmented = augment_with_scd2(&customer_dim());
        let plan = plan_scd2(
            &augmented,
            "SELECT * FROM src",
            &["customer_id".into()],
            &["email".into(), "name".into()],
            "abc",
            Some("event_ts"),
        );
        let close_out = &plan.statements[1];
        assert!(close_out.contains("UPDATE warehouse.customer_dim t"));
        assert!(close_out.contains("SET valid_to = c._event_ts"));
        assert!(close_out.contains("is_current = false"));
        assert!(close_out.contains("FROM _scd2_changed_abc c"));
        assert!(close_out.contains("t.customer_id = c.customer_id"));
        assert!(!close_out.contains("now()"));
    }

    #[test]
    fn event_time_insert_uses_event_ts_for_valid_from() {
        let augmented = augment_with_scd2(&customer_dim());
        let plan = plan_scd2(
            &augmented,
            "SELECT * FROM src",
            &["customer_id".into()],
            &["email".into(), "name".into()],
            "abc",
            Some("event_ts"),
        );
        let insert = &plan.statements[2];
        // valid_from is _event_ts, valid_to is NULL, is_current is true.
        assert!(insert.contains("_event_ts, NULL, true"));
        // Must not regress to now() for the valid_from slot.
        assert!(!insert.contains("now(), NULL, true"));
    }

    #[test]
    fn event_time_handles_composite_keys() {
        let mut spec = customer_dim();
        spec.columns[1].primary_key = true;
        let augmented = augment_with_scd2(&spec);
        let plan = plan_scd2(
            &augmented,
            "SELECT * FROM src",
            &["customer_id".into(), "email".into()],
            &["name".into()],
            "abc",
            Some("event_ts"),
        );
        let create_temp = &plan.statements[0];
        assert!(create_temp.contains("DISTINCT ON (customer_id, email)"));
        assert!(create_temp.contains("ORDER BY customer_id, email, event_ts DESC"));
        let close_out = &plan.statements[1];
        assert!(close_out.contains("t.customer_id = c.customer_id AND t.email = c.email"));
    }

    #[test]
    fn out_of_order_check_sql_targets_changed_rows_older_than_current() {
        let augmented = augment_with_scd2(&customer_dim());
        let sql = build_out_of_order_check_sql(&augmented, &["customer_id".into()], "abc");
        assert!(sql.contains("FROM _scd2_changed_abc c"));
        assert!(sql.contains("JOIN warehouse.customer_dim t"));
        assert!(sql.contains("t.customer_id = c.customer_id"));
        assert!(sql.contains("t.is_current"));
        assert!(sql.contains("WHERE t.valid_from > c._event_ts"));
        assert!(sql.contains("count(*)::bigint"));
    }
}
