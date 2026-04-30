//! Phase 6: TruncateReplace strategy planner.
//!
//! Same-DB plan: TRUNCATE then INSERT...SELECT, both in one transaction.
//! Cross-DB plan: stage to a temp table first, then TRUNCATE + INSERT FROM
//! stage in one transaction so the target keeps old rows on failure.

use crate::strategy::append::plan_same_db_append;
use crate::types::TableSpec;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TruncatePlan {
    /// SQL statements to execute in order, inside a single transaction.
    pub statements: Vec<String>,
    /// True when the target carries `_loaded_at`/`_batch_id`; the executor
    /// must bind a UUID parameter to the second statement.
    pub has_metadata: bool,
}

pub fn plan_truncate_replace(target: &TableSpec, source_query: &str) -> TruncatePlan {
    let append_plan = plan_same_db_append(target, source_query);
    let truncate_sql = format!("TRUNCATE TABLE {}.{}", target.schema, target.name);
    TruncatePlan {
        statements: vec![truncate_sql, append_plan.sql],
        has_metadata: append_plan.has_metadata,
    }
}

#[cfg(test)]
mod tests {
    use crate::strategy::append::augment_with_metadata;
    use crate::strategy::truncate::plan_truncate_replace;
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
            unique_constraints: Vec::new(),
            fingerprint: String::new(),
        }
    }

    #[test]
    fn truncate_plan_emits_truncate_then_insert() {
        let augmented = augment_with_metadata(&customer_dim());
        let plan = plan_truncate_replace(&augmented, "SELECT * FROM src.customers");
        assert_eq!(plan.statements.len(), 2);
        assert!(plan.statements[0].starts_with("TRUNCATE TABLE warehouse.customer_dim"));
        assert!(plan.statements[1].contains("INSERT INTO warehouse.customer_dim"));
        assert!(plan.statements[1].contains("FROM (SELECT * FROM src.customers) src"));
    }

    #[test]
    fn truncate_plan_carries_metadata_flag() {
        let augmented = augment_with_metadata(&customer_dim());
        let plan = plan_truncate_replace(&augmented, "SELECT * FROM src.customers");
        assert!(plan.has_metadata);
    }

    #[test]
    fn truncate_plan_handles_no_metadata_target() {
        let plan = plan_truncate_replace(&customer_dim(), "SELECT * FROM src.customers");
        assert!(!plan.has_metadata);
        assert!(!plan.statements[1].contains("$1::uuid"));
    }

    #[test]
    fn truncate_plan_uses_restart_identity_optionally() {
        // For now we don't reset sequences; the SQL must be a plain TRUNCATE
        // so sequence-backed surrogate keys keep advancing. (If users need
        // RESTART IDENTITY they can opt in later.)
        let plan = plan_truncate_replace(&customer_dim(), "SELECT * FROM src");
        assert!(!plan.statements[0].contains("RESTART IDENTITY"));
    }
}
