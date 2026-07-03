//! Σ.Q.L4′ — PhysicalOptimizerRule that pushes pre-built blooms INTO
//! `EmatixFastParquetExec` scans as `ColumnPredicate::I64InBloom`
//! predicates on the BridgeFilter, instead of wrapping the scan in a
//! post-scan `BloomFilterExec` (which is what `EnableContextBloomRule`
//! does for the distributed Flight path).
//!
//! ## Difference from `EnableContextBloomRule`
//!
//! - `EnableContextBloomRule` wraps `EmatixFastParquetExec` in
//!   `BloomFilterExec`. The scan still decodes every row; the bloom
//!   filter just drops them after decode. Useful when the bloom
//!   arrives from a remote stage and the scan can't be re-planned.
//!
//! - This rule instead REWRITES the scan's `BridgeFilter` to include
//!   the bloom probe. The masked-decode path skips rows whose key
//!   isn't in the bloom — saving the decode work itself, not just
//!   the downstream join probe.
//!
//! Both rules consume the same `ContextBlooms` bag, so callers can
//! enable either or both. Typical local-node usage: install only this
//! rule. Typical distributed worker usage: install both (this for
//! emat-Parquet scans, the post-scan rule for other parquet readers
//! that don't have bridge-filter integration).
//!
//! ## Coverage
//!
//! - Only `EmatixFastParquetExec` scans (the only reader with a
//!   BridgeFilter integration). FastParquet / DataFusion's default
//!   reader are out of scope here — use `EnableContextBloomRule` for
//!   those.
//! - Only `Int64` columns. Bloom keys are i64-hashed; extending to
//!   string/i32 is a follow-up.
//! - Build-side bloom emission is upstream of this rule (Σ.J.2.b.vii's
//!   `emit_build_side_blooms` for the distributed path; a local emitter
//!   is Σ.Q.L4′ slice 3).

use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DfResult;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;

use crate::bloom::{ContextBlooms, column_uuid};
use crate::ematix_fast_parquet::{ColumnPredicate, EmatixFastParquetExec};

/// Σ.Q.L4′ — pushes context blooms into emat scans as I64InBloom
/// BridgeFilter predicates.
///
/// Holds the [`ContextBlooms`] behind an `Arc<RwLock<_>>` so the
/// bench/caller can mutate the bloom map between queries without
/// rebuilding the SessionState. An empty ContextBlooms is a no-op.
#[derive(Debug, Default)]
pub struct EnableInBloomScanPushdownRule {
    blooms: Arc<std::sync::RwLock<ContextBlooms>>,
}

impl EnableInBloomScanPushdownRule {
    pub fn new(blooms: ContextBlooms) -> Self {
        Self {
            blooms: Arc::new(std::sync::RwLock::new(blooms)),
        }
    }

    /// Σ.Q.L4′ — construct with a shared lock so the caller can swap
    /// the bloom map in between queries. The same rule instance is
    /// installed once on the SessionState; the lock is mutated from
    /// outside.
    pub fn with_shared(blooms: Arc<std::sync::RwLock<ContextBlooms>>) -> Self {
        Self { blooms }
    }

    /// Σ.Q.L4′ — replace the active ContextBlooms. Used by the bench
    /// harness to update the bloom map before each query.
    pub fn set(&self, blooms: ContextBlooms) {
        *self.blooms.write().unwrap() = blooms;
    }
}

impl PhysicalOptimizerRule for EnableInBloomScanPushdownRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let blooms = self.blooms.read().unwrap().clone();
        if blooms.is_empty() {
            return Ok(plan);
        }
        let result = plan.transform_up(|node| {
            let Some(scan) = node.as_any().downcast_ref::<EmatixFastParquetExec>() else {
                return Ok(Transformed::no(node));
            };
            let Some(table_stem) = table_stem_for(scan.path()) else {
                return Ok(Transformed::no(node));
            };
            // Collect every i64 projected column whose uuid matches
            // a bloom. Multiple-bloom-per-scan is supported here
            // (different from the post-scan rule, which only wraps
            // once): each predicate AND'd into the BridgeFilter is a
            // tighter probe.
            let schema = scan.schema();
            let mut new_preds: Vec<ColumnPredicate> = Vec::new();
            for (idx, field) in schema.fields().iter().enumerate() {
                if field.data_type() != &DataType::Int64 {
                    continue;
                }
                let uuid = column_uuid(&table_stem, field.name());
                if let Some(bloom) = blooms.get(&uuid) {
                    new_preds.push(ColumnPredicate::I64InBloom {
                        col_idx: idx,
                        bloom: bloom.clone(),
                    });
                }
            }
            if new_preds.is_empty() {
                return Ok(Transformed::no(node));
            }
            let rebuilt = scan.with_added_predicates(new_preds)?;
            Ok(Transformed::yes(rebuilt as _))
        })?;
        Ok(result.data)
    }

    fn name(&self) -> &str {
        "EnableInBloomScanPushdownRule"
    }

    fn schema_check(&self) -> bool {
        // Rebuilding a scan with extra bridge predicates preserves
        // the schema — masked-decode emits the same projected columns.
        true
    }
}

fn table_stem_for(path: &str) -> Option<String> {
    let p = std::path::Path::new(path);
    p.file_stem()
        .and_then(|s| s.to_str())
        .map(|s| s.to_lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bloom::{BloomFilter, ContextBlooms};
    use crate::ematix_fast_parquet::EmatixFastParquetTableProvider;
    use datafusion::execution::context::SessionContext;
    use datafusion::physical_plan::displayable;
    use ematix_parquet_codec::write::{ColumnData, write_table_to_path};
    use ematix_parquet_format::types::CompressionCodec;
    use std::collections::HashMap;

    fn tmp_parquet(name: &str) -> std::path::PathBuf {
        // Unique per CALL — see 01e6a50: PID+name-keyed fixture paths race
        // under the parallel test runner; the counter also immunizes
        // against case-insensitive-filesystem name collisions.
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "inbloom_scan_rule_test_{}_{}_{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed),
            name
        ));
        let _ = std::fs::create_dir_all(&dir);
        dir.join(format!("{name}.parquet"))
    }

    fn write_test_table(path: &std::path::Path) {
        let key: Vec<i64> = (0..1024).collect();
        let val: Vec<i64> = (0..1024).map(|x| x * 10).collect();
        let other: Vec<i32> = (0..1024).collect();
        write_table_to_path(
            path,
            &[
                ("l_orderkey", ColumnData::I64(&key)),
                ("l_partkey", ColumnData::I64(&val)),
                ("l_linenumber", ColumnData::I32(&other)),
            ],
            CompressionCodec::Uncompressed,
        )
        .unwrap();
    }

    fn make_bloom(keys: &[i64]) -> Arc<BloomFilter> {
        let mut b = BloomFilter::for_keys((keys.len() * 2).max(50));
        for &k in keys {
            b.insert_i64(k);
        }
        Arc::new(b)
    }

    #[tokio::test]
    async fn empty_context_blooms_is_noop() {
        let path = tmp_parquet("empty_ctx");
        write_test_table(&path);
        let prov = EmatixFastParquetTableProvider::try_new(path.to_str().unwrap()).unwrap();

        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(prov)).unwrap();
        let plan = ctx
            .sql("SELECT l_orderkey FROM t")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();

        let rule = EnableInBloomScanPushdownRule::new(ContextBlooms::default());
        let rewritten = rule
            .optimize(plan.clone(), &ConfigOptions::default())
            .unwrap();
        // Same plan tree, no rewrites — Arc identity preserved.
        assert!(Arc::ptr_eq(&plan, &rewritten));
    }

    #[tokio::test]
    async fn pushdown_adds_inbloom_predicate() {
        let path = tmp_parquet("match");
        write_test_table(&path);
        let table_stem = path.file_stem().unwrap().to_str().unwrap().to_lowercase();
        let prov = EmatixFastParquetTableProvider::try_new(path.to_str().unwrap()).unwrap();

        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(prov)).unwrap();
        let plan = ctx
            .sql("SELECT l_orderkey FROM t")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();

        // Build a bloom for l_orderkey under the file's stem (so the
        // uuid matches what the rule computes).
        let bloom_keys: Vec<i64> = (0..100i64).map(|i| i * 10).collect();
        let mut map = HashMap::new();
        map.insert(
            column_uuid(&table_stem, "l_orderkey"),
            make_bloom(&bloom_keys),
        );
        let blooms = ContextBlooms::new(map);
        let rule = EnableInBloomScanPushdownRule::new(blooms);
        let rewritten = rule.optimize(plan, &ConfigOptions::default()).unwrap();

        // The rewritten scan must carry a BridgeFilter with an
        // I64InBloom predicate on l_orderkey (col_idx=0).
        let mut found = false;
        let _ = rewritten.clone().transform_up(|node| {
            if let Some(scan) = node.as_any().downcast_ref::<EmatixFastParquetExec>() {
                let bf = scan.filter().expect("scan should have BridgeFilter");
                let has_inbloom = bf
                    .predicates()
                    .iter()
                    .any(|p| matches!(p, ColumnPredicate::I64InBloom { col_idx: 0, .. }));
                assert!(
                    has_inbloom,
                    "expected I64InBloom on col 0:\n{}",
                    displayable(rewritten.as_ref()).indent(true)
                );
                found = true;
            }
            Ok(Transformed::no(node))
        });
        assert!(found, "no EmatixFastParquetExec found in plan");
    }
}
