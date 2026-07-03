//! Σ.J.2.b.vi — `PhysicalOptimizerRule` that wraps probe-side
//! parquet scans in [`crate::bloom::BloomFilterExec`] using blooms
//! shipped from the build side via the Σ.J.2.b.v Flight passthrough.
//!
//! ## The full distributed bloom flow
//!
//! ```text
//! Build worker:                         Probe worker:
//!   ┌───────────────────┐                 ┌────────────────────┐
//!   │ scan small_table  │                 │ scan large_table   │
//!   │ join key build    │                 │       ↑            │
//!   │       ↓           │                 │ BloomFilterExec    │  ← THIS RULE
//!   │ insert into bloom │                 │       ↑            │
//!   │       ↓           │                 │ Flight stream IN   │
//!   │ to_header_map ----│-- HTTP hdrs --->│ ContextBlooms      │
//!   │       ↓           │                 │ (from headers)     │
//!   │ Flight stream OUT │                 └────────────────────┘
//!   └───────────────────┘
//! ```
//!
//! This bite ships the **probe-side rule** (right column). The
//! build-side bloom emitter is Σ.J.2.b.vii (needs HashJoinExec
//! interception — separate design).
//!
//! ## The uuid scheme
//!
//! Both sides agree on `<table>.<column>`:
//!
//! - For [`crate::ematix_fast_parquet::EmatixFastParquetExec`],
//!   `<table>` = the file path's basename stripped of extension
//!   (e.g. `/data/sf1/lineitem.parquet` → `lineitem`).
//! - `<column>` = the lowercase Arrow field name in the scan's
//!   projected schema.
//!
//! Both halves are lowercased so case-mangling doesn't break matching.
//! See [`crate::bloom::column_uuid`].
//!
//! ## Per-request scope
//!
//! The rule is constructed *per inbound query* on the probe worker —
//! its `WorkerQueryContext::build_state` callback decodes inbound
//! `x-ematix-bloom-*` headers into a [`ContextBlooms`], constructs
//! [`EnableContextBloomRule::new`] with that bag, and registers the
//! rule on the per-request `SessionStateBuilder`. The rule is then
//! consumed once during physical planning.
//!
//! ## When the rule no-ops
//!
//! - Empty `ContextBlooms` (no inbound blooms) → no plan rewrites.
//! - Scan has no Int64 columns whose uuid matches a bloom →
//!   that scan isn't wrapped (other scans may still be).
//! - Non-`EmatixFastParquetExec` scans (FastParquetExec, DataSourceExec)
//!   → silently skipped. Σ.J.2.b.viii could broaden the matcher.
//!
//! Wrong answers are impossible: `BloomFilterExec` drops only rows
//! whose key the bloom doesn't contain, and the build side only ever
//! adds keys it actually emitted. False positives at most send extra
//! rows downstream; never false negatives.

use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DfResult;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;

use crate::bloom::{BloomFilterExec, ContextBlooms, column_uuid};
use crate::ematix_fast_parquet::EmatixFastParquetExec;

/// Σ.J.2.b.vi — per-request rule. Constructed with the
/// [`ContextBlooms`] decoded from the inbound Flight request headers.
#[derive(Debug)]
pub struct EnableContextBloomRule {
    blooms: ContextBlooms,
}

impl EnableContextBloomRule {
    pub fn new(blooms: ContextBlooms) -> Self {
        Self { blooms }
    }
}

/// Σ.J.2.b.vi — install the rule on a `SessionStateBuilder`. Per-
/// request callers (worker `build_state`) own the wiring; the
/// `ContextBlooms` is moved into the rule once at construction.
pub fn install_context_bloom_rule(
    builder: SessionStateBuilder,
    blooms: ContextBlooms,
) -> SessionStateBuilder {
    builder.with_physical_optimizer_rule(Arc::new(EnableContextBloomRule::new(blooms)))
}

impl PhysicalOptimizerRule for EnableContextBloomRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if self.blooms.is_empty() {
            return Ok(plan); // hot-path fast-out for non-distributed queries
        }
        let result = plan.transform_up(|node| {
            let Some(scan) = node.as_any().downcast_ref::<EmatixFastParquetExec>() else {
                return Ok(Transformed::no(node));
            };
            let Some(table_stem) = table_stem_for(scan.path()) else {
                return Ok(Transformed::no(node));
            };
            // Find an Int64 column whose uuid matches one of our
            // blooms. If multiple match, the first wins — wrapping
            // multiple times is correct but wasteful, and Σ.J.2.b.vi
            // ships the single-bloom case; multi-bloom-per-scan is a
            // follow-up (Σ.J.2.b.ix).
            let schema = scan.schema();
            for (idx, field) in schema.fields().iter().enumerate() {
                if field.data_type() != &DataType::Int64 {
                    continue;
                }
                let uuid = column_uuid(&table_stem, field.name());
                if let Some(bloom) = self.blooms.get(&uuid) {
                    let wrapped = BloomFilterExec::try_new(node.clone(), idx, bloom.clone())?;
                    return Ok(Transformed::yes(Arc::new(wrapped) as _));
                }
            }
            Ok(Transformed::no(node))
        })?;
        Ok(result.data)
    }

    fn name(&self) -> &str {
        "EnableContextBloomRule"
    }

    fn schema_check(&self) -> bool {
        // Wrapping a scan in BloomFilterExec preserves the schema —
        // it's a row-filter, not a projection.
        true
    }
}

/// Strip directory + extension from a parquet file path. Returns
/// lowercase. Returns `None` if the path has no usable basename.
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
    use arrow_schema::DataType;
    use datafusion::execution::context::SessionContext;
    use datafusion::physical_plan::displayable;
    use ematix_parquet_codec::write::{ColumnData, write_table_to_path};
    use ematix_parquet_format::types::CompressionCodec;
    use std::collections::HashMap;

    fn tmp_parquet(name: &str) -> std::path::PathBuf {
        // Unique per CALL: on case-insensitive filesystems (macOS)
        // `tmp_parquet("match")` and `tmp_parquet("Match")` otherwise
        // collide on the SAME path, and the parallel runner interleaves
        // one test's write with the other's provider read ("footer
        // length 0 exceeds file size 0" flake).
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "context_bloom_rule_test_{}_{}_{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed),
            name
        ));
        let _ = std::fs::create_dir_all(&dir);
        dir.join(format!("{name}.parquet"))
    }

    fn write_test_table(path: &std::path::Path) {
        // Two int64 columns + one int32 (which the rule must skip).
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

    fn plan_text(plan: &Arc<dyn ExecutionPlan>) -> String {
        format!("{}", displayable(plan.as_ref()).indent(true))
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

        // Rule with empty blooms must be a perfect no-op.
        let rule = EnableContextBloomRule::new(ContextBlooms::default());
        let cfg = ConfigOptions::default();
        let after = rule.optimize(plan.clone(), &cfg).unwrap();
        assert!(
            !plan_text(&after).contains("BloomFilterExec"),
            "rule must not inject BloomFilterExec when ContextBlooms is empty:\n{}",
            plan_text(&after)
        );
    }

    #[tokio::test]
    async fn matching_uuid_wraps_scan() {
        let path = tmp_parquet("match");
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

        // Build a ContextBlooms with the uuid the rule should produce:
        // `<file_stem>.<column>` = `match.l_orderkey` (lowercased).
        let mut map: HashMap<String, Arc<BloomFilter>> = HashMap::new();
        map.insert("match.l_orderkey".into(), make_bloom(&[1, 2, 3]));
        let blooms = ContextBlooms::new(map);

        let rule = EnableContextBloomRule::new(blooms);
        let cfg = ConfigOptions::default();
        let after = rule.optimize(plan, &cfg).unwrap();
        let text = plan_text(&after);
        assert!(
            text.contains("BloomFilterExec"),
            "rule should wrap the scan; got:\n{text}"
        );
        // Schema preserved (one column, Int64).
        let fields = after.schema().fields().clone();
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].data_type(), &DataType::Int64);
    }

    #[tokio::test]
    async fn case_insensitive_match() {
        let path = tmp_parquet("Match"); // capitalised file basename
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

        // Build-side wrote uppercase; rule must lowercase both halves
        // before lookup.
        let mut map: HashMap<String, Arc<BloomFilter>> = HashMap::new();
        map.insert("match.l_orderkey".into(), make_bloom(&[1]));
        let blooms = ContextBlooms::new(map);

        let rule = EnableContextBloomRule::new(blooms);
        let cfg = ConfigOptions::default();
        let after = rule.optimize(plan, &cfg).unwrap();
        assert!(plan_text(&after).contains("BloomFilterExec"));
    }

    #[tokio::test]
    async fn non_int64_columns_ignored() {
        let path = tmp_parquet("ignore_i32");
        write_test_table(&path);
        let prov = EmatixFastParquetTableProvider::try_new(path.to_str().unwrap()).unwrap();

        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(prov)).unwrap();
        let plan = ctx
            .sql("SELECT l_linenumber FROM t") // Int32
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();

        // Even with a bloom whose uuid matches, the column is Int32
        // → BloomFilterExec only supports Int64, so we must skip.
        let mut map: HashMap<String, Arc<BloomFilter>> = HashMap::new();
        map.insert("ignore_i32.l_linenumber".into(), make_bloom(&[1]));
        let blooms = ContextBlooms::new(map);

        let rule = EnableContextBloomRule::new(blooms);
        let cfg = ConfigOptions::default();
        let after = rule.optimize(plan, &cfg).unwrap();
        assert!(
            !plan_text(&after).contains("BloomFilterExec"),
            "non-Int64 column must not be wrapped"
        );
    }

    #[tokio::test]
    async fn unmatched_uuid_no_op() {
        let path = tmp_parquet("unmatched");
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

        let mut map: HashMap<String, Arc<BloomFilter>> = HashMap::new();
        map.insert("other_table.some_col".into(), make_bloom(&[1]));
        let blooms = ContextBlooms::new(map);

        let rule = EnableContextBloomRule::new(blooms);
        let cfg = ConfigOptions::default();
        let after = rule.optimize(plan, &cfg).unwrap();
        assert!(
            !plan_text(&after).contains("BloomFilterExec"),
            "no match → no wrap"
        );
    }
}
