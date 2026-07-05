//! Adaptive mesh gate — a tri-state, per-query gate in front of
//! `datafusion_distributed::DistributedPhysicalOptimizerRule`.
//!
//! ## Why
//!
//! Once peers are configured, the stage splitter distributes EVERY
//! query across the Arrow Flight mesh — including queries so small
//! that mesh coordination overhead dominates (measured 2026-07:
//! distributed Q22 ~120 ms vs ~24 ms single-node at SF=10). The gate
//! lets a session keep its peer mesh configured while deciding per
//! query whether the plan actually receives the stage-splitter
//! treatment.
//!
//! ## Semantics (`EMAT_MESH` tri-state, house pattern from
//! `ematix_flow_core::flags::tri_state`)
//!
//! - `EMAT_MESH=1`/`true` → always distribute (the pre-gate behavior).
//! - `EMAT_MESH=0`/`false` → never distribute: the physical plan is
//!   returned untouched (same `Arc`), byte-identical to the
//!   single-node plan. Peers stay configured but unused.
//! - unset / unrecognized → AUTO: sum `total_byte_size` across the
//!   plan's scan leaves. If the known sum is >= `EMAT_MESH_MIN_BYTES`
//!   (default [`DEFAULT_MESH_MIN_BYTES`] = 4 GiB — an initial value
//!   pending campaign calibration) → distribute; below → keep the
//!   single-node plan. If NO leaf reports a byte size at all
//!   (all `Precision::Absent`) → distribute, preserving the pre-gate
//!   behavior for stat-less sources (MemTable streams, custom scans
//!   without stats, `collect_statistics(false)` sessions).
//!
//! The decision happens inside [`PhysicalOptimizerRule::optimize`],
//! i.e. **per query at plan time** — `from_env()` only snapshots the
//! flag values once at session build.

use std::sync::Arc;

use datafusion::common::Result as DfResult;
use datafusion::common::config::ConfigOptions;
use datafusion::common::stats::Precision;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_distributed::DistributedPhysicalOptimizerRule;

/// Default AUTO threshold for `EMAT_MESH_MIN_BYTES`: 4 GiB of total
/// scan bytes. **Initial value pending campaign calibration** — chosen
/// so SF=1-class scans (~1 GB) stay single-node while SF=10-class
/// fact-table scans (~7 GB lineitem) distribute; the 2026-07 campaign
/// will refine it from measured crossover points.
pub const DEFAULT_MESH_MIN_BYTES: u64 = 4 * 1024 * 1024 * 1024;

/// Config for the adaptive mesh gate. Carried as a field of
/// [`AdaptiveMeshGateRule`] so tests construct it explicitly via the
/// pure constructors (no process-global env races — the
/// `CascadeChainConfig` house pattern).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MeshGateConfig {
    /// `EMAT_MESH` tri-state: `Some(true)` force distribute,
    /// `Some(false)` force single-node, `None` = AUTO (decide per
    /// query from scan statistics).
    pub mode: Option<bool>,
    /// AUTO threshold: distribute when the summed known scan bytes
    /// are >= this. `EMAT_MESH_MIN_BYTES`, default
    /// [`DEFAULT_MESH_MIN_BYTES`].
    pub min_bytes: u64,
}

impl MeshGateConfig {
    /// Resolve from the environment (`EMAT_MESH` +
    /// `EMAT_MESH_MIN_BYTES`). Called once at session build; the
    /// per-query decision then replays this snapshot at plan time.
    pub fn from_env() -> Self {
        todo!("mesh gate: from_env")
    }

    /// Force single-node: never distribute (what `EMAT_MESH=0`
    /// resolves to).
    pub fn off() -> Self {
        todo!("mesh gate: off")
    }

    /// Force distribute: always run the stage splitter (what
    /// `EMAT_MESH=1` resolves to — the pre-gate behavior).
    pub fn forced() -> Self {
        todo!("mesh gate: forced")
    }

    /// AUTO with an explicit byte threshold — the constructor tests
    /// and harnesses use to pin the decision without env mutation.
    pub fn auto_with_min_bytes(min_bytes: u64) -> Self {
        let _ = min_bytes;
        todo!("mesh gate: auto_with_min_bytes")
    }
}

/// Pure core of the `EMAT_MESH_MIN_BYTES` parse so tests can pin the
/// parse table without racing on process-global env vars (the
/// `tri_state_of` convention).
fn min_bytes_of(val: Option<&str>) -> u64 {
    let _ = val;
    todo!("mesh gate: min_bytes_of")
}

/// Pure AUTO decision: `leaf_bytes` carries one entry per scan leaf —
/// `Some(bytes)` when the leaf reports a usable `total_byte_size`
/// (`Precision::Exact` or `Inexact`), `None` when unknown (`Absent`).
/// Returns `true` when the plan should distribute.
fn auto_should_distribute(leaf_bytes: &[Option<u64>], min_bytes: u64) -> bool {
    let _ = (leaf_bytes, min_bytes);
    todo!("mesh gate: auto_should_distribute")
}

/// `PhysicalOptimizerRule` wrapping
/// [`DistributedPhysicalOptimizerRule`] behind the [`MeshGateConfig`]
/// tri-state. Install it exactly where the bare stage splitter used
/// to go (LAST in the physical chain); the campaign parity test pins
/// that position and this rule's name.
///
/// The name deliberately contains `"distributed"` so the existing
/// tripwire's substring assertion keeps holding.
pub struct AdaptiveMeshGateRule {
    config: MeshGateConfig,
    inner: DistributedPhysicalOptimizerRule,
}

impl std::fmt::Debug for AdaptiveMeshGateRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdaptiveMeshGateRule")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl AdaptiveMeshGateRule {
    /// Build with an explicit config (tests / harnesses).
    pub fn new(config: MeshGateConfig) -> Self {
        Self {
            config,
            inner: DistributedPhysicalOptimizerRule,
        }
    }

    /// Build from the environment — what `DistributedBackend::
    /// build_context` and the campaign harness install. Env is read
    /// ONCE here (session build); the gate decision itself runs per
    /// query at plan-optimization time.
    pub fn from_env() -> Self {
        Self::new(MeshGateConfig::from_env())
    }

    /// Borrow the resolved config (logs + tests).
    pub fn config(&self) -> &MeshGateConfig {
        &self.config
    }
}

impl PhysicalOptimizerRule for AdaptiveMeshGateRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let _ = (plan, config);
        todo!("mesh gate: optimize")
    }

    fn name(&self) -> &str {
        "ematix_adaptive_distributed_mesh_gate"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Collect one `Option<bytes>` per scan leaf (nodes with no
/// children), reading `partition_statistics(None).total_byte_size`.
/// `Exact` and `Inexact` are both usable; `Absent` (or a statistics
/// error) is unknown. Row-count-based width estimation is
/// deliberately NOT attempted — a leaf without a byte size is simply
/// unknown.
fn collect_scan_leaf_bytes(plan: &Arc<dyn ExecutionPlan>, out: &mut Vec<Option<u64>>) {
    let children = plan.children();
    if children.is_empty() {
        let bytes = plan
            .partition_statistics(None)
            .ok()
            .and_then(|s| match s.total_byte_size {
                Precision::Exact(n) | Precision::Inexact(n) => Some(n as u64),
                Precision::Absent => None,
            });
        out.push(bytes);
        return;
    }
    for child in children {
        collect_scan_leaf_bytes(child, out);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Float64Array, Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use std::fs::File;

    // ---------------- pure config / parse tests ----------------

    #[test]
    fn default_mesh_min_bytes_is_4gib() {
        assert_eq!(DEFAULT_MESH_MIN_BYTES, 4_294_967_296);
    }

    #[test]
    fn min_bytes_of_parse_table() {
        // Unset → default.
        assert_eq!(min_bytes_of(None), DEFAULT_MESH_MIN_BYTES);
        // Plain u64 parses.
        assert_eq!(min_bytes_of(Some("1")), 1);
        assert_eq!(min_bytes_of(Some("123456789")), 123_456_789);
        // Garbage / empty / negative → default (same forgiving shape
        // as flags::u64_or).
        assert_eq!(min_bytes_of(Some("")), DEFAULT_MESH_MIN_BYTES);
        assert_eq!(min_bytes_of(Some("4GiB")), DEFAULT_MESH_MIN_BYTES);
        assert_eq!(min_bytes_of(Some("-5")), DEFAULT_MESH_MIN_BYTES);
    }

    #[test]
    fn pure_constructors_pin_mode_and_threshold() {
        assert_eq!(MeshGateConfig::off().mode, Some(false));
        assert_eq!(MeshGateConfig::forced().mode, Some(true));
        let auto = MeshGateConfig::auto_with_min_bytes(7);
        assert_eq!(auto.mode, None);
        assert_eq!(auto.min_bytes, 7);
    }

    /// The campaign parity tripwire asserts the appended rule's
    /// lowercased name contains "distributed" — pin that here so a
    /// rename can't silently break it from this side.
    #[test]
    fn rule_name_contains_distributed_for_parity_tripwire() {
        let rule = AdaptiveMeshGateRule::new(MeshGateConfig::off());
        assert!(rule.name().to_lowercase().contains("distributed"));
        assert_eq!(rule.name(), "ematix_adaptive_distributed_mesh_gate");
    }

    // ---------------- pure AUTO decision tests ----------------

    #[test]
    fn auto_all_unknown_distributes() {
        // No statistics anywhere → distribute (preserves the pre-gate
        // behavior; documented in the module docs).
        assert!(auto_should_distribute(&[None, None], 1024));
        assert!(auto_should_distribute(&[None], u64::MAX));
    }

    #[test]
    fn auto_known_below_threshold_stays_single() {
        assert!(!auto_should_distribute(&[Some(100)], 1024));
        // Mixed unknown + small known: the known sum decides.
        assert!(!auto_should_distribute(&[None, Some(100), Some(200)], 1024));
    }

    #[test]
    fn auto_known_at_or_above_threshold_distributes() {
        // Sum across leaves, >= is inclusive.
        assert!(auto_should_distribute(&[Some(512), Some(512)], 1024));
        assert!(auto_should_distribute(&[Some(2048)], 1024));
        assert!(auto_should_distribute(&[None, Some(1024)], 1024));
    }

    #[test]
    fn auto_saturates_instead_of_overflowing() {
        assert!(auto_should_distribute(&[Some(u64::MAX), Some(u64::MAX)], u64::MAX));
    }

    // ---------------- plan-level tests (real parquet fixture) ----------------

    /// Write a small parquet table (1000 rows: k Int64, v Float64)
    /// and return (tempdir-guard, path).
    fn write_fixture() -> (tempfile::TempDir, String) {
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().join("t.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Float64, false),
        ]));
        let n = 1000i64;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from((0..n).map(|i| i % 10).collect::<Vec<_>>())),
                Arc::new(Float64Array::from(
                    (0..n).map(|i| i as f64).collect::<Vec<_>>(),
                )),
            ],
        )
        .expect("batch");
        let file = File::create(&path).expect("create parquet");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");
        (tmp, path.to_string_lossy().into_owned())
    }

    /// Production-preset session over the fixture, with statistics
    /// collection ON (the campaign session shape) and a pinned
    /// `target_partitions` so plans are deterministic across runs.
    async fn fixture_ctx(path: &str) -> SessionContext {
        let cfg = SessionConfig::new()
            .with_collect_statistics(true)
            .with_target_partitions(4);
        let builder = ematix_flow_core::preset::with_optimizer_rules(
            SessionStateBuilder::new()
                .with_config(cfg)
                .with_default_features(),
        );
        let ctx = SessionContext::new_with_state(builder.build());
        ctx.register_parquet("t", path, Default::default())
            .await
            .expect("register parquet");
        ctx
    }

    const FIXTURE_SQL: &str = "SELECT k, SUM(v) AS s FROM t GROUP BY k ORDER BY k";

    async fn physical_plan(ctx: &SessionContext, sql: &str) -> Arc<dyn ExecutionPlan> {
        ctx.sql(sql)
            .await
            .expect("plan sql")
            .create_physical_plan()
            .await
            .expect("physical plan")
    }

    /// Gate OFF → the input plan comes back as the SAME Arc: zero
    /// rewrite, zero mesh coordination.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn gate_off_returns_input_arc_unchanged() {
        let (_tmp, path) = write_fixture();
        let ctx = fixture_ctx(&path).await;
        let plan = physical_plan(&ctx, FIXTURE_SQL).await;

        let rule = AdaptiveMeshGateRule::new(MeshGateConfig::off());
        let out = rule
            .optimize(plan.clone(), &ConfigOptions::default())
            .expect("optimize");
        assert!(
            Arc::ptr_eq(&plan, &out),
            "gate off must return the input plan Arc unchanged"
        );
    }

    /// Gate OFF installed in a session (peers-configured shape) →
    /// the rendered plan is byte-identical to a never-gated
    /// single-node session's plan for the same query.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn gate_off_plan_display_byte_identical_to_single_node() {
        let (_tmp, path) = write_fixture();

        // Never-gated single-node session.
        let single_ctx = fixture_ctx(&path).await;
        let single = physical_plan(&single_ctx, FIXTURE_SQL).await;

        // Same session shape + the gate rule appended LAST (where the
        // bare stage splitter used to sit), gate OFF.
        let cfg = SessionConfig::new()
            .with_collect_statistics(true)
            .with_target_partitions(4);
        let builder = ematix_flow_core::preset::with_optimizer_rules(
            SessionStateBuilder::new()
                .with_config(cfg)
                .with_default_features(),
        )
        .with_physical_optimizer_rule(Arc::new(AdaptiveMeshGateRule::new(MeshGateConfig::off())));
        let gated_ctx = SessionContext::new_with_state(builder.build());
        gated_ctx
            .register_parquet("t", &path, Default::default())
            .await
            .expect("register parquet");
        let gated = physical_plan(&gated_ctx, FIXTURE_SQL).await;

        let single_str = displayable(single.as_ref()).indent(true).to_string();
        let gated_str = displayable(gated.as_ref()).indent(true).to_string();
        assert_eq!(
            single_str, gated_str,
            "gate-off plan must be byte-identical to the single-node plan"
        );
    }

    /// AUTO with an unreachable threshold over a real parquet table
    /// (whose scan leaves DO report byte statistics) → single-node:
    /// the same Arc back.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn auto_with_max_threshold_returns_input_arc() {
        let (_tmp, path) = write_fixture();
        let ctx = fixture_ctx(&path).await;
        let plan = physical_plan(&ctx, FIXTURE_SQL).await;

        // Tripwire: the fixture's scan leaves must actually report
        // usable byte statistics, otherwise this test would be
        // exercising the all-unknown path instead of the
        // below-threshold path.
        let mut leaves = Vec::new();
        collect_scan_leaf_bytes(&plan, &mut leaves);
        assert!(
            leaves.iter().any(|b| b.is_some()),
            "fixture scan must report byte statistics; got {leaves:?}"
        );

        let rule = AdaptiveMeshGateRule::new(MeshGateConfig::auto_with_min_bytes(u64::MAX));
        let out = rule
            .optimize(plan.clone(), &ConfigOptions::default())
            .expect("optimize");
        assert!(
            Arc::ptr_eq(&plan, &out),
            "AUTO below threshold must keep the single-node plan Arc"
        );
    }
}
