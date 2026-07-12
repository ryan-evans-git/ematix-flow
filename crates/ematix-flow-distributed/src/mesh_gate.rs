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
//!   (default [`DEFAULT_MESH_MIN_BYTES`] = 8 MiB — calibrated 2026-07,
//!   the mesh wins/ties across the whole measured range) → distribute;
//!   below → keep the single-node plan. If NO leaf reports a byte size at all
//!   (all `Precision::Absent`) → distribute, preserving the pre-gate
//!   behavior for stat-less sources (MemTable streams, custom scans
//!   without stats, `collect_statistics(false)` sessions).
//!
//! The decision happens inside [`PhysicalOptimizerRule::optimize`],
//! i.e. **per query at plan time** — `from_env()` only snapshots the
//! flag values once at session build.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::common::Result as DfResult;
use datafusion::common::config::ConfigOptions;
use datafusion::common::stats::Precision;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{BinaryExpr, Literal};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{HashJoinExec, NestedLoopJoinExec};
use datafusion_distributed::{DistributedExec, DistributedPhysicalOptimizerRule};

/// Default AUTO threshold for `EMAT_MESH_MIN_BYTES`: 8 MiB.
///
/// **Calibrated 2026-07 from the SF1/SF10 crossover campaign** (per-query
/// `scan_bytes` + single-vs-mesh medians on 8-way-partitioned TPC-H, 4
/// nodes). The finding: once data is partitioned so scans actually fan
/// out (`files_per_task=1`), the mesh **wins or ties on every query
/// across 1 MiB – 3.8 GiB of scanned bytes — no measured loss.** So the
/// true crossover is near zero; the earlier 4 GiB placeholder (which
/// suppressed all but the very largest scans) was ~1000× too high and
/// left almost the entire mesh speedup on the table. 8 MiB sits just
/// below the smallest robustly-measured win (Q13 at ~11 MiB, a 2×
/// speedup — small scan, heavy compute) while still declining to spin up
/// the mesh for genuinely trivial scans where the benefit is noise-level.
///
/// The gate compares against `Statistics::total_byte_size` — parquet
/// row-groups' *uncompressed* size, narrowed by projection/filter
/// pushdown to the scanned columns — so it is NOT "≈ file size on disk".
/// A stat-less scan (all `Precision::Absent`) still distributes, matching
/// the pre-gate behavior.
pub const DEFAULT_MESH_MIN_BYTES: u64 = 8 * 1024 * 1024;

/// Default for `EMAT_MESH_MAX_TABLE_SCANS`: decline to distribute at
/// 3+ scan instances of one base table. Calibrated 2026-07-11 on the
/// SF=100 4-node refresh: Q21 (lineitem ×3) ran 12.0 s distributed vs
/// 6.7 s single-node — every extra instance of the dominant table is
/// a full extra shuffle for the mesh but a page-cache re-read plus
/// runtime-bloom prune for the single node. Q15 (lineitem ×2) meshes
/// at parity (1.30 s vs 1.35 s), so 2 instances stay distributed.
pub const DEFAULT_MESH_MAX_TABLE_SCANS: u32 = 3;

/// The self-join veto's capacity condition: veto only when ONE
/// instance of the repeated table is at most this fraction of system
/// RAM. The veto's whole rationale — "single-node re-reads the table
/// from page cache" — inverts once the table exceeds memory: at
/// SF=1000 a ~128 GB lineitem against 128 GB of RAM means every
/// single-node re-scan is an EBS pass, while the mesh brings 4× the
/// IO bandwidth. SF=100 calibration: Q21's per-instance ~12.8 GB on
/// a 32 GB box (0.4× RAM) → veto correct (single 6.7 s vs mesh
/// 12.0 s); SF=1000 ~128 GB on 128 GB (1.0× RAM) → veto must NOT
/// fire.
pub const VETO_INSTANCE_RAM_FRACTION: f64 = 0.5;

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
    /// Σ.MG self-join shuffle guard: in AUTO, decline to distribute
    /// when any single base table appears as this many (or more)
    /// independent scan instances in the plan. Each instance of the
    /// big table is a full extra network shuffle for the mesh, while
    /// the single-node plan re-reads it from page cache and prunes it
    /// with runtime blooms (which distributed arrow scans don't
    /// carry). TPC-H Q21 (lineitem × 3: inner + semi + anti) is the
    /// motivating shape — mesh 12.0 s vs single-node 6.7 s at SF=100
    /// on 4× c7i.4xlarge. (Q15, lineitem × 2, meshed at parity and
    /// used to stay distributed on purpose — the Σ.Q15.FP correctness
    /// veto below now forces it single-node for a different reason.)
    /// `EMAT_MESH_MAX_TABLE_SCANS` (decline at N or more; 0 disables
    /// the guard), default [`DEFAULT_MESH_MAX_TABLE_SCANS`].
    pub max_table_scans: u32,
    /// Σ.Q15.FP correctness veto (2026-07-12): force single-node when
    /// the plan equality-compares two non-literal FLOAT expressions
    /// with an order-sensitive float reduction (`sum`/`avg`) below.
    /// Distributed partial-merge order is nondeterministic, the
    /// aggregate is evaluated once per comparison side, and f64
    /// addition is not associative — the equality can miss by one ULP
    /// and silently DROP ROWS (TPC-H Q15 returned 0 rows on ~1/3 of
    /// mesh runs at SF=100). `min`/`max` are order-exact for floats
    /// and do not trip this (Q02 keeps its mesh plan). Overrides even
    /// `EMAT_MESH=1`: a forced-mesh wrong answer is still a wrong
    /// answer. `EMAT_MESH_FLOAT_EQ_VETO=0` disables — debugging only,
    /// documented as returning wrong answers. The durable fix
    /// (deterministic distributed float reduction) is a post-1.0 arc.
    pub float_eq_veto: bool,
    /// Detected system RAM (bytes) for the veto's capacity condition
    /// ([`VETO_INSTANCE_RAM_FRACTION`]). `None` when undetectable
    /// (non-Linux dev hosts) — the veto then applies unconditionally,
    /// matching the pre-capacity behavior on small local data.
    pub system_ram_bytes: Option<u64>,
}

impl MeshGateConfig {
    /// Resolve from the environment (`EMAT_MESH` +
    /// `EMAT_MESH_MIN_BYTES`). Called once at session build; the
    /// per-query decision then replays this snapshot at plan time.
    pub fn from_env() -> Self {
        let raw = std::env::var("EMAT_MESH_MIN_BYTES").ok();
        let (min_bytes, unparseable) = min_bytes_of(raw.as_deref());
        if unparseable {
            // An operator who typo'd the threshold (e.g. `4GiB`, `1e9`)
            // would otherwise silently measure the 4 GiB default — loud
            // in a benchmark-credibility context. Warn, don't fail.
            tracing::warn!(
                value = raw.as_deref().unwrap_or(""),
                default_bytes = min_bytes,
                "EMAT_MESH_MIN_BYTES is not a plain u64 byte count; \
                 falling back to the default (only bare integers are \
                 accepted, e.g. 4294967296)"
            );
        }
        Self {
            mode: ematix_flow_core::flags::tri_state("EMAT_MESH"),
            min_bytes,
            max_table_scans: max_table_scans_of(
                std::env::var("EMAT_MESH_MAX_TABLE_SCANS").ok().as_deref(),
            ),
            system_ram_bytes: detect_system_ram(),
            float_eq_veto: ematix_flow_core::flags::tri_state("EMAT_MESH_FLOAT_EQ_VETO")
                .unwrap_or(true),
        }
    }

    /// Force single-node: never distribute (what `EMAT_MESH=0`
    /// resolves to).
    pub fn off() -> Self {
        Self {
            mode: Some(false),
            min_bytes: DEFAULT_MESH_MIN_BYTES,
            max_table_scans: DEFAULT_MESH_MAX_TABLE_SCANS,
            system_ram_bytes: detect_system_ram(),
            float_eq_veto: true,
        }
    }

    /// Force distribute: always run the stage splitter (what
    /// `EMAT_MESH=1` resolves to — the pre-gate behavior).
    pub fn forced() -> Self {
        Self {
            mode: Some(true),
            min_bytes: DEFAULT_MESH_MIN_BYTES,
            max_table_scans: DEFAULT_MESH_MAX_TABLE_SCANS,
            system_ram_bytes: detect_system_ram(),
            float_eq_veto: true,
        }
    }

    /// AUTO with an explicit byte threshold — the constructor tests
    /// and harnesses use to pin the decision without env mutation.
    pub fn auto_with_min_bytes(min_bytes: u64) -> Self {
        Self {
            mode: None,
            min_bytes,
            max_table_scans: DEFAULT_MESH_MAX_TABLE_SCANS,
            // Deterministic in tests: no host-RAM dependence; the
            // veto applies unconditionally unless a test sets RAM.
            system_ram_bytes: None,
            float_eq_veto: true,
        }
    }
}

/// System RAM in bytes: Linux `/proc/meminfo` `MemTotal` (the bench
/// and production boxes); `None` elsewhere.
fn detect_system_ram() -> Option<u64> {
    let info = std::fs::read_to_string("/proc/meminfo").ok()?;
    let line = info.lines().find(|l| l.starts_with("MemTotal:"))?;
    let kb: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kb * 1024)
}

/// Pure core of the `EMAT_MESH_MAX_TABLE_SCANS` parse (same
/// convention as [`min_bytes_of`]): unset or garbage → default;
/// `0` → guard disabled (never decline on instance count).
fn max_table_scans_of(val: Option<&str>) -> u32 {
    val.and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(DEFAULT_MESH_MAX_TABLE_SCANS)
}

/// Pure core of the `EMAT_MESH_MIN_BYTES` parse so tests can pin the
/// parse table without racing on process-global env vars (the
/// `tri_state_of` convention).
///
/// Returns `(bytes, unparseable)`: unset → `(default, false)`; a bare
/// `u64` → `(n, false)`; anything else → `(default, true)` so the
/// caller can WARN rather than silently swallow a typo'd threshold.
/// We keep this local (rather than delegating to `flags::u64_or`)
/// precisely because `u64_or` cannot distinguish "unset" from
/// "garbage" — and that distinction is the whole point of the warning.
fn min_bytes_of(val: Option<&str>) -> (u64, bool) {
    match val {
        None => (DEFAULT_MESH_MIN_BYTES, false),
        Some(s) => match s.parse::<u64>() {
            Ok(n) => (n, false),
            Err(_) => (DEFAULT_MESH_MIN_BYTES, true),
        },
    }
}

/// Full AUTO decision: byte threshold AND the Σ.MG self-join veto.
/// `repeats`/`instance_bytes` come from
/// [`max_same_table_scan_instances`]; `max_table_scans == 0` disables
/// the veto. The veto additionally requires the repeated table to FIT
/// memory (one instance ≤ [`VETO_INSTANCE_RAM_FRACTION`] × RAM):
/// past that, single-node re-scans are storage passes, the page-cache
/// rationale inverts, and the mesh's aggregate IO wins even for
/// self-joins.
fn auto_decision(
    leaf_bytes: &[Option<u64>],
    repeats: u32,
    instance_bytes: u64,
    config: &MeshGateConfig,
) -> bool {
    let fits_memory = match config.system_ram_bytes {
        Some(ram) => (instance_bytes as f64) <= (ram as f64) * VETO_INSTANCE_RAM_FRACTION,
        None => true,
    };
    if config.max_table_scans > 0 && repeats >= config.max_table_scans && fits_memory {
        return false;
    }
    auto_should_distribute(leaf_bytes, config.min_bytes)
}

/// Pure byte-threshold decision: `leaf_bytes` carries one entry per
/// scan leaf — `Some(bytes)` when the leaf reports a usable
/// `total_byte_size` (`Precision::Exact` or `Inexact`), `None` when
/// unknown (`Absent`). Returns `true` when the plan should distribute.
///
/// - Any known bytes → distribute iff the (saturating) known sum
///   reaches `min_bytes`. Unknown leaves contribute nothing — the
///   known sum alone decides, so a large known fact scan still
///   distributes even when a stat-less dimension source sits next
///   to it.
/// - NO known bytes at all → distribute: with zero information the
///   gate preserves the pre-gate always-distribute behavior rather
///   than silently disabling the mesh.
fn auto_should_distribute(leaf_bytes: &[Option<u64>], min_bytes: u64) -> bool {
    let mut any_known = false;
    let mut sum: u64 = 0;
    for bytes in leaf_bytes.iter().flatten() {
        any_known = true;
        sum = sum.saturating_add(*bytes);
    }
    if !any_known {
        return true;
    }
    sum >= min_bytes
}

/// Σ.Q15.FP — is `dt` a float type whose addition is non-associative?
/// (Decimals are exact; they never trip the veto.)
fn is_float_type(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Float16 | DataType::Float32 | DataType::Float64
    )
}

/// Non-literal expression of float type against `schema`. Literals are
/// excluded on purpose: `l_discount = 0.06`-style constant comparisons
/// are deterministic (both sides identical every evaluation) — the
/// hazard needs two independently COMPUTED floats.
fn expr_is_computed_float(expr: &Arc<dyn PhysicalExpr>, schema: &Schema) -> bool {
    expr.as_any().downcast_ref::<Literal>().is_none()
        && expr
            .data_type(schema)
            .map(|dt| is_float_type(&dt))
            .unwrap_or(false)
}

/// Recursively scan a predicate for `Eq` between two computed floats.
fn predicate_has_float_eq(expr: &Arc<dyn PhysicalExpr>, schema: &Schema) -> bool {
    let Some(be) = expr.as_any().downcast_ref::<BinaryExpr>() else {
        return false;
    };
    if *be.op() == Operator::Eq
        && expr_is_computed_float(be.left(), schema)
        && expr_is_computed_float(be.right(), schema)
    {
        return true;
    }
    predicate_has_float_eq(be.left(), schema) || predicate_has_float_eq(be.right(), schema)
}

/// Does this aggregate compute an order-sensitive float reduction?
/// `sum`/`avg` over floats depend on partial-merge order; `min`/`max`
/// are exact regardless of order.
fn agg_is_order_sensitive_float(agg: &AggregateExec) -> bool {
    agg.aggr_expr().iter().any(|a| {
        let name = a.fun().name().to_ascii_lowercase();
        (name == "sum" || name == "avg") && is_float_type(a.field().data_type())
    })
}

/// Σ.Q15.FP correctness hazard detector (see
/// [`MeshGateConfig::float_eq_veto`]): true when the plan contains an
/// equality between two computed float expressions — a hash-join key,
/// a nested-loop join filter, or a `FilterExec` predicate — with an
/// order-sensitive float reduction (`sum`/`avg` over floats) anywhere
/// below. TPC-H Q15 (`total_revenue = (SELECT max(total_revenue) …)`
/// where `total_revenue` is a float `sum`) is the motivating shape; it
/// returned 0 rows on ~1/3 of distributed runs. Q02's
/// `ps_supplycost = (SELECT min(…))` does NOT trip it: `min` is exact.
pub(crate) fn float_reduction_equality_hazard(plan: &Arc<dyn ExecutionPlan>) -> bool {
    fn walk(plan: &Arc<dyn ExecutionPlan>, hazard: &mut bool) -> bool {
        let mut below = false;
        for child in plan.children() {
            below |= walk(child, hazard);
        }
        // SharedSubtreeExec hides its input from `children()` (shared
        // subtrees must not be rewritten per consumer) — and it is
        // precisely how Q15's repeated float aggregate reaches both
        // sides of the equality. Descend through the accessor: the
        // sharing is what keeps SINGLE-NODE runs deterministic, but
        // staging splits its consumers across processes, each of which
        // re-populates independently.
        if let Some(shared) = plan
            .as_any()
            .downcast_ref::<ematix_flow_core::shared_subtree_exec::SharedSubtreeExec>()
        {
            below |= walk(shared.input(), hazard);
        }
        if below && !*hazard {
            if let Some(join) = plan.as_any().downcast_ref::<HashJoinExec>() {
                let schema_l = join.left().schema();
                let schema_r = join.right().schema();
                if join.on().iter().any(|(l, r)| {
                    expr_is_computed_float(l, schema_l.as_ref())
                        || expr_is_computed_float(r, schema_r.as_ref())
                }) {
                    *hazard = true;
                }
            }
            if let Some(join) = plan.as_any().downcast_ref::<NestedLoopJoinExec>() {
                if let Some(f) = join.filter() {
                    if predicate_has_float_eq(f.expression(), f.schema()) {
                        *hazard = true;
                    }
                }
            }
            if let Some(filter) = plan.as_any().downcast_ref::<FilterExec>() {
                if predicate_has_float_eq(filter.predicate(), filter.input().schema().as_ref()) {
                    *hazard = true;
                }
            }
        }
        below
            || plan
                .as_any()
                .downcast_ref::<AggregateExec>()
                .map(agg_is_order_sensitive_float)
                .unwrap_or(false)
    }
    let mut hazard = false;
    walk(plan, &mut hazard);
    hazard
}

/// Σ.Q15.FP — does the correctness veto apply? Pure decision helper
/// (see [`MeshGateConfig::float_eq_veto`]): veto is enabled, the mode
/// could distribute (`off` never does — nothing to veto), and the
/// plan carries the float-reduction equality hazard.
fn q15_veto_applies(config: &MeshGateConfig, plan: &Arc<dyn ExecutionPlan>) -> bool {
    config.float_eq_veto && config.mode != Some(false) && float_reduction_equality_hazard(plan)
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
        // Idempotency guard: if the plan is already stage-split (root is
        // a DistributedExec), never wrap it again — re-running the inner
        // rule on a distributed root is unspecified, and AUTO would sum
        // leaves INSIDE the existing stages. Unreachable via the single
        // append the parity test pins, but cheap insurance against a
        // future double-install or plan re-optimization.
        if plan.as_any().is::<DistributedExec>() {
            tracing::debug!("mesh gate: plan already distributed, passing through");
            return Ok(plan);
        }
        // Σ.Q15.FP correctness veto — BEFORE the mode switch, so it
        // overrides even forced mesh: a distributed run of this shape
        // can silently drop rows (see MeshGateConfig::float_eq_veto).
        if q15_veto_applies(&self.config, &plan) {
            tracing::info!(
                "mesh gate: float-reduction equality hazard (Σ.Q15.FP) → \
                 single-node correctness veto (overrides EMAT_MESH=1; \
                 EMAT_MESH_FLOAT_EQ_VETO=0 disables — wrong answers possible)"
            );
            return Ok(plan);
        }
        match self.config.mode {
            // Force OFF: hand back the SAME Arc — byte-identical
            // single-node plan, zero mesh coordination.
            Some(false) => {
                tracing::debug!("mesh gate: mode=off → single-node");
                Ok(plan)
            }
            // Force ON: the pre-gate behavior.
            Some(true) => {
                tracing::debug!("mesh gate: mode=on → distribute");
                self.inner.optimize(plan, config)
            }
            // AUTO: decide from the scan leaves' byte statistics,
            // vetoed by the Σ.MG self-join shuffle guard.
            None => {
                let mut leaf_bytes = Vec::new();
                collect_scan_leaf_bytes(&plan, &mut leaf_bytes);
                let known_sum: u64 = leaf_bytes
                    .iter()
                    .flatten()
                    .fold(0u64, |acc, b| acc.saturating_add(*b));
                let (repeats, instance_bytes) =
                    max_same_table_scan_instances(&plan, self.config.min_bytes);
                let distribute = auto_decision(&leaf_bytes, repeats, instance_bytes, &self.config);
                tracing::debug!(
                    known_scan_bytes = known_sum,
                    min_bytes = self.config.min_bytes,
                    leaves = leaf_bytes.len(),
                    same_table_instances = repeats,
                    decision = if distribute {
                        "distribute"
                    } else {
                        "single-node"
                    },
                    "mesh gate: mode=auto"
                );
                if distribute {
                    self.inner.optimize(plan, config)
                } else {
                    Ok(plan)
                }
            }
        }
    }

    fn name(&self) -> &str {
        "ematix_adaptive_distributed_mesh_gate"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Σ.MG: the largest number of independent scan INSTANCES any single
/// base table has in this plan. A self-join like Q21 scans lineitem
/// three times (inner + semi + anti); each instance is a full extra
/// network shuffle when distributed, but a page-cache re-read (plus
/// runtime-bloom pruning) on the single node.
///
/// Table identity is the parent directory of the scan's first file:
/// - arrow `DataSourceExec` listings (the distributed registration):
///   one leaf per instance; identity via `FileScanConfig.file_groups`.
/// - `EmatixInterleaveUnionExec` (the ematix parted provider): the
///   union IS one instance — its children are parts of one logical
///   scan, so the walk records it once and does not descend (else a
///   single parted table would read as 8 "instances").
/// - `EmatixFastParquetExec`: identity via its file's parent dir.
///
/// Leaves with no extractable identity are ignored (never veto on
/// unknowns), as are instances below `min_instance_bytes` — three
/// scans of 25-row `nation` multiply nothing; the guard is about
/// shuffle volume, so only instances that are themselves mesh-scale
/// (the same `EMAT_MESH_MIN_BYTES` floor) count.
pub fn max_same_table_scan_instances(
    plan: &Arc<dyn ExecutionPlan>,
    min_instance_bytes: u64,
) -> (u32, u64) {
    use std::collections::HashMap;
    fn identity_of(plan: &Arc<dyn ExecutionPlan>) -> Option<String> {
        if let Some(ds) = plan
            .as_any()
            .downcast_ref::<datafusion::datasource::source::DataSourceExec>()
        {
            let cfg = ds
                .data_source()
                .as_any()
                .downcast_ref::<datafusion::datasource::physical_plan::FileScanConfig>()?;
            let first = cfg.file_groups.iter().flat_map(|g| g.files()).next()?;
            let loc = first.object_meta.location.as_ref();
            return Some(parent_dir(loc));
        }
        if let Some(scan) =
            plan.as_any()
                .downcast_ref::<ematix_flow_core::ematix_fast_parquet::EmatixFastParquetExec>()
        {
            return Some(parent_dir(scan.path()));
        }
        None
    }
    fn parent_dir(path: &str) -> String {
        match path.rsplit_once('/') {
            Some((dir, _file)) => dir.to_string(),
            None => path.to_string(),
        }
    }
    fn known_bytes(plan: &Arc<dyn ExecutionPlan>) -> u64 {
        plan.partition_statistics(None)
            .ok()
            .and_then(|s| match s.total_byte_size {
                Precision::Exact(n) | Precision::Inexact(n) => Some(n as u64),
                Precision::Absent => None,
            })
            .unwrap_or(0)
    }
    fn walk(
        plan: &Arc<dyn ExecutionPlan>,
        min_bytes: u64,
        counts: &mut HashMap<String, (u32, u64)>,
    ) {
        // A parted interleave union is ONE scan instance of one table,
        // sized as the sum of its parts.
        if plan
            .as_any()
            .is::<ematix_flow_core::ematix_fast_parquet_multi::EmatixInterleaveUnionExec>()
        {
            let total: u64 = plan.children().iter().map(|c| known_bytes(c)).sum();
            let mut node = plan.children().into_iter().next().cloned();
            while let Some(n) = node {
                if let Some(id) = identity_of(&n) {
                    if total >= min_bytes {
                        let e = counts.entry(id).or_insert((0, 0));
                        e.0 += 1;
                        e.1 = e.1.max(total);
                    }
                    return;
                }
                node = n.children().into_iter().next().cloned();
            }
            return;
        }
        let children = plan.children();
        if children.is_empty() {
            let bytes = known_bytes(plan);
            if bytes >= min_bytes {
                if let Some(id) = identity_of(plan) {
                    let e = counts.entry(id).or_insert((0, 0));
                    e.0 += 1;
                    e.1 = e.1.max(bytes);
                }
            }
            return;
        }
        for c in children {
            walk(c, min_bytes, counts);
        }
    }
    let mut counts = HashMap::new();
    walk(plan, min_instance_bytes, &mut counts);
    counts
        .values()
        .copied()
        .max_by_key(|(n, _)| *n)
        .unwrap_or((0, 0))
}

/// Collect one `Option<bytes>` per scan leaf (nodes with no
/// children), reading `partition_statistics(None).total_byte_size`.
/// `Exact` and `Inexact` are both usable; `Absent` (or a statistics
/// error) is unknown. Row-count-based width estimation is
/// deliberately NOT attempted — a leaf without a byte size is simply
/// unknown.
pub(crate) fn collect_scan_leaf_bytes(plan: &Arc<dyn ExecutionPlan>, out: &mut Vec<Option<u64>>) {
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
    fn default_mesh_min_bytes_is_8mib_calibrated() {
        // Calibrated 2026-07 from the SF1/SF10 crossover campaign (was a
        // 4 GiB placeholder). 8 MiB — mesh wins/ties across the whole
        // measured 1 MiB–3.8 GiB range, so distribute broadly.
        assert_eq!(DEFAULT_MESH_MIN_BYTES, 8 * 1024 * 1024);
        assert_eq!(DEFAULT_MESH_MIN_BYTES, 8_388_608);
    }

    #[test]
    fn min_bytes_of_parse_table() {
        // Unset → default, NOT flagged unparseable.
        assert_eq!(min_bytes_of(None), (DEFAULT_MESH_MIN_BYTES, false));
        // Plain u64 parses, not flagged.
        assert_eq!(min_bytes_of(Some("1")), (1, false));
        assert_eq!(min_bytes_of(Some("123456789")), (123_456_789, false));
        // Garbage / empty / negative / suffixed → default VALUE but
        // FLAGGED, so from_env can warn instead of silently ignoring an
        // operator's typo'd threshold. Note "4GiB"/"1e9" are exactly the
        // human-friendly forms someone would reach for — and they all
        // fail; only a bare integer works.
        assert_eq!(min_bytes_of(Some("")), (DEFAULT_MESH_MIN_BYTES, true));
        assert_eq!(min_bytes_of(Some("4GiB")), (DEFAULT_MESH_MIN_BYTES, true));
        assert_eq!(min_bytes_of(Some("1e9")), (DEFAULT_MESH_MIN_BYTES, true));
        assert_eq!(min_bytes_of(Some("-5")), (DEFAULT_MESH_MIN_BYTES, true));
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
        assert!(auto_should_distribute(
            &[Some(u64::MAX), Some(u64::MAX)],
            u64::MAX
        ));
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

    /// Σ.Q15.FP — the Q15 shape (float SUM → MAX → equality) must trip
    /// the hazard detector and stay single-node EVEN under forced
    /// mesh; the escape hatch restores distribution for debugging.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn float_sum_equality_trips_hazard_and_vetoes_forced_mesh() {
        let (_tmp, path) = write_fixture();
        let ctx = fixture_ctx(&path).await;
        let plan = physical_plan(
            &ctx,
            "SELECT a.k, a.s FROM \
             (SELECT k, SUM(v) AS s FROM t GROUP BY k) a JOIN \
             (SELECT MAX(s2) AS m FROM (SELECT k, SUM(v) AS s2 FROM t GROUP BY k)) b \
             ON a.s = b.m",
        )
        .await;
        assert!(
            float_reduction_equality_hazard(&plan),
            "Q15 shape must be detected:\n{}",
            datafusion::physical_plan::displayable(plan.as_ref()).indent(true)
        );

        let rule = AdaptiveMeshGateRule::new(MeshGateConfig::forced());
        let out = rule
            .optimize(plan.clone(), &ConfigOptions::default())
            .expect("optimize");
        assert!(
            Arc::ptr_eq(&plan, &out),
            "correctness veto must override forced mesh"
        );

        // Escape hatch (debugging only — wrong answers possible):
        // with the veto disabled the decision helper stands down, so
        // forced mesh would proceed to the splitter. (Asserted at the
        // decision level: invoking the real splitter needs a full
        // distributed session, not a bare ConfigOptions.)
        assert!(!q15_veto_applies(
            &MeshGateConfig {
                float_eq_veto: false,
                ..MeshGateConfig::forced()
            },
            &plan
        ));
        // Mode `off` never distributes — nothing to veto.
        assert!(!q15_veto_applies(&MeshGateConfig::off(), &plan));
        // And the veto itself: enabled + forced mesh + hazard.
        assert!(q15_veto_applies(&MeshGateConfig::forced(), &plan));
    }

    /// Order-exact shapes must NOT trip the hazard: `min` (Q02's
    /// shape), integer sums, and literal comparisons.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn exact_reductions_and_literals_do_not_trip_hazard() {
        let (_tmp, path) = write_fixture();
        let ctx = fixture_ctx(&path).await;
        let min_plan = physical_plan(
            &ctx,
            "SELECT a.k FROM (SELECT k, MIN(v) AS s FROM t GROUP BY k) a \
             JOIN (SELECT MIN(v) AS m FROM t) b ON a.s = b.m",
        )
        .await;
        assert!(
            !float_reduction_equality_hazard(&min_plan),
            "min is order-exact for floats (Q02 keeps its mesh plan)"
        );
        let int_plan = physical_plan(
            &ctx,
            "SELECT a.k FROM (SELECT k, SUM(k) AS s FROM t GROUP BY k) a \
             JOIN (SELECT MAX(s2) AS m FROM (SELECT k, SUM(k) AS s2 FROM t GROUP BY k)) b \
             ON a.s = b.m",
        )
        .await;
        assert!(
            !float_reduction_equality_hazard(&int_plan),
            "integer sums are exact in any order"
        );
        let lit_plan = physical_plan(
            &ctx,
            "SELECT k FROM (SELECT k, SUM(v) AS s FROM t GROUP BY k) WHERE s = 42.0",
        )
        .await;
        assert!(
            !float_reduction_equality_hazard(&lit_plan),
            "a literal comparison is identical on every evaluation"
        );
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

    /// Idempotency: a plan whose root is already `DistributedExec` is
    /// passed straight through (same Arc), even forced-ON — the gate
    /// never double-wraps an already-distributed plan.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn already_distributed_root_passes_through() {
        let (_tmp, path) = write_fixture();
        let ctx = fixture_ctx(&path).await;
        let plan = physical_plan(&ctx, FIXTURE_SQL).await;

        // Wrap the plan so its root IS a DistributedExec, mimicking a
        // plan that already went through the stage splitter.
        let distributed: Arc<dyn ExecutionPlan> = Arc::new(DistributedExec::new(plan));

        // Forced ON is the arm that would otherwise call the inner rule
        // on the distributed root; the guard must short-circuit it.
        let rule = AdaptiveMeshGateRule::new(MeshGateConfig::forced());
        let out = rule
            .optimize(distributed.clone(), &ConfigOptions::default())
            .expect("optimize");
        assert!(
            Arc::ptr_eq(&distributed, &out),
            "an already-distributed root must pass through unchanged"
        );
    }

    // ---------------- Σ.MG self-join shuffle guard ----------------

    #[test]
    fn max_table_scans_parse_table() {
        assert_eq!(max_table_scans_of(None), DEFAULT_MESH_MAX_TABLE_SCANS);
        assert_eq!(max_table_scans_of(Some("5")), 5);
        assert_eq!(max_table_scans_of(Some("0")), 0); // 0 = guard off
        assert_eq!(
            max_table_scans_of(Some("junk")),
            DEFAULT_MESH_MAX_TABLE_SCANS
        );
    }

    #[test]
    fn auto_decision_veto_table() {
        let big = &[Some(u64::MAX)][..]; // bytes alone say distribute
        let auto = MeshGateConfig::auto_with_min_bytes(0);
        let gib: u64 = 1024 * 1024 * 1024;
        // Below the instance ceiling: bytes decide.
        assert!(auto_decision(big, 1, gib, &auto));
        assert!(auto_decision(big, 2, gib, &auto));
        // At/above the ceiling (default 3): veto regardless of bytes.
        assert!(!auto_decision(big, 3, gib, &auto));
        assert!(!auto_decision(big, 7, gib, &auto));
        // max_table_scans = 0 disables the guard entirely.
        let off = MeshGateConfig {
            max_table_scans: 0,
            ..auto
        };
        assert!(auto_decision(big, 9, gib, &off));
    }

    /// The veto's capacity condition: it only fires while one
    /// instance of the repeated table fits comfortably in RAM. The
    /// two calibration points are the real boxes: SF=100 Q21
    /// (~12.8 GB instance, 32 GB box → veto) and SF=1000
    /// (~128 GB instance, 128 GB box → distribute).
    #[test]
    fn veto_capacity_condition_scales() {
        let big = &[Some(u64::MAX)][..];
        let gib: u64 = 1024 * 1024 * 1024;
        let auto = MeshGateConfig::auto_with_min_bytes(0);
        // SF=100 shape: 12.8 GiB instance on a 32 GiB box → veto.
        let sf100 = MeshGateConfig {
            system_ram_bytes: Some(32 * gib),
            ..auto
        };
        assert!(!auto_decision(big, 3, 13 * gib, &sf100));
        // SF=1000 shape: 128 GiB instance on a 128 GiB box → the
        // page-cache rationale inverts; distribute on bytes.
        let sf1000 = MeshGateConfig {
            system_ram_bytes: Some(128 * gib),
            ..auto
        };
        assert!(auto_decision(big, 3, 128 * gib, &sf1000));
        // Boundary: exactly the fraction still vetoes.
        assert!(!auto_decision(big, 3, 64 * gib, &sf1000));
        // Unknown RAM (non-Linux dev host): veto applies.
        assert!(!auto_decision(big, 3, u64::MAX, &auto));
    }

    /// The Q21 shape in miniature: THREE scan instances of one base
    /// table (inner self-joins). The counter must see 3, and AUTO
    /// must return the single-node plan Arc even though the byte
    /// threshold alone says distribute.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn three_instance_self_join_declines_fanout() {
        let (_tmp, path) = write_fixture();
        let ctx = fixture_ctx(&path).await;
        let plan = physical_plan(
            &ctx,
            "SELECT a.k FROM t a              JOIN t b ON a.k = b.k              JOIN t c ON a.k = c.k",
        )
        .await;
        assert_eq!(
            max_same_table_scan_instances(&plan, 0).0,
            3,
            "three self-join arms = three instances:\n{}",
            displayable(plan.as_ref()).indent(true)
        );
        let rule = AdaptiveMeshGateRule::new(MeshGateConfig::auto_with_min_bytes(0));
        let out = rule
            .optimize(plan.clone(), &ConfigOptions::default())
            .expect("optimize");
        assert!(
            Arc::ptr_eq(&plan, &out),
            "3+ instances of one table must veto fan-out (single-node Arc back)"
        );
    }

    /// Two instances (the Q15 shape) do NOT trip the veto — the
    /// counter reports 2 and the pure decision distributes on bytes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn two_instance_self_join_does_not_veto() {
        let (_tmp, path) = write_fixture();
        let ctx = fixture_ctx(&path).await;
        let plan = physical_plan(&ctx, "SELECT a.k FROM t a JOIN t b ON a.k = b.k").await;
        assert_eq!(max_same_table_scan_instances(&plan, 2).0, 2);
        // Above the per-instance byte floor, small scans don't count
        // at all — 3 copies of a tiny table can never veto.
        assert_eq!(max_same_table_scan_instances(&plan, u64::MAX).0, 0);
        let big = &[Some(u64::MAX)][..];
        assert!(
            auto_decision(big, 2, u64::MAX, &MeshGateConfig::auto_with_min_bytes(0)),
            "2 instances stay distributed"
        );
    }

    /// A parted ematix table (EmatixInterleaveUnionExec over N part
    /// scans) is ONE logical scan instance — the counter must NOT
    /// read its parts as N self-join arms (that would veto every
    /// parted single-table query).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn parted_union_counts_as_one_instance() {
        let tmp = tempfile::tempdir().expect("tempdir");
        // Four part files of one table in one directory.
        for i in 0..4 {
            let path = tmp.path().join(format!("t-{i:04}.parquet"));
            let schema = Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, false),
                Field::new("v", DataType::Float64, false),
            ]));
            let n = 100i64;
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from((0..n).collect::<Vec<_>>())),
                    Arc::new(Float64Array::from(
                        (0..n).map(|x| x as f64).collect::<Vec<_>>(),
                    )),
                ],
            )
            .expect("batch");
            let file = File::create(&path).expect("create");
            let mut w = ArrowWriter::try_new(file, schema, None).expect("writer");
            w.write(&batch).expect("write");
            w.close().expect("close");
        }
        let cfg = SessionConfig::new()
            .with_collect_statistics(true)
            .with_target_partitions(4);
        let builder = ematix_flow_core::preset::with_optimizer_rules(
            SessionStateBuilder::new()
                .with_config(cfg)
                .with_default_features(),
        );
        let ctx = SessionContext::new_with_state(builder.build());
        let prov =
            ematix_flow_core::ematix_fast_parquet_multi::EmatixFastParquetMultiTableProvider::try_new_dir(
                tmp.path(),
            )
            .expect("multi provider");
        ctx.register_table("t", Arc::new(prov)).expect("register");
        let plan = physical_plan(&ctx, "SELECT k, SUM(v) FROM t GROUP BY k").await;
        assert_eq!(
            max_same_table_scan_instances(&plan, 0).0,
            1,
            "a parted union is one instance:\n{}",
            displayable(plan.as_ref()).indent(true)
        );
    }
}
