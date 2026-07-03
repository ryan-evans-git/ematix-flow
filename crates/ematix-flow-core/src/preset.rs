//! Task #481, Shape B: a one-call setup for the dict-aware fast path.
//!
//! Users who want every optimisation ematix-flow exposes shouldn't
//! have to memorise the seven rules + the right `TableProvider` + the
//! right `with_dict_preservation` flag. This module gives them two
//! short calls:
//!
//! ```no_run
//! # async fn demo() -> datafusion::common::Result<()> {
//! use datafusion::execution::session_state::SessionStateBuilder;
//! use datafusion::prelude::{SessionConfig, SessionContext};
//! use ematix_flow_core::preset;
//!
//! let state = preset::with_optimizer_rules(
//!     SessionStateBuilder::new()
//!         .with_config(SessionConfig::new().with_target_partitions(14))
//!         .with_default_features(),
//! )
//! .build();
//! let ctx = SessionContext::new_with_state(state);
//! preset::register_dict_aware_parquet(&ctx, "lineitem", "data/lineitem.parquet")?;
//! # Ok(())
//! # }
//! ```
//!
//! The two halves are split so callers can mix and match — e.g. the
//! TPC-H triangulation bench installs rules but registers the table
//! itself via `FastParquetTableProvider` (different provider mix); a
//! production user might register many parquet tables but only want
//! the rule chain installed once.
//!
//! ## What "dict-aware fast path" means here
//!
//! - **Reader**: [`crate::ematix_fast_parquet::EmatixFastParquetTableProvider`]
//!   with `with_dict_preservation(true)` — string columns arrive as
//!   `Dictionary(UInt32, Utf8)` instead of `Utf8View`, which lets the
//!   dict-aware operators in the rule chain actually fire on TPC-H
//!   data (rather than silently no-op'ing as they would on default
//!   `DataSourceExec`/parquet-rs output — see memory
//!   `[[dict-arrival-blocker]]`).
//! - **Rules**: the canonical chain landed across Σ.D/Σ.E/Σ.G work —
//!   per-shape SQL injection rules + the dict-aware operator rule +
//!   the hand→JIT promoter. Each one is mutually-exclusive-by-design
//!   on the plan shapes it matches; together they cover the
//!   currently-recognised SUM-over-Filter, multi-agg + group-by, and
//!   string-COUNT-GROUP-BY shapes.

use std::sync::Arc;

use datafusion::common::Result as DfResult;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::SessionContext;

use crate::dedupe_aggregate_rule::DedupeAggregateForFloatDeterminism;
use crate::dict_aggregate_rule::EnableDictGroupCountRule;
use crate::ematix_fast_parquet::EmatixFastParquetTableProvider;
use crate::fused_aggregate_filter_multi_agg_rule::InjectFilterMultiAggRule;
use crate::fused_aggregate_filter_sum_rule::InjectFilterSumRule;
use crate::inbloom_scan_pushdown_rule::EnableInBloomScanPushdownRule;
use crate::push_down_left_semi_rule::PushDownLeftSemiRule;
use crate::robin_hood_sum_f64_exec::EnableRobinHoodSumF64Rule;
use crate::runtime_bloom_cascading_rule::EnableCascadingBloomRule;
use crate::runtime_bloom_sideband_rule::EnableRuntimeBloomSidebandRule;
use crate::shared_subtree_exec::SharedSubtreeRegistry;

/// Attach ematix-flow's optimiser rule chain to a `SessionStateBuilder`.
///
/// Rules installed (in registration order):
///
/// 1. `DedupeAggregateForFloatDeterminism` — detects f64-aggregate
///    subtrees that appear twice in the same plan (TPC-H Q15's
///    `revenue_s` CTE shape) and wraps both in `SharedSubtreeExec` so
///    they share one cached computation. **Must register FIRST** —
///    `InjectFilterMultiAggRule` / `InjectFilterSumRule` would
///    otherwise consume one side of the duplicate pair (rewriting it
///    to a fused operator with a different structural hash) and leave
///    the other as the parallel-SUM source of non-determinism. The
///    rule is a no-op on the 21/22 TPC-H queries without duplicate
///    aggregates.
/// 2. `EnableDictGroupCountRule` — `AggregateExec(Final+Partial)` on
///    `Dictionary(UInt32, Utf8|Utf8View)` + `COUNT(*)` →
///    `DictGroupCountExec`. Runs after dedupe because its shape is a
///    strict subset of `InjectFilterMultiAggRule`'s matcher.
/// 3. `InjectFilterMultiAggRule` — generic filter + group-by + multi-
///    aggregate SQL pattern → `FusedAggregateExec<FilterMultiAggSpec>`.
/// 4. `InjectFilterSumRule` — generic SUM-over-Filter SQL →
///    `FusedAggregateExec<FilterSumSpec>`.
/// 5. `SwapSemiJoinBuildSideRule` — when one input to a semi/anti
///    join has stronger cardinality stats than the other, swap so
///    the smaller side becomes build. Closes Q18's inverted
///    build-side bug.
/// 6. `PushDownLeftSemiRule` (Σ.Q.L10, logical) — pushes a top-level
///    LeftSemi join through Inner joins down to the target table,
///    eliminating an N×M intermediate that would otherwise be built
///    first.
/// 7. `EnableRobinHoodSumF64Rule` (Σ.Q.L1b) — routes
///    SUM(Float64)/COUNT GROUP BY Int64 through `RobinHoodSumF64Exec`
///    which beats DataFusion's stock vectorised AggregateExec by 1-5%.
/// 8. `EnableRuntimeBloomSidebandRule` (Σ.Q.L9 / Σ.Q.L15) — threads
///    a runtime bloom between HashJoinExec build and probe-side
///    EmatixFastParquetExec, pruning the probe scan at decode time.
///    Configured with ratio=1024, allow_inner_join=true,
///    require_filtered_build=true (the milestone config).
///
/// Σ.V alignment (2026-05-26): rules 6/7/8 were previously default-on
/// in the bench but missing from this preset — library users got
/// different plans than what we benchmarked. Now aligned.
///
/// The dedupe rule's `SharedSubtreeRegistry` is freshly allocated per
/// call to `with_optimizer_rules` and lives for the resulting
/// `SessionState`. Multiple queries on the same `SessionContext`
/// reuse the same registry — so a repeat Q15 hits the cache populated
/// by the first run.
///
/// If you need to inspect / clear the cache from outside (tests,
/// admin tools), use [`with_optimizer_rules_and_registry`] instead
/// and keep the returned `Arc<SharedSubtreeRegistry>` handle.
pub fn with_optimizer_rules(builder: SessionStateBuilder) -> SessionStateBuilder {
    with_optimizer_rules_and_registry(builder).0
}

/// Same as [`with_optimizer_rules`], but also returns the
/// `SharedSubtreeRegistry` so callers can probe cache state across
/// queries. Useful for benches that want to verify cross-query hits,
/// or for clearing the cache after a table is re-registered with
/// different data.
pub fn with_optimizer_rules_and_registry(
    builder: SessionStateBuilder,
) -> (SessionStateBuilder, Arc<SharedSubtreeRegistry>) {
    let (builder, handles) = with_optimizer_rules_overridden(builder, &HarnessOverrides::default());
    (builder, handles.registry)
}

/// Explicit, named deltas a benchmark / validation harness may layer ON
/// TOP of (or subtract from) the production optimizer chain.
///
/// **`HarnessOverrides::default()` means NO delta**: passing it to
/// [`with_optimizer_rules_overridden`] produces exactly the chain
/// [`with_optimizer_rules`] installs (that function is now literally
/// implemented this way — there is ONE chain-assembly code path, so the
/// preset stays the single source of truth and the Σ.V / Q18-SF=100
/// class of "rule present in one copy, missing from another" drift is
/// structurally impossible for the harnesses that use this API).
///
/// The three field groups:
/// - **opt-outs** (default `true` = install): switch individual
///   production members OFF for A/B isolation (the bench's
///   `EMAT_RULES` / `EMAT_SWAP_SEMI` / ... levers).
/// - **replacements / config overrides** (default `false`/`None` =
///   production member with milestone config): swap an experimental
///   variant in where the production rule would sit, or override one
///   of the L9 sideband's milestone fields.
/// - **diagnostic additions** (default `false` = not installed):
///   harness-only rules that must NEVER ship in the production preset.
///   These are the only names the harness parity tests allowlist.
#[derive(Debug, Clone)]
pub struct HarnessOverrides {
    // ---- production-member opt-outs (true = install; all true in production) ----
    /// Concurrency-aware `target_partitions` (campaign-2026-07-01 §3):
    /// when the builder's config still carries the DataFusion default
    /// (`available_parallelism()`), resolve it through
    /// [`crate::partition_registry::resolve_target_partitions`]
    /// (`EMAT_TARGET_PARTITIONS` tri-state + cross-process AUTO
    /// sensing). A solo process resolves to `available_parallelism()`,
    /// so single-process behavior is bit-identical. Harnesses that
    /// resolve partitions themselves (bench `PARTITIONS` precedence)
    /// set this `false` so the preset never second-guesses them.
    pub auto_target_partitions: bool,
    /// `DedupeAggregateForFloatDeterminism` (bench: `EMAT_RULES=all|dedupe`).
    pub dedupe_aggregate: bool,
    /// The Σ.D/Σ.E inject trio: `EnableDictGroupCountRule` +
    /// `InjectFilterMultiAggRule` + `InjectFilterSumRule`
    /// (bench: `EMAT_RULES=all|v040`).
    pub inject_fused_rules: bool,
    /// `SwapSemiJoinBuildSideRule` (bench: `EMAT_SWAP_SEMI`).
    pub swap_semi_join_build: bool,
    /// `ForceCollectLeftForSemiBoundedBuildRule` (bench/validate:
    /// `EMAT_FORCE_COLLECT_LEFT`).
    pub force_collect_left: bool,
    /// `ClusteredSinglePhaseAggRule` (RANGE.AGG). Note the rule ALSO
    /// self-gates on `EMAT_RANGE_AGG` (default ON), so harnesses keep
    /// this `true` and A/B via the env var.
    pub clustered_single_phase_agg: bool,
    /// `PushDownLeftSemiRule` — logical (bench: `EMAT_PUSH_SEMI`).
    pub push_down_left_semi: bool,
    /// `EnableRobinHoodSumF64Rule` (bench: `EMAT_RH_SUM_F64`).
    pub robin_hood_sum_f64: bool,
    /// `EnableRuntimeBloomSidebandRule` — the Σ.Q.L9/L15 milestone
    /// bloom sideband (bench: `EMAT_RT_BLOOM_SIDEBAND`).
    pub runtime_bloom_sideband: bool,
    /// `FlowQueryPlanner` — the Σ.BR production pre-plan walker
    /// pipeline (agg_semi → dim_push → q20_semi → transitive dim-semi
    /// → shape-gated reorder) + scalar-agg partition boost + the
    /// fd-groupby / late-mat / push-pipeline gated paths. Each step
    /// self-gates on its own env var (`EMAT_AGG_SEMI`, `EMAT_DIM_PUSH`,
    /// `EMAT_Q20_SEMI`, `EMAT_TRANSITIVE_DIM_SEMI`, `EMAT_REORDER_QP`,
    /// ...), so harnesses keep this `true` and A/B via those.
    pub flow_query_planner: bool,

    // ---- replacements / L9 config overrides (None/false = production) ----
    /// REV.8 (bench `EMAT_SINGLE_PASS_RADIX=1`): install
    /// `EnableSinglePassRadixSumRule` INSTEAD of the RobinHood-sum
    /// rewrite (the two match the same Partial aggregate and are
    /// mutually exclusive by design).
    pub single_pass_radix_replaces_rh_sum: bool,
    /// Σ.S.B legacy stem-fanout cascade (bench/validate
    /// `EMAT_L9_CASCADE_STEM=1`): install `EnableCascadingBloomRule`
    /// with `max_extras_per_emitter = n` INSTEAD of the base L9 rule.
    pub l9_cascade_stem_max_extras: Option<usize>,
    /// Validate's legacy `L15=0` lever: install the L9 rule via
    /// `::default()` (pre-L15 env-resolved config: ratio 64, Inner
    /// joins off) instead of the milestone config.
    pub l9_stock_defaults: bool,
    /// Override the milestone `min_probe_to_build_ratio` (1024)
    /// (bench: `EMAT_RT_BLOOM_RATIO`).
    pub l9_min_probe_to_build_ratio: Option<usize>,
    /// Override the milestone `allow_inner_join` (true)
    /// (bench: `EMAT_RT_BLOOM_INNER_JOIN`).
    pub l9_allow_inner_join: Option<bool>,
    /// Override the milestone `require_filtered_build` (true)
    /// (bench: `EMAT_L9_REQUIRE_FILTERED_BUILD`).
    pub l9_require_filtered_build: Option<bool>,

    // ---- harness-only diagnostic additions (false = production) ----
    // Every rule name these can install MUST appear in the harness
    // parity tests' documented allowlists.
    /// Σ.Q.M `SyntheticLeftSemiRule` — logical, must precede
    /// `PushDownLeftSemiRule` (bench/validate `EMAT_SYNTHETIC_LEFT_SEMI=1`).
    pub synthetic_left_semi: bool,
    /// REV.18c `EnableRobinHoodAggregateRule` (bench `EMAT_RH_COUNT=1`).
    pub robin_hood_count: bool,
    /// REV.18c `EnableRobinHoodAvgF64Rule` (bench `EMAT_RH_AVG=1`).
    pub robin_hood_avg: bool,
    /// Σ.AN.1 `AggPartitionBoostRule` (bench `EMAT_AGG_PARTITION_BOOST=1`).
    pub agg_partition_boost: bool,
    /// Σ.AE `DropRedundantBridgeFilterRule` — the install helper itself
    /// no-ops unless `EMAT_DROP_REDUNDANT_FILTER=1` is set.
    pub drop_redundant_filter: bool,
    /// HJ.4 `SwapEmatixHashJoinRule` (validate) — registered last;
    /// dormant unless `EMAT_HASH_JOIN=1`.
    pub swap_emat_hash_join: bool,
    /// Σ.Q.L4′ `EnableInBloomScanPushdownRule` (bench
    /// `EMAT_BLOOM_PUSHDOWN=1`). The installed rule's handle comes back
    /// in [`HarnessHandles::bloom_pushdown`] so the harness can swap
    /// per-query `ContextBlooms` into the shared slot.
    pub in_bloom_scan_pushdown: bool,
}

impl Default for HarnessOverrides {
    /// The PRODUCTION configuration: every production member installed,
    /// no replacements, no diagnostics.
    fn default() -> Self {
        Self {
            auto_target_partitions: true,
            dedupe_aggregate: true,
            inject_fused_rules: true,
            swap_semi_join_build: true,
            force_collect_left: true,
            clustered_single_phase_agg: true,
            push_down_left_semi: true,
            robin_hood_sum_f64: true,
            runtime_bloom_sideband: true,
            flow_query_planner: true,
            single_pass_radix_replaces_rh_sum: false,
            l9_cascade_stem_max_extras: None,
            l9_stock_defaults: false,
            l9_min_probe_to_build_ratio: None,
            l9_allow_inner_join: None,
            l9_require_filtered_build: None,
            synthetic_left_semi: false,
            robin_hood_count: false,
            robin_hood_avg: false,
            agg_partition_boost: false,
            drop_redundant_filter: false,
            swap_emat_hash_join: false,
            in_bloom_scan_pushdown: false,
        }
    }
}

/// Handles the unified constructor hands back to harnesses.
pub struct HarnessHandles {
    /// The dedupe rule's shared-subtree registry (see
    /// [`with_optimizer_rules_and_registry`]).
    pub registry: Arc<SharedSubtreeRegistry>,
    /// Present iff [`HarnessOverrides::in_bloom_scan_pushdown`] was set.
    pub bloom_pushdown: Option<Arc<EnableInBloomScanPushdownRule>>,
}

/// THE single chain-assembly function. [`with_optimizer_rules`] /
/// [`with_optimizer_rules_and_registry`] call it with
/// `HarnessOverrides::default()` (= production); the TPC-H harnesses
/// (`tpch_triangulation_bench`, `tpch_validate`) call it with their
/// env-lever-derived overrides. Production members register in the
/// exact order the preset has always used; harness additions register
/// at their historical bench/validate positions.
pub fn with_optimizer_rules_overridden(
    builder: SessionStateBuilder,
    o: &HarnessOverrides,
) -> (SessionStateBuilder, HarnessHandles) {
    let registry = Arc::new(SharedSubtreeRegistry::new());
    let mut builder = builder;
    // Registry-driven rayon global pool (campaign-2026-07-03): sized
    // once per process at session build so the intra-column-chunk page
    // decode fan-out (emat_arrow_reader's rayon par_iter) tracks the
    // per-process core share instead of defaulting to all cores. NOT
    // gated on `o.auto_target_partitions`: the harness `PARTITIONS`
    // precedence that lever protects governs the session CONFIG only,
    // while the rayon pool is process-global with its own escapes
    // (`RAYON_NUM_THREADS` — rayon-native, we keep hands off — and the
    // `EMAT_RAYON_BUDGET` tri-state, `=0` legacy). Solo AUTO resolves
    // share == cores == rayon's own default: no-op by construction.
    crate::partition_registry::size_rayon_pool_once();
    // Concurrency-aware target_partitions (campaign-2026-07-01 §3):
    // runs FIRST so the resolved partition count is in place before any
    // rule or planner reads the config. Only rewrites a config still at
    // the DataFusion default (available_parallelism()) — an explicit
    // caller `with_target_partitions(k != cores)` is always preserved.
    if o.auto_target_partitions {
        builder = apply_concurrency_aware_target_partitions(builder);
    }
    if o.dedupe_aggregate {
        builder = builder.with_physical_optimizer_rule(Arc::new(
            DedupeAggregateForFloatDeterminism::with_registry(registry.clone()),
        ));
    }
    if o.inject_fused_rules {
        builder = builder
            .with_physical_optimizer_rule(Arc::new(EnableDictGroupCountRule))
            .with_physical_optimizer_rule(Arc::new(InjectFilterMultiAggRule))
            .with_physical_optimizer_rule(Arc::new(InjectFilterSumRule));
    }
    if o.swap_semi_join_build {
        builder = builder.with_physical_optimizer_rule(Arc::new(
            crate::swap_semi_join_build_rule::SwapSemiJoinBuildSideRule,
        ));
    }
    // ForceCollectLeftForSemiBoundedBuildRule (REV.3): forces
    // PartitionMode::CollectLeft on Inner joins whose build side is
    // semi-bounded (shrunk by a pushed LeftSemi/RightSemi), then
    // re-runs EnforceDistribution to drop the now-unnecessary
    // probe-side repartition (the Σ.BS "repair partitioning after a
    // structural rewrite" pattern). Q18 SF=100: 6652 -> 398 ms
    // (16.7x), value-validated — all 22 queries PASS vs DuckDB at
    // SF=100 with this on. Runs right after SwapSemi so it sees the
    // RightSemi. Was opt-in (EMAT_FORCE_COLLECT_LEFT); now default-on
    // to match the settled decision + ship the banked win.
    if o.force_collect_left {
        builder = builder.with_physical_optimizer_rule(Arc::new(
            crate::force_collect_left_semi_build_rule::ForceCollectLeftForSemiBoundedBuildRule::default(),
        ));
    }
    // RANGE.AGG (2026-06-10): collapse Partial→hash-shuffle→Final
    // into one SinglePartitioned aggregate when the single group
    // key is provably the file's cluster key (row-group min/max
    // ranges monotone; splits placed only at strict key gaps so no
    // group spans a partition). Q18 SF=100: the 600M-row/150M-group
    // SUM drops from 43.5s CPU (two-phase + 2.2 GB shuffle) to one
    // private-table pass per partition. Declines (stock plan) on
    // any non-clustered key. Opt-out EMAT_RANGE_AGG=0.
    if o.clustered_single_phase_agg {
        builder = builder.with_physical_optimizer_rule(Arc::new(
            crate::clustered_agg_rule::ClusteredSinglePhaseAggRule,
        ));
    }
    // Σ.Q.M SyntheticLeftSemiRule (harness diagnostic): logical
    // producer that must precede the Σ.Q.L10 consumer — DataFusion
    // runs custom logical rules in registration order.
    if o.synthetic_left_semi {
        builder = builder.with_optimizer_rule(Arc::new(
            crate::synthetic_left_semi_rule::SyntheticLeftSemiRule,
        ));
    }
    // Σ.V (2026-05-26): align preset with the bench's milestone
    // config. The bench has these three rules default-on but
    // preset.rs was missing them — library users got materially
    // different plan shapes than what we benchmarked. See
    // [[Σ.V — preset alignment]].
    //
    // PushDownLeftSemiRule (Σ.Q.L10): LOGICAL rule that pushes a
    // top-level LeftSemi join through Inner joins down to its
    // target table, eliminating the N×M intermediate that would
    // otherwise be built first. Closes Q18-shape gap to DuckDB
    // when the rule was introduced (memory: Q18 SF=10 -54% at
    // commit 50825c9, before other levers stacked).
    if o.push_down_left_semi {
        builder = builder.with_optimizer_rule(Arc::new(PushDownLeftSemiRule));
    }
    // EnableRobinHoodSumF64Rule (Σ.Q.L1b): routes
    // SUM(Float64)/COUNT GROUP BY Int64 through RobinHoodSumF64Exec
    // which beats DataFusion's stock vectorised AggregateExec by 1-5%
    // (memory: project_sigma_nf3_beats_stock). REV.8 (harness A/B):
    // `single_pass_radix_replaces_rh_sum` swaps in the single-pass
    // radix aggregate — the two match the same AggregateExec(Partial)
    // and are mutually exclusive by design.
    if o.single_pass_radix_replaces_rh_sum {
        builder = builder.with_physical_optimizer_rule(Arc::new(
            crate::single_pass_radix_sum_exec::EnableSinglePassRadixSumRule::default(),
        ));
    } else if o.robin_hood_sum_f64 {
        builder =
            builder.with_physical_optimizer_rule(Arc::new(EnableRobinHoodSumF64Rule::default()));
    }
    // REV.18c harness diagnostics: COUNT(*)/AVG(f64) GROUP BY i64
    // RobinHood kernels — opt-in only (no TPC-H SF=10 shape lands in
    // the 200K-256K group band where they win).
    if o.robin_hood_count {
        builder = builder.with_physical_optimizer_rule(Arc::new(
            crate::robin_hood_agg_rule::EnableRobinHoodAggregateRule::default(),
        ));
    }
    if o.robin_hood_avg {
        builder = builder.with_physical_optimizer_rule(Arc::new(
            crate::robin_hood_avg_f64_exec::EnableRobinHoodAvgF64Rule::default(),
        ));
    }
    // Σ.AN.1 harness diagnostic: per-operator partition routing for
    // high-cardinality FinalPartitioned aggregates.
    if o.agg_partition_boost {
        builder = builder.with_physical_optimizer_rule(Arc::new(
            crate::agg_partition_boost::AggPartitionBoostRule,
        ));
    }
    // EnableRuntimeBloomSidebandRule (Σ.Q.L9 / Σ.Q.L15):
    // threads a runtime bloom sideband between HashJoinExec
    // build and probe-side EmatixFastParquetExec. The
    // milestone config uses ratio=1024 (gates out L4'-style
    // net-negative s⋈l firings while keeping small-dim→fact
    // wins), allow_inner_join=true (Σ.Q.L15), and
    // require_filtered_build=true (don't bloom on FK joins —
    // [[l9_bloom_consumer_findings]]). The remaining fields track
    // `::default()`:
    // - max_expected_keys_per_partition — Σ.AH.3 Story 2a ceiling,
    //   OPT-IN (0 = disabled) via `EMAT_L9_MAX_EXPECTED_KEYS=N`.
    // - min_probe_proj_cols — L9.WIDTH probe-payoff gate,
    //   env-defaulted (`EMAT_L9_MIN_PROBE_PROJ_COLS`, default 0).
    // - ndv_max_rows — Σ.AH.2 NDV-walk file ceiling, env-defaulted
    //   (10M; 32M under EMAT_L9_PARTITIONED=1).
    // - cascade — Σ.Q05.CHAIN cascade-chain second phase, tri-state
    //   env gated (EMAT_L9_CASCADE / EMAT_MULTIKEY_BLOOM, both AUTO).
    if o.runtime_bloom_sideband {
        let ratio = o.l9_min_probe_to_build_ratio.unwrap_or(1024);
        let allow_inner_join = o.l9_allow_inner_join.unwrap_or(true);
        let require_filtered_build = o.l9_require_filtered_build.unwrap_or(true);
        if let Some(max_extras) = o.l9_cascade_stem_max_extras {
            // Σ.S.B legacy stem-fanout cascade (harness A/B) replaces
            // the base rule; carries the same milestone gate config.
            let rule = if o.l9_stock_defaults {
                EnableCascadingBloomRule::default()
            } else {
                EnableCascadingBloomRule {
                    min_probe_to_build_ratio: ratio,
                    allow_inner_join,
                    require_filtered_build,
                    max_extras_per_emitter: max_extras,
                }
            };
            builder = builder.with_physical_optimizer_rule(Arc::new(rule));
        } else {
            let rule = if o.l9_stock_defaults {
                EnableRuntimeBloomSidebandRule::default()
            } else {
                EnableRuntimeBloomSidebandRule {
                    min_probe_to_build_ratio: ratio,
                    allow_inner_join,
                    require_filtered_build,
                    ..EnableRuntimeBloomSidebandRule::default()
                }
            };
            builder = builder.with_physical_optimizer_rule(Arc::new(rule));
        }
    }
    // Σ.AE harness diagnostic: the install helper itself no-ops unless
    // `EMAT_DROP_REDUNDANT_FILTER=1` is set at construction time.
    if o.drop_redundant_filter {
        builder = crate::drop_redundant_filter_rule::install_drop_redundant_filter_rule(builder);
    }
    // Σ.AJ.1 Lever B POC: opt-in via EMAT_L9_BROADCAST_SIBLINGS=1.
    // Default OFF. See `crates/ematix-flow-core/src/broadcast_sibling_blooms_rule.rs`.
    // (Runs after the L9 sideband rule so it can broadcast existing
    // bloom emitters to sibling same-parquet-path scans.)
    let mut builder = if crate::flags::present("EMAT_L9_BROADCAST_SIBLINGS") {
        crate::broadcast_sibling_blooms_rule::install_broadcast_sibling_blooms_rule(builder)
    } else {
        builder
    };
    // Σ.Q.L4′ harness diagnostic: in-scan bloom pushdown with an empty
    // shared bloom slot; the bench swaps `ContextBlooms` in per query.
    let bloom_pushdown: Option<Arc<EnableInBloomScanPushdownRule>> = if o.in_bloom_scan_pushdown {
        let rule = Arc::new(EnableInBloomScanPushdownRule::default());
        builder = builder.with_physical_optimizer_rule(rule.clone());
        Some(rule)
    } else {
        None
    };
    // HJ.4 harness diagnostic: SIMD-tag/RobinHood join-probe swap.
    // Registered LAST so the join's partition_mode is already assigned;
    // dormant unless EMAT_HASH_JOIN=1.
    if o.swap_emat_hash_join {
        builder = builder.with_physical_optimizer_rule(Arc::new(
            crate::swap_emat_hash_join_rule::SwapEmatixHashJoinRule,
        ));
    }
    // Σ.BR Phase 2 / #194 (2026-05-29): production wiring for the ematix
    // pre-plan walker pipeline (agg_semi → dim_push → reorder). These walkers
    // were previously applied only in the bench harness, so library users got
    // a different (slower) plan regime for the queries that use them
    // (Q17/Q08/Q18 via agg_semi, Q10 via dim_push, Q05 via reorder). Install
    // them as a QueryPlanner (NOT OptimizerRules — that would re-open the 5–8%
    // codegen-tax, [[optimizer-codegen-sensitivity]]) so they run
    // post-optimization, matching the validated bench config. Each step is
    // self-gated inside FlowQueryPlanner (EMAT_AGG_SEMI / EMAT_DIM_PUSH /
    // EMAT_REORDER_QP, all default ON, opt-OUT). Installed unless all three
    // are disabled.
    // FlowQueryPlanner also carries the scalar-agg partition-oversubscription
    // boost (morsel down-payment, 2026-06-20), which is independent of the
    // three walkers — so keep the planner installed when that lever is on even
    // if all three walkers are disabled.
    let flow_qp_on = o.flow_query_planner
        && (["EMAT_AGG_SEMI", "EMAT_DIM_PUSH", "EMAT_REORDER_QP"]
            .iter()
            .any(|var| crate::flags::enabled(var))
            || crate::auto_target_partitions::scalar_agg_boost_enabled());
    let builder = if flow_qp_on {
        builder.with_query_planner(Arc::new(crate::flow_query_planner::FlowQueryPlanner))
    } else {
        builder
    };
    (
        builder,
        HarnessHandles {
            registry,
            bloom_pushdown,
        },
    )
}

/// Concurrency-aware `target_partitions` for the preset path
/// (campaign-2026-07-01 §3): if the builder's `SessionConfig` still
/// carries DataFusion's eager default (`available_parallelism()` —
/// indistinguishable from a caller explicitly choosing full cores),
/// replace it with [`crate::partition_registry::resolve_target_partitions`]:
/// `EMAT_TARGET_PARTITIONS=N` forces N, `=0` keeps legacy full cores,
/// unset senses live ematix processes and resolves
/// `clamp(cores / live, 2, cores)`. A solo process resolves to `cores`,
/// leaving the config bit-identical. Registry failures degrade silently
/// to legacy (the resolver's contract) — this function never panics.
fn apply_concurrency_aware_target_partitions(
    mut builder: SessionStateBuilder,
) -> SessionStateBuilder {
    let cores = crate::partition_registry::available_cores();
    let cfg = builder
        .config()
        .get_or_insert_with(datafusion::prelude::SessionConfig::new);
    if cfg.target_partitions() == cores {
        let resolved = crate::partition_registry::resolve_target_partitions();
        if resolved >= 1 && resolved != cores {
            cfg.options_mut().execution.target_partitions = resolved;
        }
    }
    builder
}

/// Register a parquet file as a table on `ctx` via
/// `EmatixFastParquetTableProvider` with dict preservation enabled.
///
/// Two differences from bare `SessionContext::register_parquet`:
///
/// 1. The reader is the ematix-parquet-backed
///    `EmatixFastParquetTableProvider` rather than DataFusion's default
///    parquet integration. This gives us the row-group-parallel scan,
///    bitmap-driven predicate pushdown, and direct Arrow-bridge path
///    that DataFusion's default doesn't expose.
/// 2. `with_dict_preservation(true)` is set, so Utf8 columns arrive as
///    `Dictionary(UInt32, Utf8)` at the Arrow surface. This is the
///    precondition for `EnableDictGroupCountRule` (and future dict-aware
///    operators) to actually fire on real TPC-H data — see the
///    `[[dict-arrival-blocker]]` memory note for the history.
///
/// Pair with [`with_optimizer_rules`] on the same `SessionStateBuilder`
/// before constructing the `SessionContext` — see the module-level
/// example.
pub fn register_dict_aware_parquet(
    ctx: &SessionContext,
    name: &str,
    path: impl Into<String>,
) -> DfResult<()> {
    let prov = EmatixFastParquetTableProvider::try_new(path)?.with_dict_preservation(true);
    ctx.register_table(name, Arc::new(prov))?;
    Ok(())
}

/// The production preset's ematix rule chain, BY NAME, in registration
/// order — the pinned source of truth the harness parity tests compare
/// against. **Adding/removing a rule in
/// [`with_optimizer_rules_overridden`] requires updating this list**
/// (the `production_chain_matches_pinned_names` test fails otherwise),
/// which is the deliberate tripwire: every chain change is now a
/// conscious, single-site act that the harnesses inherit automatically.
pub const PRODUCTION_PHYSICAL_RULE_NAMES: &[&str] = &[
    "ematix_flow_dedupe_aggregate_for_float_determinism",
    "ematix_flow_enable_dict_group_count",
    "ematix_flow_inject_filter_multi_agg",
    "ematix_flow_inject_filter_sum",
    "swap_semi_join_build_side",
    "ematix_flow_force_collect_left_semi_bounded_build",
    "ematix_flow_clustered_single_phase_agg",
    "ematix_flow_enable_robin_hood_sum_f64",
    "ematix_flow_enable_runtime_bloom_sideband",
];

/// The production preset's custom LOGICAL optimizer rules, by name, in
/// registration order (see [`PRODUCTION_PHYSICAL_RULE_NAMES`]).
pub const PRODUCTION_LOGICAL_RULE_NAMES: &[&str] = &["ematix_flow_push_down_left_semi"];

/// Extract the non-stock (ematix-added) physical + logical optimizer
/// rule names from `state`, in registration order. "Non-stock" =
/// not present in a bare `SessionStateBuilder::new().with_default_features()`
/// session — so ANY custom rule is caught regardless of its naming
/// convention. Used by the preset pin test and the harness parity tests.
pub fn ematix_rule_names(
    state: &datafusion::execution::session_state::SessionState,
) -> (Vec<String>, Vec<String>) {
    use std::collections::HashSet;
    let stock = SessionStateBuilder::new().with_default_features().build();
    let stock_physical: HashSet<String> = stock
        .physical_optimizers()
        .iter()
        .map(|r| r.name().to_string())
        .collect();
    let stock_logical: HashSet<String> = stock
        .optimizers()
        .iter()
        .map(|r| r.name().to_string())
        .collect();
    let physical = state
        .physical_optimizers()
        .iter()
        .map(|r| r.name().to_string())
        .filter(|n| !stock_physical.contains(n))
        .collect();
    let logical = state
        .optimizers()
        .iter()
        .map(|r| r.name().to_string())
        .filter(|n| !stock_logical.contains(n))
        .collect();
    (physical, logical)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::SessionConfig;
    use std::path::PathBuf;

    /// The single-source-of-truth tripwire (2026-07-02 hardening, see
    /// docs/PERF_Q18.md § Hardening): the production chain assembled by
    /// `with_optimizer_rules` must match the pinned name lists above.
    /// If you add/remove/rename a preset rule, update the pinned consts
    /// — the TPC-H harnesses (tpch_triangulation_bench, tpch_validate)
    /// build through `with_optimizer_rules_overridden` and inherit the
    /// change automatically; their own parity tests re-check against
    /// the preset session at default env.
    #[test]
    fn production_chain_matches_pinned_names() {
        let state =
            with_optimizer_rules(SessionStateBuilder::new().with_default_features()).build();
        let (physical, logical) = ematix_rule_names(&state);
        assert_eq!(
            physical, PRODUCTION_PHYSICAL_RULE_NAMES,
            "production preset physical chain drifted from the pinned list — \
             update PRODUCTION_PHYSICAL_RULE_NAMES (and check the harnesses' \
             parity tests still pass)"
        );
        assert_eq!(
            logical, PRODUCTION_LOGICAL_RULE_NAMES,
            "production preset logical chain drifted from the pinned list"
        );
        // FlowQueryPlanner ships by default (its walkers self-gate on
        // EMAT_AGG_SEMI / EMAT_DIM_PUSH / EMAT_REORDER_QP, all default ON).
        assert_eq!(
            format!("{:?}", state.query_planner()),
            "FlowQueryPlanner",
            "production preset must install FlowQueryPlanner by default"
        );
    }

    /// `HarnessOverrides::default()` == production: the overridden
    /// constructor with no deltas produces the identical chain (and no
    /// bloom-pushdown handle).
    #[test]
    fn default_overrides_equal_production_chain() {
        let prod = with_optimizer_rules(SessionStateBuilder::new().with_default_features()).build();
        let (builder, handles) = with_optimizer_rules_overridden(
            SessionStateBuilder::new().with_default_features(),
            &HarnessOverrides::default(),
        );
        let over = builder.build();
        assert_eq!(ematix_rule_names(&prod), ematix_rule_names(&over));
        assert_eq!(
            format!("{:?}", prod.query_planner()),
            format!("{:?}", over.query_planner()),
        );
        assert!(handles.bloom_pushdown.is_none());
    }

    /// Concurrency-aware partitions: an explicit, non-default
    /// `with_target_partitions(k)` must survive the preset untouched
    /// (the auto hook only rewrites the DataFusion-default config), and
    /// `auto_target_partitions: false` must leave ANY config untouched
    /// — the harnesses rely on that for `PARTITIONS` precedence.
    #[test]
    fn explicit_target_partitions_survive_preset() {
        let cores = crate::partition_registry::available_cores();
        // Pick an explicit value that cannot collide with the default.
        let explicit = if cores == 4 { 3 } else { 4 };

        // auto ON (production default): explicit non-default preserved.
        let state = with_optimizer_rules(
            SessionStateBuilder::new()
                .with_config(SessionConfig::new().with_target_partitions(explicit))
                .with_default_features(),
        )
        .build();
        assert_eq!(
            state.config().target_partitions(),
            explicit,
            "explicit non-default target_partitions must never be rewritten"
        );

        // auto OFF (harness mode): even a config at the default
        // (available_parallelism) is left alone.
        let (builder, _handles) = with_optimizer_rules_overridden(
            SessionStateBuilder::new()
                .with_config(SessionConfig::new().with_target_partitions(cores))
                .with_default_features(),
            &HarnessOverrides {
                auto_target_partitions: false,
                ..HarnessOverrides::default()
            },
        );
        assert_eq!(
            builder.build().config().target_partitions(),
            cores,
            "auto_target_partitions: false must leave the config untouched"
        );
    }

    fn sf1_lineitem() -> Option<String> {
        let env = std::env::var("TPCH_DATA_DIR").ok().map(PathBuf::from);
        let dir = match env {
            Some(p) if p.exists() => p,
            _ => {
                let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
                let real = manifest.parent()?.parent()?.join("examples/tpch/data/sf1");
                if real.exists() {
                    real
                } else {
                    PathBuf::from(crate::test_support::tpch_mini_dir())
                }
            }
        };
        let p = dir.join("lineitem.parquet");
        p.exists().then(|| p.to_string_lossy().into_owned())
    }

    /// The preset's reader emits `Dictionary(UInt32, Utf8)` for string
    /// columns. (Without the preset users get Utf8View from bare
    /// FastParquet / default DataFusion — verified by
    /// `examples/probe_dict_arrival`.)
    #[tokio::test(flavor = "multi_thread")]
    async fn register_dict_aware_parquet_emits_dictionary_for_string_columns() {
        use datafusion::arrow::datatypes::DataType;

        let Some(path) = sf1_lineitem() else {
            eprintln!("TPC-H SF=1 data not generated; skipping test");
            return;
        };
        let ctx = SessionContext::new();
        register_dict_aware_parquet(&ctx, "lineitem", path).unwrap();
        let provider = ctx.table_provider("lineitem").await.unwrap();
        let schema = provider.schema();
        let returnflag = schema.field_with_name("l_returnflag").unwrap();
        assert!(
            matches!(returnflag.data_type(), DataType::Dictionary(_, _)),
            "l_returnflag should be Dictionary, got {:?}",
            returnflag.data_type()
        );
    }

    /// End-to-end: with the preset's rule chain + dict-aware provider,
    /// a `COUNT(*) GROUP BY string_col` query routes through
    /// `DictGroupCountExec`. Without the preset, that rewrite no-ops
    /// because the scan emits Utf8View.
    #[tokio::test(flavor = "multi_thread")]
    async fn preset_activates_dict_group_count_exec_end_to_end() {
        let Some(path) = sf1_lineitem() else {
            eprintln!("TPC-H SF=1 data not generated; skipping test");
            return;
        };
        let state = with_optimizer_rules(
            SessionStateBuilder::new()
                .with_config(SessionConfig::new().with_target_partitions(14))
                .with_default_features(),
        )
        .build();
        let ctx = SessionContext::new_with_state(state);
        register_dict_aware_parquet(&ctx, "lineitem", path).unwrap();

        let plan = ctx
            .sql("SELECT l_returnflag, COUNT(*) FROM lineitem GROUP BY l_returnflag")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let plan_str = displayable(plan.as_ref()).indent(true).to_string();
        assert!(
            plan_str.contains("DictGroupCountExec"),
            "preset didn't activate the dict-aware operator.\nPlan:\n{plan_str}"
        );
    }

    /// BF (2026-06-09): caching is now PER-QUERY (the dedupe rule allocates a
    /// fresh registry per `optimize()` call). The externally-exposed registry
    /// handle from `with_optimizer_rules_and_registry` therefore stays EMPTY —
    /// cross-query caching was removed because it degraded long-lived contexts
    /// (accumulation) and served stale memoized results. Two Q15-shape queries
    /// on the same ctx must each return 1 row (within-query CSE still works),
    /// and the handle must remain empty (no cross-query accumulation).
    #[tokio::test(flavor = "multi_thread")]
    async fn registry_handle_stays_empty_caching_is_per_query() {
        use datafusion::arrow::array::{Float64Array, Int64Array, RecordBatch};
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::datasource::MemTable;

        let schema = Arc::new(Schema::new(vec![
            Field::new("supplier", DataType::Int64, false),
            Field::new("revenue", DataType::Float64, false),
        ]));
        let mut suppliers: Vec<i64> = Vec::new();
        let mut revenues: Vec<f64> = Vec::new();
        for s in 0..14_i64 {
            for r in 0..100_i64 {
                suppliers.push(s);
                revenues.push((r as f64 + 1.0) * 0.1 + (s as f64) * 17.3);
            }
        }
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(suppliers)),
                Arc::new(Float64Array::from(revenues)),
            ],
        )
        .unwrap();
        let mt = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap());

        let (builder, registry) = with_optimizer_rules_and_registry(
            SessionStateBuilder::new()
                .with_config(SessionConfig::new().with_target_partitions(4))
                .with_default_features(),
        );
        let ctx = SessionContext::new_with_state(builder.build());
        ctx.register_table("revenue_t", mt).unwrap();

        let sql = "
            WITH r AS (
                SELECT supplier, sum(revenue) AS total
                FROM revenue_t
                GROUP BY supplier
            )
            SELECT r.supplier, r.total
            FROM r
            WHERE r.total = (SELECT max(total) FROM r)
        ";

        assert_eq!(registry.len(), 0, "registry handle starts empty");
        let n1: usize = ctx
            .sql(sql)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(n1, 1, "Q15-shape returns one row (within-query CSE works)");
        assert_eq!(
            registry.len(),
            0,
            "external registry handle stays empty — caching is per-query, not cross-query",
        );

        let n2: usize = ctx
            .sql(sql)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(
            n2, 1,
            "second run independently correct (no stale memoization)"
        );
        assert_eq!(
            registry.len(),
            0,
            "registry handle still empty after a second query (no cross-query accumulation)",
        );
    }

    /// Inverse: without `register_dict_aware_parquet` (using bare
    /// DataFusion `register_parquet`), the dict-aware operator does NOT
    /// fire even with the rule chain installed — the scan emits
    /// Utf8View which the rule correctly rejects. Documents the
    /// substrate dependency.
    #[tokio::test(flavor = "multi_thread")]
    async fn preset_rules_alone_do_not_activate_dict_path_on_default_reader() {
        let Some(path) = sf1_lineitem() else {
            eprintln!("TPC-H SF=1 data not generated; skipping test");
            return;
        };
        let state = with_optimizer_rules(
            SessionStateBuilder::new()
                .with_config(SessionConfig::new().with_target_partitions(14))
                .with_default_features(),
        )
        .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_parquet("lineitem", &path, Default::default())
            .await
            .unwrap();

        let plan = ctx
            .sql("SELECT l_returnflag, COUNT(*) FROM lineitem GROUP BY l_returnflag")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let plan_str = displayable(plan.as_ref()).indent(true).to_string();
        assert!(
            !plan_str.contains("DictGroupCountExec"),
            "dict-aware operator wrongly fired without dict-preserving reader.\nPlan:\n{plan_str}"
        );
    }
}
