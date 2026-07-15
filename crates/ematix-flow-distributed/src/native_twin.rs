//! Σ.TW.1 — native single-node twin routing for AUTO's local commits.
//!
//! Design: [`docs/ADR_NATIVE_TWIN_ROUTING.md`]. Evidence: the SF100
//! 4-leg A/B (STAMP `20260715T113833Z`) — a distributed session's
//! LOCAL commits pay ~1.9 s across Q21/Q15/Q05/Q11 versus the native
//! `NO_DISTRIBUTE` session on the same fleet, and the dominant lever
//! (the KEYS.2 i32-key downcast) is a PLANNING-time schema choice the
//! commit-time localize rewrite (Σ.Q15.LS) structurally cannot
//! retrofit: narrowing changes the provider's output schema, which the
//! localize rule's load-bearing `schema_check` forbids.
//!
//! So: when the distributed session's plan for a query is a local
//! commit AND contains a join, don't execute the localized plan —
//! re-plan the SQL in a **native twin**: a single-node session
//! carrying the full production single-node preset with every table
//! re-registered through the NATIVE fast-parquet provider. The twin
//! *is* the native configuration (same preset constructor, same
//! provider constructors), so it cannot drift from it — the #218/#219
//! lesson was that hand-assembled approximations of "native" go inert.
//!
//! Local commits WITHOUT joins keep the localize path: measured faster
//! there at SF100 (Q01 −706 ms, Q06 −262 ms — no join keys to narrow,
//! no second planning pass).
//!
//! Distinct from [`crate::bloom_emitter::single_node_emission_ctx`]:
//! the emission twin clones the distributed session's (stock arrow-rs)
//! providers because it only pre-executes small build sides; this twin
//! re-registers NATIVE providers because it runs whole queries.

use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::common::Result as DfResult;
use datafusion::datasource::listing::ListingTable;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};

/// Build the native twin of `ctx`: the production single-node preset
/// (default [`ematix_flow_core::preset::HarnessOverrides`] — grace ON,
/// auto target partitions, runtime blooms; `collect_statistics(true)`
/// to match the campaign/native session shape) with every catalog
/// table re-registered through [`nativize_provider`].
pub async fn native_twin_ctx(ctx: &SessionContext) -> DfResult<SessionContext> {
    let cfg = SessionConfig::new().with_collect_statistics(true);
    let builder = ematix_flow_core::preset::with_optimizer_rules(
        SessionStateBuilder::new()
            .with_config(cfg)
            .with_default_features(),
    );
    let twin = SessionContext::new_with_state(builder.build());
    for cat_name in ctx.catalog_names() {
        let Some(cat) = ctx.catalog(&cat_name) else {
            continue;
        };
        for schema_name in cat.schema_names() {
            let Some(schema) = cat.schema(&schema_name) else {
                continue;
            };
            for table_name in schema.table_names() {
                if let Ok(Some(provider)) = schema.table(&table_name).await {
                    // Same-name collisions across catalogs: last
                    // registration wins, matching lookup order (the
                    // emission-twin precedent).
                    let _ = twin.register_table(&table_name, nativize_provider(provider));
                }
            }
        }
    }
    Ok(twin)
}

/// Map a provider onto its NATIVE fast-parquet equivalent:
/// - already a fast provider → unchanged;
/// - a stock [`ListingTable`] over a single local `file://` root →
///   directory ⇒ [`EmatixFastParquetMultiTableProvider::try_new_dir`],
///   file ⇒ [`EmatixFastParquetTableProvider::try_new`] — the SAME
///   constructors the native campaign leg uses, so the KEYS.2
///   downcast auto-gate and every other provider-level lever apply;
/// - anything else (remote object store, non-parquet, unsupported
///   column type) → unchanged. The twin must stay total: an
///   unaccelerated table is correct, a missing one is not.
///
/// [`EmatixFastParquetMultiTableProvider::try_new_dir`]:
///     ematix_flow_core::ematix_fast_parquet_multi::EmatixFastParquetMultiTableProvider::try_new_dir
/// [`EmatixFastParquetTableProvider::try_new`]:
///     ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider::try_new
fn nativize_provider(provider: Arc<dyn TableProvider>) -> Arc<dyn TableProvider> {
    use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
    use ematix_flow_core::ematix_fast_parquet_multi::EmatixFastParquetMultiTableProvider;

    let any = provider.as_any();
    if any.is::<EmatixFastParquetTableProvider>() || any.is::<EmatixFastParquetMultiTableProvider>()
    {
        return provider;
    }
    let Some(listing) = any.downcast_ref::<ListingTable>() else {
        return provider;
    };
    let [url] = listing.table_paths().as_slice() else {
        return provider;
    };
    if url.scheme() != "file" {
        return provider;
    }
    // ListingTableUrl prefixes are object_store paths — rooted, no
    // leading '/' (same convention localize_scans handles).
    let local = std::path::PathBuf::from(format!("/{}", url.prefix()));
    let native: Option<Arc<dyn TableProvider>> = if local.is_dir() {
        EmatixFastParquetMultiTableProvider::try_new_dir(&local)
            .ok()
            .map(|p| Arc::new(p) as Arc<dyn TableProvider>)
    } else {
        EmatixFastParquetTableProvider::try_new(local.to_string_lossy().to_string())
            .ok()
            .map(|p| Arc::new(p) as Arc<dyn TableProvider>)
    };
    match native {
        Some(p) => p,
        None => {
            tracing::debug!(
                url = url.as_str(),
                "Σ.TW.1: fast provider construction declined; keeping stock provider"
            );
            provider
        }
    }
}

/// Walk the optimized physical plan for the datafusion-distributed
/// stage/flight boundary nodes. Node names from the
/// datafusion-distributed 1.0.0 source (not guessed):
/// `DistributedExec` is the root wrapper the stage splitter installs
/// around every plan it actually splits, with `NetworkShuffleExec` /
/// `NetworkCoalesceExec` / `NetworkBroadcastExec` as the Arrow Flight
/// boundaries inside the stages. Matching any of them ⇒ the query
/// runs on the mesh. The splitter returns the ORIGINAL plan when no
/// stage split happens, so "peers configured + EMAT_MESH=1" can still
/// legitimately be a local commit.
pub fn plan_is_mesh(plan: &Arc<dyn ExecutionPlan>) -> bool {
    if matches!(
        plan.name(),
        "DistributedExec" | "NetworkShuffleExec" | "NetworkCoalesceExec" | "NetworkBroadcastExec"
    ) {
        return true;
    }
    plan.children().into_iter().any(plan_is_mesh)
}

/// Any physical join operator in the tree (`HashJoinExec`,
/// `SortMergeJoinExec`, `NestedLoopJoinExec`, `CrossJoinExec`,
/// `GraceHashJoinExec`, ... — every DataFusion join operator carries
/// "Join" in its `name()`). Descends through
/// [`SharedSubtreeExec`](ematix_flow_core::shared_subtree_exec::SharedSubtreeExec),
/// which hides its input from `children()` (Q15's shared scalar
/// subquery lives there).
pub fn plan_has_join(plan: &Arc<dyn ExecutionPlan>) -> bool {
    use ematix_flow_core::shared_subtree_exec::SharedSubtreeExec;
    if plan.name().contains("Join") {
        return true;
    }
    if let Some(shared) = plan.as_any().downcast_ref::<SharedSubtreeExec>() {
        if plan_has_join(shared.input()) {
            return true;
        }
    }
    plan.children().into_iter().any(plan_has_join)
}

/// The Σ.TW.1 routing predicate: a locally-committed plan with a join
/// runs in the native twin; everything else stays where it is (mesh
/// plans on the mesh, scan-aggregates on the localize path).
pub fn should_route_to_twin(plan: &Arc<dyn ExecutionPlan>) -> bool {
    !plan_is_mesh(plan) && plan_has_join(plan)
}
