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

    // Collect every table's provider FIRST (catalog iteration order is
    // the catalog's, not ours — table_names() gives no ordering
    // guarantee).
    let mut pending: Vec<(String, Arc<dyn TableProvider>)> = Vec::new();
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
                    pending.push((table_name, provider));
                }
            }
        }
    }

    // Σ.TW.SCHEMA (2026-07-17): fold EVERY table into the scale
    // high-water mark BEFORE constructing any native provider. KEYS.2
    // narrowing is decided at construction time from that mark, so
    // per-table observation makes a table's key width depend on WHEN it
    // was built: build `region` before `lineitem` and neither narrows;
    // build it after and `region` alone narrows. The mixed schema puts a
    // CAST on the join key, and a CAST key is not a plain equijoin — at
    // SF=1000 that pushed Q05's `region ⋈ nation` from FIRST to LAST in
    // the join tree, so the ASIA filter stopped pruning and the twin
    // materialized a full-scale join (>250 GB, kernel OOM) for a query
    // the single leg answers in 21.8 s. Observing the dataset up front
    // makes the decision order-independent — the same remedy Σ.PK.1
    // applies across a table's parts, one level up.
    // (`nativize_provider` also observes per file; that fold is then a
    // no-op against the already-raised mark.)
    let table_paths: Vec<String> = pending
        .iter()
        .filter_map(|(_, p)| local_parquet_root(p))
        .filter_map(|p| representative_part(&p))
        .collect();
    ematix_flow_core::scale_class::observe_dataset(&table_paths);

    let mut built: Vec<(String, Arc<dyn TableProvider>)> = pending
        .into_iter()
        .map(|(name, provider)| {
            let native = nativize_provider(provider);
            (name, native)
        })
        .collect();

    // Σ.TW.SCHEMA, part 2 — SESSION-LEVEL narrowing reconciliation.
    // Pre-folding the mark makes every table decide from the same
    // classification, but they can still decide DIFFERENTLY: KEYS.2
    // narrows a `*key` only when that table's stats prove it fits i32,
    // and Σ.PK.1 forces a whole table back to raw Int64 when its parts
    // straddle. At SF=1000 `lineitem`/`orders` straddle on
    // `l_orderkey`/`o_orderkey` (max ≈ 6 B) so they keep Int64
    // *including* `l_suppkey`, while `supplier` (10 M) narrows to Int32
    // — hence the captured `CAST(supplier.s_suppkey AS Int64)` on
    // Q21's join. Disagreement is exactly Σ.PK.1's condition, one level
    // up, and takes its remedy: when any table declined to narrow while
    // another narrowed, rebuild EVERY table narrowing-OFF so the session
    // shares one raw-Int64 key schema. Below SF≈350 every key fits, no
    // table declines, and the narrowing (the twin's whole point — the
    // SF=100 lever) is kept untouched.
    if key_widths_disagree(&built) {
        tracing::debug!(
            "Σ.TW.SCHEMA: mixed KEYS.2 narrowing across tables — rebuilding all raw-Int64"
        );
        built = built
            .into_iter()
            .map(|(name, provider)| {
                let raw = renativize_no_downcast(&provider).unwrap_or(provider);
                (name, raw)
            })
            .collect();
    }

    for (table_name, provider) in built {
        // Same-name collisions across catalogs: last registration wins,
        // matching lookup order (the emission-twin precedent).
        let _ = twin.register_table(&table_name, provider);
    }
    Ok(twin)
}

/// Did KEYS.2 narrow some tables' keys but not others? Compares the
/// advertised width of every `*key` column across the session: an Int32
/// `*key` means that table narrowed, an Int64 one means it did not (it
/// straddled i32::MAX, or its stats couldn't prove the fit). Both
/// present ⇒ a join across them carries a CAST. Tables with no key
/// columns abstain.
fn key_widths_disagree(built: &[(String, Arc<dyn TableProvider>)]) -> bool {
    use datafusion::arrow::datatypes::DataType;
    let (mut any_narrow, mut any_wide) = (false, false);
    for (_, provider) in built {
        for f in provider.schema().fields() {
            if !f.name().to_ascii_lowercase().ends_with("key") {
                continue;
            }
            match f.data_type() {
                DataType::Int32 => any_narrow = true,
                DataType::Int64 => any_wide = true,
                _ => {}
            }
        }
    }
    any_narrow && any_wide
}

/// Rebuild a native provider with KEYS.2 narrowing pinned OFF, so it
/// advertises the raw parquet key widths. `None` when the provider isn't
/// one we constructed (nothing to rebuild — caller keeps the original).
fn renativize_no_downcast(provider: &Arc<dyn TableProvider>) -> Option<Arc<dyn TableProvider>> {
    use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
    use ematix_flow_core::ematix_fast_parquet_multi::EmatixFastParquetMultiTableProvider;

    let any = provider.as_any();
    if let Some(multi) = any.downcast_ref::<EmatixFastParquetMultiTableProvider>() {
        return EmatixFastParquetMultiTableProvider::try_new_files_no_downcast(multi.part_paths())
            .ok()
            .map(|p| Arc::new(p) as Arc<dyn TableProvider>);
    }
    if let Some(single) = any.downcast_ref::<EmatixFastParquetTableProvider>() {
        return EmatixFastParquetTableProvider::try_new_no_downcast(single.path())
            .ok()
            .map(|p| Arc::new(p) as Arc<dyn TableProvider>);
    }
    None
}

/// The local parquet root a stock [`ListingTable`] reads, or `None` for
/// anything [`nativize_provider`] would decline (remote store, already
/// native, non-listing). Shares that function's URL→path convention.
fn local_parquet_root(provider: &Arc<dyn TableProvider>) -> Option<std::path::PathBuf> {
    let listing = provider.as_any().downcast_ref::<ListingTable>()?;
    let [url] = listing.table_paths().as_slice() else {
        return None;
    };
    if url.scheme() != "file" {
        return None;
    }
    Some(std::path::PathBuf::from(format!("/{}", url.prefix())))
}

/// One parquet path standing in for a table: the root itself when it is a
/// file, else its first `*.parquet` part. `scale_class` scans the parent
/// directory's siblings, so any single part prices the whole table.
fn representative_part(root: &std::path::Path) -> Option<String> {
    if root.is_file() {
        return Some(root.to_string_lossy().into_owned());
    }
    let mut parts: Vec<std::path::PathBuf> = std::fs::read_dir(root)
        .ok()?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().map(|x| x == "parquet").unwrap_or(false))
        .collect();
    parts.sort();
    parts.first().map(|p| p.to_string_lossy().into_owned())
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

#[cfg(test)]
mod twin_schema_parity_tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;

    /// Write `rows` rows of a `*key` column (KEYS.2 downcast candidate)
    /// as a one-part parquet "table" dir under `root/name/`.
    async fn write_table(root: &std::path::Path, name: &str, keys: Vec<i64>) -> String {
        let dir = root.join(name);
        std::fs::create_dir_all(&dir).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            format!("{name}key"),
            DataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(keys))]).unwrap();
        let ctx = SessionContext::new();
        let df = ctx.read_batch(batch).unwrap();
        let path = dir.join(format!("{name}-0000.parquet"));
        df.write_parquet(path.to_str().unwrap(), Default::default(), None)
            .await
            .unwrap();
        dir.to_string_lossy().into_owned()
    }

    /// Σ.TW.SCHEMA: every table in the twin must advertise the same key
    /// width. This models SF=1000: a big fact table whose key straddles
    /// `i32::MAX` can never narrow (Σ.PK.1 pins it raw-Int64), while a
    /// small dimension's key fits and would narrow — mixed widths put a
    /// `CAST` on the join key, and a CAST key is not a plain equijoin,
    /// so the planner stops placing that join's filter early. That is
    /// what moved Q05's `region ⋈ nation` to the top of the tree and
    /// OOM-killed a 247 GB box on a query the single leg answers in
    /// 21.8 s.
    ///
    /// RED before the session-level reconciliation: `small` narrows to
    /// Int32 while `big` stays Int64.
    #[tokio::test]
    async fn twin_key_widths_agree_across_tables() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();

        // `big` trips the large-scale mark AND straddles i32::MAX, so its
        // key can never narrow — exactly SF=1000's l_orderkey.
        let straddle = i32::MAX as i64 + 1;
        let big_dir = write_table(root, "big", (straddle..straddle + 8).collect()).await;
        // `small` fits i32 comfortably — SF=1000's r_regionkey.
        let small_dir = write_table(root, "small", (0..5).collect()).await;

        // Threshold low enough that `big` (8 rows) classifies large.
        unsafe { std::env::set_var("EMAT_LARGE_SCALE_MIN_ROWS", "4") };

        let src = SessionContext::new();
        src.register_parquet("big", &big_dir, Default::default())
            .await
            .unwrap();
        src.register_parquet("small", &small_dir, Default::default())
            .await
            .unwrap();

        let twin = native_twin_ctx(&src).await.expect("twin builds");
        let big_t = twin
            .table_provider("big")
            .await
            .unwrap()
            .schema()
            .field_with_name("bigkey")
            .unwrap()
            .data_type()
            .clone();
        let small_t = twin
            .table_provider("small")
            .await
            .unwrap()
            .schema()
            .field_with_name("smallkey")
            .unwrap()
            .data_type()
            .clone();

        unsafe { std::env::remove_var("EMAT_LARGE_SCALE_MIN_ROWS") };

        // `big` can't narrow (values exceed i32::MAX). If `small` narrowed
        // anyway, the session carries mixed key widths — the CAST class.
        assert_eq!(
            big_t,
            DataType::Int64,
            "a key straddling i32::MAX must stay Int64"
        );
        assert_eq!(
            small_t, big_t,
            "twin key widths diverged (small={small_t:?} big={big_t:?}) — a join \
             across them would carry a CAST, which suppresses early filter \
             placement (the SF=1000 Q05 OOM)"
        );
    }
}
