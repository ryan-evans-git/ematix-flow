//! DLQ Phase 4: operator-facing DLQ + rewind operations over a
//! TOML-configured pipeline.
//!
//! The Python HTTP layer (FastAPI, `python/ematix_flow/web`) calls
//! these through the pyo3 bindings: it renders a registered
//! stream's TOML, builds an operations-only [`StreamingPipeline`]
//! (no run loop, no metrics server, no state recovery), and drives
//! depth / browse / replay / park / purge / rewind against the SAME
//! store the running pipeline resolves — `resolve_dlq_store` is the
//! single source of resolution truth (Phase 2 made it `pub` for
//! exactly this).
//!
//! ## Timestamps are passed IN
//!
//! `dlq_stats` takes an explicit `now_ms` for its arrival buckets —
//! the HTTP handler is the emission boundary that reads the clock,
//! never this layer (house convention, same as the stores).
//!
//! ## Scan bounds
//!
//! Stage breakdown + arrival rates page through `browse`; a DLQ
//! deeper than [`STATS_SCAN_CAP`] reports `truncated = true` and
//! stats over the oldest `STATS_SCAN_CAP` records rather than
//! stalling the API on an unbounded scan.

use std::collections::BTreeMap;

use ematix_flow_core::dlq::{
    DlqRecord, DlqRecordId, DlqRecordStatus, DlqSelection, ReplayOptions, ReplayReport,
};
use ematix_flow_core::streaming::{
    RewindReport, RewindTarget, StreamingPipeline, StreamingPipelineMetricsCounters,
};

use crate::{CliError, PipelineCliConfig};

/// Stats scans stop after this many records (see module docs).
pub const STATS_SCAN_CAP: u64 = 10_000;

/// Page size used for internal browse loops.
const SCAN_PAGE_SIZE: u64 = 500;

/// Depth + stage breakdown + arrival buckets for one pipeline's
/// DLQ. Arrival buckets count records whose `failed_at` falls
/// within the trailing window ending at the caller's `now_ms`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DlqStats {
    pub pending: u64,
    pub parked: u64,
    /// stage name → record count over the scanned window.
    pub by_stage: BTreeMap<String, u64>,
    pub arrivals_1m: u64,
    pub arrivals_5m: u64,
    pub arrivals_15m: u64,
    pub arrivals_60m: u64,
    /// Records the breakdown/arrival scan actually covered.
    pub scanned: u64,
    /// True when the scan hit [`STATS_SCAN_CAP`] before draining.
    pub truncated: bool,
}

/// Build an operations-only pipeline from `config`: sources +
/// targets + transform + state store are wired exactly as
/// `run_consume_with` does it (so `resolve_dlq_store` and
/// `run_dlq_replay` behave identically), but nothing is spawned and
/// no state is recovered.
pub async fn build_ops_pipeline(config: &PipelineCliConfig) -> Result<StreamingPipeline, CliError> {
    let sources = config.build_sources()?;
    let targets = config.build_targets().await?;
    if sources.is_empty() {
        return Err(CliError::Runtime("no source configured".into()));
    }
    let lookups = config.load_lookups().await?;
    let primary_table = targets
        .first()
        .map(|(_, t)| t.clone())
        .ok_or_else(|| CliError::Runtime("no target configured".into()))?;
    let pipeline_metrics = StreamingPipelineMetricsCounters::new(&config.pipeline_name);
    let mut pipeline_cfg = config.streaming_config_with_lookups_udfs_aggregate_udfs_and_metrics(
        primary_table,
        lookups,
        Vec::new(),
        Vec::new(),
        Some(&pipeline_metrics.registry),
    );
    if let Some(ss) = &config.state_store {
        pipeline_cfg = pipeline_cfg.with_state_store(ss.build().await?);
    }
    Ok(StreamingPipeline::new_multi_source_with_metrics(
        sources,
        targets,
        pipeline_cfg,
        pipeline_metrics,
    ))
}

/// Depth + stage breakdown + arrival buckets. `now_ms` is the
/// caller's clock (milliseconds since the Unix epoch).
pub async fn dlq_stats(pipeline: &StreamingPipeline, now_ms: i64) -> Result<DlqStats, CliError> {
    let store = pipeline.resolve_dlq_store().await?;
    let name = pipeline.config.pipeline_name.as_str();
    let depth = store.depth(name).await.map_err(core_err)?;

    let mut stats = DlqStats {
        pending: depth.pending,
        parked: depth.parked,
        ..Default::default()
    };
    let mut page = 0u64;
    'scan: loop {
        let records = store
            .browse(name, page, SCAN_PAGE_SIZE, None)
            .await
            .map_err(core_err)?;
        if records.is_empty() {
            break;
        }
        for r in &records {
            if stats.scanned >= STATS_SCAN_CAP {
                stats.truncated = true;
                break 'scan;
            }
            stats.scanned += 1;
            *stats
                .by_stage
                .entry(r.meta.stage.as_str().to_string())
                .or_insert(0) += 1;
            let age_ms = now_ms - r.meta.failed_at;
            if age_ms <= 60_000 {
                stats.arrivals_1m += 1;
            }
            if age_ms <= 300_000 {
                stats.arrivals_5m += 1;
            }
            if age_ms <= 900_000 {
                stats.arrivals_15m += 1;
            }
            if age_ms <= 3_600_000 {
                stats.arrivals_60m += 1;
            }
        }
        page += 1;
    }
    Ok(stats)
}

/// One page of records, oldest-first. `status` filters to
/// `"pending"` / `"leased"` / `"parked"`; `None` returns every
/// status the store can enumerate.
pub async fn dlq_records(
    pipeline: &StreamingPipeline,
    status: Option<&str>,
    page: u64,
    page_size: u64,
) -> Result<Vec<DlqRecord>, CliError> {
    let status_filter: Option<DlqRecordStatus> = match status {
        None => None,
        Some(s) => Some(DlqRecordStatus::parse(s).ok_or_else(|| {
            CliError::Runtime(format!(
                "unknown DLQ record status {s:?} \
                 (use \"pending\", \"leased\", or \"parked\")"
            ))
        })?),
    };
    let store = pipeline.resolve_dlq_store().await?;
    store
        .browse(
            &pipeline.config.pipeline_name,
            page,
            page_size,
            status_filter,
        )
        .await
        .map_err(core_err)
}

/// Find one record by id (bounded scan through `browse` — the
/// payload-download endpoint's lookup).
pub async fn dlq_record_by_id(
    pipeline: &StreamingPipeline,
    id: &str,
) -> Result<Option<DlqRecord>, CliError> {
    let store = pipeline.resolve_dlq_store().await?;
    let name = pipeline.config.pipeline_name.as_str();
    let mut page = 0u64;
    let mut scanned = 0u64;
    loop {
        let records = store
            .browse(name, page, SCAN_PAGE_SIZE, None)
            .await
            .map_err(core_err)?;
        if records.is_empty() {
            return Ok(None);
        }
        scanned += records.len() as u64;
        if let Some(hit) = records.into_iter().find(|r| r.id.0 == id) {
            return Ok(Some(hit));
        }
        if scanned >= STATS_SCAN_CAP {
            return Ok(None);
        }
        page += 1;
    }
}

/// Run one replay pass over `selection`. `max_attempts = None`
/// takes the pipeline's configured `dlq_max_attempts` default.
pub async fn dlq_replay(
    pipeline: &StreamingPipeline,
    selection: DlqSelection,
    max_attempts: Option<u32>,
) -> Result<ReplayReport, CliError> {
    let options = ReplayOptions {
        max_attempts: max_attempts.unwrap_or(pipeline.config.dlq_max_attempts),
        ..Default::default()
    };
    pipeline
        .run_dlq_replay(selection, options)
        .await
        .map_err(CliError::from)
}

/// Park the selected records; returns how many were parked.
pub async fn dlq_park(
    pipeline: &StreamingPipeline,
    selection: DlqSelection,
) -> Result<u64, CliError> {
    let store = pipeline.resolve_dlq_store().await?;
    let name = pipeline.config.pipeline_name.as_str();
    let ids: Vec<DlqRecordId> = match selection {
        DlqSelection::Ids(ids) => ids,
        // All / FirstN resolve against the PENDING set — parking is
        // an operator action on records awaiting replay; leased
        // records are in a replay's custody and parked records are
        // already parked.
        DlqSelection::All => collect_pending_ids(pipeline, u64::MAX).await?,
        DlqSelection::FirstN(n) => collect_pending_ids(pipeline, n).await?,
    };
    if ids.is_empty() {
        return Ok(0);
    }
    store.park(name, &ids).await.map_err(core_err)?;
    Ok(ids.len() as u64)
}

/// Oldest-first pending record ids, up to `limit` (bounded by
/// [`STATS_SCAN_CAP`]).
async fn collect_pending_ids(
    pipeline: &StreamingPipeline,
    limit: u64,
) -> Result<Vec<DlqRecordId>, CliError> {
    let store = pipeline.resolve_dlq_store().await?;
    let name = pipeline.config.pipeline_name.as_str();
    let limit = limit.min(STATS_SCAN_CAP);
    let mut ids: Vec<DlqRecordId> = Vec::new();
    let mut page = 0u64;
    while (ids.len() as u64) < limit {
        let records = store
            .browse(name, page, SCAN_PAGE_SIZE, Some(DlqRecordStatus::Pending))
            .await
            .map_err(core_err)?;
        if records.is_empty() {
            break;
        }
        for r in records {
            if (ids.len() as u64) >= limit {
                break;
            }
            ids.push(r.id);
        }
        page += 1;
    }
    Ok(ids)
}

/// Purge the selected records; returns the deleted count.
pub async fn dlq_purge(
    pipeline: &StreamingPipeline,
    selection: DlqSelection,
) -> Result<u64, CliError> {
    let store = pipeline.resolve_dlq_store().await?;
    store
        .purge(&pipeline.config.pipeline_name, selection)
        .await
        .map_err(core_err)
}

/// Rewind the pipeline (see `StreamingPipeline::rewind` for the
/// orchestration + the `confirm_state_reset` gate).
pub async fn rewind(
    pipeline: &StreamingPipeline,
    to: RewindTarget,
    confirm_state_reset: bool,
) -> Result<RewindReport, CliError> {
    pipeline
        .rewind(to, confirm_state_reset)
        .await
        .map_err(CliError::from)
}

/// Parse the JSON selection shape the HTTP API accepts:
/// `{"kind":"all"}` | `{"kind":"first_n","n":10}` |
/// `{"kind":"ids","ids":["…"]}`.
pub fn parse_selection(json: &str) -> Result<DlqSelection, CliError> {
    #[derive(serde::Deserialize)]
    #[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
    enum SelectionJson {
        All,
        FirstN { n: u64 },
        Ids { ids: Vec<String> },
    }
    let parsed: SelectionJson = serde_json::from_str(json).map_err(|e| {
        CliError::Runtime(format!(
            "invalid DLQ selection {json:?}: {e} (expected {{\"kind\":\"all\"}}, \
             {{\"kind\":\"first_n\",\"n\":N}}, or {{\"kind\":\"ids\",\"ids\":[…]}})"
        ))
    })?;
    Ok(match parsed {
        SelectionJson::All => DlqSelection::All,
        SelectionJson::FirstN { n } => DlqSelection::FirstN(n),
        SelectionJson::Ids { ids } => DlqSelection::Ids(ids.into_iter().map(DlqRecordId).collect()),
    })
}

/// Parse the JSON rewind-target shape:
/// `{"kind":"timestamp","ms":1700000000000}` |
/// `{"kind":"offset","bytes":[…]}` (backend-opaque offset bytes as
/// a JSON byte array).
pub fn parse_rewind_target(json: &str) -> Result<RewindTarget, CliError> {
    #[derive(serde::Deserialize)]
    #[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
    enum TargetJson {
        Timestamp { ms: i64 },
        Offset { bytes: Vec<u8> },
    }
    let parsed: TargetJson = serde_json::from_str(json).map_err(|e| {
        CliError::Runtime(format!(
            "invalid rewind target {json:?}: {e} (expected \
             {{\"kind\":\"timestamp\",\"ms\":N}} or \
             {{\"kind\":\"offset\",\"bytes\":[…]}})"
        ))
    })?;
    Ok(match parsed {
        TargetJson::Timestamp { ms } => RewindTarget::Timestamp(ms),
        TargetJson::Offset { bytes } => RewindTarget::Offset(bytes),
    })
}

/// Map a store-layer `DlqError` through `BackendError` into
/// `CliError` (the typed `Unsupported` string survives).
fn core_err(e: ematix_flow_core::dlq::DlqError) -> CliError {
    CliError::Backend(e.into())
}
