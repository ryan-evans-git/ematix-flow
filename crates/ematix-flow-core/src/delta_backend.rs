//! Phase 35b: Delta Lake backend skeleton — local filesystem + Arrow IO.
//!
//! Wraps the `deltalake` crate (which is itself a thin shim over
//! `deltalake-core`). Each `target_table` resolves to its own Delta
//! table at `<root>/<schema>/<name>` — same path-shaped model as the
//! `ObjectStoreBackend` so users can run side-by-side raw / Delta
//! pipelines under one root.
//!
//! ## What 35b ships
//!   - `DeltaBackend::open_local(root_dir)` — root for many tables.
//!   - `read_arrow_stream(query)`: `query` is a `<schema>/<name>` path
//!     relative to root; loads the table via DataFusion and yields
//!     `RecordBatch` per scan plan partition.
//!   - `write_arrow_stream(target, stream, mode)`: appends or
//!     overwrites the Delta table; auto-creates on first write.
//!   - `execute()` rejects: Delta has no SQL surface from this
//!     backend (DataFusion-the-engine could front it, but that's a
//!     different layer).
//!
//! ## What lands later in 35
//!   - 35c: `run_append` / `run_truncate` + run_history sidecar
//!   - 35d: `run_merge` (deltalake's `MergeBuilder`)
//!   - 35e: `run_scd2` + soft-delete + TTL
//!   - 35f: S3 / MinIO + cross-backend tests
//!
//! ## Why a per-table model
//! Delta's transaction log lives at `<table>/_delta_log/`. Multiple
//! Delta tables at sibling prefixes are independent commit streams.
//! Putting them under a single root is just filesystem convention.

use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use deltalake::DeltaTable;
use deltalake::DeltaTableBuilder;
use deltalake::errors::DeltaTableError;
use deltalake::protocol::SaveMode;
use futures_util::{StreamExt, TryStreamExt};
use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use url::Url;

use crate::backend::{
    ArrowBatchStream, Backend, BackendError, DeleteHandling, Dialect, StrategyRunResult,
    TargetTable, WriteMode,
};
use crate::pg::ConnectionInfo;
use crate::types::TableSpec;

/// Delta-backed implementation of `Backend`.
///
/// Holds an absolute root URL (`file:///abs/path`); each
/// `target_table` resolves to a sub-URL `<root>/<schema>/<name>`.
/// Storage credentials for cloud backends (S3/Azure/GCS) land in
/// 35f via storage_options on construction.
pub struct DeltaBackend {
    /// Absolute `file://` URL to the root directory holding all
    /// target tables. Used to build per-target URLs.
    root_url: Url,
    /// Display-only label for `connection_info` and logs.
    base_label: String,
    /// Sidecar object store rooted at the same location, used for
    /// run_history JSONL writes. Phase 35f will swap in S3/Azure/GCS
    /// for cloud roots; 35b–e use `LocalFileSystem`.
    store: Arc<dyn ObjectStore>,
}

impl DeltaBackend {
    /// Open a local-filesystem root. Each target table becomes a
    /// sub-directory at `<root>/<schema>/<name>`. The root is
    /// created if missing.
    pub fn open_local(root_dir: impl Into<PathBuf>) -> Result<Self, BackendError> {
        let root: PathBuf = root_dir.into();
        std::fs::create_dir_all(&root).map_err(|e| {
            BackendError::Connection(format!("creating delta root {}: {e}", root.display()))
        })?;
        let abs = root.canonicalize().map_err(|e| {
            BackendError::Connection(format!("canonicalizing {}: {e}", root.display()))
        })?;
        // Construct a `file://` URL with a trailing slash so resolving
        // `<schema>/<name>` against it preserves the root.
        let url_str = format!("file://{}/", abs.display());
        let root_url = Url::parse(&url_str)
            .map_err(|e| BackendError::Connection(format!("delta root url: {e}")))?;
        let store = LocalFileSystem::new_with_prefix(&abs)
            .map_err(|e| BackendError::Connection(format!("delta sidecar fs: {e}")))?;
        Ok(Self {
            root_url,
            base_label: abs.display().to_string(),
            store: Arc::new(store),
        })
    }

    /// Build the per-target table URL by joining `<schema>/<name>`
    /// against the root. Uses `Url::join` so future cloud backends
    /// (Phase 35f) inherit the same path semantics.
    fn table_url(&self, schema: &str, name: &str) -> Result<Url, BackendError> {
        let rel = if schema.is_empty() {
            format!("{name}/")
        } else {
            format!("{schema}/{name}/")
        };
        self.root_url
            .join(&rel)
            .map_err(|e| BackendError::Connection(format!("delta table url: {e}")))
    }
}

/// Path prefix under the Delta root where run_history JSONL events
/// are written. One file per run, named by run_id, mirroring
/// `objectstore_backend::RUN_HISTORY_PREFIX`. Lives next to the
/// data tables — leading underscore so the Delta log scan never sees
/// it (and `read_arrow_stream("_ematix_flow/...")` would explicitly
/// look for it as a Delta table, which it isn't).
const RUN_HISTORY_PREFIX: &str = "_ematix_flow/run_history";

/// ISO-8601 "now" with millisecond precision and a `Z` suffix. Same
/// shape as `objectstore_backend::chrono_compat_iso8601_now`; kept
/// local to avoid a cross-module import for one routine.
fn iso8601_now() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let since_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let total_secs = since_epoch.as_secs() as i64;
    let millis = since_epoch.subsec_millis();
    let days = total_secs.div_euclid(86_400);
    let time_secs = total_secs.rem_euclid(86_400);
    let h = time_secs / 3600;
    let mi = (time_secs % 3600) / 60;
    let s = time_secs % 60;
    let (y, mo, d) = civil_from_days(days);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}.{millis:03}Z")
}

/// Howard Hinnant's chrono::civil_from_days. Same as the helper in
/// `mysql_backend.rs` and `objectstore_backend.rs`.
fn civil_from_days(z: i64) -> (i32, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y as i32, m as u32, d as u32)
}

/// Append a run-history event as one-line JSON at
/// `_ematix_flow/run_history/<run_id>.jsonl`. One file per run keeps
/// concurrent runs conflict-free without needing append-blob
/// semantics. Same protocol the ObjectStoreBackend uses.
async fn record_run_event(
    store: &Arc<dyn ObjectStore>,
    run_id: &uuid::Uuid,
    event: &serde_json::Value,
) -> Result<(), BackendError> {
    let path = ObjectPath::from(format!("{RUN_HISTORY_PREFIX}/{}.jsonl", run_id.simple()));
    let mut bytes = serde_json::to_vec(event)
        .map_err(|e| BackendError::Other(format!("delta run-history serialize: {e}")))?;
    bytes.push(b'\n');
    store
        .put(&path, Bytes::from(bytes).into())
        .await
        .map_err(|e| BackendError::Connection(format!("delta run-history put: {e}")))?;
    Ok(())
}

/// Build a `DeltaTable` for `url` and try to load its log. If the
/// location isn't yet a Delta table (`NotATable`) or doesn't exist
/// at all, return the uninitialized table so callers can run
/// `WriteBuilder` against it to create on first write. Any other
/// load failure (corrupt log, permission error) propagates.
///
/// For local-FS URLs we pre-create the directory because deltalake's
/// kernel layer reports a "Path does not exist" error rather than
/// `NotATable` for a missing local directory. Cloud backends (S3 et
/// al.) treat missing prefixes as empty without this dance.
async fn open_or_uninit_delta_table(url: Url) -> Result<DeltaTable, BackendError> {
    if url.scheme() == "file" {
        if let Ok(path) = url.to_file_path() {
            std::fs::create_dir_all(&path).map_err(|e| {
                BackendError::Connection(format!(
                    "creating delta table dir {}: {e}",
                    path.display()
                ))
            })?;
        }
    }
    let mut table = DeltaTableBuilder::from_url(url.clone())
        .map_err(|e| BackendError::Connection(format!("delta builder {url}: {e}")))?
        .build()
        .map_err(|e| BackendError::Connection(format!("delta builder {url}: {e}")))?;
    match table.load().await {
        Ok(_) => {}
        Err(DeltaTableError::NotATable(_)) => {}
        // The kernel-side error for "no _delta_log/ here yet" is a
        // generic message; match by substring rather than enum
        // variant since deltalake-core's error type doesn't expose a
        // dedicated variant for it.
        Err(e) if e.to_string().contains("Path does not exist") => {}
        Err(e) => {
            return Err(BackendError::Connection(format!("delta load {url}: {e}")));
        }
    }
    Ok(table)
}

#[async_trait]
impl Backend for DeltaBackend {
    fn dialect(&self) -> Dialect {
        Dialect::Delta
    }

    fn connection_info(&self) -> ConnectionInfo {
        ConnectionInfo {
            host: "delta".into(),
            port: 0,
            dbname: self.base_label.clone(),
            user: "local".into(),
        }
    }

    fn dsn(&self) -> Option<String> {
        Some(self.root_url.to_string())
    }

    async fn ping(&self) -> Result<(), BackendError> {
        // Liveness check for a local-FS root: the directory exists
        // and is readable. A failed canonicalize already happened at
        // open() time, so this is mostly a "did it disappear" probe.
        let path = self
            .root_url
            .to_file_path()
            .map_err(|_| BackendError::Connection("delta root url has no file path".into()))?;
        if !path.is_dir() {
            return Err(BackendError::Connection(format!(
                "delta root no longer exists: {}",
                path.display()
            )));
        }
        Ok(())
    }

    /// Delta backend has no SQL surface — DataFusion-the-engine can
    /// front it but that's a different layer. Reject explicitly so
    /// users don't accidentally `execute("SELECT …")` against this.
    async fn execute(&self, _statement: &str) -> Result<u64, BackendError> {
        Err(BackendError::Other(
            "Delta backend has no execute() surface — \
             use read_arrow_stream / write_arrow_stream, or run_append \
             / run_truncate / run_merge / run_scd2 (35c–e)"
                .into(),
        ))
    }

    /// Read a Delta table at the `query` prefix as Arrow `RecordBatch`es.
    /// `query` is interpreted as `<schema>/<name>` relative to the
    /// root. Builds a `DeltaTable`, loads its log state, then calls
    /// `scan_table()` which returns a DataFusion-backed
    /// `SendableRecordBatchStream`.
    async fn read_arrow_stream(&self, query: &str) -> Result<ArrowBatchStream, BackendError> {
        let rel = query.trim_matches('/');
        let parts: Vec<&str> = rel.splitn(2, '/').collect();
        let (schema, name) = match parts.as_slice() {
            [name] => ("", *name),
            [schema, name] => (*schema, *name),
            _ => unreachable!("splitn(2) yields 1 or 2 elements"),
        };
        let url = self.table_url(schema, name)?;
        let table = open_or_uninit_delta_table(url.clone()).await?;
        // Uninitialized tables (never written to, or only dry-run
        // writes have happened) carry `version() == None`. Treat them
        // as logically empty rather than erroring on `scan_table`
        // ("No files in log segment") — that matches how DB backends
        // respond to SELECT-from-empty and how ObjectStoreBackend
        // handles an empty prefix.
        if table.version().is_none() {
            let empty = futures_util::stream::empty();
            return Ok(Box::pin(empty));
        }
        let (_table, df_stream) = table
            .scan_table()
            .await
            .map_err(|e| BackendError::Query(format!("delta scan_table: {e}")))?;
        // Adapt DataFusion's `SendableRecordBatchStream` (errors are
        // `DataFusionError`) to our `ArrowBatchStream` (errors are
        // `BackendError`).
        let mapped =
            df_stream.map(|r| r.map_err(|e| BackendError::Query(format!("delta scan: {e}"))));
        Ok(Box::pin(mapped))
    }

    /// Write a stream of Arrow `RecordBatch`es to the table at
    /// `<schema>/<name>`. `Append` adds to the existing table (or
    /// creates it on first write — `WriteBuilder` auto-creates the
    /// table from the input schema if the location is uninitialized);
    /// `Truncate` maps to `SaveMode::Overwrite`.
    async fn write_arrow_stream(
        &self,
        target: &TargetTable,
        stream: ArrowBatchStream,
        mode: WriteMode,
    ) -> Result<u64, BackendError> {
        // Buffer batches into a Vec — `WriteBuilder::write` accepts
        // `IntoIterator<Item = RecordBatch>`. Streaming-write through
        // a multipart upload is a future optimization (matters for
        // multi-GB inputs to S3 in 35f).
        let batches: Vec<RecordBatch> = stream.try_collect().await?;
        if batches.is_empty() {
            // No-op: don't materialize an empty Delta commit.
            return Ok(0);
        }
        let total: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        let url = self.table_url(&target.schema, &target.name)?;
        let table = open_or_uninit_delta_table(url.clone()).await?;
        let save_mode = match mode {
            WriteMode::Append => SaveMode::Append,
            WriteMode::Truncate => SaveMode::Overwrite,
        };
        table
            .write(batches)
            .with_save_mode(save_mode)
            .await
            .map_err(|e| BackendError::Query(format!("delta write: {e}")))?;
        Ok(total)
    }

    async fn run_append(
        &self,
        spec: &TableSpec,
        source_query: &str,
        pipeline_name: &str,
        source_backend: Option<&dyn Backend>,
        incremental_column: Option<&str>,
        last_value_literal: Option<&str>,
        dry_run: bool,
    ) -> Result<StrategyRunResult, BackendError> {
        let source = source_backend.ok_or_else(|| {
            BackendError::Other(
                "Delta run_append: source_backend is required \
                 (Delta is a target only — there is no same-DB path \
                 because Delta has no SQL source surface from this \
                 backend; cross-Delta copies will land in 35f)"
                    .into(),
            )
        })?;
        // Watermark filter wraps the source SQL in the source's
        // dialect. Delta itself doesn't track watermarks (the natural
        // surface is the data layer); users running incremental loads
        // to Delta must persist `last_value_literal` externally.
        let watermark = incremental_column.map(|c| crate::meta::WatermarkConfig {
            column: c.to_string(),
            last_value_literal: last_value_literal.map(|s| s.to_string()),
        });
        let filtered_source =
            crate::meta::wrap_with_watermark_filter(source_query, watermark.as_ref());

        let run_id = uuid::Uuid::now_v7();
        let started_at = iso8601_now();
        let target = TargetTable {
            schema: spec.schema.clone(),
            name: spec.name.clone(),
        };

        let inserted: u64 = if dry_run {
            // Probe the source so a missing query / bad credentials
            // surfaces; do not write to Delta.
            let _ = source.read_arrow_stream(&filtered_source).await?;
            0
        } else {
            let stream = source.read_arrow_stream(&filtered_source).await?;
            self.write_arrow_stream(&target, stream, WriteMode::Append)
                .await?
        };
        let finished_at = iso8601_now();
        let event = serde_json::json!({
            "run_id": run_id.to_string(),
            "pipeline_name": pipeline_name,
            "target_schema": spec.schema,
            "target_table": spec.name,
            "mode": "append",
            "path": "cross_backend",
            "started_at": started_at,
            "finished_at": finished_at,
            "status": if dry_run { "dry_run" } else { "success" },
            "rows_inserted": inserted,
            "format": "delta",
        });
        record_run_event(&self.store, &run_id, &event).await?;

        Ok(StrategyRunResult {
            run_id: run_id.to_string(),
            rows_inserted: inserted as i64,
            rows_updated: None,
            rows_unchanged: None,
            rows_closed: None,
            status: if dry_run { "dry_run" } else { "success" }.into(),
            path: "cross_backend".into(),
        })
    }

    async fn run_truncate(
        &self,
        spec: &TableSpec,
        source_query: &str,
        pipeline_name: &str,
        source_backend: Option<&dyn Backend>,
        dry_run: bool,
    ) -> Result<StrategyRunResult, BackendError> {
        let source = source_backend.ok_or_else(|| {
            BackendError::Other(
                "Delta run_truncate: source_backend is required \
                 (Delta is a target only)"
                    .into(),
            )
        })?;
        let run_id = uuid::Uuid::now_v7();
        let started_at = iso8601_now();
        let target = TargetTable {
            schema: spec.schema.clone(),
            name: spec.name.clone(),
        };

        let inserted: u64 = if dry_run {
            // Touch source but do not overwrite — Delta's Overwrite
            // mode is a real commit and not safely reversible.
            let _ = source.read_arrow_stream(source_query).await?;
            0
        } else {
            let stream = source.read_arrow_stream(source_query).await?;
            self.write_arrow_stream(&target, stream, WriteMode::Truncate)
                .await?
        };
        let finished_at = iso8601_now();
        let event = serde_json::json!({
            "run_id": run_id.to_string(),
            "pipeline_name": pipeline_name,
            "target_schema": spec.schema,
            "target_table": spec.name,
            "mode": "truncate",
            "path": "cross_backend",
            "started_at": started_at,
            "finished_at": finished_at,
            "status": if dry_run { "dry_run" } else { "success" },
            "rows_inserted": inserted,
            "format": "delta",
        });
        record_run_event(&self.store, &run_id, &event).await?;

        Ok(StrategyRunResult {
            run_id: run_id.to_string(),
            rows_inserted: inserted as i64,
            rows_updated: None,
            rows_unchanged: None,
            rows_closed: None,
            status: if dry_run { "dry_run" } else { "success" }.into(),
            path: "cross_backend".into(),
        })
    }

    async fn run_merge(
        &self,
        _spec: &TableSpec,
        _source_query: &str,
        _keys: &[String],
        _update_columns: &[String],
        _pipeline_name: &str,
        _mode_label: &str,
        _source_backend: Option<&dyn Backend>,
        _delete_handling: Option<DeleteHandling>,
        _dry_run: bool,
    ) -> Result<StrategyRunResult, BackendError> {
        Err(BackendError::Other(
            "Delta run_merge lands in Phase 35d".into(),
        ))
    }

    async fn run_scd2(
        &self,
        _spec: &TableSpec,
        _source_query: &str,
        _keys: &[String],
        _compare_columns: &[String],
        _pipeline_name: &str,
        _source_backend: Option<&dyn Backend>,
        _delete_handling: Option<DeleteHandling>,
        _event_timestamp_column: Option<&str>,
        _ttl_seconds: Option<i64>,
        _dry_run: bool,
    ) -> Result<StrategyRunResult, BackendError> {
        Err(BackendError::Other(
            "Delta run_scd2 lands in Phase 35e".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use std::sync::Arc;

    fn small_batch() -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["alice", "bob", "carol"])),
            ],
        )
        .unwrap()
    }

    fn arrow_stream_for(batch: RecordBatch) -> ArrowBatchStream {
        Box::pin(futures_util::stream::once(async move {
            Ok::<_, BackendError>(batch)
        }))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn delta_local_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let backend = DeltaBackend::open_local(dir.path()).unwrap();
        assert!(matches!(backend.dialect(), Dialect::Delta));
        backend.ping().await.unwrap();

        let target = TargetTable {
            schema: "raw".into(),
            name: "events".into(),
        };
        let n = backend
            .write_arrow_stream(&target, arrow_stream_for(small_batch()), WriteMode::Append)
            .await
            .unwrap();
        assert_eq!(n, 3);

        let stream = backend.read_arrow_stream("raw/events").await.unwrap();
        let batches: Vec<RecordBatch> = futures_util::TryStreamExt::try_collect(stream)
            .await
            .unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn delta_append_then_truncate_replaces() {
        let dir = tempfile::tempdir().unwrap();
        let backend = DeltaBackend::open_local(dir.path()).unwrap();
        let target = TargetTable {
            schema: "raw".into(),
            name: "events".into(),
        };
        // Two appends → 6 rows in two commits.
        backend
            .write_arrow_stream(&target, arrow_stream_for(small_batch()), WriteMode::Append)
            .await
            .unwrap();
        backend
            .write_arrow_stream(&target, arrow_stream_for(small_batch()), WriteMode::Append)
            .await
            .unwrap();
        let stream = backend.read_arrow_stream("raw/events").await.unwrap();
        let batches: Vec<RecordBatch> = futures_util::TryStreamExt::try_collect(stream)
            .await
            .unwrap();
        let before: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(before, 6);

        // Truncate replaces with 3 rows.
        backend
            .write_arrow_stream(
                &target,
                arrow_stream_for(small_batch()),
                WriteMode::Truncate,
            )
            .await
            .unwrap();
        let stream = backend.read_arrow_stream("raw/events").await.unwrap();
        let batches: Vec<RecordBatch> = futures_util::TryStreamExt::try_collect(stream)
            .await
            .unwrap();
        let after: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(after, 3, "Overwrite save mode replaces all rows");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn delta_execute_is_rejected_with_pointer() {
        let dir = tempfile::tempdir().unwrap();
        let backend = DeltaBackend::open_local(dir.path()).unwrap();
        let err = backend.execute("anything").await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("no execute() surface"), "got: {msg}");
    }

    // --- Phase 35c: run_append + run_truncate (DuckDB → Delta) ----------

    use crate::DuckDBBackend;

    async fn duckdb_with_events() -> Arc<dyn Backend> {
        let duck: Arc<dyn Backend> = Arc::new(DuckDBBackend::open(":memory:").unwrap());
        duck.execute("CREATE SCHEMA s").await.unwrap();
        duck.execute("CREATE TABLE s.events (id BIGINT, name VARCHAR)")
            .await
            .unwrap();
        duck.execute("INSERT INTO s.events VALUES (1, 'a'), (2, 'b'), (3, 'c')")
            .await
            .unwrap();
        duck
    }

    fn small_table_spec() -> TableSpec {
        use crate::types::{ColumnSpec, ColumnType};
        TableSpec {
            schema: "raw".into(),
            name: "events".into(),
            columns: vec![
                ColumnSpec {
                    name: "id".into(),
                    ty: ColumnType::BigInt,
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
            unique_constraints: vec![],
            fingerprint: String::new(),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn delta_run_append_from_duckdb_writes_table() {
        let dir = tempfile::tempdir().unwrap();
        let target_backend = DeltaBackend::open_local(dir.path()).unwrap();
        let source = duckdb_with_events().await;
        let spec = small_table_spec();

        let result = target_backend
            .run_append(
                &spec,
                "SELECT id, name FROM s.events ORDER BY id",
                "p35c_append",
                Some(source.as_ref()),
                None,
                None,
                false,
            )
            .await
            .unwrap();
        assert_eq!(result.rows_inserted, 3);
        assert_eq!(result.status, "success");
        assert_eq!(result.path, "cross_backend");

        // Read back via the same backend.
        let stream = target_backend
            .read_arrow_stream("raw/events")
            .await
            .unwrap();
        let batches: Vec<RecordBatch> = futures_util::TryStreamExt::try_collect(stream)
            .await
            .unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3);

        // run_history sidecar: exactly one JSONL file.
        let mut listing = target_backend
            .store
            .list(Some(&ObjectPath::from("_ematix_flow/run_history")));
        let mut history_count = 0;
        while futures_util::StreamExt::next(&mut listing).await.is_some() {
            history_count += 1;
        }
        assert_eq!(history_count, 1, "exactly one run_history event");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn delta_run_truncate_from_duckdb_replaces_target() {
        let dir = tempfile::tempdir().unwrap();
        let target_backend = DeltaBackend::open_local(dir.path()).unwrap();
        let source = duckdb_with_events().await;
        let spec = small_table_spec();

        // Two appends seed the table with 6 rows in two commits.
        for tag in ["t35c_a", "t35c_b"] {
            target_backend
                .run_append(
                    &spec,
                    "SELECT id, name FROM s.events ORDER BY id",
                    tag,
                    Some(source.as_ref()),
                    None,
                    None,
                    false,
                )
                .await
                .unwrap();
        }
        // Truncate replaces with 3 rows in a fresh Overwrite commit.
        let result = target_backend
            .run_truncate(
                &spec,
                "SELECT id, name FROM s.events ORDER BY id",
                "t35c_trunc",
                Some(source.as_ref()),
                false,
            )
            .await
            .unwrap();
        assert_eq!(result.rows_inserted, 3);
        assert_eq!(result.status, "success");

        let stream = target_backend
            .read_arrow_stream("raw/events")
            .await
            .unwrap();
        let batches: Vec<RecordBatch> = futures_util::TryStreamExt::try_collect(stream)
            .await
            .unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3, "Overwrite save_mode replaced the prior commits");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn delta_run_append_dry_run_does_not_write() {
        let dir = tempfile::tempdir().unwrap();
        let target_backend = DeltaBackend::open_local(dir.path()).unwrap();
        let source = duckdb_with_events().await;
        let spec = small_table_spec();

        let result = target_backend
            .run_append(
                &spec,
                "SELECT id, name FROM s.events ORDER BY id",
                "p35c_dry",
                Some(source.as_ref()),
                None,
                None,
                true,
            )
            .await
            .unwrap();
        assert_eq!(result.status, "dry_run");
        assert_eq!(result.rows_inserted, 0);

        // Delta table at raw/events doesn't exist yet (no write
        // happened); read should return zero rows.
        let stream = target_backend
            .read_arrow_stream("raw/events")
            .await
            .unwrap();
        let batches: Vec<RecordBatch> = futures_util::TryStreamExt::try_collect(stream)
            .await
            .unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 0);

        // Run-history event still recorded (audit trail).
        let mut listing = target_backend
            .store
            .list(Some(&ObjectPath::from("_ematix_flow/run_history")));
        let mut history_count = 0;
        while futures_util::StreamExt::next(&mut listing).await.is_some() {
            history_count += 1;
        }
        assert_eq!(history_count, 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn delta_run_append_rejects_missing_source() {
        let dir = tempfile::tempdir().unwrap();
        let backend = DeltaBackend::open_local(dir.path()).unwrap();
        let spec = small_table_spec();
        let err = backend
            .run_append(&spec, "ignored", "p", None, None, None, false)
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("source_backend is required"), "got: {msg}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn delta_merge_stub_points_at_35d() {
        let dir = tempfile::tempdir().unwrap();
        let backend = DeltaBackend::open_local(dir.path()).unwrap();
        let spec = TableSpec {
            schema: "s".into(),
            name: "t".into(),
            columns: vec![],
            unique_constraints: vec![],
            fingerprint: String::new(),
        };
        let err = backend
            .run_merge(
                &spec,
                "x",
                &["k".into()],
                &["c".into()],
                "p",
                "merge",
                None,
                None,
                false,
            )
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("Phase 35d"), "got: {msg}");
    }
}
