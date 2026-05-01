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

use arrow_array::RecordBatch;
use async_trait::async_trait;
use deltalake::DeltaTable;
use deltalake::DeltaTableBuilder;
use deltalake::errors::DeltaTableError;
use deltalake::protocol::SaveMode;
use futures_util::{StreamExt, TryStreamExt};
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
        Ok(Self {
            root_url,
            base_label: abs.display().to_string(),
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
        _spec: &TableSpec,
        _source_query: &str,
        _pipeline_name: &str,
        _source_backend: Option<&dyn Backend>,
        _incremental_column: Option<&str>,
        _last_value_literal: Option<&str>,
        _dry_run: bool,
    ) -> Result<StrategyRunResult, BackendError> {
        Err(BackendError::Other(
            "Delta run_append lands in Phase 35c".into(),
        ))
    }

    async fn run_truncate(
        &self,
        _spec: &TableSpec,
        _source_query: &str,
        _pipeline_name: &str,
        _source_backend: Option<&dyn Backend>,
        _dry_run: bool,
    ) -> Result<StrategyRunResult, BackendError> {
        Err(BackendError::Other(
            "Delta run_truncate lands in Phase 35c".into(),
        ))
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
