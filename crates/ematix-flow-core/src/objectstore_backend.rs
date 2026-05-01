//! Phase 34a: Object-store backend skeleton — local filesystem + Parquet.
//!
//! Wraps the `object_store` crate so the same `Backend` surface drives
//! local files now and S3 / Azure / GCS in 34e. 34a only handles
//! Parquet; CSV / JSONL / ORC land in 34b–d.
//!
//! ## Path model
//! - `target.schema` and `target.name` are joined into a path prefix:
//!   `<schema>/<name>` (relative to the store's root).
//! - `write_arrow_stream` generates a UUIDv7 filename and writes the
//!   stream as `<schema>/<name>/<uuid7>.parquet`. UUIDv7 is sortable so
//!   files in a prefix lexicographically reflect ingest order.
//! - `read_arrow_stream` treats the `query` argument as a path prefix
//!   relative to the store root and reads every Parquet file under it
//!   (recursively). All files must share the same Arrow schema.
//!
//! ## Strategy executors
//! Per `docs/MULTI_BACKEND_PLAN.md` §Phase 34, raw object storage
//! supports `append` (write a new file) and `truncate` (delete prefix +
//! write). `merge` / `scd2` raise an error pointing at Phase 35 —
//! Iceberg / Delta is the right tool for transactional updates against
//! object storage. 34a stubs all four with phase-marker errors; 34f
//! wires append + truncate.

use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::StreamExt;
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use parquet::arrow::AsyncArrowWriter;
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

use crate::backend::{
    ArrowBatchStream, Backend, BackendError, DeleteHandling, Dialect, ObjectFormat,
    StrategyRunResult, TargetTable, WriteMode,
};
use crate::pg::ConnectionInfo;
use crate::types::TableSpec;

/// Object-store backend.
///
/// Construct with [`ObjectStoreBackend::open_local`] for a local-FS
/// store; richer constructors for S3/Azure/GCS come in Phase 34e.
pub struct ObjectStoreBackend {
    store: Arc<dyn ObjectStore>,
    format: ObjectFormat,
    /// Display-only DSN. The store itself never re-parses this — it's
    /// the value the user passed in, used for `dsn()` and logs.
    dsn: String,
    /// Display-only base path for human-friendly identification (logs,
    /// `connection_info`). Not used for routing.
    base_label: String,
}

impl ObjectStoreBackend {
    /// Open a local-filesystem-backed store at `root_dir`. The root is
    /// created if it doesn't yet exist (matching `mkdir -p` semantics).
    /// All target tables are addressed relative to this root.
    pub fn open_local(
        root_dir: impl Into<PathBuf>,
        format: ObjectFormat,
    ) -> Result<Self, BackendError> {
        let root: PathBuf = root_dir.into();
        std::fs::create_dir_all(&root).map_err(|e| {
            BackendError::Connection(format!("creating root dir {}: {e}", root.display()))
        })?;
        let store = LocalFileSystem::new_with_prefix(&root)
            .map_err(|e| BackendError::Connection(format!("local fs: {e}")))?;
        Ok(Self {
            store: Arc::new(store),
            format,
            dsn: format!("file://{}", root.display()),
            base_label: root.display().to_string(),
        })
    }

    /// Borrow the underlying object store. Used by tests and (later)
    /// the strategy executors.
    pub fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    pub fn format(&self) -> ObjectFormat {
        self.format
    }
}

/// Build the path prefix for a target table inside the store. The
/// store is rooted at the user's base directory, so this produces a
/// path *relative* to that root.
fn target_prefix(target: &TargetTable) -> ObjectPath {
    if target.schema.is_empty() {
        ObjectPath::from(target.name.as_str())
    } else {
        ObjectPath::from(format!("{}/{}", target.schema, target.name))
    }
}

/// Generate the per-batch filename `<uuid7>.<ext>` under a prefix.
/// UUIDv7 carries a millisecond timestamp prefix, so files sort
/// lexicographically by ingest time — useful for `read_arrow_stream`'s
/// directory listing.
fn new_object_path(prefix: &ObjectPath, ext: &str) -> ObjectPath {
    let id = Uuid::now_v7();
    let mut path = prefix.to_string();
    if !path.is_empty() && !path.ends_with('/') {
        path.push('/');
    }
    path.push_str(&id.simple().to_string());
    path.push('.');
    path.push_str(ext);
    ObjectPath::from(path)
}

fn ext_for_format(format: ObjectFormat) -> &'static str {
    match format {
        ObjectFormat::Parquet => "parquet",
        ObjectFormat::Csv => "csv",
        ObjectFormat::Orc => "orc",
        ObjectFormat::JsonLines => "jsonl",
    }
}

/// Read all files under `prefix` matching the backend's format and
/// concatenate the Arrow batches they produce.
async fn read_parquet_under_prefix(
    store: &Arc<dyn ObjectStore>,
    prefix: ObjectPath,
) -> Result<Vec<RecordBatch>, BackendError> {
    let mut listing = store.list(Some(&prefix));
    let mut batches: Vec<RecordBatch> = Vec::new();
    while let Some(meta) = listing.next().await {
        let meta = meta.map_err(|e| BackendError::Connection(format!("list: {e}")))?;
        if !meta.location.as_ref().ends_with(".parquet") {
            // Skip non-Parquet siblings (manifest files, _SUCCESS markers,
            // anything left over from other write attempts).
            continue;
        }
        let reader = ParquetObjectReader::new(store.clone(), meta.location.clone())
            .with_file_size(meta.size);
        let stream_builder = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .map_err(|e| BackendError::Query(format!("parquet open: {e}")))?;
        let mut stream = stream_builder
            .build()
            .map_err(|e| BackendError::Query(format!("parquet stream: {e}")))?;
        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(|e| BackendError::Query(format!("parquet read: {e}")))?;
            batches.push(batch);
        }
    }
    Ok(batches)
}

/// Write a stream of Arrow batches as one Parquet file at `path`.
async fn write_parquet_at_path(
    store: &Arc<dyn ObjectStore>,
    path: ObjectPath,
    mut stream: ArrowBatchStream,
) -> Result<u64, BackendError> {
    // Buffer batches in memory first — keeps Phase 34a simple. Streaming
    // multipart upload via `object_store::buffered::BufWriter` is a
    // future optimization (matters for >GB inputs to S3).
    let mut batches: Vec<RecordBatch> = Vec::new();
    while let Some(b) = stream.next().await {
        batches.push(b?);
    }
    if batches.is_empty() {
        // No-op: don't create a zero-row file.
        return Ok(0);
    }
    let schema = batches[0].schema();
    // Encode to a Bytes buffer, then PUT in one call. AsyncArrowWriter
    // writes to anything implementing AsyncWrite; we use a Vec<u8>
    // wrapped in a Compat shim via `tokio::io::AsyncWriteExt`.
    let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);
    let props = WriterProperties::builder().build();
    let mut writer = AsyncArrowWriter::try_new(&mut buf, schema.clone(), Some(props))
        .map_err(|e| BackendError::Query(format!("parquet writer init: {e}")))?;
    let mut total: u64 = 0;
    for batch in &batches {
        total += batch.num_rows() as u64;
        writer
            .write(batch)
            .await
            .map_err(|e| BackendError::Query(format!("parquet write batch: {e}")))?;
    }
    writer
        .close()
        .await
        .map_err(|e| BackendError::Query(format!("parquet close: {e}")))?;
    let bytes = Bytes::from(buf);
    store
        .put(&path, bytes.into())
        .await
        .map_err(|e| BackendError::Connection(format!("put {path}: {e}")))?;
    Ok(total)
}

/// Delete every object under `prefix`. Used by `WriteMode::Truncate`.
/// Walks the listing and issues per-object deletes — `object_store`
/// has no atomic prefix-delete primitive (S3/Azure/GCS don't either,
/// and emulating it on the client is what every cloud SDK does).
async fn delete_under_prefix(
    store: &Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
) -> Result<(), BackendError> {
    let mut listing = store.list(Some(prefix));
    while let Some(meta) = listing.next().await {
        let meta = meta.map_err(|e| BackendError::Connection(format!("list: {e}")))?;
        store
            .delete(&meta.location)
            .await
            .map_err(|e| BackendError::Connection(format!("delete {}: {e}", meta.location)))?;
    }
    Ok(())
}

#[async_trait]
impl Backend for ObjectStoreBackend {
    fn dialect(&self) -> Dialect {
        Dialect::ObjectStore {
            format: self.format,
        }
    }

    fn connection_info(&self) -> ConnectionInfo {
        ConnectionInfo {
            host: "object_store".into(),
            port: 0,
            dbname: self.base_label.clone(),
            user: "local".into(),
        }
    }

    fn dsn(&self) -> Option<String> {
        Some(self.dsn.clone())
    }

    /// For object stores, "ping" means: can we list the root? A failed
    /// list (no permissions, bucket missing, root-dir gone) surfaces here
    /// instead of inside the strategy executor.
    async fn ping(&self) -> Result<(), BackendError> {
        let mut listing = self.store.list(None);
        // Drain at most one entry — full enumeration of huge prefixes
        // is wasteful for a liveness check.
        if let Some(item) = listing.next().await {
            item.map_err(|e| BackendError::Connection(format!("ping list: {e}")))?;
        }
        Ok(())
    }

    /// Object stores have no "execute SQL"-style surface. We reject
    /// rather than silently no-op so user code that calls `execute`
    /// against an object store gets a clear error.
    async fn execute(&self, _statement: &str) -> Result<u64, BackendError> {
        Err(BackendError::Other(
            "ObjectStore backend has no execute() surface — \
             use read_arrow_stream / write_arrow_stream, or run_append / \
             run_truncate (Phase 34f)"
                .into(),
        ))
    }

    async fn read_arrow_stream(&self, query: &str) -> Result<ArrowBatchStream, BackendError> {
        // `query` is interpreted as a relative path prefix. Empty string
        // → read everything under the root.
        let prefix = ObjectPath::from(query);
        // Format dispatch lands in 34b–d; 34a is Parquet only. Reject
        // mismatched calls with a clear error so users don't accidentally
        // get empty results.
        match self.format {
            ObjectFormat::Parquet => {}
            other => {
                return Err(BackendError::Other(format!(
                    "ObjectStore read_arrow_stream: format {other:?} lands in \
                     a later sub-phase (34a covers Parquet only)"
                )));
            }
        }
        let batches = read_parquet_under_prefix(&self.store, prefix).await?;
        let stream = futures_util::stream::iter(batches.into_iter().map(Ok));
        Ok(Box::pin(stream))
    }

    async fn write_arrow_stream(
        &self,
        target: &TargetTable,
        stream: ArrowBatchStream,
        mode: WriteMode,
    ) -> Result<u64, BackendError> {
        match self.format {
            ObjectFormat::Parquet => {}
            other => {
                return Err(BackendError::Other(format!(
                    "ObjectStore write_arrow_stream: format {other:?} lands in \
                     a later sub-phase (34a covers Parquet only)"
                )));
            }
        }
        let prefix = target_prefix(target);
        if mode == WriteMode::Truncate {
            delete_under_prefix(&self.store, &prefix).await?;
        }
        let path = new_object_path(&prefix, ext_for_format(self.format));
        write_parquet_at_path(&self.store, path, stream).await
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
            "ObjectStore run_append lands in Phase 34f".into(),
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
            "ObjectStore run_truncate lands in Phase 34f".into(),
        ))
    }

    /// merge has no native impl on raw object storage — files are
    /// immutable. Iceberg / Delta (Phase 35) is the right tool for
    /// transactional updates against object storage.
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
            "ObjectStore: merge / scd1 / scd2 are not supported on raw files; \
             use the IcebergBackend or DeltaBackend (Phase 35) for transactional \
             updates against object storage"
                .into(),
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
            "ObjectStore: merge / scd1 / scd2 are not supported on raw files; \
             use the IcebergBackend or DeltaBackend (Phase 35)"
                .into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};

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
    async fn objectstore_local_parquet_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let backend = ObjectStoreBackend::open_local(dir.path(), ObjectFormat::Parquet).unwrap();
        assert!(matches!(
            backend.dialect(),
            Dialect::ObjectStore {
                format: ObjectFormat::Parquet
            }
        ));
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
    async fn objectstore_truncate_clears_prefix_first() {
        let dir = tempfile::tempdir().unwrap();
        let backend = ObjectStoreBackend::open_local(dir.path(), ObjectFormat::Parquet).unwrap();
        let target = TargetTable {
            schema: "raw".into(),
            name: "events".into(),
        };
        // Two appends → two files.
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
        let total_before: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_before, 6);

        // Truncate then write — old files gone.
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
        let total_after: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_after, 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn objectstore_execute_is_rejected_with_pointer() {
        let dir = tempfile::tempdir().unwrap();
        let backend = ObjectStoreBackend::open_local(dir.path(), ObjectFormat::Parquet).unwrap();
        let err = backend.execute("anything").await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("ObjectStore backend has no execute()"),
            "got: {msg}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn objectstore_merge_is_rejected_with_iceberg_pointer() {
        let dir = tempfile::tempdir().unwrap();
        let backend = ObjectStoreBackend::open_local(dir.path(), ObjectFormat::Parquet).unwrap();
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
                "SELECT 1",
                &["id".into()],
                &["x".into()],
                "p",
                "merge",
                None,
                None,
                false,
            )
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("Iceberg"), "got: {msg}");
        assert!(msg.contains("Phase 35"), "got: {msg}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn objectstore_csv_format_unsupported_in_34a() {
        let dir = tempfile::tempdir().unwrap();
        let backend = ObjectStoreBackend::open_local(dir.path(), ObjectFormat::Csv).unwrap();
        let err = match backend.read_arrow_stream("anything").await {
            Ok(_) => panic!("expected error for CSV in 34a"),
            Err(e) => e,
        };
        let msg = err.to_string();
        assert!(msg.contains("34a covers Parquet only"), "got: {msg}");
    }
}
