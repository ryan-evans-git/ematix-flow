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
use object_store::ObjectStoreExt;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use parquet::arrow::AsyncArrowWriter;
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

use crate::backend::{
    ArrowBatchStream, Backend, BackendError, DeleteHandling, Dialect, ObjectFormat,
    ObjectWriteOptions, ParquetCompression, StrategyRunResult, TargetTable, WriteMode,
};
use crate::pg::ConnectionInfo;
use crate::types::TableSpec;

/// Path prefix under the object-store root where run_history JSONL
/// events are written. One file per run, named by run_id, so writes
/// are conflict-free under concurrent execution. Lives next to the
/// data prefixes — a peer with a leading underscore so a `target/`
/// listing won't surface them.
const RUN_HISTORY_PREFIX: &str = "_ematix_flow/run_history";

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
    /// Π.1.4: per-format write-time options (Parquet compression,
    /// CSV delimiter / header). Read paths don't consult this — the
    /// underlying readers infer codec / delimiter from file metadata.
    write_options: ObjectWriteOptions,
    /// Σ.B PR 1: original location config, retained so
    /// [`Backend::config`] can reconstruct an identical backend on
    /// another node. Carries credentials in plaintext — same trust
    /// boundary as the existing `dsn` field.
    location: crate::backend::ObjectStoreLocation,
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
            write_options: ObjectWriteOptions::default(),
            location: crate::backend::ObjectStoreLocation::Local {
                root_dir: root.display().to_string(),
            },
        })
    }

    /// Π.1.4: override the per-format write options. The defaults
    /// (built from [`ObjectWriteOptions::default`]) match the
    /// historical pre-Π.1.4 behavior — Parquet uncompressed, CSV
    /// comma-delimited with header. Use to opt in to compression or
    /// switch CSV framing.
    pub fn with_write_options(mut self, options: ObjectWriteOptions) -> Self {
        self.write_options = options;
        self
    }

    /// Read access to the configured write options. Mainly for tests +
    /// logging.
    pub fn write_options(&self) -> &ObjectWriteOptions {
        &self.write_options
    }

    /// Borrow the underlying object store. Used by tests and (later)
    /// the strategy executors.
    pub fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    pub fn format(&self) -> ObjectFormat {
        self.format
    }

    /// Open an S3-backed store. `endpoint` is the full URL including
    /// scheme (e.g. `http://localhost:9000` for MinIO,
    /// `https://s3.amazonaws.com` for real AWS — but real-AWS users
    /// usually leave endpoint empty and rely on region).
    ///
    /// HTTP endpoints (i.e. local MinIO) are allowed by enabling
    /// `with_allow_http(true)` whenever the scheme is `http://`. AWS
    /// itself only ever uses HTTPS.
    ///
    /// The bucket must already exist — `object_store` has no
    /// `CreateBucket` primitive on the trait. Tests using MinIO can
    /// create the bucket via `docker exec mkdir -p /data/<bucket>`.
    pub fn open_s3(
        endpoint: &str,
        bucket: &str,
        region: &str,
        access_key: &str,
        secret_key: &str,
        format: ObjectFormat,
    ) -> Result<Self, BackendError> {
        let mut builder = AmazonS3Builder::new()
            .with_endpoint(endpoint)
            .with_bucket_name(bucket)
            .with_region(region)
            .with_access_key_id(access_key)
            .with_secret_access_key(secret_key);
        if endpoint.starts_with("http://") {
            builder = builder.with_allow_http(true);
        }
        let s3 = builder
            .build()
            .map_err(|e| BackendError::Connection(format!("s3 build: {e}")))?;
        Ok(Self {
            store: Arc::new(s3),
            format,
            dsn: format!("s3://{bucket}@{endpoint}"),
            base_label: format!("s3://{bucket}"),
            write_options: ObjectWriteOptions::default(),
            location: crate::backend::ObjectStoreLocation::S3 {
                endpoint: endpoint.to_string(),
                bucket: bucket.to_string(),
                region: region.to_string(),
                access_key: access_key.to_string(),
                secret_key: secret_key.to_string(),
            },
        })
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
    options: &ObjectWriteOptions,
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
    let mut props_builder = WriterProperties::builder();
    if let Some(codec) = options.parquet_compression {
        props_builder = props_builder.set_compression(parquet_compression_to_codec(codec));
    }
    let props = props_builder.build();
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

/// Read all CSV files under `prefix` and concatenate their Arrow batches.
/// Each file is assumed to have a header row; the schema is inferred
/// per file from the first 1024 records (arrow-csv's default cap).
/// All files in the prefix should share a compatible schema; mismatched
/// columns surface as errors when the second file's batches arrive.
async fn read_csv_under_prefix(
    store: &Arc<dyn ObjectStore>,
    prefix: ObjectPath,
) -> Result<Vec<RecordBatch>, BackendError> {
    use arrow_csv::reader::Format;

    let mut listing = store.list(Some(&prefix));
    let mut batches: Vec<RecordBatch> = Vec::new();
    while let Some(meta) = listing.next().await {
        let meta = meta.map_err(|e| BackendError::Connection(format!("list: {e}")))?;
        if !meta.location.as_ref().ends_with(".csv") {
            continue;
        }
        let bytes = store
            .get(&meta.location)
            .await
            .map_err(|e| BackendError::Connection(format!("get {}: {e}", meta.location)))?
            .bytes()
            .await
            .map_err(|e| BackendError::Connection(format!("get bytes: {e}")))?;
        // Two passes over the bytes: first to infer schema, second to
        // build the typed reader. arrow-csv's Format consumes the
        // reader, so we hand it a fresh Cursor each time.
        let format = Format::default().with_header(true);
        let (schema, _records_inferred) = format
            .infer_schema(std::io::Cursor::new(&bytes), Some(1024))
            .map_err(|e| BackendError::Query(format!("csv infer: {e}")))?;
        let reader = arrow_csv::ReaderBuilder::new(Arc::new(schema))
            .with_header(true)
            .build(std::io::Cursor::new(bytes))
            .map_err(|e| BackendError::Query(format!("csv reader build: {e}")))?;
        for b in reader {
            let b = b.map_err(|e| BackendError::Query(format!("csv batch: {e}")))?;
            batches.push(b);
        }
    }
    Ok(batches)
}

/// Write a stream of Arrow batches as one CSV file at `path`. Header
/// row mirrors the schema's field names. Uses arrow-csv's default
/// formatting (comma delimiter, double-quote escape).
async fn write_csv_at_path(
    store: &Arc<dyn ObjectStore>,
    path: ObjectPath,
    mut stream: ArrowBatchStream,
    options: &ObjectWriteOptions,
) -> Result<u64, BackendError> {
    let mut batches: Vec<RecordBatch> = Vec::new();
    while let Some(b) = stream.next().await {
        batches.push(b?);
    }
    if batches.is_empty() {
        return Ok(0);
    }
    let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);
    let header = options.csv_header.unwrap_or(true);
    let mut builder = arrow_csv::WriterBuilder::new().with_header(header);
    if let Some(d) = options.csv_delimiter {
        builder = builder.with_delimiter(d);
    }
    let mut writer = builder.build(&mut buf);
    let mut total: u64 = 0;
    for batch in &batches {
        total += batch.num_rows() as u64;
        writer
            .write(batch)
            .map_err(|e| BackendError::Query(format!("csv write batch: {e}")))?;
    }
    drop(writer);
    let bytes = Bytes::from(buf);
    store
        .put(&path, bytes.into())
        .await
        .map_err(|e| BackendError::Connection(format!("put {path}: {e}")))?;
    Ok(total)
}

/// Π.1.4: map our narrow `ParquetCompression` enum to the parquet
/// crate's wide `Compression`. ZSTD level defaults to 3 — the
/// parquet crate's recommended default.
fn parquet_compression_to_codec(codec: ParquetCompression) -> parquet::basic::Compression {
    use parquet::basic::{Compression, GzipLevel, ZstdLevel};
    match codec {
        ParquetCompression::Uncompressed => Compression::UNCOMPRESSED,
        ParquetCompression::Snappy => Compression::SNAPPY,
        ParquetCompression::Gzip => Compression::GZIP(GzipLevel::default()),
        ParquetCompression::Zstd => Compression::ZSTD(ZstdLevel::default()),
    }
}

/// Read all JSONL files (newline-delimited JSON) under `prefix` and
/// concatenate their Arrow batches. Schema is inferred per file via
/// `infer_json_schema_from_seekable`, which seeks the reader back to
/// the start so a single Cursor handles both passes (vs CSV which
/// needs two fresh Cursors).
async fn read_jsonl_under_prefix(
    store: &Arc<dyn ObjectStore>,
    prefix: ObjectPath,
) -> Result<Vec<RecordBatch>, BackendError> {
    use arrow_json::ReaderBuilder;
    use arrow_json::reader::infer_json_schema_from_seekable;

    let mut listing = store.list(Some(&prefix));
    let mut batches: Vec<RecordBatch> = Vec::new();
    while let Some(meta) = listing.next().await {
        let meta = meta.map_err(|e| BackendError::Connection(format!("list: {e}")))?;
        let loc = meta.location.as_ref();
        // Accept both `.jsonl` (canonical) and `.json` (common alias).
        if !loc.ends_with(".jsonl") && !loc.ends_with(".json") {
            continue;
        }
        let bytes = store
            .get(&meta.location)
            .await
            .map_err(|e| BackendError::Connection(format!("get {}: {e}", meta.location)))?
            .bytes()
            .await
            .map_err(|e| BackendError::Connection(format!("get bytes: {e}")))?;
        let mut cursor = std::io::Cursor::new(bytes.as_ref());
        let (schema, _records_inferred) = infer_json_schema_from_seekable(&mut cursor, Some(1024))
            .map_err(|e| BackendError::Query(format!("json infer: {e}")))?;
        // `infer_json_schema_from_seekable` rewinds; the same Cursor
        // works for the typed reader.
        let reader = ReaderBuilder::new(Arc::new(schema))
            .build(std::io::BufReader::new(cursor))
            .map_err(|e| BackendError::Query(format!("json reader: {e}")))?;
        for b in reader {
            let b = b.map_err(|e| BackendError::Query(format!("json batch: {e}")))?;
            batches.push(b);
        }
    }
    Ok(batches)
}

/// Write a stream of Arrow batches as one JSONL file at `path`. Each
/// row becomes a JSON object on its own line.
async fn write_jsonl_at_path(
    store: &Arc<dyn ObjectStore>,
    path: ObjectPath,
    mut stream: ArrowBatchStream,
) -> Result<u64, BackendError> {
    use arrow_json::LineDelimitedWriter;

    let mut batches: Vec<RecordBatch> = Vec::new();
    while let Some(b) = stream.next().await {
        batches.push(b?);
    }
    if batches.is_empty() {
        return Ok(0);
    }
    let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);
    let mut writer = LineDelimitedWriter::new(&mut buf);
    let mut total: u64 = 0;
    for batch in &batches {
        total += batch.num_rows() as u64;
        writer
            .write(batch)
            .map_err(|e| BackendError::Query(format!("jsonl write batch: {e}")))?;
    }
    writer
        .finish()
        .map_err(|e| BackendError::Query(format!("jsonl finish: {e}")))?;
    drop(writer);
    let bytes = Bytes::from(buf);
    store
        .put(&path, bytes.into())
        .await
        .map_err(|e| BackendError::Connection(format!("put {path}: {e}")))?;
    Ok(total)
}

/// Read all ORC files under `prefix` and concatenate their Arrow batches.
/// `orc-rust` 0.6 has a sync `ArrowReaderBuilder` that needs a
/// `ChunkReader` (essentially `Read + Seek + len`); `Cursor<&[u8]>`
/// satisfies that.
async fn read_orc_under_prefix(
    store: &Arc<dyn ObjectStore>,
    prefix: ObjectPath,
) -> Result<Vec<RecordBatch>, BackendError> {
    use orc_rust::ArrowReaderBuilder;

    let mut listing = store.list(Some(&prefix));
    let mut batches: Vec<RecordBatch> = Vec::new();
    while let Some(meta) = listing.next().await {
        let meta = meta.map_err(|e| BackendError::Connection(format!("list: {e}")))?;
        if !meta.location.as_ref().ends_with(".orc") {
            continue;
        }
        let bytes = store
            .get(&meta.location)
            .await
            .map_err(|e| BackendError::Connection(format!("get {}: {e}", meta.location)))?
            .bytes()
            .await
            .map_err(|e| BackendError::Connection(format!("get bytes: {e}")))?;
        // orc-rust's ChunkReader is implemented directly on `Bytes`,
        // so we hand it the bytes without an intermediate Cursor.
        let reader = ArrowReaderBuilder::try_new(bytes)
            .map_err(|e| BackendError::Query(format!("orc open: {e}")))?
            .build();
        for b in reader {
            let b = b.map_err(|e| BackendError::Query(format!("orc batch: {e}")))?;
            batches.push(b);
        }
    }
    Ok(batches)
}

/// Write a stream of Arrow batches as one ORC file at `path`.
///
/// `ArrowWriter::close` consumes the writer and offers no `into_inner`
/// (verified through orc-rust 0.8), so we hand it a shared
/// `Arc<Mutex<Vec<u8>>>` wrapper that impls `std::io::Write`. After
/// close drops the writer (and the Arc clone it owned), the original
/// Arc is unique and we can take ownership of the Vec for the PUT.
async fn write_orc_at_path(
    store: &Arc<dyn ObjectStore>,
    path: ObjectPath,
    mut stream: ArrowBatchStream,
) -> Result<u64, BackendError> {
    use orc_rust::ArrowWriterBuilder;
    use std::sync::Mutex;

    let mut batches: Vec<RecordBatch> = Vec::new();
    while let Some(b) = stream.next().await {
        batches.push(b?);
    }
    if batches.is_empty() {
        return Ok(0);
    }
    let schema = batches[0].schema();

    struct SharedBuf(Arc<Mutex<Vec<u8>>>);
    impl std::io::Write for SharedBuf {
        fn write(&mut self, src: &[u8]) -> std::io::Result<usize> {
            let mut buf = self
                .0
                .lock()
                .map_err(|e| std::io::Error::other(format!("orc buf poisoned: {e}")))?;
            buf.extend_from_slice(src);
            Ok(src.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    // Scope the !Send writer in an inner block so it's dropped before
    // the next await — orc-rust's ArrowWriter holds a
    // `dyn ColumnStripeEncoder` without `Send` (still true through
    // 0.8), which would otherwise poison this future's Send bound.
    let (buf, total) = {
        let inner: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::with_capacity(64 * 1024)));
        let mut writer = ArrowWriterBuilder::new(SharedBuf(inner.clone()), schema)
            .try_build()
            .map_err(|e| BackendError::Query(format!("orc writer init: {e}")))?;
        let mut total: u64 = 0;
        for batch in &batches {
            total += batch.num_rows() as u64;
            writer
                .write(batch)
                .map_err(|e| BackendError::Query(format!("orc write batch: {e}")))?;
        }
        writer
            .close()
            .map_err(|e| BackendError::Query(format!("orc close: {e}")))?;
        // `close` dropped the writer and its SharedBuf clone. Our
        // `inner` is now the unique strong reference; take the Vec out.
        let buf = match Arc::try_unwrap(inner) {
            Ok(mutex) => mutex
                .into_inner()
                .map_err(|e| BackendError::Other(format!("orc buf into_inner: {e}")))?,
            Err(arc) => arc
                .lock()
                .map_err(|e| BackendError::Other(format!("orc buf lock: {e}")))?
                .clone(),
        };
        (buf, total)
    };
    let bytes = Bytes::from(buf);
    store
        .put(&path, bytes.into())
        .await
        .map_err(|e| BackendError::Connection(format!("put {path}: {e}")))?;
    Ok(total)
}

/// ISO-8601 timestamp for "now" with millisecond precision and a `Z`
/// suffix. We don't pull in `chrono` for this — the format is
/// stable enough to format directly from `SystemTime`.
fn chrono_compat_iso8601_now() -> String {
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

/// Howard Hinnant's chrono::civil_from_days. Mirrors the same helper
/// in `mysql_backend.rs` — kept local so this module has no
/// cross-backend imports just for one date routine.
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

/// Append a run-history event as a one-line JSON document at
/// `_ematix_flow/run_history/<run_id>.jsonl`. Object stores have no
/// SQL surface and append-to-existing-object semantics vary across
/// providers (S3 has none; Azure has Append Blobs but not on every
/// SKU), so each run gets its own file. Reading back is a prefix scan
/// + JSONL parse.
async fn record_run_event(
    store: &Arc<dyn ObjectStore>,
    run_id: &uuid::Uuid,
    event: &serde_json::Value,
) -> Result<(), BackendError> {
    let path = ObjectPath::from(format!("{RUN_HISTORY_PREFIX}/{}.jsonl", run_id.simple()));
    let mut bytes = serde_json::to_vec(event)
        .map_err(|e| BackendError::Other(format!("run-history serialize: {e}")))?;
    bytes.push(b'\n');
    store
        .put(&path, Bytes::from(bytes).into())
        .await
        .map_err(|e| BackendError::Connection(format!("run-history put: {e}")))?;
    Ok(())
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

    fn config(&self) -> crate::backend::BackendConfig {
        crate::backend::BackendConfig::ObjectStore(crate::backend::ObjectStoreConfig {
            location: self.location.clone(),
            format: self.format,
            write_options: self.write_options.clone(),
        })
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
        let batches = match self.format {
            ObjectFormat::Parquet => read_parquet_under_prefix(&self.store, prefix).await?,
            ObjectFormat::Csv => read_csv_under_prefix(&self.store, prefix).await?,
            ObjectFormat::JsonLines => read_jsonl_under_prefix(&self.store, prefix).await?,
            ObjectFormat::Orc => read_orc_under_prefix(&self.store, prefix).await?,
        };
        let stream = futures_util::stream::iter(batches.into_iter().map(Ok));
        Ok(Box::pin(stream))
    }

    async fn write_arrow_stream(
        &self,
        target: &TargetTable,
        stream: ArrowBatchStream,
        mode: WriteMode,
    ) -> Result<u64, BackendError> {
        let prefix = target_prefix(target);
        if mode == WriteMode::Truncate {
            delete_under_prefix(&self.store, &prefix).await?;
        }
        let path = new_object_path(&prefix, ext_for_format(self.format));
        match self.format {
            ObjectFormat::Parquet => {
                write_parquet_at_path(&self.store, path, stream, &self.write_options).await
            }
            ObjectFormat::Csv => {
                write_csv_at_path(&self.store, path, stream, &self.write_options).await
            }
            ObjectFormat::JsonLines => write_jsonl_at_path(&self.store, path, stream).await,
            ObjectFormat::Orc => write_orc_at_path(&self.store, path, stream).await,
        }
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
                "ObjectStore run_append: source_backend is required \
                 (object store is a target only — there is no same-DB \
                 path because raw files have no SQL surface)"
                    .into(),
            )
        })?;
        // Watermark filter is wrapped at the SQL layer in the source's
        // dialect — same as every DB-target backend. The object-store
        // target itself doesn't track watermarks (no MAX-queryable
        // surface); users running incremental loads to object storage
        // must persist `last_value_literal` externally.
        let watermark = incremental_column.map(|c| crate::meta::WatermarkConfig {
            column: c.to_string(),
            last_value_literal: last_value_literal.map(|s| s.to_string()),
        });
        let filtered_source =
            crate::meta::wrap_with_watermark_filter(source_query, watermark.as_ref());

        let run_id = uuid::Uuid::now_v7();
        let started_at = chrono_compat_iso8601_now();
        let target = TargetTable {
            schema: spec.schema.clone(),
            name: spec.name.clone(),
        };

        let inserted: u64 = if dry_run {
            // Probe the source so a missing query / bad credentials
            // surfaces; do not write to the target.
            let _ = source.read_arrow_stream(&filtered_source).await?;
            0
        } else {
            let stream = source.read_arrow_stream(&filtered_source).await?;
            self.write_arrow_stream(&target, stream, WriteMode::Append)
                .await?
        };
        let finished_at = chrono_compat_iso8601_now();
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
            "format": format!("{:?}", self.format),
        });
        // Best-effort: history-write failure doesn't unwind the data
        // write. Surface the error so callers can decide.
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
                "ObjectStore run_truncate: source_backend is required \
                 (object store is a target only)"
                    .into(),
            )
        })?;
        let run_id = uuid::Uuid::now_v7();
        let started_at = chrono_compat_iso8601_now();
        let target = TargetTable {
            schema: spec.schema.clone(),
            name: spec.name.clone(),
        };

        let inserted: u64 = if dry_run {
            // Touch the source but do not delete or write — a truncate
            // dry-run is a true no-op against the target.
            let _ = source.read_arrow_stream(source_query).await?;
            0
        } else {
            let stream = source.read_arrow_stream(source_query).await?;
            // `write_arrow_stream` with WriteMode::Truncate deletes
            // every existing object under the prefix before writing.
            self.write_arrow_stream(&target, stream, WriteMode::Truncate)
                .await?
        };
        let finished_at = chrono_compat_iso8601_now();
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
            "format": format!("{:?}", self.format),
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

    /// Π.1.4: Parquet writes round-trip through every supported
    /// compression codec. This is the contract the typed-Python
    /// `Target(parquet_compression=...)` field rides on.
    #[tokio::test(flavor = "multi_thread")]
    async fn parquet_round_trips_through_each_compression() {
        use crate::backend::ParquetCompression;

        // Repetitive data so the compressed file actually shrinks
        // measurably vs uncompressed — gives the test something to
        // assert on beyond "didn't crash".
        fn repetitive_batch() -> RecordBatch {
            let schema = Arc::new(ArrowSchema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("payload", DataType::Utf8, false),
            ]));
            let ids: Vec<i64> = (0..2_000).collect();
            let payloads: Vec<&'static str> = (0..2_000)
                .map(|_| "the quick brown fox jumps over the lazy dog")
                .collect();
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(ids)),
                    Arc::new(StringArray::from(payloads)),
                ],
            )
            .unwrap()
        }

        let mut sizes: std::collections::BTreeMap<&'static str, u64> =
            std::collections::BTreeMap::new();
        for (label, codec) in [
            ("uncompressed", ParquetCompression::Uncompressed),
            ("snappy", ParquetCompression::Snappy),
            ("gzip", ParquetCompression::Gzip),
            ("zstd", ParquetCompression::Zstd),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let backend = ObjectStoreBackend::open_local(dir.path(), ObjectFormat::Parquet)
                .unwrap()
                .with_write_options(ObjectWriteOptions {
                    parquet_compression: Some(codec),
                    ..Default::default()
                });
            let target = TargetTable {
                schema: "raw".into(),
                name: "events".into(),
            };
            let n = backend
                .write_arrow_stream(
                    &target,
                    arrow_stream_for(repetitive_batch()),
                    WriteMode::Append,
                )
                .await
                .unwrap();
            assert_eq!(n, 2_000, "{label}: row count");

            // Round-trip read — Parquet readers handle compression
            // transparently from file metadata.
            let stream = backend.read_arrow_stream("raw/events").await.unwrap();
            let batches: Vec<RecordBatch> = futures_util::TryStreamExt::try_collect(stream)
                .await
                .unwrap();
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 2_000, "{label}: read-back row count");

            // Capture the on-disk size for the codec compare below.
            let mut listing = backend.store().list(Some(&ObjectPath::from("raw/events")));
            let mut size = 0u64;
            while let Some(meta) = listing.next().await {
                size += meta.unwrap().size;
            }
            sizes.insert(label, size);
        }

        // Sanity-check: each non-trivial codec actually compressed the
        // file. Strict inequality — they should beat raw bytes by a
        // healthy margin on the repetitive corpus above.
        let raw = sizes["uncompressed"];
        for codec in ["snappy", "gzip", "zstd"] {
            let compressed = sizes[codec];
            assert!(
                compressed < raw,
                "{codec} = {compressed} bytes vs uncompressed = {raw} bytes — \
                 expected compression to actually reduce size"
            );
        }
    }

    /// Π.1.4: CSV writes honor user-set `csv_delimiter` + `csv_header`.
    #[tokio::test(flavor = "multi_thread")]
    async fn csv_honors_delimiter_and_header_options() {
        use object_store::path::Path as ObjectPath2;

        let dir = tempfile::tempdir().unwrap();
        let backend = ObjectStoreBackend::open_local(dir.path(), ObjectFormat::Csv)
            .unwrap()
            .with_write_options(ObjectWriteOptions {
                csv_delimiter: Some(b';'),
                csv_header: Some(false),
                ..Default::default()
            });
        let target = TargetTable {
            schema: "raw".into(),
            name: "events".into(),
        };
        backend
            .write_arrow_stream(&target, arrow_stream_for(small_batch()), WriteMode::Append)
            .await
            .unwrap();

        // Pull the raw bytes back so we can inspect framing — bypass
        // read_arrow_stream because that re-infers schema and would
        // hide the delimiter/header choice.
        let mut listing = backend.store().list(Some(&ObjectPath2::from("raw/events")));
        let entry = listing.next().await.unwrap().unwrap();
        let bytes = backend
            .store()
            .get(&entry.location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let text = std::str::from_utf8(&bytes).unwrap();
        // Header was suppressed → first line is data; uses ';' not ','.
        assert!(
            !text.starts_with("id;name") && !text.starts_with("id,name"),
            "header should not be present, got: {text:?}"
        );
        assert!(text.contains(";"), "delimiter should be ';', got: {text:?}");
        assert!(
            !text.contains("id,name"),
            "should not contain comma-delimited header, got: {text:?}"
        );
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

    // --- Phase 34d: ORC -----------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn objectstore_local_orc_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let backend = ObjectStoreBackend::open_local(dir.path(), ObjectFormat::Orc).unwrap();
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

        let names = batches[0]
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "alice");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn objectstore_orc_truncate_clears_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let backend = ObjectStoreBackend::open_local(dir.path(), ObjectFormat::Orc).unwrap();
        let target = TargetTable {
            schema: "raw".into(),
            name: "events".into(),
        };
        backend
            .write_arrow_stream(&target, arrow_stream_for(small_batch()), WriteMode::Append)
            .await
            .unwrap();
        backend
            .write_arrow_stream(&target, arrow_stream_for(small_batch()), WriteMode::Append)
            .await
            .unwrap();
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
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3, "old files removed by truncate");
    }

    // --- Phase 34b: CSV ------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn objectstore_local_csv_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let backend = ObjectStoreBackend::open_local(dir.path(), ObjectFormat::Csv).unwrap();
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

        // Spot-check a value round-trips: arrow-csv re-infers types from
        // text, so name "alice" should come back as Utf8 / String.
        let names = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "alice");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn objectstore_csv_truncate_clears_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let backend = ObjectStoreBackend::open_local(dir.path(), ObjectFormat::Csv).unwrap();
        let target = TargetTable {
            schema: "raw".into(),
            name: "events".into(),
        };
        backend
            .write_arrow_stream(&target, arrow_stream_for(small_batch()), WriteMode::Append)
            .await
            .unwrap();
        backend
            .write_arrow_stream(&target, arrow_stream_for(small_batch()), WriteMode::Append)
            .await
            .unwrap();
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
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3, "old files removed by truncate");
    }

    // --- Phase 34c: JSONL ----------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn objectstore_local_jsonl_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let backend = ObjectStoreBackend::open_local(dir.path(), ObjectFormat::JsonLines).unwrap();
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

        let names = batches[0]
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "alice");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn objectstore_jsonl_truncate_clears_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let backend = ObjectStoreBackend::open_local(dir.path(), ObjectFormat::JsonLines).unwrap();
        let target = TargetTable {
            schema: "raw".into(),
            name: "events".into(),
        };
        backend
            .write_arrow_stream(&target, arrow_stream_for(small_batch()), WriteMode::Append)
            .await
            .unwrap();
        backend
            .write_arrow_stream(&target, arrow_stream_for(small_batch()), WriteMode::Append)
            .await
            .unwrap();
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
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3, "old files removed by truncate");
    }

    // --- Phase 34f: run_append + run_truncate (DuckDB → ObjectStore) -------

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
    async fn objectstore_run_append_from_duckdb_writes_parquet() {
        let dir = tempfile::tempdir().unwrap();
        let target_backend =
            ObjectStoreBackend::open_local(dir.path(), ObjectFormat::Parquet).unwrap();
        let source = duckdb_with_events().await;
        let spec = small_table_spec();

        let result = target_backend
            .run_append(
                &spec,
                "SELECT id, name FROM s.events ORDER BY id",
                "p34f_append",
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

        // Read back via the same backend and confirm row count.
        let stream = target_backend
            .read_arrow_stream("raw/events")
            .await
            .unwrap();
        let batches: Vec<RecordBatch> = futures_util::TryStreamExt::try_collect(stream)
            .await
            .unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3);

        // run_history: one JSONL file under _ematix_flow/run_history.
        let history_stream = target_backend
            .read_arrow_stream("non-existent-prefix")
            .await
            .unwrap();
        let _: Vec<RecordBatch> = futures_util::TryStreamExt::try_collect(history_stream)
            .await
            .unwrap();
        // Direct-list the history prefix via the underlying store —
        // history files are JSONL even though the backend is set to
        // Parquet, so a format-typed read_arrow_stream would mismatch.
        let mut listing = target_backend
            .store()
            .list(Some(&ObjectPath::from("_ematix_flow/run_history")));
        let mut history_count = 0;
        while futures_util::StreamExt::next(&mut listing).await.is_some() {
            history_count += 1;
        }
        assert_eq!(history_count, 1, "exactly one run_history event");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn objectstore_run_truncate_from_duckdb_replaces_target() {
        let dir = tempfile::tempdir().unwrap();
        let target_backend =
            ObjectStoreBackend::open_local(dir.path(), ObjectFormat::Parquet).unwrap();
        let source = duckdb_with_events().await;
        let spec = small_table_spec();

        // Two appends seed the target with stale files.
        for tag in ["t34f_a", "t34f_b"] {
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
        // Truncate replaces them with one fresh write.
        let result = target_backend
            .run_truncate(
                &spec,
                "SELECT id, name FROM s.events ORDER BY id",
                "t34f_trunc",
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
        assert_eq!(total, 3, "truncate left only the latest write");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn objectstore_run_append_dry_run_does_not_write() {
        let dir = tempfile::tempdir().unwrap();
        let target_backend =
            ObjectStoreBackend::open_local(dir.path(), ObjectFormat::Parquet).unwrap();
        let source = duckdb_with_events().await;
        let spec = small_table_spec();

        let result = target_backend
            .run_append(
                &spec,
                "SELECT id, name FROM s.events ORDER BY id",
                "p34f_dry",
                Some(source.as_ref()),
                None,
                None,
                true, // dry_run
            )
            .await
            .unwrap();
        assert_eq!(result.status, "dry_run");
        assert_eq!(result.rows_inserted, 0);

        // No data file should exist under the data prefix.
        let stream = target_backend
            .read_arrow_stream("raw/events")
            .await
            .unwrap();
        let batches: Vec<RecordBatch> = futures_util::TryStreamExt::try_collect(stream)
            .await
            .unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 0, "dry_run must not write data");

        // But the run_history event should record dry_run for audit.
        let mut listing = target_backend
            .store()
            .list(Some(&ObjectPath::from("_ematix_flow/run_history")));
        let mut history_count = 0;
        while futures_util::StreamExt::next(&mut listing).await.is_some() {
            history_count += 1;
        }
        assert_eq!(history_count, 1, "dry_run still emits a history event");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn objectstore_run_append_rejects_missing_source() {
        let dir = tempfile::tempdir().unwrap();
        let target_backend =
            ObjectStoreBackend::open_local(dir.path(), ObjectFormat::Parquet).unwrap();
        let spec = small_table_spec();
        let err = target_backend
            .run_append(&spec, "ignored", "p34f_no_src", None, None, None, false)
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("source_backend is required"), "got: {msg}");
    }
}
