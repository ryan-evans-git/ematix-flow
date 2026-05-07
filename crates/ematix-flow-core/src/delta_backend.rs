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

use std::collections::HashMap;
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
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::prefix::PrefixStore;
use url::Url;

use crate::backend::{
    ArrowBatchStream, Backend, BackendError, DeleteHandling, Dialect, StrategyRunResult,
    TargetTable, WriteMode,
};
use crate::pg::ConnectionInfo;
use crate::strategy::scd2::{IS_CURRENT_COL, ROW_HASH_COL, VALID_FROM_COL, VALID_TO_COL};
use crate::types::TableSpec;

/// Delta-backed implementation of `Backend`.
///
/// Holds an absolute root URL (`file:///abs/path`); each
/// `target_table` resolves to a sub-URL `<root>/<schema>/<name>`.
/// Storage credentials for cloud backends (S3/Azure/GCS) land in
/// 35f via storage_options on construction.
pub struct DeltaBackend {
    /// Absolute URL to the root directory holding all target tables
    /// (`file://...` for local, `s3://bucket/prefix/` for cloud).
    /// Used to build per-target URLs.
    root_url: Url,
    /// Display-only label for `connection_info` and logs.
    base_label: String,
    /// Sidecar object store rooted at the same location, used for
    /// run_history JSONL writes. `LocalFileSystem` for local roots;
    /// `AmazonS3` for S3-compatible roots.
    store: Arc<dyn ObjectStore>,
    /// Storage options handed to deltalake's `DeltaTableBuilder` so
    /// the underlying object_store factory can authenticate. Empty
    /// for local FS; populated with `AWS_ACCESS_KEY_ID` etc. for S3.
    storage_options: HashMap<String, String>,
    /// Phase 40.1: column names used for partitioned writes. Empty
    /// = unpartitioned (the default + the only option pre-40.1).
    /// Applied via `WriteBuilder::with_partition_columns` on first
    /// write — Delta auto-creates the table with this layout if the
    /// location is uninitialized. Subsequent writes to an
    /// already-partitioned table must match the existing layout, or
    /// deltalake-rs raises a clear error.
    partition_columns: Vec<String>,
    /// Σ.B PR 1: original location config retained for round-trip
    /// reconstruction via [`Backend::config`].
    location: crate::backend::DeltaLocation,
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
            storage_options: HashMap::new(),
            partition_columns: Vec::new(),
            location: crate::backend::DeltaLocation::Local {
                root_dir: abs.display().to_string(),
            },
        })
    }

    /// Open an S3-backed Delta root at `s3://<bucket>/<prefix>/`.
    /// `endpoint` is the full URL (e.g. `http://localhost:9000` for
    /// MinIO; empty string for real AWS). `prefix` may be empty to use
    /// the bucket root.
    ///
    /// Sets `AWS_S3_ALLOW_UNSAFE_RENAME=true` because S3 has no atomic
    /// rename and deltalake's default policy refuses to write without
    /// either (a) DynamoDB locking or (b) an explicit opt-in. For
    /// single-writer test/dev workloads against MinIO this is fine;
    /// prod deployments with concurrent writers should configure the
    /// DynamoDB locking provider directly via `with_storage_option`.
    pub fn open_s3(
        endpoint: &str,
        bucket: &str,
        prefix: &str,
        region: &str,
        access_key: &str,
        secret_key: &str,
    ) -> Result<Self, BackendError> {
        // deltalake-aws's S3LogStore registers itself when the
        // `s3` feature is on (via the `#[ctor]` block in
        // deltalake/lib.rs). It only needs to run once per process
        // — calling it more times is a no-op.
        deltalake::aws::register_handlers(None);

        let trimmed_prefix = prefix.trim_matches('/');
        let url_str = if trimmed_prefix.is_empty() {
            format!("s3://{bucket}/")
        } else {
            format!("s3://{bucket}/{trimmed_prefix}/")
        };
        let root_url = Url::parse(&url_str)
            .map_err(|e| BackendError::Connection(format!("delta s3 url: {e}")))?;

        // Build the sidecar object_store with the same creds.
        let mut s3_builder = AmazonS3Builder::new()
            .with_endpoint(endpoint)
            .with_bucket_name(bucket)
            .with_region(region)
            .with_access_key_id(access_key)
            .with_secret_access_key(secret_key);
        if endpoint.starts_with("http://") {
            s3_builder = s3_builder.with_allow_http(true);
        }
        let s3 = s3_builder
            .build()
            .map_err(|e| BackendError::Connection(format!("delta s3 sidecar: {e}")))?;
        // Wrap with a prefix so sidecar `put`/`list` are relative to
        // the same `<bucket>/<prefix>/` namespace as Delta tables.
        let store: Arc<dyn ObjectStore> = if trimmed_prefix.is_empty() {
            Arc::new(s3)
        } else {
            Arc::new(PrefixStore::new(s3, trimmed_prefix))
        };

        // Storage options threaded into `DeltaTableBuilder`. Keys are
        // the conventional environment-style names that deltalake-aws
        // recognizes.
        let mut opts: HashMap<String, String> = HashMap::new();
        opts.insert("AWS_ACCESS_KEY_ID".into(), access_key.into());
        opts.insert("AWS_SECRET_ACCESS_KEY".into(), secret_key.into());
        opts.insert("AWS_REGION".into(), region.into());
        if !endpoint.is_empty() {
            opts.insert("AWS_ENDPOINT_URL".into(), endpoint.into());
        }
        if endpoint.starts_with("http://") {
            opts.insert("AWS_ALLOW_HTTP".into(), "true".into());
        }
        opts.insert("AWS_S3_ALLOW_UNSAFE_RENAME".into(), "true".into());

        Ok(Self {
            root_url,
            base_label: format!("s3://{bucket}/{trimmed_prefix}"),
            store,
            storage_options: opts,
            partition_columns: Vec::new(),
            location: crate::backend::DeltaLocation::S3 {
                endpoint: endpoint.to_string(),
                bucket: bucket.to_string(),
                prefix: trimmed_prefix.to_string(),
                region: region.to_string(),
                access_key: access_key.to_string(),
                secret_key: secret_key.to_string(),
            },
        })
    }

    /// Phase 40.1: configure partition columns for first-write
    /// table creation. Pre-existing tables retain their layout;
    /// deltalake-rs verifies + rejects mismatches at write time.
    pub fn with_partition_columns(
        mut self,
        partition_columns: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.partition_columns = partition_columns.into_iter().map(|s| s.into()).collect();
        self
    }

    /// Borrow the configured partition columns.
    pub fn partition_columns(&self) -> &[String] {
        &self.partition_columns
    }

    /// Open or uninitialized table at `url`, threading
    /// `self.storage_options` so cloud-backed roots authenticate.
    /// Mirrors the free-function helper from 35b–e but as a method
    /// so the credentials travel with `&self`.
    async fn open_table(&self, url: Url) -> Result<DeltaTable, BackendError> {
        if url.scheme() == "file"
            && let Ok(path) = url.to_file_path()
        {
            std::fs::create_dir_all(&path).map_err(|e| {
                BackendError::Connection(format!(
                    "creating delta table dir {}: {e}",
                    path.display()
                ))
            })?;
        }
        let mut builder = DeltaTableBuilder::from_url(url.clone())
            .map_err(|e| BackendError::Connection(format!("delta builder {url}: {e}")))?;
        if !self.storage_options.is_empty() {
            builder = builder.with_storage_options(self.storage_options.clone());
        }
        let mut table = builder
            .build()
            .map_err(|e| BackendError::Connection(format!("delta builder {url}: {e}")))?;
        match table.load().await {
            Ok(_) => {}
            Err(DeltaTableError::NotATable(_)) => {}
            Err(e) if e.to_string().contains("Path does not exist") => {}
            Err(e) => {
                return Err(BackendError::Connection(format!("delta load {url}: {e}")));
            }
        }
        Ok(table)
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

/// Compute one sha256 row_hash per row over the user's `compare_columns`.
/// Identical protocol to the SQLite `ematix_sha256` UDF and the
/// `unhex(sha256(...))` expression on PG/DuckDB/MySQL: stringify each
/// value with a CHAR(0)-wrapped 'NULL' sentinel, separate columns
/// with CHAR(1), feed into sha256. Returns one 32-byte BinaryArray.
fn compute_row_hash(
    batch: &arrow_array::RecordBatch,
    compare_cols: &[String],
) -> Result<arrow_array::BinaryArray, BackendError> {
    use arrow_array::Array;
    use sha2::{Digest, Sha256};

    let n_rows = batch.num_rows();
    let arrays: Vec<&dyn Array> = compare_cols
        .iter()
        .map(|name| {
            batch
                .column_by_name(name)
                .map(|a| a.as_ref())
                .ok_or_else(|| {
                    BackendError::Other(format!(
                        "scd2: compare column '{name}' not found in source schema"
                    ))
                })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut builder = arrow_array::builder::BinaryBuilder::with_capacity(n_rows, n_rows * 32);
    for row in 0..n_rows {
        let mut hasher = Sha256::new();
        for (i, arr) in arrays.iter().enumerate() {
            if i > 0 {
                hasher.update([0x01u8]);
            }
            if arr.is_null(row) {
                hasher.update([0x00u8]);
                hasher.update(b"NULL");
                hasher.update([0x00u8]);
            } else {
                let s = arrow_value_as_string(*arr, row);
                hasher.update(s.as_bytes());
            }
        }
        let bytes: [u8; 32] = hasher.finalize().into();
        builder.append_value(bytes);
    }
    Ok(builder.finish())
}

/// Stringify a single Arrow value at `row` for hashing. Covers the
/// types the framework's other backends emit; uses Arrow's own Display
/// impl as a fallback so we don't have to enumerate every variant.
fn arrow_value_as_string(arr: &dyn arrow_array::Array, row: usize) -> String {
    use arrow_array::cast::AsArray;
    use arrow_array::types::*;
    use arrow_schema::DataType;
    match arr.data_type() {
        DataType::Int8 => arr.as_primitive::<Int8Type>().value(row).to_string(),
        DataType::Int16 => arr.as_primitive::<Int16Type>().value(row).to_string(),
        DataType::Int32 => arr.as_primitive::<Int32Type>().value(row).to_string(),
        DataType::Int64 => arr.as_primitive::<Int64Type>().value(row).to_string(),
        DataType::UInt8 => arr.as_primitive::<UInt8Type>().value(row).to_string(),
        DataType::UInt16 => arr.as_primitive::<UInt16Type>().value(row).to_string(),
        DataType::UInt32 => arr.as_primitive::<UInt32Type>().value(row).to_string(),
        DataType::UInt64 => arr.as_primitive::<UInt64Type>().value(row).to_string(),
        DataType::Float32 => arr.as_primitive::<Float32Type>().value(row).to_string(),
        DataType::Float64 => arr.as_primitive::<Float64Type>().value(row).to_string(),
        DataType::Boolean => arr
            .as_any()
            .downcast_ref::<arrow_array::BooleanArray>()
            .map(|b| b.value(row).to_string())
            .unwrap_or_default(),
        DataType::Utf8 => arr
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .map(|s| s.value(row).to_string())
            .unwrap_or_default(),
        DataType::LargeUtf8 => arr
            .as_any()
            .downcast_ref::<arrow_array::LargeStringArray>()
            .map(|s| s.value(row).to_string())
            .unwrap_or_default(),
        // Fallback: Arrow's array formatter handles everything else
        // (timestamps, decimals, lists). Slower than typed access but
        // correct for any DataType.
        _ => arrow_cast::display::array_value_to_string(arr, row).unwrap_or_default(),
    }
}

/// Append `row_hash` (Binary) to each batch.
fn augment_with_row_hash(
    batches: &[arrow_array::RecordBatch],
    compare_cols: &[String],
) -> Result<Vec<arrow_array::RecordBatch>, BackendError> {
    use arrow_array::Array;
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};

    let mut out = Vec::with_capacity(batches.len());
    for b in batches {
        let hash_array = compute_row_hash(b, compare_cols)?;
        let mut new_columns: Vec<Arc<dyn Array>> = b.columns().iter().map(Arc::clone).collect();
        new_columns.push(Arc::new(hash_array));
        let mut new_fields: Vec<Arc<Field>> = b.schema().fields().iter().map(Arc::clone).collect();
        new_fields.push(Arc::new(Field::new(ROW_HASH_COL, DataType::Binary, false)));
        let new_schema = Arc::new(ArrowSchema::new(new_fields));
        out.push(
            arrow_array::RecordBatch::try_new(new_schema, new_columns)
                .map_err(|e| BackendError::Other(format!("scd2 augment row_hash: {e}")))?,
        );
    }
    Ok(out)
}

/// Append `valid_from` / `valid_to` / `is_current` to each batch.
/// `valid_from = now_micros` (microseconds since UTC epoch),
/// `valid_to = NULL`, `is_current = true`.
fn augment_with_scd2_cols(
    batches: &[arrow_array::RecordBatch],
    now_micros: i64,
) -> Result<Vec<arrow_array::RecordBatch>, BackendError> {
    use arrow_array::builder::TimestampMicrosecondBuilder;
    use arrow_array::{Array, BooleanArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema, TimeUnit};

    let mut out = Vec::with_capacity(batches.len());
    for b in batches {
        let n = b.num_rows();
        let mut vf = TimestampMicrosecondBuilder::with_capacity(n);
        let mut vt = TimestampMicrosecondBuilder::with_capacity(n);
        for _ in 0..n {
            vf.append_value(now_micros);
            vt.append_null();
        }
        let is_current = BooleanArray::from(vec![true; n]);

        let mut new_columns: Vec<Arc<dyn Array>> = b.columns().iter().map(Arc::clone).collect();
        new_columns.push(Arc::new(vf.finish()));
        new_columns.push(Arc::new(vt.finish()));
        new_columns.push(Arc::new(is_current));

        let mut new_fields: Vec<Arc<Field>> = b.schema().fields().iter().map(Arc::clone).collect();
        new_fields.push(Arc::new(Field::new(
            VALID_FROM_COL,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        )));
        new_fields.push(Arc::new(Field::new(
            VALID_TO_COL,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )));
        new_fields.push(Arc::new(Field::new(
            IS_CURRENT_COL,
            DataType::Boolean,
            false,
        )));
        let new_schema = Arc::new(ArrowSchema::new(new_fields));

        out.push(
            arrow_array::RecordBatch::try_new(new_schema, new_columns)
                .map_err(|e| BackendError::Other(format!("scd2 augment scd2 cols: {e}")))?,
        );
    }
    Ok(out)
}

/// Project just the merge-key columns from each batch, preserving
/// schema. Used to build the source for soft-delete merges where we
/// only care about whether each target key has a corresponding source
/// row.
fn project_columns(
    batches: &[arrow_array::RecordBatch],
    columns: &[String],
) -> Result<Vec<arrow_array::RecordBatch>, BackendError> {
    use arrow_schema::Schema as ArrowSchema;

    let mut out = Vec::with_capacity(batches.len());
    for b in batches {
        let indices: Vec<usize> = columns
            .iter()
            .map(|c| {
                b.schema()
                    .index_of(c)
                    .map_err(|_| BackendError::Other(format!("scd2 project: column '{c}' missing")))
            })
            .collect::<Result<_, _>>()?;
        let cols: Vec<_> = indices.iter().map(|i| Arc::clone(b.column(*i))).collect();
        let fields: Vec<_> = indices
            .iter()
            .map(|i| Arc::clone(&b.schema().fields()[*i]))
            .collect();
        let schema = Arc::new(ArrowSchema::new(fields));
        out.push(
            arrow_array::RecordBatch::try_new(schema, cols)
                .map_err(|e| BackendError::Other(format!("scd2 project: {e}")))?,
        );
    }
    Ok(out)
}

/// Convert an ISO-8601 millisecond string back into microseconds since
/// the UTC epoch — for the close-out / TTL update predicates that
/// embed `valid_from`/`valid_to` literals.
fn iso8601_now_micros() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

// (Phase 35b–e had a free-function version of `open_table` that didn't
// thread storage_options. 35f replaces it with a method on
// DeltaBackend so credentials travel with `&self` for cloud roots.)

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

    fn config(&self) -> crate::backend::BackendConfig {
        crate::backend::BackendConfig::Delta(crate::backend::DeltaConfig {
            location: self.location.clone(),
            partition_columns: self.partition_columns.clone(),
        })
    }

    async fn ping(&self) -> Result<(), BackendError> {
        // Local roots: the directory still exists. Cloud roots: list
        // the prefix — a connection / permission failure surfaces
        // here even when the prefix is empty.
        if self.root_url.scheme() == "file" {
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
            return Ok(());
        }
        let mut listing = self.store.list(None);
        if let Some(item) = futures_util::StreamExt::next(&mut listing).await {
            item.map_err(|e| BackendError::Connection(format!("delta ping list: {e}")))?;
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
        let table = self.open_table(url.clone()).await?;
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
        let table = self.open_table(url.clone()).await?;
        let save_mode = match mode {
            WriteMode::Append => SaveMode::Append,
            WriteMode::Truncate => SaveMode::Overwrite,
        };
        let mut writer = table.write(batches).with_save_mode(save_mode);
        if !self.partition_columns.is_empty() {
            // Phase 40.1: only meaningful on the first write to a
            // fresh location; deltalake-rs validates against an
            // existing table's layout and errors clearly on mismatch.
            writer = writer.with_partition_columns(self.partition_columns.clone());
        }
        writer
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
        spec: &TableSpec,
        source_query: &str,
        keys: &[String],
        update_columns: &[String],
        pipeline_name: &str,
        mode_label: &str,
        source_backend: Option<&dyn Backend>,
        delete_handling: Option<DeleteHandling>,
        dry_run: bool,
    ) -> Result<StrategyRunResult, BackendError> {
        use deltalake::datafusion::datasource::MemTable;
        use deltalake::datafusion::prelude::SessionContext;

        let source = source_backend.ok_or_else(|| {
            BackendError::Other(
                "Delta run_merge: source_backend is required \
                 (Delta is a target only)"
                    .into(),
            )
        })?;
        let run_id = uuid::Uuid::now_v7();
        let started_at = iso8601_now();

        let (inserted, updated, deleted, status): (i64, i64, i64, &'static str) = if dry_run {
            // Probe the source query so a missing query / bad
            // credentials surfaces; do not commit a merge.
            let _ = source.read_arrow_stream(source_query).await?;
            (0, 0, 0, "dry_run")
        } else {
            // Read source as a single batched DataFusion DataFrame.
            // The merge engine needs random access to the source for
            // its join, so streaming isn't a fit here — buffer and
            // wrap in a `MemTable` provider.
            let stream = source.read_arrow_stream(source_query).await?;
            let batches: Vec<RecordBatch> = stream.try_collect().await?;
            if batches.is_empty() {
                // No rows → no inserts, no updates, no source-driven
                // deletes. With handle_deletes::Hard a zero-row source
                // would normally tombstone the entire target; we error
                // there to avoid an accidental wipe.
                if matches!(delete_handling, Some(DeleteHandling::Hard)) {
                    return Err(BackendError::Other(
                        "Delta run_merge with handle_deletes=Hard refuses an empty \
                         source (would delete every target row); pass an explicit \
                         truncate if that is the intent"
                            .into(),
                    ));
                }
                (0, 0, 0, "success")
            } else {
                let url = self.table_url(&spec.schema, &spec.name)?;
                let table = self.open_table(url.clone()).await?;
                if table.version().is_none() {
                    return Err(BackendError::Other(format!(
                        "Delta run_merge: target table {url} is uninitialized; \
                         run an append first to create the schema"
                    )));
                }

                let arrow_schema = batches[0].schema();
                let memtable = MemTable::try_new(arrow_schema, vec![batches])
                    .map_err(|e| BackendError::Query(format!("delta merge memtable: {e}")))?;
                let ctx = SessionContext::new();
                let df = ctx
                    .read_table(Arc::new(memtable))
                    .map_err(|e| BackendError::Query(format!("delta merge read_table: {e}")))?;

                // Predicate as a string — deltalake parses
                // `target.k = source.k AND …` against the merged
                // schema. Same shape the SQL backends emit for their
                // merge planners.
                let predicate = keys
                    .iter()
                    .map(|k| format!("target.{k} = source.{k}"))
                    .collect::<Vec<_>>()
                    .join(" AND ");

                let mut merge = table
                    .merge(df, predicate.as_str())
                    .with_source_alias("source")
                    .with_target_alias("target");

                if !update_columns.is_empty() {
                    let cols = update_columns.to_vec();
                    // Filter the update to only rows where at least one
                    // update column actually changed. Otherwise Delta
                    // (and most MERGE engines) bumps every matched row
                    // unconditionally, which inflates rows_updated and
                    // creates a redundant rewrite. `IS DISTINCT FROM`
                    // is NULL-safe — `target.x = source.x` would skip
                    // a transition between value and NULL.
                    let update_predicate = cols
                        .iter()
                        .map(|c| format!("target.{c} IS DISTINCT FROM source.{c}"))
                        .collect::<Vec<_>>()
                        .join(" OR ");
                    merge = merge
                        .when_matched_update(|u| {
                            let mut u = u.predicate(update_predicate.as_str());
                            for col_name in &cols {
                                u = u.update(col_name.as_str(), format!("source.{col_name}"));
                            }
                            u
                        })
                        .map_err(|e| {
                            BackendError::Query(format!("delta merge when_matched_update: {e}"))
                        })?;
                }

                let user_cols: Vec<String> = spec.columns.iter().map(|c| c.name.clone()).collect();
                merge = merge
                    .when_not_matched_insert(|i| {
                        let mut i = i;
                        for col_name in &user_cols {
                            i = i.set(col_name.as_str(), format!("source.{col_name}"));
                        }
                        i
                    })
                    .map_err(|e| {
                        BackendError::Query(format!("delta merge when_not_matched_insert: {e}"))
                    })?;

                if matches!(delete_handling, Some(DeleteHandling::Hard)) {
                    merge = merge
                        .when_not_matched_by_source_delete(|d| d)
                        .map_err(|e| {
                            BackendError::Query(format!(
                                "delta merge when_not_matched_by_source_delete: {e}"
                            ))
                        })?;
                }

                let (_table, metrics) = merge
                    .await
                    .map_err(|e| BackendError::Query(format!("delta merge: {e}")))?;
                (
                    metrics.num_target_rows_inserted as i64,
                    metrics.num_target_rows_updated as i64,
                    metrics.num_target_rows_deleted as i64,
                    "success",
                )
            }
        };

        let finished_at = iso8601_now();
        let event = serde_json::json!({
            "run_id": run_id.to_string(),
            "pipeline_name": pipeline_name,
            "target_schema": spec.schema,
            "target_table": spec.name,
            "mode": mode_label,
            "path": "cross_backend",
            "started_at": started_at,
            "finished_at": finished_at,
            "status": status,
            "rows_inserted": inserted,
            "rows_updated": updated,
            "rows_deleted": deleted,
            "format": "delta",
        });
        record_run_event(&self.store, &run_id, &event).await?;

        // Plumb target-row deletes through `rows_unchanged` for the
        // sidecar log; the StrategyRunResult shape keeps `rows_closed`
        // for SCD2-flavored stats and we use `rows_unchanged` here as
        // the next-best slot (DBs report 0 here for merges with no
        // xmax-style split).
        Ok(StrategyRunResult {
            run_id: run_id.to_string(),
            rows_inserted: inserted,
            rows_updated: Some(updated),
            rows_unchanged: Some(0),
            rows_closed: if deleted > 0 { Some(deleted) } else { None },
            status: status.into(),
            path: "cross_backend".into(),
        })
    }

    async fn run_scd2(
        &self,
        spec: &TableSpec,
        source_query: &str,
        keys: &[String],
        compare_columns: &[String],
        pipeline_name: &str,
        source_backend: Option<&dyn Backend>,
        delete_handling: Option<DeleteHandling>,
        event_timestamp_column: Option<&str>,
        ttl_seconds: Option<i64>,
        dry_run: bool,
    ) -> Result<StrategyRunResult, BackendError> {
        use deltalake::datafusion::datasource::MemTable;
        use deltalake::datafusion::prelude::SessionContext;

        // Validation: only Soft delete is meaningful for SCD2 (Hard
        // is for merge — wholly removes target rows; SCD2's premise
        // is that history is preserved).
        if let Some(dh) = delete_handling
            && !matches!(dh, DeleteHandling::Soft)
        {
            return Err(BackendError::Other(format!(
                "Delta run_scd2: only DeleteHandling::Soft is supported (got {dh:?}); \
                 Hard is for merge"
            )));
        }
        // Event-time SCD2 lands in a follow-up; the framework's spec
        // calls for it but the diff/dedup logic is its own pass and
        // I want the simpler now()-flavored SCD2 in code first.
        if event_timestamp_column.is_some() {
            return Err(BackendError::Other(
                "Delta run_scd2: event_timestamp_column is not yet supported \
                 (basic SCD2 with valid_from = now() ships in 35e; event-time \
                 DISTINCT ON dedup is a follow-up)"
                    .into(),
            ));
        }

        let source = source_backend.ok_or_else(|| {
            BackendError::Other(
                "Delta run_scd2: source_backend is required (Delta is target only)".into(),
            )
        })?;

        let run_id = uuid::Uuid::now_v7();
        let started_at = iso8601_now();
        let now_micros = iso8601_now_micros();

        if dry_run {
            let _ = source.read_arrow_stream(source_query).await?;
            let finished_at = iso8601_now();
            let event = serde_json::json!({
                "run_id": run_id.to_string(),
                "pipeline_name": pipeline_name,
                "target_schema": spec.schema,
                "target_table": spec.name,
                "mode": "scd2",
                "path": "cross_backend",
                "started_at": started_at,
                "finished_at": finished_at,
                "status": "dry_run",
                "rows_inserted": 0,
                "rows_closed": 0,
                "format": "delta",
            });
            record_run_event(&self.store, &run_id, &event).await?;
            return Ok(StrategyRunResult {
                run_id: run_id.to_string(),
                rows_inserted: 0,
                rows_updated: None,
                rows_unchanged: None,
                rows_closed: Some(0),
                status: "dry_run".into(),
                path: "cross_backend".into(),
            });
        }

        // Read source as Arrow batches.
        let stream = source.read_arrow_stream(source_query).await?;
        let source_batches: Vec<RecordBatch> = stream.try_collect().await?;

        if source_batches.is_empty() {
            // Empty source + Soft-delete = wipe. Same guard as merge:
            // refuse to tombstone every current row from a missing
            // source query. Pass-through otherwise (no-op).
            if matches!(delete_handling, Some(DeleteHandling::Soft)) {
                return Err(BackendError::Other(
                    "Delta run_scd2 with handle_deletes=Soft refuses an empty \
                     source (would close every current row); pass an explicit \
                     truncate if that is the intent"
                        .into(),
                ));
            }
            let finished_at = iso8601_now();
            let event = serde_json::json!({
                "run_id": run_id.to_string(),
                "pipeline_name": pipeline_name,
                "target_schema": spec.schema,
                "target_table": spec.name,
                "mode": "scd2",
                "path": "cross_backend",
                "started_at": started_at,
                "finished_at": finished_at,
                "status": "success",
                "rows_inserted": 0,
                "rows_closed": 0,
                "format": "delta",
            });
            record_run_event(&self.store, &run_id, &event).await?;
            return Ok(StrategyRunResult {
                run_id: run_id.to_string(),
                rows_inserted: 0,
                rows_updated: None,
                rows_unchanged: None,
                rows_closed: Some(0),
                status: "success".into(),
                path: "cross_backend".into(),
            });
        }

        // Compute row_hash on every source row over the user's
        // compare_columns. Same protocol the DB backends use.
        let source_with_hash = augment_with_row_hash(&source_batches, compare_columns)?;

        let url = self.table_url(&spec.schema, &spec.name)?;
        let table = self.open_table(url.clone()).await?;

        let inserted_count: u64;
        let mut closed_count: u64 = 0;

        if table.version().is_none() {
            // First load: every source row becomes a new current
            // version. Schema for the target = source schema +
            // (valid_from, valid_to, is_current). row_hash already
            // appended.
            let with_scd2 = augment_with_scd2_cols(&source_with_hash, now_micros)?;
            let total: u64 = with_scd2.iter().map(|b| b.num_rows() as u64).sum();
            let target = self.open_table(url.clone()).await?;
            target
                .write(with_scd2)
                .with_save_mode(SaveMode::Append)
                .await
                .map_err(|e| BackendError::Query(format!("delta scd2 first-write: {e}")))?;
            inserted_count = total;
        } else {
            // Subsequent load. Compute "changed" = source rows whose
            // current target row has a different row_hash, OR source
            // rows whose key isn't in target.
            //
            // We use DataFusion in-memory: register source-with-hash
            // as `source`, register target-current as `target`, run
            // a LEFT JOIN, filter where target.row_hash IS NULL OR
            // target.row_hash != source.row_hash.
            // Inline target scan: open the target's table at this
            // URL, run scan_table to get an Arrow stream, filter to
            // is_current=true rows in DataFusion, and collect.
            let scan_url = url.clone();
            let scan_table = self.open_table(scan_url).await?;
            let target_current_batches: Vec<RecordBatch> = if scan_table.version().is_none() {
                vec![]
            } else {
                let (_t, df_stream) = scan_table
                    .scan_table()
                    .await
                    .map_err(|e| BackendError::Query(format!("scd2 target scan_table: {e}")))?;
                let all: Vec<RecordBatch> = df_stream
                    .map(|r| r.map_err(|e| BackendError::Query(format!("scd2 target scan: {e}"))))
                    .try_collect()
                    .await?;
                // Filter by is_current=true via DataFusion.
                if all.is_empty() {
                    vec![]
                } else {
                    let schema_for_filter = all[0].schema();
                    let provider = Arc::new(
                        MemTable::try_new(schema_for_filter, vec![all]).map_err(|e| {
                            BackendError::Query(format!("scd2 target memtable: {e}"))
                        })?,
                    );
                    let ctx = SessionContext::new();
                    ctx.register_table("delta_target_all", provider)
                        .map_err(|e| BackendError::Query(format!("scd2 target register: {e}")))?;
                    let df = ctx
                        .sql(&format!(
                            "SELECT * FROM delta_target_all WHERE {IS_CURRENT_COL} = true"
                        ))
                        .await
                        .map_err(|e| BackendError::Query(format!("scd2 target filter sql: {e}")))?;
                    df.collect().await.map_err(|e| {
                        BackendError::Query(format!("scd2 target filter collect: {e}"))
                    })?
                }
            };

            let ctx = SessionContext::new();
            let src_schema = source_with_hash[0].schema();
            let src_provider = Arc::new(
                MemTable::try_new(src_schema.clone(), vec![source_with_hash.clone()])
                    .map_err(|e| BackendError::Query(format!("scd2 source memtable: {e}")))?,
            );
            ctx.register_table("source", src_provider)
                .map_err(|e| BackendError::Query(format!("scd2 register source: {e}")))?;

            let target_schema = if !target_current_batches.is_empty() {
                target_current_batches[0].schema()
            } else {
                // No current rows in target — use source-with-hash
                // schema as a proxy so register_table doesn't fail.
                src_schema.clone()
            };
            let tgt_provider = Arc::new(
                MemTable::try_new(target_schema, vec![target_current_batches.clone()])
                    .map_err(|e| BackendError::Query(format!("scd2 target memtable: {e}")))?,
            );
            ctx.register_table("target_current", tgt_provider)
                .map_err(|e| BackendError::Query(format!("scd2 register target: {e}")))?;

            let join_clause = keys
                .iter()
                .map(|k| format!("s.{k} = t.{k}"))
                .collect::<Vec<_>>()
                .join(" AND ");
            // We project only `s.*` so the changed-source rows have
            // the source schema (user cols + row_hash). The hash
            // comparison handles NULL via the IS DISTINCT FROM-OR
            // pattern.
            let diff_sql = format!(
                "SELECT s.* FROM source s LEFT JOIN target_current t ON {join_clause} \
                 WHERE t.{ROW_HASH_COL} IS NULL OR t.{ROW_HASH_COL} != s.{ROW_HASH_COL}"
            );
            let diff_df = ctx
                .sql(&diff_sql)
                .await
                .map_err(|e| BackendError::Query(format!("scd2 diff sql: {e}")))?;
            let changed_batches = diff_df
                .collect()
                .await
                .map_err(|e| BackendError::Query(format!("scd2 diff collect: {e}")))?;
            let changed_count: u64 = changed_batches.iter().map(|b| b.num_rows() as u64).sum();

            if changed_count > 0 {
                // Pass 1: close-out via merge. Source = changed
                // (only need the keys), predicate joins keys + filters
                // is_current=true on target. Update sets valid_to
                // and is_current=false.
                let changed_keys_only = project_columns(&changed_batches, keys)?;
                let close_schema = changed_keys_only[0].schema();
                let close_provider = Arc::new(
                    MemTable::try_new(close_schema, vec![changed_keys_only])
                        .map_err(|e| BackendError::Query(format!("scd2 close memtable: {e}")))?,
                );
                let close_ctx = SessionContext::new();
                let close_df = close_ctx
                    .read_table(close_provider)
                    .map_err(|e| BackendError::Query(format!("scd2 close read_table: {e}")))?;
                let predicate = keys
                    .iter()
                    .map(|k| format!("target.{k} = source.{k}"))
                    .collect::<Vec<_>>()
                    .join(" AND ");
                let predicate = format!("{predicate} AND target.{IS_CURRENT_COL} = true");

                // Embed the now-microseconds timestamp as a literal.
                // Delta's update parser accepts
                // `arrow_cast(literal, 'Timestamp(Microsecond, None)')`,
                // but the simpler path is the DataFusion SQL form
                // `to_timestamp_micros(<int>)`.
                let now_lit = format!("to_timestamp_micros({now_micros})");
                let target_for_close = self.open_table(url.clone()).await?;
                let (_t, m) = target_for_close
                    .merge(close_df, predicate.as_str())
                    .with_source_alias("source")
                    .with_target_alias("target")
                    .when_matched_update(|u| {
                        u.update(VALID_TO_COL, now_lit.as_str())
                            .update(IS_CURRENT_COL, "false")
                    })
                    .map_err(|e| BackendError::Query(format!("scd2 close when_matched: {e}")))?
                    .await
                    .map_err(|e| BackendError::Query(format!("scd2 close merge: {e}")))?;
                closed_count += m.num_target_rows_updated as u64;

                // Pass 2: append new versions for the changed rows.
                let with_scd2 = augment_with_scd2_cols(&changed_batches, now_micros)?;
                let target_for_insert = self.open_table(url.clone()).await?;
                target_for_insert
                    .write(with_scd2)
                    .with_save_mode(SaveMode::Append)
                    .await
                    .map_err(|e| BackendError::Query(format!("scd2 insert: {e}")))?;
            }
            inserted_count = changed_count;
        }

        // Soft-delete: close out current rows whose key isn't in source.
        if matches!(delete_handling, Some(DeleteHandling::Soft)) {
            let source_keys_only = project_columns(&source_batches, keys)?;
            let key_schema = source_keys_only[0].schema();
            let provider = Arc::new(
                MemTable::try_new(key_schema, vec![source_keys_only])
                    .map_err(|e| BackendError::Query(format!("scd2 soft-del memtable: {e}")))?,
            );
            let ctx = SessionContext::new();
            let df = ctx
                .read_table(provider)
                .map_err(|e| BackendError::Query(format!("scd2 soft-del read_table: {e}")))?;
            let predicate = keys
                .iter()
                .map(|k| format!("target.{k} = source.{k}"))
                .collect::<Vec<_>>()
                .join(" AND ");
            let now_lit = format!("to_timestamp_micros({now_micros})");
            let target_for_soft = self.open_table(url.clone()).await?;
            let (_t, m) = target_for_soft
                .merge(df, predicate.as_str())
                .with_source_alias("source")
                .with_target_alias("target")
                .when_not_matched_by_source_update(|u| {
                    u.predicate(format!("target.{IS_CURRENT_COL} = true"))
                        .update(VALID_TO_COL, now_lit.as_str())
                        .update(IS_CURRENT_COL, "false")
                })
                .map_err(|e| BackendError::Query(format!("scd2 soft-del when_not_matched: {e}")))?
                .await
                .map_err(|e| BackendError::Query(format!("scd2 soft-del merge: {e}")))?;
            closed_count += m.num_target_rows_updated as u64;
        }

        // TTL: close out current rows whose valid_from is older than now - ttl.
        if let Some(ttl) = ttl_seconds {
            let threshold_micros = now_micros - ttl * 1_000_000;
            let predicate = format!(
                "{IS_CURRENT_COL} = true AND {VALID_FROM_COL} < to_timestamp_micros({threshold_micros})"
            );
            let now_lit = format!("to_timestamp_micros({now_micros})");
            let target_for_ttl = self.open_table(url.clone()).await?;
            let (_t, m) = target_for_ttl
                .update()
                .with_predicate(predicate.as_str())
                .with_update(VALID_TO_COL, now_lit.as_str())
                .with_update(IS_CURRENT_COL, "false")
                .await
                .map_err(|e| BackendError::Query(format!("scd2 ttl: {e}")))?;
            closed_count += m.num_updated_rows as u64;
        }

        let finished_at = iso8601_now();
        let event = serde_json::json!({
            "run_id": run_id.to_string(),
            "pipeline_name": pipeline_name,
            "target_schema": spec.schema,
            "target_table": spec.name,
            "mode": "scd2",
            "path": "cross_backend",
            "started_at": started_at,
            "finished_at": finished_at,
            "status": "success",
            "rows_inserted": inserted_count,
            "rows_closed": closed_count,
            "format": "delta",
        });
        record_run_event(&self.store, &run_id, &event).await?;

        Ok(StrategyRunResult {
            run_id: run_id.to_string(),
            rows_inserted: inserted_count as i64,
            rows_updated: None,
            rows_unchanged: None,
            rows_closed: Some(closed_count as i64),
            status: "success".into(),
            path: "cross_backend".into(),
        })
    }

    /// Δ.X1: hand the streaming runtime a `TableSpec` reflected
    /// from the Delta table's Arrow schema. **PK info is not
    /// surfaced from Delta in PR 1** — Delta tables don't carry
    /// PK constraints natively, and the kernel crate gates its
    /// `Metadata.configuration` accessor behind an `internal_api`
    /// macro that's not in the public surface. So every reflected
    /// column reports `primary_key = false`; the streaming
    /// dispatch path errors on first CDC apply with the clear
    /// "no primary-key column" message that points the user at
    /// the @ematix.table decorator. **Direct callers** of
    /// `Backend::run_cdc` (which hand-build the spec with PK
    /// info) work fine — that's the path Δ.X1 PR 1 ships and
    /// tests.
    ///
    /// Δ.X1.1 follow-up: thread the user's `@ematix.table(
    /// primary_key=...)` declaration through the streaming
    /// runtime as a side channel, OR adopt the JSON-roundtrip
    /// route through `serde_json::to_value(&metadata)` to read
    /// the configuration map from outside the kernel crate.
    async fn reflect_table_spec(&self, target: &TargetTable) -> Result<TableSpec, BackendError> {
        use deltalake::kernel::engine::arrow_conversion::TryIntoArrow as _;

        let url = self.table_url(&target.schema, &target.name)?;
        let table = self.open_table(url.clone()).await?;
        if table.version().is_none() {
            return Err(BackendError::Other(format!(
                "reflect_table_spec: Delta table {url} is uninitialized; \
                 write at least one batch before pointing a CDC pipeline at it"
            )));
        }
        let snapshot = table
            .snapshot()
            .map_err(|e| BackendError::Other(format!("Delta reflect_table_spec snapshot: {e}")))?;
        let kernel_schema = snapshot.schema();
        // KernelSchemaRef is Arc<StructType>; the kernel crate
        // exposes a TryIntoArrow blanket impl that goes from
        // &StructType to ArrowSchema via TryFromKernel.
        let arrow_schema: arrow_schema::Schema =
            kernel_schema
                .as_ref()
                .try_into_arrow()
                .map_err(|e: arrow_schema::ArrowError| {
                    BackendError::Other(format!("Delta reflect_table_spec arrow conv: {e}"))
                })?;

        let columns: Vec<crate::types::ColumnSpec> = arrow_schema
            .fields()
            .iter()
            .map(|f| crate::types::ColumnSpec {
                name: f.name().clone(),
                ty: arrow_to_column_type(f.data_type()),
                nullable: f.is_nullable(),
                primary_key: false,
            })
            .collect();
        Ok(TableSpec {
            schema: target.schema.clone(),
            name: target.name.clone(),
            columns,
            unique_constraints: Vec::new(),
            fingerprint: String::new(),
        })
    }

    /// Δ.X1: apply CDC events to a Delta target via a single
    /// `MERGE INTO` per batch. Conceptually cleaner than the
    /// Postgres per-event-statement loop — Delta's MERGE absorbs
    /// the c/u/d dispatch + transactional commit automatically,
    /// and ACID is enforced by Delta's transaction log.
    ///
    /// **Within-batch ordering**: events are deduplicated by
    /// primary key, keeping the row with the highest `ts_ms`.
    /// Multiple updates to the same PK in one batch collapse to
    /// the newest post-image. A delete with the highest ts_ms for
    /// a PK wins over earlier UPDATEs.
    ///
    /// **Between-batch idempotency**: relies on MERGE's natural
    /// idempotency — UPDATE / INSERT with equal post-image is a
    /// no-op, DELETE-of-absent-row is a no-op. The "older event in
    /// a later batch clobbers newer state" case requires an
    /// explicit per-row `_cdc_last_ts` gate, deferred to Δ.X1.1.
    /// Non-issue when the source's per-PK ordering is preserved
    /// (Kafka with Debezium's default partition-by-PK).
    ///
    /// **Schema evolution**: `Skip` policy uses Delta's
    /// `with_merge_schema(true)` — new columns in the source
    /// auto-evolve the target. Better than Postgres's "warn +
    /// drop" because Delta picks them up automatically. `Fail`
    /// policy pre-flights the source columns against the
    /// reflected target schema and aborts before MERGE if any
    /// column is missing.
    ///
    /// **Soft-delete**: replaces the `WHEN MATCHED AND op = 'd'
    /// THEN DELETE` clause with `WHEN MATCHED AND op = 'd' THEN
    /// UPDATE SET <soft_col> = current_timestamp()`.
    async fn run_cdc(
        &self,
        spec: &TableSpec,
        batch: RecordBatch,
        cdc_config: &crate::cdc::CdcConfig,
        pipeline_name: &str,
    ) -> Result<crate::backend::CdcRunResult, BackendError> {
        use crate::cdc::{DeleteMode, ParsedRow, SchemaEvolutionPolicy};
        use deltalake::datafusion::datasource::MemTable;
        use deltalake::datafusion::prelude::SessionContext;

        // Phase 1 — parse + filter. Same surface as the
        // PostgresBackend wrapper: tombstones + parse failures
        // count as `skipped` and don't bubble up.
        let parsed = crate::cdc::events_from_batch(&batch, cdc_config)?;
        let mut events: Vec<crate::cdc::CdcEvent> = Vec::with_capacity(parsed.len());
        let mut skipped: i64 = 0;
        for row in parsed {
            match row {
                ParsedRow::Event(e) => events.push(e),
                ParsedRow::Tombstone => skipped += 1,
                ParsedRow::ParseError(e) => {
                    tracing::warn!(
                        target: "ematix_flow::cdc",
                        error = %e,
                        pipeline = pipeline_name,
                        "Delta CDC envelope parse failed; row skipped",
                    );
                    skipped += 1;
                }
            }
        }

        // Identify PK column(s) from the spec. Composite PKs ride
        // through naturally — the merge predicate is the AND of
        // every PK column equality.
        let pk_cols: Vec<String> = spec
            .columns
            .iter()
            .filter(|c| c.primary_key)
            .map(|c| c.name.clone())
            .collect();
        if pk_cols.is_empty() {
            return Err(BackendError::Other(format!(
                "Delta run_cdc: target {}.{} has no primary-key column. \
                 Either declare PK on the @ematix.table decorator / \
                 [target.table] block, or set the Delta table property \
                 `ematix_flow.cdc.primary_key = \"<col,...>\"`",
                spec.schema, spec.name
            )));
        }

        // Schema-evolution Fail policy: pre-flight check against
        // the spec's columns. Skip relies on Delta's
        // with_merge_schema(true) below to auto-evolve.
        if matches!(cdc_config.schema_evolution, SchemaEvolutionPolicy::Fail) {
            let valid_cols: std::collections::HashSet<&str> =
                spec.columns.iter().map(|c| c.name.as_str()).collect();
            for event in &events {
                if let Some(after) = event.after.as_ref() {
                    for k in after.keys() {
                        if !valid_cols.contains(k.as_str()) {
                            return Err(BackendError::Other(format!(
                                "schema evolution: unknown column '{k}' in `after` \
                                 payload for pipeline '{pipeline_name}' on Delta target \
                                 {schema}.{name} (SchemaEvolutionPolicy::Fail) — \
                                 add the column to the target table or switch to Skip",
                                schema = spec.schema,
                                name = spec.name,
                            )));
                        }
                    }
                }
            }
        }

        // Within-batch dedupe: highest ts_ms per PK wins.
        // Insertion order is preserved when ts_ms is None (no
        // ordering signal available; first observation wins).
        let mut deduped: std::collections::HashMap<String, crate::cdc::CdcEvent> =
            std::collections::HashMap::new();
        for event in events {
            let pk_canon = canonical_pk_string(&event, &pk_cols);
            match deduped.get(&pk_canon) {
                None => {
                    deduped.insert(pk_canon, event);
                }
                Some(existing) => {
                    let take_new = match (existing.ts_ms, event.ts_ms) {
                        (Some(a), Some(b)) => b >= a,
                        (None, Some(_)) => true,
                        (_, None) => false,
                    };
                    if take_new {
                        deduped.insert(pk_canon, event);
                    }
                }
            }
        }
        let events: Vec<crate::cdc::CdcEvent> = deduped.into_values().collect();
        let run_id = uuid::Uuid::now_v7();

        if events.is_empty() {
            return Ok(crate::backend::CdcRunResult {
                run_id: run_id.to_string(),
                creates: 0,
                updates: 0,
                deletes: 0,
                skipped,
                idempotent_skipped: 0,
            });
        }

        // Phase 2 — build the source RecordBatch. Schema:
        // every spec column (forced-nullable so delete events with
        // only the PK populated still validate) + a synthesized
        // `__op` Utf8 column that the MERGE branches dispatch on.
        let arrow_schema = build_cdc_source_schema(spec)?;
        let source_batch = build_cdc_source_batch(arrow_schema.clone(), &events, &pk_cols)?;

        // Open the target. Like run_merge, we require the table
        // to exist — initial schema bootstrap is the user's job
        // (one append, then point the CDC pipeline at it).
        let url = self.table_url(&spec.schema, &spec.name)?;
        let table = self.open_table(url.clone()).await?;
        if table.version().is_none() {
            return Err(BackendError::Other(format!(
                "Delta run_cdc: target table {url} is uninitialized; \
                 run an append first to create the schema"
            )));
        }

        // Wrap the source batch in a DataFusion DataFrame —
        // deltalake's MergeBuilder consumes a DataFrame.
        let memtable = MemTable::try_new(arrow_schema.clone(), vec![vec![source_batch.clone()]])
            .map_err(|e| BackendError::Query(format!("delta cdc memtable: {e}")))?;
        let ctx = SessionContext::new();
        let df = ctx
            .read_table(Arc::new(memtable))
            .map_err(|e| BackendError::Query(format!("delta cdc read_table: {e}")))?;

        // PK predicate: target.<pk1> = source.<pk1> AND ...
        let predicate = pk_cols
            .iter()
            .map(|k| format!("target.{k} = source.{k}"))
            .collect::<Vec<_>>()
            .join(" AND ");

        let user_cols: Vec<String> = spec.columns.iter().map(|c| c.name.clone()).collect();

        // Skip policy → mergeSchema=true (Delta auto-evolves).
        // Fail already validated upstream. Both ride the same
        // builder shape below.
        let mut merge = table
            .merge(df, predicate.as_str())
            .with_source_alias("source")
            .with_target_alias("target")
            .with_merge_schema(matches!(
                cdc_config.schema_evolution,
                SchemaEvolutionPolicy::Skip
            ));

        // WHEN MATCHED AND __op = 'd' branch. Hard mode deletes
        // the row; soft mode updates the configured column to
        // current_timestamp() instead. Register first so the
        // c/u/r update branch below doesn't shadow it.
        match &cdc_config.delete_mode {
            DeleteMode::Hard => {
                merge = merge
                    .when_matched_delete(|d| d.predicate("source.__op = 'd'"))
                    .map_err(|e| {
                        BackendError::Query(format!("delta cdc when_matched_delete: {e}"))
                    })?;
            }
            DeleteMode::Soft { column } => {
                let soft_col = column.clone();
                merge = merge
                    .when_matched_update(|u| {
                        u.predicate("source.__op = 'd'")
                            .update(soft_col.as_str(), "current_timestamp()")
                    })
                    .map_err(|e| {
                        BackendError::Query(format!(
                            "delta cdc when_matched_update soft-delete: {e}"
                        ))
                    })?;
            }
        }

        // WHEN MATCHED (general fallthrough) → c/u/r overwrite
        // every user column from the source post-image.
        let user_cols_for_update = user_cols.clone();
        merge = merge
            .when_matched_update(|u| {
                let mut u = u.predicate("source.__op != 'd'");
                for col in &user_cols_for_update {
                    u = u.update(col.as_str(), format!("source.{col}"));
                }
                u
            })
            .map_err(|e| BackendError::Query(format!("delta cdc when_matched_update: {e}")))?;

        // WHEN NOT MATCHED AND __op != 'd' → INSERT the post-
        // image. A delete event for an absent row falls through
        // the bottom of the MERGE — natural no-op.
        let user_cols_for_insert = user_cols.clone();
        merge = merge
            .when_not_matched_insert(|i| {
                let mut i = i.predicate("source.__op != 'd'");
                for col in &user_cols_for_insert {
                    i = i.set(col.as_str(), format!("source.{col}"));
                }
                i
            })
            .map_err(|e| BackendError::Query(format!("delta cdc when_not_matched_insert: {e}")))?;

        let (_table, metrics) = merge
            .await
            .map_err(|e| BackendError::Query(format!("delta cdc merge: {e}")))?;

        // Map Delta's MergeMetrics back onto CdcRunResult. Delta
        // doesn't expose per-op CDC counts directly, so creates +
        // updates + deletes are derived from the per-MERGE-clause
        // counters. `idempotent_skipped` is always 0 in PR 1 —
        // Δ.X1.1 will populate it once the per-row ts gate lands.
        Ok(crate::backend::CdcRunResult {
            run_id: run_id.to_string(),
            creates: metrics.num_target_rows_inserted as i64,
            updates: metrics.num_target_rows_updated as i64,
            deletes: metrics.num_target_rows_deleted as i64,
            skipped,
            idempotent_skipped: 0,
        })
    }
}

/// Δ.X1: minimal Arrow `DataType` mapping for the `ColumnType`s
/// that surface on user-declared mirror tables. Coverage is
/// deliberately scoped to types that Debezium / Maxwell payloads
/// usefully populate today; richer types (NUMERIC scale/precision,
/// dialect-specific bytes formats) are follow-ups when a user
/// hits them.
fn arrow_to_column_type(dt: &arrow_schema::DataType) -> crate::types::ColumnType {
    use crate::types::ColumnType as CT;
    use arrow_schema::DataType as DT;
    match dt {
        DT::Int8 | DT::Int16 => CT::SmallInt,
        DT::Int32 => CT::Integer,
        DT::Int64 => CT::BigInt,
        DT::Float32 => CT::Float,
        DT::Float64 => CT::Double,
        DT::Boolean => CT::Boolean,
        DT::Utf8 | DT::LargeUtf8 => CT::Text,
        DT::Binary | DT::LargeBinary => CT::Bytes,
        DT::Date32 | DT::Date64 => CT::Date,
        DT::Timestamp(_, None) => CT::Timestamp,
        DT::Timestamp(_, Some(_)) => CT::TimestampTz,
        // Fallback for unmapped types (decimals, structs, lists):
        // surface as Text so reflection still produces a useful
        // spec; dialect-specific dispatch can refine later.
        _ => CT::Text,
    }
}

/// Δ.X1: build the Arrow schema for the source RecordBatch the
/// MERGE consumes — every spec column made nullable plus a
/// non-null `__op` Utf8 column.
fn build_cdc_source_schema(spec: &TableSpec) -> Result<Arc<arrow_schema::Schema>, BackendError> {
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    let mut fields: Vec<Field> = Vec::with_capacity(spec.columns.len() + 1);
    for col in &spec.columns {
        // Force nullable: delete events have everything-null
        // except the PK, and a non-null PK column would still
        // pass since the parser fills it. NULL on user columns
        // never hits the target — when_matched_delete and the
        // skipped insert branch absorb them.
        let dt = column_type_to_arrow(&col.ty)?;
        fields.push(Field::new(col.name.as_str(), dt, true));
    }
    fields.push(Field::new("__op", DataType::Utf8, false));
    Ok(Arc::new(ArrowSchema::new(fields)))
}

/// Δ.X1: inverse of [`arrow_to_column_type`] for the column types
/// MERGE source batches need typed Arrow arrays for. Errors
/// loudly on unmapped types so the user gets a clear message
/// rather than a silent fall-back to Utf8.
fn column_type_to_arrow(
    ct: &crate::types::ColumnType,
) -> Result<arrow_schema::DataType, BackendError> {
    use crate::types::ColumnType as CT;
    use arrow_schema::{DataType, TimeUnit};
    Ok(match ct {
        CT::SmallInt => DataType::Int16,
        CT::Integer => DataType::Int32,
        CT::BigInt => DataType::Int64,
        CT::Float => DataType::Float32,
        CT::Double => DataType::Float64,
        CT::Boolean => DataType::Boolean,
        CT::Text | CT::String { .. } | CT::Json | CT::Jsonb | CT::Uuid => DataType::Utf8,
        CT::Bytes => DataType::Binary,
        CT::Date => DataType::Date32,
        CT::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
        CT::TimestampTz => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        CT::Numeric { .. } => {
            return Err(BackendError::Other(
                "Delta run_cdc: ColumnType::Numeric is not yet supported on the \
                 CDC source-batch path (Δ.X1 v1). Workaround: model the column \
                 as Double or Text in the mirror table; richer NUMERIC support \
                 lands in a follow-up."
                    .into(),
            ));
        }
    })
}

/// Δ.X1: build a single source RecordBatch from the deduped
/// event vec. JSON-routes through Arrow's NDJSON reader with the
/// explicit schema so types coerce identically to the
/// JSON-decoded source path.
fn build_cdc_source_batch(
    schema: Arc<arrow_schema::Schema>,
    events: &[crate::cdc::CdcEvent],
    pk_cols: &[String],
) -> Result<RecordBatch, BackendError> {
    use crate::cdc::CdcOp;

    // Each event becomes one JSON object: the `after` payload
    // (for c/u/r) or a synthesized PK-only object (for d), plus
    // a `__op` field. Concatenated as NDJSON.
    let mut buf: Vec<u8> = Vec::with_capacity(events.len() * 128);
    for event in events {
        let mut row: serde_json::Map<String, serde_json::Value> = match event.op {
            CdcOp::Delete => {
                // Delete: synthesize from event.key. Single-PK
                // case wraps the scalar; composite-PK case
                // expects event.key already to be an Object
                // (parser guarantee for composite keys).
                let mut m = serde_json::Map::new();
                if pk_cols.len() == 1 {
                    m.insert(pk_cols[0].clone(), event.key.clone());
                } else if let serde_json::Value::Object(obj) = &event.key {
                    for c in pk_cols {
                        if let Some(v) = obj.get(c) {
                            m.insert(c.clone(), v.clone());
                        }
                    }
                }
                m
            }
            _ => match event.after.as_ref() {
                Some(map) => map.clone(),
                None => continue,
            },
        };
        let op_str = match event.op {
            CdcOp::Create => "c",
            CdcOp::Update => "u",
            CdcOp::Delete => "d",
            CdcOp::Read => "r",
        };
        row.insert("__op".into(), serde_json::Value::String(op_str.to_string()));
        buf.extend(serde_json::to_string(&row).unwrap().bytes());
        buf.push(b'\n');
    }

    if buf.is_empty() {
        // No events to apply — return an empty batch with the
        // configured schema. The MERGE call below will be a no-op.
        return RecordBatch::try_new(schema.clone(), schema_empty_columns(&schema))
            .map_err(|e| BackendError::Query(format!("delta cdc empty batch: {e}")));
    }

    let cursor = std::io::Cursor::new(buf);
    let mut reader = arrow_json::ReaderBuilder::new(schema.clone())
        .build(cursor)
        .map_err(|e| BackendError::Query(format!("delta cdc json reader: {e}")))?;
    reader
        .next()
        .ok_or_else(|| BackendError::Query("delta cdc json reader empty".into()))?
        .map_err(|e| BackendError::Query(format!("delta cdc json decode: {e}")))
}

/// Δ.X1: build a Vec of zero-row arrays for an empty-batch
/// fallback. Inlined to avoid pulling in a heavier
/// `arrow_array::create_empty_array` import path that varies
/// across arrow_array versions.
fn schema_empty_columns(schema: &arrow_schema::Schema) -> Vec<Arc<dyn arrow_array::Array>> {
    use arrow_schema::DataType as DT;
    schema
        .fields()
        .iter()
        .map(|f| -> Arc<dyn arrow_array::Array> {
            match f.data_type() {
                DT::Int16 => Arc::new(arrow_array::Int16Array::new_null(0)),
                DT::Int32 => Arc::new(arrow_array::Int32Array::new_null(0)),
                DT::Int64 => Arc::new(arrow_array::Int64Array::new_null(0)),
                DT::Float32 => Arc::new(arrow_array::Float32Array::new_null(0)),
                DT::Float64 => Arc::new(arrow_array::Float64Array::new_null(0)),
                DT::Boolean => Arc::new(arrow_array::BooleanArray::new_null(0)),
                DT::Utf8 => Arc::new(arrow_array::StringArray::new_null(0)),
                _ => Arc::new(arrow_array::StringArray::new_null(0)),
            }
        })
        .collect()
}

/// Δ.X1: canonical-string PK encoding for the within-batch
/// dedupe map. Composite PKs serialize to a JSON-array; single
/// PKs to whatever the JSON Value's stringification yields.
fn canonical_pk_string(event: &crate::cdc::CdcEvent, pk_cols: &[String]) -> String {
    if pk_cols.len() == 1 {
        return event.key.to_string();
    }
    if let serde_json::Value::Object(obj) = &event.key {
        let parts: Vec<String> = pk_cols
            .iter()
            .map(|c| {
                obj.get(c)
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "null".to_string())
            })
            .collect();
        return parts.join("\u{1f}");
    }
    event.key.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use std::sync::Arc;

    /// Phase 40.1: with_partition_columns records the column list.
    #[test]
    fn with_partition_columns_records_value() {
        let dir = tempfile::tempdir().unwrap();
        let b = DeltaBackend::open_local(dir.path())
            .unwrap()
            .with_partition_columns(["year", "month"]);
        assert_eq!(
            b.partition_columns(),
            &["year".to_string(), "month".to_string()]
        );
    }

    /// Phase 40.1: by default no partition columns (unpartitioned).
    #[test]
    fn partition_columns_default_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let b = DeltaBackend::open_local(dir.path()).unwrap();
        assert!(b.partition_columns().is_empty());
    }

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

    // --- Phase 35d: run_merge (DuckDB → Delta) -----------------------------

    use crate::meta::DeleteHandling;

    /// Seed `s.events` in DuckDB and seed Delta `raw/events` with one
    /// commit of the same data. Returns (source, target).
    async fn duckdb_and_delta_with_seed(dir: &std::path::Path) -> (Arc<dyn Backend>, DeltaBackend) {
        let source = duckdb_with_events().await;
        let target = DeltaBackend::open_local(dir).unwrap();
        let spec = small_table_spec();
        target
            .run_append(
                &spec,
                "SELECT id, name FROM s.events ORDER BY id",
                "seed",
                Some(source.as_ref()),
                None,
                None,
                false,
            )
            .await
            .unwrap();
        (source, target)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn delta_run_merge_inserts_and_updates() {
        let dir = tempfile::tempdir().unwrap();
        let (source, target) = duckdb_and_delta_with_seed(dir.path()).await;
        // Mutate the source: id=2 changes name, add id=4 as a new row.
        source
            .execute("UPDATE s.events SET name = 'b-updated' WHERE id = 2")
            .await
            .unwrap();
        source
            .execute("INSERT INTO s.events VALUES (4, 'd')")
            .await
            .unwrap();
        let spec = small_table_spec();
        let result = target
            .run_merge(
                &spec,
                "SELECT id, name FROM s.events ORDER BY id",
                &["id".into()],
                &["name".into()],
                "p35d_merge",
                "merge",
                Some(source.as_ref()),
                None,
                false,
            )
            .await
            .unwrap();
        assert_eq!(result.rows_inserted, 1, "id=4 inserted");
        assert_eq!(result.rows_updated, Some(1), "id=2 updated");
        assert_eq!(result.status, "success");

        let stream = target.read_arrow_stream("raw/events").await.unwrap();
        let batches: Vec<RecordBatch> = futures_util::TryStreamExt::try_collect(stream)
            .await
            .unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 4);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn delta_run_merge_handle_deletes_hard() {
        let dir = tempfile::tempdir().unwrap();
        let (source, target) = duckdb_and_delta_with_seed(dir.path()).await;
        source
            .execute("DELETE FROM s.events WHERE id = 3")
            .await
            .unwrap();
        let spec = small_table_spec();
        let result = target
            .run_merge(
                &spec,
                "SELECT id, name FROM s.events ORDER BY id",
                &["id".into()],
                &["name".into()],
                "p35d_hard",
                "merge",
                Some(source.as_ref()),
                Some(DeleteHandling::Hard),
                false,
            )
            .await
            .unwrap();
        assert_eq!(result.rows_inserted, 0);
        assert_eq!(result.rows_updated, Some(0));
        assert_eq!(result.rows_closed, Some(1), "id=3 deleted");

        let stream = target.read_arrow_stream("raw/events").await.unwrap();
        let batches: Vec<RecordBatch> = futures_util::TryStreamExt::try_collect(stream)
            .await
            .unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn delta_run_merge_dry_run_does_not_commit() {
        let dir = tempfile::tempdir().unwrap();
        let (source, target) = duckdb_and_delta_with_seed(dir.path()).await;
        source
            .execute("UPDATE s.events SET name = 'b-changed' WHERE id = 2")
            .await
            .unwrap();
        let spec = small_table_spec();
        let result = target
            .run_merge(
                &spec,
                "SELECT id, name FROM s.events ORDER BY id",
                &["id".into()],
                &["name".into()],
                "p35d_dry",
                "merge",
                Some(source.as_ref()),
                None,
                true,
            )
            .await
            .unwrap();
        assert_eq!(result.status, "dry_run");
        let stream = target.read_arrow_stream("raw/events").await.unwrap();
        let batches: Vec<RecordBatch> = futures_util::TryStreamExt::try_collect(stream)
            .await
            .unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn delta_run_merge_rejects_uninitialized_target() {
        let dir = tempfile::tempdir().unwrap();
        let target = DeltaBackend::open_local(dir.path()).unwrap();
        let source = duckdb_with_events().await;
        let spec = small_table_spec();
        let err = target
            .run_merge(
                &spec,
                "SELECT id, name FROM s.events",
                &["id".into()],
                &["name".into()],
                "p35d_no_target",
                "merge",
                Some(source.as_ref()),
                None,
                false,
            )
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("uninitialized"), "got: {msg}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn delta_run_merge_hard_refuses_empty_source() {
        let dir = tempfile::tempdir().unwrap();
        let (source, target) = duckdb_and_delta_with_seed(dir.path()).await;
        source.execute("DELETE FROM s.events").await.unwrap();
        let spec = small_table_spec();
        let err = target
            .run_merge(
                &spec,
                "SELECT id, name FROM s.events",
                &["id".into()],
                &["name".into()],
                "p35d_empty_hard",
                "merge",
                Some(source.as_ref()),
                Some(DeleteHandling::Hard),
                false,
            )
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("empty source"), "got: {msg}");
    }

    // --- Phase 35e: run_scd2 (DuckDB → Delta) ------------------------------

    /// Source has (customer_id, email, name); compare on (email, name).
    fn small_scd2_spec() -> TableSpec {
        use crate::types::{ColumnSpec, ColumnType};
        TableSpec {
            schema: "raw".into(),
            name: "customer_dim".into(),
            columns: vec![
                ColumnSpec {
                    name: "customer_id".into(),
                    ty: ColumnType::BigInt,
                    nullable: false,
                    primary_key: true,
                },
                ColumnSpec {
                    name: "email".into(),
                    ty: ColumnType::Text,
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

    async fn duckdb_with_customers() -> Arc<dyn Backend> {
        let duck: Arc<dyn Backend> = Arc::new(DuckDBBackend::open(":memory:").unwrap());
        duck.execute("CREATE SCHEMA s").await.unwrap();
        duck.execute("CREATE TABLE s.customers (customer_id BIGINT, email VARCHAR, name VARCHAR)")
            .await
            .unwrap();
        duck.execute(
            "INSERT INTO s.customers VALUES (1, 'a@x.com', 'alice'), (2, 'b@x.com', 'bob')",
        )
        .await
        .unwrap();
        duck
    }

    async fn count_rows(target: &DeltaBackend, table_path: &str) -> usize {
        let stream = target.read_arrow_stream(table_path).await.unwrap();
        let batches: Vec<RecordBatch> = futures_util::TryStreamExt::try_collect(stream)
            .await
            .unwrap();
        batches.iter().map(|b| b.num_rows()).sum()
    }

    async fn count_rows_where(target: &DeltaBackend, table_path: &str, predicate: &str) -> usize {
        use deltalake::datafusion::datasource::MemTable;
        use deltalake::datafusion::prelude::SessionContext;

        let stream = target.read_arrow_stream(table_path).await.unwrap();
        let batches: Vec<RecordBatch> = futures_util::TryStreamExt::try_collect(stream)
            .await
            .unwrap();
        if batches.is_empty() {
            return 0;
        }
        let schema = batches[0].schema();
        let provider = Arc::new(MemTable::try_new(schema, vec![batches]).unwrap());
        let ctx = SessionContext::new();
        ctx.register_table("t", provider).unwrap();
        let df = ctx
            .sql(&format!("SELECT count(*) AS n FROM t WHERE {predicate}"))
            .await
            .unwrap();
        let result = df.collect().await.unwrap();
        let array = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        array.value(0) as usize
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn delta_run_scd2_first_load_inserts_all_current() {
        let dir = tempfile::tempdir().unwrap();
        let target = DeltaBackend::open_local(dir.path()).unwrap();
        let source = duckdb_with_customers().await;
        let spec = small_scd2_spec();

        let r = target
            .run_scd2(
                &spec,
                "SELECT customer_id, email, name FROM s.customers ORDER BY customer_id",
                &["customer_id".into()],
                &["email".into(), "name".into()],
                "p35e_first",
                Some(source.as_ref()),
                None,
                None,
                None,
                false,
            )
            .await
            .unwrap();
        assert_eq!(r.rows_inserted, 2);
        assert_eq!(r.rows_closed, Some(0));
        assert_eq!(r.status, "success");

        assert_eq!(count_rows(&target, "raw/customer_dim").await, 2);
        assert_eq!(
            count_rows_where(&target, "raw/customer_dim", "is_current = true").await,
            2
        );
        assert_eq!(
            count_rows_where(&target, "raw/customer_dim", "valid_to IS NULL").await,
            2
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn delta_run_scd2_second_load_closes_changed_row() {
        let dir = tempfile::tempdir().unwrap();
        let target = DeltaBackend::open_local(dir.path()).unwrap();
        let source = duckdb_with_customers().await;
        let spec = small_scd2_spec();

        // First load.
        target
            .run_scd2(
                &spec,
                "SELECT customer_id, email, name FROM s.customers ORDER BY customer_id",
                &["customer_id".into()],
                &["email".into(), "name".into()],
                "p35e_a",
                Some(source.as_ref()),
                None,
                None,
                None,
                false,
            )
            .await
            .unwrap();

        // Mutate bob: bob → bob2 / b2@x.com. Alice unchanged.
        source
            .execute("UPDATE s.customers SET email='b2@x.com', name='bob2' WHERE customer_id=2")
            .await
            .unwrap();
        let r = target
            .run_scd2(
                &spec,
                "SELECT customer_id, email, name FROM s.customers ORDER BY customer_id",
                &["customer_id".into()],
                &["email".into(), "name".into()],
                "p35e_b",
                Some(source.as_ref()),
                None,
                None,
                None,
                false,
            )
            .await
            .unwrap();
        assert_eq!(r.rows_inserted, 1, "only bob is new");
        assert_eq!(r.rows_closed, Some(1), "old bob is closed");

        assert_eq!(count_rows(&target, "raw/customer_dim").await, 3);
        assert_eq!(
            count_rows_where(&target, "raw/customer_dim", "is_current = true").await,
            2
        );
        assert_eq!(
            count_rows_where(
                &target,
                "raw/customer_dim",
                "customer_id = 2 AND is_current = true",
            )
            .await,
            1
        );
        assert_eq!(
            count_rows_where(
                &target,
                "raw/customer_dim",
                "customer_id = 2 AND is_current = false",
            )
            .await,
            1
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn delta_run_scd2_idempotent_when_no_changes() {
        let dir = tempfile::tempdir().unwrap();
        let target = DeltaBackend::open_local(dir.path()).unwrap();
        let source = duckdb_with_customers().await;
        let spec = small_scd2_spec();

        for tag in ["p35e_idem_1", "p35e_idem_2"] {
            target
                .run_scd2(
                    &spec,
                    "SELECT customer_id, email, name FROM s.customers ORDER BY customer_id",
                    &["customer_id".into()],
                    &["email".into(), "name".into()],
                    tag,
                    Some(source.as_ref()),
                    None,
                    None,
                    None,
                    false,
                )
                .await
                .unwrap();
        }
        assert_eq!(count_rows(&target, "raw/customer_dim").await, 2);
        assert_eq!(
            count_rows_where(&target, "raw/customer_dim", "is_current = true").await,
            2
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn delta_run_scd2_soft_delete_closes_missing_keys() {
        let dir = tempfile::tempdir().unwrap();
        let target = DeltaBackend::open_local(dir.path()).unwrap();
        let source = duckdb_with_customers().await;
        let spec = small_scd2_spec();

        target
            .run_scd2(
                &spec,
                "SELECT customer_id, email, name FROM s.customers",
                &["customer_id".into()],
                &["email".into(), "name".into()],
                "p35e_soft_a",
                Some(source.as_ref()),
                None,
                None,
                None,
                false,
            )
            .await
            .unwrap();

        // Drop bob from source; expect Soft delete to close his current
        // row.
        source
            .execute("DELETE FROM s.customers WHERE customer_id = 2")
            .await
            .unwrap();
        let r = target
            .run_scd2(
                &spec,
                "SELECT customer_id, email, name FROM s.customers",
                &["customer_id".into()],
                &["email".into(), "name".into()],
                "p35e_soft_b",
                Some(source.as_ref()),
                Some(DeleteHandling::Soft),
                None,
                None,
                false,
            )
            .await
            .unwrap();
        assert_eq!(r.rows_inserted, 0);
        assert_eq!(r.rows_closed, Some(1));

        // Alice still current; bob no longer current; bob's old row
        // exists with valid_to set.
        assert_eq!(
            count_rows_where(
                &target,
                "raw/customer_dim",
                "customer_id = 1 AND is_current = true",
            )
            .await,
            1
        );
        assert_eq!(
            count_rows_where(
                &target,
                "raw/customer_dim",
                "customer_id = 2 AND is_current = false AND valid_to IS NOT NULL",
            )
            .await,
            1
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn delta_run_scd2_ttl_expires_stale_current() {
        let dir = tempfile::tempdir().unwrap();
        let target = DeltaBackend::open_local(dir.path()).unwrap();
        let source = duckdb_with_customers().await;
        let spec = small_scd2_spec();

        target
            .run_scd2(
                &spec,
                "SELECT customer_id, email, name FROM s.customers",
                &["customer_id".into()],
                &["email".into(), "name".into()],
                "p35e_ttl_first",
                Some(source.as_ref()),
                None,
                None,
                None,
                false,
            )
            .await
            .unwrap();

        // No source change; run again with a -1 second TTL so every
        // current row becomes "stale" and gets tombstoned. Negative
        // ttl is contrived but exercises the predicate.
        let r = target
            .run_scd2(
                &spec,
                "SELECT customer_id, email, name FROM s.customers",
                &["customer_id".into()],
                &["email".into(), "name".into()],
                "p35e_ttl_apply",
                Some(source.as_ref()),
                None,
                None,
                Some(-1),
                false,
            )
            .await
            .unwrap();
        assert_eq!(r.rows_inserted, 0);
        assert!(r.rows_closed.unwrap() >= 2, "TTL closes both current rows");

        assert_eq!(
            count_rows_where(&target, "raw/customer_dim", "is_current = true").await,
            0
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn delta_run_scd2_event_time_not_yet_supported() {
        let dir = tempfile::tempdir().unwrap();
        let target = DeltaBackend::open_local(dir.path()).unwrap();
        let source = duckdb_with_customers().await;
        let spec = small_scd2_spec();
        let err = target
            .run_scd2(
                &spec,
                "SELECT customer_id, email, name FROM s.customers",
                &["customer_id".into()],
                &["email".into(), "name".into()],
                "p35e_event_time",
                Some(source.as_ref()),
                None,
                Some("event_ts"),
                None,
                false,
            )
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("event_timestamp_column"), "got: {msg}");
    }

    // --- Δ.X1: CDC apply via single-MERGE per batch ----------

    /// Helper: build a TableSpec for a `customers` mirror table —
    /// (id BIGINT PK, email TEXT, name TEXT). Used by every Δ.X1
    /// test below. Mirror tables only need the user-declared
    /// columns; the `__op` synthesized at CDC-apply time is not
    /// part of the target schema.
    fn cdc_customers_spec() -> TableSpec {
        use crate::types::{ColumnSpec, ColumnType};
        TableSpec {
            schema: "mirror".into(),
            name: "customers".into(),
            columns: vec![
                ColumnSpec {
                    name: "id".into(),
                    ty: ColumnType::BigInt,
                    nullable: false,
                    primary_key: true,
                },
                ColumnSpec {
                    name: "email".into(),
                    ty: ColumnType::Text,
                    nullable: true,
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

    /// Helper: encode a slice of Debezium-shaped JSON rows as a
    /// schema-inferred RecordBatch — same shape Kafka source's
    /// JSON-payload decoder produces. The CDC executor calls
    /// `events_from_batch` on whatever shape arrives.
    fn cdc_record_batch_from_json(
        rows: &[serde_json::Map<String, serde_json::Value>],
    ) -> RecordBatch {
        use std::io::Cursor;
        let mut buf = Vec::new();
        for row in rows {
            buf.extend(serde_json::to_string(row).unwrap().bytes());
            buf.push(b'\n');
        }
        let mut sniff = Cursor::new(&buf);
        let (schema, _) =
            arrow_json::reader::infer_json_schema_from_seekable(&mut sniff, None).expect("schema");
        let cursor = Cursor::new(&buf);
        let mut reader = arrow_json::ReaderBuilder::new(Arc::new(schema))
            .build(cursor)
            .expect("reader");
        reader
            .next()
            .expect("at least one batch")
            .expect("decode ok")
    }

    /// Helper: seed a Delta table with the customers schema +
    /// 0 rows so subsequent run_cdc calls have a target to MERGE
    /// against. Delta MERGE requires a non-uninitialized target.
    async fn seed_empty_customers_table(backend: &DeltaBackend, target: &TargetTable) {
        use arrow_array::{Int64Array, StringArray};
        use arrow_schema::{DataType, Field, Schema as ArrowSchema};

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("email", DataType::Utf8, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        // Single-row seed batch — minimal data so the table exists
        // with a known schema. We DELETE this seed row in the first
        // CDC batch so the test doesn't have to special-case it.
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![0i64])),
                Arc::new(StringArray::from(vec![Some("seed@example.com")])),
                Arc::new(StringArray::from(vec![Some("Seed")])),
            ],
        )
        .unwrap();
        backend
            .write_arrow_stream(target, arrow_stream_for(batch), WriteMode::Append)
            .await
            .unwrap();
    }

    /// Helper: collect every row from a Delta table into a Vec
    /// of (id, email, name) tuples ordered by id. Centralises the
    /// scan-then-extract pattern so individual tests stay focused
    /// on assertions.
    async fn read_customers(
        backend: &DeltaBackend,
        target: &TargetTable,
    ) -> Vec<(i64, Option<String>, Option<String>)> {
        use arrow_array::Array;
        use arrow_array::cast::AsArray;
        use arrow_array::types::Int64Type;
        use arrow_schema::DataType;
        let stream = backend
            .read_arrow_stream(&format!("{}/{}", target.schema, target.name))
            .await
            .unwrap();
        let batches: Vec<RecordBatch> = futures_util::TryStreamExt::try_collect(stream)
            .await
            .unwrap();
        // Delta's MERGE result-cast may produce LargeUtf8 instead
        // of Utf8 — handle both via a small visitor.
        fn read_string_column(batch: &RecordBatch, idx: usize) -> Vec<Option<String>> {
            let col = batch.column(idx);
            match col.data_type() {
                DataType::Utf8 => {
                    let s = col.as_string::<i32>();
                    (0..s.len())
                        .map(|i| {
                            if s.is_null(i) {
                                None
                            } else {
                                Some(s.value(i).to_string())
                            }
                        })
                        .collect()
                }
                DataType::LargeUtf8 => {
                    let s = col.as_string::<i64>();
                    (0..s.len())
                        .map(|i| {
                            if s.is_null(i) {
                                None
                            } else {
                                Some(s.value(i).to_string())
                            }
                        })
                        .collect()
                }
                DataType::Utf8View => {
                    let s = col.as_string_view();
                    (0..s.len())
                        .map(|i| {
                            if s.is_null(i) {
                                None
                            } else {
                                Some(s.value(i).to_string())
                            }
                        })
                        .collect()
                }
                other => panic!("unexpected string-column type from Delta read: {other:?}"),
            }
        }
        let mut rows: Vec<(i64, Option<String>, Option<String>)> = Vec::new();
        for batch in &batches {
            let id_idx = batch.schema().index_of("id").unwrap();
            let email_idx = batch.schema().index_of("email").unwrap();
            let name_idx = batch.schema().index_of("name").unwrap();
            let ids = batch.column(id_idx).as_primitive::<Int64Type>();
            let emails = read_string_column(batch, email_idx);
            let names = read_string_column(batch, name_idx);
            for i in 0..batch.num_rows() {
                rows.push((ids.value(i), emails[i].clone(), names[i].clone()));
            }
        }
        rows.sort_by_key(|r| r.0);
        rows
    }

    /// Δ.X1: each of c / u / d gets a turn in its own batch so
    /// the MERGE-derived counters (creates / updates / deletes)
    /// are observable independently. Within-batch dedupe is
    /// covered by the next test.
    #[tokio::test(flavor = "multi_thread")]
    async fn delta_cdc_inserts_updates_deletes_across_batches() {
        use crate::cdc::{CdcConfig, EnvelopeKind};
        use serde_json::json;

        let dir = tempfile::tempdir().unwrap();
        let backend = DeltaBackend::open_local(dir.path()).unwrap();
        let target = TargetTable {
            schema: "mirror".into(),
            name: "customers".into(),
        };
        seed_empty_customers_table(&backend, &target).await;

        let mut cdc = CdcConfig::for_envelope(EnvelopeKind::Debezium);
        cdc.key_field = "after.id".into();
        let spec = cdc_customers_spec();

        // Batch 1: two INSERTs.
        let r1 = backend
            .run_cdc(
                &spec,
                cdc_record_batch_from_json(&[
                    json!({
                        "before": null,
                        "after": {"id": 1, "email": "alice@example.com", "name": "Alice"},
                        "source": {"ts_ms": 1_700_000_000_001_i64},
                        "op": "c",
                        "ts_ms": 1_700_000_000_001_i64,
                    })
                    .as_object()
                    .unwrap()
                    .clone(),
                    json!({
                        "before": null,
                        "after": {"id": 2, "email": "bob@example.com", "name": "Bob"},
                        "source": {"ts_ms": 1_700_000_000_002_i64},
                        "op": "c",
                        "ts_ms": 1_700_000_000_002_i64,
                    })
                    .as_object()
                    .unwrap()
                    .clone(),
                ]),
                &cdc,
                "test_delta_cdc",
            )
            .await
            .expect("batch 1");
        assert_eq!(r1.creates, 2, "two inserts in batch 1");
        assert_eq!(r1.updates, 0);
        assert_eq!(r1.deletes, 0);

        // Batch 2: UPDATE id=1, DELETE seed row id=0.
        let r2 = backend
            .run_cdc(
                &spec,
                cdc_record_batch_from_json(&[
                    json!({
                        "before": {"id": 1, "email": "alice@example.com", "name": "Alice"},
                        "after":  {"id": 1, "email": "alice@example.com", "name": "Alice Smith"},
                        "source": {"ts_ms": 1_700_000_005_000_i64},
                        "op": "u",
                        "ts_ms": 1_700_000_005_000_i64,
                    })
                    .as_object()
                    .unwrap()
                    .clone(),
                    json!({
                        "before": {"id": 0, "email": "seed@example.com", "name": "Seed"},
                        "after":  null,
                        "source": {"ts_ms": 1_700_000_010_000_i64},
                        "op": "d",
                        "ts_ms": 1_700_000_010_000_i64,
                    })
                    .as_object()
                    .unwrap()
                    .clone(),
                ]),
                &cdc,
                "test_delta_cdc",
            )
            .await
            .expect("batch 2");
        assert_eq!(r2.creates, 0, "no inserts in batch 2");
        assert_eq!(r2.updates, 1, "one update");
        assert_eq!(r2.deletes, 1, "seed row deleted");

        let surviving = read_customers(&backend, &target).await;
        assert_eq!(
            surviving,
            vec![
                (
                    1,
                    Some("alice@example.com".into()),
                    Some("Alice Smith".into())
                ),
                (2, Some("bob@example.com".into()), Some("Bob".into())),
            ]
        );
    }

    /// Δ.X1: within-batch dedupe — `c` then `u` for the same PK
    /// in one batch collapses to the post-image of the highest-ts
    /// event. Newest wins; redundant earlier events disappear
    /// before MERGE sees them.
    #[tokio::test(flavor = "multi_thread")]
    async fn delta_cdc_within_batch_dedupes_by_newest_ts() {
        use crate::cdc::{CdcConfig, EnvelopeKind};
        use serde_json::json;

        let dir = tempfile::tempdir().unwrap();
        let backend = DeltaBackend::open_local(dir.path()).unwrap();
        let target = TargetTable {
            schema: "mirror".into(),
            name: "customers".into(),
        };
        seed_empty_customers_table(&backend, &target).await;

        let mut cdc = CdcConfig::for_envelope(EnvelopeKind::Debezium);
        cdc.key_field = "after.id".into();
        let spec = cdc_customers_spec();

        // Three events for id=1 in a single batch:
        //   t=1: INSERT name="Alice"
        //   t=2: UPDATE name="Alice Smith"
        //   t=3: UPDATE name="Alice Final"
        // Plus an unrelated DELETE of the seed row id=0.
        // Dedupe should keep the t=3 row for id=1 and the delete
        // for id=0 — two events, not four.
        let result = backend
            .run_cdc(
                &spec,
                cdc_record_batch_from_json(&[
                    json!({
                        "before": null,
                        "after": {"id": 1, "email": "a@x.com", "name": "Alice"},
                        "source": {"ts_ms": 1_700_000_000_001_i64},
                        "op": "c",
                        "ts_ms": 1_700_000_000_001_i64,
                    })
                    .as_object()
                    .unwrap()
                    .clone(),
                    json!({
                        "before": {"id": 1, "email": "a@x.com", "name": "Alice"},
                        "after": {"id": 1, "email": "a@x.com", "name": "Alice Smith"},
                        "source": {"ts_ms": 1_700_000_000_002_i64},
                        "op": "u",
                        "ts_ms": 1_700_000_000_002_i64,
                    })
                    .as_object()
                    .unwrap()
                    .clone(),
                    json!({
                        "before": {"id": 1, "email": "a@x.com", "name": "Alice Smith"},
                        "after": {"id": 1, "email": "a@x.com", "name": "Alice Final"},
                        "source": {"ts_ms": 1_700_000_000_003_i64},
                        "op": "u",
                        "ts_ms": 1_700_000_000_003_i64,
                    })
                    .as_object()
                    .unwrap()
                    .clone(),
                    json!({
                        "before": {"id": 0, "email": "seed@example.com", "name": "Seed"},
                        "after":  null,
                        "source": {"ts_ms": 1_700_000_000_004_i64},
                        "op": "d",
                        "ts_ms": 1_700_000_000_004_i64,
                    })
                    .as_object()
                    .unwrap()
                    .clone(),
                ]),
                &cdc,
                "test_delta_cdc_dedupe",
            )
            .await
            .expect("dedupe batch");

        // id=1 didn't exist → counts as INSERT (the t=3 post-image).
        // id=0 existed → DELETE.
        assert_eq!(result.creates, 1, "id=1 collapsed to single insert");
        assert_eq!(result.updates, 0);
        assert_eq!(result.deletes, 1, "seed deleted");

        let surviving = read_customers(&backend, &target).await;
        assert_eq!(
            surviving,
            vec![(1, Some("a@x.com".into()), Some("Alice Final".into()))],
            "post-image of the highest-ts event wins"
        );
    }

    /// Δ.X1: soft-delete mode. A `d` op flips the configured
    /// column to current_timestamp() instead of removing the row.
    #[tokio::test(flavor = "multi_thread")]
    async fn delta_cdc_soft_delete_flips_column() {
        use crate::cdc::{CdcConfig, DeleteMode, EnvelopeKind};
        use crate::types::{ColumnSpec, ColumnType};
        use serde_json::json;

        let dir = tempfile::tempdir().unwrap();
        let backend = DeltaBackend::open_local(dir.path()).unwrap();
        let target = TargetTable {
            schema: "mirror".into(),
            name: "customers_soft".into(),
        };

        // Seed a Delta table that includes a deleted_at column.
        use arrow_array::{Int64Array, StringArray, TimestampMicrosecondArray};
        use arrow_schema::{DataType, Field, Schema as ArrowSchema, TimeUnit};
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("email", DataType::Utf8, true),
            Field::new(
                "deleted_at",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
        ]));
        let seed = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1i64])),
                Arc::new(StringArray::from(vec![Some("alice@example.com")])),
                Arc::new(
                    TimestampMicrosecondArray::from(vec![None as Option<i64>]).with_timezone("UTC"),
                ),
            ],
        )
        .unwrap();
        backend
            .write_arrow_stream(&target, arrow_stream_for(seed), WriteMode::Append)
            .await
            .unwrap();

        let spec = TableSpec {
            schema: target.schema.clone(),
            name: target.name.clone(),
            columns: vec![
                ColumnSpec {
                    name: "id".into(),
                    ty: ColumnType::BigInt,
                    nullable: false,
                    primary_key: true,
                },
                ColumnSpec {
                    name: "email".into(),
                    ty: ColumnType::Text,
                    nullable: true,
                    primary_key: false,
                },
                ColumnSpec {
                    name: "deleted_at".into(),
                    ty: ColumnType::TimestampTz,
                    nullable: true,
                    primary_key: false,
                },
            ],
            unique_constraints: vec![],
            fingerprint: String::new(),
        };

        let mut cdc = CdcConfig::for_envelope(EnvelopeKind::Debezium);
        cdc.key_field = "after.id".into();
        cdc.delete_mode = DeleteMode::Soft {
            column: "deleted_at".into(),
        };

        let result = backend
            .run_cdc(
                &spec,
                cdc_record_batch_from_json(&[json!({
                    "before": {"id": 1, "email": "alice@example.com"},
                    "after":  null,
                    "op": "d",
                    "ts_ms": 1_700_000_000_500_i64,
                })
                .as_object()
                .unwrap()
                .clone()]),
                &cdc,
                "test_delta_soft",
            )
            .await
            .expect("soft delete run_cdc");
        // Soft-delete is wired through `when_matched_update`, so
        // Delta reports it as a row update — not a deletion.
        assert_eq!(result.deletes, 0, "soft mode → not a hard delete");
        assert_eq!(result.updates, 1, "soft-delete is a column flip");

        // Row still present, deleted_at populated.
        let stream = backend
            .read_arrow_stream(&format!("{}/{}", target.schema, target.name))
            .await
            .unwrap();
        let batches: Vec<RecordBatch> = futures_util::TryStreamExt::try_collect(stream)
            .await
            .unwrap();
        let n: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(n, 1, "row still present after soft delete");
        let batch = &batches[0];
        let dt_idx = batch.schema().index_of("deleted_at").unwrap();
        let deleted_at = batch.column(dt_idx);
        use arrow_array::Array;
        assert!(!deleted_at.is_null(0), "deleted_at populated");
    }

    /// Δ.X1: SchemaEvolutionPolicy::Fail aborts the batch when
    /// the source carries a column missing on the target spec.
    #[tokio::test(flavor = "multi_thread")]
    async fn delta_cdc_schema_fail_aborts() {
        use crate::cdc::{CdcConfig, EnvelopeKind, SchemaEvolutionPolicy};
        use serde_json::json;

        let dir = tempfile::tempdir().unwrap();
        let backend = DeltaBackend::open_local(dir.path()).unwrap();
        let target = TargetTable {
            schema: "mirror".into(),
            name: "customers".into(),
        };
        seed_empty_customers_table(&backend, &target).await;
        let spec = cdc_customers_spec();

        let mut cdc = CdcConfig::for_envelope(EnvelopeKind::Debezium);
        cdc.key_field = "after.id".into();
        cdc.schema_evolution = SchemaEvolutionPolicy::Fail;

        let err = backend
            .run_cdc(
                &spec,
                cdc_record_batch_from_json(&[json!({
                    "before": null,
                    "after": {"id": 1, "email": "a@x.com", "name": "A", "phone": "+1"},
                    "op": "c",
                    "ts_ms": 1_700_000_000_001_i64,
                })
                .as_object()
                .unwrap()
                .clone()]),
                &cdc,
                "test_delta_fail",
            )
            .await
            .expect_err("Fail policy must error on unknown column");
        let msg = err.to_string();
        assert!(
            msg.contains("phone"),
            "error must name the offending column, got: {msg}"
        );
    }

    // ----------------------------------------------------------
    // Coverage backfill round 3 — pure-function helpers in
    // delta_backend.rs. Each test below pins a small surface that
    // didn't show up under the executor-level tests (no MERGE
    // happens to drive these specific paths) but is reachable
    // unit-style with no infrastructure.
    // ----------------------------------------------------------

    /// `arrow_to_column_type` covers every mapped Arrow data type
    /// it knows about, plus the catch-all Text fallback for
    /// unmapped types. The reflect_table_spec tests only exercise
    /// the common Utf8 / Int64 mappings; this one round-trips the
    /// rest.
    #[test]
    fn arrow_to_column_type_covers_every_mapped_variant() {
        use crate::types::ColumnType;
        use arrow_schema::DataType;

        assert_eq!(arrow_to_column_type(&DataType::Int8), ColumnType::SmallInt);
        assert_eq!(arrow_to_column_type(&DataType::Int16), ColumnType::SmallInt);
        assert_eq!(arrow_to_column_type(&DataType::Int32), ColumnType::Integer);
        assert_eq!(arrow_to_column_type(&DataType::Int64), ColumnType::BigInt);
        assert_eq!(arrow_to_column_type(&DataType::Float32), ColumnType::Float);
        assert_eq!(arrow_to_column_type(&DataType::Float64), ColumnType::Double);
        assert_eq!(
            arrow_to_column_type(&DataType::Boolean),
            ColumnType::Boolean
        );
        assert_eq!(arrow_to_column_type(&DataType::Utf8), ColumnType::Text);
        assert_eq!(arrow_to_column_type(&DataType::LargeUtf8), ColumnType::Text);
        assert_eq!(arrow_to_column_type(&DataType::Binary), ColumnType::Bytes);
        assert_eq!(
            arrow_to_column_type(&DataType::LargeBinary),
            ColumnType::Bytes
        );
        assert_eq!(arrow_to_column_type(&DataType::Date32), ColumnType::Date);
        assert_eq!(arrow_to_column_type(&DataType::Date64), ColumnType::Date);
        assert_eq!(
            arrow_to_column_type(&DataType::Timestamp(
                arrow_schema::TimeUnit::Microsecond,
                None
            )),
            ColumnType::Timestamp
        );
        assert_eq!(
            arrow_to_column_type(&DataType::Timestamp(
                arrow_schema::TimeUnit::Microsecond,
                Some("UTC".into())
            )),
            ColumnType::TimestampTz
        );
        // Catch-all fallback for unmapped DataTypes (decimals,
        // structs, lists, etc.) — surfaces as Text. Locks the
        // permissive choice so a future version that errors
        // instead produces a loud diff.
        assert_eq!(
            arrow_to_column_type(&DataType::Decimal128(10, 2)),
            ColumnType::Text
        );
    }

    /// `column_type_to_arrow` covers every mapped `ColumnType`
    /// plus the explicit Numeric error path.
    #[test]
    fn column_type_to_arrow_round_trips_every_mapped_variant() {
        use crate::types::ColumnType;
        use arrow_schema::DataType;

        assert_eq!(
            column_type_to_arrow(&ColumnType::SmallInt).unwrap(),
            DataType::Int16
        );
        assert_eq!(
            column_type_to_arrow(&ColumnType::Integer).unwrap(),
            DataType::Int32
        );
        assert_eq!(
            column_type_to_arrow(&ColumnType::BigInt).unwrap(),
            DataType::Int64
        );
        assert_eq!(
            column_type_to_arrow(&ColumnType::Float).unwrap(),
            DataType::Float32
        );
        assert_eq!(
            column_type_to_arrow(&ColumnType::Double).unwrap(),
            DataType::Float64
        );
        assert_eq!(
            column_type_to_arrow(&ColumnType::Boolean).unwrap(),
            DataType::Boolean
        );
        assert_eq!(
            column_type_to_arrow(&ColumnType::Text).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            column_type_to_arrow(&ColumnType::String { length: 64 }).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            column_type_to_arrow(&ColumnType::Json).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            column_type_to_arrow(&ColumnType::Jsonb).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            column_type_to_arrow(&ColumnType::Uuid).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            column_type_to_arrow(&ColumnType::Bytes).unwrap(),
            DataType::Binary
        );
        assert_eq!(
            column_type_to_arrow(&ColumnType::Date).unwrap(),
            DataType::Date32
        );
        assert_eq!(
            column_type_to_arrow(&ColumnType::Timestamp).unwrap(),
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)
        );
        assert_eq!(
            column_type_to_arrow(&ColumnType::TimestampTz).unwrap(),
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into()))
        );

        // Numeric is explicitly unsupported in Δ.X1 PR 1; the
        // error message points users at the Double/Text workaround.
        let err = column_type_to_arrow(&ColumnType::Numeric {
            precision: 18,
            scale: 4,
        })
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("Numeric"), "got: {msg}");
        assert!(msg.contains("Δ.X1") || msg.contains("not yet supported"));
    }

    /// `schema_empty_columns` produces a zero-row Arc<dyn Array>
    /// for every DataType in the input schema, matching shape +
    /// length 0. The catch-all branch (any unmapped DataType
    /// devolves to a zero-row StringArray) is locked too.
    #[test]
    fn schema_empty_columns_returns_zero_row_arrays_for_each_field() {
        use arrow_schema::{DataType, Field, Schema as ArrowSchema, TimeUnit};

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("i16", DataType::Int16, true),
            Field::new("i32", DataType::Int32, true),
            Field::new("i64", DataType::Int64, true),
            Field::new("f32", DataType::Float32, true),
            Field::new("f64", DataType::Float64, true),
            Field::new("b", DataType::Boolean, true),
            Field::new("s", DataType::Utf8, true),
            // Unmapped DataType → catch-all branch.
            Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        ]));
        let cols = schema_empty_columns(&schema);
        assert_eq!(cols.len(), 8);
        for col in &cols {
            assert_eq!(col.len(), 0, "every empty-batch column has zero rows");
        }
    }

    /// `canonical_pk_string` round-trips the three branches:
    /// single-PK fast path (1), composite-PK Object branch (2),
    /// composite-PK with a missing key (the "null" fallback).
    #[test]
    fn canonical_pk_string_covers_every_branch() {
        use crate::cdc::{CdcEvent, CdcOp};
        use serde_json::{Map, Value, json};

        // Single PK — returns event.key.to_string() directly.
        let single = CdcEvent {
            op: CdcOp::Create,
            key: json!(42),
            ts_ms: Some(1),
            after: None,
            before: None,
        };
        assert_eq!(canonical_pk_string(&single, &["id".to_string()]), "42");

        // Composite PK with all keys present — joins values
        // with U+001F (the unit separator).
        let mut k = Map::new();
        k.insert("tenant_id".into(), Value::Number(7.into()));
        k.insert("id".into(), Value::Number(3.into()));
        let composite = CdcEvent {
            op: CdcOp::Update,
            key: Value::Object(k),
            ts_ms: Some(2),
            after: None,
            before: None,
        };
        let pk_cols = vec!["tenant_id".to_string(), "id".to_string()];
        let canon = canonical_pk_string(&composite, &pk_cols);
        assert!(
            canon.contains('\u{1f}'),
            "composite uses unit-separator joiner"
        );
        assert!(canon.contains('7') && canon.contains('3'));

        // Composite PK with one column missing — that column
        // serialises to "null".
        let mut k2 = Map::new();
        k2.insert("tenant_id".into(), Value::Number(11.into()));
        let partial = CdcEvent {
            op: CdcOp::Delete,
            key: Value::Object(k2),
            ts_ms: Some(3),
            after: None,
            before: None,
        };
        let canon = canonical_pk_string(&partial, &pk_cols);
        assert!(
            canon.contains("null"),
            "missing PK column → \"null\"; got: {canon}"
        );

        // Composite-style pk_cols but key is a scalar (parser
        // produces this shape for delete events with a scalar
        // before-image PK). Falls through to the bottom branch.
        let scalar_with_composite_cols = CdcEvent {
            op: CdcOp::Delete,
            key: json!(99),
            ts_ms: None,
            after: None,
            before: None,
        };
        assert_eq!(
            canonical_pk_string(&scalar_with_composite_cols, &pk_cols),
            "99"
        );
    }
}
