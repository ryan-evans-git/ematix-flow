//! Σ.J.2 — cross-stage bloom filter for distributed joins.
//!
//! ## Why this is the right distributed lever
//!
//! In a distributed hash join, the build side (smaller table) lives
//! on one set of workers; the probe side (larger table) lives on
//! another. The probe side ships rows over Flight to the join workers
//! based on partitioning. For a selective join (say, 1% of probe rows
//! match a build row), 99% of the shuffle is wasted bandwidth.
//!
//! A bloom filter computed on the build side, shipped via Flight
//! metadata to the probe-side scans, lets probe-side workers skip
//! 90-99% of those rows *before they hit the wire*. This is **the**
//! single biggest distributed lever publicly demonstrated by
//! Photon/Trino/Spark — and not implemented in OSS DataFusion
//! distributed today.
//!
//! ## Design
//!
//! - [`BloomFilter`] — fixed-size bit vector with k hash functions.
//!   Tuned for ~1% FPR at 10 bits per key. Standard split-block design
//!   (cache-line-aligned blocks of 256 bits, k=8 hashes per block) for
//!   fast SIMD-friendly lookup.
//! - [`BloomBuilder`] — accumulates keys on the build side; finalises
//!   to a [`SerialisedBloom`] for shipping.
//! - [`SerialisedBloom`] — wire format. Tiny header (n_blocks, k_hashes,
//!   seed) + raw bytes; round-trippable via `to_bytes` / `from_bytes`.
//!
//! ## Tonight's scope: kernel + serialisation
//!
//! Plumbing through `HashJoinExec` + Flight metadata propagation is a
//! follow-up bite (`Σ.J.2.b`). What lands here:
//!
//! 1. BloomFilter kernel + builder.
//! 2. Serialisation format (so the Flight-metadata wiring can drop
//!    in without redesign).
//! 3. Tests proving FPR is within spec.
//! 4. A `RecordBatch` builder helper that accepts an Arrow column
//!    and produces a BloomBuilder over its values.

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::Array;

/// Bits per key. 10 bits per key + k=8 hash functions ≈ 1% FPR.
pub const DEFAULT_BITS_PER_KEY: usize = 10;
/// Hash functions per lookup. Split-block design — all k hits land in
/// a single 256-bit block, so each lookup is one cache line.
pub const DEFAULT_K_HASHES: usize = 8;
/// Block size in bits. Cache-line sized → one lookup = one fetch.
pub const BLOCK_BITS: usize = 256;
pub const BLOCK_BYTES: usize = BLOCK_BITS / 8;

/// Σ.J.2 — split-block bloom filter. Cache-line-aligned blocks, k
/// hashes per block, single block touched per lookup.
#[derive(Debug, Clone)]
pub struct BloomFilter {
    /// Raw bytes, length = n_blocks * BLOCK_BYTES.
    bits: Vec<u8>,
    n_blocks: usize,
    seed: u64,
}

impl BloomFilter {
    /// Construct sized for `expected_keys` with the default 1% FPR.
    pub fn for_keys(expected_keys: usize) -> Self {
        Self::with_capacity(expected_keys, DEFAULT_BITS_PER_KEY)
    }

    pub fn with_capacity(expected_keys: usize, bits_per_key: usize) -> Self {
        let total_bits = (expected_keys.max(1) * bits_per_key).max(BLOCK_BITS);
        let n_blocks = total_bits.div_ceil(BLOCK_BITS);
        Self {
            bits: vec![0u8; n_blocks * BLOCK_BYTES],
            n_blocks,
            seed: 0xc0ffee_u64,
        }
    }

    fn block_idx(&self, h: u64) -> usize {
        (h % self.n_blocks as u64) as usize
    }

    /// Σ.J.2 — insert a hash. Caller is responsible for hashing the
    /// key (so we can support i32/i64/string/etc. uniformly).
    pub fn insert_hash(&mut self, h: u64) {
        let block = self.block_idx(h);
        let block_start = block * BLOCK_BYTES;
        // k=8 split hashes from one u64. Each picks one bit position
        // within the block.
        let mut x = h.wrapping_mul(0x9e37_79b9_7f4a_7c15);
        for _ in 0..DEFAULT_K_HASHES {
            x = x.wrapping_mul(0x9e37_79b9_7f4a_7c15);
            let bit = (x >> 32) as usize % BLOCK_BITS;
            let byte = block_start + bit / 8;
            self.bits[byte] |= 1 << (bit % 8);
        }
    }

    /// Σ.J.2 — check if `h` might be present.
    pub fn might_contain_hash(&self, h: u64) -> bool {
        let block = self.block_idx(h);
        let block_start = block * BLOCK_BYTES;
        let mut x = h.wrapping_mul(0x9e37_79b9_7f4a_7c15);
        for _ in 0..DEFAULT_K_HASHES {
            x = x.wrapping_mul(0x9e37_79b9_7f4a_7c15);
            let bit = (x >> 32) as usize % BLOCK_BITS;
            let byte = block_start + bit / 8;
            if self.bits[byte] & (1 << (bit % 8)) == 0 {
                return false;
            }
        }
        true
    }

    /// Σ.J.2 — convenience: insert an i64 key.
    pub fn insert_i64(&mut self, v: i64) {
        self.insert_hash(hash_i64(v, self.seed));
    }

    pub fn might_contain_i64(&self, v: i64) -> bool {
        self.might_contain_hash(hash_i64(v, self.seed))
    }

    /// Σ.J.2 — convenience: insert a string key.
    pub fn insert_str(&mut self, s: &str) {
        self.insert_hash(hash_bytes(s.as_bytes(), self.seed));
    }

    pub fn might_contain_str(&self, s: &str) -> bool {
        self.might_contain_hash(hash_bytes(s.as_bytes(), self.seed))
    }

    /// Σ.J.2 — wire-format serialisation. 24-byte header (magic +
    /// version + n_blocks + seed) + raw bits.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(24 + self.bits.len());
        out.extend_from_slice(b"EBLM0001"); // 8 bytes magic + version
        out.extend_from_slice(&(self.n_blocks as u64).to_le_bytes());
        out.extend_from_slice(&self.seed.to_le_bytes());
        out.extend_from_slice(&self.bits);
        out
    }

    /// Σ.J.2.b — encode for Flight HTTP header (tonic
    /// `MetadataMap` value). Uses hex (lowercase) — slightly larger
    /// than base64 (2x vs 1.33x) but zero deps and gRPC-header-safe
    /// (no ascii-binary boundary issues). Acceptable since
    /// Σ.J.2.b's header path is ≤5K-key blooms ≤6 KiB → ≤12 KiB hex.
    /// Wait — gRPC headers cap at ~8 KiB. So practical threshold is
    /// ≤3K-key blooms in hex. Larger blooms must use the proto path
    /// (Σ.J.2.c, see docs/PHASE_SIGMA_J2B_FLIGHT_BLOOM.md).
    pub fn to_flight_header(&self) -> String {
        let bytes = self.to_bytes();
        let mut out = String::with_capacity(bytes.len() * 2);
        const HEX: &[u8; 16] = b"0123456789abcdef";
        for b in bytes {
            out.push(HEX[(b >> 4) as usize] as char);
            out.push(HEX[(b & 0xf) as usize] as char);
        }
        out
    }

    /// Σ.J.2.b — decode from a Flight HTTP header value. Returns
    /// `Err(BadMagic)` if the hex decodes to non-bloom bytes.
    pub fn from_flight_header(header: &str) -> Result<Self, BloomError> {
        if header.len() % 2 != 0 {
            return Err(BloomError::BadMagic);
        }
        let mut bytes = Vec::with_capacity(header.len() / 2);
        let b = header.as_bytes();
        for i in (0..b.len()).step_by(2) {
            let hi = hex_val(b[i])?;
            let lo = hex_val(b[i + 1])?;
            bytes.push((hi << 4) | lo);
        }
        Self::from_bytes(&bytes)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, BloomError> {
        if bytes.len() < 24 {
            return Err(BloomError::TooSmall);
        }
        if &bytes[0..8] != b"EBLM0001" {
            return Err(BloomError::BadMagic);
        }
        let n_blocks =
            u64::from_le_bytes(bytes[8..16].try_into().unwrap()) as usize;
        let seed = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
        let expected_len = 24 + n_blocks * BLOCK_BYTES;
        if bytes.len() != expected_len {
            return Err(BloomError::LengthMismatch);
        }
        let bits = bytes[24..].to_vec();
        Ok(Self {
            bits,
            n_blocks,
            seed,
        })
    }

    /// Bit count — useful for telemetry / FPR estimation.
    pub fn estimated_population(&self) -> usize {
        self.bits.iter().map(|b| b.count_ones() as usize).sum()
    }

    /// Footprint in bytes — what's shipped over Flight.
    pub fn wire_size(&self) -> usize {
        24 + self.bits.len()
    }
}

/// Σ.J.2.b.v — header-name prefix used for the multi-bloom flight
/// transport. Probe-side workers scan inbound headers for this prefix
/// to pick up build-side blooms shipped by the coordinator.
pub const FLIGHT_BLOOM_HEADER_PREFIX: &str = "x-ematix-bloom-";

/// Σ.J.2.b.vi — per-request, probe-side bag of `(column_uuid →
/// BloomFilter)`. Constructed once per inbound query, then handed to
/// [`crate::context_bloom_rule::EnableContextBloomRule`] which wraps
/// matching scans in [`BloomFilterExec`] before execution.
///
/// **The uuid scheme is `<table>.<column>` (both lowercase).** For
/// path-based parquet scans, `table` is the file basename without
/// extension (e.g. `lineitem.parquet` → `lineitem.l_orderkey`).
/// Identical strings on build + probe side are how the two halves
/// match up — both compute the uuid from the same scheme.
///
/// Storage-only; the http-bound `from_headers` constructor lives in
/// `ematix-flow-distributed::bloom_flight` so this crate stays
/// http-dep-free.
#[derive(Debug, Default, Clone)]
pub struct ContextBlooms {
    inner: std::sync::Arc<std::collections::HashMap<String, std::sync::Arc<BloomFilter>>>,
}

impl ContextBlooms {
    pub fn new(
        blooms: std::collections::HashMap<String, std::sync::Arc<BloomFilter>>,
    ) -> Self {
        Self {
            inner: std::sync::Arc::new(blooms),
        }
    }

    pub fn get(&self, column_uuid: &str) -> Option<&std::sync::Arc<BloomFilter>> {
        self.inner.get(column_uuid)
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.inner.keys().map(|s| s.as_str())
    }
}

/// Σ.J.2.b.vi — canonical column-uuid for the bloom keying scheme.
/// `table` is normalised to lowercase; same for `column`. This is the
/// string both build and probe sides hash on.
pub fn column_uuid(table: &str, column: &str) -> String {
    let mut s = String::with_capacity(table.len() + 1 + column.len());
    for c in table.chars() {
        s.extend(c.to_lowercase());
    }
    s.push('.');
    for c in column.chars() {
        s.extend(c.to_lowercase());
    }
    s
}

/// Σ.J.2.b.v — gRPC max header value size. Standard tonic / nginx /
/// envoy default is ~8 KiB per header value. To leave room for
/// framing + chunking, we cap a single bloom header at 6 KiB hex
/// (= 3 KiB binary = ~2.4K keys @ 10 bits/key). Blooms bigger than
/// this go through the upstream proto path (Σ.J.2.c, deferred).
pub const FLIGHT_BLOOM_MAX_HEADER_HEX: usize = 6 * 1024;

/// Σ.J.2.b.v — bound on the *combined* size of all bloom headers on
/// one request. gRPC implementations limit total headers to ~16 KiB.
/// Drop blooms past this in `blooms_to_header_pairs` so a single
/// oversized payload doesn't block all blooms.
pub const FLIGHT_BLOOM_MAX_TOTAL_HEX: usize = 12 * 1024;

/// Σ.J.2.b.v — marshall a set of `(column_uuid, BloomFilter)` to
/// `(header_name, header_value)` pairs ready for an `http::HeaderMap`.
/// The distributed crate converts these to `HeaderMap` at the
/// boundary; this fn stays http-dep-free so the core crate doesn't
/// pull `http`.
///
/// Filtering rules:
/// - blooms whose hex value would exceed [`FLIGHT_BLOOM_MAX_HEADER_HEX`]
///   are dropped (caller should ship them via the proto path)
/// - once cumulative hex bytes pass [`FLIGHT_BLOOM_MAX_TOTAL_HEX`],
///   remaining blooms are dropped (best-effort partial set)
///
/// The returned vec preserves input order so callers can detect which
/// inputs got dropped (output.len() < input.len()).
pub fn blooms_to_header_pairs(blooms: &[(String, &BloomFilter)]) -> Vec<(String, String)> {
    let mut out = Vec::with_capacity(blooms.len());
    let mut total = 0usize;
    for (uuid, bloom) in blooms {
        let header = bloom.to_flight_header();
        if header.len() > FLIGHT_BLOOM_MAX_HEADER_HEX {
            continue; // too big for header transport
        }
        if total + header.len() > FLIGHT_BLOOM_MAX_TOTAL_HEX {
            break; // remaining blooms would blow the request limit
        }
        let name = format!("{FLIGHT_BLOOM_HEADER_PREFIX}{uuid}");
        total += header.len();
        out.push((name, header));
    }
    out
}

/// Σ.J.2.b.v — server-side: extract blooms from inbound header pairs.
/// Iterates any iterator of `(header_name, header_value)` so the
/// distributed crate can pass `http::HeaderMap::iter()` (with values
/// converted to `&str`) without depending on its concrete type here.
///
/// Returns one (column_uuid, BloomFilter) per parseable matching
/// header. Headers without the `x-ematix-bloom-` prefix are skipped.
/// Malformed bloom values are dropped (logged via tracing if the
/// feature is enabled).
pub fn header_pairs_to_blooms<'a, I>(headers: I) -> Vec<(String, BloomFilter)>
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    let mut out = Vec::new();
    for (name, value) in headers {
        // HTTP/2 lowercases header names; tonic's MetadataMap is
        // case-insensitive on read. Lowercase the prefix check to be
        // safe across both transports.
        if name.len() < FLIGHT_BLOOM_HEADER_PREFIX.len() {
            continue;
        }
        let (head, tail) = name.split_at(FLIGHT_BLOOM_HEADER_PREFIX.len());
        if !head.eq_ignore_ascii_case(FLIGHT_BLOOM_HEADER_PREFIX) {
            continue;
        }
        match BloomFilter::from_flight_header(value) {
            Ok(bloom) => out.push((tail.to_string(), bloom)),
            Err(_) => continue, // skip malformed; better than failing the query
        }
    }
    out
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BloomError {
    TooSmall,
    BadMagic,
    LengthMismatch,
}

impl std::fmt::Display for BloomError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for BloomError {}

/// Σ.J.2 — builder accumulating join keys on the build side. Wraps a
/// BloomFilter; finalise to ship.
pub struct BloomBuilder {
    bloom: BloomFilter,
    n_inserted: usize,
}

impl BloomBuilder {
    pub fn for_keys(expected_keys: usize) -> Self {
        Self {
            bloom: BloomFilter::for_keys(expected_keys),
            n_inserted: 0,
        }
    }

    pub fn insert_i64(&mut self, v: i64) {
        self.bloom.insert_i64(v);
        self.n_inserted += 1;
    }
    pub fn insert_str(&mut self, s: &str) {
        self.bloom.insert_str(s);
        self.n_inserted += 1;
    }

    /// Σ.J.2 — bulk insert from an Arrow Int64Array column. Skips
    /// nulls (they can't match join keys anyway).
    pub fn insert_int64_array(&mut self, arr: &dyn Array) {
        if let Some(a) = arr.as_primitive_opt::<Int64Type>() {
            for i in 0..a.len() {
                if !a.is_null(i) {
                    self.insert_i64(a.value(i));
                }
            }
        }
    }

    pub fn into_bloom(self) -> BloomFilter {
        self.bloom
    }

    pub fn n_inserted(&self) -> usize {
        self.n_inserted
    }
}

// ---------------------------------------------------------------------
// Σ.J.2.b.iv — BloomFilterExec wrapper.
// ---------------------------------------------------------------------

use std::any::Any;
use std::sync::Arc;
use datafusion::arrow::array::{BooleanArray, Int64Array, RecordBatch};
use datafusion::arrow::compute::filter_record_batch;
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use futures_util::StreamExt;

/// Σ.J.2.b.iv — wraps a child ExecutionPlan; drops rows whose value
/// in `key_col_idx` doesn't pass the bloom filter. Used on the
/// probe side of a distributed join — the build-side bloom arrives
/// via Flight metadata (Σ.J.2.b) and is installed around the probe
/// scan.
///
/// Today supports Int64 join keys (covers most TPC-H join shapes:
/// l_partkey, l_orderkey, c_custkey, etc.). String keys + bloom
/// composition is a follow-up.
#[derive(Debug)]
pub struct BloomFilterExec {
    input: Arc<dyn ExecutionPlan>,
    key_col_idx: usize,
    bloom: Arc<BloomFilter>,
    properties: Arc<PlanProperties>,
}

impl BloomFilterExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        key_col_idx: usize,
        bloom: Arc<BloomFilter>,
    ) -> DfResult<Self> {
        let schema = input.schema();
        if key_col_idx >= schema.fields().len() {
            return Err(DataFusionError::Internal(format!(
                "BloomFilterExec: key_col_idx={key_col_idx} out of bounds"
            )));
        }
        if schema.field(key_col_idx).data_type() != &arrow_schema::DataType::Int64 {
            return Err(DataFusionError::Internal(format!(
                "BloomFilterExec: key column must be Int64, got {:?}",
                schema.field(key_col_idx).data_type()
            )));
        }
        let eq_props = EquivalenceProperties::new(schema.clone());
        let n_parts = input.properties().partitioning.partition_count();
        let properties = Arc::new(PlanProperties::new(
            eq_props,
            Partitioning::UnknownPartitioning(n_parts),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            key_col_idx,
            bloom,
            properties,
        })
    }

    pub fn bloom(&self) -> &Arc<BloomFilter> {
        &self.bloom
    }
}

impl DisplayAs for BloomFilterExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "BloomFilterExec(key_col_idx={}, bloom_blocks={})",
            self.key_col_idx, self.bloom.n_blocks
        )
    }
}

impl ExecutionPlan for BloomFilterExec {
    fn name(&self) -> &str {
        "BloomFilterExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let new_input = children.pop().ok_or_else(|| {
            DataFusionError::Internal("BloomFilterExec requires exactly 1 child".into())
        })?;
        Ok(Arc::new(Self::try_new(
            new_input,
            self.key_col_idx,
            self.bloom.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        let key_col_idx = self.key_col_idx;
        let bloom = self.bloom.clone();
        let schema = self.input.schema();
        let schema_clone = schema.clone();

        let filtered = input_stream.map(move |batch_result| {
            let batch = batch_result?;
            let key_arr = batch
                .column(key_col_idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "BloomFilterExec: column {key_col_idx} not Int64Array"
                    ))
                })?;
            // Build boolean mask: true if bloom might-contain the key.
            let mut mask = Vec::with_capacity(batch.num_rows());
            for i in 0..batch.num_rows() {
                let pass = if key_arr.is_null(i) {
                    false
                } else {
                    bloom.might_contain_i64(key_arr.value(i))
                };
                mask.push(pass);
            }
            let bool_arr = BooleanArray::from(mask);
            let out = filter_record_batch(&batch, &bool_arr)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
            DfResult::Ok(out)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema_clone,
            filtered,
        )))
    }
}

fn hash_i64(v: i64, seed: u64) -> u64 {
    // splitmix64
    let mut x = (v as u64).wrapping_add(seed);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

fn hex_val(b: u8) -> Result<u8, BloomError> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => Err(BloomError::BadMagic),
    }
}

/// FNV-1a 64-bit — deterministic, cheap, good enough for bloom.
fn hash_bytes(b: &[u8], seed: u64) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325 ^ seed;
    for &byte in b {
        h ^= byte as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    // Splitmix finalizer for better dispersion.
    h = (h ^ (h >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    h = (h ^ (h >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    h ^ (h >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_lookup_no_false_negatives() {
        let mut b = BloomFilter::for_keys(1000);
        for i in 0..1000i64 {
            b.insert_i64(i);
        }
        // No false negatives — every inserted key must be found.
        for i in 0..1000i64 {
            assert!(b.might_contain_i64(i), "missing {i}");
        }
    }

    #[test]
    fn false_positive_rate_within_spec() {
        let n = 10_000usize;
        let mut b = BloomFilter::for_keys(n);
        for i in 0..n as i64 {
            b.insert_i64(i);
        }
        // Probe with N values we did NOT insert. FPR should be ~1%.
        let mut fp = 0;
        let probes = 10_000;
        for i in (n as i64)..(n as i64 + probes) {
            if b.might_contain_i64(i) {
                fp += 1;
            }
        }
        let rate = fp as f64 / probes as f64;
        // Theoretical FPR for 10 bits/key + k=8 is ~0.46% but
        // split-block doubles it. Spec: ≤3%.
        assert!(rate < 0.03, "FPR {rate} too high");
    }

    #[test]
    fn string_keys_work() {
        let mut b = BloomFilter::for_keys(100);
        for s in &["AIR", "MAIL", "SHIP", "RAIL", "FOB", "TRUCK", "REG AIR"] {
            b.insert_str(s);
        }
        for s in &["AIR", "MAIL", "SHIP", "RAIL", "FOB", "TRUCK", "REG AIR"] {
            assert!(b.might_contain_str(s));
        }
        assert!(!b.might_contain_str("NOPE_NOT_THERE_BOGUS"));
    }

    #[test]
    fn serialise_round_trip() {
        let mut b = BloomFilter::for_keys(1000);
        for i in 0..1000i64 {
            b.insert_i64(i);
        }
        let bytes = b.to_bytes();
        let b2 = BloomFilter::from_bytes(&bytes).unwrap();
        // After round trip, all keys still match.
        for i in 0..1000i64 {
            assert!(b2.might_contain_i64(i));
        }
        // Bit patterns identical.
        assert_eq!(b.bits, b2.bits);
        assert_eq!(b.n_blocks, b2.n_blocks);
        assert_eq!(b.seed, b2.seed);
    }

    #[test]
    fn serialise_rejects_bad_magic() {
        let mut bytes = vec![0u8; 24];
        bytes[0..8].copy_from_slice(b"NOTBLOOM");
        match BloomFilter::from_bytes(&bytes) {
            Err(BloomError::BadMagic) => {}
            other => panic!("expected BadMagic, got {other:?}"),
        }
    }

    #[test]
    fn wire_size_reasonable_for_typical_join() {
        // ~25K customer keys at 10 bits/key = ~31 KB.
        let b = BloomFilter::for_keys(25_000);
        // Allow ±50% slop from block alignment.
        assert!(b.wire_size() >= 25_000);
        assert!(b.wire_size() <= 50_000);
    }

    #[test]
    fn flight_header_round_trip() {
        let mut b = BloomFilter::for_keys(100);
        for i in 0..100i64 {
            b.insert_i64(i);
        }
        let header = b.to_flight_header();
        // Hex-only, lowercase.
        assert!(header.chars().all(|c| matches!(c, '0'..='9' | 'a'..='f')));
        let b2 = BloomFilter::from_flight_header(&header).unwrap();
        for i in 0..100i64 {
            assert!(b2.might_contain_i64(i));
        }
    }

    #[test]
    fn flight_header_rejects_bad_hex() {
        let r = BloomFilter::from_flight_header("zzznotvalidhex");
        assert!(matches!(r, Err(BloomError::BadMagic)));
    }

    #[test]
    fn flight_header_rejects_odd_length() {
        let r = BloomFilter::from_flight_header("abc");
        assert!(matches!(r, Err(BloomError::BadMagic)));
    }

    #[tokio::test]
    async fn bloom_filter_exec_drops_rows_not_in_bloom() {
        use arrow_array::{Int64Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};
        use datafusion::datasource::MemTable;
        use datafusion::prelude::SessionContext;
        use datafusion::physical_plan::ExecutionPlanProperties;
        use futures_util::TryStreamExt;
        use std::sync::Arc;

        // Build a bloom containing only keys {1, 2, 3}.
        let mut bloom = BloomFilter::for_keys(10);
        for k in [1i64, 2, 3] {
            bloom.insert_i64(k);
        }

        // Input table has keys 1..=10. We expect output to retain
        // {1, 2, 3} plus possibly some false positives — and definitely
        // exclude many of 4..=10.
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from((1..=10i64).collect::<Vec<_>>()))],
        )
        .unwrap();
        let mt = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(mt)).unwrap();

        let df = ctx.sql("SELECT k FROM t").await.unwrap();
        let scan = df.create_physical_plan().await.unwrap();
        let bfe = BloomFilterExec::try_new(scan, 0, Arc::new(bloom)).unwrap();

        let mut s = bfe.execute(0, ctx.task_ctx()).unwrap();
        let mut kept: Vec<i64> = Vec::new();
        while let Some(b) = s.try_next().await.unwrap() {
            let arr = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..b.num_rows() {
                kept.push(arr.value(i));
            }
        }
        // Must include all true positives.
        for k in [1, 2, 3] {
            assert!(kept.contains(&k), "missing inserted key {k}");
        }
        // Must drop some false negatives (rows not in bloom that
        // bloom correctly identified as absent). At least one of
        // 4..=10 should NOT be in `kept` — for a 10-key bloom with
        // 3 inserts, expected FP rate is ≪ 1.
        let dropped = (4..=10i64).filter(|k| !kept.contains(k)).count();
        assert!(
            dropped > 0,
            "BloomFilterExec didn't drop any non-bloom keys (kept all 4..=10)"
        );
    }

    // ---- Σ.J.2.b.v — HeaderMap marshalling ----

    #[test]
    fn header_pairs_roundtrip() {
        let mut a = BloomFilter::for_keys(100);
        a.insert_i64(1);
        a.insert_i64(2);
        let mut b = BloomFilter::for_keys(100);
        b.insert_i64(100);

        let inputs: Vec<(String, &BloomFilter)> =
            vec![("join0_left".into(), &a), ("join1_left".into(), &b)];
        let pairs = blooms_to_header_pairs(&inputs);
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0].0, "x-ematix-bloom-join0_left");
        assert_eq!(pairs[1].0, "x-ematix-bloom-join1_left");

        // Decode the pairs as a server would.
        let decoded = header_pairs_to_blooms(
            pairs.iter().map(|(n, v)| (n.as_str(), v.as_str())),
        );
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].0, "join0_left");
        assert_eq!(decoded[1].0, "join1_left");
        // Verify the BloomFilter content survived.
        assert!(decoded[0].1.might_contain_i64(1));
        assert!(decoded[0].1.might_contain_i64(2));
        assert!(decoded[1].1.might_contain_i64(100));
    }

    #[test]
    fn header_pairs_drop_oversized() {
        // A 100K-key bloom is ~120 KB → ~240 KB hex, way over the
        // 6 KiB per-header limit.
        let big = BloomFilter::for_keys(100_000);
        let small = BloomFilter::for_keys(50);
        let inputs: Vec<(String, &BloomFilter)> = vec![
            ("oversized".into(), &big),
            ("ok".into(), &small),
        ];
        let pairs = blooms_to_header_pairs(&inputs);
        // big is dropped; small survives.
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].0, "x-ematix-bloom-ok");
    }

    #[test]
    fn header_pairs_caseinsensitive_prefix() {
        let mut b = BloomFilter::for_keys(50);
        b.insert_i64(42);
        let pairs = blooms_to_header_pairs(&[("test".into(), &b)]);
        // Server-side: simulate a case-mangled prefix (HTTP/2 should
        // lowercase, but be defensive).
        let mangled = format!("X-Ematix-Bloom-{}", "test");
        let decoded =
            header_pairs_to_blooms(vec![(mangled.as_str(), pairs[0].1.as_str())]);
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].0, "test");
        assert!(decoded[0].1.might_contain_i64(42));
    }

    #[test]
    fn header_pairs_skip_non_bloom_headers() {
        let mut b = BloomFilter::for_keys(50);
        b.insert_i64(7);
        let bloom_hdr = b.to_flight_header();
        let decoded = header_pairs_to_blooms(vec![
            ("content-type", "application/grpc"),
            ("x-ematix-bloom-join0", bloom_hdr.as_str()),
            ("traceparent", "00-aaa-bbb-01"),
        ]);
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].0, "join0");
    }

    #[test]
    fn header_pairs_skip_malformed() {
        let decoded = header_pairs_to_blooms(vec![
            ("x-ematix-bloom-broken", "not-valid-hex-zz"),
            ("x-ematix-bloom-short", "abcd"),
        ]);
        assert!(decoded.is_empty(), "malformed blooms should be dropped");
    }

    #[test]
    fn builder_bulk_insert_from_arrow() {
        use arrow_array::Int64Array;
        let mut b = BloomBuilder::for_keys(100);
        let arr = Int64Array::from(vec![Some(10), Some(20), None, Some(30)]);
        b.insert_int64_array(&arr);
        let bloom = b.into_bloom();
        assert!(bloom.might_contain_i64(10));
        assert!(bloom.might_contain_i64(20));
        assert!(bloom.might_contain_i64(30));
        // 40 was never inserted — should miss (with FPR caveat).
        // For only 3 keys in a 256-bit block, FPR is ~0; this should
        // reliably miss.
        assert!(!bloom.might_contain_i64(99_999_999));
    }
}
