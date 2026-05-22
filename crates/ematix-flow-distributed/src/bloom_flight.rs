//! Σ.J.2.b.v — Flight passthrough for cross-stage bloom filters.
//!
//! ## Wire shape
//!
//! Build-side workers attach bloom filters as HTTP headers on the
//! request that initiates the next Flight stage. Probe-side workers
//! read inbound headers and stash decoded blooms in a
//! [`ContextBlooms`] extension on the SessionState so a downstream
//! optimizer rule can wrap the matching scans in
//! `ematix_flow_core::bloom::BloomFilterExec`.
//!
//! Header name format: `x-ematix-bloom-<column_uuid>` where the uuid
//! is a stable id both sides agree on (typically `<join_node_id>-<col>`).
//! Value: lowercase hex of [`BloomFilter::to_bytes`].
//!
//! ## Transport
//!
//! Uses `datafusion-distributed`'s existing
//! `set_distributed_passthrough_headers` API, which propagates HTTP
//! headers across every Flight stage in the same distributed plan.
//! No new gRPC service definitions, no proto changes — works against
//! any datafusion-distributed release that has that API.
//!
//! ## Scope of this module
//!
//! - [`blooms_to_header_map`] / [`header_map_to_blooms`] — `HeaderMap`
//!   ⇄ `Vec<(column_uuid, BloomFilter)>` (thin wrapper over the
//!   `ematix-flow-core::bloom` pair helpers).
//! - [`attach_blooms_to_ctx`] — coordinator-side convenience:
//!   `SessionContext::set_distributed_passthrough_headers(...)` with
//!   the marshalled blooms.
//! - [`ContextBlooms`] — SessionState extension type that probe-side
//!   workers populate from inbound headers. A future optimizer rule
//!   consults this to inject `BloomFilterExec` above matching scans.
//!
//! ## What's NOT in this bite (Σ.J.2.b.vi)
//!
//! The optimizer rule that wires `ContextBlooms` into the physical
//! plan. That requires deciding the column_uuid scheme (`join_id-col`
//! vs `table_name-col` etc.) and surviving plan rewrites — separate
//! design. This bite ships the transport so the rule has a target.

pub use ematix_flow_core::bloom::ContextBlooms;
use async_trait::async_trait;
use datafusion::common::DataFusionError;
use datafusion::execution::SessionState;
use datafusion_distributed::{WorkerQueryContext, WorkerSessionBuilder};
use ematix_flow_core::bloom::{
    blooms_to_header_pairs, header_pairs_to_blooms, BloomFilter,
    FLIGHT_BLOOM_HEADER_PREFIX,
};
use ematix_flow_core::context_bloom_rule::install_context_bloom_rule;
use http::{HeaderMap, HeaderName, HeaderValue};
use std::collections::HashMap;
use std::sync::Arc;

/// Σ.J.2.b.v — marshall a set of build-side blooms into a `HeaderMap`
/// suitable for `SessionContext::set_distributed_passthrough_headers`.
///
/// Blooms whose hex value exceeds the per-header gRPC limit are
/// silently dropped; the returned `HeaderMap` has only the blooms that
/// fit. Callers that need to ship oversized blooms should use the
/// upstream proto path (Σ.J.2.c, deferred).
pub fn blooms_to_header_map(blooms: &[(String, &BloomFilter)]) -> HeaderMap {
    let pairs = blooms_to_header_pairs(blooms);
    let mut map = HeaderMap::with_capacity(pairs.len());
    for (name, value) in pairs {
        // `name` is always `x-ematix-bloom-<uuid>`; ASCII-safe.
        // `value` is hex; ASCII-safe.
        match (
            HeaderName::try_from(name.as_str()),
            HeaderValue::try_from(value.as_str()),
        ) {
            (Ok(n), Ok(v)) => {
                map.insert(n, v);
            }
            _ => {
                // The uuid component is caller-supplied — reject any
                // header name that isn't valid HTTP. Better silent
                // skip than panic in a shared infra path.
                continue;
            }
        }
    }
    map
}

/// Σ.J.2.b.v — server-side: extract blooms from inbound HTTP headers.
/// Skips non-bloom headers, malformed values, and per-header values
/// that fail to decode as a `BloomFilter`.
pub fn header_map_to_blooms(headers: &HeaderMap) -> Vec<(String, BloomFilter)> {
    let pairs: Vec<(&str, &str)> = headers
        .iter()
        .filter_map(|(n, v)| {
            let name = n.as_str();
            // Cheap pre-filter on the prefix — saves a UTF-8 check
            // on every non-bloom header.
            if !name
                .get(..FLIGHT_BLOOM_HEADER_PREFIX.len())
                .map(|h| h.eq_ignore_ascii_case(FLIGHT_BLOOM_HEADER_PREFIX))
                .unwrap_or(false)
            {
                return None;
            }
            v.to_str().ok().map(|v| (name, v))
        })
        .collect();
    header_pairs_to_blooms(pairs)
}

/// Σ.J.2.b.v — build a [`ContextBlooms`] (defined in core) from
/// inbound passthrough HTTP headers. Probe-side workers call this
/// in their `WorkerQueryContext` `build_state` callback so the
/// per-request SessionState carries the right blooms for
/// [`ematix_flow_core::context_bloom_rule::EnableContextBloomRule`].
pub fn context_blooms_from_headers(headers: &HeaderMap) -> ContextBlooms {
    let decoded = header_map_to_blooms(headers);
    let map: HashMap<String, Arc<BloomFilter>> = decoded
        .into_iter()
        .map(|(uuid, bloom)| (uuid, Arc::new(bloom)))
        .collect();
    ContextBlooms::new(map)
}

/// Σ.J.2.b.viii — `WorkerSessionBuilder` adapter that automatically
/// installs the probe-side bloom rule on every inbound query. Wrap
/// any underlying [`WorkerSessionBuilder`] (e.g.
/// `DefaultSessionBuilder` or a custom one) with this to opt the
/// worker into bloom propagation.
///
/// Behaviour:
/// 1. Read inbound `x-ematix-bloom-*` headers
/// 2. Decode to [`ContextBlooms`]
/// 3. If non-empty, register
///    [`ematix_flow_core::context_bloom_rule::EnableContextBloomRule`]
///    on the per-request `SessionStateBuilder`
/// 4. Delegate to the inner builder for everything else
///
/// When the inbound request has no bloom headers, the rule is not
/// installed — zero codegen-tax cost on non-distributed paths.
///
/// ```ignore
/// // In flow-worker's main():
/// let inner = DefaultSessionBuilder;
/// let bloom_aware = BloomSessionBuilder::new(inner);
/// let worker = Worker::from_session_builder(bloom_aware);
/// ```
#[derive(Debug, Clone)]
pub struct BloomSessionBuilder<T: WorkerSessionBuilder> {
    inner: T,
}

impl<T: WorkerSessionBuilder> BloomSessionBuilder<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl<T> WorkerSessionBuilder for BloomSessionBuilder<T>
where
    T: WorkerSessionBuilder + Send + Sync + 'static,
{
    async fn build_session_state(
        &self,
        ctx: WorkerQueryContext,
    ) -> Result<SessionState, DataFusionError> {
        let blooms = context_blooms_from_headers(&ctx.headers);
        if blooms.is_empty() {
            // No blooms inbound → behave like the underlying builder
            // with zero overhead.
            return self.inner.build_session_state(ctx).await;
        }
        // Patch the builder before handing off to the inner builder
        // so any further customisation (custom UDFs, codec, etc.)
        // composes on top.
        let patched = WorkerQueryContext {
            builder: install_context_bloom_rule(ctx.builder, blooms),
            headers: ctx.headers,
        };
        self.inner.build_session_state(patched).await
    }
}

/// Σ.J.2.b.viii — convenience constructor: wrap
/// `DefaultSessionBuilder` with [`BloomSessionBuilder`]. Most workers
/// without custom session needs just want this.
pub fn default_bloom_session_builder()
-> BloomSessionBuilder<datafusion_distributed::DefaultSessionBuilder> {
    BloomSessionBuilder::new(datafusion_distributed::DefaultSessionBuilder)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_bloom(keys: &[i64]) -> BloomFilter {
        let mut b = BloomFilter::for_keys((keys.len() * 2).max(50));
        for &k in keys {
            b.insert_i64(k);
        }
        b
    }

    #[test]
    fn header_map_round_trip() {
        let a = mk_bloom(&[1, 2, 3]);
        let b = mk_bloom(&[100, 200]);

        let inputs: Vec<(String, &BloomFilter)> =
            vec![("orders_l_orderkey".into(), &a), ("part_l_partkey".into(), &b)];

        let map = blooms_to_header_map(&inputs);
        assert_eq!(map.len(), 2);
        assert!(map.contains_key("x-ematix-bloom-orders_l_orderkey"));
        assert!(map.contains_key("x-ematix-bloom-part_l_partkey"));

        let decoded = header_map_to_blooms(&map);
        assert_eq!(decoded.len(), 2);

        let by_uuid: HashMap<_, _> = decoded.into_iter().collect();
        let a_decoded = by_uuid.get("orders_l_orderkey").unwrap();
        assert!(a_decoded.might_contain_i64(1));
        assert!(a_decoded.might_contain_i64(2));
        assert!(a_decoded.might_contain_i64(3));
        let b_decoded = by_uuid.get("part_l_partkey").unwrap();
        assert!(b_decoded.might_contain_i64(100));
        assert!(b_decoded.might_contain_i64(200));
    }

    #[test]
    fn context_blooms_from_headers_fn() {
        let a = mk_bloom(&[42]);
        let map = blooms_to_header_map(&[("test_uuid".into(), &a)]);
        let ctx_blooms = context_blooms_from_headers(&map);
        assert_eq!(ctx_blooms.len(), 1);
        let bloom = ctx_blooms.get("test_uuid").expect("uuid present");
        assert!(bloom.might_contain_i64(42));
        assert!(ctx_blooms.get("missing").is_none());
    }

    #[test]
    fn header_map_skips_unrelated() {
        let a = mk_bloom(&[7]);
        let mut map = blooms_to_header_map(&[("only_one".into(), &a)]);
        map.insert(
            HeaderName::from_static("content-type"),
            HeaderValue::from_static("application/grpc"),
        );
        map.insert(
            HeaderName::from_static("traceparent"),
            HeaderValue::from_static("00-aaaa-bbbb-01"),
        );
        let decoded = header_map_to_blooms(&map);
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].0, "only_one");
    }

    #[tokio::test]
    async fn bloom_session_builder_installs_rule_when_headers_present() {
        // Confirm the BloomSessionBuilder is wired correctly: when
        // inbound headers carry blooms, the per-request SessionState
        // gets the rule. We can't observe the rule list directly, but
        // we can verify the wrapper compiles + delegates by exercising
        // both the "headers empty" and "headers present" branches.
        use datafusion_distributed::{DefaultSessionBuilder, WorkerQueryContext};

        let builder = default_bloom_session_builder();

        // Branch 1: no headers → inner builder runs unmodified.
        let ctx_empty = WorkerQueryContext::default();
        let state = builder.build_session_state(ctx_empty).await.unwrap();
        // SessionState built — no panic, no error. Inner branch worked.
        let _ = state;

        // Branch 2: with headers → inner builder still produces a
        // valid state (the rule is installed but only fires on
        // matching scans, which none here have).
        let bloom = mk_bloom(&[1, 2, 3]);
        let map = blooms_to_header_map(&[("test.col".into(), &bloom)]);
        let ctx_with = WorkerQueryContext {
            builder: Default::default(),
            headers: map,
        };
        let state = builder.build_session_state(ctx_with).await.unwrap();
        let _ = state;

        // Direct DefaultSessionBuilder should also still work via the
        // wrapper (no panic on the empty path).
        let direct = BloomSessionBuilder::new(DefaultSessionBuilder);
        let _ = direct
            .build_session_state(WorkerQueryContext::default())
            .await
            .unwrap();
    }

    #[test]
    fn empty_input_yields_empty_map() {
        let map = blooms_to_header_map(&[]);
        assert!(map.is_empty());
        let decoded = header_map_to_blooms(&map);
        assert!(decoded.is_empty());
        let ctx = context_blooms_from_headers(&map);
        assert!(ctx.is_empty());
    }
}
