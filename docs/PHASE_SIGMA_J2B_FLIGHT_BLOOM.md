# Σ.J.2.b — Flight metadata propagation for cross-stage bloom

## What ships in this bite

Three artefacts:

1. **`BloomFilter::to_flight_header` / `BloomFilter::from_flight_header`**
   in `crates/ematix-flow-core/src/bloom.rs` — base64-encoded bloom
   bytes for `tonic::metadata::MetadataMap` (Flight's gRPC headers).
2. **`crates/ematix-flow-distributed/src/bloom_flight.rs`** — helpers
   that attach a `BloomFilter` to a Flight call's request headers
   and read it back on the server side.
3. **This design doc** — explains both the HTTP-header path landing
   tonight (works against today's datafusion-distributed without
   upstream changes) and the cleaner upstream-proto path that's
   Σ.J.2.c.

## Why HTTP headers (not the proto's `flight_app_metadata`)

`datafusion-distributed` already has a `flight_app_metadata` protobuf
on the FlightData stream — it's how `MetricsCollection` rides on the
last message of each partition. A clean bloom-propagation design
would add a new variant:

```proto
message FlightAppMetadata {
  oneof content {
    MetricsCollection metrics_collection = 1;
    BloomFilterPayload bloom = 2;   // <-- new
  }
}
```

But this is **upstream work** — we'd need a PR against
`datafusion-distributed`'s proto, a release cycle, a workspace bump.
Not blocking for tomorrow's bench.

The HTTP-header path (gRPC `tonic::metadata::MetadataMap`) gives us
the same propagation TODAY:

- Bloom is base64-encoded into a custom header `x-ematix-bloom-<id>`.
- The id keys multiple blooms (build-side ships one per join column).
- Client attaches before `DoGet`; server inspects on request arrival.
- Backward-compatible: a peer that doesn't understand the header
  ignores it (correct fallback — no bloom, more rows ship; merely
  slower, not broken).

## Wire format

```text
x-ematix-bloom-<column_uuid>:
  <base64 of BloomFilter::to_bytes()>
```

- One header per (join, build_column).
- `column_uuid` is computed from the join's physical-plan node id +
  column index — same id space both sides agree on.
- Max header value is gRPC-limited to ~8 KiB; bloom is ≤ 50 KiB for
  typical TPC-H build sides. **Constraint**: blooms bigger than 8 KiB
  must use the proto path. For TPC-H SF=1 a 25K-key build (customer)
  produces ~31 KB → needs chunking or proto. **Practical scope of
  Σ.J.2.b**: only emit header-path blooms when `expected_keys ≤ 5000`
  (5K * 10 bits = 6 KB ≤ header limit). Larger build sides defer to
  Σ.J.2.c (proto path).

## What probe-side does with it

In the probe-side worker's flight handler, after constructing the
ExecutionPlan but before streaming rows:

1. Inspect the request `MetadataMap` for headers matching
   `x-ematix-bloom-*`.
2. For each, decode to `BloomFilter`.
3. Wrap the relevant scan(s) in a `BloomFilterExec` that drops rows
   whose join key doesn't pass the bloom — this happens BEFORE the
   shuffle write, so the saved rows never hit the wire.
4. Rows that do pass continue downstream as normal.

`BloomFilterExec` is a tiny ExecutionPlan that, per batch, evaluates
the bloom against the join key column and filters using
`arrow::compute::filter`. ~80 LOC.

## What lands tonight (Σ.J.2.b)

Just (1) and (2) — the marshalling. The probe-side wrapper exec is
Σ.J.2.b.iv (next bite). Even just the marshalling is testable
end-to-end via in-process round trip.

## Σ.J.2.c — upstream proto path (when we need >8KB blooms)

For build sides exceeding 5K keys, we need:

1. PR to `datafusion-distributed`:
   ```proto
   message BloomFilterPayload {
     string column_uuid = 1;
     bytes  bloom_bytes = 2;
   }
   ```
   added as a `flight_app_metadata.content` variant.
2. Our handler emits it on the first FlightData of each shuffle
   partition (the same place metrics ride on the last).
3. Probe-side reads it in the existing message inspection loop
   already inside `network_shuffle.rs` etc.

Effort once upstream lands: ~1 day of integration.

## Risks

- **HTTP header path is hacky.** Acknowledged. It's a stepping
  stone — proves the lever works, lets us ship before upstream.
- **8 KiB limit excludes ~25-50K key build sides.** Customer +
  supplier + nation in TPC-H are under 5K each at SF=1; lineitem
  joins on l_orderkey (1.5M build → bloom won't fit). For these,
  Σ.J.2.b doesn't help; need Σ.J.2.c.
- **Bloom probe overhead on probe side.** For each row, 8 hash
  computes + memory lookup. ~1ns/row on modern CPUs → ~6ms for 6M
  lineitem rows. Wins only when ≥10% of rows can be skipped before
  shuffle.

## Expected wins on tomorrow's distributed bench

Where bloom-on-build helps in TPC-H:

- **Q05** (customer ⋈ orders ⋈ lineitem ⋈ supplier ⋈ nation ⋈ region):
  several small build sides. Bloom on `n_regionkey` and `r_regionkey`
  saves ~80% of orders/lineitem rows before shuffle.
- **Q07** (similar shape).
- **Q08** (similar shape, plus nation-restricted).
- **Q10** (customer ⋈ orders ⋈ lineitem ⋈ nation).

Estimated wall-time win on these: **20-40% distributed-only,
0% single-node**. Single-node hits the parallel-AND path that already
filters efficiently; distributed wins because it saves shuffle bytes.

## Sequencing

- Σ.J.2 — kernel + serialisation (done)
- Σ.J.2.b — header marshalling + tests (this bite)
- Σ.J.2.b.iv — probe-side `BloomFilterExec` wrapper (next)
- Σ.J.2.c — upstream proto for blooms > 8 KiB (upstream cycle)
