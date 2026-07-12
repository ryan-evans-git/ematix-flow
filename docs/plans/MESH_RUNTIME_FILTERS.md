# Σ.MG.2 — Runtime filters for the mesh (plan-embedded bloom transport)

## Problem

Distributed plans register tables through arrow-rs listings so the
stage splitter can shard them — so they carry none of the single-node
fast path's runtime-bloom pruning. Measured cost (SF=100, 4×
c7i.4xlarge, 2026-07-11): Q07 mesh 5.9 s vs single-node 3.1 s; the
mesh shuffles ~600 M unpruned lineitem rows that the single node
blooms down to ~8 M at scan time. Trino (dynamic filtering) and Spark
(AQE runtime bloom/DPP) both do this by default — at SF=1000 it is
their best catch-up vector against us.

## What exists (Σ.J.2.b, all merged)

- `bloom_emitter` — coordinator-side: walks the optimized logical
  plan, pre-executes small build sides (LIMIT-clamped at
  `max_build_rows`, default 50 K), emits `(probe_table.col →
  BloomFilter)`. Σ.MG extended eligibility (2026-07-11): BOTH join
  orientations, and probe-side descent through Inner joins /
  LeftSemi left sides — TPC-H candidates went from {Q17,Q18,Q19}
  to 12 of 22 queries incl. Q07 (2) and Q21 (1).
- `bloom_flight` — HeaderMap ⇄ blooms; ships via
  `set_distributed_passthrough_headers`.
- `flow-worker` — `default_bloom_session_builder()` already decodes
  inbound `x-ematix-bloom-*` headers and installs
  `EnableContextBloomRule` per request. Deployed workers are ready.

## Why headers can't carry the blooms that matter

gRPC metadata is limited to ~8–16 KB. A useful bloom at SF=100 is
~80 K keys (Q07's nation-filtered supplier ≈ 120 KB at 12 bits/key);
at SF=1000, ~800 K keys ≈ 1.2 MB. Chunked headers and config-extension
propagation both ride the same HTTP/2 header-list limit. The
`max_build_rows=50K` cap is what keeps today's header path honest —
and also what keeps SF≥100 blooms from shipping at all.

## Design: embed blooms in the serialized stage plan

datafusion-distributed supports user codecs
(`with_distributed_user_codec`, both coordinator and worker session
builders). Stage plan payloads are protobuf messages with MB-scale
limits — the right vehicle.

1. **Codec**: `PhysicalExtensionCodec` for
   `ematix_flow_core::bloom::BloomFilterExec` — encode `{bloom bytes,
   probe column}`; decode reconstructs the exec around the child.
2. **Coordinator wiring** (per query, async, before physical
   planning): emit blooms from the optimized logical plan, then wrap
   the matching arrow scans with `BloomFilterExec` — either via a
   physical rule reading a per-query slot installed before the mesh
   gate, or by applying `EnableContextBloomRule` explicitly to the
   pre-split plan. The wrap must happen BEFORE the stage splitter so
   the exec lands inside worker stages.
3. **Worker**: register the codec in `flow-worker`'s session builder
   (one line next to `default_bloom_session_builder`). Workers must
   roll BEFORE coordinators start emitting plan-embedded blooms —
   version-gate with a capability header or a release-note ordering.
4. **Cap**: raise `max_build_rows` for the codec path (bloom size is
   then bounded by proto message limits, not headers); keep the 50 K
   cap for the header path.

## Expected effect

Mesh Q07 → ~single-node parity (pruned before shuffle); Q21's inner
arm blooms l1 (semi/anti arms remain — see the Σ.MG self-join veto,
which stays as the backstop, capacity-gated for SF≥1000). At SF=1000
this is the difference between shuffling ~6 B rows and ~500 M on the
bloom-heavy queries.

## Status

- Eligibility + both orientations: **merged** (#197), oracles in
  `bloom_emitter::tests`.
- Codec + coordinator wiring + worker registration: **implemented**
  (2026-07-11 late): `bloom_codec.rs` (`BloomExecCodec` +
  take-once `BloomSlot` + `EmbeddedBloomRule`),
  `EnableContextBloomRule` grew the arrow-scan arm (+ part-suffix
  stripping), worker session builder registers the codec
  unconditionally, campaign + `DistributedBackend` emit + arm per
  query — INSIDE the trial timer (the bloom build is measured work,
  same as Trino/Spark pay for dynamic filters).
  `EMAT_MESH_BLOOM_SHIP` tri-state (default ON),
  `EMAT_MESH_BLOOM_MAX_KEYS` (default 1M keys ≈ 1.5 MB bloom,
  under proto message limits).
- Remaining: box-scale validation (mesh Q07 expected → ~3 s), then
  the SF=1000 campaign measures the real payoff.
