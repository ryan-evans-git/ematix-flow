# Σ.D spike — distributed streaming engine selection

**Question.** Σ.D adds distributed streaming to ematix-flow. The plan
calls for a 2-week build-vs-adopt spike comparing Arroyo, RisingWave,
and a DIY path on Arrow Flight + the existing partitioned state
store. Which option (if any) makes the cross-host streaming-SQL story
worth building, given this project's constraints (no AWS, no real
cluster hardware in the runway, single-developer pace)?

**Status.** **Research-level spike, 2026-05-05.** Not a hands-on POC
build cycle — no Arroyo cluster spun up, no Denormalized integration
tested, no DIY checkpoint coordinator written. The conclusions below
are reasoned from public docs, recent landscape changes (Arroyo's
2025 Cloudflare acquisition), and the existing in-tree Phase 39
streaming infrastructure. **A full 2-week spike with code-level POCs
remains a separate engineering item** if the recommendation here
(defer) is rejected; this doc is the strategic decision artifact.

Plan: [`docs/PHASE_SIGMA_PLAN.md`](PHASE_SIGMA_PLAN.md) Σ.D.

---

## What "distributed streaming" actually unlocks

The plan's summary table (line 97 of `PHASE_SIGMA_PLAN.md`) is
explicit about the value:

> Distributed shuffle for streaming SQL. Cross-partition windowed
> joins, global aggregations, and exactly-once across re-partition
> stop bottlenecking through one node's state. Per-partition
> throughput doesn't change (that's `rdkafka` / `parquet` /
> `object_store` raw speed). What unlocks: workloads where the
> *fan-out of join keys exceeds one node's RAM*.

That's a narrow but real workload. Most production streaming jobs
fit single-node — Phase 39.4 (windows), 39.5 (sessions), and 39.5b
(stream-stream joins) already cover them. Σ.D specifically targets
the "per-key state too big for one box" case.

The existing single-node streaming surface is the floor under any
Σ.D decision: **dropping or rewriting it would be a regression**, so
whatever distributed path lands has to be additive.

---

## Candidates investigated

Four paths surveyed. The original plan listed three (Arroyo,
RisingWave, DIY); Denormalized surfaced during this spike as a
fourth — a project that didn't exist or wasn't visible when the plan
was written.

### Path 1 — Arroyo (adopt as `ArroyoBackend`)

**Original architecture (2023):** Library-first. The blog post
"We built a new SQL Engine on Arrow and DataFusion" is explicit:

> "Instead we structured the Arroyo runtime as a library, which
> would be invoked by the actual pipeline code"

Sample API from the same post:

```rust
Stream::<()>::with_parallelism(1).source(KafkaSource::new(...))
```

This is exactly the embedding shape Σ.D wants: a host process
constructs a pipeline, hands it to Arroyo's runtime, receives Arrow
batches back. `BackendConfig::Arroyo { ... }` would round-trip
cleanly.

**0.10 (early 2024):** Major rewrite. Arroyo "ships as a single,
compact binary" — deployment is `arroyo cluster` + Web UI at
:5115. The library-mode API is no longer documented or marketed. The
SQL engine was rebuilt on DataFusion + Arrow (which is favorable for
ABI alignment with ematix-flow's DataFusion 53), but the deployment
shift moved Arroyo away from "library you embed" toward "service
you integrate with."

**2025 — Cloudflare acquisition.** Arroyo was acquired by Cloudflare
and now powers Cloudflare Pipelines (serverless streaming
ingestion + transformation). The open-source repo is still public,
but the engineering focus has clearly shifted to Cloudflare-internal
priorities. Adopting Arroyo today means:

- Building against a codebase whose roadmap is set by a third party
  with unrelated commercial priorities.
- Library-mode embedding is architecturally still possible but
  outside the documented surface — you'd be reverse-engineering
  internals that may break across releases.
- Any upstream PR to restore / harden the embedding API needs
  Cloudflare's review priority.

**Verdict: not viable as a primary adoption path.** The
embeddability story regressed at exactly the wrong moment for this
project's needs. Worth keeping a watching brief; not worth committing
4–6 weeks of integration work to.

### Path 2 — RisingWave (sidecar deployment)

**Architecture:** PostgreSQL-wire-protocol streaming database. Not
embeddable. Deployment options per the README: Docker Compose, K8s
with Helm, K8s with Operator, RisingWave Cloud (managed). Quote:

> "RisingWave connects via the PostgreSQL wire protocol and works
> with psql, JDBC, and any Postgres-compatible tooling."

**Integration shape if adopted:** `RisingWaveBackend` in ematix-flow
talks to a separately-deployed RisingWave cluster over Postgres
wire. ematix-flow's role is config + orchestration; the streaming
SQL runs in RisingWave's process space.

**Tradeoffs:**

- **Operational:** Users have to run + monitor a separate
  RisingWave cluster. The "small footprint" claim ematix-flow
  makes (single binary, ~150 MB image) doesn't extend to the
  RisingWave path.
- **State boundary:** RisingWave owns its state store. ematix-flow's
  Phase 39.5 `state_store/` (in_memory + postgres) becomes
  irrelevant on this path.
- **Type system / dialect:** RisingWave's SQL surface is
  Postgres-compatible but has its own streaming-specific extensions
  (CREATE MATERIALIZED VIEW EMITTING ...). Σ.A2's dialect translator
  would need a RisingWave target.
- **Single-region only.** RisingWave's distributed state store is
  single-region; multi-region active-active isn't supported.

**Verdict: viable as an integration backend, NOT as a distributed
streaming engine for ematix-flow itself.** "Bring your own
RisingWave" is a reasonable user story for shops that already
operate one — but it doesn't satisfy the Σ.D thesis (distributed
streaming as a first-class ematix-flow capability). File under
"future connector backend" rather than "Σ.D engine."

### Path 3 — Denormalized (adopt as `DenormalizedBackend`)

**This was not in the original plan.** Surfaced via 2025-era HN
discussion: "Show HN: Denormalized – Embeddable Stream Processing
in Rust and DataFusion."

**Architecture:** Explicitly embeddable Rust library built on
DataFusion (same engine as ematix-flow). Public API supports
registering Kafka sources, applying windowed aggregations and
filters, executing the streaming pipeline programmatically. State
checkpointing via SlateDB. README quote:

> "a fast embeddable stream processing engine built on Apache
> DataFusion"

> "kafka as a real-time source and sink, windowed aggregations,
> and stream joins"

**This is the architectural shape Σ.D needs.** Same engine,
embeddable API, shared trajectory.

**The catch — production-readiness:**

- 376 stars, 172 commits, **zero releases published** as of the
  spike date.
- README is explicit: "Denormalized is _work-in-progress_ and we
  are actively seeking design partners."
- No version on crates.io (verified by absence in cached registry).
- Single small team; bus factor unknown.

**Adoption risk:**

- Pre-1.0 + zero releases is a strong "not yet" signal.
- The "actively seeking design partners" framing suggests early
  development. Adopting now means committing to a moving API,
  potentially co-maintaining patches.
- If Denormalized stalls or pivots, ematix-flow inherits the
  rewrite cost.

**Upside:** ABI-aligned with ematix-flow's DataFusion 53 / Arrow
58 stack. The integration cost would be lower than Arroyo (no
fork-and-maintain) and qualitatively lower than DIY (someone else
is solving the hard problems).

**Verdict: most architecturally promising, but not safe to
adopt yet.** The right move is a watching brief — re-evaluate when
Denormalized cuts a 1.0, or when a concrete workload in this
project's runway forces the decision earlier.

### Path 4 — DIY on Arrow Flight + existing state store

**The pieces ematix-flow already has:**

- **Single-node streaming.** Phase 39.4 (tumbling/hopping windows),
  39.5 (sessions), 39.5b (stream-stream joins), 39.5a (watermarks +
  late-data handling, dead-letter routing).
- **State store tier.** `crates/ematix-flow-core/src/state_store/`
  with in-memory + Postgres backends, Phase 39's seek/snapshot
  semantics, Phase 39.5's session-blob serialization.
- **Distributed batch executor.** Σ.B's
  `crates/ematix-flow-distributed/`: tonic worker server, Arrow
  Flight cross-pod shuffle via `datafusion-distributed`,
  TLS / mTLS, lookup-table broadcast, multi-process bench harness.
- **Streaming-distributed combo gate.** Σ.B follow-up
  rejects `engine = "distributed"` with `[transform.window]` /
  `[transform.join]` at config-load — the *trait-level* coupling
  exists but the typed wrappers are still hard-pinned to
  `LazySqlTransform`. Lifting that constraint is on the deferred
  list.

**Pieces still missing (from the plan's Path B):**

1. **Distributed state store.** Partitioned + replicated tier on
   top of FoundationDB / DynamoDB / Cassandra. ~3–4 weeks.
2. **Watermark propagation across shuffle.** Per-edge watermark
   message in Arrow Flight; min-across-input-channels at every
   operator. ~1–2 weeks.
3. **Chandy-Lamport checkpoint coordinator.** Barrier injection,
   per-operator state snapshot, atomic topology-wide commit. The
   riskiest sub-component per the plan. ~3–4 weeks.
4. **Continuous Arrow Flight shuffle.** Today's Σ.B shuffle is
   batch-bounded; streaming needs continuous flow with key-
   partitioned routing. ~2–3 weeks.
5. **Credit-based backpressure.** Per-edge credit-based flow
   control to prevent fast producers OOMing slow consumers. ~1–2
   weeks.
6. **Per-pipeline failure recovery.** Roll back to last checkpoint
   on executor death; cross-pipeline isolation. ~2–3 weeks.

**Total: 12–20 weeks of focused work**, per the plan estimate.
Validated by mapping each piece to comparable components in Apache
Flink (each has shipped equivalents; engineering scope is well-
bounded but real).

**Single-developer pace: 6–10 months calendar.** The pieces compose
incrementally — each can land behind a cargo feature flag and be
exercised in isolation — but the user-visible "distributed
streaming works" claim only makes sense once #1, #2, #3, #4 are
all live.

**Verdict: known-quantity engineering; expensive on calendar.** Not
a research bet, but a long pole. Worth doing if (and only if) a
concrete workload demands it.

---

## Decision matrix

| Path | Adoption cost | Operational cost | API shape | Risk | Verdict |
|---|---|---|---|---|---|
| Arroyo | 4–6 wk integration | Cluster process | DataFusion-aligned, but library mode unmaintained | Cloudflare ownership; embedding is undocumented | **No** — regressed embeddability |
| RisingWave | 2–4 wk connector | Separate cluster | Postgres wire | Single-region; not embeddable | **No** as engine; **Yes** as future backend connector |
| Denormalized | 2–4 wk integration | Embedded | DataFusion-aligned, library-first | Pre-1.0, zero releases | **Watch** — re-evaluate at 1.0 |
| DIY | 12–20 wk build | Embedded | DataFusion-aligned, full control | Long calendar; checkpoint coord is the riskiest piece | **Defer** until a workload demands it |

---

## Recommendation: defer Σ.D, watching brief on Denormalized

**Strategic call.** None of the four paths is a clean "adopt now"
on this project's calendar. The honest framing:

1. **The user-demand case for distributed streaming is unverified
   in this project's runway.** Σ.B's distributed batch SQL was
   driven by the "scales as well as PySpark" headline (now
   grounded — see `docs/BENCHMARKS.md` Σ.C extension). Σ.D's
   counterpart claim — "distributed streaming SQL" — has no
   equivalent demand-side anchor. Building a 12–20 week piece of
   infrastructure for a hypothetical workload is the wrong
   sequencing.

2. **The single-node streaming surface is feature-complete for
   the workloads ematix-flow has actually shipped.** Phase
   39.4 / 39.5 / 39.5b cover tumbling, hopping, session windows
   and stream-stream joins on a single host. Pipelines that fit
   one box's RAM are unaffected by Σ.D's absence.

3. **Adopting now buys operational complexity without commensurate
   user value.** Arroyo is owned by Cloudflare (third-party
   priorities), RisingWave is a separate cluster (operational
   surface area), Denormalized is pre-release (instability).
   None of these reduce the user's footprint; they all expand it.

4. **The DIY engineering is real but bounded.** When a concrete
   workload demands it, the 6 sub-pieces are well-understood
   (Apache Flink shipped equivalents over a decade ago; the
   research papers exist). It's a calendar question, not a
   research-risk question.

**Concrete next steps:**

- **Close Σ.D as "deferred until demand"** in
  `PHASE_SIGMA_PLAN.md`. Σ.A / Σ.B / Σ.C stay shipped; v0.1.0
  release path is unblocked.
- **Add `examples/streaming-single-node/`** as a positioning
  artifact: shows what Phase 39 already supports + names the
  ceiling (per-key state ≤ single host RAM).
- **Trigger Σ.D implementation** when one of:
  - A real workload requires per-key state larger than a single
    host can hold.
  - Denormalized cuts a 1.0 release with stable API guarantees.
  - A user / customer commits to funding the 12–20 week DIY
    track explicitly.
- **Σ.D plan stays in `PHASE_SIGMA_PLAN.md`** — the engineering
  decomposition (6 sub-pieces) is preserved for future use; this
  spike doc captures *why* we didn't pull the trigger now.

---

## Open questions (for whoever picks Σ.D up)

1. **Does Denormalized publish a 1.0?** Re-spike when it does. The
   integration shape in this doc assumes the same DataFusion ABI
   as ematix-flow; if Denormalized pins a different DataFusion
   minor, that's a real coordination cost.
2. **Does Cloudflare / Arroyo restore the documented library API?**
   If yes, Path 1 reopens. Watch the Arroyo OSS commits for
   `Stream::with_parallelism` / `KafkaSource::new`-style examples
   in tree.
3. **Build-vs-buy when a concrete workload arrives.** The decision
   matrix above assumes no specific user case. With a real workload
   in hand the math changes — e.g., if the workload is "join two
   high-cardinality Kafka topics with key-fan-out > host RAM," the
   DIY pieces #1/#2/#3/#4 are *exactly* what's needed and a 12–20
   week buy-in becomes the right call. Without a workload, every
   path is hypothetical.
4. **Is there a useful intermediate step between single-node
   Phase 39 and full Σ.D?** Possibly: per-source-partition
   parallelism (each Kafka partition consumed by a separate
   process; checkpoint barriers per partition) covers a wide
   class of workloads without the cross-partition shuffle
   complexity. Worth a half-day spike before committing to the
   full Path B if/when Σ.D triggers.

---

## What this spike intentionally did *not* do

For honesty with future readers re-evaluating this decision:

- **No POC code.** The plan called for "50-line proof-of-concept
  code" per candidate path. None of the four paths got one in this
  spike. A future spike triggered by a real workload should land
  POCs against Denormalized (highest signal-to-noise) and DIY
  (validate the watermark-propagation mental model on a 2-process
  toy).
- **No build attempt.** Did not `cargo build` Arroyo, did not
  spin up a RisingWave Docker, did not pull the Denormalized git
  repo. Adding any of these to a future spike is cheap (~half-day
  per).
- **No latency / throughput measurement.** All performance
  reasoning above is about ABI alignment + operational shape,
  not measured numbers.
- **No survey of alternative DIY substrates.** Materialize
  (Differential Dataflow), Pathway, Bytewax — each could be a
  fifth path. Skipped because none change the strategic answer
  (defer until demand).

---

## Appendix — references

- Arroyo blog "We built a new SQL Engine on Arrow and DataFusion"
  (2024): library-mode architecture
- Arroyo 0.10 release notes (early 2024): standalone-binary shift
- Cloudflare acquisition press (2025): Arroyo → Cloudflare Pipelines
- RisingWave README: PostgreSQL wire protocol + cluster deployment
  options
- Denormalized README: embeddable Rust streaming on DataFusion;
  WIP / design-partner status
- `docs/PHASE_SIGMA_PLAN.md` Σ.D section: original 6-piece DIY
  decomposition
- `docs/PHASE_SIGMA_B_TRAIT_SPIKE.md`: pattern this spike doc
  follows
- Apache Flink streaming snapshots paper (arXiv:1506.08603):
  Chandy-Lamport reference for DIY Path B piece #3
