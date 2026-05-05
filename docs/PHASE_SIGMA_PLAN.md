# Phase Σ — Distributed compute (scale SQL like PySpark)

> **2026-05-05 — pre-pivot terminology preserved as historical
> record.** This plan was authored when Σ.B was specced against
> Apache Ballista. **Σ.B pivoted to
> [`datafusion-distributed`](https://crates.io/crates/datafusion-distributed)
> mid-implementation** when Ballista 52's DataFusion ^52 pin
> collided with the workspace's DataFusion 53. Throughout the
> body of this plan, read each "Ballista" reference as
> "`datafusion-distributed`" and each `BallistaBackend` /
> `ematix-flow-ballista` reference as `DistributedBackend` /
> `ematix-flow-distributed`. The pivot rationale lives in the
> Σ.B section's "PR 2 distributed-engine pivot" block (search
> for `2026-05-05 update`) and in
> [`docs/PHASE_SIGMA_B_TRAIT_SPIKE.md`](PHASE_SIGMA_B_TRAIT_SPIKE.md).
> The pre-pivot PR-by-PR plan body (PRs 2–5) is retained verbatim
> as historical context — actual implementation diverged from
> those PRs (no separate scheduler/executor binaries, no
> `flow-scheduler` daemon) per the pivot. Σ.A / Σ.B / Σ.C are
> all shipped as of this date; Σ.D's spike is shipped + deferred
> (see [`docs/PHASE_SIGMA_D_SPIKE.md`](PHASE_SIGMA_D_SPIKE.md)).

**Status:** **Σ.A / Σ.B / Σ.C all shipped (2026-05-05); Σ.D deferred
until demand.** v0.1.0 release path is unblocked. Original drafted-
not-yet-started status is preserved in the body below; this banner
captures the post-implementation reality.

**Goal.** Make ematix-flow's batch SQL surface match or beat PySpark's
performance at <20% of PySpark's image footprint, on the same
benchmarks (TPC-H), then extend to distributed streaming. Built on
DataFusion (already a workspace dep); no JVM, no shuffle service to
install separately.

This phase ships across five sub-phases:

- **Σ.A1** — TPC-H benchmark harness + audit single-node DataFusion
  for the SQL surface PySpark users hit. Standalone-valuable.
- **Σ.A2** — User-config SQL dialect selector + AST-rewrite
  translator built on `sqlparser-rs`. Validates dialect handling on
  single-node before B distributes it.
- **Σ.B** — Optional `DistributedBackend` peer to in-process
  DataFusion (built on `datafusion-distributed` after the
  Ballista pivot). One-time connector-trait refactor lands here.
- **Σ.C** — Three-way head-to-head benchmark: PySpark vs
  ematix-flow-distributed vs single-node DataFusion. Public-
  facing artifact.
- **Σ.D** — Distributed streaming. Gated on a 2-week build-vs-adopt
  spike against [Arroyo](https://github.com/ArroyoSystems/arroyo) and
  [RisingWave](https://github.com/risingwavelabs/risingwave) before
  committing to either path.

The Σ-block is gated so each sub-phase produces standalone value:
shipping A1 alone gets you a benchmark harness + a documented audit
of single-node SQL gaps. Shipping A1 + A2 gets you Spark-dialect
support without distribution. Shipping A1–C gets you the public claim.
Σ.D triggers after the batch claim is benchmarked + announced.

## Non-goals

- **Reinventing Flink from scratch in Σ.D.** The Σ.D week-1 spike
  evaluates Arroyo and RisingWave as embeddable backends first.
  DIY-on-Arrow-Flight is the fallback only if neither integrates.
- **Distributed streaming SQL in Σ.B.** Streaming pipelines stay
  partition-parallel (Kafka-Streams-style) through A1 → C. Σ.B
  distributes batch SQL only; streaming distribution waits for Σ.D.
- **Universal SQL dialect coverage in Σ.A2.** Initial ship is Spark +
  DuckDB + native DataFusion. Postgres / Trino / Presto on user
  demand. Unsupported AST nodes diagnose with a clear error pointing
  at the equivalent rewrite, not silently miscompile.
- **TPC-DS in Σ.C.** TPC-H's 22 queries are the published comparison
  surface against Spark. TPC-DS adds correlated subqueries + complex
  type stress that the Σ.A2 dialect translator hits before Σ.B does.
  Σ.C reports TPC-H; TPC-DS lands as a follow-up.
- **Native cluster orchestration.** Distributed execution runs
  as `flow-worker` peer processes; we ship a docker-compose
  example (`examples/distributed-cluster/`), not a Helm chart or
  Kubernetes operator. Cluster ops stays the user's problem
  (same boundary as Spark on EMR, where Spark doesn't ship the
  EMR control plane). *(Pre-pivot wording: "Ballista runs as
  scheduler + executor binaries.")*
- **Yanking `[transform] engine = "in-process"` once distributed
  lands.** In-process DataFusion stays the default forever;
  distributed is opt-in via config. Small workloads benefit
  from skipping the network hop.

## What Σ does and doesn't speed up

A common framing question is "will Σ make my reads / writes faster?"
Σ scales work **across machines**, not **within a machine**. ematix-
flow already beats PySpark single-node by ~2–4× on TPC-H (DataFusion's
optimizer + vectorized executor + no JVM warmup); Σ.B's job is making
sure that single-node lead doesn't disappear when workloads outgrow one
box. If your goal is "make a single 100 GB Postgres read 5× faster on
the same hardware," Σ alone is not the answer — that wants targeted
async-I/O work in the existing backends, separate from this plan.

### Object-store reads / writes

| Sub-phase | Effect |
|---|---|
| Σ.A1 | Audits the existing scan path on TPC-H Parquet; surfaces any pessimal patterns. Fixes flow into single-node, free speedup. |
| Σ.A2 | None (SQL parsing is sub-ms; not on the hot path). |
| Σ.B  | **Yes — distributed scan.** 100 Parquet files across 4 executors → ~4× scan-bound speedup, capped by S3-prefix throughput limits and your egress bandwidth. Predicate pushdown into the scan is already DataFusion's strength. |
| Σ.B  | **Writes are target-specific.** Per-executor file-per-partition (deterministic naming) works out of the box. Coordinated atomic commit needs target-side support — Delta Lake's transaction log handles it; raw object-store writes get documented patterns rather than a universal commit coordinator. |
| Σ.D  | None for batch; streaming object-store sources/targets land if/when they exist (today object-store is batch-only — see ROADMAP P4 #28). |

### Database reads / writes (Postgres / MySQL / SQLite / DuckDB)

| Sub-phase | Effect |
|---|---|
| Σ.A1 | None directly; benchmark surface is Parquet-only. |
| Σ.A2 | None. |
| Σ.B  | **Yes for reads, capped by the database.** Range-partition reads (`SELECT … WHERE id BETWEEN x AND y`) across N executors give 5–20× on big tables, but you're paying for it in database load + connection pressure. The connector-trait refactor in Σ.B PR 1 is the right place to add async chunked reads + range-partition support uniformly across all DB backends — **side benefit: improves the single-node path too.** |
| Σ.B  | **Writes scale only with idempotent targets.** MERGE / UPSERT keys allow per-executor parallel writes safely. Transactional inserts (append, no key) still want a single-writer pattern; documented in `examples/distributed-cluster/` patterns. |
| Σ.D  | None. |

### Streaming (Kafka / Pubsub / Kinesis / RabbitMQ)

| Sub-phase | Effect |
|---|---|
| Σ.A1 | None — batch-only. |
| Σ.A2 | None — batch-only. |
| Σ.B  | None — batch-only. **Streaming is already partition-parallel today**: N Kafka partitions × N pods = N-way concurrency, achieved without any Σ work. |
| Σ.D  | **Yes — distributed shuffle for streaming SQL.** Cross-partition windowed joins, global aggregations, and exactly-once across re-partition stop bottlenecking through one node's state. Per-partition throughput doesn't change (that's `rdkafka` / `parquet` / `object_store` raw speed). What unlocks: workloads where the *fan-out* of join keys exceeds one node's RAM. |

### What Σ doesn't address

These are not Σ's job; they need separate work if they're priorities:

- **Single-node I/O microbenchmarks.** Reading a single 1 GB Parquet
  file on one machine. DataFusion is already at par with PyArrow here;
  improvements come from `parquet`-crate-level work, not from Σ.
- **Database connection pooling efficiency.** Each backend's pool
  config (deadpool-postgres, mysql_async) is tuned per backend.
  Σ.B's connector-trait refactor consolidates the surface but doesn't
  touch the pool internals.
- **Streaming per-partition throughput.** `rdkafka` is near-line-rate
  on consume; the ceiling is hardware. Improvements come from batch-
  size tuning + state-store write coalescing, not Σ.
- **Cold-start time for the in-process path.** Today's `flow consume`
  starts in <100 ms; Σ doesn't change this. Distributed cold-start
  is in Σ.C's measurement scope (target ≤ 30% of Spark's).

### Indirect spillover from Σ work

Each sub-phase produces fixes that benefit users who never enable
the distributed path:

- **Σ.A1 audit.** Running TPC-H Q1/Q3/Q6/Q19 will surface DataFusion
  errata + transform.rs gaps. Fixes land in the single-node path.
- **Σ.B PR 1 (connector-trait refactor).** Consolidates async I/O,
  predicate pushdown, and range-partition support across all 12
  backends. Single-node users benefit from any pushdown the trait
  enables (e.g., projecting only requested columns through Postgres
  COPY when the SQL only references 3 of 50 columns).
- **Σ.B PR 6 (image-size budget).** Forces a `cargo bloat` audit;
  any duplicated symbols across statically linked deps get fixed.
  Wheel size shrinks for everyone.

## Architecture

```text
                  ┌──────────────────────────────────────────┐
                  │  user SQL (one of the supported          │
                  │  dialects: spark / duckdb / datafusion)  │
                  └──────────────────────────────────────────┘
                                  │
                                  ▼
                  ┌──────────────────────────────────────────┐
                  │  Σ.A2: dialect translator                │
                  │    sqlparser-rs::parse(<dialect>)        │
                  │    ─→ AST                                │
                  │    ─→ rewrite (function names, syntax)   │
                  │    ─→ emit DataFusion SQL                │
                  │  diagnostic on unsupported nodes         │
                  └──────────────────────────────────────────┘
                                  │
                                  ▼ DataFusion SQL
                  ┌──────────────────────────────────────────┐
                  │  Σ.A1: transform.rs (DataFusion)         │
                  │    DataFusionTransform / LazySqlXform    │
                  │  Σ.B fork point ──┐                      │
                  └───────────────────┼──────────────────────┘
                                      │
                                      ▼ engine selector
        in-process (default)                       distributed (opt-in)
                  │                                           │
                  ▼                                           ▼
        ┌────────────────────┐               ┌──────────────────────────────┐
        │ DataFusion         │               │  Σ.B: DistributedBackend     │
        │ SessionContext     │               │    flow-worker × N (peers)   │
        │ (single-process)   │               │    Arrow Flight shuffle      │
        │                    │               │    hash-partitioned join     │
        └────────────────────┘               │  ┌──────┬──────┬──────┐      │
                  │                          │  │ peer │ peer │ peer │      │
                  │                          │  └──────┴──────┴──────┘      │
                  │                          │  partitioned state store ◀─── Σ.D
                  │                          └──────────────────────────────┘
                  │                                           │
                  └─────────────────────┬─────────────────────┘
                                        ▼
                                  RecordBatch
                                        │
                                        ▼ write_arrow_stream
                                  target backend
```

The two new components are the **dialect translator** (Σ.A2) and the
**distributed backend** (Σ.B, built on `datafusion-distributed`
post-pivot). Σ.A1 audits + benchmarks the existing single-node path
so the rest of the block has a regression baseline. Σ.D extends
the distributed box leftward into streaming, replacing the
batch-bounded shuffle with a continuous one.

---

## Σ.A1 — single-node DataFusion baseline + TPC-H harness

**PR scope.** Estimated 4 PRs, ~1–2 weeks total.

### PR 1 — TPC-H data generation + `examples/tpch/` directory

- New crate dep: `arrow-tpch` (read-only generator that emits TPC-H
  schemas + row data into Arrow `RecordBatch` directly, no `dbgen`
  shell-out). Pin to whatever line is current at PR-write time;
  document version in the bench file's top-of-file comment.
- `examples/tpch/generate.rs` — generates SF=1 (~1 GB compressed) into
  `examples/tpch/data/sf1/` as one Parquet file per TPC-H table
  (lineitem.parquet, orders.parquet, etc.). Snappy compression to
  match TPC-H's published artifacts. Idempotent: regenerates only if
  files don't exist.
- `examples/tpch/queries/` — 22 .sql files matching TPC-H spec
  numbering. Q1 / Q3 / Q6 / Q19 are the cheap representative set
  used by Σ.A1 benches; rest land as references for Σ.A2 + Σ.C.
- `.gitignore` rule for `examples/tpch/data/` — generated artifacts
  don't go in git. README in `examples/tpch/` documents the
  generation step + expected disk usage.
- `cargo test -p ematix-flow-core --test tpch_smoke` — single
  integration test that runs Q6 (the simplest aggregate-only query)
  end-to-end, asserting row count + sum match the TPC-H reference
  values for SF=1. Smoke test, not a benchmark.

**Acceptance.** `cargo run --example tpch_generate -- --sf 1`
produces 8 Parquet files totaling ~1 GB; `cargo test tpch_smoke`
passes against them.

**TDD.** Smoke test is the failing test that drives generation
correctness — written first, fails until PR 1 ends, then passes.

### PR 2 — Criterion benches at SF=1 for Q1 / Q3 / Q6 / Q19

- New file: `crates/ematix-flow-core/benches/tpch.rs` (alongside
  existing `transform.rs` bench). Uses `criterion`'s
  `BenchmarkGroup` with `measurement_time = 60s` per query
  (Q1 ~30s on M1, Q3 ~15s, Q6 ~5s, Q19 ~10s — measurement window
  needs to dwarf the per-iteration time).
- Bench harness wires in: `SessionContext::new()`, register Parquet
  paths from `examples/tpch/data/sf1/`, run the SQL, materialize the
  result into a `Vec<RecordBatch>`, return it. Materialization is
  required so we measure execution, not just plan-building.
- `cargo bench --bench tpch -- tpch_q6` runs one query in isolation
  for fast iteration; `cargo bench --bench tpch` runs all four.
- A `--save-baseline sf1_<hostname>` cargo-bench convention; commit
  baseline numbers from one canonical machine (recommend
  ryan's M1 + a Linux x86_64 EC2 m6i.large) into
  `docs/BENCHMARKS.md`. Subsequent PRs that touch transform.rs
  re-run and diff against this baseline.
- README pointer: `examples/tpch/README.md` describes how to run the
  benches + interpret the criterion output.

**Acceptance.** All four queries complete on SF=1 within 60s
measurement windows on the canonical Linux x86_64 host. Numbers
committed to `docs/BENCHMARKS.md` as the Σ.A1 baseline.

**TDD.** Each bench is the failing test for its query — if the SQL
errors out (e.g., function-not-found), the bench panics; the fix
goes in PR 3 below.

### PR 3 — fix any audit gaps surfaced by the benches

- The bench failures from PR 2 are the audit punch list. Likely
  candidates based on TPC-H query shape:
  - Q1: simple aggregate; should work on any vectorized engine.
    Likely the canary that proves the harness wiring works.
  - Q3: 3-way join with `LIMIT`; needs hash-join (DataFusion has it).
  - Q6: scan + filter + aggregate; the simplest baseline.
  - Q19: complex disjunctive `WHERE` clause; sometimes exposes
    optimizer-rewrite cliffs.
- For each query that doesn't run on PR 2's harness, debug:
  - Read the DataFusion error → identify the missing piece
    (function, optimizer rewrite, type coercion).
  - Either fix in `transform.rs` (if it's our wrapping that drops
    the feature) or upstream (DataFusion PR + version bump). Prefer
    upstream when possible — keeps `transform.rs` thin.
  - Add a per-fix unit test in `transform.rs` exercising the
    minimal SQL that triggered the failure.
- This PR is the actual "audit single-node DataFusion" deliverable.
  Punch list lands in `docs/BENCHMARKS.md` under "Σ.A1 audit findings"
  with each fix's commit hash.

**Acceptance.** All four TPC-H queries run cleanly. Per-fix unit tests
in `transform.rs` pass. Audit findings committed to docs.

### PR 4 — head-to-head vs single-node PySpark on the same hardware

- New script: `scripts/bench-tpch-pyspark.py` — runs the same 4
  queries against PySpark's local mode (`SparkSession.builder.master('local[*]')`)
  on the same Parquet files generated by PR 1. Report wall time per
  query (3-trial median).
- New section in `docs/BENCHMARKS.md`: "Σ.A1 baseline — single-node
  DataFusion vs PySpark." Table format:

  ```
  | Query | DataFusion (s) | PySpark (s) | DataFusion / PySpark |
  |-------|----------------|-------------|----------------------|
  | Q1    |  X.XX          |  Y.YY       |  Z.ZZ                |
  ...
  ```

  Σ.A1 acceptance is `DataFusion / PySpark < 1.0` on all four queries
  (DataFusion typically wins single-node by ~2–4×; if we don't, dig
  in before Σ.A2).
- Hardware spec captured in the doc: CPU model, core count, RAM,
  storage backend, OS, JDK version (PySpark side), Python version,
  Rust version. Reproducibility is the point.

**Acceptance.** DataFusion wins all four queries by ≥1.5× geomean on
the canonical Linux host. If not — Σ.A1 PR 4.5 investigates before
Σ.A2 starts.

### Σ.A1 deferred for follow-up

- **TPC-H at SF=10 / SF=100.** Σ.A1 runs SF=1 only (laptop-friendly).
  Σ.C extension landed SF=10 4-query rep-set numbers
  (`ematix-flow-distributed` 3 in-process workers vs single-node
  DataFusion vs PySpark `local[*]`); SF=100 still deferred (data
  alone is 100 GB; needs real cluster hardware).
- **TPC-DS suite.** Heavier dialect workout than TPC-H; lands in Σ.A2
  as the dialect translator's primary acceptance harness.
- **Audit findings that need upstream fixes.** Some DataFusion
  features (e.g., specific window-frame syntax) may need upstream
  PRs that block on DataFusion's release cadence. Document these in
  `docs/BENCHMARKS.md` "Σ.A1 audit findings"; track separately.

---

## Σ.A2 — SQL dialect selector + AST-rewrite translator

**PR scope.** Estimated 5 PRs, ~2–3 weeks total.

### PR 1 — config surface + passthrough dialect

- TOML: `[transform] dialect = "datafusion" | "spark" | "duckdb"`
  (default `"datafusion"`, zero-cost passthrough). Typed-Python:
  `Transform(dialect="spark")` builder kwarg with the same default.
- Plumb the dialect string through `LazySqlTransform::new` /
  `DataFusionTransform::new` constructors. `dialect = "datafusion"`
  short-circuits — no parsing, no rewriting; SQL goes straight to
  `SessionContext::sql()`.
- New module: `crates/ematix-flow-core/src/dialect.rs`. Stub
  `translate(sql: &str, from: Dialect) -> Result<String>` that
  panics on `Dialect::Spark` / `Dialect::DuckDb` until PRs 2–4
  land.
- Add CLI parse test for `dialect = "spark"` to confirm it round-
  trips through TOML config without errors (still panics at
  execution because PR 1 doesn't implement the translation; the
  config-load path is what's tested here).

**Acceptance.** `dialect = "datafusion"` continues to work
identically to today. `dialect = "spark"` parses but panics on
execution with a clear "translation not implemented" message.

### PR 2 — Spark dialect: parse + emit roundtrip + function-name remap

- Add `sqlparser` directly as a workspace dep (already in
  DataFusion's tree as a transitive — promote to direct so we can
  call its `Spark` parser). Pin to whatever DataFusion 53.x uses to
  avoid arrow-ABI-style version split-brain.
- `dialect.rs::translate` for `Dialect::Spark`:
  1. `Parser::parse_sql(&SparkDialect, sql)` → `Vec<Statement>`.
  2. AST walk that rewrites function names per a static
     `SPARK_TO_DATAFUSION_FUNCTIONS` table:
     `current_timestamp` → `now`, `from_unixtime` → `from_unixtime`
     (signature differs, also rewrite args), `expr` (Spark's no-op
     wrapper) → strip, etc.
  3. `statement.to_string()` to emit DataFusion-compatible SQL.
- New unit-test file: `crates/ematix-flow-core/tests/dialect_spark.rs`.
  Each test pair: input Spark SQL → expected DataFusion SQL. ~30
  pairs covering the function-name remap surface.
- Integration test: parametrize the Σ.A1 TPC-H benches with
  `dialect = "spark"` reading `examples/tpch/queries/spark/*.sql`
  (Spark dialect of the same queries — generated by hand for the 4
  representative ones in PR 2; rest in PR 4).

**Acceptance.** Q1 / Q3 / Q6 / Q19 in Spark dialect produce identical
results (row-by-row equality) to the same queries in DataFusion
dialect. `cargo test dialect_spark` passes ~30 function-remap
roundtrip tests.

### PR 3 — Spark dialect: structural rewrites

- `LATERAL VIEW EXPLODE(arr) AS x` → `UNNEST(arr) AS x` (DataFusion
  syntax). Walk the FROM clause; rewrite the lateral-view node into
  the equivalent `cross join unnest`.
- Window-frame syntax differences: Spark's `ROWS BETWEEN UNBOUNDED
  PRECEDING AND CURRENT ROW` is identical, but `RANGE BETWEEN INTERVAL
  ... PRECEDING ...` needs DataFusion's range-frame interval format.
- Complex-type literal rewrites: Spark's `array(1, 2, 3)` →
  DataFusion's `[1, 2, 3]`; Spark's `named_struct('a', 1, 'b', 2)`
  → DataFusion's `struct(1 AS a, 2 AS b)`.
- Each rewrite has a dedicated walker pass with unit tests in
  `tests/dialect_spark.rs`.

**Acceptance.** Hand-curated set of TPC-DS queries that exercise each
rewrite (we don't run the full DS suite yet, but we run ~10 queries
that cover the surface) produces correct results.

### PR 4 — Spark dialect: TPC-DS run

- Generate TPC-DS SF=1 via the `arrow-tpch` crate's TPC-DS module
  (or `tpcds-kit` if the Rust crate doesn't cover it — investigate
  in PR 4 spike).
- `examples/tpcds/queries/spark/*.sql` — all 99 TPC-DS queries
  in Spark dialect.
- Bench harness: `cargo bench tpcds_dialect_smoke` runs each query
  through `dialect = "spark"` and asserts only "executes without
  error" (correctness comparison against PySpark is Σ.C territory).
- Failures from this run are the audit findings for Σ.A2 — file
  each as `docs/PHASE_SIGMA_PLAN.md` "Σ.A2 dialect findings" with a
  workaround pointer (e.g., "MAP_FROM_ENTRIES not yet translated;
  rewrite as `transform_keys(...)` until next release").

**Acceptance.** ≥80% of 99 TPC-DS queries execute under
`dialect = "spark"` without error. Documented gaps for the rest.
80% threshold is the criterion-fail line; if we land at 60%, Σ.A2
PR 4.5 fills more gaps before PR 5.

### PR 5 — DuckDB dialect translator

- Same shape as Spark, narrower scope — DuckDB SQL is closer to
  DataFusion's. Mostly a function-name remap (DuckDB's `regexp_matches`
  → DataFusion's `regexp_match`, etc.) plus a few date-literal
  syntax differences.
- Acceptance: ≥90% of TPC-H + a hand-curated DuckDB-specific test
  set executes under `dialect = "duckdb"` without error.

### Σ.A2 deferred for follow-up

- **Postgres dialect.** Most Postgres SQL works under `datafusion`
  already; the gaps are around array literals, JSON path operators
  (`->>`), and date-add interval syntax. Lands when a user demand
  surfaces.
- **Trino / Presto dialect.** Specifically requested by users
  migrating from Trino installations. Same shape as Spark; defer
  until somebody asks.
- **Diagnostic hints that auto-suggest the rewrite.** If translation
  hits an unsupported node, current behavior is "error pointing at
  the AST node." A future improvement: suggest the equivalent
  DataFusion SQL fragment in the error message. Out of scope for the
  initial ship — error-points-to-source is enough.

---

## Σ.B — Optional `DistributedBackend`

> **2026-05-05 update**: Σ.B pivoted from **Ballista** to
> **`datafusion-distributed`** at PR 2 — the DataFusion-version pin
> for Ballista 52 (DF ^52) is incompatible with the workspace's DF
> 53.1, and the library-only model of `datafusion-distributed`
> better matches the "footprint like ematix-flow" pitch (no
> separate scheduler/executor binaries to ship). The locked
> trait-shape decisions in `docs/PHASE_SIGMA_B_TRAIT_SPIKE.md`
> stay; PR 2 deliverables shrink (no docker-compose with separate
> scheduler+executors, no scheduler-process to operate). See the
> spike doc's "PR 2 distributed-engine pivot" section for the
> rationale + side-by-side comparison.
>
> **PR 1 status**: shipped (4 commits a/b/c/d, 2026-05-05). All 10
> backends migrate to `BackendConfig` + `Backend::config()` +
> `backend_from_config`. Streaming-backend builder-state round-trip
> deferred to Σ.B PR 2 follow-up; `as_postgres()` removal still
> deferred. The PR-numbered subsections below were written
> pre-pivot — read each "Ballista" reference as
> "datafusion-distributed" until this section is rewritten.

**PR scope.** Estimated 7 PRs, ~3–6 weeks total. The connector-trait
refactor is the biggest single chunk; line up all the Σ-block needs
(serializability, URL-only config, `Send + Sync + 'static`) before
absorbing the breaking change.

### PR 1 — connector-trait refactor (the breaking change)

- Audit all `Backend` implementors. Each one currently carries:
  - Connection pool / cache state
  - Schema-registry caches (Kafka)
  - Pre-computed type maps (Postgres / MySQL)
- Refactor: make every Backend constructible from a single URL
  (already mostly true) + a `Properties` map of strings (TOML-flat).
  Connection pools / caches construct lazily on first use.
- Connector trait gains:
  - `Send + Sync + 'static` bounds (serializable across threads).
  - `serialize_config(&self) -> String` / `deserialize_config(s: &str) -> Self`
    for shipping over Arrow Flight.
- Audit + update every backend impl. ~12 backends; each one gets
  its own commit within this PR.
- This is the single largest commit in the Σ block; dedicated PR
  with thorough review. Pre-1.0 budget approved per ROADMAP.

**Acceptance.** Existing `cargo test --workspace --all-targets`
passes. Per-backend integration tests still pass. New round-trip
test: `serialize → deserialize → serialize` produces stable bytes.

### PR 2 — `crates/ematix-flow-ballista` workspace member

- New crate. Two binary targets: `flow-scheduler` + `flow-executor`.
  Library exports `BallistaBackend` for the parent workspace.
- Depend on `ballista` ~0.12 (whatever's current at PR-write time).
  Pin tight; expect to bump.
- Scheduler binary boots from `flow-scheduler.toml` (port, executor
  registration timeout, …). Executor binary takes
  `--scheduler-url ...` + `--bind-port ...` + `--data-dir ...`
  flags.
- `BallistaBackend` impl: implements the same `Backend` trait as
  `DataFusionTransform`'s in-process shape; constructor takes
  `scheduler_url: String`, `executor_pool_size: usize`. Submit each
  SQL via `BallistaContext::sql(...)`, materialize the result, hand
  back to the calling `LazySqlTransform`.
- Wheel matrix: ballista crate is included in the main wheel under a
  `ballista` cargo feature, off by default. Users opt in with
  `pip install 'ematix-flow[ballista]'` (extras_require redirects
  to a separate PyPI package `ematix-flow-ballista` that pins the
  matching scheduler/executor binaries — TBD whether one wheel or
  two).

**Acceptance.** `cargo build -p ematix-flow-ballista --bins`
produces working binaries. Smoke test: bring up scheduler + 1
executor locally; submit `SELECT 1`; get the right answer back.

### PR 3 — config selector

- TOML: `[transform] engine = "datafusion" | "ballista"` (default
  `"datafusion"`). When `ballista`, also `ballista_scheduler_url = "..."`
  required.
- Runtime selection in the pipeline builder: build a
  `BallistaTransform` (new type) instead of `DataFusionTransform`
  when `engine = "ballista"`. `BallistaTransform` implements the
  same `BatchTransform` trait but routes execution via Ballista.
- CLI: `flow consume --engine ballista` flag overrides the TOML
  setting per-invocation (matches the existing `--restart-on-error`
  CLI override pattern).

**Acceptance.** Toggle between engines via TOML alone (no
recompilation); same SQL produces identical results modulo float-
ordering nondeterminism in distributed execution (assert with
`assert_batches_eq!` after sorting).

### PR 4 — `examples/ballista-cluster/` docker-compose

- `docker-compose.yml`: scheduler + 3 executors + MinIO (for shared
  S3-compatible storage that all executors can read).
- `examples/ballista-cluster/README.md`: bring-up instructions, how
  to point a `flow consume` at the cluster.
- `examples/ballista-cluster/sample-pipeline.toml` — a tiny pipeline
  that reads from MinIO Parquet, runs a small SQL transform, writes
  back. End-to-end smoke from a user perspective.
- One-command bring-up: `make ballista-up` (or shell script) brings
  everything up; `make ballista-down` tears it down.

**Acceptance.** `make ballista-up && flow consume --engine ballista
sample-pipeline.toml` runs successfully on a fresh laptop.

### PR 5 — failure-mode integration test

- New test: `tests/integration_ballista_failover.rs`. Spins up
  scheduler + 3 executors via the docker-compose from PR 4. Submits
  a long-running query (TPC-H SF=10 Q1, ~30s on 3 executors). Mid-
  query, kills one executor (`docker kill ballista-executor-2`).
  Asserts: query retries on a remaining executor + completes within
  3× expected time + returns correct result.
- Marked `#[ignore]` (slow, requires Docker). Run on tag pushes only
  via release.yml's `verify` job, not on every push.

**Acceptance.** Failure-mode test passes deterministically; retry
behavior documented in `docs/BENCHMARKS.md`.

### PR 6 — image-size budget verification

- Build the scheduler + executor binaries with `--release` +
  `strip = true`. Measure on Linux x86_64.
- Build a `Dockerfile.ballista-executor` that COPYs the executor
  binary into `gcr.io/distroless/cc-debian12`. Measure final image.
- Acceptance: scheduler image ≤ 50 MB, executor image ≤ 100 MB
  (vs PySpark's 920 MB+). Numbers committed to `docs/BENCHMARKS.md`.
- If we're over budget, PR 6.5 investigates: most likely culprit is
  duplicated arrow-rs symbols across statically linked deps. Fix
  via `cargo bloat -p ematix-flow-ballista --release` audit.

### PR 7 — CI coverage

- Add `ballista-build` job to release.yml that builds both binaries
  in the existing manylinux_2_28 container. Artifacts uploaded
  alongside the wheels.
- Add `ballista-integration` job (skipped on PR runs, runs on tag
  push) that brings up the docker-compose and runs the failover
  test from PR 5.
- Document the cluster-deploy story in `docs/USER_GUIDE.md` —
  pointer to `examples/ballista-cluster/`, list of supported
  cloud-orchestration patterns (k8s YAML, ECS task definition).

### Σ.B deferred for follow-up

- **Helm chart for k8s deploys.** Ships as a separate repo +
  release cadence; not part of Σ.B. The docker-compose example +
  binary downloads cover the "I want to try it" path.
- **Auto-scaling executors.** The cluster has a fixed-size executor
  pool. Auto-scaling integrates with whatever orchestrator the user
  picked (k8s HPA, ECS auto-scaling, etc.); we don't ship one-size-
  fits-all logic.
- **Scheduler HA.** Single-scheduler deployment. HA needs Raft or
  a similar consensus protocol; out of scope.

---

## Σ.C — TPC-H head-to-head vs PySpark

**PR scope.** Estimated 3 PRs, ~1–2 weeks total.

> **Scope shift (2026-05-05).** Original Σ.C plan called for a
> 4×`m6i.4xlarge` AWS cluster as the canonical bench machine. This
> project is no longer deploying to AWS — Σ.C's primary deliverable
> is now docker-compose multi-process numbers on developer hardware,
> with the AWS recipe (`infra/`, `scripts/tpch-bench-multi.sh`)
> retained as an alternative path for anyone who *does* have cluster
> access. The "Ballista" name throughout the original section is
> obsolete — the engine pivoted to `datafusion-distributed` (see
> Σ.B note above + spike doc). Honest framing: docker-compose on
> one host shares kernel/CPU/memory/disk and uses loopback
> networking — these numbers measure multi-process distributed-plan
> overhead, not cross-host scaling. Real cross-host claims need
> network-separated hardware (homelab k3s, rented bare-metal) and
> remain a deferred item.

### PR 1 — bench harness for the distributed-execution path *(shipped)*

- `crates/ematix-flow-distributed/benches/tpch_distributed.rs`
  routes Q1 / Q3 / Q6 / Q19 through `DistributedBackend` under two
  configurations per run:
  1. `distributed_of_one` (`peers = []`) — degenerate single-worker
     cluster, measures trait-surface overhead vs raw `SessionContext`
  2. `distributed_3_workers` (in-process tonic on free localhost
     ports) — exercises the `DistributedPhysicalOptimizerRule`-
     wrapped plan + Arrow Flight shuffle
- Reads from `examples/tpch/data/sf<N>/*.parquet` generated via
  `cargo run --release -p ematix-flow-core --example tpch_generate`.
- Group labels derive `sf<N>` tag from the data dir basename so
  runs at different scale factors don't collide.
- Single-node DataFusion control bench lives in
  `crates/ematix-flow-core/benches/tpch.rs` (Σ.A1 PR 2; same data
  dir, different harness).
- PySpark baseline harness: `scripts/bench-tpch-pyspark.py`
  (shipped in PR 2 prep below). Runs on the same hardware for
  apples-to-apples.

### PR 2 — multi-process baseline numbers + write-up *(in progress)*

#### PR 2 prep (shipped 2026-05-05)

- `infra/cloud-init-worker.sh` + `infra/README.md` — manual EC2
  provisioning recipe (4× `m6i.4xlarge`, security groups, TPC-H
  data staging). Retained for anyone with AWS access; **not** the
  primary path.
- `scripts/tpch-bench-multi.sh` — driver that runs the three
  benches against pre-provisioned hardware and feeds output to
  the summarizer.
- `scripts/tpch-bench-multi-summarize.py` — produces the
  comparison markdown table from criterion + PySpark output.
- `examples/distributed-cluster/docker-compose.yml` mounts
  `examples/tpch/data` read-only at `/data` in every worker so
  a host coordinator can register Parquet via the same path the
  workers see. Added 2026-05-05.
- `tpch_distributed.rs` honours `EMATIX_DISTRIBUTED_PEERS`
  (comma-separated URLs) — point it at the docker-compose stack
  instead of in-process workers. Added 2026-05-05.

#### PR 2 publish (pending)

Run the harness on developer hardware:

1. `cargo bench -p ematix-flow-core --bench tpch` — single-node
   DataFusion control
2. `docker compose -f examples/distributed-cluster/docker-compose.yml up -d`
3. `EMATIX_DISTRIBUTED_PEERS=http://localhost:50051,...,:50053`
   `TPCH_DATA_DIR=/data/sf10`
   `cargo bench -p ematix-flow-distributed --bench tpch_distributed`
4. `python scripts/bench-tpch-pyspark.py --sf 10` for the PySpark
   baseline on the same hardware
5. Feed output through `scripts/tpch-bench-multi-summarize.py`

Publish `docs/BENCHMARKS.md` with:

- Three-way table (DataFusion / distributed-on-compose / PySpark)
  for Q1 / Q3 / Q6 / Q19 at SF=1 + SF=10
- Per-query median + P95 from criterion / PySpark output
- Memory / image-size / cold-start columns measured on the same
  developer hardware (`docker stats` for memory, `docker image
  inspect` for image size)
- **Disclaimer banner** on every header pinning "single-host,
  multi-process, NOT cross-host" so blog-post readers don't
  miscite the numbers
- Honest gaps: any TPC-H queries that don't run on
  ematix-flow-distributed listed with workarounds

**Acceptance.**

- Median Q1 / Q3 / Q6 / Q19 latency: ematix-flow-distributed
  numbers within 30% of single-node DataFusion on the same
  hardware. Distributed planning has overhead at SF=1; small
  spread is expected. This is a regression-canary, not a
  scaling claim.
- Geomean per-process memory: flow-worker ≤ 50% of a Spark
  executor's footprint on the same hardware.
- Worker image size: flow-worker image ≤ 20% of Spark image.
- Cold-start time: docker-compose stack up to first query
  ≤ 30% of equivalent Spark stack.

The "scales as well as PySpark" headline cannot be made from
these numbers alone — that requires real cross-host hardware
(deferred, see below).

### PR 3 — announcement + repro instructions

- `docs/BENCHMARKS.md` becomes the source of truth: paste-into-
  Hacker-News header, plus per-query breakdown + reproducibility
  appendix (exact docker-compose / cargo / pyspark commands).
- AWS recipe (`infra/README.md`) gets a callout in the appendix
  so anyone who *does* have cluster hardware can produce real
  cross-host numbers — those land as a follow-up if/when someone
  runs them.
- Top-of-file disclaimer makes the single-host framing impossible
  to miss.
- Honest gaps section: TPC-H queries that don't run on
  ematix-flow-distributed (correlated subqueries, certain CTEs)
  listed with workarounds. We claim "small footprint + correct
  distributed plans on the queries it supports," not "100%
  TPC-H coverage day one" and not "cross-host scaling proof."

### Σ.C deferred for follow-up

- **Real cross-host numbers.** The publishable "scales as well as
  PySpark" claim needs network-separated hardware (homelab k3s
  across multiple boxes, or rented bare-metal at Hetzner / OVH).
  The `infra/` recipe still works for AWS users; this project's
  primary path is docker-compose. Lands when a contributor runs
  the harness on real cluster hardware.
- **TPC-DS at any scale factor.** TPC-DS is a heavier workout
  than TPC-H but published Spark numbers are sparser. Follow-up.
- **Larger scale factors (SF=100 / SF=1000).** SF=10 is the
  practical ceiling for laptop-class hardware (data alone is
  ~10 GB; SF=100 is ~100 GB and exceeds typical dev memory).
  Larger factors need real cluster hardware + budget.
- **Multi-cloud benchmark numbers.** Out of scope until there's
  a primary cloud target — and there isn't one.

---

## Σ.D — distributed streaming (deferred until demand)

> **Status (2026-05-05): deferred.** The research-level spike
> [`docs/PHASE_SIGMA_D_SPIKE.md`](PHASE_SIGMA_D_SPIKE.md)
> evaluated four candidate paths (Arroyo, RisingWave, Denormalized,
> DIY) and recommended deferring Σ.D until either a concrete
> workload demands per-key state larger than a single host can
> hold, or Denormalized cuts a 1.0 release. Findings in brief:
> Arroyo's embeddability regressed (Cloudflare 2025 acquisition);
> RisingWave is sidecar-only (Postgres-wire protocol, separate
> cluster); Denormalized is the right architectural shape but
> pre-1.0 / 0 releases; DIY on Arrow Flight + state_store is 12-20
> weeks of bounded-but-real engineering. The decision artifact
> below is preserved for the engineering decomposition; the strategic
> "build now" trigger lives in the spike doc.

**PR scope.** Spike is 2 weeks; implementation is 4–20 weeks
depending on the spike outcome. Total Σ.D: 6–22 weeks.

### Spike (week 1–2) — Arroyo + RisingWave evaluation *(closed; see [PHASE_SIGMA_D_SPIKE.md](PHASE_SIGMA_D_SPIKE.md))*

Two-week dedicated investigation, no PRs landing in this window.
Output is `docs/PHASE_SIGMA_D_SPIKE.md` documenting:

1. **Arroyo as embeddable backend.** Can `BallistaBackend`'s pattern
   (separate scheduler + executor) be replicated with Arroyo's
   pipeline runtime? Specifically: does Arroyo expose a "submit a
   streaming SQL query, give me back a stream of result batches"
   API, or is it a top-down product expecting to own the whole
   process? (As of last check, closer to a product than a library.)
   Findings + 50-line proof-of-concept code.
2. **RisingWave as embeddable backend.** Same investigation. RisingWave
   is closer to a database than a library — likely needs a "RisingWave
   sidecar" deployment pattern rather than embedding.
3. **DIY on Arrow Flight + existing state-store partitioned tier.**
   Estimate cost (4–6 months engineering); identify the riskiest
   sub-component (probably distributed checkpoint coordination —
   see Chandy-Lamport notes below).

Spike concludes with a recommendation: adopt Arroyo / adopt RisingWave
/ build DIY. Subsequent PRs depend on the choice.

### Path A (Arroyo): 4–6 weeks of integration

- `crates/ematix-flow-arroyo` workspace member, modeled on
  `ematix-flow-ballista`. New `ArroyoBackend` peer to in-process
  streaming.
- TOML: `[streaming] engine = "in-process" | "arroyo"`. Default
  `"in-process"` (today's behavior unchanged).
- Wire Arroyo's pipeline-submission API to ematix-flow's existing
  source/target backends. Adapter at the connector boundary so a
  user's `KafkaSource` works against Arroyo's exec the same way it
  works against in-process today.
- `examples/arroyo-cluster/` docker-compose; integration test
  modeled on Σ.B PR 5.

### Path B (DIY): 12–20 weeks

The deep build. Required pieces, each its own PR or sub-phase:

1. **Distributed state store.** Build out Phase 39.5's `state_store/`
   into a partitioned + replicated tier. Either layer over an
   existing K/V (FoundationDB recommended for transaction semantics
   + multi-region) or build on top of DynamoDB / Cassandra at the
   cost of latency.
2. **Watermark propagation.** Across shuffle boundaries, align
   watermarks via Flink's "min across all input channels" algorithm.
   Needs a per-edge watermark message in the Arrow Flight stream.
3. **Chandy-Lamport checkpoint coordinator.** Inject barriers at
   the shuffle inputs; each operator snapshots state when all
   inputs have seen the barrier; atomic commit across the
   topology. References: Apache Flink's [streaming snapshots
   paper](https://arxiv.org/abs/1506.08603).
4. **Continuous streaming shuffle.** Different from Ballista's
   batch-bounded shuffle: continuous Arrow Flight stream with
   key-partitioned routing. Needs flow-control to prevent fast
   producers OOMing slow consumers (see #5).
5. **Credit-based backpressure.** Per-edge credit-based flow
   control. Mirrors Flink's `NetworkBufferPool`. Each consumer
   advertises a buffer credit; producers stop emitting when credit
   exhausted.
6. **Per-pipeline failure recovery.** When an executor dies mid-
   stream, the rest of the topology rolls back to the last
   checkpoint, the dead executor is re-launched, and processing
   resumes from the checkpoint. Cross-pipeline isolation: failures
   in one streaming job don't kill others.

Each piece is 2–4 weeks of focused work. Path B ships in increments;
each increment lands a piece of distributed streaming behind a
separate cargo feature flag so it can be exercised independently.

### Σ.D deferred for follow-up

- **Stream-stream join with > 2 sources.** Currently Phase 39.5b
  joins are 2-source only. N-source joins land as a follow-up after
  Σ.D's 2-source distributed shape is proven.
- **Arbitrary user-defined stateful operators.** Today's stateful
  ops are session-window, tumbling-window, hopping-window, 2-source
  join. Letting users write a `Stateful` trait impl that participates
  in the Σ.D distributed-state machinery is meaningful follow-up
  work; out of scope for the initial ship.
- **Cross-region streaming.** Σ.D's distributed state store is
  single-region. Multi-region active-active has the usual write-
  conflict / latency tradeoffs; out of scope.

---

## Open design questions

Captured here so they're addressed before kicking off the relevant
sub-phase.

### Σ.A1

1. **TPC-H data generation strategy.** Use `arrow-tpch` crate, or
   shell out to `dbgen` and convert? Crate is more reproducible
   (same Rust version → same bytes); `dbgen` is the canonical
   reference. Spike in PR 1 to decide.
2. **Bench machine canonicalization.** `docs/BENCHMARKS.md` numbers
   need to be from a stable hardware target. **Resolved 2026-05-05.**
   This project is not deploying to AWS, so the canonical bench
   machine is developer hardware (Apple Silicon M-series Mac, ≥32 GB
   RAM) running the docker-compose 3-worker stack — the AWS EC2
   `m6i.4xlarge` recipe in `infra/` is retained but is no longer
   the canonical target. See Σ.C below for the honest single-host
   framing.

### Σ.A2

3. **Dialect-specific function aliases vs shared catalog.** Spark
   has ~200 functions; DuckDB has ~500. Some overlap with DataFusion
   names, some don't. Build one big `HashMap<(Dialect, &str), &str>`
   or per-dialect modules with their own remap tables? Initial
   implementation: per-dialect modules (clearer ownership; easier
   to test per dialect).
4. **Round-trip stability.** When a Spark SQL query goes through
   the translator + back through `Statement::to_string()`, is the
   output stable across `sqlparser` releases? If not, the output is
   unsuitable for caching. Likely needs a thin output canonicalizer
   layer in PR 2.

### Σ.B

5. **Wheel packaging strategy.** Ship Ballista binaries inside the
   main `ematix-flow` wheel (at `pip install ematix-flow[ballista]`
   extra), or as a separate `ematix-flow-ballista` PyPI package?
   Two-package model is cleaner separation of concerns; one-package
   is simpler for users. Decide before PR 2.
6. **Connector-trait refactor scope.** ~~Open question~~ **Resolved 2026-05-05.**
   Trait shape locked in [`docs/PHASE_SIGMA_B_TRAIT_SPIKE.md`](PHASE_SIGMA_B_TRAIT_SPIKE.md):
   `BackendConfig` tagged enum (Serialize + Deserialize) + new
   `fn config(&self) -> BackendConfig` method on the trait + free
   function `backend_from_config(cfg) -> Arc<dyn Backend>`. All 6
   sub-questions decided per the spike's recommended defaults;
   `as_postgres()` escape hatch removed in PR 1 to force the
   universal Arrow path. ~3-week migration across 4 staged commits.

### Σ.C

7. **Spark version baseline.** Pin to which Spark release — 3.5.x
   (most common in production) or 4.x (latest)? 3.5.x is the
   defensible default; 4.x as a follow-up if the gap is large.
8. **Cold-start measurement.** Where does "cold start" begin?
   Container start? First query submission? First result? Lock
   the definition in Σ.C PR 1 so the comparison is apples-to-apples.

### Σ.D

9. **Build-vs-adopt decision criteria.** What threshold of "Arroyo
   integrates cleanly" triggers Path A vs Path B? Recommend:
   if the spike's 50-line POC works end-to-end against a Kafka
   source + DataFusion SQL, Path A. If the POC needs Arroyo-specific
   source adapters, Path B (because we'd be locked into Arroyo's
   ecosystem rather than reusing our own).
10. **Streaming SQL surface in Path B.** Distributed streaming SQL
    is a much smaller subset than batch SQL — windowed aggregations,
    stream-stream joins, stream-table lookups. What's our Σ.D
    initial surface? Recommend: 39.4 + 39.5a + 39.5b feature parity,
    distributed. New surface (e.g., distributed CTEs) lands later.

---

## Sizing summary (mirrors `docs/ROADMAP.md` P5 block)

| Sub-phase | Effort | Calendar | Validates |
|---|---|---|---|
| Σ.A1 | 1 dev | 1–2 wk | DataFusion single-node ≥ PySpark single-node |
| Σ.A2 | 1 dev | 2–3 wk | Spark dialect runs on ematix-flow without rewrites |
| Σ.B  | 1 dev | 3–6 wk | Distributed batch SQL works at <150 MB image |
| Σ.C  | 1 dev | 1–2 wk | Public-facing benchmark vs PySpark |
| Σ.D  | 1 dev | 6–22 wk | Distributed streaming SQL (gated on spike) |
| **A1–C only** | | **~7–13 wk** | Production-ready vs Spark batch |
| **A1–D** | | **~13–35 wk** | Production-ready vs Spark batch + streaming |

A1 starts the moment v0.1.0 lands on PyPI. Σ.D's start is gated on
A1–C's TPC-H benchmark publishing — no point launching streaming
until the batch claim is benchmarked + announced.
