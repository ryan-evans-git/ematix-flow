# Benchmarks

Measurement notes + canonical baselines for ematix-flow's performance
gates. Subsequent PRs that touch `transform.rs` or upgrade DataFusion
re-run the relevant suite + diff against the baseline; any regression
≥10% needs a justification in the PR body.

Plan: [`docs/PHASE_SIGMA_PLAN.md`](PHASE_SIGMA_PLAN.md).

---

## Σ.A1 — TPC-H representative set at SF=1

The Σ.A1 audit-by-running anchor: Q1 / Q3 / Q6 / Q19 against
single-node DataFusion 53.1, reading Snappy Parquet from
`examples/tpch/data/sf1/`. Materializes results into `Vec<RecordBatch>`
so we measure execution wall-time, not just plan-building.

### Σ.A1 PR 2 baseline (2026-05-05)

Hardware: Apple M3 Pro, 36 GB RAM, macOS 15. SF=1 dataset = 320 MB
across 8 Parquet files (lineitem.parquet 202 MB, orders.parquet
52 MB, partsupp.parquet 39 MB, ...).

Method: criterion `sample_size = 10`, `measurement_time = 20s`,
warm-up 3s. Median of 10 samples reported with [low, high] from
criterion's bootstrap CI. Saved as baseline `sigma_a1_sf1_m3pro`.

| Query | Median | Range | Iterations | Workload shape |
|---|---|---|---|---|
| Q1  | **48.7 ms** | [48.5, 48.8]  | 440  | scan + 10-aggregate group-by |
| Q3  | **34.6 ms** | [34.4, 34.7]  | 605  | 3-way hash join + ORDER + LIMIT 10 |
| Q6  | **18.2 ms** | [18.2, 18.3]  | 1155 | scan + filter + single sum |
| Q19 | **38.0 ms** | [37.4, 38.4]  | 605  | 2-way join + complex disjunctive WHERE |

### Σ.C PR 1 — distributed batch SQL on the same TPC-H reps (2026-05-05)

First public numbers for the `DistributedBackend` path.
`crates/ematix-flow-distributed/benches/tpch_distributed.rs` runs
the same Q1/Q3/Q6/Q19 reps as the Σ.A1 single-node bench, but
through `DistributedBackend::read_arrow_stream`. Two
configurations:

- **distributed-of-one** (`peers = []`): degenerate single-worker
  cluster. Vanilla DataFusion under the hood; isolates the trait-
  surface cost of going through `DistributedBackend` vs raw
  `SessionContext`.
- **3 in-process workers**: spawns 3 tonic-served `Worker`s on
  free localhost ports, points the coordinator at them via
  `StaticWorkerResolver`. Exercises `DistributedPhysicalOptimizerRule`
  + cross-"pod" Arrow Flight at the floor RPC cost.

M3 Pro / SF=1 / criterion `sample_size = 10`,
`measurement_time = 15s`:

| Query | DF (1-node) | distributed-of-one | 3 in-process workers | of_one overhead | 3-worker overhead |
|---|---|---|---|---|---|
| Q1  | 48.7 ms | **48.9 ms** | **53.9 ms** | +0.4% | +10.7% |
| Q3  | 34.6 ms | **34.8 ms** | **37.1 ms** | +0.6% | +7.2% |
| Q6  | 18.2 ms | **18.5 ms** | **19.9 ms** | +1.6% | +9.3% |
| Q19 | 38.0 ms | **38.3 ms** | **43.9 ms** | +0.8% | +15.5% |

**Findings:**

1. **Trait-surface overhead is ≤1.6%.** Going through
   `DistributedBackend` instead of constructing a raw
   `SessionContext` adds essentially no cost. Validates the Σ.B
   design — the distributed code path costs nothing when no peers
   are configured.

2. **3-worker RPC overhead at SF=1 is 7–15%** — RPC + plan-
   serialization cost dominates the work for queries that finish
   in <50ms. This is the *floor* of distributed cost, not a real
   workload result. Expected crossover (where distributed wins) is
   at SF=10+ where per-query work scales past the fixed RPC cost.

3. **Cross-host numbers require EC2.** This bench runs all 3
   workers on the same machine, so RPC latency is loopback (~50µs).
   Real cross-host m6i.4xlarge × 4 numbers add network latency
   (~500µs intra-AZ) + I/O contention. They go in Σ.C PR 2 once
   we have the cluster provisioned.

**Reproducer:**

```sh
TPCH_MEASUREMENT_TIME_S=15 \
  cargo bench -p ematix-flow-distributed --bench tpch_distributed
```

Reads from `examples/tpch/data/sf1/`. Generate first via
`cargo run --release -p ematix-flow-core --example tpch_generate
-- --sf 1 --out examples/tpch/data/sf1`.

**What's still open in Σ.C:**

- **PR 2 — multi-host comparison**: 3-pod ematix-flow-distributed
  cluster vs 3-executor PySpark on identical EC2 hardware. Same
  TPC-H queries, both at SF=10 + SF=100. Requires AWS
  provisioning.
- **PR 3 — repro script + announcement-ready artifact**:
  `scripts/tpch-bench.sh` that brings up both clusters + runs the
  suite + emits the comparison table.

### Σ.A2 PR 5 — DuckDB-dialect audit (2026-05-05)

DuckDB's SQL surface is closer to DataFusion's than Spark's (both
inherit Postgres-style syntax + arrow-rs types). The translator's
expected scope was a handful of function-name aliases; in practice
the remap table has one entry: `list_value → make_array`.

**Acceptance: ≥90% pass rate on TPC-H + curated set.**

- TPC-H Q1/Q3/Q6/Q19 e2e through `dialect = "duckdb"` → DataFusion:
  **4/4 PASS** (revenue + row count match TPC-H reference).
- DuckDB unit tests (`tests/dialect_duckdb.rs`): **13/13 PASS** —
  pass-through (basic SELECT, aggregates, joins, window, CTE, INTERVAL
  literal), `list_value → make_array` remap, error paths.
- TPC-DS suite under `dialect = "duckdb"` (same 103 Spark-canonical
  queries from PR 4): **103/103 PASS**. DuckDB's parser is
  permissive enough to accept Spark-style queries; the only DuckDB-
  specific function (`list_value`) doesn't appear in TPC-DS.

```sh
cargo run --release -p ematix-flow-core --example tpcds_dialect_audit -- duckdb
# === Σ.A2 dialect audit (DuckDb) ===
# total:           103
# PASS:            103 (100.0%)
```

**Implication**: DuckDB→DataFusion is essentially a no-op for the
canonical query surface. Translator value-add is in cases where
users explicitly use DuckDB-isms (currently just `list_value`); add
new entries to `DUCKDB_TO_DF` as real-world queries surface gaps.

Σ.A2 status after PR 5: all 5 sub-PRs done. Acceptance gates met
(or vastly exceeded). Ready to start Σ.B (Ballista trait refactor)
or pause to ship v0.1.0.

### Σ.A2 PR 4 — TPC-DS Spark-dialect audit (2026-05-05)

Plan-only translator audit: each of the 103 official Apache Spark
TPC-DS queries (99 numbered + 4 a/b variants like `q14a`, `q23a`)
fed through `dialect::translate(_, Dialect::Spark)`, output handed
to a `SessionContext` registered with the canonical TPC-DS schema
(24 tables, types translated `CHAR(N) → VARCHAR`). DataFusion's
`create_physical_plan` exercises the planner. No data; no execution.

```text
=== Σ.A2 PR 4 audit ===
total:           103
PASS:            103 (100.0%)
TRANSLATE_FAIL:  0
PLAN_FAIL:       0

acceptance gate: ≥80% PASS — MET
```

**Headline: 100% PASS, no findings.** PR 2's function-name remap +
PR 3's LATERAL VIEW EXPLODE rewrite + DataFusion 53's native SQL
surface together cover the entire TPC-DS Spark dialect. The plan's
speculative concerns (correlated subqueries, complex-type literals,
window-frame INTERVAL syntax) all just work — either the queries
don't lean on those constructs, or DataFusion accepts them as-is.

**Audit reproducer:**

```sh
cargo run --release -p ematix-flow-core --example tpcds_dialect_audit
```

Reads `examples/tpcds/queries/spark/q*.sql` (Apache-Spark-canonical)
and `examples/tpcds/schema.sql` (auto-generated from
`apache/spark`'s `TPCDSSchema.scala`). Per-query failures land on
stderr with the underlying error; clusters by error-message-prefix
in the summary so common root causes are obvious.

**What this implies for Σ.A2:**

- PR 4's "iterate on translator gaps until ≥80%" task is a no-op
  for the canonical TPC-DS suite. Any future translator work
  triggers from real-world user queries that surface idioms the
  audit didn't catch.
- The 103 .sql files stay in the repo as a regression suite — any
  future change to the dialect translator or DataFusion bump gets
  re-checked against them via `tpcds_dialect_audit`.
- Σ.A2 acceptance is met. PR 5 (DuckDB dialect) is the next
  remaining sub-phase work item.

### Σ.A1 audit findings

**None.** All four queries execute cleanly through DataFusion 53.1
with no SQL surface gaps. The Σ.A1 plan reserved a PR 3 for "fix
audit gaps surfaced by the benches" — that PR will be a no-op for
the representative set. Σ.A2 (dialect translator) + Σ.C (TPC-H full
22-query suite) will surface gaps the four representative queries
don't reach (correlated subqueries, certain CTEs, complex-type
literal forms).

### Σ.A1 PR 4 head-to-head: DataFusion vs single-node PySpark (2026-05-05)

Same M3 Pro host, same SF=1 Parquet under `examples/tpch/data/sf1/`,
same `.sql` files. DataFusion via the criterion bench above; PySpark
via `scripts/bench-tpch-pyspark.py` (3 trials per query after a
discarded JIT warm-up; median reported).

Software baseline: DataFusion 53.1 (workspace pin), PySpark 4.1.1,
OpenJDK 23.0.2 (Homebrew). Spark configured `local[*]` (12 logical
cores), 4 GB driver heap, `spark.sql.adaptive.enabled = true`,
`shuffle.partitions = 8`.

| Query | DataFusion (ms) | PySpark (ms) | DataFusion / PySpark | PySpark trials min/max | rows |
|---|---|---|---|---|---|
| Q1  | **48.7**  | 192.6 | **0.253** (DF 4.0× faster) | 191.1 / 324.6 |  4 |
| Q3  | **34.6**  | 235.5 | **0.147** (DF 6.8× faster) | 228.5 / 437.2 | 10 |
| Q6  | **18.2**  |  64.2 | **0.283** (DF 3.5× faster) |  55.1 / 186.8 |  1 |
| Q19 | **38.0**  | 130.8 | **0.290** (DF 3.4× faster) | 112.0 / 342.6 |  1 |

**Geomean speedup: ~4.3×.** Σ.A1 PR 4 acceptance gate was DataFusion
median ≤ PySpark median on all four with geomean ≥1.5×; cleared
comfortably.

Why DataFusion wins by this margin on these workloads:
- No JVM cold-start tax (Spark first-trial is consistently 1.5–3×
  slower than median; DataFusion has none of that).
- Vectorized scan + arrow-rs batch dispatch outperforms Spark's
  Tungsten codegen on small/medium queries because Spark's
  optimizer + planner overhead is amortized over fewer rows.
- No shuffle in any of these four queries (Q3's join is broadcast at
  this scale); both engines run single-stage. SF=10/100 with bigger
  shuffle is where Spark's distributed plan helps + its single-node
  edge narrows.

Row counts match exactly across both engines (Q1: 4, Q3: 10, Q6: 1,
Q19: 1) — confirms the SQL produces equivalent results on both.

### Σ.A1 PR 4 follow-up: vs Polars (2026-05-05)

Polars is the closest peer to DataFusion in positioning (Rust under
Python, in-process, vectorized), so a head-to-head matters even
though both engines are single-node only. Same M3 Pro / SF=1 /
Parquet / .sql files. Polars 1.40.1 via `polars.SQLContext`. Median
of 3 trials after 1 discarded warm-up.

| Query | DataFusion (ms) | Polars (ms) | PySpark (ms) | DF/Polars | Polars/PySpark | rows |
|---|---|---|---|---|---|---|
| Q1  | 48.7 | **FAIL**  | 192.6 | — | — | — |
| Q3  | 34.6 |  46.8     | 235.5 | 0.739 (DF 1.35× faster) | 0.199 (Polars 5.0× faster than Spark) | 10 |
| Q6  | 18.2 | **10.0**  |  64.2 | **1.82 (Polars 1.82× faster)** | 0.156 (Polars 6.4× faster than Spark) | 1 |
| Q19 | 38.0 | 366.3     | 130.8 | 0.104 (DF 9.6× faster) | 2.800 (Polars 2.8× **slower** than Spark) | 1 |

**Audit findings — surface for Σ.A2 dialect translator:**

- **Q1 fails on Polars**:
  `SQLSyntaxError: unsupported interval syntax ('INTERVAL '90' DAY')`.
  TPC-H spec uses `INTERVAL` literals; Polars's SQL parser is younger
  and doesn't yet accept them. A Polars-targeted dialect translator
  would rewrite `DATE 'X' - INTERVAL 'N' DAY` → `DATE 'X-N-days'`
  (concrete literal). Σ.A2 future work.
- **Q19 collapses on Polars** (9.6× slower than DataFusion, 2.8×
  slower than PySpark). The 3-clause disjunctive `WHERE (... OR ... OR
  ...)` over a 2-way join apparently doesn't simplify into something
  Polars's optimizer can vectorize. Worth a docs.rs investigation
  but not a Σ.A1 blocker — DataFusion handles this cleanly.

**Where Polars wins**: Q6 (vectorized filter + single sum) beats
DataFusion 1.82×. Polars's tight scan-+-aggregate loop is well-tuned
for this workload shape. Same-engine-family Polars-vs-DataFusion is
close (~10–80 ms swings); DataFusion's win on the suite comes from
SQL coverage + complex-query robustness, not raw scan speed.

#### Q6 tuning audit (2026-05-05)

We swept `SessionConfig` knobs to see if any close the Polars gap on
Q6. None do; in fact the in-decoder filter knobs make it worse.
Reproducer: `cargo run --release -p ematix-flow-core --example
tpch_q6_tune`.

| Config | Median (ms) |
|---|---|
| default | 16.9 |
| + `target_partitions=12` | 17.2 |
| + `repartition_file_scans=true` | 17.1 |
| + `parquet.pushdown_filters=true` | **28.3** (worse) |
| + `parquet.reorder_filters=true` | **62.9** (much worse) |

`target_partitions` is already at `num_cpus::get()` by default;
DataFusion already splits the Parquet into 12 byte-range scan groups
automatically (visible in the EXPLAIN as
`file_groups={12 groups: [[…0..17M], …]}`). The Q6 predicates are
cheap enough to evaluate post-decode on Arrow batches; pushing them
into the Parquet decoder pays a per-batch filter-mask cost without
recovering it.

**Implications for the bench harness + Σ.B work:**
- Keep the criterion bench's `SessionContext::new()` (no custom
  config). The defaults are right.
- **Do not** globally enable `pushdown_filters` — it hurts simple-
  aggregate queries like Q6.
- Polars's 1.82× edge on Q6 is hand-tuned vectorized inner loops,
  not a config gap. Closing it from here would need profiling
  DataFusion's vectorized aggregate path + likely upstream PRs.
  Out of scope for Σ.A1; revisit if Σ.C's TPC-H suite shows
  systematic per-query gaps.

**Net for the Σ.A1 rep set**:
- DataFusion: clean wins on Q1 (Polars fails), Q3 (1.35×), Q19 (9.6×).
- DataFusion: loses to Polars on Q6 (1.82× the wrong way).
- Both crush PySpark by 3–6× single-node, except Polars's Q19
  collapse where Spark's optimizer gets a rare win.

This makes the case for ematix-flow's positioning: same-class single-
node performance to Polars on hot loops, broader SQL surface,
distributed scaling path via Σ.B (Polars has no distributed story).

### JDK note

PySpark 4.x officially supports JDK 17 / 21. Homebrew's `openjdk`
cask installs 23, which works once Spark is started with
`-Djava.security.manager=allow` (JDK 18+ deprecated
`Subject.getSubject` and Spark's UGI shim still calls it). The script
sets that flag automatically. If running on JDK 17 / 21, the flag is
harmless.

### Reproducing the head-to-head

```sh
# 1. Generate data + run DataFusion benches (Σ.A1 PR 1 + 2 above).
cargo run --release -p ematix-flow-core --example tpch_generate -- \
    --sf 1 --out examples/tpch/data/sf1
cargo bench -p ematix-flow-core --bench tpch -- \
    --save-baseline sigma_a1_sf1_<host>

# 2. Activate the venv with PySpark installed; ensure Java is on PATH.
source .venv/bin/activate
export JAVA_HOME=/opt/homebrew/opt/openjdk  # or wherever
export PATH="$JAVA_HOME/bin:$PATH"

# 3. Run the head-to-head.
python scripts/bench-tpch-pyspark.py

# 4. Edit DATAFUSION_BASELINE_M3PRO_SF1_MS in the script to match
#    your host's DataFusion numbers if you've re-baselined; the
#    DF/PySpark ratio in the output table assumes M3-Pro DF numbers
#    by default.

# 5. Polars sibling: no Java needed.
python scripts/bench-tpch-polars.py
```

If running on Linux x86_64 EC2 m6i.4xlarge for Σ.C, both columns
will need re-running. Σ.A1 numbers are the M3-Pro reference; Σ.C
will land the canonical Linux numbers alongside Ballista cluster
results.

### Reproducing

```sh
# 1. Generate SF=1 data (~10s, 320 MB).
cargo run --release -p ematix-flow-core --example tpch_generate -- \
    --sf 1 --out examples/tpch/data/sf1

# 2. Run all four benches (~90s wall-clock with the defaults).
cargo bench -p ematix-flow-core --bench tpch

# 3. Run a single query for fast iteration:
cargo bench -p ematix-flow-core --bench tpch -- q06

# 4. Save / compare against a named baseline:
cargo bench -p ematix-flow-core --bench tpch -- --save-baseline mine
cargo bench -p ematix-flow-core --bench tpch -- --baseline mine

# 5. Tighten the measurement window for fast iteration / loose for CI:
TPCH_MEASUREMENT_TIME_S=10 cargo bench -p ematix-flow-core --bench tpch
```

If the data dir lives somewhere else, point the bench at it via
`TPCH_DATA_DIR=/path/to/sf1 cargo bench -p ematix-flow-core
--bench tpch`. Bench panics with a clear message if any expected
Parquet file is missing.

### When to re-run

- **PRs that touch `crates/ematix-flow-core/src/transform.rs`** or
  any of the `LazySqlTransform` / `DataFusionTransform` machinery.
  Compare against `sigma_a1_sf1_m3pro` if running on M3-class
  hardware; otherwise capture a fresh baseline on the PR's host.
- **DataFusion / arrow upgrades.** Any version bump in
  `Cargo.toml`'s `datafusion` or `arrow-*` workspace deps. Diff
  against the baseline; flag regressions ≥10% in the PR body.
- **Σ.B Ballista work.** Once a `BallistaBackend` exists, run the
  same four queries against it and report side-by-side. Σ.C lands
  the full 22-query head-to-head.

### Hardware variance

Numbers above are from M3 Pro (Apple Silicon). Linux x86_64
m6i.4xlarge baseline lands in Σ.A1 PR 4 alongside PySpark numbers;
expected within ~2× of M3 Pro on these queries.
