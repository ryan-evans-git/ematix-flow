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
