# Distributed TPC-H benchmark plan

**Goal:** measure ematix-flow's distributed query performance against
Spark and 1-2 other open-source distributed engines, using a shared
EKS cluster, shared S3 parquet, and a fixed query set (TPC-H 22).

**Primary comparator:** Apache Spark 3.5 on Kubernetes.
**Secondary:** Trino (mature SQL engine).
**Stretch:** Daft (newer Rust-based, distributes via Ray).

Single-node performance is already covered by the current AWS validation
campaign (`docs/AWS_VALIDATION_PLAN.md`) — this is the *fanned-out across
N workers* story we cannot test on a laptop.

## What we want to learn

1. **Scaling:** does ematix-flow get faster from 1→2→4→8 workers? Where
   does Spark's curve sit? Where does the engine become I/O-bound vs
   shuffle-bound?
2. **Absolute wall time** per query at SF=10 and SF=100, 4-worker fixed
   topology — the comparison most users care about.
3. **Cost per query** — wall time × node-hr cost. Spark on K8s
   typically wins on raw throughput but loses on $/TB-scanned when
   compared to vectorised columnar engines.
4. **Cold vs warm cache** — Spark's first-touch parquet scan is famously
   slow vs subsequent runs; how does ematix-flow compare with and
   without parquet metadata caching.

## Phases (incremental, each phase tears down before the next)

### Phase α — ematix-flow distributed bootstrap (~$1)

Stand up just the ematix-flow side. No comparators yet. Goal:
distributed flow worker image runs, partitions TPC-H queries across N
workers, and produces correct results that match the SF=1 single-node
oracle.

- **Cluster:** EKS 1.30, 4× m5.xlarge (4 vCPU, 16 GB) — bigger than the
  t3.medium in the current campaign because we need real work-per-node.
- **Data:** SF=10 lineitem (~7.5 GB parquet) regenerated on a fresh EC2
  and dropped in S3 under `s3://<bucket>/tpch/sf10/`, partitioned by
  `l_shipdate` month (~84 row groups).
- **Worker image:** reuse `infra/test-validation/Dockerfile.flow-worker`
  from the current campaign with `cargo build --release --bin flow-worker`.
  Worker reads a `WorkUnit` JSON from stdin describing query +
  partition range + S3 input prefix, writes Arrow IPC result to S3.
- **Coordinator:** runs on the campaign EC2. Calls `K8sJobExecutor` to
  submit N parallel Jobs; each pod gets `--query Q14 --partitions
  0:21 --bucket ...`; coordinator joins partial aggregates locally.
- **Test:** Q1, Q14, Q22 (cheap, mid, expensive) at SF=10, N ∈ {1, 2, 4}.
  Compare each result to the single-node oracle from current campaign's
  stage 06/07 outputs.
- **What we get:** correctness gate + the first scaling curve.

### Phase β — Spark 3.5 comparator (~$1.50)

Add Spark on the same EKS, same data, same queries.

- **Spark deployment:** [Spark on K8s operator](https://github.com/kubeflow/spark-operator)
  (cleaner than `spark-submit --master k8s://...` for benchmarking).
  Same 4× m5.xlarge node pool. Driver pod + 4 executor pods.
- **Spark config:** 3 GB executor memory, 3 cores, AQE on, Iceberg/Hive
  off (read parquet directly from S3 — no catalog dependency).
- **TPC-H queries:** the canonical SQL from
  [databricks/tpch-dbgen](https://github.com/databricks/tpch-dbgen)
  Q01-Q22. Same schema, same partitioning.
- **Harness:** spark-submit a Scala/Python job per query, time the
  collect() to driver; record [Spark UI](https://example.invalid) link
  for stage-level breakdown.
- **Apples-to-apples checklist:**
  - same S3 prefix, same parquet files (no `.parquet` re-write)
  - same `read_parquet` semantics — page index pruning on for both
  - JVM warm-up: discard run 1, time runs 2-5, report median
  - N workers: 1, 2, 4, 8 (same cluster, scale executor count)

### Phase γ — Trino comparator + SF=100 (~$3)

Trino's strength: low-latency SQL with mature parquet pushdown. Adds a
3rd point on the curve.

- **Trino deployment:** [trinodb/trino helm chart](https://github.com/trinodb/charts).
  1 coordinator + 4 workers on the same 4× m5.xlarge pool.
- **Catalog:** Hive metastore — minimal config pointing at the S3
  prefix from Phase α.
- **Queries:** same TPC-H 22.
- **SF=100 generation:** ~75 GB parquet. Use a beefier EC2 (c7i.4xlarge
  or `tpch_generate` on a r5.2xlarge) to write the data once, then
  benchmark all three engines against it.
- **Cost watch:** SF=100 + 3 engines × 22 queries × N=4 cluster ≈ 2 hr
  of m5.xlarge × 4 = ~$1.50, plus EKS control plane + Trino's
  coordinator overhead.

### Phase δ (stretch) — Daft + cost-per-query roll-up (~$1)

Daft is a Rust-based distributed engine using Ray for scheduling. Same
philosophy as ematix-flow (columnar, native, no JVM) so it's the
fairest open-source neighbour to compare against.

- **Daft deployment:** Ray on K8s via [KubeRay operator](https://github.com/ray-project/kuberay).
  Same 4× m5.xlarge pool. Daft `daft.read_parquet(...).sql(...)` from a
  driver pod.
- **Caveat:** Daft's TPC-H coverage is partial — confirm which queries
  it can express in its DataFrame DSL before timing them.
- **Final artifact:** `docs/BENCHMARKS-DISTRIBUTED.md` with one table
  per engine × scale-factor × N-workers grid + a "wall-clock × $/hr"
  column.

### Phase ε — teardown verification

Same model as the current campaign:

```
aws eks list-clusters --query 'clusters[?starts_with(@, `ematix-flow-bench-`)]'
aws ecr describe-repositories --query 'repositories[?starts_with(repositoryName, `ematix-flow-bench-`)]'
aws s3 ls | grep ematix-flow-bench || true
```

All empty == clean.

## Cost roll-up

| Phase | Service | Estimated cost |
|---|---|---|
| α | EKS 4× m5.xlarge (1 hr) + S3 generation | ~$1.00 |
| β | EKS same cluster (2 hr active) | ~$1.50 |
| γ | EKS + Trino + SF=100 data (3 hr) | ~$3.00 |
| δ | EKS + Daft (1 hr) | ~$1.00 |
| — | S3 storage SF=100 parquet (~75 GB × 1 day) | ~$0.05 |
| | **Total worst case** | **~$6.50** |

Phases are independent — α + β alone is the minimum useful answer (~$2.50)
and covers the user's primary ask of "ematix-flow vs Spark."

## Decisions to lock before starting

| # | Question | Default | Why |
|---|---|---|---|
| 1 | Worker instance type | m5.xlarge | Sweet spot: 4 vCPU, 16 GB RAM, $0.192/hr. Spark needs the memory; ematix-flow doesn't but it's the apples-to-apples constraint. |
| 2 | Spark version | 3.5.x | Latest GA. 4.0 is too new for stable behavior. |
| 3 | Storage layout | Parquet on S3, partitioned by `l_shipdate` month | Mirrors current single-node SF=10 layout. Avoids Iceberg/Delta indirection. |
| 4 | What "queries" means | TPC-H 22 SQL (databricks/tpch-dbgen) | Reference suite, lets us cite [DuckDB's benchmarks](https://duckdb.org/2022/05/27/tpch-benchmarks.html), Theseus, Photon, etc. as external comparators. |
| 5 | Statistics methodology | 5 runs, discard 1st (JVM warm-up), report median + p95 | Industry-standard for Spark; we extend the same to ematix-flow for fairness. |
| 6 | Spark→S3 driver | s3a:// via Hadoop-AWS 3.3.x | Avoids the EMR magic-committer path which would invalidate cross-engine timing. |
| 7 | Result correctness check | Per-query, hash the result set after sorting | TPC-H Q15 needs LATERAL subqueries; ordering matters. Hash post-sort guards against false positives. |
| 8 | Whether to also run on c7i instances | **Defer** | Sapphire Rapids AVX-512 is single-node's win. Distributed is shuffle/network-bound; instance type matters less. |

## Cross-cutting safeguards (carried over from current campaign)

- Single region (`us-east-2`).
- All resources tagged `Project=ematix-flow-bench`,
  `Owner=ryanevans23@gmail.com`, `MaxLifetimeHours=8`.
- AWS Budgets alert raised to $20 monthly (campaign expected $6.50).
- No NAT gateway, no EIP — public-IP EC2 + S3 VPC gateway endpoint
  keeps egress free.
- `terraform destroy` wired into Phase ε; janitor script grep-by-tag
  as belt + braces.

## Open follow-ups (NOT in scope for first run)

- **NUMA scaling** (deferred from current campaign) on c7i.metal — needs
  its own ~$15 single-instance campaign once we have the dist baseline.
- **Cross-region S3 read** latency study — interesting but expensive.
- **Photon comparison** — needs paid Databricks; defer.
- **Snowflake / BigQuery** — managed, paid, different cost model.
  Out of scope.

## Implementation surface

- `infra/distributed-tpch/` (new):
  - `main.tf` — EKS + ECR + S3 + IAM, reuses pieces of `test-validation`
  - `helm/` — Spark operator values, Trino values, KubeRay values
  - `coordinator/` — Python driver that fans out work to executors
  - `bench-spark.py`, `bench-trino.py`, `bench-flow.py`, `bench-daft.py`
  - `scripts/teardown-verify.sh`
- New `flow worker` subcommand (see spec below)

## `flow worker` CLI spec

The missing piece in `crates/ematix-flow-cli`: today the binary has
`flow consume` (long-running streaming pipeline) and `flow scheduler` /
`flow status` (operator surface). It does NOT have a "execute one
TPC-H subquery from a WorkUnit and exit" mode — that's what each K8s
pod and Lambda invocation needs.

### Invocation

```
flow worker \
  --work-unit-file /etc/work-unit/spec.json     # OR
  --work-unit-stdin                              # for Lambda

# OR with inline args (debug + local):
flow worker \
  --query Q14 \
  --tpch-dir s3://bucket/tpch/sf10 \
  --row-group-range 0:21 \
  --output s3://bucket/results/run-<uuid>/q14-shard-3.arrow
```

Exit codes:
- `0` — success, result written to `--output`
- `2` — config error (bad WorkUnit, unreachable S3, malformed query)
- `3` — execution error (query failed mid-run)
- `4` — output error (write to S3 failed)

Stdout is reserved for **structured JSON metrics** (one line on exit):

```json
{
  "work_unit_id": "wu-7a3f",
  "query": "Q14",
  "row_group_range": [0, 21],
  "rows_in": 6_001_215,
  "rows_out": 1,
  "wall_ms": 423,
  "decode_ms": 287,
  "exec_ms": 136,
  "output_uri": "s3://bucket/results/run-<uuid>/q14-shard-3.arrow"
}
```

Stderr is reserved for `tracing` logs (level configurable via
`RUST_LOG`). This split lets the coordinator parse stdout cleanly even
when logs are noisy.

### WorkUnit JSON schema

```json
{
  "$schema": "https://ematix-flow/work-unit.v1.json",
  "id": "wu-7a3f",
  "query": {
    "kind": "tpch",
    "id": "Q14"
  },
  "input": {
    "kind": "parquet_partition",
    "uri_prefix": "s3://bucket/tpch/sf10",
    "tables": ["lineitem", "part"],
    "row_group_range": {"lineitem": [0, 21]}
  },
  "output": {
    "kind": "arrow_ipc",
    "uri": "s3://bucket/results/run-c4f8/q14-shard-3.arrow"
  },
  "execution": {
    "threads": 4,
    "with_dict_preservation": true,
    "with_late_mat": true,
    "with_adaptive_predicate": true
  }
}
```

The `query.kind = "tpch"` discriminator lets us add `"kind": "sql"`
later (arbitrary SQL string), `"kind": "udf"`, etc. without breaking
the schema. Pin `$schema` so coordinators can version-detect.

### Sharding strategy (coordinator side)

For TPC-H 22, the cheapest fan-out is **lineitem-row-group sharding**:
lineitem dominates the I/O for every query that touches it, and
row-group boundaries align with parquet's prune unit. Each worker
reads its slice + the full smaller tables (orders, customer, part,
supplier, nation, region) — those fit in memory at SF=100.

```
N workers, lineitem has R row groups → each worker reads R/N row groups.
Worker output: per-shard partial aggregate (Arrow IPC).
Coordinator: read all N shards, run the final aggregate locally,
write to BENCHMARKS-DISTRIBUTED.md.
```

Caveat: queries with cross-table joins on lineitem need a smarter
plan (broadcast small dim tables, hash-partition on join key). For
Phase α scope we hand-write each query's partition strategy in the
coordinator (22 queries × ~5 lines each); revisit in Phase γ if we
want a general planner.

### Crate wiring

- New module: `crates/ematix-flow-cli/src/worker.rs`
- New struct: `WorkUnit` (serde Deserialize) in
  `crates/ematix-flow-distributed/src/work_unit.rs` (next to
  `LambdaExecutor` / `K8sJobExecutor` which produce these)
- Reuses: existing `ematix_flow_core::EmatixFastParquetTableProvider`
  + `with_dict_preservation` / `with_late_mat` builders. The worker is
  ~80% glue: parse → build session → run query → write Arrow IPC → exit.

### What changes in `LambdaExecutor` + `K8sJobExecutor`

Both already produce a Job/invocation that runs the flow binary. Today
they pass a TOML config; we add a `WorkUnitExecutor` trait method that
serialises a `WorkUnit` to JSON, mounts it as a ConfigMap (K8s) or
passes it as the event payload (Lambda), and the worker reads it via
`--work-unit-file` or `--work-unit-stdin`.

Existing Lambda/K8s wiring stays — this is additive.

### Test plan for the worker subcommand

- Unit: `WorkUnit` round-trips through JSON (happy path + each error mode)
- Integration: spawn `flow worker --work-unit-file ...` on a SF=1
  lineitem file in a tmpdir, assert Arrow IPC output matches the
  reference Q14 result hash from `test_tpch_triangulation.rs`
- Real-AWS: stage `41-distributed-tpch-worker` in the future campaign,
  one K8s Job that consumes a SF=10 lineitem row-group shard

### Engineering estimate

- Worker subcommand + WorkUnit + smoke test: **2 days**
- Coordinator (Python, drives K8s Jobs, merges partial Arrow IPC): **2 days**
- Spark TPC-H runner (Scala or PySpark): **1 day** (the SQL is canonical)
- Bench harness wiring (timing, statistics, BENCHMARKS-DISTRIBUTED.md): **1 day**
- Buffer for cross-engine apples-to-apples calibration: **2 days**

Total Phase α + β implementation: **~1 working week**.

## What we publish

- `docs/BENCHMARKS-DISTRIBUTED.md` — the headline table.
- Per-engine traces (Spark UI screenshots, ematix-flow run log JSON).
- A blog post / README section: "TPC-H 22 at SF=100 across 4 nodes:
  ematix-flow vs Spark vs Trino" with the cost-per-query roll-up.

## Locking sequence

1. **Land current campaign + Σ.E3b PRs** (in progress).
2. **Phase α implementation**: ~1 week. Worker binary, coordinator,
   correctness check vs single-node oracle.
3. **Phase β implementation**: ~3 days. Spark operator + TPC-H runner
   + harness.
4. **Run Phase α + β**: half a day, ~$2.50.
5. Decide whether to invest in γ + δ based on what α + β tell us.
