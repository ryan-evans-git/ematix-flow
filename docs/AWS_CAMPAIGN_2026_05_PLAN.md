# AWS validation campaign — 2026-05 plan

Refresh of the Tier 3 campaign for the post-Σ.P codebase. Two distinct
runs:

1. **Single-node @ SF=1 and SF=10** — `ematix-flow` vs **Polars** vs
   **DuckDB**, 20 trials × 5 warmups
2. **Distributed @ SF=10 and SF=100** — `ematix-flow distributed` vs
   **PySpark distributed** vs **Trino**, 5 trials × 2 warmups

Output: raw run data + per-engine logs in S3; headline tables in
`BENCHMARKS-SINGLE.md` + `BENCHMARKS-DISTRIBUTED.md` committed to repo.

## Why this shape

- Single-node is the proven-fast story we already lead on; 20 trials
  gives us publishable medians (cf. our 2026-05-22 finding that 3-10
  trials lie on sub-15ms queries).
- Distributed at SF=10/100 is the scale story we haven't published.
  PySpark + Trino are the canonical "distributed SQL on parquet" peers
  prospective users evaluate. Beating both is the headline.

## Cost roll-up

| Phase | Resources | Wall time | $ |
|---|---|---|---|
| A1: SF=1 + SF=10 single-node | 1× c7i.2xlarge spot | ~1.5 hr | ~$0.50 |
| A2: SF=100 data generation | 1× c7i.4xlarge spot + 200GB EBS | ~45 min | ~$1.00 |
| B: SF=10 distributed × 3 engines | 4× c7i.2xlarge spot | ~3 hr | ~$10 |
| C: SF=100 distributed × 3 engines | 4× c7i.4xlarge spot | ~6 hr | ~$30 |
| D: S3 storage of generated data + results | ~150 GB × 24h | — | ~$0.50 |
| **Total** | | ~12 hr | **~$42** |

Kill switch: each phase is independently terraform-gated. If something
breaks mid-campaign we tear down the part that's done and skip the
rest.

## What changes vs the existing campaign

### Single-node (already implemented, needs tuning)

`infra/test-validation/scripts/bench.sh` stages 06/07 already run
`tpch_triangulation_bench` with the right engine list (ematix vs DuckDB
vs Polars). Defaults are 5 trials × 2 warmups. Two-line change:

```diff
- stage "06-tpch-triangulation-sf1" '...'
+ stage "06-tpch-triangulation-sf1" '
+   TPCH_TRIALS=20 TPCH_WARMUPS=5 \
+   TPCH_DATA_DIR=examples/tpch/data/sf1 ...
```

### Distributed (new)

New phase needs:

1. **Terraform: 4-node cluster** — `infra/test-validation-distributed/`
   - 1× coordinator + 3× workers, same AZ, security group allows
     intra-cluster traffic
   - All nodes pull `s3://$bucket/tpch-data/sf{10,100}/` as the source
     parquet
2. **ematix-flow distributed runner** — extend `flow-worker` docker
   image (already built in Phase D) with a new entrypoint:
   `tpch-distributed-bench` that:
   - Connects to the 3 workers via Arrow Flight
   - Reads parquet from S3 (already supported)
   - Runs all 22 TPC-H queries × 5 trials + 2 warmups
   - Writes results to `s3://$bucket/results/$stamp/ematix-sf{10,100}.json`
3. **PySpark deployment** — `infra/distributed-peers/pyspark/`
   - Spark 3.5 standalone cluster on the same 4 nodes (master + 3
     workers)
   - `tpch_bench.py` runs queries via `spark.sql(open('q01.sql').read())`
   - Output: same JSON shape as ematix runner
4. **Trino deployment** — `infra/distributed-peers/trino/`
   - Trino 440 on the same 4 nodes (coordinator + 3 workers)
   - Hive connector reads parquet from S3 (no Hive Metastore — use the
     `hive.metastore=file` configured against a local Glue-free catalog,
     OR use AWS Glue Data Catalog if it's simpler; pick whichever is
     cheaper)
   - `tpch_bench.sh` runs queries via `trino-cli --execute "$(cat q01.sql)"`
   - Output: same JSON shape

### Output schema

Each engine writes one JSON per (engine, scale) pair:

```json
{
  "engine": "ematix" | "pyspark" | "trino",
  "scale_factor": 10,
  "cluster_size": 4,
  "queries": {
    "Q01": {
      "trials_ms": [27.4, 28.1, ...],
      "median_ms": 27.6,
      "p95_ms": 28.5,
      "rows_returned": 4
    },
    ...
  }
}
```

A Python aggregator (`scripts/aggregate_distributed_bench.py`) reads all
three JSONs and produces a markdown comparison table.

## Build order

1. **Phase A tuning** — bench.sh tweak, single-node 20-trial run
   (verify in isolation, ~$1)
2. **SF=100 data gen** — terraform variant that just generates data and
   uploads to S3, ~$1, one-time
3. **Distributed terraform** — 4-node cluster module, no engines yet,
   smoke test SSH + intra-cluster connectivity
4. **ematix distributed bench script** — get it running on the cluster
   first (no peers comparison yet), validate output shape
5. **Trino deployment** — add to cluster, validate it can read SF=10
   parquet from S3, run a single query
6. **PySpark deployment** — same gate
7. **Full distributed sweep** — all three engines × SF=10 × 22 queries
8. **SF=100 distributed sweep** — gated on (7) succeeding
9. **Aggregator + commit** — assemble into BENCHMARKS-DISTRIBUTED.md

Each step is independently kill-able. (3)-(8) all build on the same
4-node terraform; only (8) needs the bigger c7i.4xlarge variant.

## What we explicitly aren't doing

- **Spark Thrift Server / SparkSQL CLI** — using `pyspark` directly.
  Less ops overhead, same query engine.
- **Glue / Athena / managed services** — they're not distributed
  engines you self-host, so they're a different comparison ("managed
  warehouse" not "self-host distributed SQL").
- **Multi-cloud or cross-region** — single AZ, single account.
- **Real network latency between coordinator and workers** — all in
  one AZ. The point is to measure the engines' parallel-execution
  efficiency, not network shape.

## Resolved risks

- **Trino + S3 metastore** → use **AWS Glue Data Catalog**. Trino's
  `hive.metastore=glue` connector reads parquet from S3 via Glue,
  which acts as a managed Hive metastore. Negligible Glue cost at our
  scale. No local metastore to maintain.
- **SF=100 RAM headroom** → **c7i.4xlarge (32 GB)** for the SF=100
  phase only. SF=10 phase stays on c7i.2xlarge. Step-up adds ~$15 to
  the campaign total but eliminates the Q18/Q21 OOM risk.
- **PySpark startup framing** → **report both wall-time and
  post-warmup**. Two columns per engine in the headline table: "first
  trial (includes warm-up cost)" and "median of trials 3-5". Lets the
  reader judge cold-start vs steady-state independently.

## Next step

Confirm the plan + build order. Then start with Phase A tuning (the
quick win + lowest risk).
