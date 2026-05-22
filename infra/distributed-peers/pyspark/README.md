# PySpark distributed peer — TPC-H bench

Phase B4 of the AWS validation campaign (see
`docs/AWS_CAMPAIGN_2026_05_PLAN.md`). This directory provisions a
**Spark 3.5.4 standalone cluster** across the 4-node TPC-H benchmark
fleet and runs the 22 canonical queries against S3-resident parquet,
producing a JSON results file in the campaign's shared schema.

The terraform that brings the EC2 nodes up is built in parallel — these
scripts assume the cluster already exists and is reachable.

## Layout

| File         | What it does                                            |
|--------------|---------------------------------------------------------|
| `install.sh` | Per-node bootstrap. Installs JDK 21, Spark, s3a JARs, writes config, starts systemd. |
| `bench.py`   | Runs all 22 TPC-H queries on the cluster, emits JSON, uploads to S3. |
| `README.md`  | This file.                                              |

## Versions (pinned)

| Component             | Version  |
|-----------------------|----------|
| Spark                 | 3.5.4    |
| Bundled Hadoop        | 3.3.4    |
| `hadoop-aws` JAR      | 3.3.4    |
| `aws-java-sdk-bundle` | 1.12.262 |
| OpenJDK (Corretto)    | 21       |
| Python                | 3.12     |

Changing any of these requires re-validating the JAR matrix —
`hadoop-aws` is strict about the SDK version it links against.

## Install order

For each of the 4 EC2 nodes (master + 3 workers) — typically via
cloud-init `user_data`, but works fine when run interactively too:

```bash
# On the master node:
sudo bash install.sh --role master --master-host "$COORDINATOR_PRIVATE_IP"

# On each worker (with the *same* master IP):
sudo bash install.sh --role worker --master-host "$COORDINATOR_PRIVATE_IP"
```

`install.sh` is idempotent — re-running it re-applies config and
bounces the relevant systemd unit. The script auto-detects the EC2
instance type (`c7i.2xlarge` → 12 GB worker / `c7i.4xlarge` → 28 GB
worker) and sizes worker memory accordingly. Cores = `nproc - 1`.

Verify the cluster is up by hitting the master's web UI:

```bash
curl -sS http://${COORDINATOR_PRIVATE_IP}:8080/ | grep -E "Workers|Alive"
# expect: 3 alive workers
```

## Running the benchmark

From the master node:

```bash
export BENCH_BUCKET=my-tpch-bench-bucket   # same one the data lives in

# install pyspark + boto3 once
python3.12 -m pip install --user pyspark==3.5.4 boto3

# run SF=10 (5 trials × 2 warmups by default)
python3.12 bench.py \
    --sf 10 \
    --bucket "$BENCH_BUCKET" \
    --trials 5 \
    --warmups 2
```

Result file lands at:

* local: `./pyspark-sf10.json`
* s3:    `s3://$BENCH_BUCKET/results/$STAMP/pyspark-sf10.json`

Where `$STAMP` defaults to a UTC `YYYYMMDD-HHMMSS` and can be pinned
with `--stamp` to align with the other engines' results in the same
campaign run.

### Environment variables expected

| Variable        | Used by    | Notes                                          |
|-----------------|------------|------------------------------------------------|
| `BENCH_BUCKET`  | operator   | S3 bucket holding `tpch-data/sf{10,100}/*.parquet` and the `results/` prefix. Passed to `bench.py --bucket`. |
| `AWS_REGION`    | install.sh | Defaults to `us-east-1`; override in `spark-defaults.conf` if running elsewhere. |
| `JAVA_HOME`     | systemd    | Set automatically by `install.sh` to the JDK 21 install path. |

No static AWS credentials anywhere — every node uses its EC2 instance
profile via `InstanceProfileCredentialsProvider`.

## SQL dialect adaptation

`bench.py` reads the queries from `examples/tpch/queries/q{01..22}.sql`
(the same files DataFusion uses) and applies a small set of mechanical
rewrites in `adapt_sql_for_spark()`:

| Query | Construct                                  | Rewrite                              |
|-------|--------------------------------------------|--------------------------------------|
| Q01   | `interval '90' day`                        | `interval 90 day` (unquoted numeral) |
| Q22   | `substring(c_phone from 1 for 2)` (ANSI)   | `substring(c_phone, 1, 2)`           |

Everything else — date literals, `extract(year from ...)`, CTEs with
column aliases, NOT EXISTS, scalar subqueries — parses in Spark 3.5
natively.

## Output schema

One JSON per `(engine, scale)` pair, matching the campaign aggregator:

```json
{
  "engine": "pyspark",
  "version": "3.5.4",
  "scale_factor": 10,
  "cluster_size": 4,
  "stamp": "20260522-143012",
  "trials": 5,
  "warmups": 2,
  "queries": {
    "Q01": {
      "trials_ms": [...],
      "median_ms": 0.0,
      "p95_ms": 0.0,
      "first_trial_ms": 0.0,
      "median_trials_3_5_ms": 0.0,
      "rows_returned": 0
    }
  }
}
```

`first_trial_ms` is the first **measured** trial (post-warmup);
`median_trials_3_5_ms` is the post-warmup steady-state median. The
campaign reports both — PySpark's first-trial cost is real (Catalyst
codegen, classloader churn) and we want to disclose it rather than
hide it behind warmups.

## Troubleshooting

### `java.nio.file.AccessDeniedException: s3a://... InstanceProfileCredentialsProvider`

The EC2 IAM role doesn't have `s3:GetObject` on the bucket. Confirm with:

```bash
aws s3 ls s3://$BENCH_BUCKET/tpch-data/sf10/
```

…run from one of the worker nodes (not your laptop). If that fails, fix
the IAM policy on the EC2 role — `install.sh` does not touch IAM.

### Worker doesn't show up in master web UI

Most often the master IP supplied to `--master-host` is wrong, the
security group is blocking 7077 between hosts, or DNS resolution
differs between master and worker. Check from a worker:

```bash
nc -zv $COORDINATOR_PRIVATE_IP 7077    # must succeed
journalctl -u spark-worker -n 100 --no-pager
```

The worker's logs will say "Failed to connect to master" with the host
it actually tried.

### `OutOfMemoryError: Java heap space` at SF=100

Expected on Q18 / Q21 with the SF=10 worker sizing. Re-bootstrap the
SF=100 cluster on `c7i.4xlarge` instances — `install.sh` will detect
the larger box and set `SPARK_WORKER_MEMORY=28g` automatically. If
you're already on `c7i.4xlarge` and still OOMing, lower
`spark.sql.shuffle.partitions` and/or
`spark.sql.autoBroadcastJoinThreshold` in `spark-defaults.conf`.

### `Connection reset by peer` reading parquet from S3

s3a's default thread pool is small. We bump it in `spark-defaults.conf`
(`fs.s3a.threads.max=64`, `fs.s3a.connection.maximum=200`), but at
SF=100 you may want to push these higher. Spark logs at WARN level
will show the s3a retry loop.

### `ClassNotFoundException: org.apache.hadoop.fs.s3a.S3AFileSystem`

The `hadoop-aws` JAR didn't land in `/opt/spark/jars/`. Re-run
`install.sh` — the download is idempotent and will re-fetch if the JAR
is missing. Confirm with:

```bash
ls -la /opt/spark/jars/ | grep -E "hadoop-aws|aws-java-sdk-bundle"
```

### Catalyst error: cannot resolve `substring(... from ... for ...)`

Means the dialect adapter missed a construct. Add a rewrite to
`adapt_sql_for_spark()` in `bench.py` and re-run; the rewriter is
deliberately conservative (only the patterns we've actually seen in
the TPC-H queries are rewritten).

## Sizing reference

| Phase | Instance      | Worker mem | Worker cores | Notes                |
|-------|---------------|------------|--------------|----------------------|
| SF=10 | c7i.2xlarge   | 12 g       | 7            | 16 GB / 8 vCPU box   |
| SF=100| c7i.4xlarge   | 28 g       | 15           | 32 GB / 16 vCPU box  |

Expected wall-clock for the full SF=10 sweep at 5 trials × 22 queries:
roughly 3 hours of cluster time across all 4 nodes, ~$10 spot.
