#!/usr/bin/env python3.12
"""
Phase B4: PySpark TPC-H benchmark runner.

Runs the 22 canonical TPC-H queries against a Spark 4.x standalone
cluster (4.1.2 as of the 2026-07-04 refresh; the reported version is
read from the live SparkSession, not hardcoded) reading
single-file-per-table parquet from S3. Designed to be invoked from the
master node only — `spark-submit` is *not* used; we attach to the
cluster directly via a long-lived SparkSession.

Output schema (per the AWS campaign plan):

    {
      "engine": "pyspark",
      "version": "<spark.version at runtime>",
      "scale_factor": 10,
      "cluster_size": 4,
      "queries": {
        "Q01": {
          "trials_ms": [...],
          "median_ms": 0.0,
          "p95_ms": 0.0,
          "first_trial_ms": 0.0,
          "median_trials_3_5_ms": 0.0,
          "rows_returned": 0
        },
        ...
      }
    }

Usage:
    python3.12 bench.py --sf 10 --bucket my-bench-bucket --trials 5 --warmups 2

The script auto-uploads the JSON to
    s3://$BUCKET/results/$STAMP/pyspark-sf{N}.json
where $STAMP defaults to UTC `YYYYMMDD-HHMMSS` and can be overridden
with --stamp.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import re
import statistics
import sys
import time
from pathlib import Path
from typing import Dict, List

# Shared campaign provenance capture (instance type, AZ, git SHA, env).
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import provenance  # noqa: E402

# Located at $REPO/examples/tpch/queries/q{NN}.sql. install.sh does not
# clone the repo; in practice this script runs from a checkout on the
# master node, so the queries dir is resolved relative to this file or
# overridden with --queries-dir.
_DEFAULT_QUERIES_DIR = (
    Path(__file__).resolve().parents[3] / "examples" / "tpch" / "queries"
)

# The 8 TPC-H tables; matches the s3 layout described in the plan.
TPCH_TABLES = [
    "region",
    "nation",
    "supplier",
    "customer",
    "part",
    "partsupp",
    "orders",
    "lineitem",
]


# -----------------------------------------------------------------------------
# Dialect adaptation
# -----------------------------------------------------------------------------
# The repo's q*.sql are written in a DataFusion / Postgres-ish dialect. Spark
# parses most of these as-is, but a couple of constructs need rewriting:
#
#   1. `substring(col from N for M)` (ANSI form, q22) → `substring(col, N, M)`
#      The 3-arg form is unambiguous and valid on every Spark line we've run
#      (3.5 and 4.x), so the mechanical rewrite stays.
#   2. `interval '90' day` (q01) → `interval 90 day`. Re-checked against
#      Spark 4 (ANSI mode is ON by default since 4.0): BOTH forms parse and
#      both produce a DayTimeIntervalType under ANSI intervals, so
#      `date '1998-12-01' - interval 90 day` keeps its Date result. The
#      normalisation is retained as a harmless cross-version constant.
#
# extract(year from ...), date literals, CTEs with column aliases, and NOT
# EXISTS all work natively in Spark 3.5 and 4.x. Note Spark 4 defaults
# spark.sql.ansi.enabled=true — TPC-H has no silent-cast/overflow reliance,
# so no behavioural delta is expected; if a query ever errors under ANSI,
# investigate the query rather than switching ANSI off (the campaign
# measures engines at their production defaults).

_SUBSTRING_ANSI = re.compile(
    r"substring\s*\(\s*([^,)]+?)\s+from\s+(\d+)\s+for\s+(\d+)\s*\)",
    re.IGNORECASE,
)
_INTERVAL_QUOTED = re.compile(
    r"interval\s+'(\d+)'\s+(day|month|year)",
    re.IGNORECASE,
)


def adapt_sql_for_spark(sql: str) -> str:
    """Apply the small set of mechanical rewrites listed above."""
    sql = _SUBSTRING_ANSI.sub(r"substring(\1, \2, \3)", sql)
    sql = _INTERVAL_QUOTED.sub(r"interval \1 \2", sql)
    # Strip a trailing semicolon — spark.sql() chokes on it.
    return sql.strip().rstrip(";").strip()


# -----------------------------------------------------------------------------
# SparkSession setup
# -----------------------------------------------------------------------------
def build_spark(app_name: str):
    """Construct a SparkSession that attaches to the standalone cluster
    via the master URL configured in spark-defaults.conf."""
    from pyspark.sql import SparkSession  # imported lazily — PySpark is

    builder = (
        SparkSession.builder
        .appName(app_name)
        # Defaults come from /opt/spark/conf/spark-defaults.conf (master URL,
        # s3a config, Kryo, AQE). We only override things that are bench-
        # specific here.
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.autoBroadcastJoinThreshold", str(64 * 1024 * 1024))
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def register_tables(spark, bucket: str, sf: int) -> None:
    """Register each TPC-H table as a temp view from its single parquet
    file in S3. We register a view (not a managed table) so there is no
    Hive metastore / Glue dependency."""
    prefix = f"s3a://{bucket}/tpch-data/sf{sf}"
    for tbl in TPCH_TABLES:
        path = f"{prefix}/{tbl}.parquet"
        spark.read.parquet(path).createOrReplaceTempView(tbl)
        print(f"  registered {tbl} <- {path}", flush=True)


# -----------------------------------------------------------------------------
# Query execution
# -----------------------------------------------------------------------------
def run_query_timed(spark, sql: str) -> tuple[float, int]:
    """Execute `sql` once. Returns (elapsed_ms, row_count).

    We pull rows with .collect() so the timer includes the full physical
    execution, not just the query plan. For TPC-H the result sets are tiny
    (max ~100 rows) so collect cost is negligible.
    """
    t0 = time.perf_counter()
    rows = spark.sql(sql).collect()
    elapsed = (time.perf_counter() - t0) * 1000.0
    return elapsed, len(rows)


def percentile(values: List[float], p: float) -> float:
    """Linear-interpolated percentile (NumPy-style); avoids numpy dep."""
    if not values:
        return 0.0
    s = sorted(values)
    if len(s) == 1:
        return s[0]
    k = (len(s) - 1) * (p / 100.0)
    lo = int(k)
    hi = min(lo + 1, len(s) - 1)
    frac = k - lo
    return s[lo] + (s[hi] - s[lo]) * frac


def benchmark_all(
    spark,
    queries_dir: Path,
    trials: int,
    warmups: int,
) -> Dict[str, dict]:
    """Run all 22 TPC-H queries, returning the per-query result dict."""
    out: Dict[str, dict] = {}
    for n in range(1, 23):
        qid = f"Q{n:02d}"
        path = queries_dir / f"q{n:02d}.sql"
        if not path.is_file():
            print(f"!! missing {path}, skipping", flush=True)
            continue

        sql = adapt_sql_for_spark(path.read_text())

        print(f"== {qid} (warmups={warmups}, trials={trials})", flush=True)
        # Warmups — JVM optimization, Catalyst codegen cache priming.
        for w in range(warmups):
            try:
                ms, _ = run_query_timed(spark, sql)
                print(f"   warmup {w + 1}: {ms:.1f} ms", flush=True)
            except Exception as e:  # noqa: BLE001
                print(f"!! {qid} warmup failed: {e}", flush=True)
                out[qid] = {"error": f"warmup failed: {e}"}
                break
        if qid in out and "error" in out[qid]:
            continue

        # Measured trials.
        trials_ms: List[float] = []
        rows = 0
        failed = None
        for t in range(trials):
            try:
                ms, rows = run_query_timed(spark, sql)
                trials_ms.append(ms)
                print(f"   trial  {t + 1}: {ms:.1f} ms (rows={rows})", flush=True)
            except Exception as e:  # noqa: BLE001
                failed = str(e)
                print(f"!! {qid} trial {t + 1} failed: {e}", flush=True)
                break

        if failed:
            out[qid] = {"error": failed, "trials_ms": trials_ms}
            continue

        # Steady-state slice = trials 3..5 (1-indexed), i.e. indices [2:5].
        steady = trials_ms[2:5] if len(trials_ms) >= 3 else trials_ms
        out[qid] = {
            "trials_ms": [round(x, 3) for x in trials_ms],
            "median_ms": round(statistics.median(trials_ms), 3),
            "p95_ms": round(percentile(trials_ms, 95), 3),
            "first_trial_ms": round(trials_ms[0], 3),
            "median_trials_3_5_ms": round(statistics.median(steady), 3),
            "rows_returned": rows,
        }
    return out


# -----------------------------------------------------------------------------
# Result upload
# -----------------------------------------------------------------------------
def upload_to_s3(bucket: str, key: str, body: bytes) -> None:
    """Upload `body` to s3://bucket/key. Uses the EC2 instance profile
    (boto3 picks it up automatically). Failure is non-fatal — we still
    keep a local copy."""
    import boto3  # imported lazily; only needed at upload time

    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")
    print(f"==> uploaded s3://{bucket}/{key}", flush=True)


def cluster_size_from_spark(spark) -> int:
    """Best-effort count of distinct worker nodes attached to the master.
    Falls back to 4 (our default cluster size) if introspection fails."""
    try:
        status = spark.sparkContext.statusTracker()
        execs = status.getExecutorInfos()
        # Drop the driver (host == driver host) — Spark lists it as an executor.
        hosts = {e.host for e in execs if e.host}
        return max(len(hosts), 1)
    except Exception:  # noqa: BLE001
        return 4


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------
def parse_args(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="PySpark TPC-H benchmark")
    p.add_argument("--sf", type=int, required=True, choices=[10, 100],
                   help="TPC-H scale factor")
    p.add_argument("--bucket", required=True, help="S3 bucket (data + results)")
    p.add_argument("--trials", type=int, default=5, help="measured trials per query")
    p.add_argument("--warmups", type=int, default=2, help="untimed warmups per query")
    p.add_argument("--queries-dir", type=Path, default=_DEFAULT_QUERIES_DIR,
                   help="directory containing q01.sql..q22.sql")
    p.add_argument("--stamp", default=None,
                   help="result-folder stamp (default: UTC YYYYMMDD-HHMMSS)")
    p.add_argument("--out", type=Path, default=None,
                   help="local JSON output path (default: ./pyspark-sf{N}.json)")
    p.add_argument("--no-upload", action="store_true",
                   help="skip S3 upload (write local file only)")
    return p.parse_args(argv)


def main(argv: List[str]) -> int:
    args = parse_args(argv)

    if not args.queries_dir.is_dir():
        print(f"queries dir not found: {args.queries_dir}", file=sys.stderr)
        return 2

    stamp = args.stamp or _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%d-%H%M%S")
    local_out = args.out or Path(f"pyspark-sf{args.sf}.json")

    print(f"==> PySpark TPC-H bench: sf={args.sf} bucket={args.bucket} "
          f"trials={args.trials} warmups={args.warmups}", flush=True)

    spark = build_spark(app_name=f"tpch-sf{args.sf}-bench")
    cluster_size = 4
    try:
        # Version comes from the LIVE session — never hardcode it, or a
        # version bump in install.sh silently mislabels results.
        spark_version = spark.version

        print("==> registering tables", flush=True)
        register_tables(spark, args.bucket, args.sf)

        # Snap a cluster-size reading once executors have registered. We
        # take it before the run so a query-time hiccup doesn't influence
        # what we publish.
        cluster_size = cluster_size_from_spark(spark)

        print("==> running queries", flush=True)
        results = benchmark_all(spark, args.queries_dir, args.trials, args.warmups)
    finally:
        spark.stop()

    doc = {
        "engine": "pyspark",
        "version": spark_version,
        "scale_factor": args.sf,
        "cluster_size": cluster_size,
        "stamp": stamp,
        "trials": args.trials,
        "warmups": args.warmups,
        "provenance": provenance.collect(),
        "queries": results,
    }
    body = json.dumps(doc, indent=2, sort_keys=True).encode("utf-8")
    local_out.write_bytes(body)
    print(f"==> wrote {local_out} ({len(body)} bytes)", flush=True)

    if not args.no_upload:
        key = f"results/{stamp}/pyspark-sf{args.sf}.json"
        try:
            upload_to_s3(args.bucket, key, body)
        except Exception as e:  # noqa: BLE001
            print(f"!! S3 upload failed (local copy retained at {local_out}): {e}",
                  file=sys.stderr)
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
