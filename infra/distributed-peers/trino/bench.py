#!/usr/bin/env python3
"""TPC-H benchmark runner for Trino (482 at the 2026-07-04 refresh; the
reported version is read from the live cluster, not hardcoded) against
the AWS-campaign cluster.

Reads SQL files from examples/tpch/queries/q*.sql, runs each one through
the coordinator's HTTP API via the `trino` Python client, times trial
runs, and uploads a JSON results file to S3.

Output matches the schema in docs/AWS_CAMPAIGN_2026_05_PLAN.md so the
aggregator in scripts/aggregate_distributed_bench.py can consume it
alongside ematix-flow + PySpark results.

Usage (run from coordinator):
    pip install trino boto3
    python3 bench.py --sf 10 --bucket my-bench-bucket --trials 5 --warmups 2

Optional flags:
    --host            Trino coordinator host (default: localhost)
    --port            Trino coordinator port (default: 8080)
    --queries-dir     where to find q*.sql (default: examples/tpch/queries
                      relative to repo root, auto-resolved)
    --queries         comma-separated subset (e.g. "Q01,Q06,Q14")
    --stamp           override the S3 results path stamp (default: UTC now)
    --no-upload       skip the S3 upload (useful for local smoke tests)
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import statistics
import sys
import time
from pathlib import Path
from typing import Iterable

# Shared campaign provenance capture (instance type, AZ, git SHA, env).
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import provenance  # noqa: E402

# These get imported lazily inside main() so --help works without them.
# (When run on a fresh coordinator the script does `pip install` itself
# first, so we want --help to still work pre-install.)


# ---------------------------------------------------------------------------
# Query loading + per-engine SQL adaptation
# ---------------------------------------------------------------------------

# Trino's SQL is closer to ANSI than Polars's, but a few DataFusion-isms
# need translating. Each entry maps Qxx -> a callable that returns the
# adapted SQL. If a query is not listed, the raw file content is used
# verbatim.
#
# Documented adaptations:
#
#   Q15: DataFusion's `WITH name (col1, col2) AS (SELECT ...)` form with
#        explicit CTE column aliases is valid ANSI but the SELECT inside
#        omits aliases for the aggregated column. Trino accepts the
#        column-list form, so this works as-is. No rewrite needed.
#
#   Q22: SUBSTRING(c_phone FROM 1 FOR 2) — standard SQL syntax, Trino
#        supports it natively. No rewrite needed. (Re-verified for the
#        482 refresh, 2026-07-04: the ANSI FROM/FOR form is unchanged.)
#
#   Q01: `date '1998-12-01' - interval '90' day` — Trino accepts the
#        interval-year-month and interval-day-second short forms with a
#        unit keyword. No rewrite needed. (Re-verified for 482: ANSI
#        interval literals unchanged.)
#
# Net result: all 22 queries currently pass through unmodified. The
# REWRITES dict is kept here so we can patch a specific query without
# touching the shared examples/tpch/queries/*.sql files (which are
# DataFusion-canonical).
REWRITES: dict[str, "callable[[str], str]"] = {}


def apply_tpch_query_params(qid: str, sql: str, sf: int) -> str:
    """Mirror of ematix_flow_core::tpch_params::apply_tpch_query_params —
    Q11's HAVING fraction is 0.0001/SF per the TPC-H spec, so at SF>1 the
    literal in q11.sql must be scaled or the query is degenerate AND the
    ematix runner (which applies the same transform) would report a
    different row count, tripping the aggregator's correctness flag."""
    if qid == "Q11" and sf > 1:
        return sql.replace("0.0001", f"(0.0001 / {sf})", 1)
    return sql


def load_query(queries_dir: Path, qid: str) -> str:
    """Load Qxx → SQL text, applying any per-query Trino adaptation."""
    # Files are lowercase q01.sql etc.
    path = queries_dir / f"{qid.lower()}.sql"
    sql = path.read_text()
    # Strip trailing semicolons + whitespace — the Trino client wants one
    # statement per execute call and chokes on a trailing ';' followed by
    # nothing.
    sql = sql.rstrip().rstrip(";").rstrip()
    if qid in REWRITES:
        sql = REWRITES[qid](sql)
    return sql


def discover_queries(queries_dir: Path) -> list[str]:
    """List Q01..Q22 that actually have a .sql file on disk."""
    out = []
    for n in range(1, 23):
        qid = f"Q{n:02d}"
        if (queries_dir / f"{qid.lower()}.sql").exists():
            out.append(qid)
    return out


# ---------------------------------------------------------------------------
# Trino execution + timing
# ---------------------------------------------------------------------------

def run_query(conn, sql: str) -> tuple[float, int]:
    """Execute `sql` against `conn`, returning (elapsed_ms, row_count).

    We measure wall-clock from cursor.execute() until the result iterator
    is fully drained. This matches what the ematix and PySpark runners do
    (they materialise to a count too). Counting rows — not bytes — is
    enough for our comparison; row counts also let us sanity-check the
    query against expected TPC-H result cardinalities.
    """
    cur = conn.cursor()
    t0 = time.perf_counter()
    cur.execute(sql)
    rows = 0
    # fetchmany() in a loop avoids materialising the whole result set in
    # memory for queries like Q01/Q10 that produce many rows. Q18 returns
    # up to ~57 rows on SF=10; everything stays small. The loop drains
    # the iterator so we measure the full server-side time.
    while True:
        chunk = cur.fetchmany(10_000)
        if not chunk:
            break
        rows += len(chunk)
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    cur.close()
    return elapsed_ms, rows


def percentile(values: list[float], p: float) -> float:
    """p in [0, 100]. Linear interpolation between the two nearest ranks."""
    if not values:
        return 0.0
    s = sorted(values)
    if len(s) == 1:
        return s[0]
    k = (len(s) - 1) * (p / 100.0)
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def bench_one(conn, qid: str, sql: str, trials: int, warmups: int) -> dict:
    """Warm up + measure one query. Returns the per-query result dict."""
    print(f"  {qid}: {warmups} warmups + {trials} trials", flush=True)
    last_rows = 0
    for w in range(warmups):
        _, last_rows = run_query(conn, sql)
        print(f"    warmup {w+1}/{warmups} ({last_rows} rows)", flush=True)

    trials_ms: list[float] = []
    for t in range(trials):
        elapsed, last_rows = run_query(conn, sql)
        trials_ms.append(elapsed)
        print(f"    trial  {t+1}/{trials}: {elapsed:.1f} ms ({last_rows} rows)",
              flush=True)

    median_ms = statistics.median(trials_ms)
    p95_ms = percentile(trials_ms, 95.0)
    first_trial_ms = trials_ms[0]
    # "median of trials 3-5" — uses 1-based indexing per the plan doc.
    # If we have fewer than 3 trials we fall back to the full median to
    # avoid emitting nulls.
    tail = trials_ms[2:5] if len(trials_ms) >= 3 else trials_ms
    median_trials_3_5_ms = statistics.median(tail)

    return {
        "trials_ms": [round(x, 3) for x in trials_ms],
        "median_ms": round(median_ms, 3),
        "p95_ms": round(p95_ms, 3),
        "first_trial_ms": round(first_trial_ms, 3),
        "median_trials_3_5_ms": round(median_trials_3_5_ms, 3),
        "rows_returned": last_rows,
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--sf", type=int, choices=[1, 10, 100], required=True)
    p.add_argument("--bucket", required=True, help="S3 results bucket name (no s3:// prefix)")
    p.add_argument("--trials", type=int, default=5)
    p.add_argument("--warmups", type=int, default=2)
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", type=int, default=8080)
    p.add_argument("--queries-dir", type=Path, default=None,
                   help="defaults to <repo>/examples/tpch/queries")
    p.add_argument("--queries", default=None,
                   help="comma-separated subset, e.g. Q01,Q06")
    p.add_argument("--stamp", default=None,
                   help="results/<stamp>/ prefix; default: UTC YYYYMMDDTHHMMSSZ")
    p.add_argument("--no-upload", action="store_true",
                   help="don't upload to S3, just write JSON locally")
    p.add_argument("--cluster-size", type=int, default=4)
    args = p.parse_args(argv)

    # Lazy imports — let --help succeed without these installed.
    try:
        import trino  # type: ignore
        from trino.dbapi import connect  # type: ignore
    except ImportError:
        print("error: pip install trino", file=sys.stderr)
        return 2

    queries_dir = args.queries_dir
    if queries_dir is None:
        # When invoked from coordinator we expect the repo to be checked
        # out at /opt/ematix-flow or the script's grandparent dir. Try
        # both relative-to-script and the well-known path.
        here = Path(__file__).resolve()
        candidates = [
            here.parents[3] / "examples" / "tpch" / "queries",
            Path("/opt/ematix-flow/examples/tpch/queries"),
        ]
        for c in candidates:
            if c.exists():
                queries_dir = c
                break
        else:
            print(f"error: --queries-dir not given and no candidate matched "
                  f"({[str(c) for c in candidates]})", file=sys.stderr)
            return 2
    print(f"==> using queries from {queries_dir}")

    all_qids = discover_queries(queries_dir)
    if args.queries:
        wanted = {q.strip().upper() for q in args.queries.split(",") if q.strip()}
        qids = [q for q in all_qids if q in wanted]
        missing = wanted - set(qids)
        if missing:
            print(f"warning: requested queries not on disk: {sorted(missing)}",
                  file=sys.stderr)
    else:
        qids = all_qids
    print(f"==> running {len(qids)} queries: {qids}")

    schema = f"tpch_sf{args.sf}"
    print(f"==> connecting to trino://{args.host}:{args.port} catalog=hive schema={schema}")
    conn = connect(
        host=args.host,
        port=args.port,
        user="bench",
        catalog="hive",
        schema=schema,
        http_scheme="http",
        # Long timeout — Q21 on SF=100 can take a minute or two on Trino.
        request_timeout=600,
    )

    # Engine version from the LIVE cluster — never hardcode it, or a
    # version bump in install.sh silently mislabels results.
    try:
        vcur = conn.cursor()
        vcur.execute("SELECT version()")
        trino_version = str(vcur.fetchone()[0])
        vcur.close()
    except Exception as exc:  # noqa: BLE001
        print(f"warning: could not read version(): {exc}", file=sys.stderr)
        trino_version = "unknown"
    print(f"==> trino version: {trino_version}")

    queries_out: dict[str, dict] = {}
    failures: dict[str, str] = {}
    for qid in qids:
        sql = apply_tpch_query_params(qid, load_query(queries_dir, qid), args.sf)
        try:
            queries_out[qid] = bench_one(conn, qid, sql, args.trials, args.warmups)
        except Exception as exc:  # noqa: BLE001 - we want to keep going
            print(f"  {qid}: FAILED — {exc}", file=sys.stderr, flush=True)
            failures[qid] = str(exc)
            queries_out[qid] = {
                "trials_ms": [],
                "median_ms": 0.0,
                "p95_ms": 0.0,
                "first_trial_ms": 0.0,
                "median_trials_3_5_ms": 0.0,
                "rows_returned": 0,
                "error": str(exc),
            }

    result = {
        "engine": "trino",
        "version": trino_version,
        "scale_factor": args.sf,
        "cluster_size": args.cluster_size,
        "trials": args.trials,
        "warmups": args.warmups,
        "provenance": provenance.collect(),
        "queries": queries_out,
    }

    stamp = args.stamp or dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    local_path = Path(f"/tmp/trino-sf{args.sf}.json")
    local_path.write_text(json.dumps(result, indent=2))
    print(f"==> wrote {local_path} ({local_path.stat().st_size} bytes)")

    if not args.no_upload:
        try:
            import boto3  # type: ignore
        except ImportError:
            print("error: pip install boto3 (or pass --no-upload)", file=sys.stderr)
            return 2
        key = f"results/{stamp}/trino-sf{args.sf}.json"
        print(f"==> uploading to s3://{args.bucket}/{key}")
        boto3.client("s3").upload_file(
            Filename=str(local_path),
            Bucket=args.bucket,
            Key=key,
            ExtraArgs={"ContentType": "application/json"},
        )
        print(f"==> upload complete: s3://{args.bucket}/{key}")

    if failures:
        print(f"!! {len(failures)} queries failed: {sorted(failures.keys())}",
              file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
