#!/usr/bin/env python3
"""Single-node TPC-H benchmark runner for DuckDB, Polars, pandas, and ClickHouse.

This is the single-node peer of the Trino/PySpark campaign runners
(infra/distributed-peers/trino/bench.py). It runs each of the 22 TPC-H
queries against one of three embedded engines and emits a JSON results
file in the EXACT same schema, so the aggregator in
scripts/aggregate_distributed_bench.py can consume it alongside the
ematix-flow, PySpark, and Trino results.

Data layout: each table lives in its own subdirectory under --data-dir
holding one or more parquet parts ("parted" layout), e.g.

    <data-dir>/lineitem/lineitem-0001.parquet
    <data-dir>/lineitem/lineitem-0002.parquet
    ...

Tables are registered/scanned via a glob `<data-dir>/<table>/*.parquet`
so the runner is agnostic to how many parts a table has.

Engines:
    duckdb  — CREATE VIEW over read_parquet(glob), runs canonical q*.sql.
    polars  — pl.SQLContext over scan_parquet(glob), runs q*.polars.sql
              variants (skips a query when its .polars.sql is missing).
    pandas  — hand-coded per-query functions imported from
              scripts/bench-tpch-pandas.py, reading whole tables via
              pd.read_parquet. pandas at SF10/SF100 is expected to OOM;
              failures are recorded per-query and the run continues.
    clickhouse — embedded ClickHouse via chdb (pip install chdb), the
              in-process peer of the duckdb arm. CREATE VIEW over
              file(glob, Parquet), runs ClickHouse's own published
              TPC-H queries vendored as q*.clickhouse.sql.

Usage:
    pip install duckdb polars pandas pyarrow boto3
    python3 bench.py --engine duckdb --sf 10 --bucket my-bench-bucket
    python3 bench.py --engine polars --sf 1 --data-dir /var/tmp/tpch-parted/sf1 --no-upload

Optional flags:
    --data-dir        local dir with per-table subdirs
                      (default: /opt/ematix/data/sf{sf})
    --queries-dir     where to find q*.sql / q*.polars.sql
                      (default: examples/tpch/queries relative to repo
                      root, auto-resolved)
    --queries         comma-separated subset (e.g. "Q01,Q06,Q14")
    --stamp           override the S3 results path stamp (default: UTC now)
    --no-upload       skip the S3 upload (useful for local smoke tests)
    --cluster-size    recorded in the output (default: 1 — single node)
"""

from __future__ import annotations

import argparse
import datetime as dt
import importlib.util
import json
import os
import statistics
import sys
import time
from pathlib import Path

# Shared campaign provenance capture (instance type, AZ, git SHA, env).
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import provenance  # noqa: E402

# The engine libs (duckdb / polars / pandas) are imported lazily inside
# main() so --help works without them installed, and so choosing one
# engine doesn't require the other two.

# The 8 TPC-H tables, in dependency order (small dims first).
TABLES = [
    "region", "nation", "supplier", "customer",
    "part", "partsupp", "orders", "lineitem",
]


# ---------------------------------------------------------------------------
# Query loading + per-query SF parameter scaling
# ---------------------------------------------------------------------------

def apply_tpch_query_params(qid: str, sql: str, sf: int) -> str:
    """Mirror of ematix_flow_core::tpch_params::apply_tpch_query_params —
    Q11's HAVING fraction is 0.0001/SF per the TPC-H spec, so at SF>1 the
    literal in q11.sql must be scaled or the query is degenerate AND the
    ematix runner (which applies the same transform) would report a
    different row count, tripping the aggregator's correctness flag."""
    if qid == "Q11" and sf > 1:
        return sql.replace("0.0001", f"(0.0001 / {sf})", 1)
    return sql


def load_query(queries_dir: Path, qid: str, variant: str = "") -> str | None:
    """Load Qxx → SQL text. `variant` selects the file suffix: "" for the
    canonical q01.sql, "polars" for q01.polars.sql. Returns None if the
    file does not exist (used by polars to skip missing variants)."""
    suffix = f".{variant}.sql" if variant else ".sql"
    path = queries_dir / f"{qid.lower()}{suffix}"
    if not path.exists():
        return None
    sql = path.read_text()
    # Strip trailing semicolons + whitespace — the engines want a single
    # statement per execute call.
    return sql.rstrip().rstrip(";").rstrip()


def discover_queries(queries_dir: Path) -> list[str]:
    """List Q01..Q22 that have a canonical .sql file on disk."""
    out = []
    for n in range(1, 23):
        qid = f"Q{n:02d}"
        if (queries_dir / f"{qid.lower()}.sql").exists():
            out.append(qid)
    return out


# ---------------------------------------------------------------------------
# Timing helpers
# ---------------------------------------------------------------------------

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


def bench_one(runner, qid: str, trials: int, warmups: int) -> dict:
    """Warm up + measure one query. `runner` is a zero-arg callable that
    executes the query and returns (elapsed_ms, row_count). Returns the
    per-query result dict."""
    print(f"  {qid}: {warmups} warmups + {trials} trials", flush=True)
    last_rows = 0
    for w in range(warmups):
        _, last_rows = runner()
        print(f"    warmup {w+1}/{warmups} ({last_rows} rows)", flush=True)

    trials_ms: list[float] = []
    for t in range(trials):
        elapsed, last_rows = runner()
        trials_ms.append(elapsed)
        print(f"    trial  {t+1}/{trials}: {elapsed:.1f} ms ({last_rows} rows)",
              flush=True)

    median_ms = statistics.median(trials_ms)
    p95_ms = percentile(trials_ms, 95.0)
    first_trial_ms = trials_ms[0]
    # "median of trials 3-5" — 1-based indexing per the plan doc. Fewer
    # than 3 trials falls back to the full median to avoid emitting nulls.
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


def failed_query(exc: Exception) -> dict:
    """Per-query result dict for a failed/skipped query — same shape as
    the Trino runner emits on failure."""
    return {
        "trials_ms": [],
        "median_ms": 0.0,
        "p95_ms": 0.0,
        "first_trial_ms": 0.0,
        "median_trials_3_5_ms": 0.0,
        "rows_returned": 0,
        "error": str(exc),
    }


# ---------------------------------------------------------------------------
# Engine setup — each returns (version, {qid: zero-arg runner callable})
# ---------------------------------------------------------------------------

def setup_duckdb(data_dir: Path, queries_dir: Path, qids: list[str], sf: int):
    import duckdb  # type: ignore

    con = duckdb.connect(config={"threads": os.cpu_count()})
    for t in TABLES:
        glob = f"{data_dir}/{t}/*.parquet"
        con.execute(
            f"CREATE OR REPLACE VIEW {t} AS SELECT * FROM read_parquet('{glob}')"
        )

    runners: dict[str, "callable"] = {}
    skipped: dict[str, str] = {}
    for qid in qids:
        sql = load_query(queries_dir, qid)
        if sql is None:
            skipped[qid] = "missing q*.sql"
            continue
        sql = apply_tpch_query_params(qid, sql, sf)

        def make(sql=sql):
            def run() -> tuple[float, int]:
                t0 = time.perf_counter()
                rows = con.execute(sql).fetchall()
                elapsed_ms = (time.perf_counter() - t0) * 1000.0
                return elapsed_ms, len(rows)
            return run

        runners[qid] = make()
    return duckdb.__version__, runners, skipped


def setup_polars(data_dir: Path, queries_dir: Path, qids: list[str], sf: int):
    import polars as pl  # type: ignore

    ctx = pl.SQLContext()
    for t in TABLES:
        glob = f"{data_dir}/{t}/*.parquet"
        ctx.register(t, pl.scan_parquet(glob))

    runners: dict[str, "callable"] = {}
    skipped: dict[str, str] = {}
    for qid in qids:
        # Polars uses its own SQL variant files (q01.polars.sql). If a
        # variant is missing for some qid, skip it — don't crash.
        sql = load_query(queries_dir, qid, variant="polars")
        if sql is None:
            print(f"  {qid}: no .polars.sql variant — skipping", flush=True)
            skipped[qid] = "missing q*.polars.sql"
            continue
        sql = apply_tpch_query_params(qid, sql, sf)

        def make(sql=sql):
            def run() -> tuple[float, int]:
                t0 = time.perf_counter()
                df = ctx.execute(sql, eager=True)
                elapsed_ms = (time.perf_counter() - t0) * 1000.0
                return elapsed_ms, df.height
            return run

        runners[qid] = make()
    return pl.__version__, runners, skipped


def setup_clickhouse(data_dir: Path, queries_dir: Path, qids: list[str], sf: int):
    """ClickHouse via chdb — the embedded ClickHouse engine, the exact
    in-process peer of the duckdb arm (same box, same parquet globs, no
    server ops). Queries are ClickHouse's OWN published TPC-H set
    (ClickHouse/ClickHouse tests/benchmarks/tpc-h/queries), vendored as
    q*.clickhouse.sql with comment lines stripped (Q11's 0.0001 literal
    must be the FIRST occurrence for apply_tpch_query_params) and Q15's
    CREATE VIEW/DROP VIEW pair inlined as a CTE (single-statement
    harness; SELECT body identical to upstream). settings.json from the
    same upstream dir sets join_use_nulls=1 (SQL-standard outer-join
    NULLs — Q13 correctness)."""
    import chdb  # type: ignore
    from chdb import session as chs  # type: ignore

    sess = chs.Session()
    # Upstream tests/benchmarks/tpc-h/settings.json.
    sess.query("SET join_use_nulls = 1")
    # Match the duckdb arm's posture: the engine gets the whole box
    # (duckdb's default memory_limit is 80% of RAM; chdb inherits
    # server-style per-query caps that would handicap SF>=100 joins).
    sess.query("SET max_memory_usage = 0")
    for t in TABLES:
        glob = f"{data_dir}/{t}/*.parquet"
        sess.query(
            f"CREATE OR REPLACE VIEW {t} AS SELECT * FROM file('{glob}', Parquet)"
        )

    runners: dict[str, "callable"] = {}
    skipped: dict[str, str] = {}
    for qid in qids:
        sql = load_query(queries_dir, qid, variant="clickhouse")
        if sql is None:
            print(f"  {qid}: no .clickhouse.sql variant — skipping", flush=True)
            skipped[qid] = "missing q*.clickhouse.sql"
            continue
        sql = apply_tpch_query_params(qid, sql, sf)

        def make(sql=sql):
            def run() -> tuple[float, int]:
                t0 = time.perf_counter()
                res = sess.query(sql, "CSV")
                out = str(res)
                elapsed_ms = (time.perf_counter() - t0) * 1000.0
                if getattr(res, "has_error", lambda: False)():
                    raise RuntimeError(res.error_message())
                rows = sum(1 for line in out.splitlines() if line.strip())
                return elapsed_ms, rows
            return run

        runners[qid] = make()
    engine_version = str(sess.query("SELECT version()", "CSV")).strip().strip('"')
    return f"{engine_version} (chdb {chdb.__version__})", runners, skipped


def setup_pandas(data_dir: Path, queries_dir: Path, qids: list[str], sf: int):
    """pandas has no SQL — it reuses the hand-coded per-query functions
    from scripts/bench-tpch-pandas.py. That module hardcodes its data dir
    (examples/tpch/data/sf1) and a single-file `load()`, so we import it
    as a module, then monkeypatch its `load` to read the parted glob under
    our --data-dir. This reuses the query IMPLEMENTATIONS verbatim while
    swapping only where the data comes from."""
    import pandas as pd  # type: ignore
    from glob import glob as _glob

    here = Path(__file__).resolve()
    # infra/distributed-peers/singlenode/bench.py -> repo root is parents[3]
    repo_root = here.parents[3]
    pandas_mod_path = repo_root / "scripts" / "bench-tpch-pandas.py"
    if not pandas_mod_path.exists():
        raise FileNotFoundError(f"pandas query module not found: {pandas_mod_path}")

    spec = importlib.util.spec_from_file_location("bench_tpch_pandas", pandas_mod_path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    def load(name: str) -> "pd.DataFrame":
        parts = sorted(_glob(str(data_dir / name / "*.parquet")))
        if not parts:
            raise FileNotFoundError(f"no parquet parts for table {name} under {data_dir}")
        if len(parts) == 1:
            return pd.read_parquet(parts[0])
        return pd.concat([pd.read_parquet(f) for f in parts], ignore_index=True)

    # Rebind the module's data loader to our parted glob.
    mod.load = load  # type: ignore[attr-defined]

    runners: dict[str, "callable"] = {}
    skipped: dict[str, str] = {}
    query_set = mod.QUERIES  # {qid: fn}
    for qid in qids:
        fn = query_set.get(qid)
        if fn is None:
            # pandas only implements a subset (correlated subqueries /
            # windows don't translate). Record the rest as skipped.
            skipped[qid] = "no pandas implementation"
            continue

        def make(fn=fn):
            def run() -> tuple[float, int]:
                t0 = time.perf_counter()
                res = fn()
                # Force full materialisation. Some queries (Q06, Q14)
                # return a scalar; treat those as 1 row.
                rows = len(res) if hasattr(res, "__len__") else 1
                elapsed_ms = (time.perf_counter() - t0) * 1000.0
                return elapsed_ms, rows
            return run

        runners[qid] = make()
    return pd.__version__, runners, skipped


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--engine", choices=["duckdb", "polars", "pandas", "clickhouse"],
                   required=True)
    p.add_argument("--sf", type=int, choices=[1, 10, 100, 1000], required=True)
    p.add_argument("--data-dir", type=Path, default=None,
                   help="local dir with per-table subdirs "
                        "(default: /opt/ematix/data/sf{sf})")
    p.add_argument("--bucket", default=None,
                   help="S3 results bucket name (no s3:// prefix)")
    p.add_argument("--trials", type=int, default=5)
    p.add_argument("--warmups", type=int, default=2)
    p.add_argument("--queries-dir", type=Path, default=None,
                   help="defaults to <repo>/examples/tpch/queries")
    p.add_argument("--queries", default=None,
                   help="comma-separated subset, e.g. Q01,Q06")
    p.add_argument("--stamp", default=None,
                   help="results/<stamp>/ prefix; default: UTC YYYYMMDDTHHMMSSZ")
    p.add_argument("--no-upload", action="store_true",
                   help="don't upload to S3, just write JSON locally")
    p.add_argument("--cluster-size", type=int, default=1)
    args = p.parse_args(argv)

    if not args.no_upload and not args.bucket:
        print("error: --bucket is required unless --no-upload is given",
              file=sys.stderr)
        return 2

    data_dir = args.data_dir or Path(f"/opt/ematix/data/sf{args.sf}")
    if not data_dir.exists():
        print(f"error: data dir does not exist: {data_dir}", file=sys.stderr)
        return 2
    print(f"==> using data from {data_dir}")

    queries_dir = args.queries_dir
    if queries_dir is None:
        # infra/distributed-peers/singlenode/bench.py -> repo is parents[3].
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
    print(f"==> engine={args.engine} sf={args.sf}: {len(qids)} candidate queries")

    # Lazy engine setup — imports happen inside these.
    try:
        if args.engine == "duckdb":
            version, runners, skipped = setup_duckdb(data_dir, queries_dir, qids, args.sf)
        elif args.engine == "polars":
            version, runners, skipped = setup_polars(data_dir, queries_dir, qids, args.sf)
        elif args.engine == "clickhouse":
            version, runners, skipped = setup_clickhouse(data_dir, queries_dir, qids, args.sf)
        else:
            version, runners, skipped = setup_pandas(data_dir, queries_dir, qids, args.sf)
    except ImportError as exc:
        print(f"error: pip install {args.engine} (import failed: {exc})",
              file=sys.stderr)
        return 2
    print(f"==> {args.engine} version: {version}")
    if skipped:
        print(f"==> {len(skipped)} queries skipped: {sorted(skipped)}")

    queries_out: dict[str, dict] = {}
    failures: dict[str, str] = {}

    for qid in qids:
        if qid in skipped:
            queries_out[qid] = failed_query(Exception(skipped[qid]))
            continue
        try:
            queries_out[qid] = bench_one(runners[qid], qid, args.trials, args.warmups)
        except Exception as exc:  # noqa: BLE001 — one query's OOM must not kill the run
            print(f"  {qid}: FAILED — {exc}", file=sys.stderr, flush=True)
            failures[qid] = str(exc)
            queries_out[qid] = failed_query(exc)

    result = {
        "engine": args.engine,
        "version": version,
        "scale_factor": args.sf,
        "cluster_size": args.cluster_size,
        "trials": args.trials,
        "warmups": args.warmups,
        "provenance": provenance.collect(),
        "queries": queries_out,
    }

    stamp = args.stamp or dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    local_path = Path(f"/tmp/{args.engine}-sf{args.sf}.json")
    local_path.write_text(json.dumps(result, indent=2))
    print(f"==> wrote {local_path} ({local_path.stat().st_size} bytes)")

    if not args.no_upload:
        try:
            import boto3  # type: ignore
        except ImportError:
            print("error: pip install boto3 (or pass --no-upload)", file=sys.stderr)
            return 2
        key = f"results/{stamp}/{args.engine}-sf{args.sf}.json"
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
