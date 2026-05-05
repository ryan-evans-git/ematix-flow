#!/usr/bin/env python3
"""Σ.A1 PR 4 follow-up: Polars single-node head-to-head against
DataFusion + PySpark.

Same shape as `scripts/bench-tpch-pyspark.py` — runs Q1 / Q3 / Q6 / Q19
against the same SF=1 Parquet under `examples/tpch/data/sf1/`, reports
3-trial median. Lets us land a 3-engine table in `docs/BENCHMARKS.md`
covering single-node DataFusion (Rust, in-process), PySpark (Java/JVM,
heavyweight), and Polars (Rust under Python, in-process — closest peer
to DataFusion in positioning).

Usage:

    python scripts/bench-tpch-polars.py [--data-dir DIR] [--trials N]

Polars's SQL support sits behind `polars.SQLContext`. Some TPC-H
queries may hit SQL-surface gaps (Polars's SQL is more limited than
DataFusion's); this script reports such failures rather than crashing
so the audit picks up.

Output: a markdown table on stdout. Exit code 0 always — measurement,
not gating.
"""

from __future__ import annotations

import argparse
import statistics
import sys
import time
from pathlib import Path

import polars as pl

REPO_ROOT = Path(__file__).resolve().parent.parent
QUERIES_DIR = REPO_ROOT / "examples" / "tpch" / "queries"
DEFAULT_DATA_DIR = REPO_ROOT / "examples" / "tpch" / "data" / "sf1"
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
QUERY_FILES = {
    "q01": QUERIES_DIR / "q01.sql",
    "q03": QUERIES_DIR / "q03.sql",
    "q06": QUERIES_DIR / "q06.sql",
    "q19": QUERIES_DIR / "q19.sql",
}


def build_context(data_dir: Path) -> pl.SQLContext:
    ctx = pl.SQLContext()
    for table in TPCH_TABLES:
        path = data_dir / f"{table}.parquet"
        if not path.exists():
            sys.exit(
                f"missing {path}\n"
                "generate first:\n"
                "    cargo run --release -p ematix-flow-core "
                "--example tpch_generate -- --sf 1 "
                f"--out {data_dir}"
            )
        # `scan_parquet` produces a LazyFrame so Polars can push down
        # predicates + projections through the optimizer.
        ctx.register(table, pl.scan_parquet(str(path)))
    return ctx


def load_query(name: str) -> str:
    path = QUERY_FILES[name]
    raw = path.read_text()
    return raw.strip().rstrip(";").strip()


def time_query(ctx: pl.SQLContext, sql: str) -> tuple[float, int]:
    """Run the query, materializing the full result via `.collect()`.
    Returns (seconds, row_count)."""
    start = time.perf_counter()
    df = ctx.execute(sql).collect()
    elapsed = time.perf_counter() - start
    return elapsed, df.height


def median_then_range(values: list[float]) -> tuple[float, float, float]:
    return statistics.median(values), min(values), max(values)


def run(ctx: pl.SQLContext, name: str, trials: int) -> dict[str, float | str]:
    sql = load_query(name)

    # Warm-up: 1 untimed run to populate any caches Polars maintains.
    try:
        _, row_count = time_query(ctx, sql)
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}"}

    timings: list[float] = []
    for trial in range(trials):
        elapsed, rows_now = time_query(ctx, sql)
        if rows_now != row_count:
            print(
                f"  WARN {name} trial {trial}: row count {rows_now} != "
                f"warm-up {row_count}",
                file=sys.stderr,
            )
        timings.append(elapsed)
        print(f"  {name} trial {trial + 1}/{trials}: {elapsed * 1000:.2f} ms")

    median, lo, hi = median_then_range(timings)
    return {
        "median_ms": median * 1000,
        "min_ms": lo * 1000,
        "max_ms": hi * 1000,
        "rows": row_count,
    }


# DataFusion + PySpark M3 Pro SF=1 numbers from earlier Σ.A1 PR runs
# (committed to docs/BENCHMARKS.md). Edit if rebenching on different
# hardware.
DATAFUSION_BASELINE_M3PRO_SF1_MS = {
    "q01": 48.7,
    "q03": 34.6,
    "q06": 18.2,
    "q19": 38.0,
}
PYSPARK_BASELINE_M3PRO_SF1_MS = {
    "q01": 192.6,
    "q03": 235.5,
    "q06": 64.2,
    "q19": 130.8,
}


def emit_markdown(results: dict[str, dict[str, float | str]]) -> str:
    lines = [
        "| Query | DataFusion (ms) | Polars (ms) | PySpark (ms) | "
        "DF/Polars | Polars/PySpark | rows |",
        "|---|---|---|---|---|---|---|",
    ]
    for name in ("q01", "q03", "q06", "q19"):
        df_med = DATAFUSION_BASELINE_M3PRO_SF1_MS.get(name, 0.0)
        spark_med = PYSPARK_BASELINE_M3PRO_SF1_MS.get(name, 0.0)
        r = results[name]
        if "error" in r:
            polars_med = "FAIL"
            df_polars = "—"
            polars_spark = "—"
            rows = "—"
            lines.append(
                f"| {name.upper()} | {df_med:>5.1f} | {polars_med} | "
                f"{spark_med:>7.1f} | {df_polars} | {polars_spark} | {rows} |"
            )
        else:
            polars_med_v = float(r["median_ms"])
            polars_spark_ratio = polars_med_v / spark_med if spark_med else 0.0
            df_polars_ratio = df_med / polars_med_v if polars_med_v else 0.0
            lines.append(
                f"| {name.upper()} | {df_med:>5.1f} | {polars_med_v:>7.1f} | "
                f"{spark_med:>7.1f} | {df_polars_ratio:.3f} | "
                f"{polars_spark_ratio:.3f} | {int(r['rows'])} |"
            )

    # Errors, if any, in a follow-on block.
    errors = {n: r["error"] for n, r in results.items() if "error" in r}
    if errors:
        lines.append("")
        lines.append("**Polars SQL-surface failures:**")
        lines.append("")
        for name, msg in errors.items():
            lines.append(f"- `{name}`: `{msg}`")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    p.add_argument("--trials", type=int, default=3)
    return p.parse_args()


def main() -> None:
    args = parse_args()

    print(f"==> data dir: {args.data_dir}")
    print(f"==> trials per query: {args.trials} (after 1 warm-up)")
    print(f"==> Polars {pl.__version__}")
    print()

    ctx = build_context(args.data_dir)

    results: dict[str, dict[str, float | str]] = {}
    for name in ("q01", "q03", "q06", "q19"):
        print(f"-- {name} --")
        results[name] = run(ctx, name, args.trials)
        if "error" in results[name]:
            print(f"  ERROR: {results[name]['error']}")
        print()

    print("=== Results ===")
    print(emit_markdown(results))


if __name__ == "__main__":
    main()
