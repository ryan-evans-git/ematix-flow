#!/usr/bin/env python3
"""Σ.A1 PR 4: PySpark single-node head-to-head against DataFusion.

Runs the same four representative TPC-H queries (Q1 / Q3 / Q6 / Q19)
that `crates/ematix-flow-core/benches/tpch.rs` runs through DataFusion,
but against PySpark's `local[*]` mode on the same Parquet files. Lets
us land a side-by-side comparison in `docs/BENCHMARKS.md` so the
"DataFusion is faster than Spark single-node" claim is grounded in
numbers from the same hardware + same data + same SQL.

Usage:

    python scripts/bench-tpch-pyspark.py [--data-dir DIR] [--trials N]

Defaults: 3 trials per query (after 1 warm-up that's discarded — Spark
needs a few queries for the JIT to settle). Reads from
`examples/tpch/data/sf1/` relative to the workspace root.

Requires Java 17+. PySpark 4.x officially supports JDK 17 / 21; JDK 23
works with Py4J reflection-warning noise that's safe to ignore.

Output: a markdown table on stdout suitable for pasting into
docs/BENCHMARKS.md, plus per-trial detail. Exit code 0 always — this
script doesn't gate; it just measures.
"""

from __future__ import annotations

import argparse
import os
import statistics
import sys
import time
from pathlib import Path

# Suppress PySpark's chatty INFO logs; keep WARN+ so real problems
# surface. Set BEFORE importing pyspark — `pyspark.SparkContext`
# reads PYSPARK_LOG_LEVEL on init.
os.environ.setdefault("PYSPARK_LOG_LEVEL", "ERROR")

from pyspark.sql import SparkSession

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


def build_spark() -> SparkSession:
    # JDK 18+ deprecated `javax.security.auth.Subject.getSubject(...)`,
    # which Spark's UGI shim still calls during analyzer setup. Without
    # `-Djava.security.manager=allow` this throws
    # `UnsupportedOperationException: getSubject is supported only if a
    # security manager is allowed` on the first DataFrame call. Officially
    # Spark 4.x supports JDK 17/21; we set the flag so JDK 23 (Homebrew's
    # `openjdk` cask) also works for local benching.
    java_opts = "-Djava.security.manager=allow"
    return (
        SparkSession.builder.master("local[*]")
        .appName("ematix-flow-tpch-bench")
        .config("spark.driver.memory", "4g")
        .config("spark.driver.extraJavaOptions", java_opts)
        .config("spark.executor.extraJavaOptions", java_opts)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def register_tables(spark: SparkSession, data_dir: Path) -> None:
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
        spark.read.parquet(str(path)).createOrReplaceTempView(table)


def load_query(name: str) -> str:
    path = QUERY_FILES[name]
    raw = path.read_text()
    # Strip terminating semicolons — Spark's `spark.sql()` rejects
    # them. The .sql files have them so DataFusion's harness + a human
    # reading the file see standard SQL.
    return raw.strip().rstrip(";").strip()


def time_query(spark: SparkSession, sql: str) -> tuple[float, int]:
    """Run the query, materializing the full result. Returns (seconds,
    row_count). Materialization via `.collect()` matches what the
    DataFusion criterion bench does (`df.collect().await`)."""
    start = time.perf_counter()
    rows = spark.sql(sql).collect()
    elapsed = time.perf_counter() - start
    return elapsed, len(rows)


def median_then_range(values: list[float]) -> tuple[float, float, float]:
    return statistics.median(values), min(values), max(values)


def run(spark: SparkSession, name: str, trials: int) -> dict[str, float]:
    sql = load_query(name)

    # Warm-up: 1 untimed run to let the JIT settle.
    _, row_count = time_query(spark, sql)

    timings: list[float] = []
    for trial in range(trials):
        elapsed, rows_now = time_query(spark, sql)
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


def emit_markdown(results: dict[str, dict[str, float]], df_baseline: dict[str, float]) -> str:
    """Emit a markdown table comparable to docs/BENCHMARKS.md style."""
    lines = [
        "| Query | DataFusion (ms) | PySpark (ms) | DF/PySpark | "
        "PySpark trials min/max (ms) | rows |",
        "|---|---|---|---|---|---|",
    ]
    for name in ("q01", "q03", "q06", "q19"):
        spark_med = results[name]["median_ms"]
        df_med = df_baseline.get(name, 0.0)
        ratio = (df_med / spark_med) if spark_med else 0.0
        rng = f"{results[name]['min_ms']:.1f} / {results[name]['max_ms']:.1f}"
        lines.append(
            f"| {name.upper()} | {df_med:>5.1f} | {spark_med:>7.1f} | "
            f"{ratio:.3f} | {rng} | {int(results[name]['rows'])} |"
        )
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help=f"TPC-H Parquet directory (default: {DEFAULT_DATA_DIR})",
    )
    p.add_argument(
        "--trials",
        type=int,
        default=3,
        help="timed trials per query after a discarded warm-up (default: 3)",
    )
    return p.parse_args()


# DataFusion baseline numbers from `cargo bench --bench tpch -- --save-baseline
# sigma_a1_sf1_m3pro` on M3 Pro at SF=1 (committed in docs/BENCHMARKS.md).
# Rebench DataFusion + edit here if running on different hardware so the
# DF/PySpark ratio in the output table reflects same-host numbers.
DATAFUSION_BASELINE_M3PRO_SF1_MS = {
    "q01": 48.7,
    "q03": 34.6,
    "q06": 18.2,
    "q19": 38.0,
}


def main() -> None:
    args = parse_args()

    print(f"==> data dir: {args.data_dir}")
    print(f"==> trials per query: {args.trials} (after 1 warm-up)")
    print()

    spark = build_spark()
    print(f"==> Spark {spark.version}, JDK {os.environ.get('JAVA_HOME', 'system default')}")
    register_tables(spark, args.data_dir)

    results: dict[str, dict[str, float]] = {}
    for name in ("q01", "q03", "q06", "q19"):
        print(f"-- {name} --")
        results[name] = run(spark, name, args.trials)
        print()

    spark.stop()

    print("=== Results ===")
    print(emit_markdown(results, DATAFUSION_BASELINE_M3PRO_SF1_MS))


if __name__ == "__main__":
    main()
