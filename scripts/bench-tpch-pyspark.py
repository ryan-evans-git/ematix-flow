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
# Σ.C extension: all 22 TPC-H spec queries. The .sql files are
# canonical-substituted output from the Rust extractor at
# crates/ematix-flow-core/examples/tpch_extract_queries.rs.
QUERY_NAMES = [f"q{i:02d}" for i in range(1, 23)]
QUERY_FILES = {name: QUERIES_DIR / f"{name}.sql" for name in QUERY_NAMES}


def build_spark() -> SparkSession:
    # JDK 18+ deprecated `javax.security.auth.Subject.getSubject(...)`,
    # which Spark's UGI shim still calls during analyzer setup. Without
    # `-Djava.security.manager=allow` this throws
    # `UnsupportedOperationException: getSubject is supported only if a
    # security manager is allowed` on the first DataFrame call. Officially
    # Spark 4.x supports JDK 17/21; we set the flag so JDK 23 (Homebrew's
    # `openjdk` cask) also works for local benching.
    java_opts = "-Djava.security.manager=allow"
    # Driver memory + shuffle partitions are env-overridable so the same
    # harness scales from SF=1 (4g / 8 parts) up to SF=100 (e.g. 16g / 64
    # parts) where 600M-row shuffles need a bigger heap + finer partitions
    # to spill cleanly. Defaults preserve the original SF=1/SF=10 config.
    driver_mem = os.environ.get("SPARK_DRIVER_MEM", "4g")
    shuffle_parts = os.environ.get("SPARK_SHUFFLE_PARTS", "8")
    return (
        SparkSession.builder.master("local[*]")
        .appName("ematix-flow-tpch-bench")
        .config("spark.driver.memory", driver_mem)
        .config("spark.driver.extraJavaOptions", java_opts)
        .config("spark.executor.extraJavaOptions", java_opts)
        .config("spark.sql.shuffle.partitions", shuffle_parts)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
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


def scale_factor_from_data_dir(data_dir: Path) -> int:
    """Derive the integer scale factor from a trailing `sf<N>` path component
    (e.g. .../data/sf10 -> 10). Returns 1 when absent (mini/unknown layouts).
    Mirrors ematix_flow_core::tpch_params::scale_factor_from_data_dir."""
    for part in reversed(data_dir.parts):
        if part.startswith("sf"):
            try:
                return int(part[2:])
            except ValueError:
                pass
    return 1


def apply_q11_fraction(name: str, sql: str, scale_factor: int) -> str:
    """#315: TPC-H Q11's HAVING threshold parameter is FRACTION = 0.0001 / SF.
    The q11.sql file holds the SF=1 value; at SF>=10 that's degenerate (0 rows
    for every engine). Scale it so Spark measures a real Q11, matching the
    ematix/DuckDB harnesses. No-op for q!=11 or SF<=1."""
    if name == "q11" and scale_factor > 1:
        return sql.replace("0.0001", f"(0.0001 / {scale_factor})", 1)
    return sql


def load_query(name: str, scale_factor: int = 1) -> str:
    path = QUERY_FILES[name]
    raw = path.read_text()
    # Strip terminating semicolons — Spark's `spark.sql()` rejects
    # them. The .sql files have them so DataFusion's harness + a human
    # reading the file see standard SQL.
    sql = raw.strip().rstrip(";").strip()
    return apply_q11_fraction(name, sql, scale_factor)


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


def run(
    spark: SparkSession, name: str, trials: int, warmups: int = 1, scale_factor: int = 1
) -> dict[str, float]:
    sql = load_query(name, scale_factor)

    # Warm-ups: N untimed runs to let the JIT + Catalyst codegen settle.
    row_count = 0
    for _ in range(max(1, warmups)):
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
        "first_trial_ms": (timings[0] * 1000) if timings else float("nan"),
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
    for name in QUERY_NAMES:
        if name not in results:
            continue
        spark_med = results[name]["median_ms"]
        df_med = df_baseline.get(name, 0.0)
        ratio = (df_med / spark_med) if spark_med and df_med else 0.0
        rng = f"{results[name]['min_ms']:.1f} / {results[name]['max_ms']:.1f}"
        df_cell = f"{df_med:>5.1f}" if df_med else "  TBD"
        ratio_cell = f"{ratio:.3f}" if ratio else "—"
        lines.append(
            f"| {name.upper()} | {df_cell} | {spark_med:>7.1f} | "
            f"{ratio_cell} | {rng} | {int(results[name]['rows'])} |"
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
        help="timed trials per query after warm-ups (default: 3)",
    )
    p.add_argument(
        "--warmups",
        type=int,
        default=1,
        help="untimed warm-up runs per query (default: 1)",
    )
    return p.parse_args()


# DataFusion baseline numbers from `cargo bench --bench tpch` on M3 Pro
# at SF=1. The first 4 (Q1/Q3/Q6/Q19) are the committed Σ.A1 PR 2 baseline;
# the remaining 18 came from the Σ.C extension run (2026-05-05). Rebench
# DataFusion + edit here if running on different hardware so the
# DF/PySpark ratio in the output table reflects same-host numbers.
# Queries not listed will render as "TBD" / "—" in the output.
DATAFUSION_BASELINE_M3PRO_SF1_MS: dict[str, float] = {
    "q01": 48.7,
    "q03": 34.6,
    "q06": 18.2,
    "q19": 38.0,
}


def main() -> None:
    args = parse_args()

    print(f"==> data dir: {args.data_dir}")
    print(f"==> trials per query: {args.trials} (after {args.warmups} warm-up{'s' if args.warmups != 1 else ''})")
    print()

    spark = build_spark()
    print(f"==> Spark {spark.version}, JDK {os.environ.get('JAVA_HOME', 'system default')}")
    register_tables(spark, args.data_dir)

    scale_factor = scale_factor_from_data_dir(Path(args.data_dir))
    if scale_factor > 1:
        print(f"==> scale factor {scale_factor} (Q11 fraction scaled to 0.0001/{scale_factor})")

    results: dict[str, dict[str, float]] = {}
    for name in QUERY_NAMES:
        print(f"-- {name} --")
        try:
            results[name] = run(spark, name, args.trials, args.warmups, scale_factor)
        except Exception as e:  # noqa: BLE001 — surface, don't kill the run
            print(f"  FAIL: {e}")
        print()

    spark.stop()

    # #209: emit a campaign-shaped JSON (Q{NN} keys, same shape as the ematix
    # distributed campaign) for cross-engine comparison. Opt-in via env.
    json_out = os.environ.get("PYSPARK_JSON_OUT")
    if json_out:
        import json as _json

        payload = {
            "engine": "pyspark",
            "scale_factor": scale_factor,
            "cluster_size": 1,  # local[*] — single JVM, all cores
            "queries": {
                f"Q{int(name[1:]):02d}": {
                    "median_ms": r["median_ms"],
                    "first_trial_ms": r.get("first_trial_ms", r["median_ms"]),
                    "min_ms": r.get("min_ms"),
                    "max_ms": r.get("max_ms"),
                    "rows_returned": int(r["rows"]),
                }
                for name, r in results.items()
            },
        }
        Path(json_out).write_text(_json.dumps(payload, indent=2))
        print(f"==> wrote {json_out}")

    print("=== Results ===")
    print(emit_markdown(results, DATAFUSION_BASELINE_M3PRO_SF1_MS))


if __name__ == "__main__":
    main()
