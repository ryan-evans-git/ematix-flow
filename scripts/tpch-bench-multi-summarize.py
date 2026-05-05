#!/usr/bin/env python3
"""Σ.C PR 2 prep: parse the three benchmark output files
(`datafusion.txt`, `distributed.txt`, `pyspark.txt`) and emit a
markdown comparison table pasteable into docs/BENCHMARKS.md.

Each input is captured stdout from the corresponding benchmark.
Criterion outputs lines of the shape:

    tpch_sf1/q06            time:   [17.168 ms 17.223 ms 17.280 ms]

We pull the median (middle of the three numbers). PySpark output
follows the format `bench-tpch-pyspark.py` emits — a markdown table
in its trailing section.

Output is markdown:

    | Query | DataFusion (ms) | distributed (ms) | PySpark (ms) | dist/df | dist/pyspark |
    | ---   | ---             | ---              | ---          | ---     | ---          |
    | Q1    | 48.7            | 53.9             | 192.6        | 1.11×   | 0.28×        |
    ...
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

CRITERION_NAME = re.compile(r"^(?P<name>[a-zA-Z0-9_/]+)\s*$")
CRITERION_TIME = re.compile(
    # criterion's per-bench summary spans two lines: a line with just
    # the name (e.g. `tpch_sf1/q06`), then a line with the timing
    # numbers (e.g. `                        time:   [17.168 ms ...]`).
    # Match the time line on its own; pair with the most recent
    # name line during the linear scan.
    r"^\s*time:\s+\[(?P<lo>[0-9.]+) ms (?P<med>[0-9.]+) ms (?P<hi>[0-9.]+) ms\]"
)
PYSPARK_TABLE_ROW = re.compile(
    # bench-tpch-pyspark.py emits rows like:
    # | Q01 |  48.7 |   192.6 | 0.253 | 191.1 / 324.6 |  4 |
    r"^\|\s*(?P<q>Q\d+)\s*\|\s*[0-9.]+\s*\|\s*(?P<med>[0-9.]+)\s*\|"
)


def parse_criterion(path: Path, suite_prefix: str) -> dict[str, float]:
    """Return {q01: median_ms, ...} for criterion entries in `path`
    that start with `suite_prefix/` (e.g. `tpch_sf1` or
    `tpch_sf1_distributed_3_workers`).

    criterion's pretty-printer splits each entry across two lines:

        tpch_sf1_distributed_3_workers/q01
                                time:   [53.278 ms 53.617 ms 54.103 ms]

    so we track the most recent matching name and pair it with the
    next `time:` line we see.
    """
    results: dict[str, float] = {}
    if not path.exists():
        return results
    pending_query: str | None = None
    for line in path.read_text().splitlines():
        if (m := CRITERION_NAME.match(line)) is not None:
            name = m.group("name")
            if name.startswith(f"{suite_prefix}/"):
                pending_query = name.split("/", 1)[1]
            else:
                pending_query = None
            continue
        if pending_query is None:
            continue
        if (m := CRITERION_TIME.match(line)) is not None:
            results[pending_query] = float(m.group("med"))
            pending_query = None
    return results


def parse_pyspark(path: Path) -> dict[str, float]:
    results: dict[str, float] = {}
    if not path.exists():
        return results
    for line in path.read_text().splitlines():
        m = PYSPARK_TABLE_ROW.match(line)
        if not m:
            continue
        results[m.group("q").lower()] = float(m.group("med"))
    return results


def fmt_ms(v: float | None) -> str:
    if v is None:
        return "-"
    return f"{v:.1f}"


def fmt_ratio(num: float | None, den: float | None) -> str:
    if num is None or den is None or den == 0:
        return "-"
    return f"{num / den:.2f}×"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sf", required=True)
    ap.add_argument("--datafusion-output", type=Path, required=True)
    ap.add_argument("--distributed-output", type=Path, required=True)
    ap.add_argument("--pyspark-output", type=Path, required=True)
    ap.add_argument("--output", type=Path, default=Path("/dev/stdout"))
    args = ap.parse_args()

    df = parse_criterion(args.datafusion_output, f"tpch_sf{args.sf}")
    dist = parse_criterion(
        args.distributed_output, f"tpch_sf{args.sf}_distributed_3_workers"
    )
    pyspark = parse_pyspark(args.pyspark_output)

    queries = ["q01", "q03", "q06", "q19"]
    lines = [
        f"### Σ.C PR 2 — TPC-H SF={args.sf} multi-host comparison",
        "",
        "| Query | DataFusion (ms) | distributed (ms) | PySpark (ms) | dist/df | dist/pyspark |",
        "|---|---|---|---|---|---|",
    ]
    for q in queries:
        d = df.get(q)
        di = dist.get(q)
        py = pyspark.get(q)
        lines.append(
            f"| {q.upper()} | {fmt_ms(d)} | {fmt_ms(di)} | {fmt_ms(py)} | "
            f"{fmt_ratio(di, d)} | {fmt_ratio(di, py)} |"
        )

    txt = "\n".join(lines) + "\n"
    if str(args.output) == "/dev/stdout":
        sys.stdout.write(txt)
    else:
        args.output.write_text(txt)
        print(f"wrote {args.output}", file=sys.stderr)


if __name__ == "__main__":
    main()
