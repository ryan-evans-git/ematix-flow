#!/usr/bin/env python3
"""
Σ.AI.1 — aggregate per-invocation outputs of `tpch_triangulation_bench`
into a single summary table.

For each query, computes across the input run files:
  - mean   — average of the per-invocation medians
  - stdev  — sample stdev across invocations (the noise-floor signal)
  - cv     — coefficient of variation (stdev / mean × 100)
  - min/max — the per-invocation range

When `--discard-first` is set, the first run file is excluded from
aggregation (it captures binary-load + page-cache cold reads + planner
warm-up). Reported in the summary header for transparency.

Input files are expected to be the `TPCH_OUT` markdown emitted by
`crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` —
specifically, a single ematix-flow column with `median ± stdev`
formatting per row, like:

    | Q01  | 234.83 ± 9.63 | 247.66 ± 19.28 | ...
"""

import argparse
import os
import re
import statistics
import sys


ROW_RE = re.compile(r'^\|\s*Q(\d{2})\s*\|\s*([\d.]+)\s*±\s*([\d.]+)\s*\|')


def parse_run(path: str) -> dict[str, tuple[float, float]]:
    """Returns {query_id: (median_ms, within_run_sigma_ms)}."""
    rows: dict[str, tuple[float, float]] = {}
    with open(path) as f:
        for line in f:
            m = ROW_RE.match(line)
            if m:
                q, med, sig = m.group(1), float(m.group(2)), float(m.group(3))
                rows[q] = (med, sig)
    return rows


def fmt_pct(x: float) -> str:
    return f"{x:+5.1f}%"


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", nargs="+", required=True,
                    help="run-*.md files to aggregate")
    ap.add_argument("--discard-first", action="store_true",
                    help="exclude the first run (catches cold-start)")
    ap.add_argument("--out", required=True, help="write summary to this path")
    args = ap.parse_args(argv)

    runs = sorted(args.runs)
    if len(runs) < 2:
        print(f"ERROR: need ≥2 run files (got {len(runs)})", file=sys.stderr)
        return 2

    discarded = []
    if args.discard_first:
        discarded.append(runs.pop(0))
    if len(runs) < 2:
        print(f"ERROR: need ≥2 runs after discard-first (got {len(runs)})", file=sys.stderr)
        return 2

    parsed = {os.path.basename(r): parse_run(r) for r in runs}
    if not parsed:
        print(f"ERROR: no rows parsed from runs", file=sys.stderr)
        return 2

    # Quality check — every run must have the same query set.
    qsets = [set(rows.keys()) for rows in parsed.values()]
    common = set.intersection(*qsets)
    missing = set.union(*qsets) - common
    if missing:
        print(f"WARNING: queries missing from at least one run: {sorted(missing)}",
              file=sys.stderr)

    # Aggregate. Report BOTH median-of-medians (robust to single-run
    # outliers — what we trust) and mean (for completeness). A common
    # failure mode under the strict protocol is a single bad-run outlier
    # (background macOS process, thermal event); median is invariant to
    # one outlier in N=3+ runs whereas mean inflates.
    rows = []
    for q in sorted(common):
        medians = [parsed[r][q][0] for r in parsed]
        median = statistics.median(medians)
        mean = statistics.mean(medians)
        sigma = statistics.stdev(medians) if len(medians) > 1 else 0.0
        cv = (sigma / median * 100) if median > 0 else 0.0
        rows.append({
            "q": q,
            "median": median,
            "mean": mean,
            "sigma": sigma,
            "cv": cv,
            "min": min(medians),
            "max": max(medians),
            "medians": medians,
        })

    # Write summary.
    with open(args.out, "w") as f:
        f.write("# Σ.AI.1 strict 22q SF=10 bench summary\n\n")
        f.write(f"- Runs aggregated: {len(parsed)} of {len(runs) + len(discarded)} "
                f"(discarded {len(discarded)} cold-start: "
                f"{[os.path.basename(d) for d in discarded]})\n")
        f.write(f"- Run files: {[os.path.basename(r) for r in runs]}\n\n")
        f.write("| Query | median ms | mean ms | σ ms | CV% | per-run medians |\n")
        f.write("|------:|----------:|--------:|-----:|----:|:----------------|\n")
        for r in rows:
            medians_str = " / ".join(f"{m:.1f}" for m in r["medians"])
            f.write(
                f"| Q{r['q']} | {r['median']:.2f} | {r['mean']:.2f} | "
                f"{r['sigma']:.2f} | {r['cv']:.2f}% | {medians_str} |\n"
            )
        f.write("\n")
        total_median = sum(r["median"] for r in rows)
        total_mean = sum(r["mean"] for r in rows)
        max_cv = max(r["cv"] for r in rows)
        median_cv = statistics.median(r["cv"] for r in rows)
        f.write(f"**Sum of medians**: {total_median:.2f} ms  (primary, robust to single-run outliers)\n")
        f.write(f"**Sum of means**: {total_mean:.2f} ms  (secondary)\n")
        f.write(f"**Median CV**: {median_cv:.2f}%  (noise-floor across invocations)\n")
        f.write(f"**Max CV**: {max_cv:.2f}%  (worst-case per-query noise — likely single-run outlier)\n")

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
