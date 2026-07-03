#!/usr/bin/env python3
"""
Σ.AI.3 — aggregate strict_throughput.sh output into a throughput summary.

Input layout (produced by strict_throughput.sh):

    RUN_DIR/<engine>/s<N>/batch-<b>/stream-<s>.md   triangulation table per stream
    RUN_DIR/<engine>/s<N>/batch-<b>/batch.json      {"makespan_ms": ..., ...}
    RUN_DIR/env.json                                optional machine metadata

Per (engine, stream-count): the first batch is DISCARDED (cold start:
binary load, page-cache, planner warm-up), mirroring the discard-first
discipline of strict_22q.sh. From the surviving batches we report:

  - makespan: median across batches of the wall time to drain all N streams
  - QPH: N * queries-per-stream / (makespan hours) — TPC-H throughput style
  - per-query p50/p95/p99 latency across every (stream, batch) sample

Parsing is COLUMN-AWARE: a solo-engine run leaves the other engines'
columns as "—", so each engine's numbers are read from its own column of
the triangulation table.
"""

import argparse
import glob
import json
import os
import re
import statistics
import sys

# Column order in write_benchmarks_md: ematix-flow, DuckDB, Polars.
ENGINE_COLS = {"ematix": 0, "duckdb": 1, "polars": 2}

Q_RE = re.compile(r"^Q(\d{2})")
VAL_RE = re.compile(r"([\d.]+)\s*±")


def parse_stream(path: str, engine: str) -> dict[str, float]:
    """{query_id: median_ms} for `engine`'s column of a stream table."""
    col = ENGINE_COLS[engine]
    rows: dict[str, float] = {}
    with open(path) as f:
        for line in f:
            if not line.startswith("|"):
                continue
            cells = [c.strip() for c in line.strip().strip("|").split("|")]
            if not cells or not Q_RE.match(cells[0]):
                continue
            q = Q_RE.match(cells[0]).group(1)
            if len(cells) < col + 2:
                continue
            m = VAL_RE.search(cells[col + 1])
            if m:
                rows[q] = float(m.group(1))
    return rows


def pctile(sorted_vals: list[float], p: float) -> float:
    """Nearest-rank percentile on a pre-sorted list."""
    if not sorted_vals:
        return float("nan")
    k = max(0, min(len(sorted_vals) - 1,
                   round(p / 100.0 * (len(sorted_vals) - 1))))
    return sorted_vals[k]


def main(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args(argv)

    sections = []
    combos = sorted(glob.glob(os.path.join(args.run_dir, "*", "s*")))
    if not combos:
        print(f"ERROR: no <engine>/s<N> dirs under {args.run_dir}", file=sys.stderr)
        return 2

    for combo in combos:
        engine = os.path.basename(os.path.dirname(combo))
        if engine not in ENGINE_COLS:
            print(f"WARNING: skipping unknown engine dir {combo}", file=sys.stderr)
            continue
        n_streams = int(os.path.basename(combo).lstrip("s"))
        batches = sorted(
            glob.glob(os.path.join(combo, "batch-*")),
            key=lambda p: int(p.rsplit("-", 1)[1]),
        )
        if len(batches) < 2:
            print(
                f"ERROR: {combo} has {len(batches)} batch(es); need >=2 "
                f"(the first is discarded as cold-start)", file=sys.stderr)
            return 2
        discarded, kept = batches[0], batches[1:]

        makespans, samples, queries_per_stream = [], {}, None
        for b in kept:
            meta = json.load(open(os.path.join(b, "batch.json")))
            makespans.append(float(meta["makespan_ms"]))
            for sf in sorted(glob.glob(os.path.join(b, "stream-*.md"))):
                rows = parse_stream(sf, engine)
                if queries_per_stream is None and rows:
                    queries_per_stream = len(rows)
                for q, ms in rows.items():
                    samples.setdefault(q, []).append(ms)

        queries_per_stream = queries_per_stream or 22
        makespan_med = statistics.median(makespans)
        qph = (n_streams * queries_per_stream) / (makespan_med / 1000.0 / 3600.0)

        lines = [
            f"## {engine} — {n_streams} concurrent stream(s)",
            "",
            f"- Batches: {len(kept)} kept, 1 discarded as cold-start "
            f"(`{os.path.basename(discarded)}`)",
            f"- Makespan (median across batches): {makespan_med:.0f} ms",
            f"- Throughput: **{qph:.1f} QPH** "
            f"({n_streams} streams x {queries_per_stream} queries)",
            "",
            "| Query | p50 ms | p95 ms | p99 ms | samples |",
            "|------:|-------:|-------:|-------:|--------:|",
        ]
        for q in sorted(samples):
            vals = sorted(samples[q])
            lines.append(
                f"| Q{q} | {pctile(vals, 50):.2f} | {pctile(vals, 95):.2f} "
                f"| {pctile(vals, 99):.2f} | {len(vals)} |")
        sections.append("\n".join(lines))

    header = ["# Σ.AI.3 strict throughput summary", ""]
    env_path = os.path.join(args.run_dir, "env.json")
    if os.path.exists(env_path):
        env = json.load(open(env_path))
        header += [
            f"- Machine: {env.get('chip', '?')} "
            f"({env.get('perf_cores', '?')}P+{env.get('efficiency_cores', '?')}E), "
            f"macOS {env.get('macos', '?')}",
            f"- Git: {env.get('git_sha', '?')[:12]} "
            f"(dirty: {env.get('git_dirty', '?')})",
            f"- Run config: {env.get('run', {})}",
            "",
        ]

    with open(args.out, "w") as f:
        f.write("\n".join(header) + "\n" + "\n\n".join(sections) + "\n")
    print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
