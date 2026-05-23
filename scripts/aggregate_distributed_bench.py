#!/usr/bin/env python3
"""
Aggregate distributed-bench JSONs from S3 into BENCHMARKS-DISTRIBUTED.md.

Reads JSON files produced by the per-engine bench runners
(ematix / trino / pyspark) at:

    s3://$BUCKET/results/<stamp>/<engine>-sf<N>.json

…and generates a single markdown table at
crates/ematix-flow-core/../../BENCHMARKS-DISTRIBUTED.md (repo root) with
side-by-side comparison.

Two metric columns per engine per SF:
- median(all trials)     — steady-state perf, the publishable number
- first_trial            — wall-time including any warmup tail (PySpark
                            JVM startup, Trino plan-cache miss). Lets
                            readers see cold-start vs hot-cache.

Usage:
    ./aggregate_distributed_bench.py --bucket <s3-bucket> --stamp <YYYYmmddTHHMMSSZ>
    ./aggregate_distributed_bench.py --local results/<stamp>/  # pre-downloaded
"""

import argparse
import json
import math
import os
import sys
from pathlib import Path

try:
    import boto3  # optional, only needed for --bucket mode
except ImportError:
    boto3 = None


REPO_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_PATH = REPO_ROOT / "BENCHMARKS-DISTRIBUTED.md"

ENGINES = ["ematix", "pyspark", "trino"]
SCALES = [10, 100]


def fetch_local(stamp_dir: Path) -> dict:
    """Load every <engine>-sf<N>.json from a local directory."""
    out = {}
    for engine in ENGINES:
        for sf in SCALES:
            f = stamp_dir / f"{engine}-sf{sf}.json"
            if f.exists():
                with f.open() as fh:
                    out[(engine, sf)] = json.load(fh)
            else:
                print(f"  missing: {f}", file=sys.stderr)
    return out


def fetch_s3(bucket: str, stamp: str) -> dict:
    if boto3 is None:
        sys.exit("boto3 not installed; pip install boto3 or use --local")
    s3 = boto3.client("s3")
    prefix = f"results/{stamp}/"
    out = {}
    for engine in ENGINES:
        for sf in SCALES:
            key = f"{prefix}{engine}-sf{sf}.json"
            try:
                obj = s3.get_object(Bucket=bucket, Key=key)
                out[(engine, sf)] = json.loads(obj["Body"].read())
            except s3.exceptions.NoSuchKey:
                print(f"  missing s3://{bucket}/{key}", file=sys.stderr)
    return out


def fmt_ms(v: float | None) -> str:
    if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
        return "—"
    if v >= 10000:
        return f"{v / 1000:.1f}s"
    if v >= 1000:
        return f"{v:.0f}"
    return f"{v:.1f}"


def render_table(results: dict, sf: int) -> str:
    """Render the per-SF table. Columns: Q, ematix-med, ematix-first,
    pyspark-med, pyspark-first, trino-med, trino-first, best, notes."""
    engines_present = [e for e in ENGINES if (e, sf) in results]
    if not engines_present:
        return f"\n## SF={sf}\n\n_No data available._\n"

    # Union of query labels across all engines (typically all 22).
    queries = set()
    for e in engines_present:
        queries.update(results[(e, sf)]["queries"].keys())
    sorted_qs = sorted(queries, key=lambda q: int(q[1:]))

    cluster_size = results[(engines_present[0], sf)].get("cluster_size", "?")
    lines = []
    lines.append(f"\n## SF={sf}, {cluster_size}-node cluster\n")
    lines.append(
        "Two columns per engine: `med` = median across all measured trials "
        "(steady-state). `first` = first trial wall-time (cold-cache, "
        "JVM-startup-inclusive). All values in **ms** except where suffixed "
        "with `s` (seconds, for >10s queries).\n"
    )

    # Header
    header = ["Q"]
    for e in engines_present:
        header += [f"{e} med", f"{e} first"]
    header += ["best", "notes"]
    lines.append("| " + " | ".join(header) + " |")
    lines.append("|" + "|".join(["---:"] * len(header)) + "|")

    # Rows
    for q in sorted_qs:
        row = [q]
        meds = {}
        firsts = {}
        rows_returned = set()
        for e in engines_present:
            stats = results[(e, sf)]["queries"].get(q)
            if not stats:
                row += ["—", "—"]
                continue
            med = stats.get("median_ms")
            first = stats.get("first_trial_ms")
            row += [fmt_ms(med), fmt_ms(first)]
            if med is not None and not (isinstance(med, float) and math.isnan(med)):
                meds[e] = med
            if first is not None and not (isinstance(first, float) and math.isnan(first)):
                firsts[e] = first
            r = stats.get("rows_returned")
            if r is not None:
                rows_returned.add(r)

        # Best column (by median)
        if meds:
            best_engine = min(meds, key=lambda k: meds[k])
            row.append(best_engine)
        else:
            row.append("—")

        # Notes: row-count mismatch is a CORRECTNESS warning
        if len(rows_returned) > 1:
            row.append(f"⚠ rows differ: {sorted(rows_returned)}")
        else:
            row.append("")
        lines.append("| " + " | ".join(row) + " |")

    # Geomean row (across all queries where every engine has a number).
    common = [
        q
        for q in sorted_qs
        if all(
            results[(e, sf)]["queries"].get(q)
            and not math.isnan(results[(e, sf)]["queries"][q].get("median_ms", float("nan")))
            for e in engines_present
        )
    ]
    if common:
        lines.append("")
        lines.append(f"**Geomean (across {len(common)} queries with all-engine data):**\n")
        ge = []
        for e in engines_present:
            ms = [results[(e, sf)]["queries"][q]["median_ms"] for q in common]
            gm = math.exp(sum(math.log(v) for v in ms) / len(ms))
            ge.append(f"- **{e}**: {fmt_ms(gm)} ms")
        lines.extend(ge)

    return "\n".join(lines) + "\n"


def render_doc(results: dict, stamp: str | None) -> str:
    out = []
    out.append("# Distributed TPC-H bench — ematix-flow vs PySpark vs Trino\n")
    out.append(
        "4-node cluster (1 coordinator + 3 workers) on AWS. Bench harness "
        "in `infra/test-validation-distributed/`. Per-engine runners under "
        "`infra/distributed-peers/` and "
        "`crates/ematix-flow-distributed/examples/tpch_distributed_campaign.rs`. "
        "All engines hit the same parquet at "
        "`s3://<bench-bucket>/tpch-data/sf{10,100}/`.\n"
    )
    if stamp:
        out.append(f"_Source: `s3://<bucket>/results/{stamp}/`_\n")

    for sf in SCALES:
        out.append(render_table(results, sf))

    out.append("\n## Methodology\n")
    out.append("- **trials**: 5 measured per query, after 2 untimed warmups.\n")
    out.append(
        "- **median**: across all 5 measured trials. `first` = the first "
        "measured trial only — exposes cold-cache / JVM startup tail.\n"
    )
    out.append(
        "- **rows**: every engine's row count is checked; any divergence "
        "flags a ⚠ in the notes column (treat as a correctness issue).\n"
    )
    out.append(
        "- **SF=10 cluster**: c7i.2xlarge × 4 (16 GB RAM per node).\n"
        "- **SF=100 cluster**: c7i.4xlarge × 4 (32 GB RAM per node).\n"
    )
    out.append("- **SQL**: queries at `examples/tpch/queries/q*.sql`.\n")
    out.append(
        "    - **Trino** consumes the canonical files unmodified.\n"
        "    - **PySpark** applies 2 mechanical regex rewrites (Q01 `interval '90' day` "
        "→ `interval 90 day`; Q22 `substring(c_phone from 1 for 2)` → "
        "`substring(c_phone, 1, 2)`) — see `infra/distributed-peers/pyspark/bench.py`.\n"
        "    - **ematix-flow** uses the files via DataFusion's parser, which "
        "accepts the canonical TPC-H dialect.\n"
    )
    return "".join(out)


def main():
    p = argparse.ArgumentParser()
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--bucket", help="S3 bucket with results")
    src.add_argument("--local", type=Path, help="Local directory with <engine>-sf<N>.json")
    p.add_argument("--stamp", help="Timestamp directory under results/ (required for --bucket)")
    p.add_argument("--out", type=Path, default=OUTPUT_PATH)
    args = p.parse_args()

    if args.bucket and not args.stamp:
        sys.exit("--bucket requires --stamp")

    if args.local:
        results = fetch_local(args.local)
        stamp = args.local.name
    else:
        results = fetch_s3(args.bucket, args.stamp)
        stamp = args.stamp

    if not results:
        sys.exit("No JSON files found; nothing to aggregate")

    doc = render_doc(results, stamp)
    args.out.write_text(doc)
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
