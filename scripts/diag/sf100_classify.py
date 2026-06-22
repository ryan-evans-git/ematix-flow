#!/usr/bin/env python3
"""SF=100 loss classifier — turns the instrumented tpch_preset_rebench logs into
a per-query root-cause verdict.

It reads the `[trial]` and `[peak]` markers emitted by the diagnostic build of
`tpch_preset_rebench` (cpu_s / eff / pin_mb / peak_rss_mb) and applies the
decision tree documented in docs/plans/SF100_LOSS_DIAGNOSTIC.md to classify each
query's SF=100 loss into one of:

  NOT-A-LOSS         isolated-warm parity (ratio <= 1.05) — nothing to fix
  BOX-ARTIFACT       only slow in-sweep; the 36 GB box, not the engine
  RSS/CACHE-BOUND    high peak-RSS + still paging in warm → demand reduction
  PARALLEL-EFFICIENCY same total CPU as DuckDB but fewer cores busy → morsel
  COMPUTE-EXCESS     we burn more CPU than DuckDB → needs samply + plan-diff
  UNRESOLVED         real loss, no clean signal → re-measure / inspect

Stdlib only. Usage:
  sf100_classify.py --ematix iso_ematix.log --duckdb iso_duckdb.log \\
                    [--insweep insweep.log] [--queries 8,9,10]

The two required logs MUST be ISOLATED runs (SKIP_DUCKDB=1 for --ematix,
SKIP_EMATIX=1 for --duckdb) — peak RSS and pageins are only attributable to one
engine when that engine is the only thing in the process (DuckDB runs in-process
here, so an interleaved run pollutes both). Pass several invocations concatenated
into each log to honour the 3-invocation rule; trials are pooled before the
median.
"""
import argparse
import re
import statistics as st
import sys
from collections import defaultdict

# ---- decision-tree thresholds (see docs/plans/SF100_LOSS_DIAGNOSTIC.md) -------
LOSS_THRESH = 1.05    # ratio below this is inside the SF=100 per-query noise floor
RSS_BLOAT = 1.5       # ematix peak RSS >= 1.5x DuckDB => memory-demand class candidate
PAGEIN_HIGH = 1500.0  # MB paged in during a WARM timed trial => working set > page cache
EFF_LOW = 9.0         # mean cores busy < this (of 14) => under-parallelised
CPU_PARITY = 0.15     # |cpu_ematix/cpu_duckdb - 1| <= this => same total work
BOX_DELTA = 1.25      # in-sweep / isolated wall >= this => the loss is box cache pressure
CORES = 14

KV = re.compile(r"(\w+)=(-?[\d.]+)")


QIDS = re.compile(r"qids=([\d.]+)")


def parse_log(path):
    """Return (trials, peak_records).

    trials: {(eng, q): {field: [values...]}} pooled across all invocations.
    peak_records: list of (frozenset(qids), peak_rss_mb) — one per [peak] line. A
    record with a single qid is per-query (one query/process); a multi-qid record
    is a run-level fallback."""
    trials = defaultdict(lambda: defaultdict(list))
    peaks = []
    with open(path) as fh:
        for line in fh:
            if line.startswith("[trial]"):
                kv = dict(KV.findall(line))
                eng = "ematix" if "eng=ematix" in line else ("duckdb" if "eng=duckdb" in line else None)
                if eng is None or "ti" not in kv or "q" not in kv:
                    continue  # skip warmups (warm{wi}) and malformed lines
                q = int(float(kv["q"]))
                for f in ("ms", "cpu_s", "eff", "pin_mb", "rss_mb"):
                    if f in kv:
                        trials[(eng, q)][f].append(float(kv[f]))
            elif line.startswith("[peak]"):
                kv = dict(KV.findall(line))
                m = QIDS.search(line)
                qids = frozenset(int(x) for x in m.group(1).split(".") if x) if m else frozenset()
                if "peak_rss_mb" in kv:
                    peaks.append((qids, float(kv["peak_rss_mb"])))
    return trials, peaks


def med(xs):
    return st.median(xs) if xs else float("nan")


def peak_for(q, records):
    """Per-query peak RSS: median of single-qid records for q, else median of all
    (run-level fallback when queries shared a process)."""
    per_q = [mb for (qids, mb) in records if qids == frozenset({q})]
    if per_q:
        return med(per_q)
    return med([mb for (_, mb) in records])


def classify(q, e, d, e_peak, d_peak, insweep_e):
    """e/d: per-engine median dicts. Returns (verdict, lever, detail)."""
    wall_e, wall_d = e.get("ms", float("nan")), d.get("ms", float("nan"))
    if wall_e != wall_e or wall_d != wall_d:  # NaN guard
        return ("NO-DATA", "-", "missing ematix or duckdb timing")
    W = wall_e / wall_d
    R = (e_peak / d_peak) if (e_peak and d_peak) else float("nan")
    E = e.get("eff", float("nan"))
    cpu_e, cpu_d = e.get("cpu_s", float("nan")), d.get("cpu_s", float("nan"))
    Cpu = (cpu_e / cpu_d) if (cpu_d and cpu_d == cpu_d and cpu_d > 0) else float("nan")
    P = e.get("pin_mb", float("nan"))
    box = (insweep_e / wall_e) if (insweep_e and insweep_e == insweep_e) else float("nan")
    detail = (f"W={W:.2f} R={R:.2f} eff={E:.1f}/{CORES} cpu_ratio={Cpu:.2f} "
              f"pin={P:.0f}MB box={box:.2f}")

    # Box artifact: in-sweep much slower than isolated AND isolated ~ parity.
    if box == box and box >= BOX_DELTA and W <= LOSS_THRESH:
        return ("BOX-ARTIFACT", "demand-reduction OR publish isolated-warm", detail)
    # Not a real loss isolated-warm.
    if W <= LOSS_THRESH:
        return ("NOT-A-LOSS", "-", detail)
    # Real isolated loss — route by mechanism.
    if R == R and R >= RSS_BLOAT and P == P and P >= PAGEIN_HIGH:
        return ("RSS/CACHE-BOUND", "streaming decode / bounded pool / spill", detail)
    if Cpu == Cpu and abs(Cpu - 1.0) <= CPU_PARITY and E == E and E < EFF_LOW:
        return ("PARALLEL-EFFICIENCY", "morsel / work-stealing region", detail)
    if Cpu == Cpu and Cpu > 1.0 + CPU_PARITY:
        return ("COMPUTE-EXCESS", "samply split + duckdb_profile_dump plan-diff", detail)
    return ("UNRESOLVED", "re-measure (3x) / inspect signal vector", detail)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ematix", required=True, help="isolated ematix log (SKIP_DUCKDB=1)")
    ap.add_argument("--duckdb", required=True, help="isolated DuckDB log (SKIP_EMATIX=1)")
    ap.add_argument("--insweep", help="optional interleaved (both engines) log for box-delta")
    ap.add_argument("--queries", help="comma list, e.g. 8,9,10 (default: all seen)")
    a = ap.parse_args()

    et, eps = parse_log(a.ematix)
    dt, dps = parse_log(a.duckdb)
    it, _ = parse_log(a.insweep) if a.insweep else ({}, [])

    qs = ([int(x) for x in a.queries.split(",") if x.strip()] if a.queries
          else sorted({q for (eng, q) in list(et) + list(dt)}))

    e_peak_all = med([mb for (_, mb) in eps])
    d_peak_all = med([mb for (_, mb) in dps])
    print(f"# SF=100 loss classification  (ematix peak~{e_peak_all:.0f}MB  duckdb peak~{d_peak_all:.0f}MB; per-query below)")
    print(f"# isolated ematix={a.ematix}  duckdb={a.duckdb}  insweep={a.insweep or '-'}")
    print(f"{'Q':>3} {'verdict':<20} {'signal vector':<58} lever")
    print("-" * 110)
    rows = []
    for q in qs:
        e = {f: med(v) for f, v in et.get(("ematix", q), {}).items()}
        d = {f: med(v) for f, v in dt.get(("duckdb", q), {}).items()}
        e_peak, d_peak = peak_for(q, eps), peak_for(q, dps)
        insweep_e = med(it.get(("ematix", q), {}).get("ms", [])) if it else float("nan")
        verdict, lever, detail = classify(q, e, d, e_peak, d_peak, insweep_e)
        rows.append((q, verdict, lever))
        print(f"Q{q:02} {verdict:<20} {detail:<58} {lever}")
    print("-" * 110)
    # Summary by class.
    by = defaultdict(list)
    for q, v, _ in rows:
        by[v].append(f"Q{q:02}")
    print("\n## summary")
    for v in ("RSS/CACHE-BOUND", "PARALLEL-EFFICIENCY", "COMPUTE-EXCESS",
              "BOX-ARTIFACT", "UNRESOLVED", "NOT-A-LOSS", "NO-DATA"):
        if by.get(v):
            print(f"  {v:<20} {', '.join(by[v])}")


if __name__ == "__main__":
    sys.exit(main())
