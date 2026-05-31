#!/usr/bin/env python3
"""
Σ.AI.2 — per-query A vs B diff under strict interleaved A/B bench.

Reads two `strict-22q-summary.md` files, computes per-query Δ ms and
Δ%, classifies as win/loss/noise using 2× max(σ_A, σ_B) as the bar.

Outputs a markdown diff table + net sum-of-medians comparison.
"""

import argparse
import os
import re
import sys


# strict-summarize summary rows look like:
# | Q01 | 250.40 | 252.23 | 6.23 | 2.49% | ...
ROW_RE = re.compile(r'^\|\s*Q(\d{2})\s*\|\s*([\d.]+)\s*\|\s*([\d.]+)\s*\|\s*([\d.]+)\s*\|')


def parse(path: str) -> dict[str, dict[str, float]]:
    rows = {}
    with open(path) as f:
        for line in f:
            m = ROW_RE.match(line)
            if m:
                q = m.group(1)
                rows[q] = {
                    "median": float(m.group(2)),
                    "mean": float(m.group(3)),
                    "sigma": float(m.group(4)),
                }
    return rows


def main(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument("--a", required=True)
    ap.add_argument("--b", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args(argv)

    a = parse(args.a)
    b = parse(args.b)
    common = sorted(set(a.keys()) & set(b.keys()))

    rows = []
    for q in common:
        a_med, a_sig = a[q]["median"], a[q]["sigma"]
        b_med, b_sig = b[q]["median"], b[q]["sigma"]
        delta = b_med - a_med
        pct = (delta / a_med * 100) if a_med > 0 else 0.0
        bar = 2 * max(a_sig, b_sig)
        if abs(delta) <= bar:
            verdict = "noise"
        elif delta < 0:
            verdict = "WIN"
        else:
            verdict = "regression"
        rows.append({
            "q": q,
            "a": a_med, "a_sigma": a_sig,
            "b": b_med, "b_sigma": b_sig,
            "delta": delta,
            "pct": pct,
            "bar": bar,
            "verdict": verdict,
        })

    total_a = sum(r["a"] for r in rows)
    total_b = sum(r["b"] for r in rows)

    with open(args.out, "w") as f:
        f.write("# Σ.AI.2 strict interleaved A/B diff\n\n")
        f.write(f"- A summary: `{os.path.basename(os.path.dirname(args.a))}/strict-22q-summary.md`\n")
        f.write(f"- B summary: `{os.path.basename(os.path.dirname(args.b))}/strict-22q-summary.md`\n\n")
        f.write("Per-query Δ = B − A. Verdict uses 2× max(σ_A, σ_B) as the noise bar.\n\n")
        f.write("| Query | A median | B median | Δ ms | Δ % | bar (2σ) | verdict |\n")
        f.write("|------:|---------:|---------:|-----:|----:|---------:|:--------|\n")
        for r in rows:
            f.write(
                f"| Q{r['q']} | {r['a']:.2f} | {r['b']:.2f} | "
                f"{r['delta']:+.2f} | {r['pct']:+.2f}% | "
                f"{r['bar']:.2f} | {r['verdict']} |\n"
            )
        f.write("\n")
        f.write(f"**Sum of A medians**: {total_a:.2f} ms\n")
        f.write(f"**Sum of B medians**: {total_b:.2f} ms\n")
        delta_total = total_b - total_a
        pct_total = (delta_total / total_a * 100) if total_a > 0 else 0.0
        f.write(f"**Net Δ**: {delta_total:+.2f} ms ({pct_total:+.2f}%)\n\n")

        wins = [r for r in rows if r["verdict"] == "WIN"]
        regs = [r for r in rows if r["verdict"] == "regression"]
        f.write(f"**Clear wins (>2σ)**: {len(wins)}  ")
        f.write(", ".join(f"Q{r['q']} {r['delta']:+.1f}ms" for r in wins) + "\n")
        f.write(f"**Clear regressions (>2σ)**: {len(regs)}  ")
        f.write(", ".join(f"Q{r['q']} {r['delta']:+.1f}ms" for r in regs) + "\n")


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
