# SF=10 tie-breaking — 2026-07-03/04

Goal: flip the two SF=10 statistical ties (Q01, Q05) without losing
anywhere else.

## Q01 — FLIPPED (clear win)

Root cause (fresh partitions-matrix diagnostic, `pm-*` runs): the
58-RG / 14-partition pigeonhole tail; only 1-RG-per-partition removes
it (P=58 → −7%; P=28 neutral, which is why the old Gate-B multiplier
measured neutral). RG costs are uniform (dbgen), so static balancing
cannot help.

Levers landed:
1. `perf/lpt-rg-assignment` — LPT cost-balanced RG assignment
   (default ON, `EMAT_BALANCED_RG_ASSIGN=0` escape). TPC-H-neutral;
   pays on cost-skewed real-world files.
2. `perf/gateb-rg-granularity` — Gate-B low-card GROUP BY boost
   retuned to one-RG-per-partition (join-free + dict-NDV ≤ 50K +
   single multi-RG scan + SOLO via registry; declines when
   num_rgs > 8×core-share — partial granulation measured +7-8% on
   SF=100; vanishes under multi-stream load). Default ON.

Verdicts:
- Lever A/B (Q01-only, 6 pairs, isolate): **−13.8 ms (−6.4%), clear
  WIN** (bar 5.7) — `ab-q01-boost-only/`.
- vs DuckDB same-session: **ematix 208.5 vs 241.6 ms — clear win
  (+15.9%, bar 3.7)** — `verdict-q1q5.md`.

## No-loss gates (all clean)

- Full-22q A/B (both levers off vs defaults): SF=10 / SF=1 / SF=100
  all **zero clear regressions** (`ab-newlevers-sf{10,1,100}/`).
- Throughput SF=10 × 10 streams: 28,908 QPH ≈ baseline (Gate-B
  correctly dormant under load) — `tput-sf10-s10-check/`.

## Q05 — still the one tie

- Narrow keys re-tested post-I64onI32/unification: **rejected again**
  (8 clear regressions, net +4.4%) — `ab-narrow-keys/`.
- Partition-count insensitive (flat at P∈{14,20,28,58}) → the tie
  lives in the join pipeline, not the scan.
- Current standing: ematix 127.4 vs DuckDB 130.8 (nominally ahead,
  inside the 4.7 ms bar). Needs a ~5 ms pipeline lever; fresh
  production-shape profile is the next step (the PERF_Q05 waste map
  predates the dim-semi splice).

## 2026-07-03 evening addendum — thermal-soak invalidates late-day solo verdicts

The Q05-lever battery's end-of-day full verdict pass measured BOTH
engines ~20-25% slower than morning (ematix Q01 258→277→307 ms drift
WITHIN the battery; DuckDB 242→295), with ematix's bandwidth-heavy
queries (Q01/Q08/Q09) losing disproportionately on the soaked machine
— producing 3 spurious "duckdb faster" verdicts that contradict the
morning's tight-bar wins. Interleaved A/Bs are immune (drift cancels
across arms) and remain authoritative for lever effects: the
STATVEC/MEMO battery shows zero regressions at all scales + Q20
−27 ms clear win. Engine-vs-engine verdicts require a SETTLED machine;
a 1h-cooldown re-run supersedes the soaked pass
(`q05-lever-2026-07-04/verdict-sf10-settled.md`).
