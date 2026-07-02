# Strict benchmark protocol (Σ.AI)

**These scripts are the SOLE source of truth for win/loss claims** —
per-query latency verdicts vs DuckDB/Polars and throughput (QPH)
comparisons. Any number produced another way (single invocations of
`tpch_triangulation_bench`, the AWS campaign's in-process sweeps, ad-hoc
`cargo run` timings) is smoke-grade: useful while iterating, never
quotable.

## Why this exists

The 2026-05 campaign mixed two protocols with incompatible bias
profiles and the standings became undecidable:

- **In-process sweeps** run all 22 queries × 3 engines sequentially in
  one process: ematix reuses its `SessionContext` (and plan cache when
  enabled) across trials while DuckDB/Polars re-parse with fresh
  connections per trial; engine order is blocked (competitors always run
  on a warmer box); nothing records thermal state. Observed CV: 5–34%.
- **Strict runs** isolate invocations, interleave A/B, discard
  cold-starts, and take median-of-medians. Observed CV: 1.3–2%.

On top of that, results were compared across two different machines
(M3 Pro → M4 Max) with no metadata recording which was which. Every
strict run now writes `env.json` and embeds it in the summary header.

## Scripts

| Script | Purpose |
|---|---|
| `strict_22q.sh` | Single-mode latency characterization or triangulated verdict run. `--sf 1\|10\|100`, `--triangulate` (cross-engine), `--isolate` (process per query), `--plan-cache on\|off` (default off), `--cache-policy warm\|cold`. |
| `strict_ab.sh` | Interleaved A/B (A1 B1 A2 B2 …) for lever validation. Same knobs as above plus `--env-a/--env-b`. |
| `strict_throughput.sh` | Concurrent-stream throughput (TPC-H throughput style): N ∈ {1,10,100} simultaneous streams per engine, each a seeded permutation of the 22 queries. Reports makespan, QPH, per-query p50/p95/p99. |
| `strict_summarize.py` | Median-of-medians aggregation + metadata header (`--env-json`). |
| `strict_diff.py` | Per-query A/B verdicts with a 2× max(σ_A, σ_B) noise bar. |
| `strict_throughput_summarize.py` | Throughput aggregation (discard-first-batch, column-aware engine parsing). |
| `strict_common.sh` | Shared discipline: `thermal_wait`, `capture_env`, `apply_cache_policy`. |

## Protocol rules

1. **Thermal gating.** No invocation starts while `pmset -g therm`
   reports `CPU_Speed_Limit < 100`. Bounded wait (120 s default), then
   proceed with a recorded WARNING. Cooldowns are adaptive.
2. **Plan-cache fairness.** `EMAT_PLAN_CACHE` is set explicitly on every
   run (default **off**). In-process plan reuse benefits only ematix, so
   triangulated verdict runs must keep it off. Lever A/B may enable it
   symmetrically.
3. **Page-cache policy.** `warm` (default): first invocation discarded,
   results labeled warm-cache. `cold`: `/usr/bin/purge` before each
   invocation (needs passwordless sudo for purge). At SF=100 (~34 GB
   data vs 36 GB RAM) the policy dominates the noise floor — always
   state it.
4. **Verdict bar.** A win/loss claim requires the strict A/B diff to
   clear 2× max(σ_A, σ_B); anything inside the bar is "noise" and must
   be reported as such.
5. **Machine attribution.** The summary header (from `env.json`) states
   chip, cores, RAM, macOS, power source, git SHA, engine versions and
   EMAT flags. Numbers without this header don't get pasted into docs.
6. **Idle machine.** Nothing else heavy may run during a strict sweep —
   no builds, no other benches. If in doubt, rerun.

## Canonical campaign runbook

```bash
B="cargo build --release --example tpch_triangulation_bench --features triangulation"
$B

# 1. Latency rebaseline (all levers at defaults), per SF.
#    SOLO passes per engine — engines never share a process (RAM/thermal
#    isolation; the 2026-06-21 provenance protocol). Diff the summaries.
for sf in 1 10 100; do
  t=10; [[ $sf == 100 ]] && t=5
  scripts/bench/strict_22q.sh --sf $sf --engine ematix --isolate --trials $t --out bench-results/strict-sf$sf-ematix
  scripts/bench/strict_22q.sh --sf $sf --engine duckdb --isolate --trials $t --out bench-results/strict-sf$sf-duckdb
  python3 scripts/bench/strict_diff.py \
      --a bench-results/strict-sf$sf-ematix/strict-22q-summary.md \
      --b bench-results/strict-sf$sf-duckdb/strict-22q-summary.md \
      --label-a ematix --label-b duckdb \
      --out bench-results/strict-sf$sf-verdicts.md
done

# 2. Per-lever A/B (repeat per lever flag, ematix-only).
scripts/bench/strict_ab.sh --sf 10 --env-b "EMAT_<LEVER>=1" --out /tmp/ab-<lever>-sf10

# 3. Throughput at 1/10/100 streams.
scripts/bench/strict_throughput.sh --sf 10 --streams "1,10,100" --out bench-results/tput-sf10
scripts/bench/strict_throughput.sh --sf 100 --streams "1,10" --out bench-results/tput-sf100  # RAM guard
```

Rough wall-clock (M4 Max): SF1 rebaseline ~25 min; SF10 ~1.5 h; SF100
(5 trials, isolated) ~4–6 h; throughput SF10 ~1–2 h. Run overnight or
on a dedicated box; the thermal gate stretches these when the room is
warm.

## Tests

```bash
python3 -m pytest scripts/bench/tests/ -q
```
