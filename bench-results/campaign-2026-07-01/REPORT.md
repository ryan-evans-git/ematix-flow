# Campaign report — 2026-07-01/02 (integration/campaign-2026-07-01)

> **2026-07-02 ADDENDUM — superseded SF=100 verdicts.** After this
> campaign, two follow-up arcs landed on main: (1) the Q18 dig found the
> bench chain was missing production-default `ClusteredSinglePhaseAggRule`
> (RANGE.AGG) — §1's SF=100 table under-states ematix on RANGE.AGG-eligible
> shapes; (2) scale-gated auto defaults (row-count tri-state gating) plus
> the `Bound::I64onI32` bloom-binding fix that turned the Q08 ALL-ON
> hazard into a win. The corrected, quotable SF=100 standing lives in
> `bench-results/final-sweep-2026-07-02/`: **auto defaults vs forced-off =
> 16 clear wins / 1 tiny regression (Q15 +19.5 ms), net −8.3%**; **vs
> DuckDB = 16 wins / 4 losses (Q01 −214, Q05 −331, Q16 −16, Q18 −283 ms)
> / 2 noise**, ematix net +10.4% on sum-of-medians. SF=10 auto-vs-off is
> noise-identical (gate correctly dormant). EMAT_L9_PARTITIONED re-checked
> post-fix: still net-negative on Q08, stays opt-in. Q18's remaining
> −283 ms has a designed arc (RANGE.AGG Stage 2, docs/PERF_Q18.md).

**Machine:** Apple M4 Max (10P+4E, 36 GB), macOS 26.5.1, AC power.
**Protocol:** strict harness only (`scripts/bench/README.md`) — solo-engine
passes, per-query process isolation, thermal gating, plan cache OFF,
warm-cache with discard-first, 2σ verdict bars, env.json provenance in
every summary. Value-validation gate passed pre-campaign (22/22 vs
DuckDB, flags off AND all new levers on).

**Binary:** commit `75c9ba3` = main (`1724102`) + harness hardening +
Σ.AH.4 data re-emit + five new opt-in levers (all default OFF):
`EMAT_L9_PARTITIONED`, `EMAT_FD_GROUPBY`, `EMAT_NARROW_KEY_DECODE`
(+`EMAT_DOWNCAST_KEYS`), `EMAT_DATE_BUILD_SIDE`, plus pre-existing
opt-in `EMAT_NDV_BUILD_SIDE`.

---

## 1. Single-stream latency (production defaults, all levers off)

| Scale | Clear ematix wins | Clear DuckDB wins | Noise | Net Σ medians |
|---|---|---|---|---|
| SF=1 | **22/22** | 0 | 0 | ematix 297 ms vs DuckDB 769 ms (2.59×) |
| SF=10 | **21/22** | 1 (Q05 −17 ms) | 0 | ematix 2308 ms vs DuckDB 3186 ms (1.38×) |
| SF=100 | **13/22** | 7 (Q01 −151, Q03 −27, Q05 −420, Q09 −114, Q10 −860, Q16 −15, Q18 −1510 ms) | 2 | ematix 35.3 s vs DuckDB 34.8 s (0.99×) |

Movement vs the 2026-06-21 tables: Q08 SF100 flipped to a clear ematix
WIN (+288 ms — consistent with the Σ.AH.4 supplier.parquet 1→14
row-group re-emit; supplier feeds Q08's chain). Q18 SF100 is now the
dominant loss (−1.5 s). Q05 regressed to a small clear loss at SF10
(−17 ms) and a large one at SF100 (−420 ms).

## 2. Lever A/Bs (strict interleaved, 2σ bars)

### SF=10 (full 22q per arm)

| Lever | Net Δ | Clear wins | Clear regressions | Verdict |
|---|---|---|---|---|
| L9_PARTITIONED | +0.8% | — | — | plan-inert at SF10, as designed |
| FD_GROUPBY | +0.3% | — | — | neutral |
| NARROW keys | **+10.0%** | Q05 −44, Q09 −36 | 11 queries (Q08 +124, Q20 +84, Q18 +44…) | **must be scale/shape-gated** |
| DATE_BUILD_SIDE | −0.3% | — | — | neutral (preset path routes around it) |
| NDV_BUILD_SIDE | +0.2% | — | — | neutral |
| ALL-ON | +10.0% | — | — | = narrow-keys regression; others compose neutrally |

### SF=100 (targeted query sets, 5 trials)

| Lever | Queries | Clear wins | Clear regressions |
|---|---|---|---|
| L9_PARTITIONED | 5,7,8,9 | **Q09 −138 ms** | Q08 +62 ms |
| NARROW keys | 9,10 | **Q09 −1075 ms** | none |
| FD_GROUPBY | 10,13 | **Q10 −99 ms** | none |
| DATE+NDV swaps | 8,10 | **Q10 −947 ms** | none |
| ALL-ON | 3,5,8,9,10,16,18,21 | Q03 −53, Q05 −72, **Q09 −1023**, **Q10 −1109**, Q21 −150 | **Q08 +1461 ms** |

**Both consistent SF100 losses now have flipping levers**: Q09's −114 ms
gap flips via narrow keys (−1075 ms, the DRAM-spill build going
cache-resident); Q10's −860 ms gap flips via the build-side swaps
(−947 ms — root cause: DataFusion 53 interval analysis has no Date32
support, so date-range filters got a flat 20% selectivity and inverted
build sides) plus FD group-by (−99 ms).

**Composition hazard:** ALL-ON destroys Q08 (+1461 ms; worse than any
solo arm — narrow-keys' Int32 key advertisement × L9 tax compound).
Levers must ship gated, not blanket-on.

## 3. Throughput — concurrent streams ("tph"), production defaults

Streams are seeded 22-query permutations; engines measured solo;
inflight cap 10 (SF10) / 3 (SF100); first batch discarded.

| Config | ematix QPH | DuckDB QPH | ratio |
|---|---|---|---|
| SF10, 1 stream | **26,739** | 21,770 | ematix 1.23× |
| SF10, 10 streams | 8,645 | **30,135** | duckdb 3.49× |
| SF10, 100 streams | 11,290 | **29,444** | duckdb 2.61× |
| SF100, 1 stream | **2,212** | 1,990 | ematix 1.11× |
| SF100, 10 streams | 1,376 | 1,339 | parity (high variance) |

**Root cause found and confirmed:** ematix defaults
`target_partitions = available_parallelism()` **per process**, so 10
concurrent streams = ~140 runnable threads on 14 cores. Diagnostic
re-run of SF10 s10 with `PARTITIONS=2` per stream
(`tput-sf10-p2-diag/`): makespan 91.6 s → **29.1 s**, QPH 8,645 →
**27,232** — from 3.5× behind DuckDB to within 10%, no code changes.
DuckDB's morsel scheduler degrades gracefully; ematix needs
concurrency-aware partitioning (or documented deployment guidance) to
close the remaining ~10%.

Operational note: an uncapped 100-process launch OOM-force-restarted
this 36 GB machine twice; the harness now enforces `--max-inflight` +
a pre-batch free-memory gate.

## 4. Flag-default recommendations

- **All five new levers stay default-OFF.** At SF10 the production
  preset is already optimal (every lever neutral or harmful there).
- **Path to default-on** (each needs the gate + a full-22q strict A/B):
  - NARROW keys: scale-gate to SF≥100 shapes (the PR #159 pattern) AND
    exclude Q08's shape (or fix the composition with L9) — the Q09 win
    is the single largest lever measured in this campaign.
  - DATE_BUILD_SIDE (+NDV): scale-gate to SF≥100; alternatively fix
    upstream (Date32 in DataFusion interval analysis) which would make
    the stock join_selection do the right thing everywhere.
  - FD_GROUPBY: safe-looking (no regressions anywhere) but only −99 ms;
    gate on the Q10 shape and re-A/B after the swaps land.
  - L9_PARTITIONED: net-negative outside Q09; keep opt-in until the
    Q08 tax is understood.

## 5. What still stands between us and all-22 at every SF × concurrency

1. **Q18 SF100 (−1.5 s)** — the dominant remaining loss; no current
   lever touches it. Needs its own dig (LeftSemi shape at SF100).
2. **Q05** — small clear loss at SF10 (−17 ms) and larger at SF100
   (−420 ms; ALL-ON recovers only −72 ms). The 6-way join plan shape.
3. **Q01 SF100 (−151 ms)** and **Q16 SF100 (−15 ms)** — untargeted.
4. **Concurrency scheduler work** — concurrency-aware target_partitions
   (or admission control) to make the PARTITIONS=2 result the default
   behavior, then close the residual ~10% vs DuckDB at SF10 s10+.
5. Land the lever gating (item 4 above) so the Q09/Q10 flips apply
   under production defaults.

## Raw outputs

`latency-sf{1,10,100}/` (solo passes + verdicts.md), `ab-sf10-*/`,
`ab-sf100-*/` (diff.md each), `tput-sf10/`, `tput-sf100/`,
`tput-sf10-p2-diag/` (PARTITIONS=2 diagnostic), `validate-flags-*.txt`,
`campaign.log`, per-run `env.json`.
