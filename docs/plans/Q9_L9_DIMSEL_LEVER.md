# Q9 L9.DIMSEL lever — built, proven for Q9, but over-fires (opt-in only)

**Branch:** `feat/q9-l9-pushdown`. **Flag:** `EMAT_L9_DIM_BLOOM` (default OFF).
**Status:** correct + a big Q9 win; NOT default-on-safe (over-fires). Box was
degraded during measurement — trust the on/off and on/DuckDB RATIOS, not absolutes.

## What it does

L9's ratio gate (`build × 1024 ≥ probe`) rejected Q9's `part(LIKE %green%)⋈lineitem`
because the filtered part build estimates ~4M keys. But the bloom on `l_partkey`
drops ~80–95% of the 600M lineitem probe regardless of build size — the build/probe
proxy is wrong for a filtered dimension. The fix adds a rescue: admit when the build
is a single filtered dim scan whose est. filter-selectivity ≤ `EMAT_L9_DIM_BLOOM_SEL`
(default 0.5), reusing the tight path's pre-gates (probe ≥ 8M, ≥3 cols, fused-eval).

## Q9 result (the win)

| arm | SF100 ms | vs DuckDB |
|---|---|---|
| OFF (baseline) | 8912 | 1.31× (loss) |
| **ON (bloom)** | **5016** | **0.74× (WIN)** |

**−43.7%**; Q9 flips from the clearest remaining loss to a 26% win. Correct:
`tpch_validate` SF10 `L15 + EMAT_L9_DIM_BLOOM=1` = **22/22 cell-match vs DuckDB**
(sound membership pre-filter — no false negatives).

## Why it's NOT default-on-safe (the blocker)

Blast radius = 5 queries (Q03/Q08/Q09/Q20/Q21). SF100 on/off:

| Q | on/off | verdict |
|---|---|---|
| Q09 | 0.54× | WIN |
| Q21 | 1.03× | neutral |
| Q08 | 1.07× | REGRESS |
| Q20 | 1.10× | REGRESS |
| Q03 | 1.16× | REGRESS |

**Every one of the 5 estimates `filter_sel = 0.200`** — DataFusion returns a generic
constant selectivity for LIKE / equality / range filters alike, so the plan-time
selectivity gate **cannot discriminate** Q9's win from the others' regressions. The
tight path's Guard-2 runtime disarm does not neutralize them either. (Q08 fires the
admit on 2 joins, one net-negative.) This is the recurring L9 lesson: bloom payoff is
not predictable at plan time.

## Disposition

Shipped **opt-in** (banked, correct, byte-identical when off). Default-on (or a clean
broad opt-in) is blocked on a discriminator that plan-time selectivity can't provide.
Candidate follow-ons: (1) a runtime-adaptive disarm tuned to drop net-negative fires;
(2) a structural gate (e.g. downstream-join depth — Q9's bloomed join is the deepest);
(3) true-NDV-based selectivity instead of the 0.200 constant. Until then it stays a
Q9-targeted opt-in.
