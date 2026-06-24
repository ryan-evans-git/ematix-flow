# Q9 L9.DIMSEL lever — over-firing FIXED by a runtime discriminator (opt-in, SF=100 gate pending)

**Branch:** `feat/q9-l9-pushdown`. **Flag:** `EMAT_L9_DIM_BLOOM` (default OFF).
**Status:** correct + Q9/Q20 wins; the over-firing that blocked default-on is now
**resolved** by the L9.DIMSEL.RT discriminator (see the bottom section). Still opt-in
pending SF=100 validation. The two sections below are the ORIGINAL dig (over-firing
diagnosis); the **L9.DIMSEL.RT** section at the end is the built fix. Box was degraded
during the original measurement — trust the on/off RATIOS, not absolutes.

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

---

# L9.DIMSEL.RT — runtime build-selectivity discriminator (2026-06-24, commit b5c53dd)

**The discriminator the section above asked for is built.** It is **runtime-adaptive**
(candidate 1), not plan-time — because the SF=10 dig proved candidate 3 (NDV
selectivity) *cannot* work: the most-selective filter REGRESSED.

## Why plan-time selectivity (candidate 3) is dead

`estimate_filter_selectivity_via_emat_stats` gives `col = literal → 1/NDV` but
LIKE / range / col-col → the 0.2 constant. Measured filter types & the verdict:

| Q | bloomed join | filter | plan-time est | **real sel** | SF=10 raw verdict |
|---|---|---|---|---|---|
| Q9 | part→lineitem (l_partkey) | `p_name LIKE '%green%'` | 0.2 (LIKE) | **5.4%** | **−9% WIN** |
| Q20 | part→lineitem (l_partkey) | `p_name LIKE 'forest%'` | 0.2 (LIKE) | **1.1%** | **−11% WIN** |
| Q3 | customer→orders (o_custkey) | `c_mktsegment=` | 0.2 (1/5 NDV) | 20% | +40% LOSS |
| Q8 | orders→lineitem (l_orderkey) | `o_orderdate` range | 0.2 (range) | 30% | +43% LOSS |
| Q21 | orders→lineitem (l_orderkey) | `o_orderstatus=` | — | 49% | +18% LOSS |

Q8's `p_type=` part filter is the MOST selective (1/150 by NDV) yet Q8 regresses —
**bloom selectivity does not predict the win.** And Q9 (LIKE→0.2) and Q3 (1/5→0.2)
estimate identically but are 5.4% vs 20% real. No plan-time number separates them.

## The discriminator: measure the true selectivity at the build's PUBLISH time

`actual_build_keys / dim_total` is *exactly* the fraction of the fact the bloom lets
through, and it's known once the build drains. `EMAT_L9_DIM_BLOOM_RT_SEL` (default
**0.10**) is the keep/disarm cut — it sits in a wide gap (1–5% keep vs 20–49% disarm).
Traced SF=10 blast radius = exactly 5 queries: Q9 0.054 KEEP, Q20 0.011 KEEP,
Q3 0.200 DISARM, Q21 0.487 DISARM, Q8 dedup-SKIP.

## …but a disarm alone is necessary-NOT-sufficient (rule #1: profile, don't infer)

Disarming the bloom (publishing empty) left Q3/Q8 **still +40%**. The cost is not the
bloom payload — it's the wrap *attachment*, which the trace pinned to two distinct,
**generally-correct** bugs (neither is the bloom):

1. **Q8 = displacement.** A scan holds ONE sideband. The orders→lineitem DIMSEL fire
   OVERWROTE the beneficial part→lineitem bloom on the shared lineitem scan, then
   disarmed itself → lineitem fully unfiltered. → **DIMSEL dedup**: never attach to an
   already-wrapped scan.
2. **Q3 = a cascade.** `BuildSideBloomEmitterExec` returned `Absent` partition_statistics
   (latent bug — it's a pass-through). Wrapping customer made the parent join's
   `estimate_build_rows` come back `None`, and the L9 ratio gate (None == not-rejected)
   admitted a net-negative SECONDARY `(customer⋈orders)→lineitem` bloom absent from the
   un-wrapped plan. → **partition_statistics delegates to input** (general correctness
   fix; the only unconditional change — validated to leave the DIM_BLOOM=0 baseline
   fire-set byte-identical).

Plus **DIMSEL no-late-arm**: an eager-polled dim→fact probe that races past the build
stays on the inline reader instead of routing eager to await a maybe-empty publish.

## Result (SF=10, controlled A/B on the 5-query blast radius)

Q9 **−13%**, Q20 **−15%** (wins preserved); Q3 / Q8 / Q21 **neutral** (within ±5%
noise, down from +40 / +43 / +18%). Other 17 queries don't fire DIMSEL → untouched.
`tpch` row counts match DuckDB; lib suite 1238/0.

## Still opt-in — what default-on now waits on

The blockers from the section above are **resolved structurally** (not by tuning), and
the discriminator is scale-robust by construction (a selectivity ratio, not a row
count). The ONE remaining gate is **SF=100 validation**: the box has no SF=100 data
right now, and SF=10↔SF=100 behavior is known to diverge for this lever (Q20 *regressed*
1.10× at SF=100 in the original dig but *wins* at SF=10). Flipping default-on without
re-measuring the 5-query blast radius at SF=100 would violate the project's bench
methodology. Path to default-on: re-provision SF=100 → order-balanced interleaved A/B
on Q3/Q8/Q9/Q20/Q21 → if clean, flip `dim_bloom_enabled()` to opt-out.
