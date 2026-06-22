# SF=100 Loss Diagnostic — a repeatable method for "what are we doing wrong?"

**Status:** active (started 2026-06-21). **Owner:** perf campaign.
**Artifacts:** `scripts/diag/sf100_diagnose.sh` (driver), `scripts/diag/sf100_classify.py`
(decision tree), instrumented `tpch_preset_rebench` (`cpu_s`/`eff`/`pin_mb`/`[peak]`).

## Why this exists

SF=100 loss counts have whipsawed all campaign long — a single sweep says "6 losses",
a 3-invocation re-measure says "2". The reason is that **most SF=100 "losses" are not
engine gaps at all** — they are the 36 GB box: ematix uses DataFusion's *unbounded*
memory pool, so peak RSS runs ~2–3× DuckDB; once the working set + retained heap exceeds
RAM the page cache can't hold `lineitem` and every query re-reads its columns cold. That
penalty appears **only when all 22 queries share one process** (in-sweep) and **vanishes
when a query is measured isolated-warm**. Eyeballing wall-time ratios cannot tell that
apart from a real compute gap.

So the method's #1 job is to **separate measurement artifact from engine gap**, and only
then **classify the real gaps by mechanism** — using *measured* signals, never inferred
ones, and **profiling DuckDB directly** (campaign rule #1).

## The method: two stages

### Stage 0 — Is the loss real? (artifact vs engine)

A query is a **real** loss only if it is still slower than DuckDB when measured
**isolated-warm** (its own process, warmups priming the cache), across **≥3 invocations**
with directional consistency (the 3-invocation rule). Compare against the same query's
**in-sweep** wall:

| isolated-warm ratio | in-sweep / isolated | verdict |
|---|---|---|
| ≤ 1.05 | ≥ 1.25 | **BOX-ARTIFACT** — the 36 GB box, not the engine |
| ≤ 1.05 | ~1.0 | **NOT-A-LOSS** — parity |
| > 1.05 | any | **REAL** — go to Stage 1 |

### Stage 1 — What mechanism? (the signal vector)

For each REAL loss, capture six measured signals (all from the isolated runs, where they
are attributable — see "Why isolated" below):

| sym | signal | how measured | what it isolates |
|---|---|---|---|
| **W** | wall ratio ematix/DuckDB | median of pooled trials | loss magnitude |
| **R** | peak-RSS ratio ematix/DuckDB | `[peak] peak_rss_mb` (getrusage) per isolated run | memory demand |
| **E** | effective cores (ematix) | `eff = cpu_s / wall_s` (getrusage utime+stime) | parallel efficiency |
| **Cpu** | total-CPU ratio ematix/DuckDB | `cpu_s` per engine | do we do *more work*? |
| **P** | MB paged-in during a *warm* trial | `pin_mb` (vm_stat Pageins Δ × 16 KiB) | cache-pressure / cold reads |
| **box** | in-sweep / isolated wall | Stage-0 ratio | box vs engine |

### Stage 2 — Decision tree → root cause → lever

Applied by `sf100_classify.py` (thresholds in one place at the top of that file):

```
box ≥ 1.25 and W ≤ 1.05           → BOX-ARTIFACT        lever: demand-reduction OR publish isolated-warm
W ≤ 1.05                          → NOT-A-LOSS          lever: —
R ≥ 1.5 and P ≥ 1500 MB           → RSS / CACHE-BOUND   lever: streaming decode / bounded pool / spill
|Cpu−1| ≤ 0.15 and E < 9          → PARALLEL-EFFICIENCY lever: morsel / work-stealing region
Cpu > 1.15                        → COMPUTE-EXCESS      lever: samply split + duckdb plan-diff (below)
else                              → UNRESOLVED          lever: re-measure / inspect by hand
```

- **RSS/CACHE-BOUND**: we demand far more memory and are still reading from disk while
  "warm" → the known multi-month demand-reduction program (comprehensive spill + streaming
  decode; partial bounded-pool was measured all-or-nothing, see
  `project_sf100_demand_reduction`). Often the *same* root as a BOX-ARTIFACT, just severe
  enough to bite even isolated.
- **PARALLEL-EFFICIENCY**: identical total CPU to DuckDB but fewer cores busy → we leave
  cores idle. This is the morsel-engine thesis (`docs/plans/MORSEL_ENGINE.md`).
- **COMPUTE-EXCESS**: we burn *more* CPU than DuckDB → we are doing redundant work. This
  is the only class that needs the two semi-manual tools to localise *which* work:
  - **ematix self-time** — `profile_query` under samply:
    `TPCH_DATA_DIR=examples/tpch/data/sf100 TPCH_QUERY=NN TRIALS=10 samply record ./target/release/examples/profile_query`
    → is the excess in decode-decompress, the agg kernel, the join probe, or take/gather?
  - **plan-diff vs DuckDB** — `duckdb_profile_dump NN` gives DuckDB's per-operator
    cardinalities; compare against ematix's `EMAT_EXPLAIN=plan` / EXPLAIN ANALYZE. A much
    larger ematix intermediate ⇒ join-order/pushdown lever; a hot high-card agg ⇒ radix agg.

The classifier auto-decides BOX-ARTIFACT / NOT-A-LOSS / RSS-CACHE / PARALLEL-EFFICIENCY
from harness signals alone; for COMPUTE-EXCESS it points at the right next tool.

## Why isolated runs for RSS/CPU/pageins

DuckDB runs **in-process** here (the `duckdb` crate, same path users hit). `getrusage`
(RSS, CPU) and `vm_stat` (pageins) are process- and system-wide, so an interleaved run
mixes both engines. The driver therefore runs each engine **alone** (`SKIP_DUCKDB=1` /
`SKIP_EMATIX=1`); only then is peak RSS "this engine's peak" and `cpu_s` "this engine's
work". Wall time is fair either way, but the *mechanism* signals require isolation.

## Running it

```sh
cargo build --release -p ematix-flow-core --example tpch_preset_rebench --features triangulation
SCALE=sf100 QUERIES=8,9,10,3,5 INV=3 scripts/diag/sf100_diagnose.sh
# smoke-test the plumbing fast first:
SCALE=sf1 QUERIES=1,6,10 INV=2 TRIALS=2 WARMUPS=1 INSWEEP_TRIALS=1 scripts/diag/sf100_diagnose.sh
```

Output: a per-query verdict table + class summary (also saved to `$OUTDIR/verdict.txt`),
backed by the raw logs (`iso_ematix.log`, `iso_duckdb.log`, `insweep.log`).

## What's instrumented vs. gaps

- **Instrumented** (this method): wall, peak RSS (both engines, isolated), CPU/eff,
  pageins, in-sweep box-delta, DuckDB EXPLAIN ANALYZE (`duckdb_explain` already in harness).
- **Gaps / future**:
  - No **bounded memory pool** is wired in the engine today (the old `EMAT_MEM_POOL_MB`
    spike was never landed). So we cannot yet A/B "does bounding RSS fix the loss?" inside
    this harness — it would need re-implementing the FairSpillPool wire-up. Demand-reduction
    memory already concluded this is all-or-nothing, so it's low priority.
  - `pin_mb` is **system-wide** (vm_stat), so background processes add noise; run on a
    quiet box and trust it at pass scale, not per-trial.
  - COMPUTE-EXCESS localisation (decode/agg/probe split) is **not** auto-classified —
    samply + plan-diff are deliberately human-in-the-loop.

## Grounding (state at method start, 2026-06-21)

From this session's 3-round SF=100 medians, the candidate REAL losses to point the method
at first are **Q10** (~1.38×, the dominant gap), then **Q8** (~1.05×) and **Q9** (~1.07×),
with Q3/Q5 marginal. Memory predicts Q10 = RSS/CACHE or PARALLEL-EFFICIENCY (high-RSS wide
customer aggregate) and Q8 = COMPUTE-EXCESS (decode/probe of the 60M part⋈lineitem). The
method's job is to confirm or refute that with measured signals rather than carry the
prediction forward as fact.
