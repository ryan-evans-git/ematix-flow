# Ψ — Column statistics + cost-based optimisation

**Goal:** ematix-flow's planner makes shape decisions based on data
characteristics, not just hand-coded rules. Enables Σ.G.4's
cost-driven dispatch + better join ordering on arbitrary user
queries.

**Status:** stub. Two-stage plan:
- **Ψ.1** — column statistics + cardinality estimator (~4 wk)
- **Ψ.2** — full CBO (~3-6 months, separate decision point)

Will be promoted to a full plan after Σ.G + Φ.3 have landed (Σ.G.4
needs Ψ.1 to be useful).

## Why this is two stages

**Ψ.1 (stats + estimator) is cheap.** Histograms, NDV (number of
distinct values), min/max, null fraction — all derived from a single
column-stats scan per parquet file (which we can do at load time,
or piggy-back on existing decode passes). Cardinality estimator is
~500 lines of join-predicate math. Plenty of literature (Selinger '79
through Microsoft's "How good are query optimizers" 2015).

**Ψ.2 (full CBO) is expensive.** Plan-space search (DP, Cascades,
or learned), cost model per operator, IBM-style join-graph
exploration. Years of work end-to-end. Calcite is the reference for
"took a community 10+ years to mature."

We do Ψ.1 first, decide whether Ψ.2 is worth committing to based on
what Ψ.1 unlocks.

## What Ψ.1 covers

| Layer | What | Effort |
|---|---|---|
| Column stats collection | At parquet open time, extract from existing parquet metadata (min/max, null count) + sample NDV from dict page if dict-encoded | ~1 wk |
| Stats persistence | Cache in a sidecar `.stats.json` or memoize per `TableProvider` instance | ~3 days |
| Histogram (optional) | Equi-height histogram per column, sampled from the first row group. Defer if Σ.G.4 works with just min/max + NDV | ~1 wk |
| Cardinality estimator | Standard selectivity rules: equi-predicate = 1/NDV, range = (max-val) / (max-min), join = leftrows × rightrows × selectivity_of_join_predicate | ~1 wk |
| Plug into Σ.G.4 | Use estimator output to gate the "fused path or default path" decision | ~3 days |

**Total Ψ.1: ~4 wk.**

## What Ψ.2 covers (deferred)

| Layer | What | Why expensive |
|---|---|---|
| Join graph enumeration | DP-based bushy join ordering on N tables | O(3^N) plans — Q5/Q8/Q9 have 6+ tables |
| Plan cost model | Per-operator cost: scan cost ≈ rows × col-bytes; hash cost ≈ rows × log(unique); etc. | Calibration matters — wrong cost model picks slow plans |
| Search strategy | Cascades-style group enumeration with memoization | Code complexity |
| Adaptive Query Execution | Re-plan mid-execution when actual cardinality diverges from estimate | Spark AQE inspiration |

Honest estimate: **3-6 months of focused work** by someone who's
done it before. 12 months by someone who hasn't.

## Non-goals (for now)

- **Multi-column correlation statistics** — captures whether
  `l_orderkey` and `o_orderkey` follow the FK relationship's expected
  distribution. Big help on TPC-H Q5/Q8/Q9 but hard to estimate
  cheaply. Defer to Ψ.3 if Ψ.2 ships.
- **ML-based cardinality** — neural cardinality estimators
  (NeuroCard, MSCN) — research-grade, not production-ready.
- **Learned plan ordering** — Microsoft's NEO, Cardinality
  Estimation Benchmark — research-grade.

## Gates

**Ψ.1 gate:**
- Σ.G.4's cost-driven dispatch picks the fused path on ≥90% of TPC-H
  queries where it would help (= the queries our hand-coded rules
  cover today)
- Σ.G.4 picks the default path on ≥90% of queries where the fused
  path would lose (= negative tests we'd need to write)
- TPC-H SF=1 + SF=10 numbers don't regress

**Ψ.2 gate:** to be decided based on Ψ.1's outcome.

## Sequencing

- **Prerequisites:** Σ.G (to have a planner rule that can consume
  stats); ideally Φ.1+Φ.3 too so the fused path is broader.
- **Why we defer:** the immediate competitor (DuckDB) has good stats
  but ematix-flow can match its TPC-H numbers on stuff DuckDB cares
  about *without* stats. Stats earn their place when we go after
  workloads outside TPC-H.

## Honest open questions

- Is full CBO worth it given how much TPC-H success we're getting
  from rule-based? Spark has a CBO and people still complain about
  bad plans. DuckDB's planner is simple by design and competitive.
- Could "Ψ.1 + Σ.G.4 + Φ" be the entire optimiser story, and we
  skip Ψ.2 forever? Plausible. Decide after Ψ.1.
