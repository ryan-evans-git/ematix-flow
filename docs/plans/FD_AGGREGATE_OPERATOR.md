# FD-aware aggregate operator — Q10 SF=100 lever

**Status (2026-06-21):** kernel + operator built, TDD-green, shipped opt-in/inert.
FD-detection rule + SF=100 default-on validation = remaining, correctness-gated.

## The win (de-risked)

Q10's customer aggregate groups by 7 columns (`c_custkey` + 5 wide strings + `n_name`).
DataFusion encodes all 7 into a comparable row-format group key for every ~11.5M input
rows — measured **4.86 CPU-s** (`time_calculating_group_ids`, 98% of the aggregate) vs
DuckDB's **1.65 CPU-s** on the *identical* 7-col grouping (`duckdb_profile_dump 10`
confirmed DuckDB does NOT FD-reduce — it just has a ~3× faster multi-col group-by).

The 6 non-key columns are functionally determined by `c_custkey` (customer PK), so the
group identity is fully decided by the i64 key. Hashing **only** the i64 key and
carrying the payload by first-occurrence (gathered once via Arrow `interleave`) is, in a
properly-parallel kernel, **2.35× faster on wall** than DataFusion's 7-col group-by
(`q10_agg_kernel_bench`: B‖ 85 ms vs A 200 ms, eff 10.8, 3.76× less CPU, correct).

## Built (this PR, opt-in / inert)

- **`fd_aggregate.rs`** — `FdSumAccumulator`: hash i64 key, sum f64, carry payload by
  first-occurrence `(batch,row)`, gather via `interleave` at finalize. Null-key group +
  SQL `SUM`-of-all-null = NULL. 5 unit tests.
- **`fd_aggregate_exec.rs`** — `FdAggregateExec`: single-phase ExecutionPlan,
  `required_input_distribution = HashPartitioned([key])` (cheap i64 shuffle, disjoint
  keys per partition → no merge), output schema `[key, payload…, sum]` matching the
  `AggregateExec` it will replace, empty-partition safe. 2 tests vs stock `GROUP BY`.
- De-risk examples banked: `q10_fd_spike.rs`, `q10_agg_kernel_bench.rs`.

Nothing invokes the operator yet, so it is inert — zero production/regression risk
(precedent: kernels shipped ahead of their default-on rule, e.g. `EmatixHashJoinExec`).

## Remaining — Story 3 (FD-detection rule) — CORRECTNESS-CRITICAL

The operator groups by `key` ALONE; correct **only** when the payload is functionally
determined by `key`. A wrong FD assumption ⇒ **wrong query results**. So the swap rule
must fire **only when the FD is provable**, never on a heuristic.

Design options (in safety order):
1. **Provable via metadata (correct, preferred):** consult the aggregate input's
   functional dependencies (DataFusion `FunctionalDependencies`, derived from declared
   table PK/unique `Constraints` propagated through joins/projections). Fire only when
   `{key} → {payload}` is proven. **Open question:** whether FD info survives to the
   point a `PhysicalOptimizerRule` runs — if not, implement as a `LogicalOptimizerRule`
   /`AnalyzerRule` that tags the aggregate, with the physical swap reading the tag.
   **Enabler:** `EmatixFastParquetTableProvider` must declare the TPC-H PKs
   (`customer.c_custkey`, …) via `TableProvider::constraints()` so the FD propagates —
   a small, correct provider enhancement (the keys genuinely are unique).
2. **Opt-in contract (footgun):** `EMAT_FD_AGG=1` swaps on shape alone, trusting the
   caller that the non-key group cols are FD on the key. Sharper footgun than the other
   opt-in kernels (which are always-correct) — only acceptable default-off + documented.

Recommendation: do (1). It is the only path to a *default-on* Q10 win that can't return
wrong answers. Mirror `robin_hood_agg_rule.rs` for the shape match + `transform_up` swap
+ opt-in installer; add the FD-proof guard + the provider-constraints enabler.

## Remaining — Story 4 (gate / default-on decision)

- End-to-end Q10 SF=100 A/B with the operator firing (via the diagnostic harness,
  `scripts/diag/sf100_diagnose.sh`). **Re-confirm the microbench ratio survives the real
  2-phase / StringView / repartition agg** — the microbench's DataFusion arm (2.31
  CPU-s) is *half* the real agg (4.94), so the realized win may differ.
- `tpch_validate` 22/22 row-for-row (default-off ⇒ must be unchanged).
- 22q SF=10 + SF=100 strict interleaved A/B with the rule on — no regressions.
- Codegen-perturbation check: confirm adding the operator/rule code to flow-core didn't
  move the 22q geomean (the [[optimizer-codegen-sensitivity]] tax); if it did, move the
  kernel to a sibling crate.
- Decision: default-on iff Q10 flips AND 22q neutral AND correct; else banked opt-in.
