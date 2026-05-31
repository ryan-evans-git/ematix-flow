# REV.8 — Single-pass radix-partitioned aggregation (scope)

## Motivation (measured — REV.7, 2026-05-30)

Q18 SF=100: **ematix 4419 ms vs DuckDB 2116 ms (2.09×)**. Both EXPLAIN
ANALYZE plans show the gap is the `SUM(l_quantity) GROUP BY l_orderkey`
subquery (600M → 150M groups), and within it DataFusion's **two-phase**
shape:

```
AggregateExec(FinalPartitioned)   elapsed_compute 23.4s  (time_calculating_group_ids 22.85s)
  RepartitionExec(Hash[l_orderkey], 14)  send_time 13.5s, 2.2 GB, 150M rows
    AggregateExec(Partial)        elapsed_compute 18.3s  (reduction_factor 25%)
      EmatixFastParquetExec(lineitem, 600M)
```

`l_orderkey` is near-unique **and lineitem is sorted by it**, so each key is
fully aggregated inside one input partition: the Partial phase already emits
~final groups (25% reduction = the 4 lineitems/order, nothing more to
combine). We then **shuffle ~150M near-final groups and re-hash them in the
Final phase** — 22.85s of `time_calculating_group_ids` doing a hash build
that combines almost nothing. DuckDB does one radix-partitioned in-RAM
aggregation with no second hash pass.

(The *other* half of the REV.7 gap — i64 vs DuckDB's u32-compressed keys —
is a separate lever, not addressed here.)

## Goal

Replace `AggregateExec(Partial) → RepartitionExec(Hash) → AggregateExec(FinalPartitioned)`
with a single operator that radix-partitions during build and combines each
bin exactly once — no global re-hash of partial groups — for the high-card
`SUM(f64) GROUP BY i64` shape.

## THE GATE (step 1 — go/no-go before building the operator)

DataFusion's two-phase is **already** a radix-shuffle (RepartitionExec hashes
the key and routes to one of 14 partitions). The win therefore is NOT
self-evident: single-pass avoids the Final re-hash, but it must move raw rows
(or aggregated sub-tables) between threads instead of the 150M partial
groups. Whether that is net-faster is a **parallel-coordination** question
that a single-threaded microbench (REV.5.b) cannot answer.

**Gate:** a 14-thread microbench on the Q18 per-query shape (R rows, R/4
distinct contiguous-sorted i64 keys) comparing:
- `two_phase`: 14× local Partial hash-agg → hash-shuffle partial groups into
  14 buckets → 14× Final hash-agg.
- `single_pass`: 14× radix-scatter raw rows into B bins (local) → barrier →
  parallel per-bin aggregate-once.

**Pass criterion: `single_pass` must beat `two_phase` by ≥ 1.25×.** The
margin guards the integration tax that has erased every prior kernel win at
this scale (REV.5 radix, REV.5.b spill, Σ.N COUNT arc, Σ.R.2 AVG). Below
1.25× → do not build; the gap is key-width / probe-speed, pursue u32 keys
instead.

## Operator design (only if the gate passes)

`SinglePassRadixSumF64Exec`:
- `required_input_distribution = UnspecifiedDistribution` — consumes the N
  scan partitions directly; **no upstream RepartitionExec**.
- Phase 1 (parallel, per input partition `i`): radix-partition rows into
  `B = target_partitions` bins by `hash(key)` high bits; build B local
  `RobinHoodI64F64` sub-tables. Deposit the B sub-tables into shared state
  (`Vec<Mutex<Vec<RobinHoodI64F64>>>` indexed by bin, or a `[P][B]` grid).
- Barrier: all input partitions finish Phase 1 (CollectLeft-style shared
  build, like `HashJoinExec`).
- Phase 2 (parallel, output partition `p` emits bin `p`): gather the P
  sub-tables for bin `p`, merge-sum into one table, emit one RecordBatch.
- `output_partitioning = Hash([key], B)` so the downstream join sees correct
  distribution; `EmissionType::Final`, `Boundedness::Bounded`.

Correctness: bin = `hash(key)` ⇒ all rows for key K land in bin K across all
partitions ⇒ per-bin combine is complete (no key spans bins). SUM ignores
null values (match DataFusion). Output schema `[key:i64, sum:f64]`.

## Optimizer rule (only if the gate passes)

`EnableSinglePassRadixSumRule` (opt-in `EMAT_SINGLE_PASS_RADIX=1`): match an
`AggregateExec(Final*)` whose input chain is
`Partial-agg → (RepartitionExec(Hash)) → …` with a single `i64` gby + single
`SUM(f64)`, **high-card gate** (est. groups > ~1M, where the Final re-hash
dominates). Replace the whole Partial→Repartition→Final subtree with one
`SinglePassRadixSumF64Exec` over the Partial's input. Refuse on low card
(Partial reduces well → no win) or any shape departure.

## Risks

1. **Integration tax** — kernel wins have not translated (REV.5/5.b, Σ.N,
   Σ.R.2). The ≥1.25× gate margin is the guard.
2. **Memory** — holds all B sub-tables (= full 150M-group result) in RAM in
   Phase 2 (~1.5 GB/partial); same as DuckDB in-RAM. Spilling deferred
   (REV.5.b showed spilling loses).
3. **Codegen tax** — a new operator perturbs LLVM codegen (~5-8% geomean
   risk, see optimizer-codegen-sensitivity). Opt-in flag + high-card gate
   confine it to the Q18 shape.
4. **i64 keys** — single-pass still hashes i64; the u32 lever is orthogonal.

## Plan

1. **GATE** — 14-thread microbench, decide on ≥1.25×. ← this step.
2. If go: `SinglePassRadixSumF64Exec` (TDD: 1-partition + N-partition
   correctness vs DataFusion agg).
3. `EnableSinglePassRadixSumRule` (TDD) + high-card gate.
4. E2E: Q18 SF=100 A/B (target ≤ ~3000 ms) + 22q SF=10 regression + tpch_validate.
5. Decision: ship opt-in / default / reject.

## OUTCOME (2026-05-30) — built, correct, but E2E REGRESSES 0.62× → NOT shipped

Operator + rule built, 7 unit tests green (correctness vs stock DataFusion),
wired opt-in (`EMAT_SINGLE_PASS_RADIX=1`, exclusive with rh_sum). E2E Q18
SF=100 (3 trials): **ematix 7082 ms vs 4419 ms baseline = 0.62× — a 60%
REGRESSION** (6398 rows, correct). The gate's 1.71× did not survive
integration; it went negative.

**Why — the operator diverged from the gated algorithm:**
- The gate's `single_pass` scatters RAW rows into bins and aggregates each
  bin **once** in parallel, then just **counts** groups (≈R inserts, fully
  parallel, zero result materialization). It measured the *core algorithm*,
  ~760 ms-equiv for 600M.
- The OPERATOR, to stay memory-safe, does per-partition radix aggregation
  (`RobinHoodSumF64GlobalRadixAgg` with 128 MB mid-stream-drain budget) →
  **then a cross-partition per-bin merge** (an extra ~150M inserts: R+D
  total, same as the two-phase it replaced, not the gate's R) → then a
  **serial gather** of all 150M groups into two 1.2 GB Vecs + ~18K
  RecordBatches → emitted through a **single output partition**, so the
  downstream `FilterExec(sum>300)` runs serial over 150M rows.
- Net: the operator is ~7× slower than the gate's ideal. The integration
  artifacts — 1-partition serialization, serial 150M-group materialization,
  doubled (R+D) insert work, and REV.5.b's mid-stream-drain residency
  erosion — dominate and flip the win.

**Decision: REJECT for shipping; keep operator + rule as opt-in infra**
(tested, correct). Recovering the gate's win would need a from-scratch
faithful rebuild — B-output-partition shared-build (OnceFut across outputs,
HashJoinExec-CollectLeft style) + raw-scatter parallel combine + parallel
per-partition emit — which is major, carries ~9.6 GB raw-hold OOM risk at
SF=100, and still wouldn't address the *other* half of the REV.7 gap (u32
keys + decode). The banked win from this arc remains CollectLeft (Q18 SF=10
1.05×). REV.6 shape-realism lesson, confirmed one level deeper: a gate
microbench of the *core algorithm* (idealised, count-only) is not
operator-realistic — it omits emission, partitioning, and materialization,
which here cost more than the algorithm itself.
