# Fused aggregate shape catalog (Σ.G.1 audit)

Read-only audit of the existing `FusedQN` operators, output of
Σ.G.1 per [[PHASE_SIGMA_G_GENERIC_FUSED_AGGREGATE]]. Decides
whether Σ.G.2-G.5 are worth doing and which shapes belong in the
same unified operator.

## File layout (smaller than expected)

The "5+ FusedQN operators" framing turns out to be 3 physical
operators serving 5 TPC-H queries:

| TPC-H query | Physical operator | File | Lines |
|---|---|---|---|
| Q6 | `FusedFilterSumExec` | `crates/ematix-flow-core/src/fused.rs` | 628 |
| Q1 | `FusedFilterMultiAggExec` | `crates/ematix-flow-core/src/fused_multi_agg.rs` | 823 |
| Q3, Q5, Q12 | `FusedPostJoinExec` (3 accumulate-fn variants) | `crates/ematix-flow-core/src/fused_post_join.rs` | 989 |
| (Σ.E3b) | `DictGroupCountExec` | `crates/ematix-flow-core/src/dict_aggregate.rs` | 602 |

Rules + shared walker live in:
- `crates/ematix-flow-core/src/fused_jit_rule.rs` (2288 lines) — all 5
  `InjectFusedQN` rules + `AggregateShapeConfig`
- `crates/ematix-flow-core/src/dict_aggregate_rule.rs` —
  `EnableDictGroupCountRule`

**This already tells us something:** Q3/Q5/Q12 are *already* sharing
a physical operator (`FusedPostJoinExec`) with per-query accumulate
functions. The post-join family has been generalised once.

## Hot-loop anatomy per shape

### Q6 (`FusedFilterSumExec`)

- **Inputs:** Float64 (qty, price, disc), Date32 (shipdate)
- **Aggregates:** `SUM(price * disc)` — single scalar
- **Shape:** single-table scan + range filter, no group-by
- **Filter:** `shipdate ∈ [lo, hi), disc ∈ [lo, hi], qty < hi` (5 scalar bounds)
- **Accumulator:** pre-allocated scalar `f64`, streamed per-partition
- **Hot loop:** 36 LOC (`process_q6_batch_hand`, fused.rs:325-360)
- **SIMD:** LLVM auto-vectorises

### Q1 (`FusedFilterMultiAggExec`)

- **Inputs:** Utf8View × 2 (returnflag, linestatus), Float64 × 5 (qty, price, disc, tax + sums), Date32 (shipdate)
- **Aggregates:** 5 SUM + 1 COUNT per group → 8 output columns post-divisions
- **Shape:** single-table scan + Date32 cutoff filter, fixed 5-group GROUP BY (returnflag, linestatus)
- **Filter:** single Date32 `shipdate <= cutoff`
- **Accumulator:** fixed `[Q1Aggs; 5]` — hardcoded group routing by `(returnflag, linestatus)` first-byte match
- **Hot loop:** 72 LOC (`process_q1_batch_hand`, fused_multi_agg.rs:426-497)

### Q3 (`FusedPostJoinExec::accumulate_q3_batch`)

- **Inputs:** post-join columns from `lineitem ⋈ orders ⋈ customer`
- **Aggregates:** `SUM(price * (1-disc))` grouped by `(orderkey, orderdate, shippriority)`
- **Shape:** post-join — operator sees the join output, no filter at this level
- **Accumulator:** `HashMap<(i64, Date32, i32), f64>` — dynamic cardinality, grown on first observation
- **Output:** sorted Vec by revenue desc, top-N
- **Hot loop:** 47 LOC (fused_post_join.rs:419-465)

### Q5 (`FusedPostJoinExec::accumulate_q5_batch`)

- **Inputs:** post-join (supplier/part/nation chain)
- **Aggregates:** `SUM(price * (1-disc))` grouped by nation name (Utf8View)
- **Shape:** post-join, no executor-level filter
- **Accumulator:** `HashMap<String, f64>` (clones strings), 25 nations
- **Hot loop:** 29 LOC (fused_post_join.rs:491-519)

### Q12 (`FusedPostJoinExec::accumulate_q12_batch`)

- **Inputs:** Utf8View × 2 (shipmode, orderpriority)
- **Aggregates:** 2 conditional `COUNT(CASE WHEN ... THEN 1 END)` per group
- **Shape:** post-join (`lineitem ⋈ orders`), `WHERE shipmode IN ('MAIL', 'SHIP')`
- **Accumulator:** fixed `[Q12Bin; 3]` — branchless dispatch on shipmode first-byte
- **Hot loop:** 29 LOC (fused_post_join.rs:539-567)

### `DictGroupCountExec` (Σ.E3b)

- **Inputs:** `Dictionary(UInt32, Utf8|Utf8View)` group key
- **Aggregates:** `COUNT(*)` grouped by dict value
- **Accumulator:** per-batch `code_to_slot` table built once from batch's dict (≤ dict.len() string hashes), then per-row code → slot → `counts[slot] += 1`
- **Hot loop:** 55 LOC (dict_aggregate.rs:280-334)
- **Distinct trade-off:** correctness across batches with differing dictionary orderings adds the per-batch slot-resolution step

## What's identical across the operators

**~35% of the execution hot-path is structural duplication:**

- **Array downcast boilerplate** — `batch.column(i).as_any().downcast_ref::<...>()` patterns. ~78 sites across the 5 operators.
- **Per-batch loop skeleton** — `.values()` / `.views()` extraction → `for i in 0..batch.num_rows() { ... }`. 5-10 LOC × 5 operators.
- **Partition streaming** — async-per-partition collect-and-merge. ~60 LOC duplicated near-verbatim across fused.rs, fused_multi_agg.rs, fused_post_join.rs.
- **Output batch construction** — column builders + `RecordBatch::try_new`. 15-30 LOC per operator.
- **JIT routing** — `try_new_*_jit()` constructors building specs. ~20 LOC per Q1/Q6/Q14.

## What's genuinely different

| Axis | Q6 | Q1 | Q3 | Q5 | Q12 |
|---|---|---|---|---|---|
| **Input** | scan | scan | join out | join out | join out |
| **Filter** | yes (range) | yes (date) | no | no | yes (IN) |
| **GROUP BY card** | 0 | 5 fixed | dynamic | ~25 dynamic | 2 fixed |
| **Group key type** | — | tuple bytes | composite tuple | Utf8 | first-byte |
| **Aggregates** | 1 SUM | 5 SUM + 1 COUNT | 1 SUM | 1 SUM | 2 cond-COUNT |
| **Accumulator** | scalar | fixed array | HashMap | HashMap | fixed array |
| **Output sort** | no | no | yes (desc) | yes (desc) | no |

**Two natural families emerge:**

1. **Single-table family** (Q1, Q6, Q12): fixed-cardinality groups, accumulator is a fixed array (or scalar for Q6), filter is pushed into the hot loop, no output sort.
2. **Post-join family** (Q3, Q5): dynamic-cardinality groups, accumulator is a HashMap, no executor-level filter, output sort required.

Q12 is a hybrid (post-join + fixed cardinality + conditional aggregates) but its accumulator pattern is closer to Q1's fixed-array than Q3's HashMap.

## `AggregateShapeConfig` walker — what it models, what it doesn't

**Models (structural plan topology):**
- Top-level Sort / Projection / Aggregate(Final|FinalPartitioned|PartialAgg) presence
- Group-by column count (0 for Q6, 1 for Q1, 2-3 for Q3/Q5/Q12)
- Aggregate-expression count
- CSE projection presence (e.g. `extprice * (1-discount)` rewrite)
- Output column-name capture

**Does NOT model (semantic / data shape):**
- Predicate structure or scalar bounds (left to per-rule `extract_qN_predicate` extractors)
- Group key types or cardinality
- Accumulator layout (scalar vs. fixed array vs. HashMap)
- Output sort order
- Dictionary encoding (DictGroupCountExec is outside the walker)
- Join type or multi-table input shape

The walker is a **plan-shape matcher**, not a semantic descriptor.
For Σ.G.2 it would need to grow a *data-shape* descriptor — the
fields the unified operator would consume at runtime.

## Σ.G.2 feasibility recommendation

**Single-table fold (Q1 + Q6 + Q12) is plausible — moderate effort:**

The Q1, Q6, Q12 hot-loop structure is identical at the skeleton
level (per-row walk + accumulator update + branchless group
dispatch). The differences are *types and counts*, not *logic*.
A `FusedAggregateExec<S: AggregateSpec>` with `S` capturing:

- Group cardinality + group-key dispatch fn
- Aggregate count + per-aggregate types + per-aggregate kernel
- Filter predicate (range / cutoff / IN-set)
- Output schema

…would absorb all three operators behind a single generic. ~2-week
effort, broken down:

- Days 1-2: Extend `AggregateShapeConfig` with a data-shape descriptor
- Days 3-4: Parametrise the hot-loop skeleton (generic column
  extraction + accumulator trait)
- Days 5-6: Concrete `AggregateSpec` impls for Q1, Q6, Q12
- Days 7-8: Update `InjectFusedQ{1,6,12}` rules to emit the unified
  operator; equivalence tests vs hand-coded oracle
- Days 9-10: TPC-H SF=1 regression gate (must stay within 5% of
  2026-05-17 BENCHMARKS-SF1.md baseline)

**Multi-table family (Q3 + Q5) should stay on `FusedPostJoinExec`
for now.** They already share infrastructure — three accumulate
functions in one operator. The natural Σ.G work there is to extend
the same accumulate-fn pattern to other post-join shapes (Q9 + Q14
when retired-from-deprecation), not to unify with Q1/Q6/Q12.

**Σ.G.4 (cost-driven dispatch) becomes tractable** once the
unified operator exists: the cost model only has to recognise
"single-table + fixed cardinality aggregate" as a family, rather
than the 3 individual Q1/Q6/Q12 patterns.

**`DictGroupCountExec` stays separate.** Its per-batch dict
resolution is a fundamentally different hot loop (per-batch table
build + per-row code lookup). Worth its own simplification work
(Φ.3 vectorised gather kernel), not unification with the FusedQN
family.

## Concrete next step

Open a tracking ticket for **Σ.G.2** (single-table fold):
- Scope: unify Q1 + Q6 + Q12 hot-paths behind `FusedAggregateExec<S>`
- Effort: ~2 weeks
- Gate: TPC-H SF=1 ematix-flow times within 5% of the
  2026-05-17 baseline (Q1: 78ms, Q6: 12ms, Q12: 23ms)
- Output: 5 operators of ~500 LOC each → 1 generic operator + 3
  specs of ~80 LOC each. Net reduction ~1500 LOC.
