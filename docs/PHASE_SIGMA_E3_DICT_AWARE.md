# Σ.E3 — dict-aware execution across operator boundaries

**Status:** scoped 2026-05-15, not started. Sits in the Σ.E perf-engine
track (Σ.E1 mimalloc + Σ.E2 FastParquet scan provider already shipped).

**One-line goal:** carry parquet dictionary encoding through filter /
hash-join / group-by / hash-aggregate, only materializing the string
payload at output or when a non-dict-aware operator demands it.

**Why now:** the Photon paper (Behm et al., VLDB 2022 §4.2) cites dict-
through-pipeline as one of its top three string-workload levers; FB
Velox's "Flat-encoded vectors with dictionary indirections" works the
same way. DataFusion's default path materializes the dictionary at
the scan boundary — every downstream operator sees a `Utf8` /
`Utf8View` column and pays the cost of full-string compare / hash /
copy on every row. The savings on a low-cardinality column (think
TPC-H `l_shipmode`, `l_returnflag`, `p_brand`, `n_name`) are
proportional to (avg_string_len / sizeof(dict_code)) — usually 4-16×
on real workloads.

## Where we are today

* `Utf8View` promotion (Σ.E2 follow-on, commit on `feat/fast-parquet-
  utf8view`) closed Q01/Q19 SF=10 losses by replacing `Utf8` with the
  inline-+-view representation. Memory record:
  `project_sigma_e2_fastparquet_remaining_losses.md`.
* FastParquet emits `DictionaryArray<UInt32, Utf8View>` only when the
  parquet dictionary survives intact (single dict page, dict-encoded
  column, no fallback). Most other paths flatten on read.
* DataFusion's `HashAggregateExec`, `HashJoinExec`, and `FilterExec`
  all treat `DictionaryArray` opaquely — they cast through
  `as_string()` and compare/hash the values, throwing away the
  dictionary's identity.

Net: today we have dict-encoded *arrays* at the scan but not dict-
encoded *execution*. Σ.E3 closes that gap.

## Scope — in / out

**In scope (Σ.E3):**
* `FilterExec` on dict columns — preserve dict codes when the predicate
  is `col = lit` / `col IN (lits...)` / `col LIKE 'prefix%'`; evaluate
  by translating each literal to its code (or set of codes) and
  bitmap-filtering on the indices array.
* `HashAggregateExec` group-by on a single dict column — group by
  dict code, no rehash, no string materialization until output.
* `HashJoinExec` build/probe on a dict column — if both sides share a
  dictionary (the common case after a dimension scan), join on codes;
  if not, build a code-translation table once.
* `ProjectionExec` passthrough — projection of a dict column to itself
  preserves the dict.

**Out of scope (deferred to Σ.E4 or later):**
* SortMergeJoin on dict columns.
* Sort on dict columns (preserves order, but order over codes ≠ order
  over values — needs a sort-aware dict representation).
* Multi-column group-by where the dictionaries don't trivially merge
  (a "dict-cross" representation — possible but heavy).
* Window functions on dict partition keys.
* Spilling dict-encoded hash tables to disk (single-pass only for now).

**Won't do:**
* Build a competing engine. The work lives as DataFusion
  `PhysicalOptimizer` rules + custom `ExecutionPlan` variants
  registered under our existing extension hooks, not as a fork.

## Concrete shape

Three landable subphases, each shippable on its own.

### Σ.E3a — `DictFilterExec`

**Insert point.** A `PhysicalOptimizerRule` runs after DataFusion's
default rules; it walks `FilterExec` nodes whose input schema contains
at least one dictionary column referenced by the predicate, and
rewrites to `DictFilterExec` when the predicate is one of:

* `dict_col = string_literal`
* `dict_col IN (string_literal, ...)`
* `dict_col LIKE 'prefix%'` (constant prefix only)
* AND-of-the-above; OR-of-the-above on the *same* dict column.

Any other shape falls back to the default `FilterExec` (the rule is
strictly speculative).

**Execution.** On first batch of each partition:
1. Resolve every literal to its dict code via the input's dictionary
   (linear scan over `dict.len()` is fine — dicts are ≤ thousands of
   entries on the columns this matters for). For `LIKE 'prefix%'`,
   build a `RoaringBitmap` of codes whose value starts with the
   prefix; this becomes the membership test.
2. Per batch: build the predicate mask by indexing the membership
   structure with the indices array. No string compares.
3. Apply the mask to all batch columns (existing
   `arrow::compute::filter` does this cheaply for `DictionaryArray`
   too — kept dict-encoded on output).

**Bench target.** TPC-H Q12 — `l_shipmode IN ('MAIL', 'SHIP')` is a
2-code IN-list against a 7-entry dictionary; the filter today scans
the full inline-string column. Expect ≥1.5× on the filter step at
SF=1, ≥2× at SF=10 (dictionary fits L1; flat-string column doesn't).

### Σ.E3b — `DictHashAggregateExec` (single key)

**Insert point.** Same optimizer rule, matches `HashAggregateExec`
where `group_expr.len() == 1` and the group column is a dictionary.

**Execution.** Hash table is keyed by `(code: u32)` rather than by
`hash(string_bytes)`. The group lookup is a direct `Vec<Option<usize>>`
indexed by code (codes are dense small integers by construction). Agg
state lives in column-orientated `Vec`s parallel to the slot table.
Materialization on `output()` resolves each surviving group's code
back to its string only once.

**Bench target.** Q01 — group-by `l_returnflag, l_linestatus`. Two
columns, but each is a one-or-two-character string with a 4-element
dictionary. As a smoke test we start with one-key (e.g. `l_shipmode`-
only aggregate); a Σ.E3c can extend to multi-key by packing dict codes
into a `u64` group key.

Expect ≥3× on the hash-aggregate step at SF=10 vs the default path
(Photon paper cites 4-8× on equivalent shapes).

### Σ.E3c — `DictHashJoinExec` (probe side dict-encoded)

**Insert point.** Same optimizer rule, matches `HashJoinExec` where
the probe-side join key column is a dictionary.

**Execution.** During build phase, if both sides share a parquet
dictionary id (we can detect this from FastParquet metadata when both
sides come from the same dataset — rare in practice for typical TPC-H
joins but common for self-joins / star-schema fact-fact joins), probe
on codes directly. Otherwise, build a `Vec<Option<u64>>` translation
table keyed by probe-side dict code that maps to build-side hash
buckets; one lookup replaces one full string hash per probe row.

**Bench target.** Q14 lineitem ⨝ part on `p_partkey` — partkey isn't
a string column so this won't help Q14 directly. The real win is on
queries like Q05 (`l_orderkey ⨝ o_orderkey ⨝ c_custkey ⨝ n_name ⨝
r_name`) where the nation/region joins are on low-cardinality strings.

Expect ≥2× on hash-join step where the probe column is dict-encoded.

## Implementation surface

Files we'll add (estimates):

```
crates/ematix-flow-core/src/dict_exec/mod.rs              (~200 loc)
crates/ematix-flow-core/src/dict_exec/filter.rs           (~400 loc)
crates/ematix-flow-core/src/dict_exec/aggregate.rs        (~500 loc)
crates/ematix-flow-core/src/dict_exec/join.rs             (~600 loc)
crates/ematix-flow-core/src/dict_exec/optimizer_rule.rs   (~300 loc)
crates/ematix-flow-core/src/dict_exec/dict_resolve.rs     (~150 loc)
```

Files we'll touch:

* `crates/ematix-flow-core/src/lib.rs` — register the optimizer rule
  alongside `EnableFusedJitRule`.
* `examples/tpch_full_bench.rs` — add a "dict-on / dict-off" toggle so
  the regression sweep keeps the default path benchmarked.

## Acceptance criteria

A Σ.E3 release lands when **all** of these hold:

1. **Correctness.** A new `dict_exec_correctness_*` test module
   (target: 60+ tests across the three execs) pins bit-identical
   results between `DictFilterExec` / `DictHashAggregateExec` /
   `DictHashJoinExec` and DataFusion's default execs, on every TPC-H
   query that exercises the relevant shape (Q01, Q03, Q05, Q12, Q19
   for filters/aggs; Q05, Q07, Q08, Q09, Q10 for joins).
2. **Bench wins.** SF=10 statistical bench (15 trials, σ-envelope):
   ≥3 outright wins on string-heavy queries (Q01, Q12, Q19 are the
   most likely candidates); 0 outright losses across all 22 queries.
3. **No regressions on non-dict paths.** Queries with no string keys
   (Q06, Q14, Q15) within run-to-run noise (σ-envelope "noise"
   classification, not "loss").
4. **Optimizer rule is opt-out, not opt-in.** Default-on; disabled by
   a session config flag (`ematix.dict_exec.enable = false`) for A/B
   testing.
5. **README update.** New row in the "what we beat" table — string-
   heavy SF=10 numbers — with the new claim wired into the existing
   regression-sweep harness so it stays honest.

## Risks + open questions

* **Dict ID stability across row groups.** Parquet doesn't guarantee
  the same dict across row groups of the same column; FastParquet
  *currently* concatenates dicts seamlessly when they're identical and
  falls back to flat when they diverge. The optimizer rule has to
  detect the "stable dict across all input batches" property at plan
  time or be willing to fall back at runtime when a batch arrives with
  a different dict. Runtime fallback is the safer call.
* **DataFusion API stability.** `ExecutionPlan` trait is the largest
  API surface we hook. Pinned to DF 53.1 currently; an upgrade to 54+
  will need re-validation of every `execute()` / `properties()` /
  `with_new_children()` signature.
* **Multi-key group-by.** Σ.E3b ships single-key only. Multi-key
  needs a `u64` (or `u128` for wide dicts) packed group representation
  + a hash function over packed codes. Doable but punted to Σ.E3c+.
* **Memory accounting.** Photon's memory manager tracks dict-encoded
  group tables vs flat tables separately. We rely on DataFusion's
  `MemoryReservation`; dict-encoded tables are smaller, so default
  spill thresholds will trigger later (good for us but means the
  estimate is loose).

## Effort + sequencing

* **Σ.E3a (DictFilterExec):** ~1 week. The optimizer rule + filter
  exec is the smallest landing; gets the pattern in place and tested
  before the larger pieces.
* **Σ.E3b (DictHashAggregateExec):** ~2 weeks. Hash-table layout +
  agg-state co-design is the meat of the work.
* **Σ.E3c (DictHashJoinExec):** ~2 weeks. Translation-table build is
  the new code; probe-side reuses the patterns from Σ.E3b's hash
  table.
* **Total:** ~5 weeks single-developer, sequenced. Σ.E3a is shippable
  on its own and gives a measurable Q12 win immediately — don't wait
  for the trio.

## Predecessors / what unlocks this

* **Σ.E2 FastParquet** — required: only FastParquet preserves the
  dictionary across the scan boundary today.
* **Σ.D3 JIT substrate** — *not* required for Σ.E3, but the
  `FusedFilterAggSpec` shape could later host JIT'd dict-aware
  predicates (Σ.E4 candidate: JIT'd dict-code AND-OR predicate trees).

## What this is not

* It is **not** a Photon clone or a Velox port. We pick the one
  Photon idea that maps cleanly onto DataFusion's existing extension
  hooks. Operator fusion (the other big Photon idea) is already
  partially handled by Σ.D3's fused execs + JIT substrate; adaptive
  shuffle is N/A until we have distributed pressure.
* It is **not** a generalized columnar-encoding framework. We hard-
  code parquet dictionary as the encoding; RLE / bit-packed / delta
  encodings stay flattened at scan. If a future workload demands
  dict-aware-equivalent execution over a different encoding, that's a
  separate phase.
