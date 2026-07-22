# P3 — Native SQL front-end

**Goal.** Give the clean-room engine (`crates/ematix-flow-engine`) its own SQL
front-end, so queries **plan onto engine pipelines** instead of being
hand-assembled in Rust. This is the P3 phase of `NATIVE_ENGINE.md` — the real
unlock: today exactly one query (Q08) runs engine-native and it is hand-wired.

**Kill-gate (phase).** Arbitrary TPC-H SQL plans onto the engine, not hand-built
pipelines. First concrete gate: **Q6 from a SQL string == the hand-built
`run_tpch_q6_native`** (revenue `123141078.2283`, matched `114160`), then Q08.

## Perf campaign — CLOSEOUT (2026-07-19): at the structural floor

The native engine runs **TPC-H 22/22** and **TPC-DS 103/103 (sf1 + sf10)**
engine-native, parity-clean vs in-process DuckDB, **no query > ~5 s solo at
sf10**, zero DataFusion in the path. After a long lever campaign the
incremental perf frontier is **spent** — the remaining gaps are not reachable
by incremental optimization. Evidence, from this session's four consecutive
digs (each profiled-first, gated, measured):

1. **Hash-agg grouped merge** → net-NEGATIVE (+5.3 s), reverted. Sorted
   grouped output is load-bearing (rollup contiguity + sort amortized into the
   parallel per-RG build); a "faster merge" that drops it pays downstream.
2. **Interned / vectorized group-key build** → INERT, reverted. The premise
   (string-compare cost) was refuted by profiling — q39 is MERGE-bound
   (26.5 M-entry k-way merge), not build-bound; the per-row `Arc<str>` alloc
   is real waste but never on the critical path.
3. **Dim-build / root-decode overlap** → MARGINAL (~2.5 %), banked opt-in
   (`EMAT_OVERLAP`, default off). Overlap SHIFTS time rather than saving it:
   dim-build 75→176 ms as root-scan 221→115 ms, ~1:1 — the dim build is
   memory-BANDWIDTH-bound, so the "idle" cores are not free.
4. **Decode-bandwidth** → floor-bound (scoped, not built). Native decode is
   ~80 % Snappy **decompress** for numeric columns (Q08 dim-build sample) and
   **dictionary-expansion / `ByteArray` materialization** for strings (Q1:
   `get_batch_with_dict<ByteArray>` + `extend_with` + `Vector::utf8` ≫
   decompress). The levers:
   - **RG/page stats pruning — DEAD on this data.** Probed sf10
     `store_sales.ss_sold_date_sk`, `orders.o_orderdate`,
     `lineitem.l_shipdate`, `store_sales.ss_item_sk`: **every** RG's min/max
     spans 100 % of the column range, so a range predicate touches ALL row
     groups. TPC-H/DS generators don't cluster; nothing is prunable (the
     general form of the Q14 page-index dead-end).
   - **Faster decompress kernel — no.** Hand-rolled Snappy already rejected
     (microbench win, −12 % real); decompress is bound on output-write
     bandwidth, not the algorithm, so no drop-in codec helps.
   - **Projection pushdown — already tight** (only registered columns decode).
   - **Late materialization — ~3 % ceiling** (pages decompress as a unit).
   - **Writer-side codec (LZ4/uncompressed)** — real, but *different data*, not
     comparable to DuckDB-on-Snappy: a product/deployment lever, not a
     benchmark win.

This aligns with the standing SF=100 finding: **all headroom is plan
structure, not decode.** And at sf10 the plan structure is already tight.

**The one genuine remaining headroom is a PROJECT, not a lever:** dictionary
preservation for low-cardinality string columns (keep `(dict, indices)`
instead of expanding) — real bandwidth + index-domain filter/group-by wins,
but multi-week, and the prior attempt (Σ.K.2) was **+41 % per-table yet a
GLOBAL regression**, so it needs deliberate per-table dict routing, not a
global flip.

**Recommendation.** Treat the perf campaign as CLOSED at the structural floor.
Marginal effort now yields more from **breadth / robustness** (more SQL
surface, NULL semantics, correctness edges) than from speed. Reopen perf only
for (a) the dictionary-preservation project if string-heavy workloads become a
priority, or (b) a genuinely new shape the current gate doesn't cover.

Banked, correct, opt-in levers available if a specific deployment wants them:
`EMAT_OVERLAP` (bandwidth-rich boxes), order-preserving-i64 merge interning
(q39-shaped, ~0.5 s, unbuilt), FD-recognition (redundant group-key drop,
scoped/unbuilt).

## Pipeline

```
SQL text
  → parse      sqlparser::Parser → AST            (0.61, the sanctioned bootstrap; tokenize→AST only, not DataFusion)
  → bind       AST + Catalog → LogicalPlan        (name/type resolution; owned, typed Expr; constant-fold in decimal)
  → optimize   LogicalPlan → LogicalPlan          (P3 tail: re-home the Σ rules onto the owned IR; skipped in slices 1–3)
  → plan       LogicalPlan → engine pipeline       (scan + ops + sink over DataChunk/Selection)
  → execute    run on the engine → result
```

## Owned IR (the pieces that must exist)

- **`ScalarValue`** — `Int32 | Int64 | Float64 | Date32 | Boolean | Utf8 |
  Decimal(i128, scale) | Null`. Decimal is load-bearing for correctness (see
  the decimal-fold decision).
- **`Expr`** (bound, typed): `Column(idx) | Literal(ScalarValue) | Binary{op,
  lhs, rhs}` first; then `Between`, `Cast`, `IsNull`, `AggregateCall{func,
  arg}`, `Not`. Bound = columns are indices into the decoded chunk, not names.
- **`LogicalPlan`**: `Scan{table, projection} | Filter{input, predicate} |
  Projection{input, exprs} | Aggregate{input, group_exprs, agg_exprs}`; then
  `Join`, `Sort`, `Limit`.
- **`Catalog`**: `table name → { parquet path, columns: [(name, leaf_idx,
  LogicalType)] }`. Generic — schemas are registered / read from parquet
  metadata, never TPC-H-hardcoded in engine code.

## Key decisions

1. **`sqlparser` for parse only.** The sanctioned bootstrap (a standalone lib,
   not DataFusion). We own everything from the AST inward. Revisit an owned
   parser only if error quality / parse latency is measured to matter.
2. **Interpreted-first evaluation.** The expression layer starts as a
   bound-tree interpreter over `DataChunk` (correctness-first; matches the
   program's "interpreted-first, revisit JIT as a measured adaptive layer"
   stance). Vectorized / compiled evaluation is a labelled follow-on — the
   hand-coded fast paths (`q6_over_chunks`) stay until the general path is
   measured and closed to them.
3. **Constant-fold in decimal, cast to f64 at the leaf.** `l_discount between
   0.06 - 0.01 and 0.06 + 0.01` must fold in **decimal** → `[0.05, 0.07]`, then
   the bound is cast to the nearest f64. Folding `0.06 + 0.01` in f64 lands one
   ULP below stored `0.07` and silently drops ~1/3 of Q6's matches (the lesson
   recorded at `lib.rs:62`). This is the binder's first real correctness
   obligation.
4. **The Σ rules re-home later.** The 28 DataFusion optimizer rules are written
   against DF's `LogicalPlan`/`TreeNode`; they port only once the owned IR + a
   rewrite framework exist. Slices 1–3 deliberately skip optimization — a naive
   plan that runs correctly first, rules second.

## Status

- **Slice 1 DONE** (2026-07-18, `d11531ea`) — `expr.rs`, gate `tests/expr_eval.rs`.
- **Slice 2 DONE** (2026-07-18, `4160e7e5`) — `catalog.rs` + `logical.rs` +
  `bind.rs`, gate `tests/bind_q6.rs` (decimal-exact `[0.05, 0.07]` bounds
  pinned; f64-fold wrongness pinned by `assert_ne!(0.06+0.01, 0.07)`).
- **Slice 3 DONE (2026-07-18, `dc08fb25`) — first P3 kill-gate HIT.**
  `plan.rs`; gate `tests/sql_q6.rs`: `bind_sql(Q6) → execute` over SF-1 ==
  `run_tpch_q6_native` **bit-for-bit** (same filter, same per-chunk partial
  association, same chunk order ⇒ `assert_eq!` on the f64), plus the DuckDB
  oracle (123141078.2283) asserted independently. The full pipeline — AST →
  binder → owned `LogicalPlan` → physical plan → native scan → expression
  eval → aggregate — is engine code end to end; DataFusion appears nowhere.
  Sequential + interpreted on purpose; the parallel morsel driver and
  `HashAggregateSink` exist and the planner grows into them next.
- **Slice 4a DONE** (2026-07-18, `7e1cc747`) — GROUP BY from SQL. Binder
  partitions SELECT into group keys (must precede aggregates, must match
  GROUP BY in order, must be integer-family columns — all bind-time checked)
  and aggregate calls; executor hash-accumulates per key tuple (BTreeMap ⇒
  key-sorted output). Gate `tests/sql_groupby.rs`: revenue by `l_linenumber`
  over the 1994 window vs a pyarrow oracle (7 groups, rel 1e-9; exercises the
  Int32 column path).
- **Slice 4b DONE** (2026-07-18, `e94a0ce9`) — two-table joins from SQL.
  Multi-table binder: comma-form FROM, WHERE split into conjuncts (cross-table
  equality ⇒ join condition; else single-table attribution via a
  touched-tables set ⇒ that table's filter; anything else errors by name).
  `logical::Join` = inner equi-join, right side key-only; the executor
  consumes the right side into key→match-**count** and narrows the left with
  **selection multiplicity** (a live row kept once per match) — unique keys
  degenerate to exactly the probe-narrow semijoin, duplicate keys stay
  correct. Gate `tests/sql_join.rs`: both directions vs pyarrow — unique-key
  (lineitem⋈orders-1994, 7 groups) and duplicate-key (orders⋈lineitem,
  weighted sum over 6,001,215 rows — a membership-only join lands ~4× low).
- **Slices 4c + 5 DONE (2026-07-18, `4f5b5765`) — second P3 kill-gate HIT:
  Q08 from SQL.** The IR went flat: `BoundQuery` = a join **graph** (per-table
  scan+filters, join edges over a global slot space, group/agg/output exprs) —
  the select-project-join block the Σ rules will rewrite. Binder: aliases +
  qualified names (`nation n1/n2`), N-table FROM, row-space output projections
  with aggregate extraction (`sum(a)/sum(b)`), EXTRACT(YEAR)/CASE/Div/strings/
  count(*), connectivity validation. Executor: tree rooted at the **largest
  table by parquet row count** (first cost-based physical decision); dim
  subtrees → key→(count, payload) maps — key-only stays the semijoin narrow
  with multiplicity, referenced dim columns **bubble up chains as payloads**
  and attach to root chunks (unique-key payload joins, runtime-checked);
  string scans route via the stock reader; full agg set COUNT(*)/MIN/MAX/AVG/
  SUM; Q6's scalar-SUM bit-equality association preserved. Gates:
  `sql_join_tree.rs` (payload bubbling + key-only chains), `sql_aggfuncs.rs`
  (count == the Q6 gate's 114,160 — cross-checked), `sql_strings.rs` (1451
  parts, the hand-built dim gate's numbers from SQL), `sql_exprs.rs`
  (division-of-sums, CASE), **`sql_q08.rs` — the flattened 8-table Q08 with
  self-joined nation, shares 1995 = 0.0344359 / 1996 = 0.0414855 (canonical
  SF-1 answer) at rel 1e-9**. 55 engine tests green.
- **P4 breadth (2026-07-18, `bada7eb4` + `9039641c` + `77754d4d`):
  13 of 22 canonical TPC-H queries run engine-native from SQL** —
  {Q1, Q3, Q4, Q5, Q6, Q08, Q10, Q11, Q12, Q14, Q16, Q18, Q19}, each read
  verbatim from `examples/tpch/queries/*.sql` and gated vs independent
  oracles (`tests/tpch_from_sql.rs`; results match canonical published SF-1
  answers). Capabilities landed, each forced by a real query:
  - typed group keys (Int/Float/Str, total-ordered), ORDER BY/LIMIT/HAVING;
  - interval date arithmetic (day/month/year, civil round-trip), IN lists,
    LIKE (greedy %/_ matcher);
  - multi-table WHERE conjuncts → **post-join filters** at the root, with
    binder **OR-factoring** (`(J∧A)∨(J∧B) → J∧(A∨B)`, Q19's shape);
  - join **cycles** → spanning tree + residual post-join equalities (Q5),
    tree edges chosen breadth-first in WHERE order;
  - **uncorrelated subqueries**: recursive `bind_query`, executor pre-pass
    substitutes `ScalarSub`→literal (Q11 HAVING threshold) and
    `InSub`→materialized i64 `InSet` (Q18, Q16 NOT IN); agg-less IN-inner
    SELECTs get set semantics via injected GROUP BY;
  - `COUNT(DISTINCT)` (Q16); grouped HAVING over non-selected aggregates;
  - **EXISTS decorrelation** (Q4): the equality-correlated EXISTS rewrites
    at bind to the `IN` semijoin it is (set semantics — no row
    multiplication).

- **P4 derived tables + decorrelation (2026-07-18, `672222fe` + `23ba4277`):
  20 of 22 TPC-H engine-native from SQL.** Added: **composite join keys**
  (multi-edge table pairs fuse into one multi-column link — Q9's
  lineitem⋈partsupp); **derived-table inlining** (plain select-project
  FROM-subqueries merge into the outer join graph, view columns bind by
  defining expression — Q7/Q9/Q22, the classic view-merge); **materialized
  derived tables + WITH/CTEs** (`TableSource::Derived`, executor
  materializes the inner `BoundQuery`, schema inferred via `output_types` —
  Q15, CTE referenced twice); **correlated scalar decorrelation** (single or
  multi-key correlation → grouped derived join; comparison lands in the
  post-join filter via normal attribution — Q17, Q2, Q20's composite-key
  correlation inside a nested IN-subquery); **plain row queries** (no
  agg/GROUP BY → slot-space outputs, one row per joined row, no silent
  dedup — Q2, Q15); **SUBSTRING** (Q22). All gates == canonical published
  SF-1 answers.

- **P4 TPC-H COMPLETE (2026-07-18, `90fdaee3`): 22/22 canonical TPC-H
  queries plan from their production SQL texts and execute engine-native**,
  each gated vs an independent oracle matching the canonical published SF-1
  answers. The last two: **Q13** — `JOIN … ON` + LEFT OUTER (preserved-side
  root forcing, miss-keeps-row narrowing, type-default payload attach, and
  `count(left col)` → `CountMatched` via a synthetic matched-flag column —
  the no-NULL engine's outer-join counting); **Q21** — the count-based
  EXISTS rewrite: `EXISTS(same k, different s)` → a per-k
  `count(distinct s)`/`min(s)`/`__m=1` derived table LEFT-joined on k, with
  `EXISTS ⟺ __m=1 ∧ (cd≥2 ∨ ms≠s)` and `NOT EXISTS ⟺ __m=0 ∨ (cd=1 ∧ ms=s)`.

- **Parallel planned executor (2026-07-18, `7d3766d2`): planned Q08 SF-10
  7000 ms → 836 ms (8.4×).** Morsel-parallel root pipeline (per-RG partials
  merged in row-group order — deterministic at any thread count; the Q6
  bit-equality gate passes parallel), morsel-parallel dim scans, and
  zero-allocation probes (`DimMap` single-key specialization +
  scratch-slice lookups; single-threaded alone 7000→5347 ms). Full
  measured ladder in the commit. All 22 gates green under the parallel
  executor. Remaining ~6× to the hand-built 141 ms arm: sequential dim
  MAP BUILDING (15M single-threaded inserts), per-row interpreted
  expression eval, payload attach — the named next levers.

- **Sharded columnar dim maps (2026-07-18, `33de3a16`): planned Q08 SF-10
  836 ms → 350 ms (2.4×; cumulative 20× from the 7000 ms interpreted
  baseline).** All three named levers landed as one `DimResult` redesign:
  dim builds run as two morsel-parallel phases (scan+emit into
  per-(row-group, shard) buffers → concurrent per-shard merges, folding
  row groups in row-group order so layout and dup-key errors stay
  deterministic); payloads store COLUMNAR per shard (`PayCol`), probes
  return a payload row index recorded during the narrow probe itself, and
  the attach gathers typed values with no re-probe, no `ScalarValue`
  boxing; `FastHasher` (multiplicative mix + fmix64) replaces SipHash in
  the dim maps; probe/emit loops read keys and payload sources by direct
  slice view (`KeyCol`/`PaySrc`) instead of per-row interpreter dispatch.
  Thread scaling 2952/840/350 ms at 1/4/14 threads; single-thread total
  work itself dropped 1.8× (5347→2952 ms). 76/76 green incl. the Q6
  bit-equality gate. **Remaining 2.5× to the 141 ms arm decomposes as:
  root decode ~140 ms (at the Snappy-decompress wall — REV.20), root
  probe/attach/agg ~100 ms, and the dim phase serialized BEFORE the root
  ~110 ms. The next structural lever is pipelining dim builds under root
  decode (bounded decode-ahead queue) — an architecture change, not a
  local optimization.**

- **TPC-DS breadth campaign begun (2026-07-18, commits `680b4d99` →
  `223a5a5a` → `c00a5a6e` → string-keys): 0 → 40/103 canonical Spark
  TPC-DS queries EXECUTE engine-native at sf1** (57/103 bind). The
  harness (`examples/tpcds_coverage.rs`) registers the 24-table catalog
  from parquet footers and prints a failure taxonomy per run; single-query
  debug mode `[qNN]`. Substrate added: `Catalog::register_parquet`
  (schema/decimals/nullability from the footer), INT-backed DECIMAL
  decode (scaled f64), definition-level validity decode, NULL semantics
  (3VL-lite predicates, null-skipping aggregates, null join keys never
  match, NULL group keys, payload validity end-to-end — LEFT misses now
  attach NULL), set operations (UNION [ALL]/INTERSECT/EXCEPT), window
  functions (partition aggregates, cumulative ROWS/RANGE frames,
  rank/dense_rank/row_number) post-HAVING in an extended row space,
  string join keys via a per-run interner, string IN-sets, CAST folding,
  ORDER-BY-expression hidden outputs, SELECT * expansion, scalar
  abs/coalesce/nullif rewrites, stddev_samp, CASE-operand desugar, NULL
  literal, IS [NOT] NULL. Correctness gates: `tests/tpcds_decode.rs`
  (pyarrow oracles: decimal sums with 130k NULLs, per-year join sums
  dropping 129,850 NULL date keys). Remaining top gaps: duplicate-key
  payload joins (fact-to-fact; needs chained payload rows + compaction
  at the join), LEFT-ON extra conditions, correlated-scalar over CTEs,
  ROLLUP, EXISTS multi-table FROM, cross-join blocks, general JOIN…ON,
  ambiguous-name scoping.

- **Duplicate-key payload join (2026-07-18, `a38ea0fb`): TPC-DS exec
  40 → 48.** A payload dim can now hold several rows per join key (a
  fact-to-fact or grouped-derived join — e.g. q65's per-(store, item)
  revenue joined to `item` on item_sk alone). The dim map threads the
  extra rows onto a per-key singly-linked chain (`Shard.chain`, built
  lazily on the first duplicate payload key a shard sees), and a fan-out
  child REMATERIALIZES the root view to the expanded length: each live
  row emits one output row per matching dim row, existing columns
  gathered by the source root row and this dim's payloads by each output
  row's own dim row; later children read keys from the view's slot
  columns so a second fan-out composes. The unique-key fast path
  (including Q08's 15M-row dims) is byte-for-byte unchanged — Q08 SF-10
  still 350 ms. Fan-out **below** the root (a multi dim as a grandchild —
  q17/q25/q29/q91) and LEFT fan-out remain explicit errors: the dim
  *build* would have to expand too. Gate: `tests/dup_key_payload_join.rs`
  (125,811 fanned rows, revenue 104159495.71 vs pyarrow).

- **Oracle parity gate (2026-07-18, `bdb62c55`): all 48 native-executing
  TPC-DS queries MATCH DuckDB value-for-value at SF=1.**
  `ematix-flow-core/examples/tpcds_native_oracle.rs` runs each query the
  native engine can execute AND the same text on in-process DuckDB over
  the identical Parquet, comparing full result sets (sorted multiset,
  1e-6 FP tolerance, trimmed strings — the `tpch_validate` contract);
  non-zero exit on any mismatch. The sweep found and fixed three real
  correctness bugs that *executed and returned plausible wrong numbers*:
  ORDER BY sorted NULLs FIRST (SQL default is NULLS LAST — with LIMIT it
  changed which rows survived); SUM/MIN/MAX/AVG/STDDEV over zero non-NULL
  values returned 0.0 instead of NULL; and `count(*)` over an otherwise-
  unreferenced table returned 0 (the empty projection lost the row
  count — this broke `count(*)` over *any* single table). Regression
  pins in `tests/sql_null_semantics.rs`. The lesson: coverage (runs) is
  not correctness (right answer) — only a value-oracle caught these.

- **Fan-out below the root (2026-07-19, `9bab9ea1`): TPC-DS exec
  48 → 52, all 52 parity-match DuckDB.** A duplicate-key payload dim can
  now appear as a grandchild, not only as a root child. `build_dim`'s
  emit gained a cartesian-product path: when a child dim is `multi`, the
  table's surviving row expands to one dim row per combination of its
  children's payload rows (an odometer over per-child match lists — a
  chain contributes several). The parent dim becomes `multi` in turn, so
  the fan-out composes recursively up to the root's `fanout_child`. The
  no-multi-child fast path is verbatim, so Q08 SF-10 stays 350 ms and the
  Q6 bit gate holds. q17/q25/q29/q91 now execute and match DuckDB;
  regression `tests/dup_key_payload_join.rs::fanout_below_root`.

- **Date-string coercion in BETWEEN / IN (2026-07-19, `ce6fd812`):
  TPC-DS exec 52 → 56, all 56 parity-match DuckDB.** The comparison arm
  already folds a `'YYYY-MM-DD'` string to Date32 against a date column,
  but BETWEEN and IN build their comparisons via `binary()` directly and
  skipped it — so `d_date BETWEEN '…' AND …` reached the evaluator as a
  string-vs-integer compare and panicked the worker (q32/q83/q92/q95).
  Both desugars now coerce their bounds / elements. Regression in
  `tests/sql_null_semantics.rs`.

- **`SELECT DISTINCT` (2026-07-19, `815d39ea`): a correctness fix, not a
  coverage bump — exec holds 56/103, parity 56/56.** DISTINCT was silently
  dropped: a plain-row DISTINCT emitted one output row per joined row
  (duplicates and all), wrong-but-hidden only because the queries that use
  it (q38/q87) feed INTERSECT/EXCEPT, which dedup anyway. Now the common
  no-GROUP-BY DISTINCT **folds into a GROUP BY over the projected columns**
  (reusing the oracle-verified set-semantics path — dedups during
  aggregation, no post-materialize), and a `BoundQuery::distinct` flag
  dedups the final rows for any residual shape (DISTINCT layered on an
  explicit GROUP BY, or over an aggregate) so it is never silently wrong.
  `DISTINCT ON` errors; `SELECT ALL` is the no-dedup default. The folded
  path leaves the flag false, so unique queries pay nothing. q41 (the one
  query where top-level DISTINCT is load-bearing) stays gated by an
  unrelated feature (correlated `i1.i_category` scalar subquery), so the
  exec count is unchanged. Regression `tests/sql_distinct.rs` (3 paths:
  fold-into-group, flag-dedup over explicit GROUP BY, DISTINCT+ORDER+LIMIT).

- **Two-key LEFT-JOIN ON (2026-07-19, `2943daf8`): TPC-DS exec
  56 → 57, all 57 parity-match DuckDB.** The TPC-DS outer-join shape
  `LEFT OUTER JOIN cr ON (cs.k1 = cr.k1 AND cs.k2 = cr.k2)` (q5/q40/q49/
  q72/q80) parses the whole ON as **one parenthesized `Nested(a AND b)`
  node**, and `split_and` didn't descend through parens — so the two
  equijoins never split into edges; the whole thing hit the non-equi
  branch and errored as "ON condition on a LEFT JOIN's preserved side."
  `split_and` now unwraps `Nested` (as `ident_parts` already did), so the
  conjunction splits into two edges the planner merges into one
  **composite-key** dim (the `links` path already existed). One-line
  binder fix, no executor change. q40 now executes + parity-matches
  (its `coalesce(cr_refunded_cash, 0)` exercises composite-key + LEFT
  NULL-fill together). The other four stay gated by unrelated features
  (unsupported agg fn q5/q80; agg in ORDER BY q49; interval-on-column
  q72) — this was the join-shape fix, not those. Regression
  `tests/sql_left_join_composite_key.rs`: both keys discriminate
  (144,067 two-key vs 1,458,686 one-key inner), and the LEFT join
  preserves every driving row (1,441,548 == catalog_sales alone).

- **Interval arithmetic on a date column (2026-07-19, `f7cb0f47`):
  q72's bind unblocked.** `date_column ± interval N days` (q72's
  `d3.d_date > d1.d_date + interval 5 days`) previously errored — only
  literal dates folded. Since Date32 is days-since-epoch and the evaluator
  compares dates as day counts, a day/week interval is a **constant
  integer offset**, so it lowers to an integer add — no new evaluator
  path. Month/year on a column stay literal-only (variable length).
  Regression `tests/sql_interval_on_column.rs`. **q72 still does not
  execute**: its `catalog_sales ⋈ inventory ON item_sk` is a fact-to-fact
  fan-out on a low-cardinality key that blows the intermediate up → OOM
  (SIGKILL) — a join-planning/scale problem, not this feature.

### Remaining LEFT-JOIN-cluster blockers (each a separate, larger feature)
The five two-key-LEFT-JOIN queries split after the join-shape + interval
fixes: **q40 executes+parity** (done). The rest each need substantial,
*unrelated* features, not a small unblock:
- **q5, q80**: `concat` mis-routed to `bind_aggregate` (concat is scalar,
  needs a string-concat Expr) **+ `GROUP BY ROLLUP`** (grouping sets — a
  major feature, also gating q18/22/27/36/67/77) **+ multi-CTE UNION-ALL
  feeding an outer aggregate**.
- **q49**: `rank() OVER` window functions over a **derived table** +
  aggregates nested inside CAST/division + a WHERE filter on a LEFT join's
  right side (which changes join semantics).
- **q72**: interval-on-column done (above), but blocked on the
  inventory fact-to-fact fan-out join plan (OOM).
The highest-leverage next unit is **ROLLUP** (unblocks the most queries);
window-over-derived is the q49 prerequisite.

- **GROUP BY ROLLUP + concat (2026-07-19, `9146be23` + `9839a287`): q80
  executes + parity-matches DuckDB (100 rows).** Two features:
  - **ROLLUP** — after the base groups are built over all term columns,
    each coarser grouping set is derived by re-merging the base groups that
    share its surviving prefix (`AggState::merge` — SUM/COUNT/MIN/MAX/AVG
    all merge exactly), dropped columns keyed `GroupKey::Rollup` (renders
    NULL but is a DISTINCT map key, so a subtotal never collapses into a
    genuine-NULL group). No change to the hot per-row grouping path.
    Regression `tests/sql_rollup.rs` (grand total = sum of parts; row-count
    identity `|ROLLUP(a,b)| = |(a,b)| + |(a)| + 1`).
  - **concat + scalar-fn plain projections** — `concat(...)` builds an
    owned string, so `Expr::Concat` is evaluated only via `eval_value`
    (never the borrowed `Val` path); NULL args skipped (DuckDB). AND the
    plain-row gate now keys off a new `contains_aggregate` (transparent to
    scalar functions) instead of `contains_function`, so a group-less
    scalar projection (the q5/q80 inner branch) is plain rows. Regression
    `tests/sql_concat.rs`.
  q80's `coalesce(sr_return_amt,0)` over the two-key LEFT join + ROLLUP +
  concat all reconcile against DuckDB.
- **Case-insensitive identifiers + decimal-cast typing (2026-07-19): q5
  executes + parity-matches DuckDB (100 rows) → exec 62→63/102, 63/63
  parity, 0 MISMATCH.** Two coupled fixes:
  - **Case-insensitive identifier resolution** — unquoted SQL identifiers
    are case-insensitive (Spark/DuckDB). q5 aliases `sum(return_amt) AS
    RETURNS` in a CTE, then references `returns` outer. `Catalog::table`,
    `TableDef::column`, `ViewMap::get`, the used-projection dedup, and every
    table-alias/display comparison now prefer an exact hit and fall back to
    `eq_ignore_ascii_case` (so a repeated mixed-case reference dedups to one
    projection slot instead of duplicating). Regression
    `tests/sql_case_insensitive_idents.rs`.
  - **`CAST(_ AS DECIMAL/NUMERIC/FLOAT/DOUBLE)` types as Float64** — the
    cast previously passed a bare int literal straight through, so q5's
    UNION-ALL padding branch `cast(0 AS DECIMAL(7,2)) AS return_amt` was
    typed integer while the sibling branch's real `sr_return_amt` was
    Float64 — a materialization type clash. A numeric *literal* under a
    decimal/float cast is now coerced to `Float64` (int targets still pass
    through integral). This lets the union reconcile.
- **Window-over-derived + LEFT→INNER demotion + aggregate-in-CAST
  (2026-07-19): q49 executes + parity-matches DuckDB (34 rows) → exec
  63→66/102 (q49 + two ratio queries), 66/66 parity, 0 MISMATCH.** Three
  stacked features, each a prerequisite for the next layer of q49:
  - **Aggregate inside a CAST** — `cast(sum(x) AS DECIMAL(15,4)) /
    cast(sum(y) AS DECIMAL(15,4))` (a decimal ratio of two sums). A cast
    previously hid the aggregate from `contains_aggregate`/`contains_function`
    (they had no `Cast` arm), so the query was misclassified as plain-rows
    and the `sum` hit the generic scalar path ("aggregate calls are only
    allowed in the SELECT list"). Both predicates now descend through `Cast`,
    and `bind_output` gained a `Cast` arm that binds the inner aggregate in
    row space then applies the same numeric-cast → Float64 typing (shared
    `float_cast_expr` helper with the scalar path).
  - **Window over a no-GROUP-BY input** (`rank() OVER (ORDER BY r) FROM
    derived`) — the new `windowed_plain` mode: the row space IS the slot
    space, so non-window projections pass through as slot columns
    (`Binder::plain_passthrough`) and windows append after the last slot
    (remap base = slot count, not group+agg count). The executor gathers
    each row group's surviving rows into a slot-indexed `RgOut::Chunk`,
    `concat_chunks` merges them into the single global input, then the
    existing window stage appends one column per window and projects.
    `output_types` / `infer_windowed_slot_type` type a `Column ≥ nslots` as
    its window value (rank → Int64). Regression `tests/sql_window_over_derived.rs`.
  - **LEFT→INNER demotion** — a WHERE predicate on a LEFT JOIN's nullable
    side that rejects NULLs (`wr.wr_return_amt > 10000` — any
    comparison/arithmetic is UNKNOWN on NULL, so unmatched rows drop) is
    exactly an INNER join. The binder now clears the edge's `preserved`
    marking and routes the predicate as that table's filter instead of
    erroring; an `IS NULL` conjunct (the anti-join case) is still refused.
- **GROUPING() + int-cast rounding + alias-in-ORDER-BY-expr (2026-07-19):
  q27/q36/q54/q70 (+q22-family) → exec 66→71/102, 71/71 parity, 0
  MISMATCH.** The ROLLUP-adjacent cluster, four coupled features:
  - **`GROUPING(col)`** — 1 when `col` is a ROLLUP subtotal (aggregated
    away), else 0. Falls straight out of the ROLLUP design: a dropped
    column's group key is the `GroupKey::Rollup` sentinel, so
    `GROUPING(col_k) = (key[k] == Rollup)`. Row space becomes `[keys, aggs,
    grouping-flags, windows]` — the executor appends one 0/1 flag column per
    key (before windows, so a window `PARTITION BY grouping(a)+grouping(b)`
    reads them: q36/q70). Bound via a `GROUPING_BASE` sentinel remapped
    alongside `WINDOW_BASE` at block end. `BoundQuery::has_grouping` gates
    the flag columns.
  - **`CAST(<fractional> AS INT)` rounds to nearest** (`Expr::CastInt`,
    DuckDB semantics: `10714.82 → 10715`). Integer casts had passed the
    float straight through — a latent bug q54 surfaced (its
    `cast(revenue/50 AS INT)` segment differed from DuckDB by the fraction).
  - **SELECT alias inside an ORDER BY expression** — `ORDER BY CASE WHEN
    lochierarchy = 0 THEN i_category END` (q36/q70) references the alias
    `lochierarchy` *within* an expression, not as a bare key. The binder now
    exposes output aliases (`Binder::output_aliases`) during the ORDER BY
    pass, and `bind_output`'s fast path falls through to structural
    recursion (instead of erroring) when the whole expression isn't itself a
    single GROUP BY key — so a compound expression over keys/aliases binds.
  Regression `tests/sql_grouping_and_intcast.rs`.
- **Join shapes + name resolution + CTE decorrelation (2026-07-19,
  `e30c46ec`): exec 71→81/102, 81/81 parity, 0 MISMATCH** — q1 q6 q14b q16
  q30 q41 q75 q81 q93 q94. Three sub-units: (a) inline-derived FROM bodies
  route JOIN…ON through `add_join`; a WHERE equijoin touching a LEFT
  join's nullable side demotes it to INNER (q93 was a silent MISMATCH
  until demoted); bare `LEFT JOIN` accepted; WHERE `IS NULL` on the
  nullable side = anti-join routed as a post-join filter over the
  NULL-filled payload (`tests/sql_left_join_shapes.rs` pins the anti+semi
  partition identity). (b) `SELECT *` expands to table-qualified refs and
  skips synthetic tables; unqualified names prefer real tables over
  `__corr`/`__ex` materializations. (c) `try_decorrelate_scalar` accepts
  aliased/CTE inner tables and inner-qualified correlation refs, and
  OR-factors the subquery WHERE first (q41's correlation duplicated in
  every OR branch).
- **q72 OOM fix (2026-07-19): batched fan-out with early residual
  filtering.** Multi (duplicate-key) children now process LAST (stable
  reorder after dim build) so all single-key payloads are attached; the
  fan-out filters candidate expansions against the post-join conjuncts
  whose columns are already available, in 256k-candidate batches that
  materialize ONLY the residual's columns — survivors-only full
  materialization. q72 (cs⋈inventory on item_sk, ~660×/row ≈ 1B-row
  intermediate) went OOM-SIGKILL → 100 rows, 1.7GB peak, PARITY_OK;
  removed from both harnesses' OOM_SKIP. Regression
  `tests/sql_fanout_residual.rs` (early residual ≡ composite-key join).
- **FULL OUTER JOIN via rewrite (2026-07-19): q51/q97.** `A FULL OUTER
  JOIN B ON c` rewrites at the AST to a UNION ALL derived wrapper —
  `(A LEFT JOIN B ON c)` ∪ `(B LEFT JOIN A ON c WHERE a.key IS NULL)`
  (the mirrored anti-join) — with side-qualified references in the
  enclosing select rewritten to the wrapper's prefixed `__fo_{a,b}_col`
  columns. Zero executor change; reuses the LEFT + anti + UNION ALL
  machinery hardened above.

- **The last 19 (2026-07-19): exec 84→103/103 — EVERY TPC-DS query
  executes and parity-matches DuckDB.** Nine coupled features:
  - **Parenthesized set-op sides** — `a UNION ALL (SELECT …)` parses as a
    `SetExpr::Query` wrapper; wrappers with no own WITH/ORDER/LIMIT
    flatten recursively before binding (q2/q23ab/q87).
  - **Row-space concat/SUBSTR** in grouped projections (`bind_output`
    arms; q66/q85/q8).
  - **Scalar-aggregate cross-join views** — a no-GROUP-BY aggregate
    derived/CTE (guaranteed 1 row) past the first FROM item becomes a
    view of single-column scalar subqueries: references substitute as
    constants, so the cross join needs no edge (q28/q61/q77/q88/q90).
    `SELECT *` expands over views. `count(DISTINCT <float>)` keys by bit
    pattern (`eval_opt_distinct_key`; q28 panicked on Float input).
  - **Multi-table EXISTS** — one correlation equality found among the
    conjuncts, stripped, rest bound as a set-semantics IN-subquery
    (q10/q35/q69).
  - **Inline-collision → materialize** — twin plain deriveds over the
    same CTE/table names can't both inline into one scope (q2/q59).
  - **Keyless cross-join children** — the bind connectivity check is
    GONE: a disconnected component is legitimate SQL (a cross join) and
    attaches as a constant-key (`fill_key` pushes 0) fan-out child; the
    batched early residual prunes during expansion. Covers q2's
    `d_week_seq1 = d_week_seq2 - 53` arithmetic join and q8's
    `substr(s_zip,1,2) = substr(V1.ca_zip,1,2)` expression join.
  - **round(x[,d]) + upper(x)** — `Expr::Round` (numeric, borrowed path);
    `Expr::Upper` yields an OWNED string so it evaluates via `eval_value`
    in projections and INLINE inside =/<> comparisons (no owned value
    escapes the borrowed `Val` path); q24ab's
    `c_birth_country = upper(ca_country)`, q2/q78's `round(ratio, 2)`.
  - **Oracle-harness DuckDB fixups** — q90's bare `at` alias and q77's
    bare `returns` alias are DuckDB-reserved; quoted/AS-prefixed on the
    DuckDB side only.

- **sf10 parity gate (2026-07-19, `56dd31eb`): 103/103 PARITY_OK at BOTH
  scales.** `tpcds_native_oracle` takes a scale arg (`sf10 [qNN]`);
  the sf10 sweep runs per-query subprocesses (an OOM SIGKILL loses one
  query, not the run) — 0 mismatch, 0 deaths; q72's batched fan-out holds
  at 133M-row inventory; slowest q4 62s / q23ab ~50s. The sweep surfaced
  ONE scale-dependent bug: the counted `<>`-EXISTS rewrite ignored NULL
  semantics on the compared column (q16's nullable `cs_warehouse_sk`) —
  a NULL outer value must make EXISTS false (the `cd ≥ 2` shortcut fired
  anyway), and an all-NULL inner key must make NOT EXISTS true. NULL
  guards added; `tests/sql_exists_neq_nulls.rs` pins both counts against
  an independent python row-walk oracle.

- **CTE-sharing perf (2026-07-19, `e4e83c4a`): q4 sf10 62s→13s.** CTE
  references share one `Arc<BoundQuery>`; a per-execute `DerivedMemo`
  (pointer-keyed) materializes each shared CTE once (q4 ran `year_total`
  6×; measured via `EMAT_TRACE_DERIVED`). Only `strong_count > 1` Arcs
  memoize — caching single-reference deriveds would pin their row
  vectors for nothing. q11/q14ab/q51 dropped under 10s; q23ab 50→23s;
  q39ab 24→12s. Both parity gates + suite re-run green. Remaining heavy
  sf10 shapes (different bottlenecks): q78 48s, q67 36s, q23ab 23s.

- **Heavy-tail perf campaign (2026-07-19, `e9a6cae0`+`d97a040d`+
  `f1ac6f19`): the three worst sf10 shapes, 4 general levers.** The
  oracle now prints per-side timing in single-query mode (`native N ms,
  duck M ms`); `EMAT_TRACE_AGG=1` prints merge/rollup/finalize phases.
  1. **Constant pushdown into single-referenced CTE group keys**
     (bind.rs pre-pass): outer `WHERE cte_col = const` — directly or
     transitively via join equalities, LEFT-ON edges flowing only into
     the nullable side — injects into the CTE's own WHERE. q78's `ss`
     CTE stopped materializing 24.4M groups for one wanted year
     (48.7s→8.8s). Shared CTEs are never touched;
     `tests/sql_cte_const_pushdown.rs` pins IR + results + the
     shared-CTE guard.
  2. **Cascaded ROLLUP levels** (plan.rs): level t re-aggregates level
     t+1 with a linear run-merge (sorted iteration keeps equal prefixes
     contiguous) instead of re-probing the whole map per level. q67's
     8-level rollup: 16.5s→1.9s. `tests/sql_rollup_levels.rs` pins
     ROLLUP ≡ union of its prefix GROUP BYs (multi-col terms, merged
     avg; floats compared at 9 sig digits — subtotal avg legitimately
     differs in the last ULPs from a flat GROUP BY).
  3. **K-way + range-partitioned parallel merge of per-RG partials**:
     run-head heap over sorted maps, large inputs split by sampled
     pivots (`BTreeMap::split_off`) and merged per-thread. q23a's 13.8M
     group merge 9s→2.5s.
  4. **Agg-only HAVING pre-filter** (new exhaustive `Expr::for_each_col`):
     when HAVING touches only agg slots, groups filter BEFORE key
     columns materialize; survivors bulk-rebuild and the rejected
     majority frees on a detached thread (retain() paid ~4.3s in
     rebalance+drops; now 0.47s).
  Final sf10 solo (native incl. DuckDB-side check per run): q78 4.5s,
  q67 13.5s, q23a 5.1s, q23b 5.3s, q4 8.7s. Full sf1+sf10 sweeps
  103/103 re-run green after each lever. Remaining q67 tail is
  kernel-shaped: finalize row materialization, the 5.8M-row rank
  window (top-K pushdown candidate), string-keyed merge itself.

- **q59 offset-equijoin (2026-07-19, `103ad8fa`): 15.0s→0.91s (16.5×).**
  The sample profile showed 14s of per-row `Expr::eval` + per-access
  `from_utf8`: the outer `WHERE store1 = store2 AND week1 = week2 - 52`
  joined on the ~102-distinct store id alone (the arithmetic conjunct
  couldn't be an edge) and string-filtered ~17M fanned candidates. Two
  levers:
  1. **Offset-equijoin promotion** (bind pre-pass): `a = b ± N` between
     two derived FROM items appends `<b's expr> ± N AS __ejk<i>` to b's
     projection, rewrites the conjunct to a plain equality, and
     force-materializes BOTH participants (inlining one re-opens a join
     cycle whose broken edge falls back to the residual). The planner
     then merges the equalities into one composite-key hash join. Also
     fires on q2's `- 53` (neutral there — its cost is the 21.6M-row
     materialized-union CTE, a different shape).
     `tests/sql_offset_equijoin.rs` pins edges/no-residual/both-
     materialized + result equivalence vs a hand-computed key.
  2. **Validate-once UTF-8**: `Vector::utf8` checks the whole buffer +
     offset char boundaries at construction; `Utf8View::get` slices
     unchecked (was re-validating per access on every string path).
     Construction-rejects-invalid tests pin the invariant.
  Post-lever sweep 103/103; new sf10 tail: q67 14s, q95 13s, q4/q14ab 9s.

- **q95 set-narrowing + chunked deriveds (2026-07-19, `49aaeaf0`):
  11.5s→1.2s (10×).** Sample profile: ~10s in a SINGLE-THREADED
  BTreeMap dedup over the 74.8M-row `ws_wh` self-join CTE (a derived
  scans as ONE row group), plus the web_returns join fanning ~125 dup
  rows per order. Two levers:
  1. **Bounded derived chunks** — `result_to_chunks` slices a
     materialized derived into ≤2^21-row chunks; per-RG parallelism now
     applies to every big-derived scan (11.5→5.2s alone).
     `tests/sql_derived_chunking.rs` gates identity at sf1 (2.88M-row
     derived = 2 chunks).
  2. **CTE set-narrowing** (bind pre-pass) — a CTE whose EVERY reference
     sits inside an IN-subquery (set semantics: multiplicity can't
     matter) and uses only some columns narrows to `SELECT DISTINCT
     <used>`. Reference accounting is word-boundary counting over body +
     other CTE defs; `collect_idents` refuses unmodeled expr variants
     (blocks rather than guesses). ws_wh → 600k rows; the fan join
     vanishes (5.2→1.2s). `tests/sql_cte_set_narrowing.rs` pins narrowed
     IR width, result equivalence, and the row-context blocker.
  Sweep 103/103. sf10 tail now: q67 14s (kernel-shaped, rank top-K
  designed), then the 9s cluster (q4/q14ab).

- **Rank top-K + columnar hand-off (2026-07-19, `84d4308f` +
  `1ada5eac`): q67 12.3→8.4s, q4 8.5→6.2s, q78→3.8s.**
  1. **Rank top-K window prune** — outer `WHERE rk <= K` on a derived's
     LONE rank()/row_number() window arms `WindowExpr::top_k` at bind
     time; the executor selects each partition's K-th best row
     (`select_nth`, linear) and keeps only rows at-or-before it before
     the window sort/projection. q67's dw2: 5.79M → 1,107 rows. Rank
     values on the prefix are provably identical; threshold ties are
     trimmed by the still-applied filter; dense_rank excluded (its
     rank-K frontier passes the K-th best row); shared CTEs never take
     the hint. `tests/sql_window_topk.rs`.
  2. **Columnar derived hand-off** — `QueryResult::col_chunks`: a
     derived whose outputs are plain columns/literals (no windows /
     ORDER / LIMIT / DISTINCT) returns bounded chunks — finalize
     selects VECTORS from the row-space chunk (Arc clone when HAVING
     kept everything) instead of evaluating millions of ScalarValue
     cells; UNION ALL concatenates side chunk lists; `result_to_chunks`
     passes vectors through with rows-path-equivalent coercions; mixed
     set-op sides downgrade via `rows_from_chunks`. The representation
     is internal to table_src↔consumers — the top level always
     materializes rows. `tests/sql_derived_chunking.rs` + full suite.
  Final sweeps: sf1 103/103, sf10 103/103 solo. sf10 tail after this
  session: q67 8.4s (dw1 merge/rollup/finalize kernels), q14ab ~8s,
  q4 6.2s — all kernel/CTE-internal now, no plan-shape lever visible.

- **Set-flavored side dedup (2026-07-19, `ded80a8a`): q14a 8.2→1.8s,
  q14b→1.7s, q38→0.9s, q87→1.0s.** The agg-kernel dig started at q14a
  and the SAMPLE overturned the assumption: its 7s was execute_set's
  single-threaded row sort — the INTERSECT sides arrived as ~28.8M raw
  fact rows. A side combined by UNION/INTERSECT/EXCEPT (anything but
  UNION ALL) contributes only its DISTINCT rows, so `bind_set_query`
  binds those sides (and the base when the first op is flavored) with
  set semantics → the projection folds into a GROUP BY and dedup runs
  in the parallel aggregation. The fold fires only for PLAIN-IDENTIFIER
  projections: the first cut (window-guard only) broke q75's
  `cs_quantity - COALESCE(…)` UNION sides — caught by the
  sweep-after-commit discipline as a 102/103, root-caused
  ("neither an aggregate nor a GROUP BY key"), narrowed. execute_set's
  combine-time dedup still applies everywhere, so the fold is purely an
  optimization. `tests/sql_setop_side_dedup.rs` (folded IR; INTERSECT ≡
  IN-subquery and EXCEPT ≡ NOT-IN — independent machinery; UNION ALL
  multiplicity pin). Final sweeps sf1+sf10 103/103. Tail: q67 10s
  (dw1: merge 1.8 + rollup 2.1 + row_chunk build 3.2 — the true
  agg-kernel unit: hash agg + string interning), q4 7s, 6s cluster
  (q72/q51/q28/q23ab).

- **Sorted-Vec agg spine (2026-07-19, `9f492b68`): q67 9.0→3.0s, q4
  6.1→3.7s, q51→4.5s, q23a→3.4s, q23b→3.5s.** The planned "hash-agg
  kernel rewrite" turned out unnecessary — the sample showed ~40% of
  q67's merge phase was `BTreeMap::from_iter` re-SORTING and
  re-tree-building the k-way merge's already-sorted output, and the
  single hottest remaining symbol (712 samples, 5× anything) was DROP
  GLUE freeing 5.8M `Vec<GroupKey>` keys inline. Post-merge grouped
  representation is now a sorted `Vec<(key, states)>` (`GroupsVec`):
  merge returns the concatenated range output directly; the ROLLUP
  cascade iterates contiguously and APPENDS its levels after the base
  (finest first — new pinned convention); HAVING pre-filter keeps a
  vec; finalize builds row-space columns in PARALLEL (per-column scoped
  threads) and the columnar hand-off projects chunks in parallel; the
  consumed groups vec drops on a detached thread.
  `tests/sql_agg_vec_spine.rs` (level-append order + bit-identical at
  1/13/default threads).

- **Column-at-a-time filter masks (2026-07-19, `d8019201`): q28
  4.4→0.94s, q88→0.4s.** q28's six 28.8M-row slices spent 20× more in
  recursive per-row `Expr::eval` than in decode. `filter_expr` now
  builds typed whole-column boolean masks for And/Or (hybrid per-row
  fallback on unmaskable siblings, evaluated only on undecided rows),
  cmp col-vs-lit / col-vs-col with the interpreter's exact promotion
  rules, IS NULL from validity, IN sets, LIKE. NULL→false masks compose
  exactly through And/Or (no NOT node exists — per-leaf negation flags
  handled inside each leaf). Sparse selections (<1/4 live) keep the
  per-row path. `tests/expr_filter_mask.rs` row-equivalence vs the
  interpreter across every leaf kernel/validity/promotion/negation.

- **Fan-out key widening + probe-domain mask fix (2026-07-19,
  `c7ba2080`): q72 5.0→3.6s.** The dig found the mask fast path never
  engaged on fan-out residual batches — `n_rows()` reads the FIRST
  column (an empty placeholder in fan-out views), failing every leaf
  length check; the domain now comes from the selection (this is the
  whole q72 win — the batch residual evaluates columnar). The widening
  lever itself (post-join `Eq(colA, colB)` across subtrees → promoted
  into the dim's composite key; build appends the subtree-side value at
  emit, probe appends the root-side column, attach-order constrained)
  landed with two measured guards: the LARGER subtree wins the
  orientation (widening tiny d1 dragged the fan-out ahead of the
  narrowing dims: 5s→12s), and a dim larger than the probe side never
  widens, with NO small-side fallback (inventory's 133M-row build went
  1.0→7.1s at 26.6M widened Multi keys — Vec-alloc per emit — costing
  more than the probe saved). So q72 runs UNwidened; the lever fires
  2000× on dim-smaller-than-root shapes (gate suite 466s→0.2s).
  EMAT_NO_WIDEN / EMAT_TRACE_JOIN. `tests/sql_fanout_key_widen.rs`
  (widened ≡ pre-joined formulations through independent machinery).

- **Vectorized agg-argument eval (2026-07-19, `5bb21952`): TPC-H Q1
  sf10 1508→1143ms (−24%); INERT on the TPC-DS tail (honest).** A
  compound numeric agg arg (arithmetic tree, e.g. Q1's
  `sum(l_ext * (1 - l_disc) * (1 + l_tax))`) evaluates once as a typed
  column per chunk (`eval_num_col`, the agg-arg analog of the filter
  mask) instead of the interpreter per row. Bit-identical to
  `eval_opt_f64`: integer stays i64 until consumed, float promotes,
  Div→f64, NULL propagates; fires only for dense selections so Q6's
  selective per-row summation (and its bit-equality gate) is untouched.
  ★ LESSON — the profile OVERTURNED my "helps the whole board" pitch:
  q4's `Expr::eval` was `eval_value` on the *group keys* + the
  BTreeMap-build closure, NOT the agg arg (`num_binary` dropped to ~1
  sample after — the precompute fired but the arg was never the cost).
  The TPC-DS tail (q4/q39/q51) is grouped-BUILD + k-way-merge +
  gather_payload bound; this lever is real only for the compute-in-agg
  family (TPC-H Q1). Verified: A/B via EMAT_NO_VEC_AGG, sf10 sweep
  103/103 neutral (3 flagged deltas re-timed to baseline solo =
  consecutive-warm thermal). `tests/expr_num_col.rs`.
  ★ THE MEASURED TAIL LEVER IS NOW EXPLICIT: grouped aggregation over
  BTreeMap + per-RG sorted-merge — per-row `Vec<GroupKey>` alloc,
  string-key `Arc<str>` churn, and cross-partial `AggState::merge`
  (q39 6.12M-group merge 2.4s).

- **Hash-based grouped aggregation — BUILT, MEASURED NET-NEGATIVE,
  REVERTED (2026-07-19).** Replaced per-RG BTreeMap build + k-way sorted
  merge with per-RG `FastMap` build + worker-side shard routing +
  hash-partitioned parallel merge (fold per shard in RG order → SUM
  bit-identical at any thread count; unsorted-but-deterministic output,
  parity gate sorts). Fully implemented + gated (a determinism/value
  guard, 4 order-assuming tests updated to sort-before-compare, sf1
  103/103). Result: **q39 merge 2.4→1.5s (−0.5s total)** — the ONE win
  (string-key comparisons removed) — but **q67 3.0→7.3s** and **q51
  4.5→6.9s**, net **+5.3s worse**. ★★ WHY (the load-bearing insight):
  the old design's SORTED output is relied on across the engine —
  (a) the ROLLUP cascade needs key-contiguity, and the old BTreeMap
  build AMORTIZES that sort into the parallel per-RG build (q67's
  "merge" was 208ms *because* the runs were pre-sorted); the hash design
  defers it to one big final sort of 5.79M rows × 8 string keys = 5.7s;
  (b) non-rollup consumers are order-sensitive too — q51's FULL-OUTER-
  JOIN-via-UNION rewrite regressed 1.5s on unsorted grouped input. Hash
  merge only wins a large NON-rollup terminal aggregation with expensive
  key comparisons (q39), and the aggregation can't know if its consumer
  needs order. ★ LESSON: a "faster merge" that drops a global invariant
  (sorted output) pays for it downstream — measure the WHOLE query, not
  the merge in isolation; and amortized-into-parallel-build can beat a
  single post-pass. Not retried without: interned-i64 keys to keep the
  k-way merge (attacks q39's actual cost — string compares — WITHOUT
  losing sort), or a per-consumer "needs-sorted" signal.

- **Interned/vectorized group-key build — BUILT, MEASURED INERT, REVERTED
  (2026-07-19).** Followed the hash-agg retry precondition ("intern string
  group keys to attack q39's cost while keeping the sorted merge"). PROFILE
  FIRST refuted the premise: in q39a's per-RG BUILD, string comparison
  (memcmp+Head::cmp) is only ~5% — the real per-row waste is a fresh
  `Arc<str>` allocation for the string group key on *every* row
  (`GroupKey::from(eval_value)` → `Arc::from(s)` at expr.rs:422), q39a's ~20
  distinct `w_warehouse_name`s over millions of rows. Built a bit-identical
  vectorized key build: per-chunk `KeyColumn` (numeric widened in bulk,
  strings deduped so one Arc per distinct value), per-row indexes it;
  `EMAT_NO_VEC_KEY` A/B hatch; gated dense; a bit-identity test (vec ≡
  per-row, incl. row order — sorted spine preserved). sf1 103/103, A/B
  identical. **Result: INERT** — q39a 3900→3900ms, q67 2986↔2996ms, TPC-H
  Q1 interleaved 1291↔1271ms (per-row marginally *faster*; ±200ms noise
  swamps it). ★ WHY: the per-row Arc alloc is real waste but NEVER on the
  critical path — the `EMAT_TRACE_MERGE` trace showed q39a is MERGE-bound
  (1084 runs, **26.5M** partial entries → the k-way merge dominated by
  leading-string memcmp + `AggState::merge`), q67 is merge/window-bound,
  and TPC-H Q1's build is dominated by agg arithmetic + the 59M BTreeMap
  *probes* (which still compare strings). Killing the string cost safely
  means order-preserving *integer* keys in the MERGE+probe (not just
  deduped Arc alloc in the build) — the heavy path the hash-agg lesson
  warns against. ★ LESSON (again): a "should-help" string lever must be
  aimed at the phase the profile says is hot; the group-KEY *build* isn't
  it. Reverted (inert code in the hot group-build path = complexity +
  codegen risk for zero win). Retry only with: (a) order-preserving i64
  interning threaded through the MERGE comparison (keeps sorted output;
  ~0.5s optimistic on q39a alone — likely not worth the merge-machinery
  risk), or (b) FD-recognition that drops the redundant `w_warehouse_name`
  group key (it's 1:1 with `w_warehouse_sk` and not in q39a's SELECT), so
  the merge keys go all-integer.

- **Dim-build / root-decode OVERLAP — BUILT, MEASURED MARGINAL, BANKED
  OPT-IN (2026-07-19, `EMAT_OVERLAP`, default OFF).** The last named
  structural lever toward Q08's hand-built 141 ms. Phase split (native
  Q08 sf10): the dim build is a ~75 ms SERIAL prefix (orders ~46 ms, part
  ~28 ms) before the ~220 ms morsel-parallel `lineitem` scan. Kill-gate
  passed — the dim build is only ~40% parallel-efficient at 14 threads
  (1-thread 425 ms → 76 ms; but 4→14 threads only 1.8×), so cores look
  idle. Built a bounded decode-ahead: while the dims build, `ndec` spare
  threads prefetch a bounded (`cap`) prefix of root row groups into a
  per-RG cache; a scope-join barrier stops them before the main scan (no
  oversubscription), which then TAKES a cached chunk or decodes. sf1
  parity **103/103 with overlap ON and OFF**; engine suite green. ★ WHY
  ONLY MARGINAL: the `EMAT_TRACE_PHASE` split shows overlap SHIFTS time,
  it doesn't save it — dim-build 75→176 ms (+101) as root-scan 221→115 ms
  (−106), ~1:1. The prefetch decoders contend with the dim build for
  **memory BANDWIDTH** — the "idle" cores were bandwidth-bound, not free.
  Net: **~2.5% Q08, ~3% q19** (122→118 ms warm interleaved), neutral on
  small fact scans (q42/q52/q55), no regressions. ★ LESSON (for the
  multi-month morsel engine): the dim-build phase is bandwidth-bound, so
  the classic "overlap I/O with compute" win is near-zero-sum here — the
  real prize is reducing total bandwidth demand (codec / late-mat) or the
  probe cost, NOT adding concurrent bandwidth-hungry decode. Same shape as
  the rejected "2× parallelism budget," but gated OFF so it never touches
  the default path. Kept opt-in for bandwidth-rich deployments; NOT
  default-on (win is at the noise floor and bandwidth-fragile).

## SQL-surface breadth (pivot from perf, 2026-07-19)

Perf is closed at the structural floor (see closeout). Effort now goes to
**breadth / robustness** — the binder implemented exactly the TPC-H + TPC-DS
subset; general analytical SQL had broad gaps. Durable asset built:
`crates/ematix-flow-core/tests/sql_parity.rs` — a **general engine-vs-DuckDB
parity harness** for arbitrary SQL (sorted-multiset compare over the same
parquet, the `tpcds_native_oracle` contract generalized). Every new feature
lands a case here.

- **Tier-1 bundle — SHIPPED (`bc99f566`).** `EXTRACT` beyond YEAR
  (`Extract{field}` — month/day/quarter/dow/isodow/doy + ISO-8601 week,
  DuckDB-matched); scalar fns `lower/floor/ceil/mod/length/trim/replace`
  (new `NumFn`/`StrFn` nodes; string fns work in `WHERE` via the generalized
  owned-string comparison-inline path); unary minus on runtime exprs
  (`-x`→`0-x`); positional `GROUP BY 1`; function-valued GROUP BY keys bind
  as group refs in the projection. 4/4 parity tests, sf1 103/103 no
  regression, suite green.
- **Tier-2 bundle — SHIPPED (`4df978d2`).** *2a:* `var_samp`/`variance`,
  `var_pop`, `stddev_pop` (a shared `AggState::variance` off sum/sumsq/count;
  var_pop(1 row)=0, var_samp=NULL); `date_trunc('unit', date)`→Date32
  (year/quarter/month/ISO-week/day, new `Expr::DateTrunc`). *2b:* navigation
  windows `lag`/`lead`(offset, NULL past edge), `first_value`, `ntile`(even
  buckets) — new `WindowFunc` variants. 8/8 parity, sf1 103/103, suite green.
  **Deferred:** `last_value` (default RANGE-frame peer semantics — own unit);
  `string_agg` (needs in-agg ORDER BY); `bool_and`/`bool_or` (boolean output
  typing); `median`/`percentile_cont` (per-group ordering); named `WINDOW`
  clause; string/date-typed `lag`/`lead` (numeric-only today).
- **Tier-3 (NOT + 3VL) — SHIPPED (`88ebb6c4`).** New `Expr::Not(Box<Expr>)`
  (eval inverts Bool, passes Null through) plus the *enabling* fix: rewrote
  `eval_binary` AND/OR from 2VL to 3VL — AND FALSE-dominant, OR TRUE-dominant,
  unknown → `Val::Null` instead of collapsing to `Bool(false)`. Safe because
  `expect_bool` still collapses `Null→false` at bool consumers (WHERE keeps
  only TRUE), so only *projected* boolean values change — to the correct NULL.
  Design: `Not` returns `None` from `try_mask`, so the filter path falls back
  to per-row `eval_bool` (the typed mask path is untouched). Parity test
  injects NULLs via `nullif(l_linenumber,1)`; covers NOT over
  comparisons/IN/LIKE/IS NULL, NOT over AND/OR with NULLs, double negation,
  projected NOT-bool NULL. 9/9 parity, sf1 103/103, suite green.
- **Tier-3 join residuals — SHIPPED (`7cc285a7`).** `RIGHT`/`RIGHT OUTER
  JOIN` as a mirrored LEFT: the joined table is preserved (roots the tree),
  the OLD table it is keyed against becomes nullable, so all LEFT machinery
  (grouped NULL-extension, matched-only `COUNT`, WHERE-side demote-to-INNER)
  applies. `add_join` now walks the ON in two passes — equi conjuncts →
  edges (preserved = joined table for RIGHT / old side for LEFT / none for
  INNER), discovering the single nullable old table from the equi key; then
  non-equi conjuncts route (nullable single-table → pre-join filter; INNER
  multi → post-filter; preserved-side → error). A RIGHT keyed to two
  different old tables is rejected (two preserved roots). **Non-equi joins
  already worked** — a cross-table inequality is a post-join filter; a pure
  non-equi ON is a filtered cross join — now locked with gate tests. Gates:
  `right_outer_join` + `non_equi_join` (native == DuckDB over orders ⋈
  lineitem, a 1-to-many key with a filtered ON), `right_outer_mirrors_left`
  (data-driven RIGHT == LEFT over store_sales ⋈ store_returns),
  `check_rejected` on the two-old-table boundary. Parity 11/11, sf1 103/103
  0 MISMATCH, suite green.
- **Fan-out payload materialization — SHIPPED (`e6195738`).** Lifted the
  "LEFT duplicate-key payload" limit: an outer join whose preserved side
  fans out (one preserved row → many dim rows on a filtered ON) now
  materializes. `fanout_child` gained a `left` flag — an unmatched source
  row survives once with a `NO_REF` payload (`gather_payload` already renders
  that NULL-filled) and a 0 matched-flag; matched expansions carry 1, and the
  matched-flag column appends in the same fixed layout the non-multi LEFT
  path uses (CountMatched + final post-filter unchanged). A LEFT fan-out
  skips the early residual (which only tames INNER blow-up and would wrongly
  drop kept misses); INNER fan-out is behavior-identical. **Unblocks FULL
  OUTER on real 1-to-many keys** (the UNION-ALL rewrite's LEFT branch) **and
  fan-out anti-joins** (LEFT + RIGHT mirror). Gate:
  `outer_join_fanout_materialization` (nullable-column projection across the
  fan-out, COUNT/matched-COUNT/SUM, LEFT+RIGHT anti-joins, FULL OUTER count +
  projection). Parity 12/12, sf1 103/103 0 MISMATCH (q72 INNER fan-out
  unchanged), suite green.
- **CUBE / GROUPING SETS — SHIPPED (`d03fd49d`).** The general grouping-set
  forms ROLLUP's prefix cascade cannot express. New `BoundQuery.grouping_sets`
  (each set = active group-column indices). Binder: `CUBE(t₁..tₙ)` → all 2ⁿ
  subsets of the terms; `GROUPING SETS((…),…)` → the listed sets verbatim
  (duplicates kept per SQL); both intern distinct columns into `group` (a
  column repeated across terms/sets shares one slot); CUBE capped at 20 terms.
  Executor `build_grouping_sets` re-aggregates the base groups (keyed by all
  columns) into each set — kept columns retained, the rest re-keyed
  `GroupKey::Rollup` (renders NULL, distinct from a real NULL group), one
  sorted BTreeMap pass per set; the base's raw rows are replaced by the union
  over all sets. `GROUPING(col)` flags read the Rollup markers unchanged;
  ROLLUP keeps its dedicated cascade (q67 untouched). Gate:
  `grouping_sets_and_cube` (CUBE over 2/3 dims, GROUPING SETS incl. grand
  total + a repeated set, GROUPING() flags, multi-column CUBE term). Parity
  13/13, sf1 103/103 0 MISMATCH, suite green.
- **Tier-2 aggregate tail (median / percentile_cont / bool_and / bool_or) —
  SHIPPED (`11d47d78`).** *Buffered:* `median(x)` and `percentile_cont(p)
  WITHIN GROUP (ORDER BY x)` retain non-NULL values in a new `AggState::buf`
  (empty/unallocated for every foldable aggregate — the perf-gated queries
  pay only the 24-byte header, like the existing `distinct` set), sort at
  finalize, and linearly interpolate at `p` (median = 0.5); Float64, NULL over
  an empty group; `buf` concatenates on merge (sorted at finalize, so order-
  independent). *Foldable booleans:* `bool_and`/`bool_or` fold via `min`/`max`
  of the 0/1-coerced input into an Int64 0/1 column, rendered BOOLEAN by
  wrapping the reference as `slot = 1` at the single agg-reference site — a
  comparison → `Val::Bool` that propagates SQL NULL (`NULL = 1 → NULL`), so no
  `LogicalType::Boolean` / hot-eval-match change. `percentile_cont` reads the
  fraction from `f.args` and the value from `f.within_group`. Gate:
  `tier2_aggregate_tail` (median/percentile grouped+scalar, bool_and/bool_or
  grouped+scalar+HAVING, empty-group NULLs). Parity 14/14, sf1 103/103 0
  MISMATCH (foldable hot path untouched), suite green.
- **Window-frame breadth + last_value — SHIPPED (`2a23a573`).** The frame
  binder now accepts `UNBOUNDED FOLLOWING` (ROWS/RANGE) and records
  `WindowExpr::frame_end_unbounded`; such a frame is whole-partition regardless
  of ORDER BY, so aggregate windows produce the partition total on every row
  (the executor routes `order.is_empty() || frame_end_unbounded` to the
  whole-partition path). `last_value(x)` (new `WindowFunc::LastValue`, mirror of
  first_value): whole-partition frame → partition's last ordered row; default
  RANGE ..CURRENT ROW → the current row's peer-group last member. Gate:
  `window_last_value_and_frames`. Parity 15/15, sf1 103/103 0 MISMATCH.
- **string_agg / group_concat — SHIPPED (`b50111e9`).** Ordered string
  concatenation. Binder `bind_string_agg` parses `string_agg(value, delim
  [ORDER BY key …])` (delimiter from arg 2, ordering from the
  FunctionArgumentClause::OrderBy) onto a new `AggExpr::str_agg`. Executor:
  `AggState::sbuf` retains each `(ORDER BY key, value)` pair
  (empty/unallocated for other aggregates), merge concatenates, and agg_column
  sorts per-group (DESC honored) + joins into a Utf8 cell (NULL over an empty
  group). Gate: `string_agg_ordered` (trim()ed values so CHAR padding can't
  diverge). Parity 16/16, sf1 103/103 0 MISMATCH.
- **WITH RECURSIVE — SHIPPED (`d408bfb5`).** Iterate-to-fixpoint recursive CTEs.
  Binder `split_recursive` detects a UNION[ALL] body with a self-referencing
  term; `bind_recursive_cte` binds the anchor (for the schema), then the step
  with the self-reference resolving to the new `TableSource::WorkingSet`. The
  anchor `BoundQuery` carries the step + distinct flag in `BoundQuery::recursive`.
  Executor `execute_recursive`: run the seed, then repeatedly the step with its
  WorkingSet (a stack on `DerivedMemo`) bound to the last iteration's new rows,
  accumulating until it adds nothing (UNION dedups vs everything seen, UNION ALL
  keeps all); a 1M-iteration guard trips non-termination. Gate: `recursive_cte`
  (series, dedup + outer agg, multi-column accumulation, non-recursive CTE
  alongside). Parity 17/17, sf1 103/103 0 MISMATCH.
- **Breadth-closeout sweep (measured).** A 56-query probe through the parity
  harness classified the remaining surface: **16 supported**, **36 honest bind
  rejections** (loud/safe — `LIMIT OFFSET`, `||`, `%`, `true`/`false`,
  `CROSS JOIN`, `IS DISTINCT FROM`, `ILIKE`, scalar math fns, `greatest`/`least`,
  `current_date`, `INTERSECT/EXCEPT ALL`, `VALUES`, FROM-less SELECT, `cume_dist`/
  `percent_rank`/`nth_value`, `n PRECEDING/FOLLOWING` frames, named `WINDOW`,
  `DISTINCT ON`, `= ANY(subq)`, `date_part`/`datediff`, `POSITION`, array/regexp/
  `mode`/`percentile_disc`), and **4 silent-wrong-answer bugs** (below). The
  probe scaffold was exploratory and not committed.
- **Silent-correctness fixes — SHIPPED (`c2a7e5e8`).** The three sweep-found
  cases where the binder accepted syntax but dropped semantics: aggregate
  `FILTER (WHERE …)` (now `CASE WHEN pred THEN arg ELSE NULL END` — every
  aggregate skips NULL); `QUALIFY` (new `BoundQuery::qualify`, bound as a
  post-window predicate and applied as a row filter before projection;
  `windowed_plain` triggers on a QUALIFY-only window); `lag`/`lead` DEFAULT
  (new `WindowExpr::lag_default`, returned past the partition edge). Gate:
  `aggregate_filter_qualify_lag_default`. Parity 18/18, sf1 103/103 0 MISMATCH.
- **Operators & scalars bundle — SHIPPED (`49f9afc1`).** The high-value sweep
  gaps: `LIMIT … OFFSET` (new `BoundQuery::offset`); `||` → concat and `%` →
  mod() (Binary-arm interception); boolean literals `true`/`false` (incl. a
  constant WHERE conjunct — `true` folds, `false` empties); `CROSS JOIN`
  (edgeless disconnected-component attach); `IS [NOT] DISTINCT FROM` (NULL-safe
  compare via IsNull/Eq — parenthesize, since sqlparser groups `IS DISTINCT
  FROM x AND y` as `IS DISTINCT FROM (x AND y)`); `ILIKE` (new `Expr::Like.ci`
  flag, ASCII case-insensitive `like_match`); scalar math `sqrt`/`ln`/`exp`/
  `sign`/`trunc`/`power`/`greatest`/`least` (new `NumFn` variants) and
  `current_date` (folds to today's Date32). Gate:
  `operators_and_scalars_bundle`. Parity 19/19, sf1 103/103 0 MISMATCH.
- **count(distinct <string>) — SHIPPED (`b71d34be`).** The distinct-key path
  was i64-only and panicked on strings. New `Expr::eval_opt_distinct` →
  `DistinctKey` (numeric keys stay on the borrowed-Val fast path so q28 is
  unchanged; strings kept exactly in a new `AggState::distinct_str`; owned-
  string args route through `eval_value`). Gate: `count_distinct_string`.
  Parity 20/20, sf1 103/103 0 MISMATCH.
- **Breadth sweep #2 (measured).** After the bundle + silent-bug fixes +
  count-distinct-string, all 13 re-probed fixes pass. Remaining is a smaller,
  well-defined long-tail (all honest bind rejections): `VALUES`, FROM-less
  SELECT, `n PRECEDING/FOLLOWING` frames, named `WINDOW`, `nth_value`/
  `cume_dist`/`percent_rank`, `DISTINCT ON`, `= ANY/ALL(subq)`, `INTERSECT/
  EXCEPT ALL`, `date_part`/`datediff`/`EXTRACT(epoch)`, `POSITION`,
  `split_part`, `mode`/`percentile_disc`, `array_agg`, `regexp_matches`. One
  new silent item found: `CAST(x AS VARCHAR)` returns the value untyped
  instead of a string (type mismatch vs DuckDB) — the next silent-bug fix.

## Next (P4 tail → P5/P6)

- Vectorized expression eval for the grouped-agg per-row path (Q1-shaped
  queries) remains banked. Morsel-engine direction: attack total memory
  bandwidth (codec / late-materialization) or probe cost, not concurrency
  (dim-build is bandwidth-bound — overlap is near-zero-sum, see above).
- Re-home the Σ rules onto `BoundQuery`; CTE result sharing (Q15
  materializes twice); NULL semantics beyond the outer-join stand-ins;
  date display formatting.

## Slice sequence (each independently gated, TDD)

1. **`expr.rs` — bound `Expr` IR + `ScalarValue` + evaluator.** The general
   expression layer the engine lacks. `Column`, `Literal`, `Binary` (arith `+
   - *`; compare `>= > <= < = <>`; logical `AND OR`). `filter_expr(chunk,
   pred) → Selection` (no-materialization narrow) and `sum_expr_f64(chunk, sel,
   arg) → f64`. Gate: unit tests eval `ep*disc`, `shipdate >= lit AND shipdate
   < lit AND disc >= lo AND disc <= hi AND qty < 24` over a synthetic chunk;
   mixed-type numeric promotion; the Selection-narrowing predicate path. **No
   SQL, no catalog.**
2. **`catalog.rs` + `logical.rs` + `bind.rs` — SQL AST → typed `LogicalPlan`.**
   Parse with sqlparser, resolve names→(idx, type) via the Catalog, desugar
   `BETWEEN`, **fold constants in decimal**, produce `Aggregate → Filter →
   Scan` with bound `Expr`. Gate: binding Q6's SQL yields the expected typed
   plan and the exact `[0.05, 0.07]` bounds; an unknown column errors.
3. **`plan.rs` — `LogicalPlan` → engine pipeline → execute.** `Scan` → native
   scan (leaf indices + types from the catalog), `Filter` → a `filter_expr`
   op, `Aggregate([], [Sum(expr)])` → a scalar-sum sink. Gate: **Q6 from a SQL
   string == `run_tpch_q6_native`** — the first query planned from SQL, zero
   hand-assembly. First P3 kill-gate hit.
4. **Q08 from SQL.** Group-by + the three joins + dim reductions, planned — not
   hand-wired. Forces: `GROUP BY` key derivation, join planning onto
   `run_join_pipeline` / `probe_narrow`, multi-aggregate. Second gate.
5. **Broaden + optimize.** More agg functions (COUNT/MIN/MAX/AVG), more join
   types, NULLs/validity, then re-home the Σ rules onto the owned IR (the P3
   tail that bleeds into P4's surface expansion).
```
