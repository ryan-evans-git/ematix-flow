# P3 — Native SQL front-end

**Goal.** Give the clean-room engine (`crates/ematix-flow-engine`) its own SQL
front-end, so queries **plan onto engine pipelines** instead of being
hand-assembled in Rust. This is the P3 phase of `NATIVE_ENGINE.md` — the real
unlock: today exactly one query (Q08) runs engine-native and it is hand-wired.

**Kill-gate (phase).** Arbitrary TPC-H SQL plans onto the engine, not hand-built
pipelines. First concrete gate: **Q6 from a SQL string == the hand-built
`run_tpch_q6_native`** (revenue `123141078.2283`, matched `114160`), then Q08.

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

## Next (P4 tail → P5/P6)

- Overlap the dim-build phase with root decode (bounded decode-ahead
  queue) — the last named structural lever toward the hand-built 141 ms;
  vectorized expression eval for the grouped-agg per-row path (Q1-shaped
  queries) remains banked.
- **TPC-DS breadth** through the same front-end.
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
