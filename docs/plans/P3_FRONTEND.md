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

## Next (P4 tail → P5/P6)

- Close the remaining planned-vs-hand-built gap: parallel/sharded dim map
  builds, `ProbeStructure` fast path for key-only dims, vectorized
  expression eval, allocation-free payload attach.
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
