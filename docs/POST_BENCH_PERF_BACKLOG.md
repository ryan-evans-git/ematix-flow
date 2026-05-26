# Post-Σ.U/Σ.V Perf Backlog

**Opened:** 2026-05-26
**Status:** captured during the Σ.U/Σ.V correctness sweep; deferred until after Σ.X (22q SF=1 + SF=10 rebench)

These are structurally-correct plans that produce the right answer but are less efficient than what a sound optimizer (DuckDB, Photon) would produce. Each is a perf opportunity, not a correctness bug — so per the **correctness-first** framing they wait until the new milestone baseline is captured.

---

## Q05 — composite-key split

**Current shape:**
```
Inner Join: lineitem.l_suppkey = supplier.s_suppkey, customer.c_nationkey = supplier.s_nationkey
```

The SQL has `WHERE l_suppkey = s_suppkey AND c_nationkey = s_nationkey` and both ARE real constraints (customer + supplier must be in the same nation). The plan evaluates both correctly via a multi-key Inner Join. DataFusion picks `CollectLeft` mode because of the composite key shape; DuckDB splits into two separate joins (one FK, one star-shape) and uses Partitioned hash join for both.

**Fix class:** logical rewrite — detect star-shape composite where one equi-key is FK and the other is a star-nation crossover; split into chain.
**Plan correctness:** OK (both forms produce identical row sets).
**Risk:** Σ.T's prior cost-model-based split attempt produced misleading orderings; needs a narrow pattern matcher, not generic cost-based reorder.

## Q07 — supplier broadcast against filtered lineitem

**Current shape:**
```
HashJoinExec: mode=CollectLeft, on=[(s_suppkey, l_suppkey)]
  ← supplier (100K rows, broadcast)
  ← lineitem (filtered to 1995-96, ~12M rows)
```

100K supplier rows broadcast across 14 partitions = 1.4M-row hash build per partition. **Audit's "nation×nation 625-row cross" claim was wrong** — DF already pushes the `IN (FRANCE,GERMANY)` filter to each nation subquery alias, so each scan produces ~2 rows. The OR-pair filter on the joined product is correctly applied.

**Fix class:** physical — switch supplier⋈lineitem to Partitioned mode when supplier exceeds a broadcast threshold.
**Plan correctness:** OK.

## Q08 — lineitem double-shuffle

**Current shape:**
```
RepartitionExec: Hash([l_partkey]) ← lineitem
  HashJoinExec on l_partkey = p_partkey
RepartitionExec: Hash([l_orderkey]) ← above output
  HashJoinExec on l_orderkey = o_orderkey
```

The 60M-row lineitem stream is shuffled twice — first on l_partkey, then re-shuffled on l_orderkey. DuckDB's bushy plan shuffles lineitem exactly once.

**Fix class:** join ordering (sound bushy planner). Σ.T attempted left-deep cost-based reorder but the cost model misjudged string-equality selectivity → picked orders→lineitem→part order which was worse. A bushy planner is multi-week work.
**Plan correctness:** OK.

## Q10 — FD-redundant GROUP BY

**Current shape:**
```
Aggregate: groupBy=[[c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment]]
```

`c_custkey` is the primary key of customer; the other 5 customer columns are functionally determined by it. `n_name` is functionally determined by `c_nationkey` (which is FD by c_custkey). Semantically equivalent: `GROUP BY c_custkey, ANY_VALUE(c_name), …`. The SQL explicitly groups by all 7 columns (standard TPC-H), so the plan is correct. A sound optimizer with FD/PK inference would rewrite to `GROUP BY c_custkey` and project the rest.

**Fix class:** logical rewrite — FD/PK-aware aggregate. Needs PK propagation from TableProvider statistics (currently absent).
**Plan correctness:** OK (extra group cols change nothing because they're FD-determined).

## Q11 — partsupp⋈supplier⋈nation subtree CSE

**Current shape:**
```
Inner Join:
  Aggregate(groupBy=[ps_partkey], sum(ps_supplycost * ps_availqty))
    Projection → partsupp ⋈ supplier ⋈ nation(GERMANY)
  SubqueryAlias __scalar_sq_1
    Aggregate(groupBy=[], sum(...) * 0.0001)
      Projection → partsupp ⋈ supplier ⋈ nation(GERMANY)   ← IDENTICAL to above
```

The 3-table join `partsupp ⋈ supplier ⋈ nation(GERMANY)` runs twice with byte-identical structure. Σ.P `SharedSubtreeExec` only dedupes at the Aggregate level (Q15 shape) — needs extension to dedupe at the *input* level so both consumers share one cached intermediate.

**Fix class:** physical — extend Σ.P to recognise identical pre-aggregate subtrees.
**Plan correctness:** OK.

## Q21 — lineitem scanned 3× (2 identical)

**Current shape:** Three lineitem TableScans inside the same plan:
- `l1`: `Filter: l_receiptdate > l_commitdate` → TableScan
- `__correlated_sq_1` (l2): unfiltered TableScan
- `__correlated_sq_2` (l3): SAME `Filter: l_receiptdate > l_commitdate` as l1 → TableScan

l1 and l3 are byte-identical. CSE-able.

**Fix class:** physical — extend Σ.P to recognise identical Filter+TableScan subtrees.
**Plan correctness:** OK.

## Q22 — customer scanned 2× with similar filters

**Current shape:**
- Outer scan: `Filter: substr(c_phone) IN ('13','31',…)` → TableScan customer
- Inner avg subquery: `Filter: c_acctbal > 0 AND substr(c_phone) IN (...)` → TableScan customer

The filters DIFFER (inner adds `c_acctbal > 0`), so an exact CSE doesn't apply. But the outer scan is a *strict superset* of the inner — we could materialise the outer scan once and apply the additional `c_acctbal > 0` filter on top for the inner consumer.

**Fix class:** physical — Σ.P CSE generalisation that recognises subset relationships between filtered scans (not just exact match).
**Plan correctness:** OK.

## Cross-cutting — `RoundRobinBatch → Hash` repartition stacking

Affects: Q02, Q08, Q09, Q16, Q17, Q19, Q22.

A `RepartitionExec: RoundRobinBatch(14)` followed by `RepartitionExec: Hash([col], 14)` on the same node performs two shuffles when one suffices. The RoundRobin step is wasted work.

**Fix class:** physical — one rule that drops `RoundRobinBatch` when the immediate parent is `Hash` on the same partition count.
**Plan correctness:** OK.

---

## Plan-diff audit vs DuckDB SF=10 (post-Σ.U/V, 2026-05-25)

**Methodology:** per-query structural diff of physical plans, not wall-time. Only NEW we-worse cases appear here — items already listed above are not duplicated. Source plans: `/tmp/ematix_plans_su/q??.plan` (ematix with Σ.U on), `/tmp/duckdb_plans/q??.plan` (DuckDB EXPLAIN).

**Bench numbers that motivated this audit (5-trial 22q SF=10, Σ.U + Σ.V on):**
- Wins: ematix 15 / DuckDB 5 / Polars 2
- DuckDB beat us on: Q05 (1.35×), Q06 (1.07×), Q07 (1.18×), Q08 (1.12×), Q15 (1.13×), Q17 (1.15×), Q18 (1.12×)
- Polars beat us on: Q06, Q15

### WE-WORSE (new) — items to add to backlog

#### Q17 — DELIM_JOIN / dedup-share missing (the 1.15× SF=10 gap)

- **DuckDB:** `LEFT_DELIM_JOIN(p_partkey IS NOT DISTINCT)` with a `DELIM_SCAN` feeding the avg-subquery's `HASH_JOIN(l_partkey=p_partkey)`. The filtered `part` set (2k rows) is materialised **once** and reused on both the outer join and the inner-correlated avg subquery. DuckDB also injects `l_partkey IN BF(p_partkey)` dynamic-filter into the outer lineitem scan.
- **Ematix:** Σ.U LeftSemi pushdown into the avg-subquery's lineitem reference is correct, but the `part` subtree is REBUILT — two `part.parquet` scans, two `Hash([p_partkey])` repartitions, two `FilterExec(p_brand=Brand#23 AND p_container=MED BOX)`.
- **Fix class:** logical/physical — extend Σ.P (`SharedSubtreeExec`) coverage to recognise the post-rewrite duplicated `part`-filter subtree across the outer join + RightSemi probe-build. Plus separately a bloom-side dynamic-filter pushdown to lineitem (a Σ.J.2.b.vi extension to scalar-subquery correlations).
- **Plan correctness:** OK.

<!-- Q18 entry retracted 2026-05-25: the original audit ran plan dumps via
     `sigma_q_explain_plan.rs` which did NOT register `PushDownLeftSemiRule`.
     With the rule active (it IS in preset.rs and default-on in the bench),
     Q18's LeftSemi sits directly above `TableScan: orders`, exactly matching
     DuckDB. The +12% SF=10 wall-time is non-plan (kernel/decode/hash).
     The explain dumper has been updated to match the bench's optimizer set. -->


### WE-BETTER (informational)

- Q01, Q06 — `FusedAggregateExec` collapses scan+filter+multi-agg into one kernel; DuckDB runs them as separate operators.
- Q12, Q14, Q19 — single in-filter `HashJoinExec` with predicate folded into the join, no double-scan of part.
- Q15 — `SharedSubtreeExec` (Σ.P) shares the per-supplier revenue aggregate between outer join and `max()` subquery.
- Q20 — `RightSemi(part⋈partsupp)` then `Inner(partsupp⋈lineitem agg)` keeps lineitem-side aggregate isolated.

### SAME (informational)

- Q02, Q03, Q04, Q09, Q10, Q11, Q13, Q14, Q16, Q19, Q20 — structurally equivalent join graphs to DuckDB. Wins/losses driven by codec, decode, or kernel speed, not plan shape.
- Q06, Q15 — gap is below the plan layer (Snappy decode floor; SharedSubtree replay cost). Already tracked in memory (`project_q06_sf10_polars_gap_wall.md`).

---

## Tracking after Σ.X bench

Σ.X re-baselined 22q at SF=1 and SF=10 with Σ.U + Σ.V active (2026-05-25):

- SF=1 result: 20 wins / 0 DuckDB / 2 Polars (Q06, Q15)
- SF=10 result: 15 wins / 5 DuckDB / 2 Polars (geomean ratio ematix/DuckDB ≈ 0.77 at 5 trials; vs historical 0.738 / 17 wins at 20 trials)
- 22/22 row-level correctness PASS at both SF=1 and SF=10

The slight regression (17→15 wins) is within 5-trial noise but the structural items above are real and worth ranking.

Ranked perf items by expected gain × effort:

1. **Cheap one-rule fixes**
   - RoundRobin→Hash dropper (cross-cutting, 7 queries affected).
2. **Medium**
   - Σ.P CSE extension (Q11, Q17 part subtree, Q21).
3. **Bigger**
   - Q05 composite-key split.
   - Q22 subset-CSE.
   - Q10 FD-aware aggregate.
4. **Multi-week**
   - Q08 bushy join planner.

Work through them in order; bench after each.
