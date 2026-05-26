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

## Tracking after Σ.X bench

Once Σ.X re-baselines 22q at SF=1 and SF=10 with Σ.U + Σ.V active:

1. Compare new geomean to historical 0.738 / 17 wins (the milestone reference).
2. Re-audit any wall-time regressions to confirm they're not structural (the "where did the lost work go" diagnostic — per the correctness-first framing).
3. Rank these perf items by expected gain × effort:
   - Cheap one-rule fixes: RoundRobin→Hash dropper.
   - Medium: Σ.P CSE extension (Q11, Q21).
   - Bigger: Q05 composite-key split, Q22 subset-CSE, Q10 FD-aware agg.
   - Multi-week: Q08 bushy join planner.
4. Work through them in order; bench after each.
