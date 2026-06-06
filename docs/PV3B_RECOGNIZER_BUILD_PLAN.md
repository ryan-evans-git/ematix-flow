# PV.3b — push-fusion recognizer build plan

**Goal:** land the Q08 −16% push-fusion win in the production plan path, behind
`EMAT_PUSH_PIPELINE=1` (default OFF). De-risked: `pv3b_q08_ab.rs` measured
**−10.8%** at SF=10 (a lower bound — the harness pays a MemTable round-trip the
in-plan splice avoids; true splice ≈ −13%, matching PV.2's −13.9%). Architect
cut **(c) validated**: fuse `part`+`orders'`, leave `supplier⋈n2` stock.

## Ground truth (measured)

**Q08 optimized LOGICAL plan** is a left-deep inner-join chain (7 joins) with the
dim-group joins INTERLEAVED, wrapped in column-pruning Projections:
```
Sort → Projection(mkt_share) → Aggregate[o_year] → SubqueryAlias all_nations
 → Projection[o_year=date_part(YEAR,o_orderdate), volume=l_ext*(1-l_disc), nation=n2.n_name]
  → Inner Join n1.n_regionkey=region.r_regionkey      (region, r_name=AMERICA)
   → Inner Join supplier.s_nationkey=n2.n_nationkey    (n2, carries n_name Utf8View)
    → Inner Join customer.c_nationkey=n1.n_nationkey   (n1, carries n_regionkey)
     → Inner Join orders.o_custkey=customer.c_custkey  (customer)
      → Inner Join lineitem.l_orderkey=orders.o_orderkey   (orders, o_orderdate BETWEEN)
       → Inner Join lineitem.l_suppkey=supplier.s_suppkey  (supplier, UNFILTERED)
        → Inner Join part.p_partkey=lineitem.l_partkey     (part, p_type=ECONOMY…)
         → [part Filter→Scan]  [lineitem Scan]
```
o_year is **Int32** (`date_part`); the fused fragment must emit it Int32 OR the
remainder must be rebuilt to group by i64 (the de-risk harness rebuilt → values
match, result o_year becomes i64 — acceptable for default-OFF).

**Reuse confirmed:** `join_reorder::flatten_inner_join_chain` (now `pub(crate)`)
descends through the pruning Projections, keeps `SubqueryAlias` (n1/n2) distinct,
and yields qualified `equi_preds` — so leaf-grouping needs no name disambiguation.

## Status — S1–S6 BUILT, wired, correctness-gated ✅

- ✅ **S1–S5 analysis** (`push_fusion_rule::analyze`) — flatten → fact (max-degree
  leaf) → BFS-group dims by fact-FK → cross-group/multi-group/residual-filter
  bails → **S5 integer-key type-gate** (every fact-incident edge must be
  i64-widenable; catches Q15's Float64 `total_revenue=max()` pseudo-star).
- ✅ **S4 classify** (`classify`) — emit-projection exprs bucketed by source
  (shared `emit_source`): part→Membership, orders→Payload(o_year, Int32 via
  date_part — type-keyed, no fn special-casing), supplier+n2→Stock.
- ✅ **S6 reconstruct** (`reconstruct`) — dim subqueries via `LogicalPlanBuilder`,
  a custom **`FusedProbeNode`** (mechanism **b**) + `FusedProbePlanner`
  ExtensionPlanner → `EmatPushPipelineExec`, stock remainder join + adapter
  projection (casts the i64 payload back to Int32), splice replacing the
  emit-projection (everything above preserved byte-for-byte).
- ✅ **Wired** into `FlowQueryPlanner::create_physical_plan` — reconstruct runs
  AFTER the logical optimizer (so it can't be undone), planned with the
  ExtensionPlanner; gated `EMAT_PUSH_PIPELINE=1`, default-OFF path byte-identical.
- ✅ Operator generalized + hot-loop-fixed (PV.3); Plan-sourced payload/membership
  builds; `key_at` widens Int32→i64 so the Int32 o_year payload needs no cast.

### Gates passed
- ✅ **Unit** (5): analyze/classify/reconstruct on real Q08 + reject-non-star +
  **reject Q15 float-keyed pseudo-star**.
- ✅ **Isolated A/A** (`pv3b_validate`, SF=1): reconstruct value-equivalent to the
  plain plan, 22/22, fires Q08.
- ✅ **Triple-walker prod A/A** (`pv3b_prod_validate`, SF=1): full preset path,
  gate OFF vs ON, **22/22 PASS, 0 mismatch**; Q08+Q09 fire, Q15 correctly bails.
  *(This gate caught the Q15 over-fire the isolated gate masked.)*
- ✅ Fires on the PRODUCTION Q08 plan: `EmatPushPipelineExec: builds=[payload,member]`.

## ⛔ Perf gate FAILED — in-plan fused Q08 is +53% SLOWER at SF=10

`pv3b_q08_perf` (SF=10, interleaved, preset path): **stock 185.8 ms vs fused 283.7 ms
= +52.6% SLOWER** — NOT the de-risk's −10.8%. The PV.0/1/2/Phase-0 numbers used
**prebuilt** probes (dims built once, *outside* the timed region), excluding the
dominant in-plan cost of building the dim probes.

Two perf bugs found + fixed via this gate (kept — both real improvements):
1. `LogicalPlanBuilder::join_on` → equi-predicate became a join *filter* →
   **NestedLoopJoinExec** (orders 4.5M × customer 1.5M = trillions of comparisons,
   ~14 min). Fixed to `.join(.., (lkeys,rkeys), None)` → keys in `on` → HashJoinExec.
   (+86% before this.)
2. `build_structure` collected partitions **serially**. Fixed to `tokio::spawn` per
   partition + merge → +86% → +52.6% (recovered 33pp).

The residual +53% is **structural**: the fused op is two-phase (build all dims →
then probe 60M lineitem) vs DataFusion's pipelined build+probe; the materialize-first
latency + CoalescePartitionsExec + build overhead don't pay back at SF=10. The PV.1
−16% was a *fully* hand-fused RG-parallel pipeline (decode+probe+agg, no inter-operator
batch); a single fused-operator splice into the surrounding pull plan doesn't reproduce it.

**Verdict: BUILT + CORRECT (22/22 both A/A gates) + default-OFF (0 risk), but does NOT
deliver the win.** Do NOT default-on. Next (fresh session): samply-profile the 283ms
(build vs probe vs remainder split) → try (a) overlap build with probe start, (b) kill
the CoalescePartitions serialization, (c) fuse the stock-remainder join into the emit.
If those don't close it, the −16% needs the full morsel pipeline, not an operator splice.

## Generality (architect verdict)
Effectively Q08-shaped. Q10 partial (orders-date membership). Q07 BAILS
(cross-group n1×n2 OR → B5). Q17 BAILS (correlated subquery — PV.3 physical may
fuse its part join). Q20 BAILS (semi cascade). Ship default-OFF.
