# Q9 SF=100 dig — the last genuine engine loss

**Status:** diagnosis complete, lever identified, fix not yet built.
**Date:** 2026-06-24. Box was degraded (a night of SF=100 thrashing → both engines
~1.6× slower than baseline), so trust the **structural facts and row counts**
(box-independent), not the absolute ms/CPU. Re-measure the prize on a cool/idle box.

## The loss

Q9 isolated-warm SF=100: ematix **8356 ms / 59.8 CPU-s** vs DuckDB **6976 ms / 48.4
CPU-s** → **1.20× wall / 1.23× CPU**. The clearest remaining genuine engine loss
after Q10 (compute-excess, not a box artifact — it loses isolated-warm).

Q9 = `part ⋈ supplier ⋈ lineitem ⋈ partsupp ⋈ orders ⋈ nation`, `p_name LIKE
'%green%'`, group by `(nation, o_year)` summing a profit expression. Group-by is
low-card (~175 groups); the cost is all in the 6-way join over lineitem.

## Root cause #1 (headline) — no part-filter pushdown into the lineitem scan

The green-part filter is **not** propagated to the lineitem scan, so ematix
decodes + probes **all 600M lineitem rows** and discards 99.3% in the part join.
DuckDB pushes it down as a **dynamic filter** and processes ~16× fewer rows.

| | ematix | DuckDB |
|---|---|---|
| lineitem rows into the part join | **600.0 M** | — |
| lineitem rows OUT of the scan (post part-filter) | 600.0 M | **35.96 M** |
| part⋈lineitem join | 10.3 s (probe 600M, hit-rate **0.68%**) | folded into scan |
| lineitem decode | 18.9 s (600M) | (dynamic-filtered) |

DuckDB's profile literally shows `optional: l_partkey < …` (a dynamic filter)
pushed onto the lineitem scan, output **35,955,379 rows** — exactly the ~5.4% of
lineitem that references a green part.

**Ematix already has this mechanism — the L9 runtime-bloom sideband**
(`runtime_bloom_sideband_rule`): a filtered build publishes a bloom the probe scan
consumes (it fires on Q17 `filtered_part→lineitem`, Q05 `orders-post-date→lineitem`,
Q07). Q9's `part(LIKE)⋈lineitem` is the **same shape** (filtered part build →
lineitem probe on `l_partkey`) — but the rule's matcher is not selecting it in the
deep 6-way plan (the lineitem scan came out plain, no sideband). **The lever is to
make L9 fire on Q9's part⋈lineitem** (push the ~1.09M green `p_partkey`s as a bloom
into the lineitem scan on `l_partkey`).

Estimated prize: removes the part-join probe (~10 CPU-s) + most of the wasted
lineitem feed; the dominant slice of the 1.23× excess. Needs a cool-box re-measure
to size exactly.

## Root cause #2 (secondary) — builds the LARGER side on the big joins

Ematix's two heaviest joins build the **larger base table** and probe the smaller
intermediate:

| join | ematix build side | build rows | ematix | DuckDB |
|---|---|---|---|---|
| orders ⋈ stream | **orders** | 150.0 M (build 6.8s) | **20.2 s** | 8.4 s |
| partsupp ⋈ stream | **partsupp** | 80.0 M (build 9.9s) | **15.6 s** | (cheaper) |

The probe stream is only **32.64 M** at both joins, so building the 150M / 80M
base table and probing 32.6M is backwards — building the 32.6M intermediate and
probing the base table (or a runtime-bloom on the base scan) would be cheaper.
This is a build-side-selection / join-order lever, independent of #1.

## Recommended next step

1. **Lever #1 first** (the L9 pushdown) — the mechanism exists; the work is
   getting the matcher to select Q9's `part⋈lineitem` and confirming the bloom
   actually prunes the lineitem feed at SF=100. Same class as DuckDB's dynamic
   filter; highest, clearest payoff.
2. Re-measure on a **cool/idle box** (the dig ran degraded) to size the true prize
   before committing — Q9 ratio 1.23× CPU is the gap to close; #1 alone may flip it.
3. **Lever #2** (build-side selection on the orders/partsupp joins) as a follow-on
   if #1 doesn't fully close it.

Not a late-mat shape (low-card narrow group key), so the Q10 machinery does not
apply here — Q9 is a filter-pushdown / join-build problem.
