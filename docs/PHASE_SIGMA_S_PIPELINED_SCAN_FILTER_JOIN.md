# Σ.S — Pipelined scan + filter + join (cascading dynamic filters)

**Status**: Design doc, not yet implemented
**Owner**: TBD
**Estimate**: 1-3 weeks for cascading-bloom MVP; multi-month for full morsel-driven
**Last updated**: 2026-05-24

## Why this lever exists

After [[sigma-q-l13-to-l16-session]] and [[l9-hashset]], the 22-query
SF=10 geomean sits at **0.75 (25% faster than DuckDB)**. The remaining
gaps are concentrated in 6 multi-join queries (Q01 1.05×, Q03 1.03×,
Q05 1.27×, Q06 1.08×, Q07 1.10×, Q08 1.09×, Q17 1.03×). Q05 is the
biggest at +43 ms / 1.27×.

The Q05 fresh-profile + DuckDB plan diff (2026-05-24) showed that
DuckDB closes the gap via a **cascading dynamic-filter chain**, not
a single bloom:

```text
region(ASIA)            → propagates to nation
nation(n_regionkey=2)   → propagates to customer (c_nationkey IN ...)
customer(filtered)      → propagates to orders (o_custkey IN BF(c_custkey))
orders(date+custkey BF) → propagates to lineitem (l_orderkey IN BF(o_orderkey))
```

Each step gets BOTH a min/max range filter AND a bloom filter, pushed
all the way down to the parquet scan, applied BEFORE the join builds.

ematix-flow's [[sigma_q_l9_landed]] L9 sideband already implements
**single-hop** bloom propagation: build side of one HashJoinExec
publishes a bloom that's consumed by the immediate probe scan. What's
missing is **multi-hop** — propagating a bloom built upstream (e.g.
filtered customer) through one or more intermediate joins (customer⋈
orders) down to the next scan (lineitem).

## Why the obvious "lower the L9 ratio gate" doesn't work

Q05 investigation 2026-05-24 measured:

| Config | Q05 ms | vs DuckDB |
|---|---:|---:|
| L9.HashSet baseline (ratio=1024) | 195 | +43 |
| ratio=64 + L9.SelectiveBuild filter gate | 202 | +49 |
| ratio=16 + filter gate | 265 | +112 |
| ratio=0 (always fire) + filter gate | 258 | +105 |

Firing L9 on more joins **regresses** Q05 even with a "build has
FilterExec" gate. The reasons map cleanly:

1. **FK-shape joins (c⋈o, all-supplier→lineitem)** have ~100% bloom
   pass rate by referential integrity. The bloom membership-test is
   pure overhead with zero downstream savings. Filter gate catches
   the worst of these.

2. **Medium-build filtered-FK joins (o⋈l with 2.3M filtered orders)**
   pay 73 ms of bloom probe cost (60M lineitem × 15 ns/probe ÷ 14
   cores) for ~35 ms of downstream savings. Net -38 ms.

3. **Per-row bloom cost is too high** on the current scalar
   sequential-multiply-shift design. DuckDB's bloom probe is ~2-5 ns;
   ours is 15.66 ns. That 3-10× gap is what makes broadcasting blooms
   net-negative for ematix where it's net-positive for DuckDB.

## Two-part plan

The lever decomposes into two independent ships:

### Part A — SIMD-friendly bloom probe (Σ.S.A)

**Goal**: reduce per-probe cost from 15 ns → 2 ns. Standalone win that
also enables Part B.

**Design**: Apache Impala "splash" bloom layout — 256-bit block, k=8
salted hashes per probe, fully data-parallel (no early-out).

Microbench measured on M3 Pro 2026-05-24:

| Workload | Current bloom | Splash bloom | Speedup |
|---|---:|---:|---:|
| Q17-shape (2K keys, 0.1% hit) | 15.66 ns | 1.54 ns | 10.15× |
| Q05-o⋈l-shape (2.3M keys, 15% hit) | 12.60 ns | 1.69 ns | 7.45× |
| FK-shape (1.5M keys, 100% hit) | 5.40 ns | 1.61 ns | 3.36× |
| Q21-shape (500 keys, 0.05% hit) | 12.94 ns | 1.38 ns | 9.36× |

Also halves to tenths the false-positive rate (3-10× lower FP across
shapes) because splash uses 8 high-quality independent salted hashes
vs current's chained multiply with bit-correlation.

**Scope**: ~150 LOC in `crates/ematix-flow-core/src/bloom.rs` —
replace the bit layout + insert + probe (both `might_contain_hash`
and `insert_hash`). Wire format gets bumped (`EBLM0001` → `EBLM0002`)
so distributed deployments staged across versions degrade gracefully
to "no bloom" rather than misreading.

**Risk**: low. Microbench is decisive; the data-parallel structure
auto-vectorises well even before adding explicit NEON intrinsics.

**Acceptance**: 22q SF=10 geomean ≤ 0.75 (no regression) AND Q05
unchanged OR improved when combined with the L9 ratio tuning enabled
by the faster probe.

### Part B — Cascading L9 (Σ.S.B)

**Goal**: propagate build-side blooms through intermediate joins to
distant scans, matching DuckDB's `c_nationkey → c_custkey → o_orderkey
→ l_orderkey` chain.

**Mechanism**: extend the L9 rule to identify **FK chains** in the
plan:

```text
HashJoinExec(A.k_a = B.k_b)             ← inner join 1
  build:   filtered_A   (small)
  probe:   B            (large, has further joins below)
    HashJoinExec(B.k_b' = C.k_c)        ← inner join 2 deeper down
      build:   from-A-derived B subset
      probe:   C scan                    ← we want the A-derived bloom HERE
```

For each HashJoinExec, after L9 builds the bloom on its build side,
ALSO walk **down past the immediate probe scan** to subsequent joins
in the same FK chain and attach the bloom to those probe scans too.
Multiple sidebands per join, multiple consumers per scan.

**Design sketch**:

1. Plan-time analysis: identify FK chains via column-name matching
   (`c_custkey` → `o_custkey` → `o_orderkey` → `l_orderkey`).
2. For each join's build subtree, find downstream joins whose probe
   side reaches a scan transitively connected via the FK chain.
3. Allocate sidebands for both immediate AND downstream probe scans.
4. The `BridgeFilter`'s `build_bitmap` already AND-combines multiple
   predicates — no infra change needed on the consumer side.

**Open questions**:

- How aggressive should FK detection be? Strict (column-name match
  on known TPC-H prefixes) keeps the rule narrow and predictable;
  general (schema-foreign-key declarations) is more correct but
  needs metadata DataFusion doesn't track today.
- Does cascading work through `BuildSideBloomEmitterExec` wrappers?
  Plan-time descent needs to handle the L9-introduced wrappers
  cleanly.
- How to bound the number of cascading sidebands per query? Q05
  has 4 viable cascades; Q07/Q08 have more. Per-query cap of N
  sidebands?

**Risk**: medium-high. Plan analysis is fiddly; needs careful tests
for correctness (wrong cascade → spurious row drops → wrong answer).
The Σ.Q.L14 col_idx bug (Q07 sum 94% wrong, masked by row-count-only
bench) is the canonical "silent correctness" trap to guard against.

**Acceptance**: tpch_validate passes all 22 queries cell-by-cell vs
DuckDB AND 22q SF=10 geomean drops ≥3% vs Part-A-only baseline.

## Out of scope

Full morsel-driven parallelism (DuckDB's pipelined operator model
where every operator processes ~10K-row chunks and stages overlap)
is a multi-month re-architecture of DataFusion's execution model.
Not started here.

The cascading-L9 approach (Σ.S.B) gets us ~80% of the morsel-driven
benefit for the FK-chain shape that dominates TPC-H, at a fraction
of the engineering cost. If post-Σ.S.B benchmarks still show a
significant structural gap on real workloads (not just TPC-H), a
morsel-driven phase can re-open.

## Sequencing

1. **Σ.S.A first** (~3-5 days). Standalone win, unlocks Part B's
   net-positive shapes.
2. **Σ.S.B prototype** (~5-10 days). Hand-curated rule wired to one
   FK chain (Q05's orders→lineitem). Validates the approach.
3. **Σ.S.B general** (~5-10 days). Walk arbitrary plan trees, attach
   sidebands across joins.
4. **Bench gate**: 22q SF=10 ≤ 0.65 OR per-query gain on Q05/Q07/Q08
   ≥ 15 ms.
