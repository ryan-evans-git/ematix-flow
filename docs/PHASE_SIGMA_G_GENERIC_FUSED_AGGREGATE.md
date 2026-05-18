# Σ.G — Generic fused aggregate

**Goal:** retire the 5+ `InjectFusedQN` per-query rules and replace
them with a single shape-parameterised `FusedAggregateExec` + a
cost-aware planner rule that emits it whenever the shape qualifies.

**Why now:** the SF=1 TPC-H triangulation (2026-05-17 AWS campaign)
confirms ematix-flow's fused path beats DuckDB on 11+ queries and
Polars on 12+. The direction works. Per-query rules were the right
*first* move; generalising them is the next.

**Non-goal:** changing what the fused operators do. Same kernels,
same Arrow code paths — the win came from the operators themselves,
not the routing.

## Inventory: what exists today

| Rule | Matches | Operator |
|---|---|---|
| `InjectFusedQ1` | `lineitem GROUP BY (l_returnflag, l_linestatus)` with 8 aggregates | `FusedQ1Exec` |
| `InjectFusedQ3` | `lineitem ⋈ orders ⋈ customer` filter+SUM+SUM | `FusedQ3Exec` |
| `InjectFusedQ5` | 6-way join with region filter | `FusedQ5Exec` |
| `InjectFusedQ6` | `lineitem` filter + `SUM(l_extendedprice * l_discount)` | `FusedQ6Exec` |
| `InjectFusedQ12` | `lineitem` filter + 2× conditional COUNT | `FusedQ12Exec` |
| `EnableDictGroupCountRule` | any aggregate with `Dictionary<UInt32, Utf8>` group key | `DictGroupCountExec` |

Shared: `AggregateShapeConfig` walker recognises group keys,
aggregates, and filters declaratively. Adding rule #7 today is ~30
lines instead of the original ~150. But each rule still produces a
*different* physical operator.

## What "generic" actually means

A single `FusedAggregateExec` parameterised by:

```rust
pub struct FusedAggregateExec {
    input: Arc<dyn ExecutionPlan>,
    shape: AggregateShapeConfig,   // group keys, agg defs, filter
    decode_strategy: DecodeStrategy, // late-mat | dict-preserving | plain
    join_strategy: Option<FusedJoinSpec>,  // for Q3/Q5/Q9 multi-way
}
```

The planner emits `FusedAggregateExec { shape: SHAPE_Q1, ... }` for
any plan whose subtree matches Q1's pattern — regardless of source
table. The shape configs themselves stay declarative; the operator
implementation handles all of them.

## Phases

### Σ.G.1 — Operator audit (~2 days, no behaviour change)

Read all 5 `FusedQN` operators side-by-side. Document the shared
subset:
- Common: decode → predicate → group hash → accumulate
- Differs: which decode kernel, which predicate shape, how many
  aggregates, whether there's a join

Output: `docs/FUSED_AGGREGATE_SHAPES.md` — a table mapping each
operator's hot loop to the abstractions a unified executor would
need. Identifies whether one operator is a strict superset (probably
Q1 — most general aggregate set) or whether they're orthogonal.

### Σ.G.2 — Unify single-table fused aggregates (~4 days)

Q1, Q6, Q12 are all `single-table → filter → group → agg`. Collapse
to one operator first.

- Add `FusedAggregateExec` with a single hot loop that runs the
  filter + group + accumulate, parameterised by `AggregateShapeConfig`
- Update `InjectFusedQ1`, `InjectFusedQ6`, `InjectFusedQ12` to all
  emit the unified operator with their respective shape configs
- Delete `FusedQ1Exec`, `FusedQ6Exec`, `FusedQ12Exec`
- **Gate:** TPC-H SF=1 triangulation, no regression > 5% on Q1, Q6,
  Q12 (use the SF=1 BENCHMARKS.md from the 2026-05-17 campaign as
  oracle: Q1=78ms, Q6=12ms, Q12=23ms)

### Σ.G.3 — Add join shape (~3 days)

Q3 and Q5 add joins. Extend `FusedAggregateExec` with an optional
`FusedJoinSpec` describing the join graph + filter pushdown.

- Q3: 3-way `lineitem ⋈ orders ⋈ customer`
- Q5: 6-way with region filter
- Single join-aware fused operator handles both shapes by config

**Gate:** Q3 (20ms SF=1), Q5 (34ms SF=1) — no regression > 5%.

### Σ.G.4 — Cost-driven rule (~5 days, the speculative bet)

Replace `InjectFusedQ1` / `Q3` / `Q5` / `Q6` / `Q12` with a single
`InjectFusedAggregate` rule that:
1. Walks any `AggregateExec(Partial)` subtree
2. Calls `AggregateShapeConfig::detect(node)` — returns
   `Some(shape)` if the subtree matches a known pattern
3. If matched + cardinality estimate clears threshold, emit
   `FusedAggregateExec { shape, ... }`

The `detect` function is the hard part — it has to handle aggregate
+ filter + join patterns *without* hard-coding TPC-H query shapes.
Most likely outcome: it recognises *families* of shapes (single-table
aggregate, star-join aggregate, conditional-count aggregate) — not
every conceivable shape, but a meaningful coverage.

**Gate:** TPC-H 22 SF=1 BENCHMARKS.md — total median wall time
within 10% of the 2026-05-17 baseline. Per-query regressions > 20%
on any of Q1/Q3/Q5/Q6/Q12 are blocking; non-TPC-H benches (e.g.
`flow_run_bench` synthetic shapes) should *gain* coverage since the
shape-family detector finds more matches than the hand-written rules.

### Σ.G.5 — Q14 + Q9 follow-up (~3 days, opportunistic)

Q14 already has a fused operator (`FusedQ14FullExec` — currently
retired per task #455). Re-add as a `FusedAggregateExec` shape config
matching `lineitem ⋈ part filter + conditional SUM`. Same for Q9
(`PartSupp join with year extract + SUM`).

**Gate:** Both queries already win at SF=1 (Q14: 19ms beat DuckDB
31ms; Q9: 50ms beat DuckDB 71ms) — keep those margins.

## Risks

1. **Hot-loop regression** — bundling 5 specialised operators into 1
   risks the compiler not specialising as aggressively. Mitigation:
   per-shape codegen via `#[inline(always)]` + monomorphised dispatch
   keyed on a `const SHAPE: u8` discriminant. If LLVM doesn't specialise,
   fall back to per-shape `match` arms inside the hot loop (the
   approach polars uses).
2. **Shape-detection false positives** — `AggregateShapeConfig::detect`
   firing on a non-TPC-H plan that doesn't actually benefit. Mitigation:
   feature-gate cost-driven dispatch behind `--features adaptive_fused`
   for the first 2-3 weeks; default off, opt-in via session config.
3. **TPC-H regression** — by definition the hand-tuned `FusedQ1Exec` is
   as fast as the generic version *can* be at the same shape. If we
   lose 5-10% by going generic, that's "we made the engine more honest
   at a small cost." Acceptable. Losing 20%+ is not.
4. **Σ.G.4 doesn't generalise** — the planner rule turns out to need
   a per-shape lookup table anyway. Mitigation: even if cost-driven
   dispatch falls short, the unified `FusedAggregateExec` from Σ.G.2-3
   is still a clean code-size + maintainability win. Σ.G.4 is the
   *stretch* phase; G.2-3 stand on their own.

## What we publish

- `docs/FUSED_AGGREGATE_SHAPES.md` — the shape catalog (a reference
  doc that grows over time, like a query optimiser's pattern library)
- A README section: "Fused execution: from query-specific to
  shape-driven" — addresses the "is this still bandaid" critique
  honestly
- Updated TPC-H BENCHMARKS.md if numbers shift

## What we DON'T do in this phase

- **Generic vectorised aggregate at the kernel level** — that's
  Photon's 5-year project. Σ.G stops at "shape-parameterised
  operator + cost-driven dispatch". The kernels stay shape-specific.
- **Cardinality estimation infrastructure** — we use simple
  selectivity heuristics for Σ.G.4 (e.g. "filter selectivity > 0.1
  enables fused path"). Real CBO is its own initiative.
- **Aggregate spilling, AQE-style adaptive replans** — separate
  follow-ups; the fused path runs in-memory only for now.

## Decisions to lock before starting

| # | Question | Default |
|---|---|---|
| 1 | When does Σ.G.2 fold in `EnableDictGroupCountRule`? | Σ.G.3 or later — dict-preservation is orthogonal, can stay separate until G.4. |
| 2 | Do we keep per-rule names as deprecation aliases? | One release cycle — `InjectFusedQ1` becomes an alias for `InjectFusedAggregate { shape: SHAPE_Q1 }`. Delete after Σ.G.5. |
| 3 | Where does `AggregateShapeConfig::detect` live? | New module `crates/ematix-flow-core/src/planner/fused_shape.rs`. Pure function — no global state, easy to test. |
| 4 | Test strategy | Same fixture-based oracle tests we use for the rules today; add a "detect" unit test per shape that exercises the matcher in isolation. |

## Engineering total

Σ.G.1 + G.2 + G.3 = **~9 working days** for the deterministic part.
Σ.G.4 + G.5 = **~8 working days**, with G.4 carrying real risk of
extending another week if shape detection turns out to be hard.

Worth doing once the current AWS campaign closes out (i.e. the SF=1
+ SF=10 BENCHMARKS.md are in main) so we have a clean oracle.

## Locking sequence

1. **Land current AWS campaign cleanup** — SF=10 re-run (next
   campaign), bench.sh fixes ([[project_aws_prebuilt_target_follow_up]])
2. **Σ.G.1 audit** — read-only, no code change. Output a shape catalog.
3. Decide whether to commit to Σ.G.2-5 based on what the audit reveals.
   If 4 of 5 operators are basically identical, G.2 is a single-day
   unify. If they're heterogeneous, G.1's audit might recommend
   keeping them separate.
