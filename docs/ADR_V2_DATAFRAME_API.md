# ADR: v2 DataFrame API shape — namespace, laziness, index model (S0.4)

**Status:** Accepted (owner-decided 2026-07-18; index decision **revised
same day** — see Decision #3 and History). Sprint S0.4 of
[`V2_SPRINT_PLAN.md`](V2_SPRINT_PLAN.md); design context in
[`V2_TARGET.md`](V2_TARGET.md) §3.

**Positioning it settles:** ematix.frame is a *faster alternative to
pandas with familiar syntax*, not a bug-for-bug pandas reimplementation.
That framing drives the index decision below.

## Context

v2 ships a pandas-shaped, Rust-backed DataFrame API (`V2_TARGET.md`
Pillar 2). Three shape questions had to be settled before the API is
built (S4/S5) and before the S0.1 shared-plan lowering is designed,
because they change the public surface and the plan-construction path:

1. **Namespace** — where the API lives in the Python package.
2. **Default execution** — eager (pandas feel) vs lazy (engine-native).
3. **Index model** — how faithfully we reproduce pandas' row index.

## Decision

### 1. Namespace — `ematix.frame`

The DataFrame API lives in an explicit `ematix.frame` module
(`import ematix.frame as ef`), separate from the pipeline decorators.
The `import ematix.pandas as pd` migration shim (S7) is a distinct
module layered on top of `ematix.frame`.

*Rationale:* keeps the analytics surface clearly delineated from the
pipeline API; avoids polluting the top-level package; leaves the
top-level namespace free for the shim to imitate pandas cleanly.

### 2. Execution — lazy by default

Operations build a plan; execution happens on a terminal op
(`.collect()`, `.to_pandas()`, `__repr__`, `.head()`). An opt-in eager
mode mirrors pandas for the notebook loop.

*Rationale:* lazy is the whole point of the shared logical plan
(S0.1) — a deferred plan is what lets the ematix CBO / fused kernels /
mesh apply to DataFrame queries. This is the Polars model and matches
the Arrow/push engine. Eager-by-default would forfeit cross-op
optimization unless the user opted into laziness.

### 3. Index model — index-light core (revised)

The `ematix.frame` core is **index-light**, the Polars model:

- Optional **positional `RangeIndex`** only; no mandatory row-label index.
- Binary ops align **positionally** (numpy/Polars semantics), NOT by
  label. `s1 + s2` is a vectorized element-wise op, never a hidden join.
- `.iloc` (positional) is first-class. Label-based `.loc`, `MultiIndex`,
  and label alignment are **not** core semantics.
- Pandas index-faithfulness is pushed to where migration actually needs
  it: the S7 `import ematix.pandas as pd` shim (opt-in, documented
  deltas) and the `.to_pandas()` escape hatch (round-trips to real
  pandas with a real index).

*Rationale:* the pandas row index is the single biggest reason a
columnar frame beats pandas — honoring it forces an **alignment join
before arithmetic**, materializes an index column through every plan
node, and (critically) breaks S0.1 plan-identity: DataFusion has no
index concept, so strict index would inject join/sort nodes that the
equivalent SQL never has, diverging the two surfaces and taxing the
optimizer. Index-light keeps the core fast AND keeps DataFrame plans
byte-identical to SQL. The index-dependent code in real workloads is
usually small-data glue, not the big groupby/join/agg where speed
matters — so we capture the value without the tax, and still serve the
faithful-pandas case via the shim + `.to_pandas()`.

## Consequences

- **M2 (S4/S5) is lighter and S0.1 plan-identity is preserved.**
  Index-light means no index layer to build, no alignment joins to
  inject, and DataFrame plans stay byte-identical to the SQL equivalent
  (the S0.1 gate). The `Frame`/`Series` carries at most an optional
  positional `RangeIndex` — no label/MultiIndex machinery threaded
  through the plan.
- **Positional-alignment semantics must be documented as a pandas
  delta** (S5.5): `s1 + s2` aligns by position, not label. This is the
  headline behavioral difference an ex-pandas user meets; the honest
  cheat-sheet must lead with it. It's a *known, documented* delta, which
  the "faster alternative, not exact match" positioning makes honest.
- **The faithful-pandas burden moves to S7**, opt-in: the
  `import ematix.pandas as pd` shim is where label alignment / `.loc` /
  `MultiIndex` get emulated for drop-in scripts, paid only by code that
  imports it — not by the core engine.
- **Lazy-by-default** means `__repr__` triggers execution — must be
  cheap (bounded `.head()` semantics), and errors surface at the
  terminal op, not at the building line. Document this clearly; it is
  the most common lazy-API surprise.
- **`ematix.frame`** namespace is fixed for the S4 skeleton and the
  S0.1 stub frame; the S7 shim targets it.
- Interop escape hatches (`.to_pandas()`/`.to_polars()`, zero-copy)
  remain — `.to_pandas()` is the faithful-index escape hatch for anyone
  who genuinely needs pandas index semantics.

## History

- 2026-07-18 (initial): index model set to **strict pandas index** for
  maximum drop-in fidelity.
- 2026-07-18 (revised, same day): flipped to **index-light core** after
  weighing the performance thesis. Strict index forces alignment joins,
  materializes an index through every plan node, and — decisively —
  breaks S0.1 plan-identity with SQL (DataFusion has no index concept).
  The goal is a *faster* pandas alternative; the index is the main thing
  that makes pandas slow. Faithful-pandas fidelity is served by the S7
  shim + `.to_pandas()` instead of taxing the whole engine.
