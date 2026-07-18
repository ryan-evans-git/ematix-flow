# ADR: v2 DataFrame API shape — namespace, laziness, index model (S0.4)

**Status:** Accepted (owner-decided 2026-07-18). Sprint S0.4 of
[`V2_SPRINT_PLAN.md`](V2_SPRINT_PLAN.md); design context in
[`V2_TARGET.md`](V2_TARGET.md) §3.

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

### 3. Index model — strict pandas index

We invest in faithful pandas index semantics: label-based alignment on
ops, `MultiIndex`, `.loc`/`.iloc` distinction, index preservation
through transforms. This is the **maximum-compatibility** choice, chosen
over the lighter "optional index + documented delta" option.

*Rationale (owner call):* migration fidelity is the product thesis —
an analyst's existing pandas code relies on index alignment, and silent
divergence there is worse than slower delivery. Strict index removes a
whole class of "results differ from pandas" surprises.

## Consequences

- **M2 (S4/S5) is heavier and will likely run longer.** Strict index —
  alignment, `MultiIndex`, index preservation — fights Arrow's
  columnar, index-free model and is real engineering. This is an
  accepted, eyes-open cost of the compatibility thesis, not a hidden
  one. The S4/S5 stories and estimates must budget for an index layer;
  flag at the S0→S1 boundary if it threatens the M2 timeline.
  - Concretely: an Arrow `Frame`/`Series` now carries an index
    structure (default `RangeIndex`, explicit label index, or
    `MultiIndex`) that must survive filter/join/groupby/concat and drive
    alignment on binary ops. This likely becomes its own S4 sub-story.
- **Lazy-by-default** means `__repr__` triggers execution — must be
  cheap (bounded `.head()` semantics), and errors surface at the
  terminal op, not at the building line. Document this clearly; it is
  the most common lazy-API surprise.
- **`ematix.frame`** namespace is now fixed for the S4 skeleton and the
  S0.1 stub frame; the S7 shim targets it.
- Interop escape hatches (`.to_pandas()`/`.to_polars()`, zero-copy)
  remain regardless — strict index makes `.to_pandas()` round-trip more
  faithful, which is a bonus.

## Revisit triggers

- If the strict-index layer materially blows the M2 timeline at the
  S4 midpoint, re-open with the owner: ship a lightweight-index M2 and
  fast-follow strict index, vs hold M2. (Do not silently downgrade.)
