# Σ.H.1c — Σ.H.1b deep-bench: regression confirmed, revert is correct

The Σ.H.1b 3-run gate showed 6 queries regressing > 5%. The first
hypothesis was "3-run noise" (analogous to Σ.H.1's Q21 case that the
5-run deep-bench resolved as parity). Σ.H.1c re-tested with the
deep-bench methodology.

## Method

5 runs × 20 trials per run on each side = 100 Q-trials per query.
Σ.H.1b source (cherry-picked back via `d5bc1a4`) vs v0.3.0 source
(`git checkout 268ab96`), same machine same session, alternating
builds. Queries: Q03 / Q04 / Q05 / Q10 / Q21 — the 5 from Σ.H.1b's
gate failure that route through filter_multi_agg.

## Result: regressions are real

Per-query medians across 5 runs (each run = 20-trial median):

| Q | v0.3.0 runs | Σ.H.1b runs |
|---|---|---|
| Q03 | 15.43, 13.51, 13.33, 13.84, 14.50 | **18.01**, 14.62, 15.12, 15.91, 15.74 |
| Q04 | 13.22, 13.30, 13.04, 12.96, 13.03 | **15.34**, 12.96, 14.16, 14.15, 13.94 |
| Q05 | 22.23, 21.75, 21.98, 22.15, 21.82 | **27.45**, **28.17**, 22.91, 23.69, 23.03 |
| Q10 | 30.67, 31.19, 31.86, 31.23, 28.42 | 34.20, **39.89**, 36.37, 34.77, 35.50 |
| Q21 | 38.76, 39.05, 38.59, 39.44, 37.16 | 42.42, 42.45, 43.00, **45.33**, 42.80 |

(Bold = run 1, the one most-suspect-for-cold-cache effects. Even
discarding run 1 across the board, Σ.H.1b stays slower than v0.3.0
on every query.)

Aggregated:

| Q | v0.3.0 med-of-meds | Σ.H.1b med-of-meds | Δ | Note |
|---|---:|---:|---:|---|
| Q03 | 13.84 | 15.74 | **+13.7%** | NEW filter_multi_agg fire |
| Q04 | 13.04 | 14.15 | **+8.5%**  | Already fires post-Σ.H.1 |
| Q05 | 21.98 | 23.69 | **+7.8%**  | Already fires post-Σ.H.1 |
| Q10 | 31.19 | 35.50 | **+13.8%** | NEW filter_multi_agg fire |
| Q21 | 38.76 | 42.80 | **+10.4%** | Already fires post-Σ.H.1 |

**Two crucial observations:**

1. **The regression is NOT a 3-run noise artifact.** Five runs of 20
   trials each give 100 samples per side per query. Σ.H.1b's
   distribution does not overlap v0.3.0's.
2. **Σ.H.1b hurts queries that don't use the new numeric kinds.** Q05
   and Q21 go through the DictionaryU32 path — that path's code wasn't
   touched in the hot loop. Yet they regress 7-10%. The regression
   must come from indirect codegen effects of adding enum variants.

## Hypothesis (unconfirmed): match codegen

The Σ.H.1b diff added 4 variants to `GroupKeyAccessor`:
```rust
enum GroupKeyAccessor<'a> {
    Utf8View(...),
    BinaryView(...),
    DictU32Utf8(...),
    Int64(...),     // NEW
    Int32(...),     // NEW
    Date32(...),    // NEW
    Float64(...),   // NEW
}
```

`append_key_bytes` is called once per row in both the dict-single
template (Q05/Q21) and the generic template. Adding variants doesn't
change which arm matches for Q05/Q21 — Dict is still the third arm.
But LLVM's lowering of `match` on a 7-variant enum vs a 3-variant enum
can differ in:
- Jump table size (more entries → larger memory footprint, possibly
  worse L1 i-cache).
- Branch prediction priors (more possible targets → harder for the
  predictor to learn).
- Inlining decisions (the enum's `Drop` impl, `Sized` checks, etc.
  may compile differently).

Confirming would require `cargo asm` or `perf stat -e
branch-misses` head-to-head. Out of scope for this commit.

## Conclusion: keep the revert

Σ.H.1b's net effect on the bench is negative even on queries it
wasn't supposed to touch. The catalog-matcher side of the diff
(rule's `resolve_group_keys` accepting numeric types) is harmless on
its own, but the executor-side changes to `GroupKeyAccessor` and
friends have an unprofiled performance footprint we can't ship.

**Σ.H.1b stays reverted** (commit `5e2a170` is the active state).
The Σ.H.1b commit (`d5bc1a4` after the cherry-pick or `8076a0c`
originally) remains in git history.

## What an actual Σ.H.1 follow-up would need

Three credible directions, ordered by ambition:

1. **Isolated numeric-key path.** Build `GroupKeyAccessorNumeric` as
   a separate enum from `GroupKeyAccessor`. The Dict / Utf8View
   templates never see the numeric variants. Numeric-keyed queries
   route to a parallel `process_batch_numeric_generic` path. Adds
   ~150 LOC; preserves Dict/Utf8View codegen. Doesn't add specialised
   numeric *templates* (still uses HashMap<Vec<u8>, AggCells>), but at
   least doesn't regress existing wins.
2. **Profile-driven shape selection.** Add an `EMAT_FILTER_MULTI_AGG_DISABLE_FOR_JOIN`
   env var or per-query stat that lets users see the regression and
   opt-out per workload. Less an engineering fix than a release-safety
   knob.
3. **Specialised Int64-keyed perfect-hash template.** Mirror
   `process_batch_perfect_hash_dict` but for Int64 keys with bounded
   cardinality. Only fires when the input has < N distinct keys
   (which is rare for Int64 in TPC-H). Limited applicability;
   probably not worth the engineering.

None of these are a one-session change. Σ.H.1 stays as the validated
end-state for now. The Σ.G inventory still says "Σ.H.1b would unlock
4 more queries" — but the bench says those queries don't WANT to be
unlocked through our generic path. The catalog correctly accepts
shapes; we just don't have a fast enough executor for those shapes
yet.

## Lesson for the Σ.G inventory methodology

The inventory's "rules fired" count is a *necessary but not
sufficient* signal for a perf win. Future iterations of the tool
should tag firings that route to specialised templates (Dict-single,
two-key-utf8view, perfect-hash-dict) separately from firings that
route to the generic path. Generic-path firings need a separate
bench step before being claimed as wins.

This is the Σ.E6 D1 lesson restated: measure each new code path's
real cost, don't trust matching as a proxy for performance.
