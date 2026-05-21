# Σ.H.1b bench gate — result: **FAILS the per-query gate**

**Run:** 2026-05-20, immediately after `8076a0c` (Σ.H.1b commit).
Methodology: 3-run multi-bench, same machine same session,
Σ.H.1b source vs v0.3.0 source (`git checkout 268ab96 -- crates/`).

## Verdict: do not ship as-is

| Metric | Result | Gate | Pass? |
|---|---|---|---|
| Geomean (Σ.H.1b / v0.3.0) | 1.0122 | ±2% | ✅ |
| Σ.H.1b NEW target Q03 | +11.4% | ≥-5% or ±3% | ❌ |
| Σ.H.1b NEW target Q10 | +0.1% | ≥-5% or ±3% | ✅ |
| Σ.H.1 target Q04 (regressed!) | +6.4% | ≥-5% or ±3% | ❌ |
| Σ.H.1 target Q05 (regressed!) | +16.6% | ≥-5% or ±3% | ❌ |
| Σ.H.1 target Q21 (regressed!) | +2.8% | ≥-5% or ±3% | ✅ |
| Per-query: 6 losses > 5% | Q01 / Q03 / Q04 / Q05 / Q07 / Q14 | — | ❌ |

Σ.H.1b widens GroupKeyKind to accept Int64 / Int32 / Date32 / Float64,
unlocking 4 new TPC-H queries to route through filter_multi_agg
(Q02 / Q03 / Q10 / Q20 per the Σ.G inventory). The 3-run bench shows
those new firings — plus the Σ.H.1 firings from the previous commit —
are **slower than DataFusion's default** in this configuration.

## What probably happened

The empty-MemTable inventory said "+4 new firings." That number was
truthful for the matcher: post-Σ.H.1b the rule now matches plans it
previously rejected. But matching isn't the same as winning. The new
generic-path code in FilterMultiAggSpec (numeric-typed key extraction
+ append_key_bytes per row + finalize() decode) is plausibly slower
than DataFusion's own native HashAggregate for shapes with:

- Numeric group keys (Q03's BIGINT+Date+INT, Q10's BIGINT+DOUBLE+...,
  Q11/Q07/Q08's INT)
- Joins immediately under the aggregate (the Σ.H.1 firings)

The combination of "join output + generic-path multi-agg" appears to
underperform DataFusion's default. Likely contributors:

- DataFusion's HashAggregate has been tuned for years; ours hasn't
  matched in the generic path. Specialised templates beat it on
  Q01-shape, but Q01 has 1-byte Utf8View keys (tiny) and tight loops
  via process_batch_two_key_utf8view. Numeric-key generic-path
  doesn't have a similar template.
- HashJoin output batches may be smaller / less aligned than scan
  batches, weakening per-batch amortisation in the spec.
- The per-batch typed-cols cache (build_typed_cols) is one extra
  hop that DataFusion's default doesn't pay.

## Also: Σ.H.1 wins evaporated

The Σ.H.1 deep-bench showed Q05 at -4.6% (a real win). The Σ.H.1b
bench shows Q05 at +16.6%. That's a 21pp swing in 30 minutes of
real time. Two factors:
- The v0.3.0 Q05 baseline drifted (22.99 → 23.70 ms — 3% session
  drift). Subtract that and Σ.H.1b's Q05 is ~+13% over a "true"
  v0.3.0 baseline. Still bad.
- The Σ.H.1b changes added overhead to the multi-agg hot path even
  for queries that weren't NEW firings. Q05 was already firing
  filter_multi_agg before Σ.H.1b. The extra match arms and key-width
  branching in append_key_bytes / finalize / packed_key_offset
  likely added per-batch overhead.

## Decision options

1. **Revert Σ.H.1b** — keep Σ.H.1 (which had a clean deep-bench).
   The numeric-group-key code stays in git history as scaffolding
   for a future attempt.
2. **Narrow the rule to detect when it'd help.** Hard to predict
   statically. Could add a per-query estimated-row-count threshold,
   but TPC-H is small and the threshold logic itself adds overhead.
3. **Make the numeric group-key path actually fast.** Add specialised
   templates for Int64-keyed group-by (similar to dict_single and
   two_key_utf8view). Probably 2-3 days. Real engineering work, not
   a "cheap unlock."
4. **Investigate per-query cost decomposition** before committing
   to a fix. Profile Q05/Q03/Q07 on Σ.H.1b vs v0.3.0 to find the
   exact slowdown source.

My recommendation: **(1) revert Σ.H.1b** for now. The branch already
has Σ.F + Σ.G + Σ.H.1 (validated). Σ.H.1b ships when it has a
specialised numeric-key template that actually beats DataFusion's
HashAggregate. The current generic-path version is correctness-only,
not a perf path.

## What this validates

- **The bench gate is correctly calibrated.** Inventory said "+4
  firings" → naive read would have shipped. Bench says "6 queries
  regress >5%" → stops the bad ship.
- **Σ.E6 D1 still holds.** Measure first; don't let inventory
  signals masquerade as perf wins.

## Σ.G inventory limitation (worth documenting separately)

The empty-MemTable inventory measures *matcher* behaviour, not
*executor* behaviour. A rule firing tells you the catalog accepts
the shape; it doesn't tell you the routed exec is faster than the
default. Future inventory iterations should add a per-rule "is the
new path actually faster" check, or at minimum tag "new firing"
queries as needing bench-confirmation before claiming a win.
