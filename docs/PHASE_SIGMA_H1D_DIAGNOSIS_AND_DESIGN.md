# Σ.H.1d — diagnosis + design

## Diagnosis: where the Σ.H.1b regression actually lives

Three-way comparison (same machine, same session, releases built
sequentially):

| Q | v0.3.0 (n=5) | Σ.H.1b on (n=5) | Σ.H.1b off via env (n=3) | binary cost | exec cost | total |
|---|---:|---:|---:|---:|---:|---:|
| Q03 | 13.84 | 15.74 | 15.10 | **+9.1%** | +4.2% | +13.7% |
| Q04 | 13.04 | 14.15 | 13.80 | +5.8% | +2.5% | +8.5% |
| Q05 | 21.98 | 23.69 | 22.95 | +4.4% | +3.2% | +7.8% |
| Q10 | 31.19 | 35.50 | 31.79 | +1.9% | **+11.7%** | +13.8% |
| Q21 | 38.76 | 42.80 | 40.92 | +5.6% | +4.6% | +10.4% |

**Both costs are real.** The total regression decomposes into two
independent contributors:

### Binary cost (~5% avg)

The Σ.H.1b binary is slower than the v0.3.0 binary on these queries
**even when the rule is runtime-disabled** via
`EMAT_DISABLE_FILTER_MULTI_AGG=1`. The rule never fires; the code
path used is DataFusion's default. Yet Σ.H.1b's binary is 2-9%
slower.

The Q03 case is the smoking gun: +9.1% binary cost on a query that
doesn't fire the rule at all. Something about Σ.H.1b's changes
affects DataFusion's default execution path. Most likely candidates:

- Enum-variant additions to `GroupKeyKind` and `GroupKeyAccessor`
  changed LLVM's match-codegen layout. Even paths that match the
  same arm pay the cost of a different jump table.
- Struct layout changes propagating through any struct that
  contains a `GroupKeyKind`.
- `unwrap_or_default` / `match` exhaustiveness checks rearranging
  hot-path instruction selection.

Confirming the exact cause needs `cargo asm` or `perf stat -e
branch-misses` head-to-head. **For Σ.H.1d we don't need to know
the exact LLVM mechanism — we know the fix shape.**

### Exec cost (~5% avg, up to +11.7% on Q10)

The rule's FilterMultiAggSpec routing is genuinely slower than
DataFusion's default HashAggregate for the newly-accepted shapes:

- Q10 has 7 group keys → our generic-path multi-key hash table
  (`HashMap<Vec<u8>, AggCells>` with per-row key cloning) is much
  slower than DataFusion's optimised aggregate.
- Q03 has 3 group keys (BIGINT + Date32 + Int32) → same story,
  smaller multiplier.
- Q04 / Q05 / Q21 have ≤2 group keys → smaller exec cost (~3%)
  but still present.

## Design: Σ.H.1d fix

Two phases, addressing the two costs independently. Both are
required for a complete fix; either alone leaves money on the table.

### Phase A — isolate the numeric handling (binary-cost fix)

**Principle:** the existing FilterMultiAggSpec and its enums
(`GroupKeyKind`, `GroupKeyAccessor`) must stay byte-for-byte unchanged
in shape and layout. The binary footprint that v0.3.0's compiled
output has must be preserved. Numeric-key handling lives in a
completely separate module.

**Architecture:**

```rust
// Existing — UNCHANGED, do not touch:
pub enum GroupKeyKind { Utf8ViewFirstByte, DictionaryU32 }
enum GroupKeyAccessor<'a> { Utf8View, BinaryView, DictU32Utf8 }
pub struct FilterMultiAggSpec { ... }

// NEW — in fused_aggregate_filter_multi_agg_numeric.rs:
pub enum NumericKeyKind { Int64, Int32, Date32, Float64 }
enum NumericKeyAccessor<'a> { Int64(&[i64]), Int32(&[i32]), Date32(&[i32]), Float64(&[f64]) }
pub struct FilterMultiAggSpecNumeric { ... }    // separate spec
impl AggregateSpec for FilterMultiAggSpecNumeric { ... }
```

**Rule dispatch:**

```rust
fn resolve_group_keys(...) -> ResolvedKeys {
    if all_keys_are_string { ResolvedKeys::String(...) }
    else if all_keys_are_numeric { ResolvedKeys::Numeric(...) }
    else { ResolvedKeys::Mixed }
}

fn try_build_replacement(...) -> Option<ExecPlan> {
    match resolve_group_keys(...) {
        ResolvedKeys::String(specs) => build_with(FilterMultiAggSpec, specs),
        ResolvedKeys::Numeric(specs) => build_with(FilterMultiAggSpecNumeric, specs),
        ResolvedKeys::Mixed => None,  // bail to DataFusion default
    }
}
```

The two specs share no enums. The Dict-single / two-key-Utf8View
paths only see the existing `GroupKeyKind`; their codegen is
unaffected.

**LOC estimate:** ~300 new + ~30 modified in the rule. The new spec
mirrors most of FilterMultiAggSpec but with the numeric accessor.
Templates (perfect-hash, dict-single, two-key-utf8view) are not
duplicated — they're string-key specific.

### Phase B — gate the rule on shape (exec-cost fix)

**Principle:** the catalog rule must only fire when the spec it
routes to actually beats DataFusion's default. We have data on
which shapes win and which lose:

| Shape | Verdict |
|---|---|
| ≤2 string keys | wins (Σ.H.1 deep-bench: Q01/Q04/Q05/Q21 all <0% or parity) |
| ≤2 numeric keys, low-cardinality | unknown — needs bench |
| 3+ keys (any type) | loses (Q03 +4.2%, Q10 +11.7% exec cost) |
| Mixed string + numeric, multi-key | unknown |

**Implementation:** in `try_build_replacement`, after extracting
group keys, gate on:

```rust
if group_keys.len() > 2 {
    return Ok(None);  // bail to DataFusion default
}
```

That's it. 3 lines. Eliminates the Q10-class exec cost regression.

The "unknown" shapes from the table need their own bench-gated
follow-ups. For now, conservative gate ships only what we've measured
as winning.

### What Phase B alone doesn't fix

Phase B without Phase A still pays the binary cost on Q01 / Q04 /
Q05 / Q21 (the queries that DO fire). Each of those is ~5% slower
in the Σ.H.1b binary than in the v0.3.0 binary, regardless of
whether the rule fires.

Phase A alone doesn't fix the +11.7% exec cost on Q10-shape plans
either (mixed/multi-key shapes would still route to our generic
path and be slower).

**Both phases are required.**

## Implementation plan

| Step | Effort | Gates |
|---|---|---|
| Σ.H.1d.1 — scaffold new numeric module + NumericKeyKind enum | ~0.5 day | tests stay green |
| Σ.H.1d.2 — implement FilterMultiAggSpecNumeric (Int64-keyed first) | ~1 day | unit tests |
| Σ.H.1d.3 — extend numeric spec to Int32/Date32/Float64 | ~0.5 day | unit tests |
| Σ.H.1d.4 — rule wires up Numeric dispatch + Mixed bails | ~0.3 day | tests green |
| Σ.H.1d.5 — add `group_keys.len() ≤ 2` gate | ~0.1 day | tests green |
| Σ.H.1d.6 — bench gate: 5×20 deep-bench on Q03/Q04/Q05/Q10/Q21 | ~0.5 day | binary cost ≤2%, exec cost ≤3% per query |

**Total: ~3 days of engineering** for a complete, bench-validated fix.

## Pass criteria for Σ.H.1d.6

1. **Binary cost decomposition:** Σ.H.1d-binary with rule disabled
   vs v0.3.0 must be within ±2% per query. (Tests Phase A.)
2. **Exec cost decomposition:** Σ.H.1d-binary with rule enabled
   vs Σ.H.1d-binary with rule disabled — each query that fires
   must be within ±3% or faster. Queries with >2 group keys
   should NOT fire the rule.
3. **Net regression:** no TPC-H query may regress > 3% vs v0.3.0.
4. **Net gain:** at least 2 queries (the existing Σ.H.1 targets
   Q04 / Q05 / Q21) preserve their <0% wins.

## What if Σ.H.1d.6 still shows binary cost > 2%?

Then the codegen ripple is from something other than enum variant
count alone. Likely candidates to investigate next:

- struct field reordering after adding a `numeric_kind: Option<NumericKeyKind>` somewhere
- new `pub use` re-exports affecting symbol visibility
- the new module triggering a separate codegen unit

Mitigation: keep Σ.H.1d.1 scaffolding minimal — add ONLY the new
module without re-exporting anything from the existing modules.
Profile with `cargo asm` on `process_batch_dict_single` to compare
the Σ.H.1d vs v0.3.0 generated code.

## Status

- Diagnostic complete (this doc).
- Implementation starts with Σ.H.1d.1 in the next commit (scaffold).
- No other work on the branch should land between Σ.H.1d.1 and the
  bench gate (Σ.H.1d.6) — the binary cost is sensitive to anything
  that changes compilation.
