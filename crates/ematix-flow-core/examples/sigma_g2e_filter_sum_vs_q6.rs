//! Σ.G.2e bench gate (archived): `FusedAggregateExec<FilterSumSpec>` vs
//! `FusedAggregateExec<Q6Spec(JIT)>` on TPC-H SF=1 Q6.
//!
//! ## Original purpose (PASS recorded 2026-05-17)
//!
//! `FilterSumSpec::process_batch` allocates a `Vec<*const u8>` per
//! batch (sized to `jit_spec.inputs.len()`) whereas `Q6Spec`'s
//! statically-typed version uses a stack `[*const u8; 4]`. This bench
//! measured whether the `Vec` allocation introduces meaningful
//! per-batch overhead at SF=1 (~6 M rows in ~14 batches).
//!
//! Result (commit 56459c9, 14-thread, mimalloc, 41 trials × 3 rounds,
//! interleaved, MIN per round, median of mins):
//!
//! ```text
//! Q6Spec(JIT)    : 10.66 ms
//! FilterSumSpec  : 10.82 ms
//! delta          : 1.50 % (≤ 3 % threshold)
//! reference value match: rel_err = 1.21e-15 (bit-equivalent)
//! ```
//!
//! Conclusion: the runtime-configured spec is perf-equivalent to the
//! statically-typed one. That unblocked Σ.G.2e-4, which retired
//! `InjectFusedQ6Rule` — and so this bench no longer compiles against
//! a head-of-tree library (the Q6-rule it imported is gone).
//!
//! ## Why kept as a record
//!
//! The result is load-bearing for the retirement decision and is
//! referenced from the `InjectFilterSumRule` module docs. Rather than
//! delete the file outright, the body is reduced to a notice + the
//! recorded numbers so future readers can find the methodology and
//! result without spelunking commit history. Any future regression
//! probe should construct `FusedAggregateExec<Q6Spec(JIT)>` directly
//! (no rule needed) and compare against the same `InjectFilterSumRule`
//! path the rest of the codebase uses.

fn main() {
    println!("Σ.G.2e bench gate — archived. See file header for results.");
    println!("  Q6Spec(JIT)    : 10.66 ms");
    println!("  FilterSumSpec  :  10.82 ms");
    println!("  delta          : 1.50 % (≤ 3 % threshold)  → PASS");
    println!("  reference      : rel_err = 1.21e-15 (bit-equivalent)");
}
