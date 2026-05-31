# L9 runtime-bloom key domains — i64 family (shipped), u64 + byte/hash (planned)

The Σ.Q.L9 runtime bloom (`EnableRuntimeBloomSidebandRule` +
`BuildSideBloomEmitterExec` + the `*InBloom`/`*InSet` `ColumnPredicate`s
consumed during the probe-side `EmatixFastParquetExec` decode) skips probe
rows whose join key can't be in the (much smaller, possibly pre-filtered)
build side. Today it is an **i64-domain** structure: `insert_i64` /
`might_contain_i64`, with the probe reading the native `INT64` parquet column.

This doc records the shipped generalization and plans the two domains that
need a *different* structure. The organizing idea: a join key has a **domain**,
and the bloom/set/probe-kernel triplet is selected per domain.

```
KeyDomain ::= I64        -- shipped (KEYS.1)
            | U64        -- planned: unsigned keys that exceed i64::MAX
            | Bytes      -- planned: string / byte-array keys
```

The rule picks ONE domain per join from the two equi-key column types, wires
the matching build emitter + probe predicate, and bails (no-op) if the two
sides can't agree on a domain.

---

## Domain 1 — i64 (SHIPPED, KEYS.1, 2026-05-31)

**Accepts** (helper `build_side_bloom_emitter_exec::widens_to_i64`): `Int8/16/
32/64`, `UInt8/16/32`, `Date32/64`. All widen to `i64` **value-preserving**.

**Why these and not more:** the bloom hashes the widened `i64`. Any type whose
values map injectively into `i64` rides the existing machinery with only a
widen-on-read at the build side; the probe already reads native `i64`.

**Touch-points (all done):**
- Gate: `runtime_bloom_sideband_rule.rs` — `widens_to_i64(l_dt) && widens_to_i64(r_dt)` (was `== Int64`).
- Build emitter: `build_side_bloom_emitter_exec.rs` — constructor accepts the family; the per-batch value-read `cast`s any non-`Int64` key column to `Int64` once (lossless), then the existing bloom/set/range logic is unchanged. The forwarded batch is the original.
- Probe: **no change** — `filter_i64_column_to_bitmap_dense` reads the native `INT64` parquet column (narrowing only changed the *advertised* Arrow type; on-disk is still `INT64`). Verified by `tpch_validate` 22/22 @ SF=10 with `EMAT_DOWNCAST_KEYS=1`.

**Motivation it unblocked:** KEYS.2 advertises `Int32` for narrowed `*key`
columns; the i64-only gate had silently dropped the bloom (Q21 SF=10 +48%).
With the widen, the bloom fires on the `Int32` key; Q21 +48%→+6%, Q03/Q05
flipped +5%→wins, Q18's plan-flip −92% preserved, all 22 still correct.

---

## Domain 2 — u64 bloom (PLANNED)

**Need:** `UInt64` join keys (and any key whose values can exceed `i64::MAX`).
Not in TPC-H, but common in real warehouses: hash-distributed surrogate keys,
Snowflake-style 64-bit sequence IDs, `xxhash`-bucketed keys. A `UInt64` cannot
widen to `i64` (overflow), so it needs its own non-negative domain.

**Design (parallel to i64; the bloom bit-array is identical, only the hashed
value type differs):**
- `BloomFilter::insert_u64(u64)` / `might_contain_u64(u64)` — hash the `u64`
  bytes (same hasher as `insert_i64`; `i64` and `u64` with the same bit pattern
  must NOT collide across domains, but they never share a bloom so it's moot).
- `I64Set` sibling `U64Set` (open-addr `Vec<u64>`), or generalize the set over
  `T: Hash + Eq + Copy`.
- New predicates: `ColumnPredicate::U64InBloom { col_idx, bloom }` +
  `U64InSet { col_idx, set }`.
- Build emitter: a `widens_to_u64(dt)` family = `UInt8/16/32/64` (and,
  optionally, non-negative-proven signed via stats — skip for v1). Cast the key
  column to `UInt64`, then the same insert loop.
- Probe kernel: `filter_u64_column_to_bitmap_dense` — mirror of the i64 kernel,
  reading the parquet column as `u64`.

**Domain-agreement gate:** use the u64 domain only when BOTH equi-key sides are
unsigned (`widens_to_u64` on each) — a signed `Int64` side can hold negatives
that have no `u64` image, so a mixed `Int64⋈UInt64` join must NOT use this
bloom (bail → no bloom, correct-but-unaccelerated). In practice `UInt64⋈UInt64`.

**Effort:** moderate, LOW risk — entirely parallel to the i64 path, touches no
existing i64 behavior. Mostly mechanical mirroring + one rule branch.

**Measurement gate:** needs a `UInt64`-key dataset (synthetic or TPC-DS-style).
Build the infra only when a real u64-key workload is in hand; until then the
i64 family covers every key that arises (TPC-H keys are `Int64`/narrowed
`Int32`, all `widens_to_i64`).

---

## Domain 3 — byte/hash string bloom + probe kernel (PLANNED, larger)

**Need:** string join keys — `Utf8 / Utf8View / Dictionary(_, Utf8)`. In TPC-H
the string joins are tiny dims (`nation` 25 rows, `region` 5) where a bloom
never helps, so this is **not** TPC-H-motivated. It matters for real warehouses
that join large fact tables on natural/string keys (codes, SKUs, account IDs).

**Design (the bit-array is again the same; the hash input is the byte slice).
Like the i64 domain, this ships BOTH an exact set (tiny builds) and a bloom
(larger builds) — `BytesInSet` / `BytesInBloom`, mirroring `I64InSet` /
`I64InBloom` and the existing set→bloom overflow in `BuildSideBloomEmitterExec`:**

- **Exact tiny-build path — `BytesInSet` (implement this, not just the bloom).**
  When the build side has few distinct keys (the common dim⋈fact case — a
  filtered dimension with hundreds/thousands of string keys), an EXACT byte set
  beats a bloom: zero false positives, so the probe drops every non-matching row
  instead of leaking the bloom's FP fraction into the downstream join. Structure:
  `BytesSet` owning its keys (`HashSet<Box<[u8]>>`, or a sorted `Vec<Box<[u8]>>`
  + binary search for cache-friendliness) — build batches are transient, so the
  set must copy the bytes. This is the string analog of `I64Set`.
- **Threshold — by BYTES, not count.** The i64 set uses a fixed count cutoff
  (`EMAT_L9_SET_THRESHOLD`, 32K entries × 8 B). Strings are variable-length, so
  cap on **total interned bytes** (e.g. ≤ a few MB) OR `min(count, byte_budget)`
  — bounds memory regardless of key length. Overflow → drop the set, fall back
  to `BytesInBloom` (exactly the i64 emitter's `local_set = None` overflow path).
- `BloomFilter::insert_bytes(&[u8])` / `might_contain_bytes(&[u8])` — hash the
  slice (ahash/xxhash); the overflow/large-build path.
- New predicates: `ColumnPredicate::BytesInSet { col_idx, set }` (exact) +
  `BytesInBloom { col_idx, bloom }` (probabilistic).
- **Build emitter:** read the build key as `StringViewArray` / `StringArray` /
  `DictionaryArray`; insert each non-null key's bytes into the local set until
  the byte budget overflows, then switch to the bloom (same two-track shape as
  the i64 emitter at `build_side_bloom_emitter_exec.rs`). For dict arrays,
  insert/hash each **dict entry** once and reuse per code (O(|dict|), not
  O(rows)).
- **Probe kernel:** `filter_bytes_column_to_bitmap_dense` — reuse the
  **dict-aware** shape already in `ematix_parquet_bridge::filter_byte_array_to_bitmap`
  (the string-`Eq` pushdown): decode the dict once, evaluate membership
  (`set.contains` for `BytesInSet`, `might_contain_bytes` for `BytesInBloom`)
  per dict entry → a `dict_mask`, then scatter over the index stream. O(|dict| +
  rows), membership paid only |dict| times — the real win on dict-encoded fact
  columns. PLAIN-encoded columns fall back to per-row membership.

**Correctness subtleties:**
- **Normalization must match both sides.** TPC-H `CHAR(n)` is space-padded;
  the string-`Eq` path already trims — the bloom must hash the SAME normalized
  bytes on build and probe or membership silently fails (dropped rows). Mirror
  the existing trim/normalize exactly.
- Completeness is correctness-critical (same as i64): a missed build value →
  dropped probe rows. The build emitter must insert every non-null key; any
  decode/hash error must surface, not silently skip (see the i64 emitter's
  cast-error handling for the pattern).

**Effort:** larger, MEDIUM risk — new predicate + bytes bloom/set + a dict-aware
probe kernel + emitter string/dict handling. But the dict-aware probe kernel is
reusable infra and composes with the existing dict-preserved decode.

**Measurement gate:** needs a large-fact string-FK workload (TPC-DS, or a
synthetic fact⋈dim on a string code). Defer until justified.

---

## Shared refactor (do alongside the first new domain)

Adding a domain currently means parallel code in three places (rule gate,
emitter insert, probe kernel). Before landing U64 or Bytes, factor the per-
domain triplet behind a `KeyDomain` enum so a new domain is a new arm, not a
new pipeline:

```
enum KeyDomain { I64, U64, Bytes }
fn domain_for(build_dt: &DataType, probe_dt: &DataType) -> Option<KeyDomain>;
// emitter: dispatch insert by domain; rule: emit the matching predicate;
// scan: dispatch the matching filter_*_to_bitmap_dense by predicate variant.
```

This keeps the "no query-specific hardcoding" property: the rule reasons about
the key's *domain*, never the query.

## Status / ordering

1. **i64 family — SHIPPED** (KEYS.1). Default-on with the existing L9 gates.
2. **u64** — build when a real `UInt64`-key workload exists. Low risk.
3. **Bytes** — build when a large-fact string-FK workload exists. Medium risk;
   land the `KeyDomain` refactor with it.
