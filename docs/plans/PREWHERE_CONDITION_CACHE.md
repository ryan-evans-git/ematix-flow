# Σ.LM.1 + Σ.CC — PREWHERE short-circuit & query condition cache

Status: **implemented 2026-07-12** (this PR); box A/B rides the next
SF100 definitive run. Both are ClickHouse-derived adoption items from
the 2026-07-12 competitive review, scoped by an audit of what flow
already had.

## Audit result (what already existed)

Flow's fast reader already does the load-bearing half of ClickHouse's
PREWHERE: pushed predicates evaluate via `BridgeFilter::build_bitmap`
(predicate columns decode FIRST, per row group), and projection
columns then masked-decode only surviving rows. RG-level stats skip
also existed. What was missing:

1. **No short-circuit across predicate columns** — an RG whose
   accumulated bitmap went all-zero after predicate 1 still decoded
   predicates 2..N (AND with zero). ClickHouse's multi-step PREWHERE
   stops there; Q06's three-predicate shape wastes two column decodes
   per data-dead RG.
2. **No cross-query reuse** — identical static predicates re-decode
   the same predicate columns every execution (dashboards, retries,
   bench trials 2..N). ClickHouse 25.x's query condition cache is the
   analog.

## Σ.LM.1 — all-zero short-circuit

`build_bitmap`'s fold now breaks after any predicate whose AND leaves
the accumulator all-zero (`bitmap_all_zero`, word-wise scan, ~free
next to one avoided decode). Predicate ORDERING (most-selective
first) was considered and deferred: without trustworthy selectivity
estimates a wrong order regresses, and the short-circuit alone
captures the bulk (the killer predicate is usually the date range,
which the planner already pushes first in TPC-H shapes).

Pinned structurally in `build_bitmap_short_circuits_on_all_zero_
accumulator`: predicate 2 names an out-of-bounds column and blows up
if evaluated — `Ok` proves the elision, the reversed order proves the
tripwire trips.

## Σ.CC — condition cache (`cond_cache.rs`)

Bounded LRU (`EMAT_COND_CACHE_BYTES`, default 256 MiB, 0 disables)
over `build_bitmap` results, keyed
`(path, mtime_ns, len, row group, predicate fingerprint)`:

- fingerprint covers every semantic field of the nine STATIC
  predicate variants (f64 via `to_bits`); the four runtime
  join-artifact variants (`I64InBloom`/`I64InSet`/`StringInBloom`/
  `StringInSet`) return `None` and bypass the cache entirely — a
  bloom is per-query and caching it would silently drop rows on the
  next query.
- file identity in the key means rewrites never false-hit; stale
  entries decay via LRU.
- hit = zero predicate-column decodes for that RG (composes with
  Σ.LM.1, which only helps the miss path).

Pinned in `cond_cache_serves_repeat_without_file_reads`: compute
once, corrupt the file in place with mtime restored (identity key
unchanged) — the repeat must return the identical bitmap without
touching the garbage; an mtime bump then forces a recompute that
fails, proving the hit came from cache.

## Methodology note (site copy carries this)

The condition cache accelerates REPEATED predicates: bench trials
2..N and `med(3-5)` benefit; `first_trial_ms` stays cold-path honest.
Same effect class as a warm page cache. Ships default-ON (bench ==
release).

## Follow-ups (measured, not assumed)

- Page-level dead-page skip inside surviving RGs (CH granule analog
  one level down) — measure after the SF100 A/B says whether RG-level
  short-circuit already saturates the win.
- Selectivity-ordered predicate evaluation — revisit if the box A/B
  shows multi-predicate RGs surviving predicate 1 often.
