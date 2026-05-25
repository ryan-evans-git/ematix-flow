# Sidecar-index integration — read-path + adaptive auto-creation

**Status:** active
**Created:** 2026-05-24
**Branch policy:** one PR per story (see "PR shape" per story); bundled per the user's "fewer, larger PRs" rule.
**Upstream gate:** ematix-parquet **v0.16.0** (`claude/release-v0.16.0` → tag pending). Until v0.16.0 is on crates.io, this work pins via `[patch.crates-io]` to the local sibling repo. Per `feedback_patch_crates_io_version_match.md`: bump `ematix-parquet-* = "0.16"` in `crates/ematix-flow-core/Cargo.toml` and verify `Cargo.lock` after the bump.

---

## Summary

Integrate ematix-parquet's new sidecar-index capability (sorted INT32/INT64/BYTE_ARRAY, page-Bloom, composite leading-prefix, inverted text — file `<source>.parquet.idx`, fingerprint-bound to its source's footer) into ematix-flow in two phases:

- **Phase 1 (read-side, mechanical, ~1–2 weeks):** `EmatixFastParquetTableProvider` discovers and consumes pre-existing sidecars; works on read-only datasets; defaults to off behind `EMAT_SIDECAR_READ=1` and flips to on once SF=1+SF=10 22q geomean is neutral or better.
- **Phase 2 (adaptive auto-creation, differentiator, ~3–4 weeks):** observer probe watches predicates, scorer ranks index candidates, background builder writes sidecars when the source prefix is writable; the read-side picks them up on subsequent runs. Persisted catalog (SQLite, alongside the existing `~/.ematix/workload.db`) survives process restart.

This is the same architectural arc as Σ.L adaptive-runtime: post-query probe → workload log → speculative resolver → wire-up. Sidecars extend it from "what *decode shape* should we use" to "what *artifact* should we materialise on disk."

**Why now:** ematix-parquet's bench numbers (26–40× equality, 9.5× @ 5% range, crossover ~60% selectivity) translate directly to Q14 / Q17 / Q19 — the queries where Σ.Q parity work has hit decoder + join-reorder walls. Sidecars sidestep both: a sorted index on `lineitem.l_partkey` makes Q14 row-group selection a 1-of-N lookup, not a full scan.

**Non-goals (Phase 1+2):**
- Iceberg write-side `attach_extension` / manifest commit. (Phase 3 candidate; see Open question OQ-IB.)
- Async / `object_store` sidecar I/O. (Codec is sync-only today per upstream §"What's not yet supported"; we mirror that limit.)
- New index types beyond what ematix-parquet v0.16.0 ships. Float / FIXED_LEN / multi-token text are upstream gaps.
- Distributed index distribution across the Flight peer mesh. Worker nodes discover sidecars from their own local view of the object store; coordinator does not push sidecar bytes over Flight.

---

## Active phase + story

- **Phase 1 — read-side consumption** is `[ACTIVE]`.
- **Story 1.1 — discovery + planner hook** is `[ACTIVE]`.

---

## Hard constraints (carried over from request, restated for the implementer)

1. **No new `PhysicalOptimizerRule`** — Σ.K.2 / `project_optimizer_codegen_sensitivity.md` show +5–8% geomean tax from LLVM codegen perturbation on each added rule, before the rule does any work. Sidecar planner hook lives in a **pre-planning helper** (`dict_routing.rs` shape) and/or stamped onto the `EmatixFastParquetTableProvider` itself.
2. **Pre-existing-dataset safe.** Read-path is permission-free. Auto-builder degrades gracefully when source prefix isn't writable — surfaces a "would-help" diagnostic instead of erroring.
3. **No TPC-H-specific hardcoding** (`feedback_no_tpch_hardcoding.md`). Candidate scoring is shape-based: predicate kind × column cardinality × selectivity × build-cost. TPC-H is the validation workload, not the target.
4. **Object-store aware**: local-FS first; cloud (`s3://`, `gs://`) flagged per milestone. Atomic-rename semantics differ; we defer the cloud write-side until upstream ships `ParquetIndex::open` over `object_store`.
5. **Fewer, larger PRs** (`feedback_fewer_prs.md`): each story below is one PR unless gated on perf / data / an external dependency.
6. **TDD** (`feedback_tdd.md`): each story names the failing test that lands first.

---

## Open questions + decisions to make explicit

These are where the user's call shapes the work. Tagging each so the plan body can cross-reference.

### OQ-CACHE: Where does the sidecar discovery cache live?

Three candidates:

| Option | Pro | Con |
|---|---|---|
| **A. Process-wide LRU** keyed by `(canonical_source_path, source_footer_fingerprint)` — mirrors `parquet_decode_cache.rs` / `RowGroupDecodeCache` shape. | One copy across all sessions; matches existing Σ.O.c.1 cache pattern. | Cross-session sharing is wasted if sessions hit disjoint files. LRU eviction may unload an index that's about to be reused. |
| **B. `SessionState` extension** | Mirrors `ContextBlooms` (`project_sigma_j2b_v_landed.md`); per-query lifetime — no stale-fingerprint risk across sessions. | Re-opens sidecars per session; loses warm-cache wins between consecutive queries in the same dashboard / bench loop. |
| **C. Stamped onto `EmatixFastParquetTableProvider`** | Provider already opens the source's parquet footer at `try_new`; opening the sidecar in the same path is trivially zero-extra-cost. | Provider is per-registered-table; same file registered twice (different aliases) opens the sidecar twice. |

**Recommendation: A + C.** Provider does *discovery* (does a sidecar exist? is it fresh? what indexes does its manifest carry?) at `try_new` — that result is cheap (one file `stat` + parquet-footer open + JSON parse, sub-millisecond) and bound to the provider's lifetime. The *opened `ParquetIndex` handle itself* (which holds the sidecar's parquet file open + its decoded manifest) lives in a process-wide LRU. Two providers pointing at the same physical file share one handle.

**Resolution required before Story 1.1 lands.** Marked as a decision point inside the story.

### OQ-CATALOG: Catalog format for the observer's predicate histogram

Three candidates:

| Option | Notes |
|---|---|
| **A. Extend `WorkloadLog` SQLite at `~/.ematix/workload.db`** | Already in-process, already concurrent-read-safe (per `workload_log.rs`). Add two tables: `predicate_observations` + `sidecar_candidates`. Lowest new infrastructure. |
| **B. JSON-on-disk** per source prefix (`<prefix>/.ematix-sidecar-catalog.json`) | Co-located with data; survives moving a prefix around. But: write-conflict-prone, and our object-store backend doesn't have an atomic-rename primitive for cloud yet. |
| **C. Net-new catalog crate** with pluggable backends | Premature; we have one consumer. |

**Recommendation: A.** Reuses Σ.L.2 infrastructure (`rusqlite` is already a workspace dep; concurrency story is solved). The JSON-co-located approach (B) becomes attractive only if multiple ematix-flow processes on different machines need to share the catalog — that's a Σ.B distributed concern, not a single-node one.

**Resolution required before Story 2.1 lands.**

### OQ-SEL-GATE: Selectivity gate placement — plan time vs. execute time

Upstream confirms the crossover is **~60% selectivity** (sidecar starts losing). Two ways to gate:

- **Plan-time gate** — use `IndexSummary.range_overlap` (the cheap manifest-level estimator) to decide *before* opening the sidecar parquet file. Lower fixed cost; but coarse — overlap is a max bound, not the actual selectivity. May open sidecars that then lose.
- **Execute-time gate** — open the sidecar, probe it (`bloom_probe` or count surviving keys via the sorted index's first lookup), then either continue or abandon for a full scan. Tighter but pays the sidecar-open cost on losers.

**Recommendation: both, staged.** Story 1.2 ships plan-time only (cheap, conservative — only fires when predicted selectivity is ≤30%, well below the crossover). Story 2.3 (after the observer is logging real selectivity outcomes) adds execute-time fallback that updates the workload log so the plan-time predictor improves.

**Resolution decision needed at Story 2.3 entry, not now.**

### OQ-DEMOTE: How does a losing index get demoted?

When the observer notes that "sidecar X was opened on Q-shape Y, then we bailed mid-execute," the scorer should reduce X's score. Three options:

- **Per-query demotion log**: `(sidecar_fingerprint, predicate_shape) → outcome`. Persistent; survives process restart.
- **In-memory cooldown**: skip sidecar X for the next N queries. Resets on restart. Fast but loses signal.
- **Hard delete**: drop the sidecar file. Aggressive — loses the build cost permanently.

**Recommendation: per-query demotion log (option 1).** Lives in the same SQLite catalog as candidates. After 3 consecutive losses, the scorer marks the candidate `demoted` and the planner skips it; a new query shape can re-promote.

**Hard delete (option 3) is never automatic.** The Web UI's "Indexes" tab exposes a delete button.

### OQ-IB: Iceberg-managed vs plain-parquet tables — what's the v1 cutoff?

ematix-parquet v0.16.0 ships `ematix-iceberg` with `attach_extension` write-path. But ematix-flow doesn't currently read Iceberg tables (search confirms: iceberg appears in `docs/ROADMAP.md` planning but no code path consumes it).

**Recommendation: scope Phase 1 + Phase 2 to plain-parquet only.** Iceberg becomes Phase 3 — and the gate for Phase 3 is "ematix-flow has an Iceberg `TableProvider` to extend in the first place." This is *also* a path-of-least-resistance choice: file-level pruning via `IndexSummary` only matters at >1000-file table scale, which the project doesn't currently target.

**Phase 3 is not in this plan.** Captured as a follow-on note at the bottom.

### OQ-PERM: What's the diagnostic when source prefix is read-only?

When the auto-builder identifies a candidate but can't write the sidecar (S3 prefix without write IAM, read-only mount, etc.):

- Surface in `WorkloadLog` table `sidecar_candidates.build_outcome = 'permission_denied'`.
- Web UI "Indexes" tab shows a "would help (read-only)" badge with the predicted speedup.
- CLI `flow indexes suggest` command dumps the same list as a manual rebuild recipe (`for file in ...; do ematix-parquet build-index ...; done`).

This is the differentiator: even when we *can't* build, we tell the user what they're leaving on the table.

### OQ-V1-TYPES: Which index types ship in Phase-1 read-side v1?

Upstream v0.16.0 supports: sorted INT32 / INT64 / BYTE_ARRAY; page-Bloom INT64; composite-prefix INT64×INT64; inverted text (BYTE_ARRAY, whitespace-lowercase tokenizer).

**Recommendation: all five in read-side v1.** The read-path code is uniform — `ParquetIndex::open` returns an opaque handle, and the type dispatch happens at predicate-match time. The marginal cost of supporting page-Bloom vs sorted is a few lines of match-arm. **Auto-builder (Phase 2) ships sorted-only in v1**; page-Bloom / composite / inverted are Phase 2.5 (story 2.5) because their "is this a good candidate?" scoring is harder.

---

## Phase 1 — Read-side consumption [ACTIVE]

**Goal:** an existing TPC-H SF=10 sidecar on `lineitem.l_partkey` (built manually via the upstream CLI) is consumed automatically by an unmodified Q14 query and runs Nx faster. Fingerprint mismatch falls back cleanly with a logged warning.

**Estimated effort:** 1–2 weeks.

**Bundle:** Stories 1.1 + 1.2 ship in one PR (discovery is dead weight without the planner hook that consumes it). Story 1.3 (bench-gate) is the same PR's CI gate. Story 1.4 (cleanup, env-flag flip) is a separate small PR after a week of soak.

### Story 1.1 — Sidecar discovery in `EmatixFastParquetTableProvider` [ACTIVE]

**Status:** `[ACTIVE]`

**Failing test (TDD anchor):**
- `crates/ematix-flow-core/tests/sidecar_discovery.rs::discovery_finds_existing_sidecar` — write a small parquet via `parquet-rs`, build a sidecar via `ematix-parquet-codec::IndexBuilder`, open the provider, assert `provider.sidecar_handle().is_some()` and that the handle's manifest reports the expected index name.
- `discovery_skips_when_sidecar_absent` — same shape, no sidecar build; assert `handle.is_none()` and no error.
- `discovery_falls_back_on_fingerprint_mismatch` — build sidecar, mutate source (write a new parquet at same path), assert provider opens cleanly with `handle.is_none()` and emits a warning trace.

**Tasks:**
- [ ] **OQ-CACHE resolution lands first.** Decision rendered in `crates/ematix-flow-core/src/sidecar_cache.rs` module docstring before any code.
- [ ] Add `sidecar_handle: Option<Arc<SidecarHandle>>` field to `EmatixFastParquetTableProvider`. Populated in `try_new` via `discover_sidecar(&path, &source_metadata)`.
- [ ] `SidecarHandle` wraps `ematix_parquet_codec::index::ParquetIndex` + the parsed manifest. Cheaply cloneable (`Arc` interior).
- [ ] `discover_sidecar` resolves the sidecar path as `<source>.parquet.idx` (matches upstream §"Pattern 1"). Object-store sources (`s3://`, `gs://`) **return None and log "deferred to Phase 1.5"** — sync codec API can't open object-store paths today.
- [ ] Process-wide `SidecarHandleCache` keyed by `(canonical_path, footer_fingerprint)`. Default capacity 64 handles, no time-based eviction (handles are sub-MB; LRU only if cap exceeded).
- [ ] Env flag `EMAT_SIDECAR_READ=0` short-circuits discovery to return None — kill-switch for the worst case.
- [ ] Per `feedback_recommend_next_step.md`: PR description's "Next:" line points at Story 1.2.

**PR shape:** Bundled with Story 1.2 (one PR).

### Story 1.2 — Planner hook that picks the indexed read path

**Status:** `[ ]`

**Failing test:**
- `crates/ematix-flow-core/tests/sidecar_planner.rs::eq_predicate_picks_indexed_path` — register a provider with a known sidecar, plan `SELECT extprice FROM t WHERE customer_id = 42`, assert that the resulting physical plan emits the indexed read (proxy: a new `metrics().num_sidecar_lookups` counter > 0 after `collect`).
- `range_above_threshold_picks_full_scan` — same setup, predicate selecting 80%+ of values; assert `num_sidecar_lookups == 0` and `num_full_scan_rgs == N`.
- `predicate_on_unindexed_column_falls_back` — predicate on a column with no sidecar coverage; assert full scan, no warnings.

**Tasks:**
- [ ] New module `crates/ematix-flow-core/src/sidecar_planner.rs` — pre-planning helper, **not** an optimizer rule (cf. hard constraint #1 + Σ.K.2 pattern).
- [ ] `pick_sidecar_index(&BridgeFilter, &SidecarHandle) -> Option<SidecarPlan>` — given the table-provider's already-extracted `BridgeFilter` and the sidecar handle's manifest, picks the best matching index or returns None.
- [ ] Matching rules:
  - `Eq(col, literal)` → sorted index on `col`, OR page-Bloom on `col`.
  - `Range(col, lo, hi)` → sorted index on `col` only.
  - `(Eq(a) AND Eq(b))` → composite-prefix index on `(a, b)`.
  - `Contains(col, token)` on Utf8/Utf8View → inverted-text index on `col`.
- [ ] Plan-time selectivity gate (OQ-SEL-GATE, plan-time half): if `BridgeFilter.estimate_pass_rate()` > 0.30, return None (full scan wins above the safety margin to the 0.60 crossover).
- [ ] `EmatixFastParquetExec::execute` checks `sidecar_plan` before dispatching to its existing decode path. When `Some(plan)`, calls `idx.read_column_*_masked_into(...)` for the projected columns; produces a single `RecordBatch` per surviving row group.
- [ ] **Fallback contract:** any `Err` from `idx.read_*` (other than `SourceFingerprintMismatch`, which can't happen mid-execute because discovery already validated) logs a warning and falls back to the non-indexed `execute` path. No panics — `feedback_no_tpch_hardcoding.md` shape: degrade, don't lock in.
- [ ] New metrics: `num_sidecar_lookups`, `num_sidecar_hits_skipped_rgs`, `num_sidecar_fallbacks`. Surface via standard `BaselineMetrics`.

**PR shape:** Same PR as Story 1.1. Combined commit count ~15-20.

### Story 1.3 — Bench gate + acceptance test

**Status:** `[ ]`

**Failing test:**
- `crates/ematix-flow-core/examples/bench_sidecar_q14_sf10.rs` — checked into the tree as a runnable example, not a `criterion` bench. Builds an `lineitem.l_partkey` sidecar via the upstream `IndexBuilder` (TDD: this script *also* exercises the upstream API we depend on), runs Q14 SF=10 5× with and 5× without `EMAT_SIDECAR_READ=1`, asserts indexed median is ≤ 60% of baseline median (the bench-gate is a target, not the upstream 26-40× number, because Q14's scan is only part of its total wall-time).
- 22q SF=10 regression check: with `EMAT_SIDECAR_READ=1` and *no* pre-built sidecars present, geomean must stay within ±1% of the current 0.80 baseline (`project_sigma_q_l13_to_l16_session.md`). The discovery path on a sidecar-less dataset is one `stat` per table — should be sub-millisecond.

**Tasks:**
- [ ] Add `bench_sidecar_q14_sf10.rs` example.
- [ ] Wire CI smoke (gate at SF=1 only — SF=10 build is too slow for CI; the SF=10 gate is operator-run before flipping the default).
- [ ] Per the **acceptance gate** at the top of this plan: explicitly run "SF=10 sidecar on `lineitem.l_partkey`, Q14 ≥2× faster, fingerprint-mismatch falls back cleanly" and record numbers in `docs/PHASE_SIDECAR_READ_BENCH.md`.

**PR shape:** Same PR as Story 1.1 / 1.2.

### Story 1.4 — Default-on flip + soak

**Status:** `[ ]`

**Failing test:**
- 22q SF=1 + SF=10 geomean check with `EMAT_SIDECAR_READ` unset (i.e., default on) must match the bench-gate from Story 1.3 — no regression vs. baseline.

**Tasks:**
- [ ] After 1 week of soak (operator-run weekly 22q with `EMAT_SIDECAR_READ=1`), flip the default in `EmatixFastParquetTableProvider::try_new`.
- [ ] Keep `EMAT_SIDECAR_READ=0` as a kill-switch indefinitely (mirrors `EMAT_RG_DECODE_CACHE` pattern from `project_sigma_oc2_provider_landed.md`).

**PR shape:** Separate small PR a week after Story 1.3 merges.

---

## Phase 2 — Adaptive auto-creation

**Goal:** after running TPC-H 22q 3× against a fresh dataset with no pre-existing sidecars, the auto-builder has created sorted indexes on the obviously-useful columns (likely `customer.c_custkey`, `orders.o_orderkey`, `lineitem.l_partkey`, `lineitem.l_suppkey`). A 4th run uses them — measurable speedup on Q14 / Q17 / Q18 / Q19. Catalog persists across process restart; observer doesn't double-count.

**Estimated effort:** 3–4 weeks.

**Bundle:** Stories 2.1 + 2.2 + 2.3 ship in one PR (the observer is dead weight without the scorer and builder). Story 2.4 (Web UI surface) is one PR. Story 2.5 (extended index types) is one PR per type, in priority order.

### Story 2.1 — Predicate-observer probe

**Status:** `[ ]`

**Failing test:**
- `crates/ematix-flow-core/tests/sidecar_observer.rs::observer_records_eq_predicate` — register a provider, run `SELECT * FROM t WHERE customer_id = 42`, assert the workload log has one row in `predicate_observations` with `(table='t', col='customer_id', op='eq', literal_card=1)`.
- `observer_dedupes_within_query` — same predicate referenced twice in a query (e.g., `WHERE x = 1 AND (x = 1 OR y = 2)`) only counts once.
- `observer_survives_process_restart` — open log, write observation, drop, reopen, assert observation persists.

**Tasks:**
- [ ] **OQ-CATALOG resolution lands first.** Decision rendered in `workload_log.rs` module docstring; new schema bumps `schema_version`.
- [ ] Extend `WorkloadLog` with two tables:
  ```sql
  CREATE TABLE predicate_observations (
    table_name TEXT NOT NULL,
    column_name TEXT NOT NULL,
    op TEXT NOT NULL,  -- 'eq' | 'range' | 'contains' | 'composite_eq'
    estimated_selectivity REAL,
    observation_count INTEGER DEFAULT 1,
    last_seen_unix INTEGER,
    PRIMARY KEY (table_name, column_name, op)
  );
  CREATE TABLE sidecar_candidates (
    table_name TEXT NOT NULL,
    column_name TEXT NOT NULL,
    index_kind TEXT NOT NULL,  -- 'sorted_i64' etc.
    score REAL NOT NULL,
    state TEXT NOT NULL,  -- 'candidate' | 'building' | 'built' | 'demoted' | 'permission_denied'
    build_outcome TEXT,
    last_score_update_unix INTEGER,
    consecutive_losses INTEGER DEFAULT 0,
    PRIMARY KEY (table_name, column_name, index_kind)
  );
  ```
- [ ] Observer is a pre-planning hook on the same path as `dict_routing::analyse_dict_arrival_for_sql`. Walks the `LogicalPlan` for `Filter` nodes, extracts `(table, col, op)` triples, records.
- [ ] **Selectivity estimate at observation time** is the predicate's `BridgeFilter::estimate_pass_rate` value — not the actual runtime selectivity (which Story 2.3 layers on top).
- [ ] Env flag `EMAT_SIDECAR_OBSERVE=0` disables observation entirely.

**PR shape:** Bundled with Stories 2.2 + 2.3.

### Story 2.2 — Candidate scorer

**Status:** `[ ]`

**Failing test:**
- `crates/ematix-flow-core/tests/sidecar_scorer.rs::scorer_picks_lineitem_partkey_after_q14_workload` — seed the workload log with 10 observations of `(lineitem, l_partkey, eq)`, run `score_all`, assert `('lineitem', 'l_partkey', 'sorted_i64')` is the top-scored candidate.
- `scorer_skips_tiny_tables` — observations on `nation` (25 rows) score zero (build cost dwarfs benefit at this size; mirrors `dict_routing::MIN_ROWS_FOR_DICT = 100_000`).
- `scorer_skips_high_selectivity` — observations with `estimated_selectivity > 0.30` score zero.

**Tasks:**
- [ ] `crates/ematix-flow-core/src/sidecar_scorer.rs` — `score_candidate(observation, source_stats) -> f64`.
- [ ] Score formula (deliberately simple — TDD-friendly, no TPC-H tuning):
  ```
  score = expected_speedup × observation_count × not_demoted_penalty
  expected_speedup = max(0, 1.0 - (selectivity / 0.60)) × 25.0  // upstream bench peak
  not_demoted_penalty = 0.0 if state='demoted' else 1.0
  ```
- [ ] Hard cutoff: `source_num_rows < MIN_ROWS_FOR_SIDECAR (= 100_000, mirrors dict gate)` → score = 0.
- [ ] Hard cutoff: `selectivity > 0.30` → score = 0.
- [ ] `pick_top_k(limit_concurrent_builds = 2)` returns the top-2 candidates not yet in state `building`.

**PR shape:** Bundled with Story 2.1 + 2.3.

### Story 2.3 — Background builder + execute-time selectivity feedback

**Status:** `[ ]`

**Failing test:**
- `crates/ematix-flow-core/tests/sidecar_builder.rs::builder_writes_sorted_sidecar` — given a parquet at `/tmp/test.parquet` and a candidate `(test, customer_id, sorted_i64)`, invoke `build_one(&candidate)`, assert `/tmp/test.parquet.idx` exists and opens with the right manifest.
- `builder_marks_permission_denied_on_readonly` — set source prefix `chmod -w`, invoke builder, assert candidate state → `'permission_denied'` (not an error throw).
- `execute_time_demote_after_three_losses` — register provider with a sidecar, run a query whose actual selectivity is 90% three times, assert candidate state → `'demoted'`.

**Tasks:**
- [ ] `crates/ematix-flow-core/src/sidecar_builder.rs`.
- [ ] **Lazy trigger, not background daemon.** First-pass: builder runs on a separate `tokio::task::spawn_blocking` *after* a query completes, not in a long-running poller. Why: keeps the surface small (no new lifecycle to manage), and the trigger condition ("we just saw a high-scoring predicate") is naturally co-located with the observer.
- [ ] Concurrency cap: at most 2 builds running, controlled by a process-wide `Semaphore(2)`. Prevents accidental DoS on the data directory.
- [ ] Source-prefix permission check before build attempt; sets `state='permission_denied'` and records `build_outcome='read_only'`.
- [ ] **Build dispatch table** — keyed on `(physical_type, index_kind)`, calls the right `IndexBuilder` method from `ematix-parquet-codec`. v1: sorted-only for INT32 / INT64 / BYTE_ARRAY.
- [ ] After build success, drop the source's `SidecarHandleCache` entry so the next query re-discovers and picks up the new sidecar.
- [ ] **Execute-time selectivity feedback (OQ-DEMOTE)**: `EmatixFastParquetExec` records actual `(rows_returned / rows_scanned)` per sidecar use. If it exceeds the 0.60 crossover, bump `consecutive_losses += 1`. At 3 consecutive losses, set `state='demoted'`.
- [ ] Env flag `EMAT_SIDECAR_BUILD=0` disables auto-build entirely (read still works).

**PR shape:** Bundled with Stories 2.1 + 2.2. Combined PR is large (~3 new modules, ~5 new tests) but the three are interdependent — splitting forces a no-op middle PR.

### Story 2.4 — Web UI "Indexes" tab

**Status:** `[ ]`

**Failing test:** Not Rust-test-driven; the gate is "the existing 22q soak run renders the right shape in the UI."

**Tasks:**
- [ ] New tab `#/indexes` next to `#/workflows`, `#/jobs`, `#/runs`, `#/dag` in `web-ui/src/App.svelte`.
- [ ] New route component `web-ui/src/routes/Indexes.svelte`.
- [ ] Backend endpoint `GET /api/indexes` — returns the contents of `sidecar_candidates` joined with `predicate_observations`, sorted by score descending.
- [ ] Per-row columns: table, column, kind, state, score, observation count, "would help" badge (when state=`permission_denied`), delete button.
- [ ] Delete button calls `DELETE /api/indexes/{table}/{column}/{kind}` — removes the sidecar file + the `sidecar_candidates` row.
- [ ] Per `project_web_ui_reskin.md`: use existing `--accent` / `--surface-` tokens; no new design system.

**PR shape:** One PR. Independent of the Rust observer/builder PR; the API endpoint can be empty in dev until 2.1+2.2+2.3 lands.

### Story 2.5 — Extended index types in the builder (page-Bloom, composite, inverted)

**Status:** `[ ]` (deferred — open after 2.1–2.4 soak)

**Failing test:**
- One TDD test per type (mirrors story 2.3's pattern). Each kicks off only when the scorer has a candidate of that kind.

**Tasks:**
- [ ] Page-Bloom scorer slot: triggered when `estimated_selectivity < 0.001` AND column cardinality is high (heuristic: `distinct_count > num_rows / 1000`). Why: sorted index's memory footprint balloons on unique-per-row columns (upstream §"Build cost"); Bloom is the right tool.
- [ ] Composite scorer slot: triggered when two columns in the same table have correlated `eq` observations in the same query shape (workload log gets a `query_shape_hash` column to support this).
- [ ] Inverted-text scorer slot: triggered when a `contains(col, literal)` observation fires on a Utf8/Utf8View column.

**PR shape:** Three separate small PRs in priority order — page-Bloom first (matches Q14/Q17 high-cardinality FK shape), then composite (Q18 multi-column shape), then inverted (no current TPC-H driver, but the read-path already handles it).

---

## Risks + things to watch

| Risk | Mitigation |
|---|---|
| **Sidecar discovery latency tax on cold-cache queries.** First-touch on a multi-file dataset is N `stat` calls. | At 1000+ files (Iceberg-scale), this matters. At 22q SF=10 scale (~10 files), it's microseconds. The Phase 1.5 cloud-object-store extension will need bulk-list to avoid the per-file `stat`. |
| **Codegen tax from the planner hook.** Hard constraint #1 says no optimizer rule, but the pre-planning helper still touches the LogicalPlan walker — could trigger the same LLVM sensitivity. | Bench-gate at Story 1.3 catches this. If 22q geomean regresses, fallback is to gate the hook behind `EMAT_SIDECAR_READ` flag with the default *off* indefinitely; ship as opt-in only. |
| **Fingerprint mismatch on a write-loop workload.** Append-only ingest (e.g., new daily file → rewrite manifest) invalidates sidecars. | Upstream rebuild story is explicit; Phase 2 builder re-fires when a fingerprint-mismatch is observed. Operator-side: document this in `docs/PHASE_SIDECAR_READ_BENCH.md` as an expected mode. |
| **Auto-builder DoS on a directory the user didn't expect us to write to.** | Permission check before write; concurrency cap (Sem(2)); env-flag kill-switch (`EMAT_SIDECAR_BUILD=0`). And: if any build returns an OS error other than permission-denied (disk full, etc.), the builder pauses for 1 hour. |
| **SQLite contention** when multiple ematix-flow processes share `~/.ematix/workload.db`. | Already handled by `WorkloadLog`'s WAL-mode + `Mutex` — see `workload_log.rs`. New tables follow the same pattern. |
| **The scorer is wrong.** It's a heuristic; some workloads will have sidecars that lose. | Demote-on-loss feedback (Story 2.3). The Web UI's per-row score column makes the bad scores visible. Hard delete via UI is the manual override. |
| **Build cost is 10× the column scan** (per upstream §"Build cost"). On a cold dataset, the first user pays. | This is an intentional trade — the build amortises across all subsequent queries. The Web UI surface tells the user *which* build is running so the bad UX of "my first query was slow" has an explanation. |

---

## Cross-references

- Upstream API docs: `/Users/ryanevans/RustroverProjects/ematix-parquet/docs/sidecar-indexes.md`
- Upstream integration patterns: `/Users/ryanevans/RustroverProjects/ematix-parquet/docs/ematix-flow-integration.md`
- Pre-planning helper template: `crates/ematix-flow-core/src/dict_routing.rs` (Σ.K.2)
- Process-wide cache template: `crates/ematix-flow-core/src/parquet_decode_cache.rs` (Σ.O.c.1) — load via env-flag, default-off, flip later
- Workload log template: `crates/ematix-flow-core/src/workload_log.rs` (Σ.L.2)
- Adaptive-runtime parent program: memory `project_sigma_l_adaptive_runtime.md`
- Web UI conventions: memory `project_web_ui_reskin.md`
- Codegen-tax precedent: memory `project_optimizer_codegen_sensitivity.md`
- Pin-bump trap: memory `feedback_patch_crates_io_version_match.md`

## Out of scope (Phase 3+ candidates)

- **Iceberg table provider + sidecar-aware file pruning** (OQ-IB). Gate: ematix-flow grows an Iceberg `TableProvider` first.
- **Async object-store sidecar I/O.** Gate: upstream `ematix-parquet-async` ships `ParquetIndex` with an async opener.
- **Distributed sidecar coordination** (worker nodes shipping sidecars over Flight). Gate: any evidence that local-only discovery is insufficient at our cluster scale.
- **New upstream index types** (FLOAT/DOUBLE sorted; multi-token text). Gate: upstream extends the wire format.
