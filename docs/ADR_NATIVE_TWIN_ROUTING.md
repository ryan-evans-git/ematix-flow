# ADR: Native-twin routing for AUTO's single-node commits (Σ.TW.1)

**Status:** accepted (2026-07-15) — Phase 1 of the AUTO→oracle arc.
**Supersedes:** PR #218 (grace demotion on local commits — refuted),
PR #219 (runtime-bloom reprune on local commits — refuted/inert).

## Context

The adaptive mesh gate (Σ.MG) runs queries in a distributed session and
commits some of them single-node (AUTO's scan-byte decision, the Q15
float veto, or `EMAT_MESH=0`). Those local commits execute through the
localize path (Σ.Q15.LS): the plan was built against **stock arrow-rs
parquet leaves** (required — distributed stages must be
codec-serializable) and `localize_scans` swaps the leaves onto
`EmatixFastParquetExec` at commit time.

The SF100 4-leg A/B (STAMP `20260715T113833Z`, c7i.4xlarge×4, parted
data, 5×2 trials) measured what that costs versus a native
`NO_DISTRIBUTE` session on the same fleet:

| leg | total (Σ medians) |
|---|---|
| mesh (`EMAT_MESH=1`) | 58.92 s |
| auto | 59.50 s |
| single forced (`EMAT_MESH=0`, localize) | 137.44 s |
| **puresingle (native)** | **58.23 s** |

AUTO's gap to the same-fleet per-query oracle (~47.1 s) decomposes into
~4.0 s of **execution** penalty on queries AUTO *correctly* committed
single (Q21 +1880 ms, Q05 +1368, Q15 +408, Q11 +357), ~3.5 s of
decision mis-picks, and ~0.8 s noise.

### Why commit-time localization cannot close the execution gap

The dominant native advantage is the KEYS.2 i32-key downcast
(`EMAT_DOWNCAST_KEYS`, scale-gated ON at SF100): the native provider
advertises narrowed Int32 `*key` columns, so joins hash/probe at half
width. Narrowing changes the provider's **output schema**
(`ematix_fast_parquet.rs:2828`); a physical plan already planned
against i64 leaves cannot be retrofitted — the localize rule's
load-bearing `schema_check` (unchanged plan schema) forbids exactly
this. Parallelism was ruled out (multi-file path already uncapped =
native); grace was refuted (#218); runtime-bloom attachment is gated
off in production shapes and small anyway (#219).

## Decision

When the distributed session's physical plan for a query is a **local
commit** (no datafusion-distributed network boundary) **and contains a
join**, do not execute the localized plan. Re-plan and execute the
query in a **native twin**: a single-node `SessionContext` carrying the
full production single-node preset with every catalog table
re-registered through the **native** fast-parquet provider
(`try_new` / `try_new_dir` — key-downcast auto-gate, runtime blooms,
grace, native scan parallelism all live, because the twin *is* the
native configuration; it cannot drift from it).

Local commits **without joins keep the localize path** — measured
faster there (Q01 −706 ms, Q06 −262 ms vs native: no join keys to
narrow, and localize skips native planning overhead).

Split rule = plan shape (join present), not measurement: the penalty
mechanism is join-key width, so joins are exactly and only where the
twin wins.

### API (ematix-flow-distributed `native_twin` module)

- `native_twin_ctx(&SessionContext) -> SessionContext` — production
  single-node preset (default `HarnessOverrides`: grace ON,
  auto target partitions, `collect_statistics(true)`); re-registers
  stock `ListingTable`s with local `file://` roots through the native
  fast provider (dir → multi-part, file → single); any other provider
  is carried over unchanged (correct, just not accelerated).
- `plan_is_mesh(&plan)` — the campaign's network-boundary walker,
  moved into the library (single source of truth).
- `plan_has_join(&plan)` — any physical join operator.
- `should_route_to_twin(&plan)` = `!plan_is_mesh && plan_has_join`.

### Campaign wiring

The route is decided once per query at the existing (untimed)
plan-mode probe; `EMAT_TWIN_ROUTE` tri-state (default **ON**) gates it,
`=0` restores today's behavior as the A/B control. Trials for
mesh/localized queries are byte-identical to today; twin-routed trials
run `twin.sql(...)` — identical to the native leg (bloom emit/arm is
skipped: the twin's preset carries the native runtime-bloom sideband
instead of the mesh's embedded blooms). `plan_mode` reports `"twin"`.

Deciding at prepare time (not per-trial) is the production posture:
the gate's verdict is deterministic per query shape, and Phase 2 of
this arc memoizes (plan-fingerprint → mode) anyway.

## Consequences

- Expected (from the A/B decomposition): AUTO ≈ 55.5 s at SF100
  production defaults — faster than both mesh (58.92) and native
  single (58.23). AWS validation required before merge (the #218/#219
  lesson: local-green ≠ SF100-real).
- Twin planning cost per routed query ≈ native planning cost (~ms),
  paid inside the trial timer, exactly as the native leg pays it.
- The twin holds a second copy of table *metadata* (footers), not data;
  the RG decode cache is process-global and shared.
- Phase 2 (separate): plan-fingerprint mode memo (measure, don't
  predict) to attack the ~3.5 s decision bucket; Phase 3: broadcast
  joins default inside AUTO's mesh commits (#216).
