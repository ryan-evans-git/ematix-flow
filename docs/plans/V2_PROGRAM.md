# v2 Execution Program — native-engine cutover, DataFusion elimination, DataFrame API

*Program doc, drafted 2026-07-18. Supersedes the DataFusion-centric framing of
[`V2_SQL_SURFACE_GAPS.md`](V2_SQL_SURFACE_GAPS.md) (which measured DF's SQL
surface on the DF-embedded push engine — that engine is being retired).
Target: [`../V2_TARGET.md`](../V2_TARGET.md). Foundation: the clean-room native
engine ([`NATIVE_ENGINE.md`](NATIVE_ENGINE.md)) is **built and merged to `v2`**
(HEAD `84143a45`).*

> Owner: Ryan Evans. Status: planning. This doc is the spine; per-phase detail
> lands in `PHASE_*` docs as each phase opens.

---

## 0. Where we actually are (grounded, 2026-07-18)

The native engine crate (`crates/ematix-flow-engine`, 100 commits) is merged and
passes its gates. **But the merge did not move the DataFusion footprint** — the
strangler cutover has not begun:

| Signal | Value on `v2` |
|---|---|
| `.rs` files importing `datafusion` | **248** (core/src 84, core/examples 132, distributed 15, py 2, cli 2, …) |
| core/src files on `RecordBatch`/`arrow::` | **89** — Arrow is core's in-memory substrate |
| core/src files on `SessionContext`/`TableProvider`/`ExecutionPlan`/`LogicalPlan` | 64 / 63 / 61 / 54 |
| `ematix_flow_engine` refs in **core/src** | **0** (3 in tests/examples — a parity oracle only) |
| `datafusion` as a manifest dependency | 4 crates (core, cli, distributed, py) |
| Native engine distributed/mesh code | **0 lines** (single-box only) |

**Reframing:** DataFusion + Arrow are not a component of core — they are its
*substrate*. Eliminating them is not a swap; it is rewriting core's types,
catalog, IO, session, and Python bindings onto the engine. This program is a
comparable-sized effort to building the engine itself.

### The engine is not yet "up to par" (from the 2026-07-18 independent review)

Removing the DF fallback is **gated** on closing these — you cannot ship an
engine as the sole path while it:

- **Panics instead of erroring** on execution-time type/value edge cases
  (`expr.rs` ~34 `panic!`/`unreachable!`; `plan.rs` "payload type mismatch",
  "join key must be integer-family"). A query that *binds* but hits an unmodeled
  type combo takes down the thread.
- **Has no memory bound on the SQL path.** The GRACE spill + adaptive breakers
  (`spill.rs`/`adaptive.rs`/`agg.rs`/`hashjoin.rs`) are **orphaned** — `plan.rs`
  never calls them and builds unbounded in-memory join/group state → large
  `GROUP BY`/join can OOM. (Spill record is hardcoded `(i64,i64)`; can't spill
  strings/decimals/wide payloads even if wired.)
- **Has correctness gaps the benchmark hides:** integer division computed in f64
  (`7/2` → `3.5`), `ORDER BY … NULLS FIRST/LAST` silently ignored, no true
  DECIMAL (all f64), window frames limited to `UNBOUNDED PRECEDING..CURRENT ROW`
  (no `RANGE`, no `N PRECEDING/FOLLOWING`), multiple correlated subquery
  conditions rejected, FULL OUTER / RIGHT JOIN shipped with no asserting tests.
- **Has no real parity gate.** "TPC-DS 103/103 parity" is a bind/exec-success
  count from a panic-swallowing `main()` — **there is no TPC-DS DuckDB oracle in
  the tree.** Asserted parity today = 22 TPC-H queries vs pyarrow constants (SF1,
  skipped if data absent) + Q6 vs DuckDB.
- **Is operationally a black box:** stringly-typed errors (`Result<_, String>`),
  zero `tracing`/metrics, no EXPLAIN, no cancellation/timeout.

---

## 1. Guiding principles

1. **Measurement-first.** No phase closes on a claim; it closes on an asserting
   gate. The real DuckDB parity gate (E0) is the instrument for the whole
   program — build it before trusting or hardening anything.
2. **Strangler-fig.** Route covered+hardened shapes to the engine; DF handles the
   rest; the fallback set shrinks each phase; when it hits zero, delete DF.
3. **Never drop the fallback ahead of coverage.** DF stays as runtime fallback
   until the engine provably covers a shape at parity *and* production quality.
4. **RED-first.** Every correctness fix and every DataFrame op ships with a
   failing parity test (DuckDB / pandas oracle) before the kernel.
5. **Clean-room is about the shipped runtime.** Retaining DataFusion as a
   **dev-only differential-test oracle** is allowed and encouraged — it is free
   confidence. The goal is zero DF in what we *ship*, not in what we *test with*.

---

## 2. Decisions locked (2026-07-18)

- **Distributed: rebuild natively** (Track D). The engine gets its own
  mesh/shuffle layer; `datafusion-distributed` is retired, not quarantined.
- **DataFrame API: start in parallel now** (Track F), designed against the
  engine's logical IR; execution wiring lands once the E2 seam exists.
- **First action: this program doc**, then E0.

---

## 3. Tracks

Three tracks run concurrently after E1; the engine-cutover spine (Track E) is the
critical path that the others depend on at defined seams.

### Track E — Engine cutover & DataFusion elimination (the spine)

| Phase | Deliverable | Exit gate |
|---|---|---|
| **E0 — Re-baseline & parity gate** | Re-scope stale docs. Build the differential **engine-vs-DuckDB** TPC-DS gate, row-exact, SF1/10/100, **in CI** (assert, not count). Honestly re-establish the coverage number. | Green CI gate reports real per-query row-parity at SF1/10; SF100 charted. The "103/103" claim is replaced by a measured figure. |
| **E1 — "Up to par" hardening** | Correctness: int division, NULLS ordering, DECIMAL, window frames, multi-correlation subqueries — each RED-first vs E0 oracle. Robustness: propagate errors (kill eval panics); **wire spill+adaptive+global memory governor into `plan.rs`**; generalize spill payloads; structured error type; `tracing`/EXPLAIN/cancellation. Consolidate the `exec.rs`-vs-`plan.rs` driver split. | No panic on any fuzzed/negative SQL; a bounded-memory large GROUP BY/join through the **SQL path**; every E0 correctness gap has a passing test. Architect sign-off on the spill/memory-governor design. |
| **E2 — The seam** | Engine **Arrow export** (`to_arrow` over Flat/Dictionary — none exists today) + a **dispatcher** that routes a query to the engine when covered+hardened, else DF fallback, returning a common (Arrow) result. Strangler switch, default DF, opt-in engine. | A query runs end-to-end through engine→Arrow→existing API path, byte-parity with the DF path; fallback measurable and defaulted safe. |
| **E3 — Consumer cutover** | Route core's SQL path through the dispatcher (fallback shrinks each iteration, tracked by E0 coverage). Then **Python API** (needs Arrow export + an engine UDF/UDAF surface — none today) and **CLI**. | Core/py/cli execute on the engine for all covered shapes at parity; DF fallback set enumerated and shrinking to a known tail. |
| **E5 — DF removal** | Fallback → zero. Delete `datafusion` from the 4 manifests + all `src` uses; port/retire the 132 DF-based examples; drop `datafusion-distributed` (see Track D). Keep DF as an optional dev-only oracle if desired. | `grep -r datafusion crates/*/src` is empty; workspace builds and the E0 gate passes with DF absent from the shipped graph. **Clean-room achieved.** |

*(E4 is Track D — native distribution — sequenced between E3 and E5.)*

### Track D — Native distribution (rebuild the mesh on the engine)

The engine has zero distributed code; this rebuilds scale-out clean-room so DF's
removal doesn't regress the shipped SF1000 capability.

| Phase | Deliverable | Exit gate |
|---|---|---|
| **D0 — Design** | Architecture for native multi-node: partitioning, shuffle/exchange operator on the engine's `DataChunk`, coordinator/worker protocol, fault model. Reuse the morsel scheduler's contracts. Architect-led. | Written design + a de-risk spike plan (kill-gate discipline). |
| **D1 — Single-query shuffle** | One exchange operator; a distributed hash-join across 2 nodes, correctness-parity with single-box. | 2-node join row-parity == single-box; honest spill/exchange stats. |
| **D2 — Mesh + benchmark** | Fan-out over the TPC-H/DS suite on a real cluster; SF1000. | SF1000 runs; charted vs the old DF-mesh numbers (no regression on the banked results). |
| **D3 — Retire `datafusion-distributed`** | Cut the distributed crate over; delete the DF-distributed dependency. | Distributed crate is DF-free; folded into E5. |

### Track F — DataFrame API (Pillar 2, "the headline feature")

Starts now on design + the engine's logical IR; execution binds at the E2 seam.

| Phase | Deliverable | Exit gate |
|---|---|---|
| **F0 — Surface design** | pandas-shaped API mapped to the engine's `BoundQuery`/logical IR (read/filter/groupby/join/window + the top-50 ops, §3.4 of V2_TARGET). Namespace + laziness decisions (V2_TARGET open Qs). | API spec + a pandas-oracle parity harness (RED-first). |
| **F1 — Core ops on the engine** | The top-50 ops executing on the engine via the E2 Arrow boundary; zero-copy interop. | Parity vs pandas oracle on the op matrix; runs on the engine, no pandas/NumPy-object path. |
| **F2 — Migration on-ramps** (Pillar 3) | `import ematix.pandas as pd` shim + `ematix migrate` scorecard + cheat-sheets. | A representative pandas script runs under the shim. |

### Cross-cutting (Track X)

- **Docs re-scope:** retire/annotate `V2_SQL_SURFACE_GAPS.md` and the `PHASE_V2_S1/2/3` docs as DF-era; keep `NATIVE_ENGINE.md` current past P2; update `docs/ROADMAP.md`.
- **Interop (Pillar 4/5):** Flight SQL + ADBC + Iceberg read on the engine's Arrow export (depends on E2).
- **CI/TDD:** the E0 gate + the F0 pandas-oracle matrix become required checks; benchmark regression sentinels on the engine.

---

## 4. Dependencies & critical path

```
E0 ──▶ E1 ──▶ E2 ──▶ E3 ──▶ E4(=D1–D3) ──▶ E5 (clean-room)
                │
                ├──▶ F0 ▶ F1 ▶ F2      (DataFrame API, parallel after the seam)
                └──▶ Interop (Flight/ADBC/Iceberg)
D0 (design) may start in parallel with E1; D1+ gated on E1's hardened breakers.
```

- **E0 gates everything** — no cutover without a real parity instrument.
- **E1 gates E2** — can't route production traffic to an engine that panics/OOMs.
- **E2 is the seam** everything downstream (E3, F1, interop) binds to.
- **Track D** must complete before E5 (can't delete `datafusion-distributed`
  until the native mesh replaces it) — this makes D the long pole on the
  clean-room date.

---

## 5. v2 exit criteria (ties to V2_TARGET pillars)

- **P1 (native SQL):** E0 gate green — 99/99 TPC-DS row-parity vs DuckDB at
  SF1/10/100; SF100 charted; window/grouping-sets vectorized on the engine.
- **P2 (DataFrame):** F1 — top-50 pandas ops at parity on the engine.
- **P4/5 (interop/scale):** Flight SQL + ADBC + Iceberg read; native mesh at
  SF1000 with no regression vs the DF-mesh baseline.
- **Clean-room:** E5 — zero `datafusion` in the shipped dependency graph.
- **Hardening (§6):** no OOM at target scales; no panic on adversarial SQL;
  EXPLAIN + cancellation present.

---

## 6. Risks & open questions

- **Substrate rewrite blast radius.** 89 core/src files on Arrow types — the
  cutover is broad. Mitigation: strangler dispatcher keeps DF live until each
  shape is covered; never a big-bang swap.
- **Track D is a program inside the program.** Native distribution is large and
  is the long pole on clean-room. Re-confirm scope if the timeline pressures the
  v2 cut (V2_TARGET §9: scale-out is "proven, not headline").
- **DataFrame-on-a-moving-substrate.** F starts now but binds at E2; F0 design
  must target the *stable* logical IR, not today's in-flux internals.
- **Oracle strategy.** Keep DF as a dev-only differential oracle through E5, or
  move to DuckDB-only earlier? (Recommend: keep DF oracle until E5 confidence.)
- **Open from V2_TARGET §11:** DataFrame namespace/laziness, pandas index model,
  dbt adapter scope, tier-1 BI clients, SF1000 in-cut vs fast-follow.

---

## 7. Immediate next actions

1. **E0.1** — stand up the differential engine-vs-DuckDB TPC-DS harness as an
   asserting test (not an example); wire it into CI; publish the real coverage
   number, replacing "103/103".
2. **E0.2** — re-scope the DF-era docs (annotate `V2_SQL_SURFACE_GAPS.md`,
   `PHASE_V2_S1/2/3`) so they don't misdirect; point `docs/ROADMAP.md` here.
3. **E1.0 / D0 / F0** open in parallel once E0 gives a trustworthy baseline:
   engine hardening backlog (from the review), native-distribution design spike,
   DataFrame surface spec.
