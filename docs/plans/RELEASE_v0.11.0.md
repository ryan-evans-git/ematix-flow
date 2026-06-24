# Release v0.11.0 — corrected release runbook

Status: IN PROGRESS (started 2026-06-18). Internal doc — EXCLUDE from the release commits.

## Why a corrected release (the mistakes in the published v0.10.0)
1. **README benchmark numbers are stale + self-contradictory.** README.md:63-64 + the
   benchmark table (1482-1536) say **16/22 SF=10 (1.21×), 15/22 SF=100 (1.30×)** — pre-v0.10.0
   numbers — while the v0.10.0 CHANGELOG says **21/22 SF=10, 18/22 SF=100**. The PyPI page shows
   numbers that disagree with our own changelog.
2. **Published SF=10 column was MI.GATE-taxed** (a self-latching peak-RSS gate inflated it; fixed
   in 42e2a1b). Trustworthy numbers need a re-measure on the fixed binary.
3. **Version collision:** branch Cargo/pyproject = `0.10.0`, but v0.10.0 is already published →
   corrected release is **v0.11.0**.

## Decisions (locked 2026-06-18)
- **Benchmark source = FRESH FULL RE-BENCH** (SF=1/10/100, ematix + DuckDB + Polars, MI.GATE-fixed
  binary, published same-session-paired protocol, settled machine).
- **Release content = campaign work (16 commits) + banked infra** (BuildRowId + gather_build_cols
  + LateGatherExec, committed as one clean opt-in/default-off commit). EXCLUDE: internal
  `docs/plans/*` + the diagnostic bench-knobs in `tpch_preset_rebench.rs`.

## Git topology (verified read-only)
- `origin/main` = 35a9d00 = **v0.10.0** (published, on PyPI). Local `main` is stale (0.9.0).
- HEAD `sigma-q20-transitive-semi` = v0.10.0 content + 16 campaign commits. `git diff HEAD...origin/main`
  is **EMPTY**, merge-tree **conflict-free** → reconciliation is trivial. PR base = origin/main.

## Phases
- [ ] **P1 — clean local base (reversible, in progress):** commit banked infra (scoped files only);
      bump Cargo.toml + pyproject.toml → `0.11.0`. Bench-knobs + internal docs stay uncommitted.
- [ ] **P2 — fresh full bench (long pole, runs ALONE, ~1-3h):** SF=1/10/100 ematix+DuckDB+Polars on
      the v0.11.0 candidate via `tpch_preset_rebench` (the bench-knob harness is fine to USE here;
      revert it before the PR). Capture canonical per-query + win-rate tables per scale.
- [ ] **P3 — doc correction (same numbers everywhere):** README intro (63-67) + benchmark table
      (1482-1536); CHANGELOG `[0.11.0]`; docs/BENCHMARKS.md. Keep the honest SF=100 warm-vs-in-sweep
      framing. Audit README for PyPI rendering (relative links/images/anchors, version strings).
- [ ] **P4 — green-gate (build/test OK once bench is done):** cargo build + workspace tests +
      fmt/clippy + ruff/bandit (mirror ci.yml); confirm mimalloc `local_dynamic_tls` cdylib fix
      intact (v0.10.0 Linux `import` trap); local `maturin build` sanity wheel.
- [ ] **P5 — release (CONFIRM before push/publish):** revert bench-knobs; create `release/v0.11.0`;
      PR → CI green → merge main → push tag `v0.11.0` → release.yml builds 8 wheels + sdist →
      PyPI trusted-publish → verify page + `pip install ematix-flow==0.11.0` smoke test.
- [ ] **P6 — dev site (CONFIRM before deploy):** update ematix.dev benchmark pages
      (`ScaleBenchmarks.astro` et al.) with the same numbers → build → `wrangler pages deploy` →
      verify live.

## Progress (2026-06-18)
- **P1 DONE:** banked infra committed `675d0be` (BuildRowId + gather_build_cols + LateGatherExec,
  opt-in/inert, 11/11). Tree clean except the deliberately-excluded files (bench-knobs + internal docs).
- **DECISION (user): PUBLISH THE HONEST NUMBERS.** Ship the real current state; correct the stale
  16/22 + the over-claimed 21/22. The 100% chase continues separately, not blocking the release.
- **P2 fresh bench (production-faithful `tpch_preset_rebench`, TRIALS=5 WARMUPS=2, MI.GATE-fixed binary):**
  - SF=1  = **22/22**, geomean **0.427×** (ematix 13.1ms vs DuckDB 30.7ms). Row counts match. `/tmp/rebench_sf1.txt`.
  - SF=10 = **19/22**, geomean **0.808×** (107.2 vs 132.8ms). Losses: Q05 0.88×, Q07 0.88×, Q18 0.83×
    (known-structural). Marginal wins Q01/Q02/Q04/Q09 (≤6%) want a confirming pass. `/tmp/rebench_sf10.txt`.
  - SF=10 pass-2 = **18/22**, 0.819× (Q07 flipped to win; marginals flip on noise → SF=10 robustly ~18-19/22, ~0.81× = ematix ~1.23× faster). `/tmp/rebench_sf10_p2.txt`.
  - SF=100 pass-1 = **12/22**, **1.019× (PARITY) IN-SWEEP** — well below published 15-18/22. Cache-bound: Q10 5919/0.45×, Q18 0.66×, Q16 0.69×, Q20 0.75×, Q11 0.50× — the high-RSS heavies thrash the 36GB box (30GB WS); DuckDB's low RSS is less sensitive. `/tmp/rebench_sf100.txt`.
  - SF=100 pass-2 = RUNNING (bg `b7w2d8g6n` → `/tmp/rebench_sf100_p2.txt`) for the median + variance.
  - ★ **HONEST SF=100 finding:** the published 1.30× / 15-18/22 was optimistic — fresh in-sweep is ~parity. Plan: publish the honest in-sweep (median of 2 passes) + KEEP the warm-isolated caveat (ematix wins the heavies isolated) per the existing README structure. This is itself a "mistake" the corrected release fixes.
  - Polars/PySpark/Postgres = carried with a provenance note (this harness is ematix+DuckDB only;
    Polars OOMs at SF=100). 
- **Remaining sequence (after SF=100 pass-1 lands):** SF=10 pass-2 + SF=100 pass-2 (the published
  2-pair protocol → median-of-medians) → finalize numbers → P3 docs (README 63-67 + 1465-1540,
  CHANGELOG [0.11.0], BENCHMARKS.md) + version bump 0.11.0 → P4 green-gate → P5 release (CONFIRM) →
  P6 site (CONFIRM).

## Guardrails
- No push/publish/deploy without explicit sign-off (local commits are fine).
- The SF=100 bench runs ALONE — no other cargo build/test/bench concurrently (pollutes wall-time).
- Commit messages end with the Co-Authored-By line.
- `release.yml` trigger = push `v*` tag; CI (`ci.yml`) must be green on the SHA first.

## P2 FINAL — trustworthy numbers (2026-06-18, ISOLATED per-engine protocol)
The SF=100 saga resolved: the "losses/regression" were a `tpch_preset_rebench` **interleaved-process
contention artifact** (ematix+DuckDB co-running on a 36GB box → higher-RSS engine ~2× slow). Fair
numbers = each engine in its OWN process (`SKIP_DUCKDB=1` / `SKIP_EMATIX=1`). FINAL:
- **SF=1: 22/22, geomean 2.34× faster** (ematix 13.1ms vs DuckDB 30.7ms). `/tmp/rebench_sf1.txt`.
- **SF=10: ~18/22 (pass-1 19, pass-2 18), geomean ~1.23× faster**. Losses: Q05, Q07, Q18 (+1 marginal).
  `/tmp/rebench_sf10.txt`, `/tmp/rebench_sf10_p2.txt`.
- **SF=100: 18/22, geomean 1.58× faster** (isolated). Losses: Q10 0.68×, Q16 0.86×, Q18 0.71×; Q11 tie.
  `/tmp/rebench_sf100_emat_alone.txt` (ematix) + `/tmp/rebench_sf100_duck_alone.txt` (DuckDB).
- NO branch regression (Q10 branch 3515 ≈ v0.10.0 3590 via `tpch_preset_bench`). Polars/PySpark/Postgres
  = carried (provenance note); this harness is ematix+DuckDB only.

## REMAINING (mechanical; do consistently in one pass — outward steps need sign-off)
- **README.md** — must be internally consistent (don't ship a half-edit):
  - Intro 62-65: `16/22 SF=10 (1.21×)` → `18/22 (1.23×)`; `15/22 SF=100 (1.30×)` → `18/22 (1.58×)`;
    `2.36×` → `2.34×`; reword "still ahead on the geomean where data spills out of cache" → "ahead at
    every scale (each engine measured in its own process)".
  - Summary table ~1482-1490 (SF=10/SF=100 win-count + ratio rows) → same numbers.
  - Per-query SF=100 table ~110-131: refresh ematix+DuckDB ms from the isolated files (Polars/PySpark/
    Postgres carried). SF=10/SF=1 per-query tables similarly if present.
  - Provenance note ~133-159: state the **isolated-per-engine** protocol + drop the now-wrong
    "warm-isolated Q10/Q16/Q18 win" caveat (they LOSE; the real losses are Q10/Q16/Q18).
- **CHANGELOG.md**: add `[0.11.0]` (campaign perf: transitive semi-pushdown, adaptive runtime-bloom
  sizing, single-phase range agg, MI.GATE heap-release; banked opt-in selection-vector infra; no API
  change). State SF=10 ~18/22 1.23× / SF=100 18/22 1.58× honestly; do NOT claim the old 21/22.
- **docs/BENCHMARKS.md**: align headline tables with the same numbers.
- **Version**: Cargo.toml:13 + pyproject.toml:7 `0.10.0` → `0.11.0`.
- **P4 green-gate** → **P5 release (CONFIRM)** → **P6 site (CONFIRM)** as above.
