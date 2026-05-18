# Σ.G.2f — `FilterMultiAggSpec` + Q1 generalisation arc

**Status:** .1–.4 shipped; umbrella task #480 still open pending the AWS pre-built target follow-up and an explicit "done" sign-off.
**Date:** 2026-05-18
**Scope:** Replace the hand-baked Q1 path (`Q1Spec` JIT + `InjectFusedQ1Rule`) with a generic multi-aggregate + group-by spec (`FilterMultiAggSpec`) plus per-batch Photon-style template specialisations that close the perf gap. Outcome: zero Q1-specific code on the hot path; one rule + one spec + N templates that any matching SQL shape lifts into.

**Why this arc:** Σ.G.2e generalised the single-SUM filter shape (`FilterSumSpec`). Σ.G.2f does the same job for the multi-aggregate + group-by shape, with Q1 as the canonical fixture. Without it, every new TPC-H-shaped query needs its own JIT-baked spec; with it, the planner rule lifts the shape from the plan.

---

## Sub-arc status

| Sub | Title | Status | PR / commit | Notes |
|-----|-------|:-----:|-------------|-------|
| Σ.G.2f.1 | `FilterMultiAggSpec` + generic process_batch (no template) | ✓ shipped | — | Hash-grouped `Vec<u8>` composite-key fallback. Correct on every shape; slow vs Q1Spec. |
| Σ.G.2f.2 | Photon-style template dispatch + typed-slice cache | ✓ shipped | #482–#485 | `process_batch_dict_single` + `process_batch_perfect_hash_dict`. Closes Dict path to ±5% of Q1Spec when strings arrive as `Dictionary<UInt32,Utf8>`. |
| Σ.G.2f.3 | `InjectFilterMultiAggRule` + end-to-end gate + delete `Q1Spec` | ✓ shipped | #486 | Multi-agg rule recognises AVG/MIN/MAX/SUM(col²); Q1Spec deleted from the codebase. |
| Σ.G.2f.4 | `process_batch_two_key_utf8view` template | ✓ shipped | #119 | Q1 SQL on `Utf8View` columns. 148.73 → 111.30 ms on SF=1 lineitem (1.34×). Closes ⅓ of the gap to dict-preserved path. |

---

## Why .4 was needed after .3

`Σ.G.2f.3` (Q1Spec deletion) was conditional on **either** of:

(a) FastParquet dict preservation — string columns arrive as `Dictionary<UInt32, Utf8>` → `Σ.G.2f.2`'s `dict_single` / `perfect_hash_dict` templates hit. Landed via #481 + ematix-parquet PR #34 + flow task #464.

(b) A Utf8View template — string columns arrive as `Utf8View` and we have a fast hot loop for that shape.

Path (a) lands the dict shape only when (i) the source columns are dict-encoded throughout (no dict-fallback) AND (ii) the reader preserves them as `Dictionary`. The TPC-H lineitem fixtures used in our regression suite ship `Utf8View` end-to-end because `EmatixFastParquet` is currently the streaming default and doesn't auto-promote (`[[dict-arrival-blocker]]`).

Path (b) — Σ.G.2f.4 — is the perf safety net: when (a) isn't possible, the spec still hits a template that beats the generic path 1.34×.

Together: Q1 SQL runs the template route whether strings arrive as `Dictionary` (fast) or `Utf8View` (also fast), no JIT, no Q1-specific shape match.

---

## What's still open (umbrella #480)

### 1. AWS pre-built target follow-up — ✓ landed via PR for `infra/aws-prebuilt-target`

**Scope:** GitHub Actions builds a `target.tar.zst` (zstd over gzip — ~3-4× faster, ~30% smaller) on every push to main, uploads to a persistent S3 bucket. Phase A userdata.sh tries `aws s3 cp` + `tar xf` first, falls back to `cargo build` on miss / SHA mismatch.

**Components shipped:**
- `infra/prebuilt-target/` — separate persistent terraform module (one-time apply). Bucket auto-expires objects at 30 days.
- `.github/workflows/prebuild-target.yml` — push-to-main trigger; builds both `ematix-flow` and `ematix-parquet` workspaces with `-C target-cpu=x86-64-v4` (Sapphire Rapids AVX-512 baseline without micro-arch pinning).
- `infra/test-validation/main.tf` — new `prebuild_target_bucket` variable + conditional IAM read-policy attachment on Phase A's EC2 role.
- `infra/test-validation/userdata.sh` — manifest-first SHA-validated fast path with graceful fallback to `cargo build`.

**One-time operator setup:** `cd infra/prebuilt-target && terraform apply`, then set GH repo variable `EMATIX_PREBUILD_BUCKET` + secrets `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`, then pass `-var prebuild_target_bucket=<bucket>` to subsequent campaign `terraform apply`s.

### 2. Σ.G.2f closure sign-off

Once the AWS pre-built target lands, #480 closes. No further code work — just a brief note in `BENCHMARKS.md` or a section here noting the geomean-on-22 number after the arc.

---

## Out of scope (deferred or owned elsewhere)

- **Multi-key Utf8View beyond 2** (e.g. 3-key `Utf8ViewFirstByte` packed into u24/u32). No TPC-H query shape needs this today; revisit if a customer query surfaces it.
- **Dict-preservation arrival across all readers** — owned by Σ.E3b / Σ.E5; `[[dict-arrival-blocker]]` captures the cross-reader state.
- **JIT for `FilterMultiAggSpec`** — not needed; the template dispatch already matches Q1Spec on the dict path and 0.43× on the Utf8View path. No customer-data evidence that a JIT lowering would beat the templates.

---

## Why the templates beat a JIT lowering here

`Q1Spec`'s Cranelift JIT bakes the 4 distinct `(returnflag, linestatus)` literals as branchless arm matches. The template equivalents:

- `perfect_hash_dict`: indexes a flat `Vec<f64>` directly by dict code — no HashMap probe in the hot loop, single arm in LLVM IR.
- `two_key_utf8view`: per-batch `HashMap<u16, usize>` local index amortises to ≈ 4 distinct pairs in Q1.

In both cases LLVM autovectorises the inner agg loop because the typed-slice cache (`Σ.G.2f.2`) eliminates `dyn` dispatch on the predicate + agg eval. The Cranelift baseline beats the templates by 0–5% on Q1, but loses on shape generality: a JIT spec only knows the shape it was baked for, while a template dispatches at batch time.

---

## Test + bench coverage

| Layer | Coverage | Location |
|-------|----------|----------|
| Unit: per-template equivalence with generic | 3 tests each for dict_single, perfect_hash, two_key_utf8view | `crates/ematix-flow-core/src/fused_aggregate_filter_multi_agg.rs` |
| Unit: dispatch routes correctly | 3 tests, one per template | same |
| Unit: cross-shard merge equivalence | 1 test per template | same |
| Bench: Q1 SQL spec-level | `tpch_q1_template_gate` example | `examples/` |
| Bench: 22-query TPC-H | `bench` harness at SF=1, SF=10 | `examples/tpch/` |

Numbers as of #119 land:
- Q1Spec (JIT) baseline: 47.35 ms
- Template / Dictionary: 45.85 ms (−3.8%)
- Template / Utf8View (TK u16): 111.30 ms (+135%)
- Generic / Utf8View: 148.73 ms (+214%) — for reference

The +135% Utf8View number is the floor without dict preservation. The +0% dict number is the ceiling. Real-world geomean lands between depending on per-table dict-preservation status.
