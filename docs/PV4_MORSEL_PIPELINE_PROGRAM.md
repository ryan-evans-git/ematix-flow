# PV.4 — Morsel-driven fused pipeline (generalize PV.1's fully-fused shape)

**Status:** ⛔ **SHELVED — PV.4.0 spike returned NO-GO (2026-06-05).** The program's
central premise (overlap the build with the decode to hide it) is REFUTED: the build
and decode are both CPU-bound and the cores are saturated, so overlapping causes
contention, not hiding. Machinery (PV.3b recognizer + PV.4.0 overlap path) kept as
correct dormant infra (default-OFF, UNCOMMITTED). Do not pursue PV.4.1–4.5.
**Original decision (2026-06-05):** commit to a scoped multi-session program to recover
the Q08 −16% via morsel-driven execution, after PV.3b proved the operator-splice loses.

## ⛔ PV.4.0 RESULT — NO-GO (the spike did its job: a one-session kill)

`pv4_q08_overlap_ab` (SF=10, interleaved, 11 trials), buffer sweep:

| buffer | stock | fused-serial | fused-overlap | BUILD (overlap) | recovered |
|--------|-------|--------------|---------------|-----------------|-----------|
| 16  | 183.4 | +43.6% | +43.8% | 95.7 ms  | −1% (≈ serial) |
| 64  | 181.2 | +49.2% | +58.9% | 123.9 ms | −20% |
| 256 | 194.5 | +40.4% | +56.7% | 141.6 ms | −40% |

**Mechanism (monotonic + decisive):** the dim BUILD inflates **90 → 96 → 124 → 142 ms**
as the decode-ahead buffer grows. Forcing the fact decode to run concurrently with the
build STEALS CPU from the build (Q08 is CPU-saturated across all 14 cores at SF=10), so
the build stretches and the total gets *worse*. There is no buffer where overlap wins —
the best case (buf=16, near-zero overlap) merely degrades to the serial result, which is
itself +44% vs stock. The "90 ms serial prefix" was NOT idle slack to fill; the cores
were already busy.

**Deeper implication (refutes the whole push-fusion win):** PV.0/1/2 — *including PV.1's
−16%* — measured with PREBUILT probes, excluding the dim build. The build is ~90–140 ms
of real CPU work on a ~185 ms query. Count it, and fusion goes from −16% to +40–57% in
BOTH the operator-splice (PV.3b) and the overlap/morsel (PV.4.0) forms. The win never
existed once the build is included; it was a measurement artifact. Stock loses nothing to
fusion here because DataFusion's native pipelined hash-joins are already at least as
efficient as `build_structure` + row-wise `fuse_batch`, and there is no scheduling slack
to reclaim.

**Verdict:** SHELVE PV.4. Redirect to levers with real headroom (SF=100 / distributed,
per the long-standing Q08 memory verdict). The narrow remaining single-node lever (SIMD-
tag the existing HashJoinExec probe, HJ.4) has a small ceiling (probe ≈ 8% of wall, ≈
parity per the HJ.3 dig) and does not touch fusion.

---


## Why — what PV.3b settled
PV.3b built a correct recognizer + custom logical node (`FusedProbeNode`) +
`EmatPushPipelineExec`, spliced it into the production plan (22/22 A/A, default-OFF),
and measured **+53% SLOWER** at SF=10 — not the de-risk's −10.8%. Profiling pinpointed
the cause:

```
fused 284 ms = BUILD 90 ms (serial prefix, blocks the probe)
             + PROBE 114 ms wall (1601 ms CPU / 14 lanes)
             + REMAINDER 66 ms (supplier⋈n2 + adapter + agg + glue)

stock 186 ms — runs the SAME 90 ms dim reduction (orders⋈customer⋈n1⋈region),
              but OVERLAPPED with the ~140 ms lineitem decode
              (decode figure from REV.20 stage profiling; re-confirm in PV.4.0)
```

The 90 ms dim build is **not extra work** — stock runs the identical reduction — but
stock **overlaps** it with the lineitem decode, while the fused operator runs it as a
**blocking prefix before decode even starts**. That single serialization is the entire
+53%.

**Central risk (the program's whole thesis rests on this):** PV.0/1/2 — *including
PV.1's −16%* — all used **prebuilt probes** (dims built once, outside the timed region),
so the −16% itself *excluded* the dim-build cost. The morsel program bets that the 90 ms
build can be **hidden under the ~140 ms decode**. If it cannot fully overlap, the win
partially or fully evaporates. **PV.4.0 exists to test exactly this before any large build.**

## Success criteria (the program must clear ALL)
1. **Q08 SF=10:** fused ≤ −10% vs stock (target −16%); interleaved A/B, ≥11 trials, cooled.
2. **Correctness:** triple-walker prod A/A 22/22, 0 mismatch, at SF=1 AND SF=10.
3. **Generality (NO TPC-H hardcoding):** wins-or-neutral on ≥2 further star queries
   (candidates Q03 / Q05 / Q10) through the *same* recognizer — no per-query code.
4. **Scale:** SF=100 neutral-or-better (ideally the win grows with fact-table size).
5. **Default-OFF** throughout; a separate default-on proposal only after 1–4.

## Assets carried forward from PV.3b (~80% of the non-execution scaffolding is done)
- Recognizer: `analyze` (S1–S5 gates incl. the integer-key type-gate that rejects Q15's
  Float64 pseudo-star), `classify`, `reconstruct`.
- Custom logical node `FusedProbeNode` + `FusedProbePlanner` ExtensionPlanner
  (mechanism **b** — no fragile physical re-detection).
- `FlowQueryPlanner` wiring, gated `EMAT_PUSH_PIPELINE=1`, default-OFF path byte-identical.
- Harnesses: `pv3b_validate` (isolated A/A), `pv3b_prod_validate` (triple-walker prod A/A),
  `pv3b_q08_perf` (interleaved A/B), `pv3b_q08_profile` + `BUILD_NANOS`/`PROBE_NANOS` counters.
- Two kept perf-bug fixes: `join_on`→`.join(on)` (NLJ→HashJoin), serial→`tokio::spawn`
  parallel build.

## The core technical change (what is NEW and hard)
Convert `EmatPushPipelineExec` from **two-phase (build-all → then probe)** to
**morsel-pipelined**:

1. **Concurrent dim build.** Don't let the probe stream `await` the `OnceCell` *before*
   fact decode starts. Kick the (already `tokio::spawn`'d) dim-build tasks AND begin
   pulling/decoding fact morsels concurrently; the probe of morsel *k* awaits
   build-ready, but decode of morsels `0..k` proceeds during the build. Build (90 ms)
   hides under decode (~140 ms).
2. **Bounded morsel buffer + backpressure.** Stream fact decode into a bounded channel;
   probe consumes as morsels arrive. The bound prevents re-materializing the 60M-row
   intermediate (the very waste stock pays).
3. **Pipelined probe → partial-agg.** Each probed morsel feeds incremental aggregation;
   eliminate the adapter projection + standalone `AggregateExec`.
4. **RG-parallel lanes.** One decode→probe→partial-agg lane per partition; final agg
   merges. (This is what made PV.1 *fully* fused.)
5. **Build/probe sync primitive.** A shared readiness signal the probe lanes await,
   populated by the concurrent build — the crux of R1.

## Phases (one bounded session each unless noted)
- **PV.4.0 — overlap spike (GO/NO-GO).** Minimal change: start fact decode *before*
  awaiting the dim-build `OnceCell`, Q08 only. Instrument decode time (close the ~140 ms
  assumption). Re-run `pv3b_q08_perf` + `pv3b_q08_profile` at SF=10.
  **Gate: recover ≥ half the +53%** (fused ≤ ~235 ms, BUILD off the critical path).
  Yes → proceed. No → the operator model can't overlap; escalate to PV.4.0b (full
  RG-parallel rewrite) or STOP.
- **PV.4.1 — streaming morsel decode + bounded buffer.** Replace burst-decode with
  backpressured morsel streaming. Gate: wall improves/holds AND peak RSS doesn't balloon.
- **PV.4.2 — pipelined partial-agg fusion.** Fuse probe output → incremental agg; drop
  the adapter+agg. Gate: removes the agg share of the 66 ms remainder.
- **PV.4.3 — generalize.** Enable Q03/Q05/Q10 via the existing recognizer; tighten gates;
  re-run triple-walker 22/22. Gate: ≥2 new wins, 0 correctness regressions, no per-query code.
- **PV.4.4 — SF=100 / distributed.** Confirm win holds/grows; no regression. (Sequence
  after the held release bench.)
- **PV.4.5 — default-on proposal.** Full 22q SF=1/10/100 geomean; gating-policy decision.

## Risks & kill criteria
- **R1 — DataFusion's pull-based `Stream` resists concurrent-build-during-decode.**
  Mitigation: builds are already detached tasks; the probe stream must poll decode
  *while* awaiting build-readiness. If tokio poll/scheduling overhead eats the overlap →
  R1 fires (caught at PV.4.0).
- **R2 — the −16% was a prebuilt-probe artifact.** Even with perfect overlap, the probe
  (114 ms, partly serial tail) may keep fused ≥ stock. Mitigation: SIMD-tag the probe
  (1-byte salt, reject high-rate misses on the 900K-key orders table). If still short →
  the win wasn't real; ship dormant, redirect. (PV.4.0 surfaces this.)
- **R3 — only Q08 wins.** Not a shippable generalized pattern (violates the
  no-TPC-H-hardcoding rule for shipped wins). Ship default-OFF infra; redirect to
  SF=100/distributed where the gaps are larger.
- **Time-box:** PV.4.0 IS the go/no-go. Do not grind the full program if the spike
  doesn't move BUILD off the critical path.

## Expected value (honest)
- **Upside:** −16% Q08 + a reusable morsel operator that plausibly helps Q03/Q05/Q10 →
  a real 22q geomean move, and the engine's first morsel-pipeline primitive.
- **Downside:** multi-session spend that may confirm R2 (win was a measurement artifact).
  PV.4.0 buys that knowledge cheaply (one session) *before* the expensive 4.1–4.3 build.
- **Bottom line:** approve **PV.4.0 as a single bounded spike**; let its number be the
  program's go/no-go.
