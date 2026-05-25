# Phase 1 / Story 1.3 — PGO vs non-PGO baseline (22q SF=10)

Linux x86_64 measurement of L3 (PGO) against the 0.80 baseline, per
the acceptance gate in `docs/plans/CURRENT.md` Phase 1:

> 22q SF=10 geomean improves by ≥3pp on PGO build vs non-PGO with
> identical source. No per-query regression > 2%.

## Environment

- Host: minipc (Linux x86_64, kernel 6.8.0-100-generic, Ubuntu 24.04).
  Per CURRENT.md §Canonical bench hardware, Linux is the **PGO**
  canonical host: the macOS aarch64 instrumented binary crashes pre-main
  in vendored OpenSSL's C++ static constructors. M3 Pro remains the
  canonical non-PGO host for V5 closure targets, but the PGO ratio
  itself is the acceptance-gate input here.
- Toolchain: `stable` (rustc 1.95.0), `cargo-pgo` 0.3.0, `llvm-tools`
  (preview), per `rust-toolchain.toml`.
- Source: `feat/pgo-instrumented-build` @ `d2853cc`.
- Workload: 22 TPC-H queries on SF=10 parquet, `ematix-flow` only
  (`TPCH_SKIP_DUCKDB=1 TPCH_SKIP_POLARS=1`). 10 timed trials after
  2 warmups, median ± σ.
- Full bench env: `EMAT_RG_DECODE_CACHE=1 EMAT_RH_SUM_F64=1` per
  `feedback_full_bench_env_checklist.md` (the 0.80 baseline assumes
  these).

## PGO training

- Pipeline: `scripts/pgo/build-instrumented.sh` →
  `scripts/pgo/train.sh` → `scripts/pgo/optimize.sh`.
- Training workload (OQ-PGO-B): 22q SF=10 single iteration, no warmup,
  ematix-only — same shape as the bench workload. One non-empty
  `.profraw` file emitted; `cargo pgo optimize` merged it and rebuilt
  with `-C profile-use`.
- Binary sizes: non-PGO 227 MB → PGO 196 MB (-14%).

## Results

| Q   | non-PGO (ms) |   PGO (ms)   |    Δ ms |    Δ % |
|----:|-------------:|-------------:|--------:|-------:|
| Q01 |   441.10     |   419.54     |  -21.56 |  -4.89 |
| Q02 |    76.69     |    75.83     |   -0.86 |  -1.12 |
| Q03 |   455.35     |   453.12     |   -2.23 |  -0.49 |
| Q04 |    99.16     |    99.97     |   +0.81 |  +0.82 |
| Q05 |   540.53     |   547.91     |   +7.38 |  +1.37 |
| Q06 |   130.48     |   130.06     |   -0.42 |  -0.32 |
| Q07 |   320.00     |   322.78     |   +2.78 |  +0.87 |
| Q08 |   703.98     |   701.94     |   -2.04 |  -0.29 |
| Q09 |  1140.91     |  1132.38     |   -8.53 |  -0.75 |
| Q10 |   437.13     |   441.16     |   +4.03 |  +0.92 |
| Q11 |    22.24     |    22.33     |   +0.09 |  +0.40 |
| Q12 |   184.35     |   169.58     |  -14.77 |  -8.01 |
| Q13 |   317.16     |   317.16     |    0.00 |   0.00 |
| Q14 |   150.67     |   153.77     |   +3.10 |  **+2.06** |
| Q15 |   147.35     |   145.41     |   -1.94 |  -1.32 |
| Q16 |   122.68     |   118.49     |   -4.19 |  -3.42 |
| Q17 |   439.03     |   427.49     |  -11.54 |  -2.63 |
| Q18 |   634.79     |   632.47     |   -2.32 |  -0.37 |
| Q19 |   225.33     |   219.68     |   -5.65 |  -2.51 |
| Q20 |   300.38     |   296.39     |   -3.99 |  -1.33 |
| Q21 |   825.63     |   821.94     |   -3.69 |  -0.45 |
| Q22 |    64.83     |    58.84     |   -5.99 |  -9.24 |

**Sums:** 7779.77 → 7708.24 ms (-71.53 ms, -0.92%).
**Geomean ratio (PGO / non-PGO):** 0.9856 → **-1.44pp**.

## Acceptance-gate verdict — **does not meet bar**

| Criterion                          | Required | Observed | Status |
|------------------------------------|---------:|---------:|:------:|
| Geomean improvement                |   ≥ 3pp  |   1.44pp |   ❌   |
| Per-query regression               |   ≤ 2%   |   +2.06% (Q14) |   ❌   |

Headline wins are Q22 (-9.24%) and Q12 (-8.01%); both are CPU-bound
group-by / scan shapes where codegen quality plausibly moves the
needle. The bulk of the workload (Q03 / Q05 / Q07–Q09 multi-join) is
within ±1.5pp — consistent with the V5 §5.2 finding that SF=10 on
commodity x86 is memory-bandwidth-bound on the multi-join queries.
PGO can't widen memory bandwidth.

Q14's +2.06% regression is borderline noise (σ on the run is ±6.64
ms — half the delta) but technically breaches the gate.

## Recommendation

Do **not** ship PGO as the L3 lever on this hardware basis. Options:

1. **Defer to M3 Pro reading** — once a Linux equivalent of the
   canonical M3 Pro baseline exists, re-measure there. The V5 plan
   notes the PGO ratio is "hardware-independent at this granularity,"
   but the observed +1.44pp on minipc is well below the 3pp target;
   M3 Pro is unlikely to swing it past the bar by 1.5pp+ either.
2. **Re-train on broader workload** — single-iteration profile may
   under-represent the long-tail multi-join codepath. A 3-iteration
   training run + microbench supplement could shift the profile.
   Estimated cost: rerun 1.2 + 1.3, ~1 hour.
3. **Drop L3 from V5** — accept that on the canonical hardware mix
   (minipc-as-PGO-host, M3 Pro-as-baseline), PGO doesn't clear the
   3pp bar. Move calendar weight to Phase 2 (L13 custom hash join,
   2-4pp at SF=1, 10-13pp at SF=10) which V5 forecasts higher.

Story 1.4 (CI hook) is gated on this baseline meeting the bar; it
should not land until either option 1 or 2 demonstrates the gate
is achievable.

## Raw bench outputs

- `/tmp/bench_nopgo.md` — non-PGO baseline
- `/tmp/bench_pgo.md` — PGO build
