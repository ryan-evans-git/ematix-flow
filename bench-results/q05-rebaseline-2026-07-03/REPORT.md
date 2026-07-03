# Q05 re-baseline — 2026-07-03 (post rule-chain unification)

Context: the rule-chain unification (merge of `chore/unify-rule-chains`)
revealed the strict bench binary had been planning Q05 WITHOUT the
production FlowQueryPlanner passes (no transitive dim-semi splice, zero
L9 wraps) at both scales. All historical strict Q05 numbers measured a
non-production plan. This directory is the corrected baseline
(binary at the unification merge; production shape verified: 6 emitters
at SF=10, 2 at SF=100).

| Scale | ematix median | DuckDB median | Verdict |
|---|---:|---:|---|
| SF=10 | 129.00 ms | 128.21 ms | noise (tie; was "−17 ms loss") |
| SF=100 | 1330.55 ms | 1504.79 ms | **ematix clear WIN +174.2 ms (+13.1%)** (was "−331 ms loss") |

Cascade lever (Σ.Q05.CHAIN) strict A/B at SF=10, auto vs
EMAT_L9_CASCADE=0: −0.58 ms, noise — the production shape's own pass-1
blooms deliver the value; the chain lever stays conservative-AUTO.

Raw: `ab-sf10-cascade/` (A arm = auto = ematix SF10 solo),
`duckdb-sf10-q05/`, `ematix-sf100-q05/`, `duckdb-sf100-q05/`,
`verdict-sf10.md`, `verdict-sf100.md`, per-run env.json.
