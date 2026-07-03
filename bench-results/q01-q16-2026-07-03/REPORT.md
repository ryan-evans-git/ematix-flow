# Q01 + Q16 SF=100 fresh verdicts — 2026-07-03

Same-session solo passes, both engines, strict protocol, post
rule-chain-unification binary (81aad51 era; Q01/Q16 plans byte-identical
through unification — this is a re-verdict, not a shape change).

| Query | ematix | DuckDB | Δ | bar (2σ) | Verdict |
|---|---:|---:|---:|---:|---|
| Q01 | 2203.06 ms | 2276.25 ms | +73.2 ms | 46.2 | **ematix clear WIN** (was −214 ms) |
| Q16 | 364.13 ms | 361.05 ms | −3.1 ms | 8.0 | noise (was −16 ms) |

Neither historical loss reproduces on the corrected harness with fresh
co-measured DuckDB. No Q01 parallelism dig required. SF=100 standing:
**19W / 0L / 3 noise (Q03, Q04, Q16)** — zero clear DuckDB wins remain.
