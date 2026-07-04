# Σ.AI.2 strict interleaved A/B diff

- A summary: `ematix-q1q5/strict-22q-summary.md`
- B summary: `duckdb-q1q5/strict-22q-summary.md`

Per-query Δ = B − A. Verdict uses 2× max(σ_A, σ_B) as the noise bar.

| Query | A median | B median | Δ ms | Δ % | bar (2σ) | verdict |
|------:|---------:|---------:|-----:|----:|---------:|:--------|
| Q01 | 208.46 | 241.59 | +33.13 | +15.89% | 3.70 | ematix faster |
| Q05 | 127.39 | 130.84 | +3.45 | +2.71% | 4.66 | noise |

**Sum of A medians**: 335.85 ms
**Sum of B medians**: 372.43 ms
**Net Δ**: +36.58 ms (+10.89%)

**Clear duckdb faster (>2σ)**: 0  
**Clear ematix faster (>2σ)**: 1  Q01 +33.1ms
