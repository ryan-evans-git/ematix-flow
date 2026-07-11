# Σ.AI.2 strict interleaved A/B diff

- A summary: `ematix-sf100-q116/strict-22q-summary.md`
- B summary: `duckdb-sf100-q116/strict-22q-summary.md`

Per-query Δ = B − A. Verdict uses 2× max(σ_A, σ_B) as the noise bar.

| Query | A median | B median | Δ ms | Δ % | bar (2σ) | verdict |
|------:|---------:|---------:|-----:|----:|---------:|:--------|
| Q01 | 2319.52 | 2343.21 | +23.69 | +1.02% | 175.78 | noise |
| Q16 | 374.97 | 375.71 | +0.74 | +0.20% | 20.82 | noise |

**Sum of A medians**: 2694.49 ms
**Sum of B medians**: 2718.92 ms
**Net Δ**: +24.43 ms (+0.91%)

**Clear duckdb faster (>2σ)**: 0  
**Clear ematix faster (>2σ)**: 0  
