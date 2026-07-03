# Σ.AI.2 strict interleaved A/B diff

- A summary: `A/strict-22q-summary.md`
- B summary: `duckdb-sf10-q05/strict-22q-summary.md`

Per-query Δ = B − A. Verdict uses 2× max(σ_A, σ_B) as the noise bar.

| Query | A median | B median | Δ ms | Δ % | bar (2σ) | verdict |
|------:|---------:|---------:|-----:|----:|---------:|:--------|
| Q05 | 129.00 | 128.21 | -0.79 | -0.61% | 2.58 | noise |

**Sum of A medians**: 129.00 ms
**Sum of B medians**: 128.21 ms
**Net Δ**: -0.79 ms (-0.61%)

**Clear duckdb faster (>2σ)**: 0  
**Clear ematix faster (>2σ)**: 0  
