# Σ.AI.2 strict interleaved A/B diff

- A summary: `ematix-sf100-q05/strict-22q-summary.md`
- B summary: `duckdb-sf100-q05/strict-22q-summary.md`

Per-query Δ = B − A. Verdict uses 2× max(σ_A, σ_B) as the noise bar.

| Query | A median | B median | Δ ms | Δ % | bar (2σ) | verdict |
|------:|---------:|---------:|-----:|----:|---------:|:--------|
| Q05 | 1330.55 | 1504.79 | +174.24 | +13.10% | 18.62 | ematix faster |

**Sum of A medians**: 1330.55 ms
**Sum of B medians**: 1504.79 ms
**Net Δ**: +174.24 ms (+13.10%)

**Clear duckdb faster (>2σ)**: 0  
**Clear ematix faster (>2σ)**: 1  Q05 +174.2ms
