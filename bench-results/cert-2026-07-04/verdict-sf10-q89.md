# Σ.AI.2 strict interleaved A/B diff

- A summary: `ematix-sf10-q89/strict-22q-summary.md`
- B summary: `duckdb-sf10-q89/strict-22q-summary.md`

Per-query Δ = B − A. Verdict uses 2× max(σ_A, σ_B) as the noise bar.

| Query | A median | B median | Δ ms | Δ % | bar (2σ) | verdict |
|------:|---------:|---------:|-----:|----:|---------:|:--------|
| Q08 | 141.40 | 153.76 | +12.36 | +8.74% | 4.84 | ematix faster |
| Q09 | 254.94 | 271.54 | +16.60 | +6.51% | 7.38 | ematix faster |

**Sum of A medians**: 396.34 ms
**Sum of B medians**: 425.30 ms
**Net Δ**: +28.96 ms (+7.31%)

**Clear duckdb faster (>2σ)**: 0  
**Clear ematix faster (>2σ)**: 2  Q08 +12.4ms, Q09 +16.6ms
