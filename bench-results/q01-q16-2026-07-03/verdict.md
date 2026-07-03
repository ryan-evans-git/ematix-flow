# Σ.AI.2 strict interleaved A/B diff

- A summary: `ematix-sf100/strict-22q-summary.md`
- B summary: `duckdb-sf100/strict-22q-summary.md`

Per-query Δ = B − A. Verdict uses 2× max(σ_A, σ_B) as the noise bar.

| Query | A median | B median | Δ ms | Δ % | bar (2σ) | verdict |
|------:|---------:|---------:|-----:|----:|---------:|:--------|
| Q01 | 2203.06 | 2276.25 | +73.19 | +3.32% | 46.20 | ematix faster |
| Q16 | 364.13 | 361.05 | -3.08 | -0.85% | 8.00 | noise |

**Sum of A medians**: 2567.19 ms
**Sum of B medians**: 2637.30 ms
**Net Δ**: +70.11 ms (+2.73%)

**Clear duckdb faster (>2σ)**: 0  
**Clear ematix faster (>2σ)**: 1  Q01 +73.2ms
