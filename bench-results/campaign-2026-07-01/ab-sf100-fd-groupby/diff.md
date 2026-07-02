# Σ.AI.2 strict interleaved A/B diff

- A summary: `A/strict-22q-summary.md`
- B summary: `B/strict-22q-summary.md`

Per-query Δ = B − A. Verdict uses 2× max(σ_A, σ_B) as the noise bar.

| Query | A median | B median | Δ ms | Δ % | bar (2σ) | verdict |
|------:|---------:|---------:|-----:|----:|---------:|:--------|
| Q10 | 2894.12 | 2794.69 | -99.43 | -3.44% | 86.68 | WIN |
| Q13 | 1976.35 | 1972.62 | -3.73 | -0.19% | 35.18 | noise |

**Sum of A medians**: 4870.47 ms
**Sum of B medians**: 4767.31 ms
**Net Δ**: -103.16 ms (-2.12%)

**Clear WIN (>2σ)**: 1  Q10 -99.4ms
**Clear regression (>2σ)**: 0  
