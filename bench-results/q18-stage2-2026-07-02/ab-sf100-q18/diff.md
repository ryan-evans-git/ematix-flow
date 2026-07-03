# Σ.AI.2 strict interleaved A/B diff

- A summary: `A/strict-22q-summary.md`
- B summary: `B/strict-22q-summary.md`

Per-query Δ = B − A. Verdict uses 2× max(σ_A, σ_B) as the noise bar.

| Query | A median | B median | Δ ms | Δ % | bar (2σ) | verdict |
|------:|---------:|---------:|-----:|----:|---------:|:--------|
| Q18 | 2400.39 | 2028.50 | -371.89 | -15.49% | 109.78 | WIN |

**Sum of A medians**: 2400.39 ms
**Sum of B medians**: 2028.50 ms
**Net Δ**: -371.89 ms (-15.49%)

**Clear WIN (>2σ)**: 1  Q18 -371.9ms
**Clear regression (>2σ)**: 0  
