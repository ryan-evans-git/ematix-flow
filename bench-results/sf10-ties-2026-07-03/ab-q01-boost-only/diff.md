# Σ.AI.2 strict interleaved A/B diff

- A summary: `A/strict-22q-summary.md`
- B summary: `B/strict-22q-summary.md`

Per-query Δ = B − A. Verdict uses 2× max(σ_A, σ_B) as the noise bar.

| Query | A median | B median | Δ ms | Δ % | bar (2σ) | verdict |
|------:|---------:|---------:|-----:|----:|---------:|:--------|
| Q01 | 217.26 | 203.47 | -13.79 | -6.35% | 5.66 | WIN |

**Sum of A medians**: 217.26 ms
**Sum of B medians**: 203.47 ms
**Net Δ**: -13.79 ms (-6.35%)

**Clear WIN (>2σ)**: 1  Q01 -13.8ms
**Clear regression (>2σ)**: 0  
