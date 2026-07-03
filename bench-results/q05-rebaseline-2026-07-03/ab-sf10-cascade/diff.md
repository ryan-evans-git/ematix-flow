# Σ.AI.2 strict interleaved A/B diff

- A summary: `A/strict-22q-summary.md`
- B summary: `B/strict-22q-summary.md`

Per-query Δ = B − A. Verdict uses 2× max(σ_A, σ_B) as the noise bar.

| Query | A median | B median | Δ ms | Δ % | bar (2σ) | verdict |
|------:|---------:|---------:|-----:|----:|---------:|:--------|
| Q05 | 129.00 | 128.42 | -0.58 | -0.45% | 2.36 | noise |

**Sum of A medians**: 129.00 ms
**Sum of B medians**: 128.42 ms
**Net Δ**: -0.58 ms (-0.45%)

**Clear WIN (>2σ)**: 0  
**Clear regression (>2σ)**: 0  
