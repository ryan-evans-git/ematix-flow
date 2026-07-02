# Σ.AI.2 strict interleaved A/B diff

- A summary: `A/strict-22q-summary.md`
- B summary: `B/strict-22q-summary.md`

Per-query Δ = B − A. Verdict uses 2× max(σ_A, σ_B) as the noise bar.

| Query | A median | B median | Δ ms | Δ % | bar (2σ) | verdict |
|------:|---------:|---------:|-----:|----:|---------:|:--------|
| Q08 | 1711.57 | 1703.54 | -8.03 | -0.47% | 21.16 | noise |
| Q10 | 2889.15 | 1942.46 | -946.69 | -32.77% | 60.72 | WIN |

**Sum of A medians**: 4600.72 ms
**Sum of B medians**: 3646.00 ms
**Net Δ**: -954.72 ms (-20.75%)

**Clear WIN (>2σ)**: 1  Q10 -946.7ms
**Clear regression (>2σ)**: 0  
