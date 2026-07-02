# Σ.AI.2 strict interleaved A/B diff

- A summary: `A/strict-22q-summary.md`
- B summary: `B/strict-22q-summary.md`

Per-query Δ = B − A. Verdict uses 2× max(σ_A, σ_B) as the noise bar.

| Query | A median | B median | Δ ms | Δ % | bar (2σ) | verdict |
|------:|---------:|---------:|-----:|----:|---------:|:--------|
| Q08 | 1770.16 | 1854.75 | +84.59 | +4.78% | 57.42 | regression |
| Q09 | 3690.12 | 3646.26 | -43.86 | -1.19% | 54.00 | noise |

**Sum of A medians**: 5460.28 ms
**Sum of B medians**: 5501.01 ms
**Net Δ**: +40.73 ms (+0.75%)

**Clear WIN (>2σ)**: 0  
**Clear regression (>2σ)**: 1  Q08 +84.6ms
