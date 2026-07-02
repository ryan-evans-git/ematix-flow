# Σ.AI.2 strict interleaved A/B diff

- A summary: `A/strict-22q-summary.md`
- B summary: `B/strict-22q-summary.md`

Per-query Δ = B − A. Verdict uses 2× max(σ_A, σ_B) as the noise bar.

| Query | A median | B median | Δ ms | Δ % | bar (2σ) | verdict |
|------:|---------:|---------:|-----:|----:|---------:|:--------|
| Q03 | 1577.09 | 1524.11 | -52.98 | -3.36% | 15.06 | WIN |
| Q05 | 1990.45 | 1918.12 | -72.33 | -3.63% | 13.20 | WIN |
| Q08 | 1725.99 | 3186.62 | +1460.63 | +84.63% | 10.90 | regression |
| Q09 | 4535.30 | 3512.24 | -1023.06 | -22.56% | 68.32 | WIN |
| Q10 | 3086.89 | 1977.49 | -1109.40 | -35.94% | 49.50 | WIN |
| Q16 | 418.66 | 406.93 | -11.73 | -2.80% | 15.38 | noise |
| Q18 | 4305.53 | 3847.58 | -457.95 | -10.64% | 683.86 | noise |
| Q21 | 3590.87 | 3440.34 | -150.53 | -4.19% | 28.72 | WIN |

**Sum of A medians**: 21230.78 ms
**Sum of B medians**: 19813.43 ms
**Net Δ**: -1417.35 ms (-6.68%)

**Clear WIN (>2σ)**: 5  Q03 -53.0ms, Q05 -72.3ms, Q09 -1023.1ms, Q10 -1109.4ms, Q21 -150.5ms
**Clear regression (>2σ)**: 1  Q08 +1460.6ms
