# Σ.AI.2 strict interleaved A/B diff

- A summary: `ematix/strict-22q-summary.md`
- B summary: `duckdb/strict-22q-summary.md`

Per-query Δ = B − A. Verdict uses 2× max(σ_A, σ_B) as the noise bar.

| Query | A median | B median | Δ ms | Δ % | bar (2σ) | verdict |
|------:|---------:|---------:|-----:|----:|---------:|:--------|
| Q01 | 226.72 | 245.04 | +18.32 | +8.08% | 8.64 | ematix faster |
| Q02 | 18.82 | 38.90 | +20.08 | +106.70% | 0.74 | ematix faster |
| Q03 | 116.79 | 135.11 | +18.32 | +15.69% | 4.64 | ematix faster |
| Q04 | 52.76 | 82.44 | +29.68 | +56.25% | 1.62 | ematix faster |
| Q05 | 147.04 | 130.08 | -16.96 | -11.53% | 1.72 | duckdb faster |
| Q06 | 24.61 | 71.24 | +46.63 | +189.48% | 1.76 | ematix faster |
| Q07 | 109.18 | 133.38 | +24.20 | +22.17% | 1.94 | ematix faster |
| Q08 | 141.90 | 154.92 | +13.02 | +9.18% | 6.32 | ematix faster |
| Q09 | 258.11 | 275.70 | +17.59 | +6.81% | 7.62 | ematix faster |
| Q10 | 185.93 | 219.47 | +33.54 | +18.04% | 3.80 | ematix faster |
| Q11 | 11.37 | 25.14 | +13.77 | +121.11% | 0.54 | ematix faster |
| Q12 | 84.47 | 110.51 | +26.04 | +30.83% | 3.94 | ematix faster |
| Q13 | 94.10 | 232.20 | +138.10 | +146.76% | 2.88 | ematix faster |
| Q14 | 78.12 | 123.98 | +45.86 | +58.70% | 3.50 | ematix faster |
| Q15 | 58.58 | 78.99 | +20.41 | +34.84% | 3.06 | ematix faster |
| Q16 | 42.96 | 56.89 | +13.93 | +32.43% | 1.14 | ematix faster |
| Q17 | 73.26 | 145.65 | +72.39 | +98.81% | 1.84 | ematix faster |
| Q18 | 169.94 | 195.35 | +25.41 | +14.95% | 3.74 | ematix faster |
| Q19 | 119.17 | 184.60 | +65.43 | +54.90% | 6.94 | ematix faster |
| Q20 | 87.82 | 131.14 | +43.32 | +49.33% | 4.20 | ematix faster |
| Q21 | 182.86 | 364.72 | +181.86 | +99.45% | 3.54 | ematix faster |
| Q22 | 23.15 | 50.62 | +27.47 | +118.66% | 0.64 | ematix faster |

**Sum of A medians**: 2307.66 ms
**Sum of B medians**: 3186.07 ms
**Net Δ**: +878.41 ms (+38.06%)

**Clear duckdb faster (>2σ)**: 1  Q05 -17.0ms
**Clear ematix faster (>2σ)**: 21  Q01 +18.3ms, Q02 +20.1ms, Q03 +18.3ms, Q04 +29.7ms, Q06 +46.6ms, Q07 +24.2ms, Q08 +13.0ms, Q09 +17.6ms, Q10 +33.5ms, Q11 +13.8ms, Q12 +26.0ms, Q13 +138.1ms, Q14 +45.9ms, Q15 +20.4ms, Q16 +13.9ms, Q17 +72.4ms, Q18 +25.4ms, Q19 +65.4ms, Q20 +43.3ms, Q21 +181.9ms, Q22 +27.5ms
