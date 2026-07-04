# Σ.AI.2 strict interleaved A/B diff

- A summary: `ematix-sf10-settled/strict-22q-summary.md`
- B summary: `duckdb-sf10-settled/strict-22q-summary.md`

Per-query Δ = B − A. Verdict uses 2× max(σ_A, σ_B) as the noise bar.

| Query | A median | B median | Δ ms | Δ % | bar (2σ) | verdict |
|------:|---------:|---------:|-----:|----:|---------:|:--------|
| Q01 | 240.37 | 316.16 | +75.79 | +31.53% | 37.44 | ematix faster |
| Q02 | 18.57 | 45.07 | +26.50 | +142.70% | 4.52 | ematix faster |
| Q03 | 134.11 | 171.51 | +37.40 | +27.89% | 18.72 | ematix faster |
| Q04 | 61.34 | 107.04 | +45.70 | +74.50% | 11.66 | ematix faster |
| Q05 | 131.55 | 171.05 | +39.50 | +30.03% | 19.74 | ematix faster |
| Q06 | 26.62 | 91.39 | +64.77 | +243.31% | 9.90 | ematix faster |
| Q07 | 128.99 | 179.06 | +50.07 | +38.82% | 20.44 | ematix faster |
| Q08 | 160.32 | 210.37 | +50.05 | +31.22% | 24.06 | ematix faster |
| Q09 | 299.10 | 386.34 | +87.24 | +29.17% | 48.54 | ematix faster |
| Q10 | 214.63 | 267.53 | +52.90 | +24.65% | 26.10 | ematix faster |
| Q11 | 12.20 | 30.13 | +17.93 | +146.97% | 3.28 | ematix faster |
| Q12 | 95.04 | 142.21 | +47.17 | +49.63% | 27.96 | ematix faster |
| Q13 | 113.95 | 327.89 | +213.94 | +187.75% | 26.40 | ematix faster |
| Q14 | 90.63 | 152.56 | +61.93 | +68.33% | 9.24 | ematix faster |
| Q15 | 68.05 | 104.01 | +35.96 | +52.84% | 10.62 | ematix faster |
| Q16 | 47.84 | 67.71 | +19.87 | +41.53% | 2.26 | ematix faster |
| Q17 | 84.71 | 192.56 | +107.85 | +127.32% | 11.40 | ematix faster |
| Q18 | 199.21 | 271.20 | +71.99 | +36.14% | 9.64 | ematix faster |
| Q19 | 136.72 | 227.32 | +90.60 | +66.27% | 8.00 | ematix faster |
| Q20 | 77.84 | 160.55 | +82.71 | +106.26% | 6.14 | ematix faster |
| Q21 | 202.62 | 501.04 | +298.42 | +147.28% | 5.98 | ematix faster |
| Q22 | 24.02 | 64.48 | +40.46 | +168.44% | 1.54 | ematix faster |

**Sum of A medians**: 2568.43 ms
**Sum of B medians**: 4187.18 ms
**Net Δ**: +1618.75 ms (+63.02%)

**Clear duckdb faster (>2σ)**: 0  
**Clear ematix faster (>2σ)**: 22  Q01 +75.8ms, Q02 +26.5ms, Q03 +37.4ms, Q04 +45.7ms, Q05 +39.5ms, Q06 +64.8ms, Q07 +50.1ms, Q08 +50.1ms, Q09 +87.2ms, Q10 +52.9ms, Q11 +17.9ms, Q12 +47.2ms, Q13 +213.9ms, Q14 +61.9ms, Q15 +36.0ms, Q16 +19.9ms, Q17 +107.9ms, Q18 +72.0ms, Q19 +90.6ms, Q20 +82.7ms, Q21 +298.4ms, Q22 +40.5ms
