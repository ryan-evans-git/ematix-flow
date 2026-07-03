# Σ.AI.2 strict interleaved A/B diff

- A summary: `ematix-sf1/strict-22q-summary.md`
- B summary: `duckdb-sf1/strict-22q-summary.md`

Per-query Δ = B − A. Verdict uses 2× max(σ_A, σ_B) as the noise bar.

| Query | A median | B median | Δ ms | Δ % | bar (2σ) | verdict |
|------:|---------:|---------:|-----:|----:|---------:|:--------|
| Q01 | 19.48 | 49.32 | +29.84 | +153.18% | 1.38 | ematix faster |
| Q02 | 7.53 | 18.28 | +10.75 | +142.76% | 0.68 | ematix faster |
| Q03 | 13.10 | 32.78 | +19.68 | +150.23% | 0.82 | ematix faster |
| Q04 | 11.31 | 23.31 | +12.00 | +106.10% | 0.92 | ematix faster |
| Q05 | 12.05 | 31.42 | +19.37 | +160.75% | 0.64 | ematix faster |
| Q06 | 1.73 | 13.39 | +11.66 | +673.99% | 0.46 | ematix faster |
| Q07 | 25.18 | 34.30 | +9.12 | +36.22% | 1.64 | ematix faster |
| Q08 | 14.68 | 39.44 | +24.76 | +168.66% | 1.66 | ematix faster |
| Q09 | 18.74 | 56.33 | +37.59 | +200.59% | 1.46 | ematix faster |
| Q10 | 20.66 | 41.99 | +21.33 | +103.24% | 0.36 | ematix faster |
| Q11 | 5.58 | 9.82 | +4.24 | +75.99% | 0.30 | ematix faster |
| Q12 | 14.80 | 25.44 | +10.64 | +71.89% | 0.90 | ematix faster |
| Q13 | 9.04 | 142.94 | +133.90 | +1481.19% | 2.62 | ematix faster |
| Q14 | 11.49 | 23.03 | +11.54 | +100.44% | 0.60 | ematix faster |
| Q15 | 11.31 | 14.86 | +3.55 | +31.39% | 0.40 | ematix faster |
| Q16 | 9.31 | 21.94 | +12.63 | +135.66% | 0.52 | ematix faster |
| Q17 | 14.17 | 25.77 | +11.60 | +81.86% | 0.72 | ematix faster |
| Q18 | 18.27 | 45.81 | +27.54 | +150.74% | 0.92 | ematix faster |
| Q19 | 17.60 | 36.36 | +18.76 | +106.59% | 0.24 | ematix faster |
| Q20 | 12.35 | 30.41 | +18.06 | +146.23% | 0.24 | ematix faster |
| Q21 | 34.27 | 77.90 | +43.63 | +127.31% | 2.90 | ematix faster |
| Q22 | 4.20 | 11.16 | +6.96 | +165.71% | 0.46 | ematix faster |

**Sum of A medians**: 306.85 ms
**Sum of B medians**: 806.00 ms
**Net Δ**: +499.15 ms (+162.67%)

**Clear duckdb faster (>2σ)**: 0  
**Clear ematix faster (>2σ)**: 22  Q01 +29.8ms, Q02 +10.8ms, Q03 +19.7ms, Q04 +12.0ms, Q05 +19.4ms, Q06 +11.7ms, Q07 +9.1ms, Q08 +24.8ms, Q09 +37.6ms, Q10 +21.3ms, Q11 +4.2ms, Q12 +10.6ms, Q13 +133.9ms, Q14 +11.5ms, Q15 +3.5ms, Q16 +12.6ms, Q17 +11.6ms, Q18 +27.5ms, Q19 +18.8ms, Q20 +18.1ms, Q21 +43.6ms, Q22 +7.0ms
