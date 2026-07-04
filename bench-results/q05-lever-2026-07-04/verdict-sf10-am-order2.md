# Σ.AI.2 strict interleaved A/B diff

- A summary: `ematix-sf10-am2/strict-22q-summary.md`
- B summary: `duckdb-sf10-am2/strict-22q-summary.md`

Per-query Δ = B − A. Verdict uses 2× max(σ_A, σ_B) as the noise bar.

| Query | A median | B median | Δ ms | Δ % | bar (2σ) | verdict |
|------:|---------:|---------:|-----:|----:|---------:|:--------|
| Q01 | 231.60 | 240.46 | +8.86 | +3.83% | 8.90 | noise |
| Q02 | 18.52 | 38.77 | +20.25 | +109.34% | 1.18 | ematix faster |
| Q03 | 123.37 | 135.26 | +11.89 | +9.64% | 3.36 | ematix faster |
| Q04 | 54.81 | 83.80 | +28.99 | +52.89% | 1.54 | ematix faster |
| Q05 | 124.45 | 128.97 | +4.52 | +3.63% | 3.66 | ematix faster |
| Q06 | 25.21 | 71.29 | +46.08 | +182.78% | 2.80 | ematix faster |
| Q07 | 118.47 | 132.55 | +14.08 | +11.88% | 3.10 | ematix faster |
| Q08 | 153.52 | 154.50 | +0.98 | +0.64% | 2.08 | noise |
| Q09 | 283.94 | 272.47 | -11.47 | -4.04% | 2.92 | duckdb faster |
| Q10 | 196.14 | 217.45 | +21.31 | +10.86% | 3.74 | ematix faster |
| Q11 | 11.44 | 25.38 | +13.94 | +121.85% | 0.38 | ematix faster |
| Q12 | 88.67 | 107.52 | +18.85 | +21.26% | 4.54 | ematix faster |
| Q13 | 103.03 | 230.15 | +127.12 | +123.38% | 2.86 | ematix faster |
| Q14 | 82.71 | 123.31 | +40.60 | +49.09% | 2.74 | ematix faster |
| Q15 | 60.53 | 79.11 | +18.58 | +30.70% | 1.50 | ematix faster |
| Q16 | 44.70 | 57.16 | +12.46 | +27.87% | 0.98 | ematix faster |
| Q17 | 74.61 | 145.42 | +70.81 | +94.91% | 1.54 | ematix faster |
| Q18 | 178.17 | 194.27 | +16.10 | +9.04% | 2.64 | ematix faster |
| Q19 | 129.06 | 185.08 | +56.02 | +43.41% | 3.60 | ematix faster |
| Q20 | 70.06 | 134.57 | +64.51 | +92.08% | 4.20 | ematix faster |
| Q21 | 188.53 | 365.04 | +176.51 | +93.62% | 6.88 | ematix faster |
| Q22 | 23.65 | 50.64 | +26.99 | +114.12% | 2.06 | ematix faster |

**Sum of A medians**: 2385.19 ms
**Sum of B medians**: 3173.17 ms
**Net Δ**: +787.98 ms (+33.04%)

**Clear duckdb faster (>2σ)**: 1  Q09 -11.5ms
**Clear ematix faster (>2σ)**: 19  Q02 +20.3ms, Q03 +11.9ms, Q04 +29.0ms, Q05 +4.5ms, Q06 +46.1ms, Q07 +14.1ms, Q10 +21.3ms, Q11 +13.9ms, Q12 +18.8ms, Q13 +127.1ms, Q14 +40.6ms, Q15 +18.6ms, Q16 +12.5ms, Q17 +70.8ms, Q18 +16.1ms, Q19 +56.0ms, Q20 +64.5ms, Q21 +176.5ms, Q22 +27.0ms
