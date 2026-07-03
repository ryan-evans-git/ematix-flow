# Σ.AI.2 strict interleaved A/B diff

- A summary: `ematix-sf10/strict-22q-summary.md`
- B summary: `duckdb-sf10/strict-22q-summary.md`

Per-query Δ = B − A. Verdict uses 2× max(σ_A, σ_B) as the noise bar.

| Query | A median | B median | Δ ms | Δ % | bar (2σ) | verdict |
|------:|---------:|---------:|-----:|----:|---------:|:--------|
| Q01 | 248.21 | 246.15 | -2.06 | -0.83% | 20.26 | noise |
| Q02 | 19.27 | 39.23 | +19.96 | +103.58% | 0.98 | ematix faster |
| Q03 | 129.27 | 135.99 | +6.72 | +5.20% | 13.18 | noise |
| Q04 | 58.45 | 84.97 | +26.52 | +45.37% | 5.90 | ematix faster |
| Q05 | 137.33 | 132.06 | -5.27 | -3.84% | 7.30 | noise |
| Q06 | 25.40 | 75.54 | +50.14 | +197.40% | 5.54 | ematix faster |
| Q07 | 118.35 | 135.24 | +16.89 | +14.27% | 7.84 | ematix faster |
| Q08 | 146.69 | 157.32 | +10.63 | +7.25% | 11.02 | noise |
| Q09 | 271.20 | 287.92 | +16.72 | +6.17% | 23.82 | noise |
| Q10 | 192.72 | 221.48 | +28.76 | +14.92% | 20.74 | ematix faster |
| Q11 | 11.41 | 25.41 | +14.00 | +122.70% | 0.56 | ematix faster |
| Q12 | 86.45 | 108.99 | +22.54 | +26.07% | 8.62 | ematix faster |
| Q13 | 102.40 | 232.85 | +130.45 | +127.39% | 9.00 | ematix faster |
| Q14 | 82.01 | 125.52 | +43.51 | +53.05% | 3.26 | ematix faster |
| Q15 | 61.76 | 81.11 | +19.35 | +31.33% | 4.16 | ematix faster |
| Q16 | 44.22 | 57.40 | +13.18 | +29.81% | 1.32 | ematix faster |
| Q17 | 80.29 | 147.09 | +66.80 | +83.20% | 4.42 | ematix faster |
| Q18 | 177.73 | 199.20 | +21.47 | +12.08% | 4.44 | ematix faster |
| Q19 | 122.03 | 188.93 | +66.90 | +54.82% | 4.62 | ematix faster |
| Q20 | 91.88 | 136.71 | +44.83 | +48.79% | 2.56 | ematix faster |
| Q21 | 180.33 | 374.52 | +194.19 | +107.69% | 7.22 | ematix faster |
| Q22 | 23.74 | 52.65 | +28.91 | +121.78% | 1.18 | ematix faster |

**Sum of A medians**: 2411.14 ms
**Sum of B medians**: 3246.28 ms
**Net Δ**: +835.14 ms (+34.64%)

**Clear duckdb faster (>2σ)**: 0  
**Clear ematix faster (>2σ)**: 17  Q02 +20.0ms, Q04 +26.5ms, Q06 +50.1ms, Q07 +16.9ms, Q10 +28.8ms, Q11 +14.0ms, Q12 +22.5ms, Q13 +130.4ms, Q14 +43.5ms, Q15 +19.4ms, Q16 +13.2ms, Q17 +66.8ms, Q18 +21.5ms, Q19 +66.9ms, Q20 +44.8ms, Q21 +194.2ms, Q22 +28.9ms
