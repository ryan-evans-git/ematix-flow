# Σ.AI.2 strict interleaved A/B diff

- A summary: `ematix-sf10-full/strict-22q-summary.md`
- B summary: `duckdb-sf10-full/strict-22q-summary.md`

Per-query Δ = B − A. Verdict uses 2× max(σ_A, σ_B) as the noise bar.

| Query | A median | B median | Δ ms | Δ % | bar (2σ) | verdict |
|------:|---------:|---------:|-----:|----:|---------:|:--------|
| Q01 | 307.06 | 294.95 | -12.11 | -3.94% | 6.84 | duckdb faster |
| Q02 | 20.81 | 41.43 | +20.62 | +99.09% | 0.92 | ematix faster |
| Q03 | 150.45 | 158.92 | +8.47 | +5.63% | 3.66 | ematix faster |
| Q04 | 70.13 | 100.77 | +30.64 | +43.69% | 2.32 | ematix faster |
| Q05 | 152.30 | 159.75 | +7.45 | +4.89% | 4.26 | ematix faster |
| Q06 | 29.14 | 90.59 | +61.45 | +210.88% | 1.84 | ematix faster |
| Q07 | 157.43 | 163.47 | +6.04 | +3.84% | 5.40 | ematix faster |
| Q08 | 195.20 | 191.06 | -4.14 | -2.12% | 3.14 | duckdb faster |
| Q09 | 363.16 | 347.28 | -15.88 | -4.37% | 12.30 | duckdb faster |
| Q10 | 251.08 | 252.41 | +1.33 | +0.53% | 2.92 | noise |
| Q11 | 13.85 | 28.34 | +14.49 | +104.62% | 1.24 | ematix faster |
| Q12 | 114.92 | 126.49 | +11.57 | +10.07% | 4.04 | ematix faster |
| Q13 | 133.55 | 292.65 | +159.10 | +119.13% | 3.06 | ematix faster |
| Q14 | 100.98 | 140.48 | +39.50 | +39.12% | 2.88 | ematix faster |
| Q15 | 74.96 | 95.30 | +20.34 | +27.13% | 4.18 | ematix faster |
| Q16 | 52.88 | 64.22 | +11.34 | +21.44% | 2.62 | ematix faster |
| Q17 | 97.80 | 179.40 | +81.60 | +83.44% | 7.20 | ematix faster |
| Q18 | 231.22 | 250.30 | +19.08 | +8.25% | 6.58 | ematix faster |
| Q19 | 165.80 | 213.83 | +48.03 | +28.97% | 6.66 | ematix faster |
| Q20 | 84.82 | 152.68 | +67.86 | +80.00% | 0.52 | ematix faster |
| Q21 | 242.36 | 458.65 | +216.29 | +89.24% | 19.20 | ematix faster |
| Q22 | 27.21 | 59.89 | +32.68 | +120.10% | 1.14 | ematix faster |

**Sum of A medians**: 3037.11 ms
**Sum of B medians**: 3862.86 ms
**Net Δ**: +825.75 ms (+27.19%)

**Clear duckdb faster (>2σ)**: 3  Q01 -12.1ms, Q08 -4.1ms, Q09 -15.9ms
**Clear ematix faster (>2σ)**: 18  Q02 +20.6ms, Q03 +8.5ms, Q04 +30.6ms, Q05 +7.4ms, Q06 +61.5ms, Q07 +6.0ms, Q11 +14.5ms, Q12 +11.6ms, Q13 +159.1ms, Q14 +39.5ms, Q15 +20.3ms, Q16 +11.3ms, Q17 +81.6ms, Q18 +19.1ms, Q19 +48.0ms, Q20 +67.9ms, Q21 +216.3ms, Q22 +32.7ms
