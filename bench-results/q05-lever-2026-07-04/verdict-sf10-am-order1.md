# Σ.AI.2 strict interleaved A/B diff

- A summary: `ematix-sf10-am1/strict-22q-summary.md`
- B summary: `duckdb-sf10-am1/strict-22q-summary.md`

Per-query Δ = B − A. Verdict uses 2× max(σ_A, σ_B) as the noise bar.

| Query | A median | B median | Δ ms | Δ % | bar (2σ) | verdict |
|------:|---------:|---------:|-----:|----:|---------:|:--------|
| Q01 | 207.64 | 243.38 | +35.74 | +17.21% | 5.36 | ematix faster |
| Q02 | 18.10 | 39.46 | +21.36 | +118.01% | 0.84 | ematix faster |
| Q03 | 117.35 | 133.70 | +16.35 | +13.93% | 2.62 | ematix faster |
| Q04 | 52.52 | 83.31 | +30.79 | +58.63% | 6.14 | ematix faster |
| Q05 | 121.14 | 128.11 | +6.97 | +5.75% | 4.84 | ematix faster |
| Q06 | 23.99 | 72.93 | +48.94 | +204.00% | 3.48 | ematix faster |
| Q07 | 111.75 | 132.82 | +21.07 | +18.85% | 2.84 | ematix faster |
| Q08 | 140.68 | 154.34 | +13.66 | +9.71% | 7.04 | ematix faster |
| Q09 | 255.53 | 271.04 | +15.51 | +6.07% | 9.24 | ematix faster |
| Q10 | 187.27 | 221.20 | +33.93 | +18.12% | 2.36 | ematix faster |
| Q11 | 11.35 | 25.41 | +14.06 | +123.88% | 0.82 | ematix faster |
| Q12 | 82.31 | 110.93 | +28.62 | +34.77% | 2.50 | ematix faster |
| Q13 | 96.57 | 231.63 | +135.06 | +139.86% | 5.22 | ematix faster |
| Q14 | 79.68 | 124.32 | +44.64 | +56.02% | 3.74 | ematix faster |
| Q15 | 59.07 | 79.64 | +20.57 | +34.82% | 2.74 | ematix faster |
| Q16 | 42.98 | 57.12 | +14.14 | +32.90% | 2.02 | ematix faster |
| Q17 | 73.58 | 145.43 | +71.85 | +97.65% | 4.82 | ematix faster |
| Q18 | 172.39 | 195.36 | +22.97 | +13.32% | 2.22 | ematix faster |
| Q19 | 118.19 | 185.76 | +67.57 | +57.17% | 5.10 | ematix faster |
| Q20 | 70.02 | 133.65 | +63.63 | +90.87% | 2.54 | ematix faster |
| Q21 | 175.01 | 367.07 | +192.06 | +109.74% | 14.80 | ematix faster |
| Q22 | 23.89 | 51.28 | +27.39 | +114.65% | 1.02 | ematix faster |

**Sum of A medians**: 2241.01 ms
**Sum of B medians**: 3187.89 ms
**Net Δ**: +946.88 ms (+42.25%)

**Clear duckdb faster (>2σ)**: 0  
**Clear ematix faster (>2σ)**: 22  Q01 +35.7ms, Q02 +21.4ms, Q03 +16.3ms, Q04 +30.8ms, Q05 +7.0ms, Q06 +48.9ms, Q07 +21.1ms, Q08 +13.7ms, Q09 +15.5ms, Q10 +33.9ms, Q11 +14.1ms, Q12 +28.6ms, Q13 +135.1ms, Q14 +44.6ms, Q15 +20.6ms, Q16 +14.1ms, Q17 +71.9ms, Q18 +23.0ms, Q19 +67.6ms, Q20 +63.6ms, Q21 +192.1ms, Q22 +27.4ms
