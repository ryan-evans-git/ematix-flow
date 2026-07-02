# Σ.AI.2 strict interleaved A/B diff

- A summary: `ematix/strict-22q-summary.md`
- B summary: `duckdb/strict-22q-summary.md`

Per-query Δ = B − A. Verdict uses 2× max(σ_A, σ_B) as the noise bar.

| Query | A median | B median | Δ ms | Δ % | bar (2σ) | verdict |
|------:|---------:|---------:|-----:|----:|---------:|:--------|
| Q01 | 2592.47 | 2378.90 | -213.57 | -8.24% | 26.54 | duckdb faster |
| Q02 | 200.01 | 304.77 | +104.76 | +52.38% | 6.50 | ematix faster |
| Q03 | 1559.33 | 1578.71 | +19.38 | +1.24% | 32.00 | noise |
| Q04 | 883.49 | 897.40 | +13.91 | +1.57% | 39.70 | noise |
| Q05 | 1947.70 | 1616.83 | -330.87 | -16.99% | 21.26 | duckdb faster |
| Q06 | 546.53 | 751.17 | +204.64 | +37.44% | 16.46 | ematix faster |
| Q07 | 1520.81 | 1674.50 | +153.69 | +10.11% | 17.62 | ematix faster |
| Q08 | 1678.90 | 2056.86 | +377.96 | +22.51% | 21.64 | ematix faster |
| Q09 | 3507.06 | 4048.84 | +541.78 | +15.45% | 238.48 | ematix faster |
| Q10 | 1989.21 | 2059.25 | +70.04 | +3.52% | 15.92 | ematix faster |
| Q11 | 179.85 | 214.04 | +34.19 | +19.01% | 5.92 | ematix faster |
| Q12 | 1034.97 | 1182.40 | +147.43 | +14.24% | 32.70 | ematix faster |
| Q13 | 1936.93 | 2390.12 | +453.19 | +23.40% | 12.72 | ematix faster |
| Q14 | 831.58 | 1052.12 | +220.54 | +26.52% | 21.66 | ematix faster |
| Q15 | 833.08 | 978.15 | +145.07 | +17.41% | 12.64 | ematix faster |
| Q16 | 404.32 | 387.96 | -16.36 | -4.05% | 10.04 | duckdb faster |
| Q17 | 1505.89 | 1571.89 | +66.00 | +4.38% | 9.62 | ematix faster |
| Q18 | 2570.14 | 2287.31 | -282.83 | -11.00% | 24.86 | duckdb faster |
| Q19 | 1237.27 | 1565.27 | +328.00 | +26.51% | 44.38 | ematix faster |
| Q20 | 1148.68 | 1276.24 | +127.56 | +11.10% | 15.54 | ematix faster |
| Q21 | 3476.91 | 4417.69 | +940.78 | +27.06% | 34.80 | ematix faster |
| Q22 | 351.08 | 574.67 | +223.59 | +63.69% | 8.00 | ematix faster |

**Sum of A medians**: 31936.21 ms
**Sum of B medians**: 35265.09 ms
**Net Δ**: +3328.88 ms (+10.42%)

**Clear duckdb faster (>2σ)**: 4  Q01 -213.6ms, Q05 -330.9ms, Q16 -16.4ms, Q18 -282.8ms
**Clear ematix faster (>2σ)**: 16  Q02 +104.8ms, Q06 +204.6ms, Q07 +153.7ms, Q08 +378.0ms, Q09 +541.8ms, Q10 +70.0ms, Q11 +34.2ms, Q12 +147.4ms, Q13 +453.2ms, Q14 +220.5ms, Q15 +145.1ms, Q17 +66.0ms, Q19 +328.0ms, Q20 +127.6ms, Q21 +940.8ms, Q22 +223.6ms
