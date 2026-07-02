# Σ.AI.2 strict interleaved A/B diff

- A summary: `ematix/strict-22q-summary.md`
- B summary: `duckdb/strict-22q-summary.md`

Per-query Δ = B − A. Verdict uses 2× max(σ_A, σ_B) as the noise bar.

| Query | A median | B median | Δ ms | Δ % | bar (2σ) | verdict |
|------:|---------:|---------:|-----:|----:|---------:|:--------|
| Q01 | 2490.41 | 2339.03 | -151.38 | -6.08% | 40.72 | duckdb faster |
| Q02 | 229.09 | 292.83 | +63.74 | +27.82% | 7.28 | ematix faster |
| Q03 | 1565.10 | 1537.90 | -27.20 | -1.74% | 9.88 | duckdb faster |
| Q04 | 875.22 | 854.95 | -20.27 | -2.32% | 22.88 | noise |
| Q05 | 1989.94 | 1570.17 | -419.77 | -21.09% | 50.06 | duckdb faster |
| Q06 | 527.06 | 747.40 | +220.34 | +41.81% | 10.14 | ematix faster |
| Q07 | 1569.44 | 1623.03 | +53.59 | +3.41% | 27.38 | ematix faster |
| Q08 | 1707.77 | 1995.33 | +287.56 | +16.84% | 25.26 | ematix faster |
| Q09 | 4496.79 | 4383.15 | -113.64 | -2.53% | 87.18 | duckdb faster |
| Q10 | 2839.69 | 1979.85 | -859.84 | -30.28% | 205.60 | duckdb faster |
| Q11 | 214.32 | 205.45 | -8.87 | -4.14% | 9.22 | noise |
| Q12 | 1015.59 | 1130.29 | +114.70 | +11.29% | 30.18 | ematix faster |
| Q13 | 1964.77 | 2315.79 | +351.02 | +17.87% | 16.38 | ematix faster |
| Q14 | 818.04 | 1018.46 | +200.42 | +24.50% | 17.62 | ematix faster |
| Q15 | 815.34 | 952.35 | +137.01 | +16.80% | 33.10 | ematix faster |
| Q16 | 392.98 | 377.54 | -15.44 | -3.93% | 7.48 | duckdb faster |
| Q17 | 1475.89 | 1518.51 | +42.62 | +2.89% | 21.44 | ematix faster |
| Q18 | 4087.79 | 2578.07 | -1509.72 | -36.93% | 647.76 | duckdb faster |
| Q19 | 1203.15 | 1507.07 | +303.92 | +25.26% | 22.56 | ematix faster |
| Q20 | 1229.47 | 1248.19 | +18.72 | +1.52% | 17.56 | ematix faster |
| Q21 | 3511.62 | 4266.60 | +754.98 | +21.50% | 37.04 | ematix faster |
| Q22 | 435.26 | 571.26 | +136.00 | +31.25% | 5.24 | ematix faster |

**Sum of A medians**: 35454.73 ms
**Sum of B medians**: 35013.22 ms
**Net Δ**: -441.51 ms (-1.25%)

**Clear duckdb faster (>2σ)**: 7  Q01 -151.4ms, Q03 -27.2ms, Q05 -419.8ms, Q09 -113.6ms, Q10 -859.8ms, Q16 -15.4ms, Q18 -1509.7ms
**Clear ematix faster (>2σ)**: 13  Q02 +63.7ms, Q06 +220.3ms, Q07 +53.6ms, Q08 +287.6ms, Q12 +114.7ms, Q13 +351.0ms, Q14 +200.4ms, Q15 +137.0ms, Q17 +42.6ms, Q19 +303.9ms, Q20 +18.7ms, Q21 +755.0ms, Q22 +136.0ms
