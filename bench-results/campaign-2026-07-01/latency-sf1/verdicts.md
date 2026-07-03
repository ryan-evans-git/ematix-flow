# Σ.AI.2 strict interleaved A/B diff

- A summary: `ematix/strict-22q-summary.md`
- B summary: `duckdb/strict-22q-summary.md`

Per-query Δ = B − A. Verdict uses 2× max(σ_A, σ_B) as the noise bar.

| Query | A median | B median | Δ ms | Δ % | bar (2σ) | verdict |
|------:|---------:|---------:|-----:|----:|---------:|:--------|
| Q01 | 17.30 | 47.88 | +30.58 | +176.76% | 1.20 | ematix faster |
| Q02 | 7.16 | 16.96 | +9.80 | +136.87% | 0.42 | ematix faster |
| Q03 | 12.21 | 31.91 | +19.70 | +161.34% | 0.60 | ematix faster |
| Q04 | 10.83 | 22.10 | +11.27 | +104.06% | 0.20 | ematix faster |
| Q05 | 16.12 | 30.60 | +14.48 | +89.83% | 0.58 | ematix faster |
| Q06 | 1.67 | 13.16 | +11.49 | +688.02% | 0.16 | ematix faster |
| Q07 | 24.23 | 32.77 | +8.54 | +35.25% | 0.42 | ematix faster |
| Q08 | 13.86 | 38.11 | +24.25 | +174.96% | 0.78 | ematix faster |
| Q09 | 17.58 | 54.50 | +36.92 | +210.01% | 0.56 | ematix faster |
| Q10 | 20.22 | 40.50 | +20.28 | +100.30% | 0.46 | ematix faster |
| Q11 | 5.33 | 9.22 | +3.89 | +72.98% | 0.38 | ematix faster |
| Q12 | 14.34 | 24.99 | +10.65 | +74.27% | 0.50 | ematix faster |
| Q13 | 8.23 | 133.31 | +125.08 | +1519.81% | 0.46 | ematix faster |
| Q14 | 11.23 | 22.05 | +10.82 | +96.35% | 0.20 | ematix faster |
| Q15 | 11.28 | 14.05 | +2.77 | +24.56% | 0.76 | ematix faster |
| Q16 | 9.11 | 21.25 | +12.14 | +133.26% | 0.20 | ematix faster |
| Q17 | 14.16 | 24.83 | +10.67 | +75.35% | 0.36 | ematix faster |
| Q18 | 17.54 | 44.37 | +26.83 | +152.96% | 0.44 | ematix faster |
| Q19 | 16.52 | 33.97 | +17.45 | +105.63% | 0.44 | ematix faster |
| Q20 | 11.79 | 28.83 | +17.04 | +144.53% | 0.70 | ematix faster |
| Q21 | 32.28 | 72.75 | +40.47 | +125.37% | 0.28 | ematix faster |
| Q22 | 3.92 | 10.61 | +6.69 | +170.66% | 0.18 | ematix faster |

**Sum of A medians**: 296.91 ms
**Sum of B medians**: 768.72 ms
**Net Δ**: +471.81 ms (+158.91%)

**Clear duckdb faster (>2σ)**: 0  
**Clear ematix faster (>2σ)**: 22  Q01 +30.6ms, Q02 +9.8ms, Q03 +19.7ms, Q04 +11.3ms, Q05 +14.5ms, Q06 +11.5ms, Q07 +8.5ms, Q08 +24.2ms, Q09 +36.9ms, Q10 +20.3ms, Q11 +3.9ms, Q12 +10.6ms, Q13 +125.1ms, Q14 +10.8ms, Q15 +2.8ms, Q16 +12.1ms, Q17 +10.7ms, Q18 +26.8ms, Q19 +17.4ms, Q20 +17.0ms, Q21 +40.5ms, Q22 +6.7ms
