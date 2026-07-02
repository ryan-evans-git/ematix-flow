# Σ.AI.2 strict interleaved A/B diff

- A summary: `A/strict-22q-summary.md`
- B summary: `B/strict-22q-summary.md`

Per-query Δ = B − A. Verdict uses 2× max(σ_A, σ_B) as the noise bar.

| Query | A median | B median | Δ ms | Δ % | bar (2σ) | verdict |
|------:|---------:|---------:|-----:|----:|---------:|:--------|
| Q05 | 2010.24 | 2019.13 | +8.89 | +0.44% | 100.32 | noise |
| Q07 | 1620.46 | 1629.63 | +9.17 | +0.57% | 15.86 | noise |
| Q08 | 1761.20 | 1823.14 | +61.94 | +3.52% | 28.22 | regression |
| Q09 | 4658.10 | 4519.64 | -138.46 | -2.97% | 124.44 | WIN |

**Sum of A medians**: 10050.00 ms
**Sum of B medians**: 9991.54 ms
**Net Δ**: -58.46 ms (-0.58%)

**Clear WIN (>2σ)**: 1  Q09 -138.5ms
**Clear regression (>2σ)**: 1  Q08 +61.9ms
