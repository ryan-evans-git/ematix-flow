# Σ.AI.2 strict interleaved A/B diff

- A summary: `A/strict-22q-summary.md`
- B summary: `B/strict-22q-summary.md`

Per-query Δ = B − A. Verdict uses 2× max(σ_A, σ_B) as the noise bar.

| Query | A median | B median | Δ ms | Δ % | bar (2σ) | verdict |
|------:|---------:|---------:|-----:|----:|---------:|:--------|
| Q09 | 4632.75 | 3558.16 | -1074.59 | -23.20% | 66.62 | WIN |
| Q10 | 2971.13 | 2933.65 | -37.48 | -1.26% | 166.38 | noise |

**Sum of A medians**: 7603.88 ms
**Sum of B medians**: 6491.81 ms
**Net Δ**: -1112.07 ms (-14.63%)

**Clear WIN (>2σ)**: 1  Q09 -1074.6ms
**Clear regression (>2σ)**: 0  
