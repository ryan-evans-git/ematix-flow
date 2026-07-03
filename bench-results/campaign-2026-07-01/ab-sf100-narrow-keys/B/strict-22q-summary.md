# Σ.AI.1 strict 22q bench summary — ematix

- Machine: Apple M4 Max (10P+4E), macOS 26.5.1, power: Now drawing from 'AC Power'
- Git: 1bc9a08d0819 on integration/campaign-2026-07-01 (dirty: True)
- Data: SF=100; plan cache: off; cache policy: warm
- Engines: {'datafusion': '53.1.0', 'duckdb': '1.10503.1', 'ematix-parquet-codec': '0.17.0', 'polars': '0.52.0'}
- EMAT flags: <none>

- Runs aggregated: 3 of 4 (discarded 1 cold-start: ['run-1.md'])
- Run files: ['run-2.md', 'run-3.md', 'run-4.md']

| Query | median ms | mean ms | σ ms | CV% | per-run medians |
|------:|----------:|--------:|-----:|----:|:----------------|
| Q09 | 3558.16 | 3577.13 | 33.31 | 0.94% | 3557.6 / 3615.6 / 3558.2 |
| Q10 | 2933.65 | 2961.04 | 83.19 | 2.84% | 2933.7 / 2895.0 / 3054.5 |

**Sum of medians**: 6491.81 ms  (primary, robust to single-run outliers)
**Sum of means**: 6538.17 ms  (secondary)
**Median CV**: 1.89%  (noise-floor across invocations)
**Max CV**: 2.84%  (worst-case per-query noise — likely single-run outlier)
