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
| Q10 | 2794.69 | 2812.12 | 43.34 | 1.55% | 2794.7 / 2861.5 / 2780.2 |
| Q13 | 1972.62 | 1966.29 | 16.91 | 0.86% | 1979.1 / 1972.6 / 1947.1 |

**Sum of medians**: 4767.31 ms  (primary, robust to single-run outliers)
**Sum of means**: 4778.41 ms  (secondary)
**Median CV**: 1.20%  (noise-floor across invocations)
**Max CV**: 1.55%  (worst-case per-query noise — likely single-run outlier)
