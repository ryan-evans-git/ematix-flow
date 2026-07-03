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
| Q03 | 1577.09 | 1577.35 | 6.00 | 0.38% | 1571.5 / 1583.5 / 1577.1 |
| Q05 | 1990.45 | 1989.75 | 4.26 | 0.21% | 1985.2 / 1990.5 / 1993.6 |
| Q08 | 1725.99 | 1726.74 | 5.45 | 0.32% | 1732.5 / 1726.0 / 1721.7 |
| Q09 | 4535.30 | 4539.42 | 11.35 | 0.25% | 4535.3 / 4530.7 / 4552.3 |
| Q10 | 3086.89 | 3100.45 | 24.75 | 0.80% | 3086.9 / 3129.0 / 3085.4 |
| Q16 | 418.66 | 418.19 | 7.69 | 1.84% | 418.7 / 425.6 / 410.3 |
| Q18 | 4305.53 | 4293.45 | 136.70 | 3.17% | 4423.7 / 4305.5 / 4151.1 |
| Q21 | 3590.87 | 3592.32 | 7.68 | 0.21% | 3600.6 / 3585.5 / 3590.9 |

**Sum of medians**: 21230.78 ms  (primary, robust to single-run outliers)
**Sum of means**: 21237.68 ms  (secondary)
**Median CV**: 0.35%  (noise-floor across invocations)
**Max CV**: 3.17%  (worst-case per-query noise — likely single-run outlier)
