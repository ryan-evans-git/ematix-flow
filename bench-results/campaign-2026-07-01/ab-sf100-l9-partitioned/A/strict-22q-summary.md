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
| Q05 | 2010.24 | 2014.14 | 12.80 | 0.64% | 2003.8 / 2028.4 / 2010.2 |
| Q07 | 1620.46 | 1620.44 | 4.10 | 0.25% | 1620.5 / 1624.5 / 1616.3 |
| Q08 | 1761.20 | 1761.38 | 10.91 | 0.62% | 1750.6 / 1772.4 / 1761.2 |
| Q09 | 4658.10 | 4639.61 | 51.63 | 1.11% | 4658.1 / 4581.3 / 4679.4 |

**Sum of medians**: 10050.00 ms  (primary, robust to single-run outliers)
**Sum of means**: 10035.57 ms  (secondary)
**Median CV**: 0.63%  (noise-floor across invocations)
**Max CV**: 1.11%  (worst-case per-query noise — likely single-run outlier)
