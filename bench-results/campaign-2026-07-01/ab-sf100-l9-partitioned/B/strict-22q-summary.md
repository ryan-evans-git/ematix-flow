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
| Q05 | 2019.13 | 2044.82 | 50.16 | 2.48% | 2019.1 / 2102.6 / 2012.7 |
| Q07 | 1629.63 | 1628.81 | 7.93 | 0.49% | 1629.6 / 1636.3 / 1620.5 |
| Q08 | 1823.14 | 1823.72 | 14.11 | 0.77% | 1809.9 / 1838.1 / 1823.1 |
| Q09 | 4519.64 | 4549.03 | 62.22 | 1.38% | 4620.5 / 4506.9 / 4519.6 |

**Sum of medians**: 9991.54 ms  (primary, robust to single-run outliers)
**Sum of means**: 10046.37 ms  (secondary)
**Median CV**: 1.08%  (noise-floor across invocations)
**Max CV**: 2.48%  (worst-case per-query noise — likely single-run outlier)
