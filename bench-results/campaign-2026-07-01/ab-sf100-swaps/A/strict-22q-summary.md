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
| Q08 | 1711.57 | 1710.52 | 5.23 | 0.31% | 1711.6 / 1715.2 / 1704.8 |
| Q10 | 2889.15 | 2905.64 | 30.36 | 1.05% | 2889.2 / 2887.1 / 2940.7 |

**Sum of medians**: 4600.72 ms  (primary, robust to single-run outliers)
**Sum of means**: 4616.17 ms  (secondary)
**Median CV**: 0.68%  (noise-floor across invocations)
**Max CV**: 1.05%  (worst-case per-query noise — likely single-run outlier)
