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
| Q08 | 1703.54 | 1706.64 | 10.58 | 0.62% | 1718.4 / 1703.5 / 1698.0 |
| Q10 | 1942.46 | 1937.83 | 9.36 | 0.48% | 1944.0 / 1942.5 / 1927.1 |

**Sum of medians**: 3646.00 ms  (primary, robust to single-run outliers)
**Sum of means**: 3644.47 ms  (secondary)
**Median CV**: 0.55%  (noise-floor across invocations)
**Max CV**: 0.62%  (worst-case per-query noise — likely single-run outlier)
