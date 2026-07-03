# Σ.AI.1 strict 22q bench summary — ematix

- Machine: Apple M4 Max (10P+4E), macOS 26.5.1, power: Now drawing from 'AC Power'
- Git: 259cf47d374a on main (dirty: True)
- Data: SF=100; plan cache: off; cache policy: warm
- Engines: {'datafusion': '53.1.0', 'duckdb': '1.10503.1', 'ematix-parquet-codec': '0.17.0', 'polars': '0.52.0'}
- EMAT flags: <none>

- Runs aggregated: 3 of 4 (discarded 1 cold-start: ['run-1.md'])
- Run files: ['run-2.md', 'run-3.md', 'run-4.md']

| Query | median ms | mean ms | σ ms | CV% | per-run medians |
|------:|----------:|--------:|-----:|----:|:----------------|
| Q08 | 1854.75 | 1855.90 | 14.02 | 0.76% | 1842.5 / 1870.5 / 1854.8 |
| Q09 | 3646.26 | 3653.19 | 27.00 | 0.74% | 3646.3 / 3683.0 / 3630.3 |

**Sum of medians**: 5501.01 ms  (primary, robust to single-run outliers)
**Sum of means**: 5509.10 ms  (secondary)
**Median CV**: 0.75%  (noise-floor across invocations)
**Max CV**: 0.76%  (worst-case per-query noise — likely single-run outlier)
