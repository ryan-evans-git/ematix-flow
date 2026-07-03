# Σ.AI.1 strict 22q bench summary — ematix

- Machine: Apple M4 Max (10P+4E), macOS 26.5.1, power: Now drawing from 'AC Power'
- Git: e5fa14865964 on main (dirty: True)
- Data: SF=100; plan cache: off; cache policy: warm
- Engines: {'datafusion': '53.1.0', 'duckdb': '1.10503.1', 'ematix-parquet-codec': '0.17.0', 'polars': '0.52.0'}
- EMAT flags: <none>

- Runs aggregated: 3 of 4 (discarded 1 cold-start: ['run-1.md'])
- Run files: ['run-2.md', 'run-3.md', 'run-4.md']

| Query | median ms | mean ms | σ ms | CV% | per-run medians |
|------:|----------:|--------:|-----:|----:|:----------------|
| Q18 | 2028.50 | 2034.11 | 54.89 | 2.71% | 1982.2 / 2028.5 / 2091.6 |

**Sum of medians**: 2028.50 ms  (primary, robust to single-run outliers)
**Sum of means**: 2034.11 ms  (secondary)
**Median CV**: 2.71%  (noise-floor across invocations)
**Max CV**: 2.71%  (worst-case per-query noise — likely single-run outlier)
