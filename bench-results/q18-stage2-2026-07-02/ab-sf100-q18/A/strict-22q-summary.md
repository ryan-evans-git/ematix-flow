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
| Q18 | 2400.39 | 2408.51 | 40.74 | 1.70% | 2372.4 / 2400.4 / 2452.7 |

**Sum of medians**: 2400.39 ms  (primary, robust to single-run outliers)
**Sum of means**: 2408.51 ms  (secondary)
**Median CV**: 1.70%  (noise-floor across invocations)
**Max CV**: 1.70%  (worst-case per-query noise — likely single-run outlier)
