# Σ.AI.1 strict 22q bench summary — ematix

- Machine: Apple M4 Max (10P+4E), macOS 26.5.1, power: Now drawing from 'AC Power'
- Git: 068564e1fc1b on main (dirty: True)
- Data: SF=10; plan cache: off; cache policy: warm
- Engines: {'datafusion': '53.1.0', 'duckdb': '1.10503.1', 'ematix-parquet-codec': '0.17.0', 'polars': '0.52.0'}
- EMAT flags: <none>

- Runs aggregated: 5 of 6 (discarded 1 cold-start: ['run-1.md'])
- Run files: ['run-2.md', 'run-3.md', 'run-4.md', 'run-5.md', 'run-6.md']

| Query | median ms | mean ms | σ ms | CV% | per-run medians |
|------:|----------:|--------:|-----:|----:|:----------------|
| Q01 | 203.47 | 204.16 | 2.02 | 0.99% | 207.1 / 205.2 / 201.8 / 203.5 / 203.3 |

**Sum of medians**: 203.47 ms  (primary, robust to single-run outliers)
**Sum of means**: 204.16 ms  (secondary)
**Median CV**: 0.99%  (noise-floor across invocations)
**Max CV**: 0.99%  (worst-case per-query noise — likely single-run outlier)
