# Σ.AI.1 strict 22q bench summary — duckdb

- Machine: Apple M4 Max (10P+4E), macOS 26.5.1, power: Now drawing from 'AC Power'
- Git: 068564e1fc1b on main (dirty: True)
- Data: SF=10; plan cache: off; cache policy: warm
- Engines: {'datafusion': '53.1.0', 'duckdb': '1.10503.1', 'ematix-parquet-codec': '0.17.0', 'polars': '0.52.0'}
- EMAT flags: {'EMAT_PLAN_CACHE': '0'}

- Runs aggregated: 3 of 4 (discarded 1 cold-start: ['run-1.md'])
- Run files: ['run-2.md', 'run-3.md', 'run-4.md']

| Query | median ms | mean ms | σ ms | CV% | per-run medians |
|------:|----------:|--------:|-----:|----:|:----------------|
| Q01 | 241.59 | 241.47 | 1.85 | 0.76% | 243.2 / 239.6 / 241.6 |
| Q05 | 130.84 | 129.82 | 2.07 | 1.58% | 131.2 / 130.8 / 127.4 |

**Sum of medians**: 372.43 ms  (primary, robust to single-run outliers)
**Sum of means**: 371.29 ms  (secondary)
**Median CV**: 1.17%  (noise-floor across invocations)
**Max CV**: 1.58%  (worst-case per-query noise — likely single-run outlier)
