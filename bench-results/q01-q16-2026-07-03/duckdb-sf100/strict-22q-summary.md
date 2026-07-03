# Σ.AI.1 strict 22q bench summary — duckdb

- Machine: Apple M4 Max (10P+4E), macOS 26.5.1, power: Now drawing from 'AC Power'
- Git: 81aad512223f on main (dirty: True)
- Data: SF=100; plan cache: off; cache policy: warm
- Engines: {'datafusion': '53.1.0', 'duckdb': '1.10503.1', 'ematix-parquet-codec': '0.17.0', 'polars': '0.52.0'}
- EMAT flags: {'EMAT_PLAN_CACHE': '0'}

- Runs aggregated: 3 of 4 (discarded 1 cold-start: ['run-1.md'])
- Run files: ['run-2.md', 'run-3.md', 'run-4.md']

| Query | median ms | mean ms | σ ms | CV% | per-run medians |
|------:|----------:|--------:|-----:|----:|:----------------|
| Q01 | 2276.25 | 2282.62 | 13.88 | 0.61% | 2298.5 / 2273.1 / 2276.2 |
| Q16 | 361.05 | 361.80 | 1.43 | 0.40% | 360.9 / 361.1 / 363.4 |

**Sum of medians**: 2637.30 ms  (primary, robust to single-run outliers)
**Sum of means**: 2644.42 ms  (secondary)
**Median CV**: 0.50%  (noise-floor across invocations)
**Max CV**: 0.61%  (worst-case per-query noise — likely single-run outlier)
