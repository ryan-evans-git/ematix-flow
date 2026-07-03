# Σ.AI.1 strict 22q bench summary — ematix

- Machine: Apple M4 Max (10P+4E), macOS 26.5.1, power: Now drawing from 'AC Power'
- Git: 81aad512223f on main (dirty: True)
- Data: SF=100; plan cache: off; cache policy: warm
- Engines: {'datafusion': '53.1.0', 'duckdb': '1.10503.1', 'ematix-parquet-codec': '0.17.0', 'polars': '0.52.0'}
- EMAT flags: {'EMAT_PLAN_CACHE': '0'}

- Runs aggregated: 3 of 4 (discarded 1 cold-start: ['run-1.md'])
- Run files: ['run-2.md', 'run-3.md', 'run-4.md']

| Query | median ms | mean ms | σ ms | CV% | per-run medians |
|------:|----------:|--------:|-----:|----:|:----------------|
| Q01 | 2203.06 | 2209.79 | 23.10 | 1.05% | 2190.8 / 2203.1 / 2235.5 |
| Q16 | 364.13 | 365.21 | 4.00 | 1.10% | 364.1 / 361.9 / 369.6 |

**Sum of medians**: 2567.19 ms  (primary, robust to single-run outliers)
**Sum of means**: 2575.00 ms  (secondary)
**Median CV**: 1.07%  (noise-floor across invocations)
**Max CV**: 1.10%  (worst-case per-query noise — likely single-run outlier)
