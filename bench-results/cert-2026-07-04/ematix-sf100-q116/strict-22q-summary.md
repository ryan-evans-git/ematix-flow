# Σ.AI.1 strict 22q bench summary — ematix

- Machine: Apple M4 Max (10P+4E), macOS 26.5.1, power: Now drawing from 'Battery Power'
- Git: a02228381f61 on integration/mesh-gate-campaign (dirty: True)
- Data: SF=100; plan cache: off; cache policy: warm
- Engines: {'datafusion': '53.1.0', 'duckdb': '1.10503.1', 'ematix-parquet-codec': '0.17.0', 'polars': '0.52.0'}
- EMAT flags: {'EMAT_PLAN_CACHE': '0'}

- Runs aggregated: 11 of 12 (discarded 1 cold-start: ['run-1.md'])
- Run files: ['run-10.md', 'run-11.md', 'run-12.md', 'run-2.md', 'run-3.md', 'run-4.md', 'run-5.md', 'run-6.md', 'run-7.md', 'run-8.md', 'run-9.md']

| Query | median ms | mean ms | σ ms | CV% | per-run medians |
|------:|----------:|--------:|-----:|----:|:----------------|
| Q01 | 2319.52 | 2330.81 | 87.89 | 3.79% | 2423.6 / 2436.2 / 2463.6 / 2218.7 / 2246.1 / 2235.7 / 2278.4 / 2267.4 / 2319.5 / 2361.7 / 2388.0 |
| Q16 | 374.97 | 374.95 | 10.41 | 2.77% | 388.1 / 388.5 / 385.3 / 361.8 / 364.4 / 363.4 / 364.5 / 371.3 / 375.0 / 382.1 / 380.0 |

**Sum of medians**: 2694.49 ms  (primary, robust to single-run outliers)
**Sum of means**: 2705.77 ms  (secondary)
**Median CV**: 3.28%  (noise-floor across invocations)
**Max CV**: 3.79%  (worst-case per-query noise — likely single-run outlier)
