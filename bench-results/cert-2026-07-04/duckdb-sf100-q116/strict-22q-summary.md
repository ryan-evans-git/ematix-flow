# Σ.AI.1 strict 22q bench summary — duckdb

- Machine: Apple M4 Max (10P+4E), macOS 26.5.1, power: Now drawing from 'Battery Power'
- Git: a02228381f61 on integration/mesh-gate-campaign (dirty: True)
- Data: SF=100; plan cache: off; cache policy: warm
- Engines: {'datafusion': '53.1.0', 'duckdb': '1.10503.1', 'ematix-parquet-codec': '0.17.0', 'polars': '0.52.0'}
- EMAT flags: {'EMAT_PLAN_CACHE': '0'}

- Runs aggregated: 11 of 12 (discarded 1 cold-start: ['run-1.md'])
- Run files: ['run-10.md', 'run-11.md', 'run-12.md', 'run-2.md', 'run-3.md', 'run-4.md', 'run-5.md', 'run-6.md', 'run-7.md', 'run-8.md', 'run-9.md']

| Query | median ms | mean ms | σ ms | CV% | per-run medians |
|------:|----------:|--------:|-----:|----:|:----------------|
| Q01 | 2343.21 | 2339.11 | 14.98 | 0.64% | 2313.9 / 2338.8 / 2343.2 / 2359.2 / 2331.0 / 2345.6 / 2352.8 / 2356.2 / 2343.3 / 2317.3 / 2328.8 |
| Q16 | 375.71 | 375.80 | 1.29 | 0.34% | 375.0 / 375.3 / 377.6 / 375.7 / 377.0 / 377.5 / 376.7 / 373.5 / 374.6 / 375.9 / 375.1 |

**Sum of medians**: 2718.92 ms  (primary, robust to single-run outliers)
**Sum of means**: 2714.91 ms  (secondary)
**Median CV**: 0.49%  (noise-floor across invocations)
**Max CV**: 0.64%  (worst-case per-query noise — likely single-run outlier)
