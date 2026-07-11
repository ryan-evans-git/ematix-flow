# Σ.AI.1 strict 22q bench summary — duckdb

- Machine: Apple M4 Max (10P+4E), macOS 26.5.1, power: Now drawing from 'Battery Power'
- Git: a02228381f61 on integration/mesh-gate-campaign (dirty: True)
- Data: SF=10; plan cache: off; cache policy: warm
- Engines: {'datafusion': '53.1.0', 'duckdb': '1.10503.1', 'ematix-parquet-codec': '0.17.0', 'polars': '0.52.0'}
- EMAT flags: {'EMAT_PLAN_CACHE': '0'}

- Runs aggregated: 11 of 12 (discarded 1 cold-start: ['run-1.md'])
- Run files: ['run-10.md', 'run-11.md', 'run-12.md', 'run-2.md', 'run-3.md', 'run-4.md', 'run-5.md', 'run-6.md', 'run-7.md', 'run-8.md', 'run-9.md']

| Query | median ms | mean ms | σ ms | CV% | per-run medians |
|------:|----------:|--------:|-----:|----:|:----------------|
| Q08 | 153.76 | 153.72 | 1.12 | 0.73% | 152.9 / 155.4 / 155.6 / 153.8 / 152.3 / 153.1 / 154.0 / 154.6 / 154.0 / 153.2 / 152.2 |
| Q09 | 271.54 | 271.62 | 2.03 | 0.75% | 269.4 / 271.5 / 271.6 / 273.9 / 271.7 / 275.2 / 269.9 / 270.4 / 270.1 / 274.5 / 269.7 |

**Sum of medians**: 425.30 ms  (primary, robust to single-run outliers)
**Sum of means**: 425.35 ms  (secondary)
**Median CV**: 0.74%  (noise-floor across invocations)
**Max CV**: 0.75%  (worst-case per-query noise — likely single-run outlier)
