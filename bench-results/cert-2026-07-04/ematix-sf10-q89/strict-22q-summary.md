# Σ.AI.1 strict 22q bench summary — ematix

- Machine: Apple M4 Max (10P+4E), macOS 26.5.1, power: Now drawing from 'Battery Power'
- Git: a02228381f61 on integration/mesh-gate-campaign (dirty: True)
- Data: SF=10; plan cache: off; cache policy: warm
- Engines: {'datafusion': '53.1.0', 'duckdb': '1.10503.1', 'ematix-parquet-codec': '0.17.0', 'polars': '0.52.0'}
- EMAT flags: {'EMAT_PLAN_CACHE': '0'}

- Runs aggregated: 11 of 12 (discarded 1 cold-start: ['run-1.md'])
- Run files: ['run-10.md', 'run-11.md', 'run-12.md', 'run-2.md', 'run-3.md', 'run-4.md', 'run-5.md', 'run-6.md', 'run-7.md', 'run-8.md', 'run-9.md']

| Query | median ms | mean ms | σ ms | CV% | per-run medians |
|------:|----------:|--------:|-----:|----:|:----------------|
| Q08 | 141.40 | 141.82 | 2.42 | 1.71% | 141.4 / 143.0 / 140.2 / 139.8 / 144.5 / 138.9 / 139.9 / 143.9 / 144.7 / 144.8 / 138.8 |
| Q09 | 254.94 | 256.33 | 3.69 | 1.45% | 260.8 / 264.8 / 255.3 / 254.3 / 253.7 / 254.9 / 257.1 / 253.5 / 253.3 / 258.6 / 253.4 |

**Sum of medians**: 396.34 ms  (primary, robust to single-run outliers)
**Sum of means**: 398.15 ms  (secondary)
**Median CV**: 1.58%  (noise-floor across invocations)
**Max CV**: 1.71%  (worst-case per-query noise — likely single-run outlier)
