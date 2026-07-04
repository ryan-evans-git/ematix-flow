# Σ.AI.1 strict 22q bench summary — ematix

- Machine: Apple M4 Max (10P+4E), macOS 26.5.1, power: Now drawing from 'AC Power'
- Git: 068564e1fc1b on main (dirty: True)
- Data: SF=10; plan cache: off; cache policy: warm
- Engines: {'datafusion': '53.1.0', 'duckdb': '1.10503.1', 'ematix-parquet-codec': '0.17.0', 'polars': '0.52.0'}
- EMAT flags: {'EMAT_PLAN_CACHE': '0'}

- Runs aggregated: 3 of 4 (discarded 1 cold-start: ['run-1.md'])
- Run files: ['run-2.md', 'run-3.md', 'run-4.md']

| Query | median ms | mean ms | σ ms | CV% | per-run medians |
|------:|----------:|--------:|-----:|----:|:----------------|
| Q01 | 208.46 | 207.44 | 1.80 | 0.86% | 208.5 / 205.4 / 208.5 |
| Q05 | 127.39 | 127.54 | 2.33 | 1.83% | 127.4 / 125.3 / 129.9 |

**Sum of medians**: 335.85 ms  (primary, robust to single-run outliers)
**Sum of means**: 334.98 ms  (secondary)
**Median CV**: 1.35%  (noise-floor across invocations)
**Max CV**: 1.83%  (worst-case per-query noise — likely single-run outlier)
