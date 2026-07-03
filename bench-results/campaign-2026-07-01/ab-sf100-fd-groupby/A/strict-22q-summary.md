# Σ.AI.1 strict 22q bench summary — ematix

- Machine: Apple M4 Max (10P+4E), macOS 26.5.1, power: Now drawing from 'AC Power'
- Git: 1bc9a08d0819 on integration/campaign-2026-07-01 (dirty: True)
- Data: SF=100; plan cache: off; cache policy: warm
- Engines: {'datafusion': '53.1.0', 'duckdb': '1.10503.1', 'ematix-parquet-codec': '0.17.0', 'polars': '0.52.0'}
- EMAT flags: <none>

- Runs aggregated: 3 of 4 (discarded 1 cold-start: ['run-1.md'])
- Run files: ['run-2.md', 'run-3.md', 'run-4.md']

| Query | median ms | mean ms | σ ms | CV% | per-run medians |
|------:|----------:|--------:|-----:|----:|:----------------|
| Q10 | 2894.12 | 2902.25 | 17.39 | 0.60% | 2894.1 / 2890.4 / 2922.2 |
| Q13 | 1976.35 | 1968.74 | 17.59 | 0.89% | 1976.3 / 1981.2 / 1948.6 |

**Sum of medians**: 4870.47 ms  (primary, robust to single-run outliers)
**Sum of means**: 4870.99 ms  (secondary)
**Median CV**: 0.75%  (noise-floor across invocations)
**Max CV**: 0.89%  (worst-case per-query noise — likely single-run outlier)
