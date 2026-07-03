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
| Q09 | 4632.75 | 4645.90 | 31.98 | 0.69% | 4622.6 / 4632.8 / 4682.4 |
| Q10 | 2971.13 | 2971.96 | 18.16 | 0.61% | 2954.2 / 2971.1 / 2990.5 |

**Sum of medians**: 7603.88 ms  (primary, robust to single-run outliers)
**Sum of means**: 7617.86 ms  (secondary)
**Median CV**: 0.65%  (noise-floor across invocations)
**Max CV**: 0.69%  (worst-case per-query noise — likely single-run outlier)
