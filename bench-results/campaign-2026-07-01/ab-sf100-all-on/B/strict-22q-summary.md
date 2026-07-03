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
| Q03 | 1524.11 | 1524.51 | 7.53 | 0.49% | 1532.2 / 1517.2 / 1524.1 |
| Q05 | 1918.12 | 1921.68 | 6.60 | 0.34% | 1917.6 / 1929.3 / 1918.1 |
| Q08 | 3186.62 | 3185.77 | 2.15 | 0.07% | 3183.3 / 3186.6 / 3187.4 |
| Q09 | 3512.24 | 3497.12 | 34.16 | 0.97% | 3521.1 / 3512.2 / 3458.0 |
| Q10 | 1977.49 | 1982.54 | 13.94 | 0.71% | 1998.3 / 1971.8 / 1977.5 |
| Q16 | 406.93 | 407.97 | 1.96 | 0.48% | 406.9 / 410.2 / 406.8 |
| Q18 | 3847.58 | 3991.28 | 341.93 | 8.89% | 4381.6 / 3847.6 / 3744.7 |
| Q21 | 3440.34 | 3440.61 | 14.36 | 0.42% | 3455.1 / 3426.4 / 3440.3 |

**Sum of medians**: 19813.43 ms  (primary, robust to single-run outliers)
**Sum of means**: 19951.48 ms  (secondary)
**Median CV**: 0.49%  (noise-floor across invocations)
**Max CV**: 8.89%  (worst-case per-query noise — likely single-run outlier)
