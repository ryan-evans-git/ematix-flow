# Σ.AI.1 strict 22q bench summary — ematix

- Machine: Apple M4 Max (10P+4E), macOS 26.5.1, power: Now drawing from 'AC Power'
- Git: 259cf47d374a on main (dirty: True)
- Data: SF=100; plan cache: off; cache policy: warm
- Engines: {'datafusion': '53.1.0', 'duckdb': '1.10503.1', 'ematix-parquet-codec': '0.17.0', 'polars': '0.52.0'}
- EMAT flags: <none>

- Runs aggregated: 3 of 4 (discarded 1 cold-start: ['run-1.md'])
- Run files: ['run-2.md', 'run-3.md', 'run-4.md']

| Query | median ms | mean ms | σ ms | CV% | per-run medians |
|------:|----------:|--------:|-----:|----:|:----------------|
| Q08 | 1770.16 | 1760.92 | 28.71 | 1.62% | 1728.7 / 1770.2 / 1783.9 |
| Q09 | 3690.12 | 3686.24 | 20.59 | 0.56% | 3664.0 / 3704.6 / 3690.1 |

**Sum of medians**: 5460.28 ms  (primary, robust to single-run outliers)
**Sum of means**: 5447.16 ms  (secondary)
**Median CV**: 1.09%  (noise-floor across invocations)
**Max CV**: 1.62%  (worst-case per-query noise — likely single-run outlier)
