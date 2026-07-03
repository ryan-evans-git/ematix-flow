# Throughput re-baseline — 2026-07-03 (concurrency-aware partitions)

Strict throughput protocol (solo engines, seeded permutations, 4 batches
first-discarded, inflight cap 10 @SF10 / 3 @SF100, 6 GB memory gate).
Binary: post `feat/concurrency-aware-partitions` merge. "auto" =
EMAT_TARGET_PARTITIONS unset → cross-process registry sensing;
"legacy" = EMAT_TARGET_PARTITIONS=0 (per-process all-cores).

## SF=10 (QPH)

| Streams | ematix auto | ematix legacy (same session) | DuckDB | vs DuckDB |
|---|---:|---:|---:|---|
| 1 | **29,519** | — | 22,006 | ematix 1.34× |
| 10 | 26,882 | 10,756 | 29,426 | duckdb +9.5% |
| 100 | 25,709 | — | 28,461 | duckdb +10.7% |

AUTO recovers +150% at s10 vs legacy (10,756 → 26,882), within 1% of the
campaign's hand-tuned PARTITIONS=2 diagnostic (27,232) — the
productization fully captures the diagnostic's win with zero operator
tuning. The historical 3.5×/2.6× collapses are eliminated.

## SF=100 (QPH)

| Streams | ematix auto | DuckDB | vs DuckDB |
|---|---:|---:|---|
| 1 | **2,562** | 2,047 | ematix 1.25× |
| 10 | **1,940** | 1,609 | ematix 1.21× |

ematix now wins SF=100 throughput at both stream counts (s10 was
"parity, high variance" in the 2026-07-01 campaign).

## Remaining gap and the arc that closes it

SF=10 multi-stream trails DuckDB by ~9-11%. Cause (per campaign
analysis): DuckDB's in-process morsel work-stealing degrades gracefully
under preemption; ematix's static per-plan partitioning cannot rebalance
mid-query. Closing it is a scheduler arc (work-stealing / morsel-style
execution), not a lever. Registry-based sensing is the right product
default until then.

Raw: `sf10-auto/`, `sf10-s10-legacy/`, `sf100-auto/` (per-engine
per-stream batch.json + p50/p95/p99 tables + env.json).
