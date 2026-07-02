#!/usr/bin/env bash
#
# Σ.AI.3 — strict concurrent-stream throughput bench (TPC-H throughput style).
#
# Measures each engine under N ∈ {1,10,100} SIMULTANEOUS query streams.
# Each stream is one process running all 22 queries in a stream-specific
# deterministic permutation (seeded shuffle — streams don't stampede the
# same query at the same moment). Engines are measured one at a time,
# never concurrently with each other, in alternating engine order across
# batches to avoid block-order thermal bias.
#
# Per (engine, N): BATCHES batches are run; the FIRST is discarded as
# cold-start. Reported per combo: median makespan, QPH, per-query
# p50/p95/p99 across streams (see strict_throughput_summarize.py).
#
# Process model: one bench-binary process per stream, launched
# concurrently; each runs its 22 queries sequentially with
# TPCH_TRIALS=1 TPCH_WARMUPS=0 (throughput tests measure sustained
# load, not steady-state repeats). Engine defaults govern per-process
# threading (ematix: target_partitions = cores, override PARTITIONS=N;
# DuckDB: its own defaults) — at N=100 on a laptop this oversubscribes
# heavily BY DESIGN; that contention is the thing being measured.
#
# MEMORY WARNING: N=100 at SF=100 means 100 processes against ~34 GB of
# parquet. On a 36 GB machine expect page-cache thrash; consider
# --streams "1,10" for SF=100 or run on a bigger box.
#
# Usage:
#   strict_throughput.sh [--sf N] [--streams "1,10,100"]
#                        [--engines "ematix,duckdb"] [--batches 4]
#                        [--plan-cache on|off] [--out PATH]
#
# Outputs:
#   $OUT/<engine>/s<N>/batch-<b>/stream-<s>.md
#   $OUT/<engine>/s<N>/batch-<b>/batch.json
#   $OUT/env.json
#   $OUT/throughput-summary.md

set -euo pipefail

SF=10
STREAMS="1,10,100"
ENGINES="ematix,duckdb"
BATCHES=4                  # first discarded per (engine, N)
PLAN_CACHE="off"           # per-process cache is fresh anyway; kept for parity
OUT="/tmp/strict-throughput-$(date +%Y%m%d-%H%M%S)"

usage() { sed -n '2,40p' "$0"; }

while [[ $# -gt 0 ]]; do
    case "$1" in
        --sf) SF="$2"; shift 2 ;;
        --streams) STREAMS="$2"; shift 2 ;;
        --engines) ENGINES="$2"; shift 2 ;;
        --batches) BATCHES="$2"; shift 2 ;;
        --plan-cache) PLAN_CACHE="$2"; shift 2 ;;
        --out) OUT="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unknown arg: $1" >&2; usage; exit 2 ;;
    esac
done

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
# shellcheck source=scripts/bench/strict_common.sh
source "$REPO/scripts/bench/strict_common.sh"

BIN="$REPO/target/release/examples/tpch_triangulation_bench"
DATA="$REPO/examples/tpch/data/sf$SF"

[[ -x "$BIN" ]] || { echo "ERROR: build first: cargo build --release --example tpch_triangulation_bench --features triangulation" >&2; exit 1; }
[[ -d "$DATA" ]] || { echo "ERROR: no data at $DATA" >&2; exit 1; }

if [[ "$PLAN_CACHE" == "on" ]]; then PC_ENV="EMAT_PLAN_CACHE=1"; else PC_ENV="EMAT_PLAN_CACHE=0"; fi

mkdir -p "$OUT"
capture_env "$OUT" "sf=$SF" "streams=$STREAMS" "engines=$ENGINES" \
    "batches=$BATCHES" "plan_cache=$PLAN_CACHE" "mode=throughput"

# Deterministic per-stream query permutation (seeded shuffle).
perm_for_stream() {
    python3 -c "import random; qs=list(range(1,23)); random.Random($1).shuffle(qs); print(','.join(map(str,qs)))"
}

engine_skip_env() {
    case "$1" in
        ematix) echo "TPCH_SKIP_DUCKDB=1 TPCH_SKIP_POLARS=1" ;;
        duckdb) echo "TPCH_SKIP_EMATIX=1 TPCH_SKIP_POLARS=1" ;;
        polars) echo "TPCH_SKIP_EMATIX=1 TPCH_SKIP_DUCKDB=1" ;;
        *) echo "ERROR: unknown engine '$1'" >&2; return 1 ;;
    esac
}

run_batch() {
    local engine="$1" n="$2" batch="$3"
    local bdir="$OUT/$engine/s$n/batch-$batch"
    mkdir -p "$bdir"
    if [[ -s "$bdir/batch.json" ]]; then
        echo "  $engine s$n batch $batch: SKIP (batch.json exists — resume)"
        return 0
    fi
    local skip_env
    skip_env="$(engine_skip_env "$engine")"

    thermal_wait 120
    local pre_therm; pre_therm="$(thermal_state)"

    local start_ms end_ms
    start_ms="$(python3 -c 'import time; print(int(time.time()*1000))')"
    local pids=()
    for s in $(seq 1 "$n"); do
        local perm; perm="$(perm_for_stream "$s")"
        # shellcheck disable=SC2086
        caffeinate -i /usr/bin/env $skip_env $PC_ENV \
            "TPCH_DATA_DIR=$DATA" "TPCH_TRIALS=1" "TPCH_WARMUPS=0" \
            "TPCH_QUERIES=$perm" "TPCH_OUT=$bdir/stream-$s.md" \
            "$BIN" >"$bdir/stream-$s.log" 2>&1 &
        pids+=($!)
    done
    local failures=0
    for pid in "${pids[@]}"; do
        wait "$pid" || failures=$((failures + 1))
    done
    end_ms="$(python3 -c 'import time; print(int(time.time()*1000))')"

    local post_therm; post_therm="$(thermal_state)"
    cat > "$bdir/batch.json" <<EOF
{
  "engine": "$engine",
  "sf": $SF,
  "streams": $n,
  "batch": $batch,
  "makespan_ms": $((end_ms - start_ms)),
  "stream_failures": $failures,
  "thermal_pre": "$pre_therm",
  "thermal_post": "$post_therm"
}
EOF
    if (( failures > 0 )); then
        echo "  WARNING: $failures/$n streams FAILED for $engine s$n batch $batch (see $bdir/stream-*.log)" >&2
    fi
    echo "  $engine s$n batch $batch: makespan $((end_ms - start_ms)) ms, failures $failures"
}

echo "=== Σ.AI.3 strict throughput bench ==="
echo "  sf: $SF | streams: $STREAMS | engines: $ENGINES | batches: $BATCHES (first discarded)"
echo "  out: $OUT"
echo

IFS=',' read -ra ENG_ARR <<< "$ENGINES"
IFS=',' read -ra N_ARR <<< "$STREAMS"

# Alternate engine order per batch (A/B then B/A) so neither engine
# always runs on the cooler box.
for n in "${N_ARR[@]}"; do
    echo "--- stream count $n ---"
    for b in $(seq 1 "$BATCHES"); do
        if (( b % 2 == 1 )); then ORDER=("${ENG_ARR[@]}"); else
            ORDER=()
            for (( i=${#ENG_ARR[@]}-1; i>=0; i-- )); do ORDER+=("${ENG_ARR[$i]}"); done
        fi
        for engine in "${ORDER[@]}"; do
            run_batch "$engine" "$n" "$b"
        done
    done
done

python3 "$REPO/scripts/bench/strict_throughput_summarize.py" \
    --run-dir "$OUT" --out "$OUT/throughput-summary.md"
cat "$OUT/throughput-summary.md"
