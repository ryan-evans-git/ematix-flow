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
# load, not steady-state repeats). Per-process threading:
#   --partitions auto    (default) set nothing — ematix's
#                        EMAT_TARGET_PARTITIONS AUTO mode senses live
#                        ematix processes via the partition registry
#                        and picks clamp(cores/N, 2, cores) itself.
#   --partitions legacy  EMAT_TARGET_PARTITIONS=0 — historical
#                        behavior, target_partitions = cores per
#                        process; at N=100 on a laptop this
#                        oversubscribes heavily BY DESIGN.
#   --partitions N       PARTITIONS=N — fixed per-process count
#                        (the pre-lever diagnostic override).
# DuckDB always uses its own defaults.
#
# MEMORY SAFETY (learned the hard way — an uncapped s100 launch OOM'd
# and force-restarted a 36 GB machine TWICE): stream processes launch
# through a counting gate. At most MAX_INFLIGHT engine processes exist
# at any instant; the remaining logical streams queue and start as slots
# free. "N streams" therefore means N logical streams SATURATING
# MAX_INFLIGHT slots — makespan and QPH keep their sustained-throughput
# meaning; per-query percentiles reflect contention at the cap, not at
# N. The cap is recorded in batch.json so summaries are attributable.
# A pre-batch memory gate additionally refuses to launch when free+
# inactive memory is under MIN_FREE_GB.
#
# Usage:
#   strict_throughput.sh [--sf N] [--streams "1,10,100"]
#                        [--engines "ematix,duckdb"] [--batches 4]
#                        [--max-inflight K] [--min-free-gb G]
#                        [--plan-cache on|off]
#                        [--partitions legacy|auto|N] [--out PATH]
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
MAX_INFLIGHT=10            # hard cap on concurrent engine processes
MIN_FREE_GB=6              # refuse to launch a batch below this headroom
PLAN_CACHE="off"           # per-process cache is fresh anyway; kept for parity
PARTITIONS_MODE="auto"     # legacy|auto|N — ematix per-process partitions
OUT="/tmp/strict-throughput-$(date +%Y%m%d-%H%M%S)"

usage() { sed -n '2,60p' "$0"; }

while [[ $# -gt 0 ]]; do
    case "$1" in
        --sf) SF="$2"; shift 2 ;;
        --streams) STREAMS="$2"; shift 2 ;;
        --engines) ENGINES="$2"; shift 2 ;;
        --batches) BATCHES="$2"; shift 2 ;;
        --max-inflight) MAX_INFLIGHT="$2"; shift 2 ;;
        --min-free-gb) MIN_FREE_GB="$2"; shift 2 ;;
        --plan-cache) PLAN_CACHE="$2"; shift 2 ;;
        --partitions) PARTITIONS_MODE="$2"; shift 2 ;;
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

# --partitions → per-stream ematix env (bash 3.2-safe: PART_ENV is
# always SET, possibly empty; the launch line expands it unquoted so
# an empty value contributes no words).
case "$PARTITIONS_MODE" in
    auto)   PART_ENV="" ;;                             # engine senses via registry
    legacy) PART_ENV="EMAT_TARGET_PARTITIONS=0" ;;     # cores per process
    ''|0|*[!0-9]*)
        echo "ERROR: --partitions must be legacy|auto|N (N>=1), got '$PARTITIONS_MODE'" >&2
        exit 2 ;;
    *)      PART_ENV="PARTITIONS=$PARTITIONS_MODE" ;;  # fixed count (back-compat)
esac

mkdir -p "$OUT"
capture_env "$OUT" "sf=$SF" "streams=$STREAMS" "engines=$ENGINES" \
    "batches=$BATCHES" "plan_cache=$PLAN_CACHE" \
    "partitions=$PARTITIONS_MODE" "mode=throughput"

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

# Free + inactive memory in whole GB (macOS vm_stat; 16 KiB pages on AS).
mem_available_gb() {
    vm_stat | awk '
        /page size of/       { ps = $8 }
        /Pages free/         { gsub("\\.",""); free = $3 }
        /Pages inactive/     { gsub("\\.",""); inact = $3 }
        END { printf "%d", (free + inact) * ps / 1073741824 }'
}

# Block while >= MAX_INFLIGHT children are alive (bash 3.2: no wait -n).
inflight_gate() {
    while (( $(jobs -rp | wc -l) >= MAX_INFLIGHT )); do
        sleep 0.2
    done
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

    local avail; avail="$(mem_available_gb)"
    if (( avail < MIN_FREE_GB )); then
        echo "  ABORT $engine s$n batch $batch: only ${avail} GB free+inactive (< ${MIN_FREE_GB} GB gate) — not launching" >&2
        return 1
    fi

    local start_ms end_ms
    start_ms="$(python3 -c 'import time; print(int(time.time()*1000))')"
    local pids=()
    for s in $(seq 1 "$n"); do
        inflight_gate
        local perm; perm="$(perm_for_stream "$s")"
        # shellcheck disable=SC2086
        caffeinate -i /usr/bin/env $skip_env $PC_ENV $PART_ENV \
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
  "max_inflight": $MAX_INFLIGHT,
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
echo "  sf: $SF | streams: $STREAMS | engines: $ENGINES | batches: $BATCHES (first discarded) | partitions: $PARTITIONS_MODE"
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
