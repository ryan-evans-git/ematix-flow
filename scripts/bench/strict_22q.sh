#!/usr/bin/env bash
#
# Σ.AI.1 — Strict SF=10 22q bench protocol (Apple Silicon).
#
# Motivation: per memory [[bench-methodology-3-invocations]] and
# [[sigma-ah-x-lever-a-closed]], single-invocation 5-trial benches of
# `tpch_triangulation_bench` under-estimate cross-invocation variance
# by 5-10×. The AH.2 Stage 6 "net-zero" finding (single 5-trial run)
# was contradicted by a 3-invocation × 10-trial validation that showed
# fused-probe is net +2.9% slower at 22q SF=10.
#
# This wrapper drives `tpch_triangulation_bench` with three pieces of
# discipline aimed at suppressing across-invocation noise:
#
#   1. `caffeinate -i` — prevent macOS from auto-sleeping or letting the
#      SoC enter deep power states between invocations.
#
#   2. `taskpolicy -a` — run with the resource-management policies
#      macOS assigns to user-facing applications. This raises the QoS
#      class enough that the scheduler consistently places the bench
#      process on the M3 Pro performance cores (10 P-cores) instead of
#      letting it land on the 4 efficiency cores. macOS doesn't expose
#      strict CPU pinning to user-space, but empirically this keeps
#      placement stable across invocations.
#
#   3. **Discard-first-invocation discipline** — the first invocation
#      pays binary-load, library symbol resolution, page-cache cold
#      reads, and DataFusion's planner warm-up. Invocations 2+ are
#      hot. We report median-of-medians across runs 2-N, with
#      across-invocation σ alongside each query's median.
#
# Default config skips Polars and DuckDB (they're 80% of wall time at
# Polars-dominant queries; only ematix-flow numbers matter for
# lever-validation work). Pass `--triangulate` to keep all three engines.
#
# Usage:
#   scripts/bench/strict_22q.sh [--invocations N] [--triangulate]
#                               [--env "KEY=VAL KEY2=VAL2"]
#                               [--out PATH]
#
# Outputs:
#   - One file per invocation:  $OUT/run-{1..N}.md
#   - Aggregated summary:        $OUT/strict-22q-summary.md
#
# Examples:
#   # Baseline characterization (4 invocations, ematix-only, discard run 1)
#   scripts/bench/strict_22q.sh --out /tmp/strict-baseline
#
#   # A/B with a single env-var difference
#   scripts/bench/strict_22q.sh --env "EMAT_L9_FUSED_PROBE=0" --out /tmp/strict-A
#   scripts/bench/strict_22q.sh --env "EMAT_L9_FUSED_PROBE=1" --out /tmp/strict-B

set -euo pipefail

# Default config.
INVOCATIONS=4              # First is discarded; report from runs 2..N.
TRIALS=10                  # within-invocation timed trials
WARMUPS=2                  # within-invocation warm-up trials (per `tpch_triangulation_bench`)
COOLDOWN_SEC=15            # pause between invocations to let thermals settle
TRIANGULATE=0              # default: ematix-only (skip Polars + DuckDB)
EXTRA_ENV=""
OUT="/tmp/strict-22q-$(date +%Y%m%d-%H%M%S)"

usage() {
    sed -n '2,38p' "$0"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --invocations) INVOCATIONS="$2"; shift 2 ;;
        --trials) TRIALS="$2"; shift 2 ;;
        --warmups) WARMUPS="$2"; shift 2 ;;
        --cooldown) COOLDOWN_SEC="$2"; shift 2 ;;
        --triangulate) TRIANGULATE=1; shift ;;
        --env) EXTRA_ENV="$2"; shift 2 ;;
        --out) OUT="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unknown arg: $1" >&2; usage; exit 2 ;;
    esac
done

mkdir -p "$OUT"

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="$REPO/target/release/examples/tpch_triangulation_bench"
DATA="$REPO/examples/tpch/data/sf10"

if [[ ! -x "$BIN" ]]; then
    echo "ERROR: bench binary not found at $BIN" >&2
    echo "Build it: cargo build --release --example tpch_triangulation_bench --features triangulation" >&2
    exit 1
fi
if [[ ! -d "$DATA" ]]; then
    echo "ERROR: SF=10 data dir not found at $DATA" >&2
    exit 1
fi

# Env vars common to all invocations.
COMMON_ENV=(
    "TPCH_DATA_DIR=$DATA"
    "TPCH_TRIALS=$TRIALS"
    "TPCH_WARMUPS=$WARMUPS"
)
if [[ "$TRIANGULATE" != "1" ]]; then
    COMMON_ENV+=("TPCH_SKIP_POLARS=1" "TPCH_SKIP_DUCKDB=1")
fi

echo "=== Σ.AI.1 strict bench protocol ==="
echo "  invocations:    $INVOCATIONS (first will be discarded)"
echo "  trials/run:     $TRIALS"
echo "  warmups/run:    $WARMUPS"
echo "  cooldown:       ${COOLDOWN_SEC}s between invocations"
echo "  triangulate:    $TRIANGULATE"
echo "  extra env:      ${EXTRA_ENV:-<none>}"
echo "  output dir:     $OUT"
echo

for i in $(seq 1 "$INVOCATIONS"); do
    OUT_FILE="$OUT/run-$i.md"
    echo "--- invocation $i / $INVOCATIONS → $OUT_FILE ---"

    # Build the env-prefix array.
    INVOCATION_ENV=("${COMMON_ENV[@]}" "TPCH_OUT=$OUT_FILE")
    if [[ -n "$EXTRA_ENV" ]]; then
        # shellcheck disable=SC2206
        EXTRA_ARR=($EXTRA_ENV)
        INVOCATION_ENV+=("${EXTRA_ARR[@]}")
    fi

    # caffeinate -i: prevent idle sleep during this command.
    # taskpolicy -a: raise QoS to app-level so the scheduler keeps us on P-cores.
    # /usr/bin/env: cleanly apply env vars to the child.
    caffeinate -i taskpolicy -a \
        /usr/bin/env "${INVOCATION_ENV[@]}" "$BIN" \
        2>&1 | tail -2
    echo

    if [[ "$i" -lt "$INVOCATIONS" ]]; then
        sleep "$COOLDOWN_SEC"
    fi
done

# Aggregate runs 2..N into a summary.
SUMMARY="$OUT/strict-22q-summary.md"
python3 "$REPO/scripts/bench/strict_summarize.py" \
    --runs "$OUT/run-"*.md \
    --discard-first \
    --out "$SUMMARY"

echo "--- aggregate summary written to $SUMMARY ---"
cat "$SUMMARY"
