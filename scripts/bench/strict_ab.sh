#!/usr/bin/env bash
#
# Σ.AI.2 — Strict interleaved A/B SF=10 bench protocol.
#
# Companion to `strict_22q.sh`. Where that runs a single mode and
# is best used for noise-floor characterization, this script runs
# TWO modes in alternating sequence (A B A B A B ...) within one
# script invocation. Eliminates between-block bias: if mode A is
# always benched first and mode B always second, accumulating
# thermal/cache state systematically biases the comparison.
#
# Discipline (same as strict_22q.sh):
#   * caffeinate -i + taskpolicy -a
#   * Discard first A + first B (cold-start for each mode)
#   * Aggregate from runs 2..N for each mode
#   * Median-of-medians per query
#
# Plus the interleaving:
#   * Sequence is A1 B1 A2 B2 A3 B3 A4 B4
#   * Both modes see the same thermal/scheduler trajectory
#
# Usage:
#   strict_ab.sh --env-b "FLAG=1" [--invocations N] [--out PATH]
#                [--env-a "OTHER=VAL"]   # optional A-mode env
#
# Outputs:
#   $OUT/A/run-{1..N}.md  $OUT/A/strict-22q-summary.md
#   $OUT/B/run-{1..N}.md  $OUT/B/strict-22q-summary.md
#   $OUT/diff.md          per-query A vs B comparison

set -euo pipefail

INVOCATIONS=4              # First A and first B discarded
TRIALS=10
WARMUPS=2
COOLDOWN_SEC=15
TRIANGULATE=0
ENV_A=""
ENV_B=""
QUERIES=""                 # comma-separated query IDs; empty = all 22
OUT="/tmp/strict-ab-$(date +%Y%m%d-%H%M%S)"

usage() {
    sed -n '2,32p' "$0"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --invocations) INVOCATIONS="$2"; shift 2 ;;
        --trials) TRIALS="$2"; shift 2 ;;
        --warmups) WARMUPS="$2"; shift 2 ;;
        --cooldown) COOLDOWN_SEC="$2"; shift 2 ;;
        --triangulate) TRIANGULATE=1; shift ;;
        --env-a) ENV_A="$2"; shift 2 ;;
        --env-b) ENV_B="$2"; shift 2 ;;
        --queries) QUERIES="$2"; shift 2 ;;
        --out) OUT="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unknown arg: $1" >&2; usage; exit 2 ;;
    esac
done

if [[ -z "$ENV_B" ]]; then
    echo "ERROR: --env-b is required (the variant being tested)" >&2
    usage
    exit 2
fi

mkdir -p "$OUT/A" "$OUT/B"

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="$REPO/target/release/examples/tpch_triangulation_bench"
DATA="$REPO/examples/tpch/data/sf10"

if [[ ! -x "$BIN" ]]; then
    echo "ERROR: bench binary not found at $BIN" >&2
    exit 1
fi

COMMON_ENV=(
    "TPCH_DATA_DIR=$DATA"
    "TPCH_TRIALS=$TRIALS"
    "TPCH_WARMUPS=$WARMUPS"
)
if [[ "$TRIANGULATE" != "1" ]]; then
    COMMON_ENV+=("TPCH_SKIP_POLARS=1" "TPCH_SKIP_DUCKDB=1")
fi
if [[ -n "$QUERIES" ]]; then
    COMMON_ENV+=("TPCH_QUERIES=$QUERIES")
fi

# Run one (mode, invocation_idx) iteration.
run_one() {
    local mode="$1" idx="$2"
    local extra_env="$3"
    local out_file="$OUT/$mode/run-$idx.md"
    echo "  → mode $mode invocation $idx → $out_file"

    local env_arr=("${COMMON_ENV[@]}" "TPCH_OUT=$out_file")
    if [[ -n "$extra_env" ]]; then
        # shellcheck disable=SC2206
        local extra_arr=($extra_env)
        env_arr+=("${extra_arr[@]}")
    fi

    caffeinate -i taskpolicy -a \
        /usr/bin/env "${env_arr[@]}" "$BIN" \
        2>&1 | tail -1
}

echo "=== Σ.AI.2 strict interleaved A/B bench ==="
echo "  invocations:    $INVOCATIONS (first of each discarded)"
echo "  trials/run:     $TRIALS"
echo "  warmups/run:    $WARMUPS"
echo "  cooldown:       ${COOLDOWN_SEC}s between A and B"
echo "  triangulate:    $TRIANGULATE"
echo "  env A:          ${ENV_A:-<none>}"
echo "  env B:          $ENV_B"
echo "  output:         $OUT"
echo

for i in $(seq 1 "$INVOCATIONS"); do
    echo "--- pair $i / $INVOCATIONS ---"
    run_one "A" "$i" "$ENV_A"
    sleep "$COOLDOWN_SEC"
    run_one "B" "$i" "$ENV_B"
    if [[ "$i" -lt "$INVOCATIONS" ]]; then
        sleep "$COOLDOWN_SEC"
    fi
done

# Aggregate.
echo
echo "--- aggregating ---"
python3 "$REPO/scripts/bench/strict_summarize.py" \
    --runs "$OUT/A/run-"*.md --discard-first --out "$OUT/A/strict-22q-summary.md"
python3 "$REPO/scripts/bench/strict_summarize.py" \
    --runs "$OUT/B/run-"*.md --discard-first --out "$OUT/B/strict-22q-summary.md"
python3 "$REPO/scripts/bench/strict_diff.py" \
    --a "$OUT/A/strict-22q-summary.md" \
    --b "$OUT/B/strict-22q-summary.md" \
    --out "$OUT/diff.md"

echo
cat "$OUT/diff.md"
