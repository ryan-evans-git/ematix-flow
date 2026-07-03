#!/usr/bin/env bash
#
# Σ.AI.2 — Strict interleaved A/B bench protocol.
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
#   * Thermal gating before every invocation (Σ.AI.3)
#   * env.json capture + summary metadata header (Σ.AI.3)
#   * Explicit plan-cache policy — EMAT_PLAN_CACHE set on BOTH sides
#     (default off), never left ambient (Σ.AI.3)
#
# Plus the interleaving:
#   * Sequence is A1 B1 A2 B2 A3 B3 A4 B4
#   * Both modes see the same thermal/scheduler trajectory
#
# Usage:
#   strict_ab.sh --env-b "FLAG=1" [--sf 1|10|100] [--invocations N]
#                [--env-a "OTHER=VAL"] [--plan-cache on|off]
#                [--cache-policy warm|cold] [--isolate]
#                [--queries "1,6,14"] [--triangulate] [--out PATH]
#                [--bin-a PATH] [--bin-b PATH]
#
# Binary A/B: pass --bin-a/--bin-b to compare two builds (e.g. pre/post a
# rule change with no toggle flag). Either --env-b or --bin-b must be given.
#
# Outputs:
#   $OUT/A/run-{1..N}.md  $OUT/A/strict-22q-summary.md
#   $OUT/B/run-{1..N}.md  $OUT/B/strict-22q-summary.md
#   $OUT/env.json         machine/flag metadata
#   $OUT/diff.md          per-query A vs B comparison

set -euo pipefail

SF=10
INVOCATIONS=4              # First A and first B discarded
TRIALS=10
WARMUPS=2
COOLDOWN_SEC=15
TRIANGULATE=0
ISOLATE=0
PLAN_CACHE="off"
CACHE_POLICY="warm"
ENV_A=""
ENV_B=""
BIN_A=""
BIN_B=""
QUERIES=""                 # comma-separated query IDs; empty = all 22
OUT="/tmp/strict-ab-$(date +%Y%m%d-%H%M%S)"

usage() {
    sed -n '2,40p' "$0"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --sf) SF="$2"; shift 2 ;;
        --invocations) INVOCATIONS="$2"; shift 2 ;;
        --trials) TRIALS="$2"; shift 2 ;;
        --warmups) WARMUPS="$2"; shift 2 ;;
        --cooldown) COOLDOWN_SEC="$2"; shift 2 ;;
        --triangulate) TRIANGULATE=1; shift ;;
        --isolate) ISOLATE=1; shift ;;
        --plan-cache) PLAN_CACHE="$2"; shift 2 ;;
        --cache-policy) CACHE_POLICY="$2"; shift 2 ;;
        --env-a) ENV_A="$2"; shift 2 ;;
        --env-b) ENV_B="$2"; shift 2 ;;
        --bin-a) BIN_A="$2"; shift 2 ;;
        --bin-b) BIN_B="$2"; shift 2 ;;
        --queries) QUERIES="$2"; shift 2 ;;
        --out) OUT="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unknown arg: $1" >&2; usage; exit 2 ;;
    esac
done

if [[ -z "$ENV_B" && -z "$BIN_B" ]]; then
    echo "ERROR: --env-b or --bin-b is required (the variant being tested)" >&2
    usage
    exit 2
fi

mkdir -p "$OUT/A" "$OUT/B"

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
# shellcheck source=scripts/bench/strict_common.sh
source "$REPO/scripts/bench/strict_common.sh"

BIN="$REPO/target/release/examples/tpch_triangulation_bench"
DATA="$REPO/examples/tpch/data/sf$SF"

BIN_A="${BIN_A:-$BIN}"
BIN_B="${BIN_B:-$BIN}"
for b in "$BIN_A" "$BIN_B"; do
    if [[ ! -x "$b" ]]; then
        echo "ERROR: bench binary not found at $b" >&2
        exit 1
    fi
done
if [[ ! -d "$DATA" ]]; then
    echo "ERROR: SF=$SF data dir not found at $DATA" >&2
    exit 1
fi

case "$PLAN_CACHE" in
    on)  PC_ENV="EMAT_PLAN_CACHE=1" ;;
    off) PC_ENV="EMAT_PLAN_CACHE=0" ;;
    *) echo "ERROR: --plan-cache must be on|off" >&2; exit 2 ;;
esac
if [[ "$CACHE_POLICY" == "cold" ]]; then
    apply_cache_policy cold >/dev/null || exit 1
elif [[ "$CACHE_POLICY" != "warm" ]]; then
    echo "ERROR: --cache-policy must be warm|cold" >&2; exit 2
fi

COMMON_ENV=(
    "TPCH_DATA_DIR=$DATA"
    "TPCH_TRIALS=$TRIALS"
    "TPCH_WARMUPS=$WARMUPS"
    "$PC_ENV"
)
if [[ "$TRIANGULATE" != "1" ]]; then
    COMMON_ENV+=("TPCH_SKIP_POLARS=1" "TPCH_SKIP_DUCKDB=1")
fi

QUERY_LIST="${QUERIES:-$(seq -s, 1 22)}"

capture_env "$OUT" "sf=$SF" "plan_cache=$PLAN_CACHE" \
    "cache_policy=$CACHE_POLICY" "isolate=$ISOLATE" \
    "triangulate=$TRIANGULATE" "trials=$TRIALS" "warmups=$WARMUPS" \
    "invocations=$INVOCATIONS" "env_a=${ENV_A:-none}" "env_b=${ENV_B:-none}" \
    "bin_a=$BIN_A" "bin_b=$BIN_B"

# Run the binary once for (mode, queries, out_file).
invoke_bin() {
    local extra_env="$1" queries="$2" out_file="$3" the_bin="$4"
    local env_arr=("${COMMON_ENV[@]}" "TPCH_QUERIES=$queries" "TPCH_OUT=$out_file")
    if [[ -n "$extra_env" ]]; then
        # shellcheck disable=SC2206
        local extra_arr=($extra_env)
        env_arr+=("${extra_arr[@]}")
    fi
    caffeinate -i taskpolicy -a \
        /usr/bin/env "${env_arr[@]}" "$the_bin" \
        2>&1 | tail -1
}

# Run one (mode, invocation_idx) iteration.
run_one() {
    local mode="$1" idx="$2"
    local extra_env="$3"
    local out_file="$OUT/$mode/run-$idx.md"
    local mode_bin="$BIN_A"
    if [[ "$mode" == "B" ]]; then mode_bin="$BIN_B"; fi
    echo "  → mode $mode invocation $idx → $out_file"

    thermal_wait 120
    if [[ "$CACHE_POLICY" == "cold" ]]; then apply_cache_policy cold; fi

    if [[ "$ISOLATE" == "1" ]]; then
        : > "$out_file"
        IFS=',' read -ra Q_ARR <<< "$QUERY_LIST"
        for q in "${Q_ARR[@]}"; do
            local tmp_q="$OUT/$mode/.tmp-q$q.md"
            if [[ "$CACHE_POLICY" == "cold" ]]; then
                apply_cache_policy cold >/dev/null
            fi
            invoke_bin "$extra_env" "$q" "$tmp_q" "$mode_bin" >/dev/null
            cat "$tmp_q" >> "$out_file"
            echo >> "$out_file"
            rm -f "$tmp_q"
        done
    else
        invoke_bin "$extra_env" "$QUERY_LIST" "$out_file" "$mode_bin"
    fi
}

echo "=== Σ.AI.2 strict interleaved A/B bench ==="
echo "  sf:             $SF"
echo "  invocations:    $INVOCATIONS (first of each discarded)"
echo "  trials/run:     $TRIALS"
echo "  warmups/run:    $WARMUPS"
echo "  cooldown:       adaptive (fallback ${COOLDOWN_SEC}s)"
echo "  triangulate:    $TRIANGULATE"
echo "  isolate:        $ISOLATE"
echo "  plan cache:     $PLAN_CACHE (both sides)"
echo "  cache policy:   $CACHE_POLICY"
echo "  env A:          ${ENV_A:-<none>}"
echo "  env B:          $ENV_B"
echo "  output:         $OUT"
echo

for i in $(seq 1 "$INVOCATIONS"); do
    echo "--- pair $i / $INVOCATIONS ---"
    run_one "A" "$i" "$ENV_A"
    if ! thermal_clean; then
        sleep "$COOLDOWN_SEC"
    fi
    run_one "B" "$i" "$ENV_B"
    if [[ "$i" -lt "$INVOCATIONS" ]]; then
        if ! thermal_clean; then
            sleep "$COOLDOWN_SEC"
        fi
    fi
done

# Aggregate.
echo
echo "--- aggregating ---"
python3 "$REPO/scripts/bench/strict_summarize.py" \
    --runs "$OUT/A/run-"*.md --discard-first \
    --env-json "$OUT/env.json" --out "$OUT/A/strict-22q-summary.md"
python3 "$REPO/scripts/bench/strict_summarize.py" \
    --runs "$OUT/B/run-"*.md --discard-first \
    --env-json "$OUT/env.json" --out "$OUT/B/strict-22q-summary.md"
python3 "$REPO/scripts/bench/strict_diff.py" \
    --a "$OUT/A/strict-22q-summary.md" \
    --b "$OUT/B/strict-22q-summary.md" \
    --out "$OUT/diff.md"

echo
cat "$OUT/diff.md"
