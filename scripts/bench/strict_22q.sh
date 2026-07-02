#!/usr/bin/env bash
#
# Σ.AI.1 — Strict 22q bench protocol (Apple Silicon).
#
# Motivation: per memory [[bench-methodology-3-invocations]] and
# [[sigma-ah-x-lever-a-closed]], single-invocation 5-trial benches of
# `tpch_triangulation_bench` under-estimate cross-invocation variance
# by 5-10×. The AH.2 Stage 6 "net-zero" finding (single 5-trial run)
# was contradicted by a 3-invocation × 10-trial validation that showed
# fused-probe is net +2.9% slower at 22q SF=10.
#
# This wrapper drives `tpch_triangulation_bench` with discipline aimed
# at suppressing across-invocation noise:
#
#   1. `caffeinate -i` — prevent macOS idle sleep / deep power states.
#
#   2. `taskpolicy -a` — app-level QoS keeps the bench on P-cores.
#
#   3. **Discard-first-invocation discipline** — invocation 1 pays
#      binary-load, page-cache cold reads, planner warm-up. Report is
#      median-of-medians across runs 2-N with across-invocation σ.
#
#   4. **Thermal gating (Σ.AI.3)** — each invocation starts only once
#      `pmset -g therm` reports CPU_Speed_Limit=100 (or after a bounded
#      wait, recorded as a WARNING). Cooldown is adaptive: skipped when
#      already thermally clean.
#
#   5. **Environment capture (Σ.AI.3)** — every run writes env.json
#      (machine, git SHA, power source, EMAT_*/TPCH_* flags, engine
#      versions) and the summary embeds it. Motivated by the M3 Pro →
#      M4 Max hardware swap that silently invalidated cross-machine
#      comparisons.
#
#   6. **Plan-cache fairness (Σ.AI.3)** — EMAT_PLAN_CACHE is set
#      EXPLICITLY (default off). In-process plan-cache reuse benefits
#      only ematix (DuckDB/Polars re-parse per trial), so verdict-grade
#      triangulated runs must keep it off; lever A/B runs may enable it
#      symmetrically on both sides.
#
# Default config skips Polars and DuckDB (ematix-only lever validation).
# Pass `--triangulate` for cross-engine verdict runs.
#
# Usage:
#   scripts/bench/strict_22q.sh [--sf 1|10|100] [--invocations N]
#                               [--triangulate] [--isolate]
#                               [--plan-cache on|off]
#                               [--cache-policy warm|cold]
#                               [--env "KEY=VAL KEY2=VAL2"]
#                               [--queries "1,6,14"] [--out PATH]
#
#   --isolate runs each query in its OWN process per invocation
#   (per-query isolation: fresh planner, fresh in-process caches).
#   Row output is concatenated per invocation, so summaries aggregate
#   identically in both layouts.
#
# Outputs:
#   - One file per invocation:  $OUT/run-{1..N}.md
#   - Environment metadata:     $OUT/env.json
#   - Aggregated summary:       $OUT/strict-22q-summary.md
#
# Examples:
#   # Baseline characterization (4 invocations, ematix-only, discard run 1)
#   scripts/bench/strict_22q.sh --out /tmp/strict-baseline
#
#   # Cross-engine verdict run at SF=100, per-query isolated
#   scripts/bench/strict_22q.sh --sf 100 --triangulate --isolate \
#       --out /tmp/strict-sf100
#
#   # A/B with a single env-var difference
#   scripts/bench/strict_22q.sh --env "EMAT_L9_FUSED_PROBE=0" --out /tmp/strict-A
#   scripts/bench/strict_22q.sh --env "EMAT_L9_FUSED_PROBE=1" --out /tmp/strict-B

set -euo pipefail

# Default config.
SF=10
INVOCATIONS=4              # First is discarded; report from runs 2..N.
TRIALS=10                  # within-invocation timed trials
WARMUPS=2                  # within-invocation warm-up trials (per `tpch_triangulation_bench`)
COOLDOWN_SEC=15            # fallback pause between invocations (adaptive: skipped when thermally clean)
TRIANGULATE=0              # default: ematix-only (skip Polars + DuckDB)
ISOLATE=0                  # per-query process isolation
PLAN_CACHE="off"           # explicit EMAT_PLAN_CACHE (see header §6)
CACHE_POLICY="warm"        # warm|cold page-cache policy
QUERIES=""                 # comma-separated query IDs; empty = all 22
EXTRA_ENV=""
OUT="/tmp/strict-22q-$(date +%Y%m%d-%H%M%S)"

usage() {
    sed -n '2,75p' "$0"
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
        --queries) QUERIES="$2"; shift 2 ;;
        --env) EXTRA_ENV="$2"; shift 2 ;;
        --out) OUT="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unknown arg: $1" >&2; usage; exit 2 ;;
    esac
done

mkdir -p "$OUT"

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
# shellcheck source=scripts/bench/strict_common.sh
source "$REPO/scripts/bench/strict_common.sh"

BIN="$REPO/target/release/examples/tpch_triangulation_bench"
DATA="$REPO/examples/tpch/data/sf$SF"

if [[ ! -x "$BIN" ]]; then
    echo "ERROR: bench binary not found at $BIN" >&2
    echo "Build it: cargo build --release --example tpch_triangulation_bench --features triangulation" >&2
    exit 1
fi
if [[ ! -d "$DATA" ]]; then
    echo "ERROR: SF=$SF data dir not found at $DATA" >&2
    exit 1
fi

case "$PLAN_CACHE" in
    on)  PC_ENV="EMAT_PLAN_CACHE=1" ;;
    off) PC_ENV="EMAT_PLAN_CACHE=0" ;;
    *) echo "ERROR: --plan-cache must be on|off" >&2; exit 2 ;;
esac
# Validate the cache policy up front (cold requires passwordless sudo purge).
if [[ "$CACHE_POLICY" == "cold" ]]; then
    apply_cache_policy cold >/dev/null || exit 1
elif [[ "$CACHE_POLICY" != "warm" ]]; then
    echo "ERROR: --cache-policy must be warm|cold" >&2; exit 2
fi

# Env vars common to all invocations.
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

echo "=== Σ.AI.1 strict bench protocol ==="
echo "  sf:             $SF"
echo "  invocations:    $INVOCATIONS (first will be discarded)"
echo "  trials/run:     $TRIALS"
echo "  warmups/run:    $WARMUPS"
echo "  cooldown:       adaptive (fallback ${COOLDOWN_SEC}s)"
echo "  triangulate:    $TRIANGULATE"
echo "  isolate:        $ISOLATE"
echo "  plan cache:     $PLAN_CACHE"
echo "  cache policy:   $CACHE_POLICY"
echo "  queries:        $QUERY_LIST"
echo "  extra env:      ${EXTRA_ENV:-<none>}"
echo "  output dir:     $OUT"
echo

# Record the environment BEFORE the first invocation so a crashed run
# still leaves attributable metadata behind. The subshell exports the
# run's env deltas so they land in env.json's emat_env snapshot.
(
    if [[ -n "$EXTRA_ENV" ]]; then
        # shellcheck disable=SC2086,SC2163
        export $EXTRA_ENV
    fi
    # shellcheck disable=SC2163
    export "$PC_ENV"
    capture_env "$OUT" "sf=$SF" "plan_cache=$PLAN_CACHE" \
        "cache_policy=$CACHE_POLICY" "isolate=$ISOLATE" \
        "triangulate=$TRIANGULATE" "trials=$TRIALS" "warmups=$WARMUPS" \
        "invocations=$INVOCATIONS"
)

# Run the binary once with the given TPCH_QUERIES / TPCH_OUT.
invoke_bin() {
    local queries="$1" out_file="$2"
    local env_arr=("${COMMON_ENV[@]}" "TPCH_QUERIES=$queries" "TPCH_OUT=$out_file")
    if [[ -n "$EXTRA_ENV" ]]; then
        # shellcheck disable=SC2206
        local extra_arr=($EXTRA_ENV)
        env_arr+=("${extra_arr[@]}")
    fi
    # caffeinate -i: prevent idle sleep. taskpolicy -a: app QoS → P-cores.
    caffeinate -i taskpolicy -a \
        /usr/bin/env "${env_arr[@]}" "$BIN" \
        2>&1 | tail -2
}

for i in $(seq 1 "$INVOCATIONS"); do
    OUT_FILE="$OUT/run-$i.md"
    echo "--- invocation $i / $INVOCATIONS → $OUT_FILE ---"
    echo "  [thermal] pre: $(thermal_state)"
    thermal_wait 120
    if [[ "$CACHE_POLICY" == "cold" ]]; then apply_cache_policy cold; fi

    if [[ "$ISOLATE" == "1" ]]; then
        # Per-query isolation: fresh process per query; concatenate the
        # per-query tables — the summarizer parses rows wherever found.
        : > "$OUT_FILE"
        IFS=',' read -ra Q_ARR <<< "$QUERY_LIST"
        for q in "${Q_ARR[@]}"; do
            TMP_Q="$OUT/.tmp-q$q.md"
            if [[ "$CACHE_POLICY" == "cold" ]]; then
                apply_cache_policy cold >/dev/null
            fi
            invoke_bin "$q" "$TMP_Q" >/dev/null
            cat "$TMP_Q" >> "$OUT_FILE"
            echo >> "$OUT_FILE"
            rm -f "$TMP_Q"
        done
        echo "  (isolated: ${#Q_ARR[@]} per-query processes)"
    else
        invoke_bin "$QUERY_LIST" "$OUT_FILE"
    fi
    echo "  [thermal] post: $(thermal_state)"
    echo

    if [[ "$i" -lt "$INVOCATIONS" ]]; then
        # Adaptive cooldown: only sleep if the box is still hot.
        if ! thermal_clean; then
            sleep "$COOLDOWN_SEC"
        fi
    fi
done

# Aggregate runs 2..N into a summary.
SUMMARY="$OUT/strict-22q-summary.md"
python3 "$REPO/scripts/bench/strict_summarize.py" \
    --runs "$OUT/run-"*.md \
    --discard-first \
    --env-json "$OUT/env.json" \
    --out "$SUMMARY"

echo "--- aggregate summary written to $SUMMARY ---"
cat "$SUMMARY"
