#!/usr/bin/env bash
#
# Σ.AI.4 — 2026-07-01 measurement campaign (integration/campaign-2026-07-01).
#
# One sequential script so the machine is never double-loaded. Phases:
#   0. Post-merge validation: 22/22 value-match at SF1, flags OFF and all
#      new levers ON (composition check).
#   1. SF1 latency rebaseline — solo ematix + solo DuckDB, per-query
#      isolated, labeled diff.
#   2. SF10 latency rebaseline — same.
#   3. Per-lever strict interleaved A/B at SF10 (ematix-only):
#      L9_PARTITIONED / FD_GROUPBY / NARROW keys / DATE_BUILD_SIDE /
#      NDV_BUILD_SIDE / ALL-ON.
#   4. SF100 latency rebaseline — solo passes, 5 trials, isolated.
#   5. SF100 targeted lever A/Bs on the loss/noise-class queries.
#   6. Throughput: SF10 streams 1,10,100; SF100 streams 1,10.
#
# Every phase writes under bench-results/campaign-2026-07-01/ and appends
# to campaign.log. Phases are skipped if their output dir already has a
# summary (crude resume). Requires: release binaries already built.

set -uo pipefail   # NOT -e: a failed phase logs and continues

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
OUT="$REPO/bench-results/campaign-2026-07-01"
LOG="$OUT/campaign.log"
SB="$REPO/scripts/bench"
VAL="$REPO/target/release/examples/tpch_validate"
DATA="$REPO/examples/tpch/data"
ALL_ON="EMAT_L9_PARTITIONED=1 EMAT_FD_GROUPBY=1 EMAT_DOWNCAST_KEYS=1 EMAT_NARROW_KEY_DECODE=1 EMAT_DATE_BUILD_SIDE=1 EMAT_NDV_BUILD_SIDE=1"

mkdir -p "$OUT"
log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$LOG"; }
have() { [[ -s "$1" ]]; }

phase_done() { have "$1" && { log "SKIP (exists): $1"; return 0; } || return 1; }

log "=== campaign start; git $(git -C "$REPO" rev-parse --short HEAD) ==="

# ---- Phase 0: post-merge validation ----------------------------------
if ! phase_done "$OUT/validate-flags-on.txt"; then
    log "phase 0: tpch_validate SF1 flags OFF"
    TPCH_DATA_DIR="$DATA/sf1" "$VAL" > "$OUT/validate-flags-off.txt" 2>&1
    tail -3 "$OUT/validate-flags-off.txt" | tee -a "$LOG"
    log "phase 0: tpch_validate SF1 all new levers ON"
    # shellcheck disable=SC2086
    env $ALL_ON TPCH_DATA_DIR="$DATA/sf1" "$VAL" > "$OUT/validate-flags-on.txt" 2>&1
    tail -3 "$OUT/validate-flags-on.txt" | tee -a "$LOG"
    if ! grep -q "FAIL:    0" "$OUT/validate-flags-off.txt" || \
       ! grep -q "FAIL:    0" "$OUT/validate-flags-on.txt"; then
        log "FATAL: validation failed post-merge — aborting campaign"
        exit 1
    fi
fi

# ---- Phases 1/2/4: latency rebaselines --------------------------------
rebaseline() {
    local sf="$1" trials="$2" invocations="$3"
    local dir="$OUT/latency-sf$sf"
    phase_done "$dir/verdicts.md" && return 0
    log "phase latency SF=$sf: solo ematix"
    "$SB/strict_22q.sh" --sf "$sf" --engine ematix --isolate \
        --trials "$trials" --invocations "$invocations" \
        --out "$dir/ematix" >> "$LOG" 2>&1
    log "phase latency SF=$sf: solo duckdb"
    "$SB/strict_22q.sh" --sf "$sf" --engine duckdb --isolate \
        --trials "$trials" --invocations "$invocations" \
        --out "$dir/duckdb" >> "$LOG" 2>&1
    python3 "$SB/strict_diff.py" \
        --a "$dir/ematix/strict-22q-summary.md" \
        --b "$dir/duckdb/strict-22q-summary.md" \
        --label-a ematix --label-b duckdb \
        --out "$dir/verdicts.md" >> "$LOG" 2>&1
    log "SF=$sf verdicts:"; grep -E "faster|^\*\*" "$dir/verdicts.md" | tail -6 | tee -a "$LOG"
}
rebaseline 1 10 4
rebaseline 10 10 4

# ---- Phase 3: per-lever A/B at SF10 (ematix-only) ---------------------
ab() {
    local name="$1" envb="$2" queries="${3:-}"
    local dir="$OUT/ab-sf10-$name"
    phase_done "$dir/diff.md" && return 0
    log "phase A/B SF=10: $name  [$envb]"
    local qflag=()
    [[ -n "$queries" ]] && qflag=(--queries "$queries")
    "$SB/strict_ab.sh" --sf 10 --env-b "$envb" "${qflag[@]}" \
        --out "$dir" >> "$LOG" 2>&1
    grep -E "faster|WIN|regression|^\*\*Net" "$dir/diff.md" | tail -5 | tee -a "$LOG"
}
ab l9-partitioned "EMAT_L9_PARTITIONED=1"
ab fd-groupby     "EMAT_FD_GROUPBY=1"
ab narrow-keys    "EMAT_DOWNCAST_KEYS=1 EMAT_NARROW_KEY_DECODE=1"
ab date-swap      "EMAT_DATE_BUILD_SIDE=1"
ab ndv-swap       "EMAT_NDV_BUILD_SIDE=1"
ab all-on         "$ALL_ON"

# ---- Phase 4: SF100 rebaseline ----------------------------------------
rebaseline 100 5 4

# ---- Phase 5: SF100 targeted lever A/Bs -------------------------------
ab100() {
    local name="$1" envb="$2" queries="$3"
    local dir="$OUT/ab-sf100-$name"
    phase_done "$dir/diff.md" && return 0
    log "phase A/B SF=100: $name on Q[$queries]"
    "$SB/strict_ab.sh" --sf 100 --env-b "$envb" --queries "$queries" \
        --trials 5 --out "$dir" >> "$LOG" 2>&1
    grep -E "faster|WIN|regression|^\*\*Net" "$dir/diff.md" | tail -5 | tee -a "$LOG"
}
ab100 l9-partitioned "EMAT_L9_PARTITIONED=1" "5,7,8,9"
ab100 narrow-keys    "EMAT_DOWNCAST_KEYS=1 EMAT_NARROW_KEY_DECODE=1" "9,10"
ab100 fd-groupby     "EMAT_FD_GROUPBY=1" "10,13"
ab100 swaps          "EMAT_DATE_BUILD_SIDE=1 EMAT_NDV_BUILD_SIDE=1" "8,10"
ab100 all-on         "$ALL_ON" "3,5,8,9,10,16,18,21"

# ---- Phase 6: throughput ----------------------------------------------
if ! phase_done "$OUT/tput-sf10/throughput-summary.md"; then
    log "phase throughput SF=10 streams 1,10,100"
    "$SB/strict_throughput.sh" --sf 10 --streams "1,10,100" \
        --engines "ematix,duckdb" --batches 4 \
        --out "$OUT/tput-sf10" >> "$LOG" 2>&1
fi
if ! phase_done "$OUT/tput-sf100/throughput-summary.md"; then
    log "phase throughput SF=100 streams 1,10 (RAM guard: no s100 at SF100)"
    "$SB/strict_throughput.sh" --sf 100 --streams "1,10" \
        --engines "ematix,duckdb" --batches 3 \
        --out "$OUT/tput-sf100" >> "$LOG" 2>&1
fi

log "=== campaign COMPLETE ==="
