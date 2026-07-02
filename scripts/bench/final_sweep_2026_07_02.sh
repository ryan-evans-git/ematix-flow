#!/usr/bin/env bash
#
# Σ.AI.5 — final validation sweep (2026-07-02, post lever-gating + RANGE.AGG
# harness fix). Answers three questions with quotable strict numbers:
#
#   1. Do the scale-gated auto defaults regress anything at SF=10?
#      (full-22q interleaved A/B: all-forced-off vs auto)
#   2. Do they deliver Q09/Q10 (and hold Q08) at SF=100, net?
#      (full-22q interleaved A/B, 5 trials)
#   3. What is the corrected SF=100 verdict table vs DuckDB now that the
#      bench chain includes RANGE.AGG (the Q18 harness artifact) and
#      auto-gating is live? (solo rebaseline + labeled diff)
#   4. Bonus: does EMAT_L9_PARTITIONED still tax Q08 after the
#      I64onI32 bloom-binding fix? (targeted SF=100 A/B on Q8,Q9)
#
# All output under bench-results/final-sweep-2026-07-02/. Resumable.

set -uo pipefail

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
OUT="$REPO/bench-results/final-sweep-2026-07-02"
LOG="$OUT/sweep.log"
SB="$REPO/scripts/bench"
# Tri-state: unset = auto (scale-gated). The OFF arm forces every gated
# lever off explicitly.
OFF="EMAT_DOWNCAST_KEYS=0 EMAT_NARROW_KEY_DECODE=0 EMAT_DATE_BUILD_SIDE=0 EMAT_NDV_BUILD_SIDE=0 EMAT_FD_GROUPBY=0"
AUTO="EMAT_SWEEP_ARM=auto"   # inert marker; gated levers resolve by scale

mkdir -p "$OUT"
log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$LOG"; }
phase_done() { [[ -s "$1" ]] && { log "SKIP (exists): $1"; return 0; } || return 1; }

log "=== final sweep start; git $(git -C "$REPO" rev-parse --short HEAD) ==="

# 1. SF=10 full-22q: forced-off (A) vs auto (B). Expect noise-only.
if ! phase_done "$OUT/ab-sf10-auto/diff.md"; then
    log "phase 1: SF=10 A/B forced-off vs auto"
    "$SB/strict_ab.sh" --sf 10 --env-a "$OFF" --env-b "$AUTO" \
        --out "$OUT/ab-sf10-auto" >> "$LOG" 2>&1
    grep -E "faster|WIN|regression|^\*\*Net" "$OUT/ab-sf10-auto/diff.md" | tail -4 | tee -a "$LOG"
fi

# 2. SF=100 full-22q: forced-off vs auto, 5 trials.
if ! phase_done "$OUT/ab-sf100-auto/diff.md"; then
    log "phase 2: SF=100 A/B forced-off vs auto (full 22q)"
    "$SB/strict_ab.sh" --sf 100 --env-a "$OFF" --env-b "$AUTO" --trials 5 \
        --out "$OUT/ab-sf100-auto" >> "$LOG" 2>&1
    grep -E "faster|WIN|regression|^\*\*Net" "$OUT/ab-sf100-auto/diff.md" | tail -4 | tee -a "$LOG"
fi

# 3. SF=100 corrected verdict rebaseline (auto defaults) vs DuckDB.
if ! phase_done "$OUT/latency-sf100/verdicts.md"; then
    log "phase 3: SF=100 solo rebaseline (corrected harness, auto defaults)"
    {
        "$SB/strict_22q.sh" --sf 100 --engine ematix --isolate --trials 5 \
            --out "$OUT/latency-sf100/ematix"
        "$SB/strict_22q.sh" --sf 100 --engine duckdb --isolate --trials 5 \
            --out "$OUT/latency-sf100/duckdb"
        python3 "$SB/strict_diff.py" \
            --a "$OUT/latency-sf100/ematix/strict-22q-summary.md" \
            --b "$OUT/latency-sf100/duckdb/strict-22q-summary.md" \
            --label-a ematix --label-b duckdb \
            --out "$OUT/latency-sf100/verdicts.md"
    } >> "$LOG" 2>&1
    grep -E "faster \(|^\*\*" "$OUT/latency-sf100/verdicts.md" | tail -5 | tee -a "$LOG"
fi

# 4. L9_PARTITIONED re-check post bloom-binding fix (on top of auto).
if ! phase_done "$OUT/ab-sf100-l9p/diff.md"; then
    log "phase 4: SF=100 A/B auto vs auto+L9_PARTITIONED on Q8,Q9"
    "$SB/strict_ab.sh" --sf 100 --env-a "$AUTO" \
        --env-b "EMAT_L9_PARTITIONED=1" --queries "8,9" --trials 5 \
        --out "$OUT/ab-sf100-l9p" >> "$LOG" 2>&1
    grep -E "faster|WIN|regression|^\*\*Net" "$OUT/ab-sf100-l9p/diff.md" | tail -4 | tee -a "$LOG"
fi

log "=== final sweep COMPLETE ==="
