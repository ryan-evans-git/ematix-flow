#!/usr/bin/env bash
# SF=100 loss diagnostic driver — implements the protocol in
# docs/plans/SF100_LOSS_DIAGNOSTIC.md and prints a per-query root-cause verdict.
#
# It runs the instrumented `tpch_preset_rebench` in three roles:
#   1. isolated ematix  (SKIP_DUCKDB=1) x INV invocations  -> clean ematix RSS/CPU/pageins
#   2. isolated DuckDB   (SKIP_EMATIX=1) x INV invocations  -> clean DuckDB RSS/CPU/pageins
#   3. in-sweep          (both engines, full 22q) x1        -> box cache-pressure (delta)
# then feeds the logs to sf100_classify.py.
#
# Per-engine RSS/pageins are only attributable in the ISOLATED runs (DuckDB runs
# in-process, so an interleaved run pollutes both) — hence the isolated pair.
# INV>=3 honours the 3-invocation rule (trials are pooled before the median).
#
# Build first (diagnostic fields live behind the triangulation feature):
#   cargo build --release -p ematix-flow-core --example tpch_preset_rebench --features triangulation
#
# Env knobs (all optional):
#   SCALE=sf100   QUERIES=8,9,10,3,5   INV=3   TRIALS=3   WARMUPS=1
#   INSWEEP_TRIALS=2   OUTDIR=/tmp/sf100_diag
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="$REPO/target/release/examples/tpch_preset_rebench"
SCALE="${SCALE:-sf100}"
QUERIES="${QUERIES:-8,9,10,3,5}"
INV="${INV:-3}"
TRIALS="${TRIALS:-3}"
WARMUPS="${WARMUPS:-1}"
INSWEEP_TRIALS="${INSWEEP_TRIALS:-2}"
OUTDIR="${OUTDIR:-/tmp/sf100_diag}"
DATA="$REPO/examples/tpch/data/$SCALE"

[ -x "$BIN" ] || { echo "FATAL: $BIN not built. Run the cargo build line in this script's header." >&2; exit 1; }
[ -d "$DATA" ] || { echo "FATAL: data dir $DATA missing." >&2; exit 1; }

mkdir -p "$OUTDIR"
ISO_E="$OUTDIR/iso_ematix.log"; : > "$ISO_E"
ISO_D="$OUTDIR/iso_duckdb.log"; : > "$ISO_D"
INSWEEP="$OUTDIR/insweep.log"; : > "$INSWEEP"

# Keep the box awake + deprioritise nothing else; matches the strict bench protocol.
WRAP=(caffeinate -i)
command -v caffeinate >/dev/null || WRAP=()

echo "== SF=100 diagnostic: scale=$SCALE queries=$QUERIES inv=$INV trials=$TRIALS warmups=$WARMUPS =="
echo "   data=$DATA  outdir=$OUTDIR"

run() { # role-label  single-query  extra-env...  -> appends markers to $LOGFILE
  local label="$1" q="$2"; shift 2
  echo "   [$label q=$q] $(date +%H:%M:%S)"
  env TPCH_DATA_DIR="$DATA" TPCH_QUERIES="$q" TRIALS="$TRIALS" WARMUPS="$WARMUPS" \
      "$@" "${WRAP[@]}" "$BIN" >>"$LOGFILE" 2>&1 || \
      echo "   WARN: $label exited nonzero (continuing)"
}

# ONE query per isolated process: peak RSS (getrusage) is process-wide and
# monotone, so it is per-query only when the query is alone in the process.
IFS=',' read -ra QLIST <<< "$QUERIES"
for i in $(seq 1 "$INV"); do
  for q in "${QLIST[@]}"; do
    [ -z "$q" ] && continue   # tolerate trailing/empty CSV fields (e.g. seq -s,)
    LOGFILE="$ISO_E"; run "iso-ematix inv$i" "$q" SKIP_DUCKDB=1
    LOGFILE="$ISO_D"; run "iso-duckdb inv$i" "$q" SKIP_EMATIX=1
  done
done

# In-sweep: the full 22-query pass reproduces the realistic working-set / page-cache
# pressure that the isolated single-query runs do not. One pass is enough for the delta.
echo "   [in-sweep full-22] $(date +%H:%M:%S)"
env TPCH_DATA_DIR="$DATA" TRIALS="$INSWEEP_TRIALS" WARMUPS="$WARMUPS" \
    "${WRAP[@]}" "$BIN" >>"$INSWEEP" 2>&1 || echo "   WARN: in-sweep exited nonzero"

echo "== classify =="
python3 "$REPO/scripts/diag/sf100_classify.py" \
  --ematix "$ISO_E" --duckdb "$ISO_D" --insweep "$INSWEEP" --queries "$QUERIES" \
  | tee "$OUTDIR/verdict.txt"
echo "== done; logs in $OUTDIR =="
