#!/usr/bin/env bash
# Story 1.2 — PGO training run for the instrumented bench binary.
#
# Pipeline step 2 of 3. Precondition: build-instrumented.sh has produced
# the instrumented binary at target/<host_triple>/release/examples/.
#
# OQ-PGO-B resolved in scripts/pgo/README.md: training workload is
# **22q SF=10 single iteration, ematix-flow only** (DuckDB / Polars
# skipped — we only want ematix-flow hot paths represented in the
# profile). Single iteration is enough; multi-iteration would re-bias
# the profile toward long-running queries (Q21, Q05) without changing
# the codegen decisions PGO actually needs.
#
# Total wall time: ~3-5 minutes on SF=10 (Apple M3 Pro).
#
# Env knobs (override defaults for smoke tests / experiments):
#   PGO_TRAIN_DATA_DIR   — TPC-H data dir       (default examples/tpch/data/sf10)
#   PGO_TRAIN_QUERIES    — query subset CSV     (default all 22)
#   PGO_TRAIN_TRIALS     — measured trials      (default 1)
#   PGO_TRAIN_WARMUPS    — untimed warmups      (default 0)
#
# Output: .profraw files in target/pgo-profiles/, one per process exit.

set -euo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$repo_root"

host_triple=$(rustc -vV | awk -F': ' '/^host:/ {print $2}')
binary_path="target/${host_triple}/release/examples/tpch_triangulation_bench"
profile_dir="${repo_root}/target/pgo-profiles"

if [ ! -x "$binary_path" ]; then
    echo "error: instrumented binary not at $binary_path" >&2
    echo "Run scripts/pgo/build-instrumented.sh first." >&2
    exit 1
fi

# Make sure the profile dir exists; the LLVM profile runtime writes
# .profraw files to it at process exit.
mkdir -p "$profile_dir"

# LLVM_PROFILE_FILE controls where .profraw files land. Use %p to
# include the pid so concurrent or repeat runs don't clobber each
# other.
export LLVM_PROFILE_FILE="${profile_dir}/training-%p.profraw"

# Defaults: SF=10, all 22 queries, single iteration, no warmup, skip
# competitor engines. tpch_triangulation_bench env contract is
# documented in its module docstring; we forward env vars verbatim.
export TPCH_DATA_DIR="${PGO_TRAIN_DATA_DIR:-examples/tpch/data/sf10}"
export TPCH_TRIALS="${PGO_TRAIN_TRIALS:-1}"
export TPCH_WARMUPS="${PGO_TRAIN_WARMUPS:-0}"
export TPCH_SKIP_DUCKDB=1
export TPCH_SKIP_POLARS=1
# tpch_triangulation_bench writes BENCHMARKS.md by default. Redirect
# to a temp file so we don't clobber the canonical bench doc.
TPCH_OUT_TMP=$(mktemp -t tpch_pgo_train_out.XXXXXX.md)
export TPCH_OUT="$TPCH_OUT_TMP"

if [ -n "${PGO_TRAIN_QUERIES:-}" ]; then
    export TPCH_QUERIES="$PGO_TRAIN_QUERIES"
fi

# Full bench env per feedback_full_bench_env_checklist.md. These are
# the flags the 0.80 baseline assumes. Training without them would
# bias the profile away from the production codepath.
export EMAT_RG_DECODE_CACHE="${EMAT_RG_DECODE_CACHE:-1}"
export EMAT_RH_SUM_F64="${EMAT_RH_SUM_F64:-1}"

echo "PGO training run"
echo "  binary:      $binary_path"
echo "  profile dir: $profile_dir"
echo "  data:        $TPCH_DATA_DIR"
echo "  trials:      $TPCH_TRIALS"
echo "  warmups:     $TPCH_WARMUPS"
echo "  queries:     ${TPCH_QUERIES:-all 22}"
echo "  out (discard): $TPCH_OUT_TMP"
echo

"$binary_path"
rc=$?

# Profile files land at $LLVM_PROFILE_FILE; cargo pgo optimize will
# read this directory. Best-effort cleanup of the temp output file.
rm -f "$TPCH_OUT_TMP"

if [ "$rc" -ne 0 ]; then
    echo "error: training binary exited with code $rc" >&2
    exit "$rc"
fi

profraw_count=$(find "$profile_dir" -name '*.profraw' -type f -size +0 2>/dev/null | wc -l | tr -d ' ')
echo
echo "training run complete: $profraw_count non-empty .profraw file(s) in $profile_dir"
echo "next: scripts/pgo/optimize.sh"
