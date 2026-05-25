#!/usr/bin/env bash
# Story 1.2 TDD anchor: smoke-test the profile-merge + optimized build.
#
# Verifies:
#   1. Given .profraw files in `target/pgo-profiles/` (precondition
#      from training run), `scripts/pgo/optimize.sh` runs
#      `cargo pgo optimize` successfully.
#   2. The resulting optimized binary exists at the same path as the
#      instrumented binary (cargo-pgo overwrites it on `optimize`).
#   3. The optimized binary differs from the instrumented one — size
#      check, since PGO-optimized binaries are typically smaller
#      (or at least different) than instrumented ones.
#
# Usage:
#     scripts/pgo/test_profile_merge.sh
#
# Pre-reqs:
#   - Instrumented binary exists (run build-instrumented.sh).
#   - Profile data captured (run train.sh).
#   - test_training_run.sh has been run (sequential dependency).

set -euo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$repo_root"

# macOS aarch64 dyld init issue: training step can't run, so the
# merge step is moot. Skip on macOS — see scripts/pgo/README.md.
if [ "$(uname -s)" = "Darwin" ] && [ "$(uname -m)" = "arm64" ]; then
    echo "SKIP: macOS aarch64 — depends on training-run output that can't be captured here"
    exit 0
fi

host_triple=$(rustc -vV | awk -F': ' '/^host:/ {print $2}')
binary_path="target/${host_triple}/release/examples/tpch_triangulation_bench"
profile_dir="target/pgo-profiles"

fail() {
    echo "FAIL: $1" >&2
    exit 1
}

pass_section() {
    echo "  ok — $1"
}

echo "test_profile_merge.sh"
echo "  repo:        $repo_root"
echo "  binary path: $binary_path"

# ---------------------------------------------------------------------
# 0. Preconditions
# ---------------------------------------------------------------------
[ -x "$binary_path" ] \
    || fail "binary not found at $binary_path — run build-instrumented.sh + train.sh first"

profraw_count=$(find "$profile_dir" -name '*.profraw' -type f -size +0 2>/dev/null | wc -l | tr -d ' ')
[ "$profraw_count" -gt 0 ] \
    || fail "no .profraw files in $profile_dir — run train.sh first"

# ---------------------------------------------------------------------
# 1. Snapshot the instrumented binary's size (or hash)
# ---------------------------------------------------------------------
size_before=$(stat -f '%z' "$binary_path" 2>/dev/null || stat -c '%s' "$binary_path")
echo "  size_before: $size_before bytes"

# ---------------------------------------------------------------------
# 2. Run optimize.sh
# ---------------------------------------------------------------------
echo "[1/2] running optimize.sh ..."
scripts/pgo/optimize.sh >/dev/null \
    || fail "optimize.sh exited non-zero"
pass_section "optimize.sh completed"

# ---------------------------------------------------------------------
# 3. Optimized binary differs from instrumented one
# ---------------------------------------------------------------------
echo "[2/2] verifying optimized binary is distinct ..."
[ -x "$binary_path" ] \
    || fail "binary at $binary_path missing after optimize.sh"

size_after=$(stat -f '%z' "$binary_path" 2>/dev/null || stat -c '%s' "$binary_path")
echo "  size_after:  $size_after bytes"

[ "$size_after" != "$size_before" ] \
    || fail "binary size unchanged after optimize.sh ($size_before bytes) — PGO optimize may have no-op'd"
pass_section "optimized binary distinct from instrumented binary"

# ---------------------------------------------------------------------
# 4. Optimized binary is NOT instrumented anymore
# ---------------------------------------------------------------------
# The optimized binary should NOT contain the __llvm_profile runtime
# (that's the whole point — instrumentation is replaced by the PGO
# optimization decisions).
if command -v nm >/dev/null 2>&1; then
    if nm "$binary_path" 2>/dev/null | grep -q '__llvm_profile_runtime' ; then
        fail "optimized binary still contains __llvm_profile_runtime symbol — optimize step did not strip instrumentation"
    fi
fi
pass_section "optimized binary has instrumentation stripped"

echo "PASS"
