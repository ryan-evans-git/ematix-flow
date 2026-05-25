#!/usr/bin/env bash
# Story 1.2 TDD anchor: smoke-test the training-run profile capture.
#
# Verifies:
#   1. Given an instrumented binary (precondition from Story 1.1's
#      build-instrumented.sh), `scripts/pgo/train.sh` runs the bench
#      against SF=10 data (or SF=1 fallback in CI) without crashing.
#   2. After the training run, `target/pgo-profiles/` contains at
#      least one non-empty `.profraw` file (the per-process profile
#      data emitted by the instrumented binary at exit).
#
# Usage:
#     scripts/pgo/test_training_run.sh
#
# Exit 0 on pass; non-zero with a one-line failure reason otherwise.
#
# Env knobs:
#   - PGO_TEST_DATA_DIR  — override TPC-H data dir (default SF=1 for the
#     smoke test, so this script runs in a few seconds; real training
#     uses SF=10 via train.sh defaults).
#   - PGO_TEST_QUERIES   — comma-separated subset (default "1,6" — two
#     fast queries are enough to populate the profile dir for the smoke
#     test; train.sh defaults to all 22).

set -euo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$repo_root"

# macOS aarch64 dyld init issue: the instrumented binary segfaults in
# a C++ static constructor (vendored OpenSSL via rdkafka) before main.
# This is not a script bug — see scripts/pgo/README.md "Platform
# support" section. Skip with a clear message rather than failing.
if [ "$(uname -s)" = "Darwin" ] && [ "$(uname -m)" = "arm64" ]; then
    echo "SKIP: macOS aarch64 — instrumented binary crashes in dyld init"
    echo "      (see scripts/pgo/README.md; Stories 1.2 + 1.3 run on Linux)"
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

echo "test_training_run.sh"
echo "  repo:        $repo_root"
echo "  binary path: $binary_path"
echo "  profile dir: $profile_dir"

# ---------------------------------------------------------------------
# 0. Precondition: instrumented binary exists
# ---------------------------------------------------------------------
[ -x "$binary_path" ] \
    || fail "instrumented binary not found at $binary_path — run build-instrumented.sh first"

# ---------------------------------------------------------------------
# 1. Drop any stale profile data so we can assert fresh files appeared
# ---------------------------------------------------------------------
scripts/pgo/clean.sh >/dev/null 2>&1 || true
[ -d "$profile_dir" ] && [ "$(ls -A "$profile_dir" 2>/dev/null | grep -c '\.profraw$' || true)" -gt 0 ] \
    && fail "clean.sh did not drop stale .profraw files from $profile_dir"
pass_section "profile dir is clean"

# ---------------------------------------------------------------------
# 2. Run the training script with a smoke-sized workload
# ---------------------------------------------------------------------
echo "[1/2] running train.sh (smoke configuration) ..."

# Use SF=1 + 2 queries by default for the smoke test. Real training
# uses SF=10 + all 22 queries.
test_data_dir="${PGO_TEST_DATA_DIR:-examples/tpch/data/sf1}"
test_queries="${PGO_TEST_QUERIES:-1,6}"

PGO_TRAIN_DATA_DIR="$test_data_dir" \
PGO_TRAIN_QUERIES="$test_queries" \
PGO_TRAIN_TRIALS=1 \
PGO_TRAIN_WARMUPS=0 \
    scripts/pgo/train.sh >/dev/null \
    || fail "train.sh exited non-zero"
pass_section "train.sh completed"

# ---------------------------------------------------------------------
# 3. .profraw files emitted
# ---------------------------------------------------------------------
echo "[2/2] verifying profile data emitted ..."
[ -d "$profile_dir" ] \
    || fail "profile dir $profile_dir does not exist after training run"

profraw_count=$(find "$profile_dir" -name '*.profraw' -type f -size +0 2>/dev/null | wc -l | tr -d ' ')
[ "$profraw_count" -gt 0 ] \
    || fail "no non-empty .profraw files in $profile_dir after training run"
pass_section "$profraw_count non-empty .profraw file(s) emitted"

echo "PASS"
