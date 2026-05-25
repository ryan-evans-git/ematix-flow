#!/usr/bin/env bash
# Story 1.1 TDD anchor: smoke-test the instrumented PGO build pipeline.
#
# Verifies:
#   1. `scripts/pgo/build-instrumented.sh` produces the instrumented bench
#      binary at `target/<host_triple>/release/examples/tpch_triangulation_bench`.
#   2. The binary is profile-instrumented (PGO instrumentation symbols
#      present — looks for `__llvm_profile_` symbols, the standard PGO
#      runtime sentinel).
#   3. `cargo build --release` (no PGO) still produces a working artifact,
#      so the PGO toolchain doesn't accidentally become mandatory for
#      non-bench development.
#
# Usage:
#     scripts/pgo/test_pgo_build_smoke.sh
#
# Exit 0 on pass; non-zero with a one-line failure reason otherwise.
#
# Pre-reqs (one-time, not asserted by this script):
#   - cargo-pgo installed: `cargo install --locked cargo-pgo`
#   - llvm-tools-preview component: pinned in rust-toolchain.toml; cargo
#     fetches on first invocation.

set -euo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$repo_root"

host_triple=$(rustc -vV | awk -F': ' '/^host:/ {print $2}')
binary_path="target/${host_triple}/release/examples/tpch_triangulation_bench"

fail() {
    echo "FAIL: $1" >&2
    exit 1
}

pass_section() {
    echo "  ok — $1"
}

echo "test_pgo_build_smoke.sh"
echo "  repo:        $repo_root"
echo "  host:        $host_triple"
echo "  binary path: $binary_path"

# ---------------------------------------------------------------------
# 1. Instrumented build
# ---------------------------------------------------------------------
echo "[1/3] running build-instrumented.sh ..."
scripts/pgo/build-instrumented.sh >/dev/null
[ -x "$binary_path" ] || fail "instrumented binary not at $binary_path"
pass_section "instrumented binary exists"

# ---------------------------------------------------------------------
# 2. Instrumentation symbols present
# ---------------------------------------------------------------------
echo "[2/3] verifying instrumentation symbols ..."
# `__llvm_profile_*` symbols are the canonical PGO runtime sentinel.
# On macOS Mach-O they appear with an extra `_` prefix
# (`___llvm_profile_*`); plain Linux ELF uses two underscores. Match
# either by searching for the inner `_llvm_profile_` fragment.
#
# We capture grep's output rather than piping nm into grep -q because
# `set -o pipefail` + grep's early-exit on -q causes nm to die with
# SIGPIPE — which then poisons the pipeline's exit status and produces
# a false "no symbols" result.
matches=""
if command -v nm >/dev/null 2>&1; then
    matches=$(nm "$binary_path" 2>/dev/null | grep -c '_llvm_profile_' || true)
fi
if [ "${matches:-0}" -eq 0 ]; then
    # Fallback to strings in case nm output is stripped on this platform.
    matches=$(strings "$binary_path" | grep -c '_llvm_profile_' || true)
fi
[ "${matches:-0}" -gt 0 ] \
    || fail "binary at $binary_path has no _llvm_profile_ symbols — not PGO-instrumented"
pass_section "$matches instrumentation symbol(s) present"

# ---------------------------------------------------------------------
# 3. Plain release build still works (no-PGO path)
# ---------------------------------------------------------------------
echo "[3/3] verifying plain `cargo build --release` still works ..."
cargo build --release -p ematix-flow-core --example tpch_22_audit >/dev/null 2>&1 \
    || fail "plain `cargo build --release` failed — PGO toolchain became mandatory"
pass_section "plain release build path still works"

echo "PASS"
