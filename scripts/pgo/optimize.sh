#!/usr/bin/env bash
# Story 1.2 — merge .profraw → .profdata and emit the PGO-optimized
# bench binary.
#
# Pipeline step 3 of 3. Precondition: train.sh has populated
# target/pgo-profiles/ with at least one non-empty .profraw file.
#
# Output: PGO-optimized `tpch_triangulation_bench` binary at the same
# path the instrumented build wrote to (cargo-pgo overwrites it).

set -euo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$repo_root"

host_triple=$(rustc -vV | awk -F': ' '/^host:/ {print $2}')
binary_path="target/${host_triple}/release/examples/tpch_triangulation_bench"
profile_dir="${repo_root}/target/pgo-profiles"

if ! command -v cargo-pgo >/dev/null 2>&1; then
    echo "error: cargo-pgo not installed. Run: cargo install --locked cargo-pgo" >&2
    exit 1
fi

profraw_count=$(find "$profile_dir" -name '*.profraw' -type f -size +0 2>/dev/null | wc -l | tr -d ' ')
if [ "$profraw_count" -eq 0 ]; then
    echo "error: no .profraw files in $profile_dir" >&2
    echo "Run scripts/pgo/train.sh first." >&2
    exit 1
fi

echo "PGO optimize"
echo "  binary path:    $binary_path"
echo "  profile dir:    $profile_dir"
echo "  .profraw count: $profraw_count"
echo

# cargo pgo optimize merges .profraw → .profdata via `llvm-profdata
# merge`, then rebuilds the binary with `-Cprofile-use=...`. Args
# after `--` match the build-instrumented.sh invocation so the same
# binary gets replaced.
exec cargo pgo optimize -- \
    -p ematix-flow-core \
    --example tpch_triangulation_bench \
    --features triangulation
