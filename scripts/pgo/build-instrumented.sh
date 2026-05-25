#!/usr/bin/env bash
# Story 1.1 — instrumented PGO build of `tpch_triangulation_bench`.
#
# Pipeline step 1 of 3. After this script, the instrumented bench
# binary is at:
#     target/<host_triple>/release/examples/tpch_triangulation_bench
#
# Step 2: scripts/pgo/train.sh
# Step 3: scripts/pgo/optimize.sh
#
# OQ-PGO-A resolved in scripts/pgo/README.md: we use `cargo-pgo`
# (the published cargo subcommand) rather than raw rustflags.

set -euo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$repo_root"

# Sanity-check the prerequisites the README documents.
if ! command -v cargo-pgo >/dev/null 2>&1; then
    cat >&2 <<EOF
error: cargo-pgo is not installed.

Install it once with:

    cargo install --locked cargo-pgo

Then retry. See scripts/pgo/README.md for the full pipeline.
EOF
    exit 1
fi

if ! rustup component list --installed 2>/dev/null | grep -q '^llvm-tools'; then
    echo "warning: llvm-tools-preview not detected; cargo-pgo will fetch it on demand." >&2
fi

# `cargo pgo build` builds the workspace in instrumented mode. Args
# after `--` pass through to cargo. We scope to the bench example so
# only the binary we care about gets emitted.
exec cargo pgo build -- \
    -p ematix-flow-core \
    --example tpch_triangulation_bench \
    --features triangulation
