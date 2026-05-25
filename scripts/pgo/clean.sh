#!/usr/bin/env bash
# Drop the PGO profile directory between training-run iterations.
#
# Why: stale .profraw files compound at `cargo pgo optimize` time.
# If the workload shape changed between runs (different queries, data
# scale, or engine code), the merged profile would mix old + new
# hot-path data and the optimizer would land on a worse-than-fresh
# decision. Always clean before re-training.

set -euo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
profile_dir="${repo_root}/target/pgo-profiles"

if [ -d "$profile_dir" ]; then
    echo "removing $profile_dir"
    rm -rf "$profile_dir"
else
    echo "$profile_dir does not exist; nothing to clean"
fi
