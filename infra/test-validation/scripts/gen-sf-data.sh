#!/bin/bash
# Generate TPC-H data at the requested scale factor and upload to
# s3://$BENCH_BUCKET/tpch-data/sf{N}/. Runs on the Phase A EC2 box
# OR locally if you have enough disk + RAM.
#
# Usage:
#   ./gen-sf-data.sh <sf> <bench_bucket>
#
# Layout: for each TPC-H table T, uploads to:
#   s3://$bucket/tpch-data/sf{N}/<T>/<T>.parquet
#
# The trailing `/<T>/` directory is required by Trino's hive
# connector (external_location must be a directory). PySpark and
# ematix both accept directory layouts (PySpark via spark.read.parquet
# of the dir, ematix via the inner <T>.parquet file path).
#
# Idempotent: skips per-table if the S3 object already exists. Use
# `aws s3 rm --recursive s3://.../tpch-data/sf{N}/` to force regen.

set -euo pipefail

SF="${1:?usage: $0 <sf> <bench_bucket>}"
BUCKET="${2:?usage: $0 <sf> <bench_bucket>}"
REGION="${AWS_REGION:-us-east-2}"
REPO_ROOT="${REPO_ROOT:-/opt/ematix/ematix-flow}"
LOCAL_DIR="${LOCAL_DIR:-/var/tmp/tpch-data}"

# SF=100 needs ~150 GB total scratch (raw CSV staging + parquet output
# + Snappy ratios make this larger than the lineitem.parquet size
# suggests). Fail fast if the volume's too small.
NEEDED_GB=$((SF * 2))
AVAIL_GB=$(df -BG "$LOCAL_DIR" 2>/dev/null | tail -1 | awk '{gsub(/G/,""); print $4}' || echo 0)
if [ "${AVAIL_GB:-0}" -lt "$NEEDED_GB" ]; then
  echo "WARN: $LOCAL_DIR has ~${AVAIL_GB}GB free; need ~${NEEDED_GB}GB for SF=$SF"
  echo "       continuing anyway — tpch_generate streams + cleans staging files"
fi

mkdir -p "$LOCAL_DIR/sf$SF"

TABLES=(region nation supplier customer part partsupp orders lineitem)

# Check what's already on S3 — skip per-table generation if S3 object exists.
echo "=== checking existing S3 objects ==="
TODO=()
for t in "${TABLES[@]}"; do
  if /usr/local/bin/aws s3 ls "s3://$BUCKET/tpch-data/sf$SF/$t/$t.parquet" --region "$REGION" >/dev/null 2>&1; then
    echo "  already present: $t.parquet"
  else
    TODO+=("$t")
  fi
done

if [ "${#TODO[@]}" -eq 0 ]; then
  echo "=== all tables already on S3; nothing to do ==="
  exit 0
fi

echo "=== generating: ${TODO[*]} ==="

# Run the existing generator — produces /var/tmp/tpch-data/sf{N}/<T>.parquet
# for all 8 tables (single file each). It's idempotent so re-running
# is cheap if it failed previously.
cd "$REPO_ROOT"
source "$HOME/.cargo/env" 2>/dev/null || true
cargo run --release -p ematix-flow-core --example tpch_generate -- \
  --sf "$SF" --out "$LOCAL_DIR/sf$SF"

# Upload to the directory layout. `aws s3 cp` with a per-table key
# prefix is more efficient than `aws s3 sync` when the destination
# already has some objects — sync would re-compare them all.
for t in "${TODO[@]}"; do
  local_path="$LOCAL_DIR/sf$SF/$t.parquet"
  s3_path="s3://$BUCKET/tpch-data/sf$SF/$t/$t.parquet"
  if [ ! -f "$local_path" ]; then
    echo "FATAL: generator did not produce $local_path"
    exit 1
  fi
  size=$(stat -c%s "$local_path" 2>/dev/null || stat -f%z "$local_path")
  echo "uploading $t.parquet (${size} bytes) -> $s3_path"
  /usr/local/bin/aws s3 cp "$local_path" "$s3_path" --region "$REGION" \
    --no-progress --storage-class STANDARD
done

echo "=== sf$SF upload complete ==="
echo "verify: aws s3 ls s3://$BUCKET/tpch-data/sf$SF/ --region $REGION --recursive --human-readable --summarize"
