#!/bin/bash
# Generate MULTI-FILE ("parted") TPC-H data and upload to
#   s3://$BUCKET/tpch-data-parted/sf{N}/<table>/<table>-NNNN.parquet
#
# Each big table is split into $PARTS physical parquet files (via
# tpch_generate --parts, which uses tpchgen's native (sf,part,part_count)
# sharding); nation and region stay single-file (tiny fixed tables). The
# distributed planner sizes a scan's cross-peer fan-out from the file
# count (files_per_task pinned to 1 in the ematix session), so multiple
# files per table are what let the Arrow Flight mesh actually engage —
# a single file collapses to one task = single-node.
#
# Usage:
#   ./gen-parted-data.sh <sf> <bench_bucket> [parts=8]
#
# Distinct from the single-file layout under tpch-data/ (gen-sf-data.sh);
# both can coexist in the bucket. Point the distributed terraform at this
# layout with -var data_prefix=tpch-data-parted.
#
# SF=100 needs a big scratch volume (~200 GB) + time — run it on an EC2
# box, not a laptop. SF=1/10 are fine locally.
#
# Idempotent: tpch_generate skips per-file when the target already exists;
# the S3 upload re-copies (cheap for the small scales).

set -euo pipefail

SF="${1:?usage: $0 <sf> <bench_bucket> [parts]}"
BUCKET="${2:?usage: $0 <sf> <bench_bucket> [parts]}"
PARTS="${3:-8}"
REGION="${AWS_REGION:-us-east-2}"
REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "$0")/../../.." && pwd)}"
LOCAL_DIR="${LOCAL_DIR:-/var/tmp/tpch-parted}"
DEST_PREFIX="tpch-data-parted"
# aws-cli lives at /usr/local/bin/aws on the EC2 AL2023 boxes but wherever
# PATH resolves it on a dev laptop — pick whichever exists.
AWS_BIN="${AWS_BIN:-$(command -v aws || echo /usr/local/bin/aws)}"

TABLES=(region nation supplier customer part partsupp orders lineitem)

mkdir -p "$LOCAL_DIR/sf$SF"
cd "$REPO_ROOT"
source "$HOME/.cargo/env" 2>/dev/null || true

echo "=== generating SF=$SF parted (parts=$PARTS) into $LOCAL_DIR/sf$SF ==="
cargo run --release -p ematix-flow-core --example tpch_generate -- \
  --sf "$SF" --parts "$PARTS" --out "$LOCAL_DIR/sf$SF"

echo "=== uploading to s3://$BUCKET/$DEST_PREFIX/sf$SF/ ==="
for t in "${TABLES[@]}"; do
  src="$LOCAL_DIR/sf$SF/$t"
  if [ ! -d "$src" ] || [ -z "$(ls -A "$src" 2>/dev/null)" ]; then
    echo "FATAL: generator produced no files for '$t' at $src"
    exit 1
  fi
  nfiles=$(ls -1 "$src"/*.parquet 2>/dev/null | wc -l | tr -d ' ')
  echo "  $t: $nfiles file(s) -> s3://$BUCKET/$DEST_PREFIX/sf$SF/$t/"
  "$AWS_BIN" s3 cp --recursive "$src/" \
    "s3://$BUCKET/$DEST_PREFIX/sf$SF/$t/" --region "$REGION" --no-progress
done

echo "=== verify ==="
"$AWS_BIN" s3 ls "s3://$BUCKET/$DEST_PREFIX/sf$SF/" \
  --region "$REGION" --recursive --human-readable --summarize | tail -25
echo "=== sf$SF parted upload complete ==="
