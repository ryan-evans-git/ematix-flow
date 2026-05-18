#!/bin/bash
# Sync campaign results from S3 to a local results/ directory.
# Run before `terraform destroy` — destroy deletes the bucket
# (force_destroy=true) and EVERYTHING in it.
#
# Idempotent: re-runs are cheap thanks to `s3 sync`'s diff logic.
#
# Usage:
#   ./fetch-results.sh
#   ./fetch-results.sh --dest /path/to/dir

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_DIR="$(dirname "$SCRIPT_DIR")"

DEST="$INFRA_DIR/results"
if [[ "${1:-}" == "--dest" ]]; then
  DEST="${2:?--dest requires a path}"
fi

cd "$INFRA_DIR"

BUCKET=$(terraform output -raw phase_b_bucket 2>/dev/null || true)
REGION=$(terraform output -raw region 2>/dev/null || echo "us-east-2")
if [[ -z "$BUCKET" ]]; then
  echo "ERROR: phase_b_bucket not found in terraform output" >&2
  echo "       (either no apply was run, or Phase B was disabled)" >&2
  exit 1
fi

mkdir -p "$DEST"
echo "==> Syncing s3://$BUCKET/results/ → $DEST/"
aws s3 sync "s3://$BUCKET/results/" "$DEST/" --region "$REGION" \
  --no-progress

# Also pull the bench/user-data logs that live at the bucket root.
echo "==> Syncing root-level logs"
aws s3 sync "s3://$BUCKET/" "$DEST/_root/" --region "$REGION" \
  --no-progress --exclude "results/*"

echo "==> Done. Results in $DEST/"
du -sh "$DEST"
