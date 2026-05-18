#!/bin/bash
# Internal helper called by retry-stage.sh (via SSM). Extracts and
# runs a single stage from bench.sh.
#
# Args:
#   $1: results bucket
#   $2: AWS region
#   $3: stage name (e.g. "10-s3-runlog-real")

set -uo pipefail

BUCKET="$1"
REGION="$2"
WANT="$3"

STAMP=$(date -u +%Y%m%dT%H%M%SZ)
PREFIX="results/$STAMP-retry-$WANT"
log="/tmp/$WANT.log"

upload() {
  local src="$1" dst="$2"
  aws s3 cp "$src" "s3://$BUCKET/$PREFIX/$dst" --region "$REGION" >/dev/null 2>&1 || true
}

cd /opt/ematix/ematix-flow
source "$HOME/.cargo/env" || true

# Run the stage. bench.sh's `stage` helper does the same thing — we
# re-implement it minimally here so we don't have to source bench.sh
# (which would run every stage).
BENCH="/opt/ematix/ematix-flow/infra/test-validation/scripts/bench.sh"
if ! grep -q "stage \"$WANT\"" "$BENCH"; then
  echo "ERROR: stage '$WANT' not found in bench.sh" >&2
  echo "Available stages:" >&2
  grep -oE 'stage "[0-9]+-[a-z0-9-]+"' "$BENCH" | sort -u >&2
  exit 1
fi

# Extract the bash command associated with this stage from bench.sh.
# Pattern: stage "<name>" '... command ...' — the heredoc-delimited
# multi-line shell command between the first ' and the matching close.
CMD=$(awk -v want="$WANT" '
  $0 ~ "stage \"" want "\"" {
    found=1
    sub(/^.*stage "[^"]*" /, "")
    delim=substr($0,1,1)
    sub(/^./,"")
    if (index($0, delim) > 0) {
      sub(/.$/, "")
      print
      exit
    }
    print
    next
  }
  found {
    if ($0 ~ "^[\"'\''] \\|\\| true$" || $0 ~ "^[\"'\''] *$" ) { exit }
    print
  }
' "$BENCH")

if [[ -z "$CMD" ]]; then
  echo "ERROR: failed to extract command for stage '$WANT'" >&2
  exit 1
fi

echo "=== [$(date -u +%H:%M:%S)] retry $WANT ===" | tee -a "$log"
echo "$CMD" >> "$log"
echo "---" >> "$log"
bash -c "$CMD" >> "$log" 2>&1
rc=$?
echo "=== [$(date -u +%H:%M:%S)] $WANT: rc=$rc ===" | tee -a "$log"
upload "$log" "$WANT.log"
exit $rc
