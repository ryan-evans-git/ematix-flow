#!/bin/bash
# Open a Session Manager shell on the Phase A EC2 box. Useful for
# ad-hoc fixes that don't fit the retry-stage.sh shape — e.g. git
# pulling latest, hand-running a specific cargo command, inspecting
# the file system after a stage failure.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_DIR="$(dirname "$SCRIPT_DIR")"
cd "$INFRA_DIR"

INSTANCE_ID=$(terraform output -raw phase_a_instance_id 2>/dev/null || true)
REGION=$(terraform output -raw region 2>/dev/null || echo "us-east-2")

if [[ -z "$INSTANCE_ID" ]]; then
  echo "ERROR: no Phase A instance found in terraform output" >&2
  exit 1
fi

echo "==> Opening SSM session on $INSTANCE_ID (region $REGION)"
exec aws ssm start-session --target "$INSTANCE_ID" --region "$REGION"
