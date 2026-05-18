#!/bin/bash
# Re-run a single bench stage on the running Phase A EC2 box via
# SSM Session Manager. Doesn't tear down or recreate any AWS
# resources — fast iteration loop for fixing one broken stage.
#
# Usage:
#   ./retry-stage.sh <stage-name>
#
# Examples:
#   ./retry-stage.sh 10-s3-runlog-real
#   ./retry-stage.sh 22-lambda-smoke-and-bench
#   ./retry-stage.sh 31-k8s-smoke
#
# The stage script lives in bench.sh; we re-extract and run just
# that stage's command. For ad-hoc commands (e.g. "git pull and
# rebuild before retrying"), use `./shell.sh` instead and run
# whatever you want interactively.

set -euo pipefail

STAGE="${1:?usage: retry-stage.sh <stage-name>}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_DIR="$(dirname "$SCRIPT_DIR")"

cd "$INFRA_DIR"

INSTANCE_ID=$(terraform output -raw phase_a_instance_id 2>/dev/null || true)
REGION=$(terraform output -raw region 2>/dev/null || echo "us-east-2")
BUCKET=$(terraform output -raw phase_b_bucket 2>/dev/null || true)
LAMBDA=$(terraform output -raw phase_c_lambda_name 2>/dev/null || true)
CLUSTER=$(terraform output -raw phase_d_cluster_name 2>/dev/null || true)
ECR=$(terraform output -raw phase_d_ecr_repo_url 2>/dev/null || true)

if [[ -z "$INSTANCE_ID" || -z "$BUCKET" ]]; then
  echo "ERROR: instance + bucket required (run terraform apply first)" >&2
  exit 1
fi

echo "==> Re-running stage '$STAGE' on $INSTANCE_ID"
echo "    Output streams to /tmp/$STAGE.log on the box AND to S3."
echo

# Send the stage via SSM. Re-exports the env vars bench.sh expects.
CMD=$(cat <<EOF
set -uo pipefail
export LAMBDA_FUNCTION_NAME='$LAMBDA'
export EKS_CLUSTER_NAME='$CLUSTER'
export ECR_REPO_URL='$ECR'
cd /opt/ematix/ematix-flow
sudo -u ec2-user bash /opt/ematix/ematix-flow/infra/test-validation/scripts/run-one-stage.sh '$BUCKET' '$REGION' '$STAGE'
EOF
)

CMD_ID=$(aws ssm send-command \
  --instance-ids "$INSTANCE_ID" \
  --region "$REGION" \
  --document-name "AWS-RunShellScript" \
  --parameters "commands=[\"$CMD\"]" \
  --query 'Command.CommandId' \
  --output text)

echo "    SSM Command ID: $CMD_ID"
echo "    Polling for completion (Ctrl-C to detach; the command keeps running)…"

while true; do
  STATUS=$(aws ssm list-command-invocations \
    --command-id "$CMD_ID" \
    --instance-id "$INSTANCE_ID" \
    --region "$REGION" \
    --query 'CommandInvocations[0].Status' \
    --output text)
  echo "    [$STATUS]"
  case "$STATUS" in
    Success|Failed|Cancelled|TimedOut)
      break
      ;;
  esac
  sleep 10
done

aws ssm get-command-invocation \
  --command-id "$CMD_ID" \
  --instance-id "$INSTANCE_ID" \
  --region "$REGION" \
  --query 'StandardOutputContent' \
  --output text

echo
echo "==> Pull the fresh log:"
echo "    aws s3 cp s3://$BUCKET/results/<timestamp>/$STAGE.log -"
