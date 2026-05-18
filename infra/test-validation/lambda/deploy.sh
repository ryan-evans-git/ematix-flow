#!/bin/bash
# Update the Lambda function created by Terraform with the real
# packaged code (replaces the stub Terraform put in place).
#
# Args:
#   $1: Lambda function name (from `terraform output -raw phase_c_lambda_name`)
#   $2: AWS region (from `terraform output -raw region`)
#
# Updates the function code, waits for the update to settle, prints
# the new code SHA so the caller can verify.

set -euo pipefail

FUNC="${1:?usage: deploy.sh <function-name> <region>}"
REGION="${2:?usage: deploy.sh <function-name> <region>}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_DIR="$(dirname "$SCRIPT_DIR")"
ZIP="$INFRA_DIR/.terraform-build/lambda-package.zip"

if [[ ! -f "$ZIP" ]]; then
  echo "ERROR: zip not found: $ZIP" >&2
  echo "       Run lambda/build-package.sh first." >&2
  exit 1
fi

echo "==> Updating function $FUNC in $REGION"

# Update the handler too — Terraform set it to "index.handler" for
# the stub; the real package uses "handler.handler".
aws lambda update-function-configuration \
  --region "$REGION" \
  --function-name "$FUNC" \
  --handler handler.handler \
  --timeout 60 \
  --memory-size 512 \
  >/dev/null

aws lambda wait function-updated \
  --region "$REGION" \
  --function-name "$FUNC"

aws lambda update-function-code \
  --region "$REGION" \
  --function-name "$FUNC" \
  --zip-file "fileb://$ZIP" \
  --output text \
  --query 'CodeSha256'

aws lambda wait function-updated \
  --region "$REGION" \
  --function-name "$FUNC"

echo "==> Deploy complete"
