#!/bin/bash
# Build the flow worker image and push to the campaign's ECR repo.
#
# Args:
#   $1: ECR repository URL (from `terraform output -raw phase_d_ecr_repo_url`)
#   $2: AWS region (from `terraform output -raw region`)
#
# Run on a machine with Docker daemon + AWS credentials. On EC2
# Phase A this is the c7i.2xlarge box itself (Docker is preinstalled
# on AL2023; the IAM role attached to the instance covers ECR
# auth via `aws ecr get-login-password`).

set -euo pipefail

ECR_URL="${1:?usage: build-and-push.sh <ecr-url> <region>}"
REGION="${2:?usage: build-and-push.sh <ecr-url> <region>}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_DIR="$(dirname "$SCRIPT_DIR")"
REPO_ROOT="$(cd "$INFRA_DIR/../.." && pwd)"

TAG="${TAG:-validation}"
FULL="${ECR_URL}:${TAG}"

echo "==> Logging into ECR (region=$REGION)"
aws ecr get-login-password --region "$REGION" \
  | docker login --username AWS --password-stdin "${ECR_URL%%/*}"

echo "==> Building flow worker image for linux/amd64"
# `buildx` enables cross-arch builds — on Mac M-series we need it
# to produce an amd64 image for EKS t3.medium nodes.
docker buildx build \
  --platform linux/amd64 \
  --tag "$FULL" \
  --file "$INFRA_DIR/Dockerfile.flow-worker" \
  --push \
  "$REPO_ROOT"

echo "==> Pushed: $FULL"

# Echo the immutable digest so the K8s Job manifest can pin by
# digest rather than tag — eliminates "image moved under us" race
# between submit and pod start.
DIGEST=$(aws ecr describe-images \
  --region "$REGION" \
  --repository-name "${ECR_URL##*/}" \
  --image-ids imageTag="$TAG" \
  --query 'imageDetails[0].imageDigest' \
  --output text)
echo "    digest=$DIGEST"
echo "$DIGEST" > "$INFRA_DIR/.terraform-build/flow-worker-digest"
