#!/bin/bash
# Teardown verification. Run AFTER `terraform destroy`.
#
# Checks every service we touch in the campaign for resources still
# tagged with the project tag. Aborts with the list if anything's
# left so we can clean it up explicitly.

set -euo pipefail

PROJECT_TAG="${PROJECT_TAG:-ematix-flow-test}"
REGION="${AWS_REGION:-us-east-2}"

red()    { printf '\033[31m%s\033[0m\n' "$*"; }
green()  { printf '\033[32m%s\033[0m\n' "$*"; }
yellow() { printf '\033[33m%s\033[0m\n' "$*"; }

check() {
  local name="$1"
  local query="$2"
  local cmd="$3"
  local out
  out=$(eval "$cmd" 2>/dev/null || true)
  if [[ -z "$out" || "$out" == "[]" || "$out" == "null" ]]; then
    green "  [OK] $name: clean"
    return 0
  else
    red   "  [LEAK] $name:"
    echo "$out" | sed 's/^/    /'
    return 1
  fi
}

yellow "Teardown verification — project=$PROJECT_TAG region=$REGION"
echo

leaks=0

check "EC2 instances (non-terminated)" "" \
  "aws ec2 describe-instances --region $REGION --filters Name=tag:Project,Values=$PROJECT_TAG Name=instance-state-name,Values=pending,running,shutting-down,stopping,stopped --query 'Reservations[].Instances[].InstanceId' --output json" \
  || leaks=$((leaks+1))

check "EBS volumes" "" \
  "aws ec2 describe-volumes --region $REGION --filters Name=tag:Project,Values=$PROJECT_TAG --query 'Volumes[].VolumeId' --output json" \
  || leaks=$((leaks+1))

check "Security groups" "" \
  "aws ec2 describe-security-groups --region $REGION --filters Name=tag:Project,Values=$PROJECT_TAG --query 'SecurityGroups[].GroupId' --output json" \
  || leaks=$((leaks+1))

check "S3 buckets" "" \
  "aws s3api list-buckets --query 'Buckets[?starts_with(Name, \`$PROJECT_TAG\`)].Name' --output json" \
  || leaks=$((leaks+1))

check "Lambda functions" "" \
  "aws lambda list-functions --region $REGION --query 'Functions[?starts_with(FunctionName, \`$PROJECT_TAG\`)].FunctionName' --output json" \
  || leaks=$((leaks+1))

check "EKS clusters" "" \
  "aws eks list-clusters --region $REGION --query 'clusters[?starts_with(@, \`$PROJECT_TAG\`)]' --output json" \
  || leaks=$((leaks+1))

check "EKS node groups (orphans, all clusters)" "" \
  "aws eks list-clusters --region $REGION --query 'clusters[?starts_with(@, \`$PROJECT_TAG\`)]' --output json" \
  || leaks=$((leaks+1))

check "ECR repositories" "" \
  "aws ecr describe-repositories --region $REGION --query 'repositories[?starts_with(repositoryName, \`$PROJECT_TAG\`)].repositoryName' --output json 2>/dev/null" \
  || leaks=$((leaks+1))

check "IAM roles" "" \
  "aws iam list-roles --query 'Roles[?starts_with(RoleName, \`$PROJECT_TAG\`)].RoleName' --output json" \
  || leaks=$((leaks+1))

check "Instance profiles" "" \
  "aws iam list-instance-profiles --query 'InstanceProfiles[?starts_with(InstanceProfileName, \`$PROJECT_TAG\`)].InstanceProfileName' --output json" \
  || leaks=$((leaks+1))

check "Key pairs" "" \
  "aws ec2 describe-key-pairs --region $REGION --filters Name=tag:Project,Values=$PROJECT_TAG --query 'KeyPairs[].KeyName' --output json" \
  || leaks=$((leaks+1))

echo
if (( leaks == 0 )); then
  green "All clear. Zero residue."
  exit 0
else
  red "$leaks category/categories leaked. Investigate above."
  exit 1
fi
