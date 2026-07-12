#!/bin/bash
# Base bootstrap — runs on every node before the engine-specific
# userdata. Idempotent. Installs git, awscli v2, python3.12, then
# clones the ematix-flow repo into /opt/ematix/ematix-flow so engine
# scripts can `cd` there for SQL queries / bench helpers.
#
# Variables provided by templatefile():
#   ${aws_region}   — region for awscli default
#   ${bench_bucket} — exported for downstream scripts
#   ${git_ref}      — ematix-flow ref to clone (must be PUSHED to GitHub)

set -uo pipefail
exec > >(tee /var/log/base-userdata.log) 2>&1

echo "=== base userdata: $(date -u +%FT%TZ) ==="
uname -a
head -5 /etc/os-release || true

# Pin defaults for the rest of cloud-init
export AWS_DEFAULT_REGION=${aws_region}
echo "export AWS_DEFAULT_REGION=${aws_region}" >> /etc/profile.d/ematix-env.sh
echo "export BENCH_BUCKET=${bench_bucket}" >> /etc/profile.d/ematix-env.sh
chmod 644 /etc/profile.d/ematix-env.sh

# AL2023 base — minimal additions
dnf install -y git python3.12 python3.12-pip jq tar gzip xz

# AWS CLI v2 (AL2023 ships v1 by default; v2 is what production users have)
if ! /usr/local/bin/aws --version 2>/dev/null | grep -q "aws-cli/2"; then
  cd /tmp
  curl -sSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o awscliv2.zip
  unzip -q awscliv2.zip
  ./aws/install --update
  rm -rf aws awscliv2.zip
fi
/usr/local/bin/aws --version

# Clone the ematix-flow repo so engine scripts can reach SQL queries
# (examples/tpch/queries/q*.sql) + bench helpers.
mkdir -p /opt/ematix
cd /opt/ematix
if [ ! -d ematix-flow ]; then
  git clone --depth 1 --branch "${git_ref}" https://github.com/ryan-evans-git/ematix-flow.git
fi
echo "ematix-flow @ $(git -C ematix-flow rev-parse HEAD) (ref ${git_ref})"

# Make repo world-readable; engine scripts run as ec2-user.
chown -R ec2-user:ec2-user /opt/ematix
chmod -R a+rX /opt/ematix

echo "=== base userdata: done ==="
