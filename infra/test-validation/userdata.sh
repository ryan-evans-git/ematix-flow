#!/bin/bash
# Phase A EC2 userdata. Templated by main.tf; runs once at first boot.
#
# Responsibilities:
#   1. Install build deps (rustc, gcc, clang, git, tmux, jq).
#   2. Clone ematix-flow + ematix-parquet sibling.
#   3. Build --release with the host-native AVX target features
#      (Sapphire Rapids = avx512f).
#   4. Run the bench harness if a results bucket was supplied.
#   5. Set up a watchdog that terminates the instance after the
#      configured ceiling, in case bench.sh hangs.
#   6. Self-terminate via `shutdown -h now` when bench.sh finishes.
#
# Logs land in /var/log/cloud-init-output.log (visible via SSM).

set -euxo pipefail

REGION="${region}"
BUCKET="${results_bucket}"
PROJECT_TAG="${project_tag}"
MAX_RUNTIME_HRS="${max_runtime_hrs}"
LAMBDA_FUNCTION_NAME="${lambda_function_name}"
EKS_CLUSTER_NAME="${eks_cluster_name}"
ECR_REPO_URL="${ecr_repo_url}"
export LAMBDA_FUNCTION_NAME EKS_CLUSTER_NAME ECR_REPO_URL
WORKDIR=/opt/ematix
LOG=/var/log/ematix-bench.log

mkdir -p "$WORKDIR"
chown ec2-user:ec2-user "$WORKDIR"

# ---- Watchdog: SSM-scheduled self-terminate ----
#
# At-scheduled `shutdown` runs even if the bench harness wedges. With
# the instance's InstanceInitiatedShutdownBehavior=terminate, the
# `shutdown -h now` deletes the EC2 + EBS.
at_seconds=$(( MAX_RUNTIME_HRS * 3600 ))
( sleep "$at_seconds" && shutdown -h now ) &>/var/log/ematix-watchdog.log &

# ---- Toolchain ----
#
# `--allowerasing` is load-bearing on AL2023: the base image ships
# `curl-minimal`, and `dnf install curl` refuses the swap in non-
# interactive mode without it. Without the flag, the first dnf call
# fails and cloud-init aborts the entire userdata script.
# `curl` is dropped from the explicit list — `curl-minimal` already
# provides the binary; we just need the cmd, not the full curl rpm.
dnf install -y --allowerasing --setopt=install_weak_deps=False \
  gcc gcc-c++ make cmake git tmux jq tar gzip clang openssl-devel pkgconf-pkg-config \
  python3.12 python3.12-pip \
  docker libpq-devel sqlite-devel cyrus-sasl-devel \
  zip unzip \
  perl-FindBin perl-IPC-Cmd perl-File-Compare perl-File-Copy

# Start Docker. Phase D needs it for `docker buildx build` (image
# build + ECR push) and the bench harness uses `docker --version` as
# a sanity check.
systemctl enable --now docker
usermod -aG docker ec2-user

# `usermod -aG docker ec2-user` only takes effect on the *next* login
# of ec2-user. Every `sudo -u ec2-user bash` later in this userdata
# inherits the supplementary-group set from the existing session and
# doesn't see the docker group, so `docker buildx build` would fail
# with "permission denied on /var/run/docker.sock".
#
# Fix: wait for the daemon to come up, then loosen socket perms so
# ec2-user can use the daemon without a re-login. The campaign EC2 is
# single-tenant + short-lived + IAM-scoped at the AWS layer, so
# widening the local socket is bounded risk.
for i in $(seq 1 30); do
  [ -S /var/run/docker.sock ] && docker info >/dev/null 2>&1 && break
  sleep 1
done
chmod 666 /var/run/docker.sock

# ---- AWS CLI v2 ----
#
# AL2023 ships with awscli v1 in some images; v2 is needed for
# `aws ecr get-login-password` to produce the right output for
# `docker login`. Idempotent install.
if ! aws --version 2>&1 | grep -q 'aws-cli/2'; then
  curl -fsSL https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip -o /tmp/awscliv2.zip
  unzip -q /tmp/awscliv2.zip -d /tmp
  /tmp/aws/install --update
fi

# ---- kubectl ----
#
# Phase D's K8s submit script uses kubectl indirectly via the Python
# `kubernetes` client; aws CLI's `update-kubeconfig` does the IRSA
# wiring.
#
# Source: official k8s release bucket via dl.k8s.io. Avoids AWS's
# dated EKS-managed paths (e.g. `amazon-eks/1.30.0/2024-09-12/...`)
# which AWS rotates and breaks links to. `stable.txt` returns the
# current GA version (e.g. "v1.31.4"), so the box always gets
# whatever k8s.io is currently shipping — minor-version skew with
# the EKS cluster is fine for kubectl client compatibility.
# The $$KUBECTL_VERSION below is intentional. Terraform's
# templatefile() walks the whole file (including comments) and
# treats $$ as an escape for a literal single $, so the runtime
# script sees a bare shell variable reference. Without the escape,
# templatefile would try to substitute a KUBECTL_VERSION input
# from the vars map (which we don't pass) and error out.
KUBECTL_VERSION=$(curl -fsSL https://dl.k8s.io/release/stable.txt)
curl -fsSL "https://dl.k8s.io/release/$${KUBECTL_VERSION}/bin/linux/amd64/kubectl" \
  -o /usr/local/bin/kubectl
chmod +x /usr/local/bin/kubectl
/usr/local/bin/kubectl version --client --short 2>/dev/null || /usr/local/bin/kubectl version --client

# rustup as ec2-user so the toolchain lives under ~/.cargo
sudo -u ec2-user bash <<'RUSTUP'
set -euxo pipefail
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable --profile minimal
echo 'source $HOME/.cargo/env' >> $HOME/.bashrc
RUSTUP

# ---- Clone repos ----
sudo -u ec2-user bash <<REPOS
set -euxo pipefail
source \$HOME/.cargo/env
cd "$WORKDIR"
git clone --depth 1 https://github.com/ryan-evans-git/ematix-parquet.git
git clone --depth 1 https://github.com/ryan-evans-git/ematix-flow.git
REPOS

# ---- Build ----
#
# RUSTFLAGS targets host-native — on c7i this picks up AVX-512F + BMI2
# + the SHA-NI extensions. We rebuild in release mode; debug builds
# are not representative.
sudo -u ec2-user bash <<'BUILD'
set -euxo pipefail
source $HOME/.cargo/env
cd /opt/ematix/ematix-flow
export RUSTFLAGS="-C target-cpu=native"
# Force openssl-sys to use the system openssl-devel headers instead of
# vendoring a fresh build. The vendored path requires perl-FindBin etc
# (added to dnf above) and is slow even when it works. System openssl
# 3.x on AL2023 is fine for our (tokio/reqwest) callers.
export OPENSSL_NO_VENDOR=1
cargo build --release --workspace 2>&1 | tee -a /var/log/ematix-build.log
BUILD

# ---- Run the bench harness ----
#
# bench.sh lives in the cloned repo. Run it directly instead of
# pulling from S3 — keeps the user's local laptop out of the launch
# critical path (no "upload bench.sh first" step).
if [[ -n "$BUCKET" ]]; then
  echo "results bucket: $BUCKET" | tee -a "$LOG"
  # `tee -a "$LOG"` inside the sudo-to-ec2-user heredoc would EPERM
  # against /var/log/ematix-bench.log (root-owned). On a previous
  # campaign that pipe failure under `set -euxo pipefail` aborted the
  # bench before any S3 upload happened. Hand the log to ec2-user.
  touch "$LOG"
  chown ec2-user:ec2-user "$LOG"
  # Glob-resilient: `chmod +x .../lambda/*.sh` under `set -e` aborts the
  # whole bench if the dir is empty (literal `*.sh` failure). `2>/dev/null
  # || true` keeps the campaign going even if the scripts directory was
  # not part of the cloned checkout.
  sudo -u ec2-user bash <<RUNBENCH
set -uxo pipefail
source \$HOME/.cargo/env
chmod +x /opt/ematix/ematix-flow/infra/test-validation/scripts/bench.sh
chmod +x /opt/ematix/ematix-flow/infra/test-validation/lambda/*.sh 2>/dev/null || true
chmod +x /opt/ematix/ematix-flow/infra/test-validation/k8s/*.sh 2>/dev/null || true
LAMBDA_FUNCTION_NAME='$LAMBDA_FUNCTION_NAME' \\
EKS_CLUSTER_NAME='$EKS_CLUSTER_NAME' \\
ECR_REPO_URL='$ECR_REPO_URL' \\
  /opt/ematix/ematix-flow/infra/test-validation/scripts/bench.sh '$BUCKET' '$REGION' 2>&1 | tee -a "$LOG"
RUNBENCH
fi

# ---- Self-terminate ----
#
# The instance's shutdown behaviour is "terminate", so this both
# powers off and deletes. Watchdog above is the backup.
shutdown -h now
