#!/bin/bash
# Phase A bench harness — orchestrates the full Tier 3 validation
# campaign. Runs on the EC2 box.
#
# Args:
#   $1: S3 bucket name for results
#   $2: AWS region
#
# Optional env (set by userdata.sh before invoking):
#   LAMBDA_FUNCTION_NAME   — Phase C function (skip Lambda stages if unset)
#   EKS_CLUSTER_NAME       — Phase D cluster (skip K8s stages if unset)
#   ECR_REPO_URL           — Phase D ECR repo (skip K8s stages if unset)
#
# Each stage streams its log to S3 as it completes, so a spot
# eviction doesn't lose earlier results.

set -uo pipefail

BUCKET="$1"
REGION="$2"
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
PREFIX="results/$STAMP"

REPO_ROOT=/opt/ematix/ematix-flow
INFRA_DIR="$REPO_ROOT/infra/test-validation"

AWS_BIN="${AWS_BIN:-$(command -v aws || echo /usr/local/bin/aws)}"

# Sanity-check aws CLI BEFORE any stage runs. If we can't upload, the
# whole campaign is wasted compute — fail loud + leave a marker on disk
# so SSM diagnosis can succeed even if we can't push to S3.
if ! "$AWS_BIN" --version >>"/tmp/aws-sanity.log" 2>&1; then
  echo "FATAL: aws CLI not at $AWS_BIN (PATH=$PATH)" | tee -a /tmp/aws-sanity.log
  echo "Available in /usr/local/bin: $(ls /usr/local/bin/ 2>&1 | head -20)" >> /tmp/aws-sanity.log
  exit 3
fi

upload() {
  local src="$1" dst="$2"
  # Don't swallow upload errors — write them to a sibling log so the
  # box can be SSM-inspected if uploads ever silently fail. Last
  # campaign produced zero S3 objects because errors here were masked.
  if ! "$AWS_BIN" s3 cp "$src" "s3://$BUCKET/$PREFIX/$dst" --region "$REGION" \
      >>"/tmp/upload-errors.log" 2>&1; then
    echo "UPLOAD FAIL rc=$? src=$src dst=$dst" >> "/tmp/upload-errors.log"
  fi
}

stage() {
  local name="$1" cmd="$2"
  local log="/tmp/$name.log"
  echo "=== [$(date -u +%H:%M:%S)] $name ===" | tee -a "$log"
  # NB: outer script sets `set -uo pipefail` (no -e). Do NOT re-arm -e
  # here; doing so primed a foot-gun where the first non-zero return
  # from `upload`, `echo`, or the next stage's first command would
  # abort the whole campaign mid-run.
  bash -c "$cmd" >>"$log" 2>&1
  local rc=$?
  echo "=== [$(date -u +%H:%M:%S)] $name: rc=$rc ===" | tee -a "$log"
  upload "$log" "$name.log"
  return $rc
}

cd "$REPO_ROOT" || { echo "FATAL: cannot cd $REPO_ROOT" >&2; exit 4; }
source "$HOME/.cargo/env" || true

# ============================================================
# Phase A: host info + Rust-side tests + benches
# ============================================================

stage "00-host-info" '
  uname -a
  echo "---"
  lscpu
  echo "---"
  free -h
  echo "---"
  df -h /
  echo "---"
  rustc --version
  cargo --version
  echo "---"
  python3 --version
  docker --version 2>/dev/null || echo "docker: not yet"
' || true

# Stage 01 (`cargo test --release --workspace`) and stage 08
# (`cargo test ... dict_preservation_end_to_end`) were dropped per
# F.2a in docs/CAMPAIGN_HARNESS_FIXES.md: CI already covers these on
# every PR, and the 2026-05-17 campaign ate ~1 hr of paid EC2 time
# on work CI does for free. The campaign should focus only on what
# CI can't reproduce (real x86 AVX-512 hardware, SF=10+ data, real
# AWS services). To re-enable: see commit history for the original.

# Skipped: stage 02-cargo-test-integration-x86. The testcontainer-
# gated integration tests need Docker access from ec2-user, but
# `usermod -aG docker ec2-user` in userdata doesn't take effect for
# the current sudo'd shell — testcontainers would error on socket
# permission denied. CI already runs this stage cleanly on linux/x86
# so there's no campaign value in repeating it here.

# ============================================================
# Phase A: TPC-H data generation FIRST (stages 03/03a need it)
# ============================================================
# Per F.5 in CAMPAIGN_HARNESS_FIXES.md: stages 03 + 03a need
# TPCH_DATA_DIR populated, so SF=1/SF=10 generation must come first.
# Previously 03/03a ran before 04/05 and silently exited with
# "TPC-H data not found", losing the AVX bench data.

stage "04-tpch-generate-sf1" '
  cd /opt/ematix/ematix-flow
  cargo run --release -p ematix-flow-core --example tpch_generate -- \
    --sf 1 --out examples/tpch/data/sf1
' || true

stage "05-tpch-generate-sf10" '
  cd /opt/ematix/ematix-flow
  cargo run --release -p ematix-flow-core --example tpch_generate -- \
    --sf 10 --out examples/tpch/data/sf10
' || true

# ============================================================
# Phase A: ematix-parquet AVX / adaptive / parallel benches
# ============================================================

stage "03-emat-parquet-avx-benches" '
  set -e
  [ -d "/opt/ematix/ematix-flow/examples/tpch/data/sf1" ] || { echo "SKIP: SF=1 data not generated"; exit 0; }
  cd /opt/ematix/ematix-parquet
  # bench_late_mat + bench_q14_late_mat are examples (under examples/),
  # not Cargo benches (there is no benches/ directory). Use `--example`,
  # not `--bench`. The harness exits non-zero on a non-existent target.
  cargo run --release -p ematix-parquet-codec --example bench_late_mat 2>&1 | tail -300
  cargo run --release -p ematix-parquet-codec --example bench_q14_late_mat 2>&1 | tail -300
' || true

# Π.14 (ematix-parquet v0.8.0) — adaptive predicate dispatch. Sweeps
# selectivity and compares bitmap_only / fused_then_gather /
# materialised / adaptive paths against real lineitem `l_shipdate`.
# Validates DEFAULT_THRESHOLD=0.10 holds on x86-Linux (today calibrated
# on M-series; expectation is the crossover stays near 0.10 but with
# absolute timings shifted by the AVX-vs-NEON unpack diff).
stage "03a-emat-adaptive-dispatch-bench" '
  set -e
  [ -d "/opt/ematix/ematix-flow/examples/tpch/data/sf1" ] || { echo "SKIP: SF=1 data not generated"; exit 0; }
  cd /opt/ematix/ematix-parquet
  TPCH_DATA_DIR=/opt/ematix/ematix-flow/examples/tpch/data/sf1 \
    cargo run --release --example bench_adaptive_dispatch -p ematix-parquet-codec 2>&1 | tail -200
' || true

# Π.15 (ematix-parquet v0.9.0) — parallel multi-RG decode scaling.
# 50-RG synthetic fixture, sequential vs `read_columns_parallel` at
# 1/2/4/8/… threads up to host CPU. The codec README notes a known
# `ParquetFile.file: Mutex<File>` serialisation point that caps
# speedup at ~1.8× on the M-series; on c7i.2xlarge (16 vCPU, single
# socket) we want to see how that ceiling moves. Multi-socket NUMA
# validation deferred until c7i.metal — out of budget for this run.
stage "03b-emat-parallel-scaling-bench" '
  set -e
  cd /opt/ematix/ematix-parquet
  cargo run --release --features parallel \
    -p ematix-parquet-codec --example bench_parallel_scaling 2>&1 | tail -200
' || true

# ============================================================
# Phase A: TPC-H triangulation (per-SF outputs, F.6)
# ============================================================
# Each stage writes its own BENCHMARKS-SF{N}.md so the SF=1 run
# doesn't get overwritten by SF=10. The example honors the
# `TPCH_OUT` env var (see crates/ematix-flow-core/examples/
# tpch_triangulation_bench.rs); if unset, falls back to legacy
# BENCHMARKS.md (kept for backward compat with local dev).

# AWS campaign 2026-05: 20 trials × 5 warmups for publishable medians.
# 5 trials × 2 warmups (the prior default) was too noisy on sub-15ms
# queries — Q06/Q15/Q22 swung ±15% run-to-run, masking real wins. At
# 20×5, sub-millisecond noise collapses. See
# docs/AWS_CAMPAIGN_2026_05_PLAN.md for the methodology rationale.
stage "06-tpch-triangulation-sf1" '
  cd /opt/ematix/ematix-flow
  TPCH_DATA_DIR=examples/tpch/data/sf1 \
    TPCH_OUT=/opt/ematix/ematix-flow/BENCHMARKS-SF1.md \
    TPCH_TRIALS=20 TPCH_WARMUPS=5 \
    cargo run --release --features triangulation \
    -p ematix-flow-core --example tpch_triangulation_bench 2>&1 | tail -400
' || true
upload /opt/ematix/ematix-flow/BENCHMARKS-SF1.md "06-tpch-triangulation-sf1-BENCHMARKS.md" 2>/dev/null || true

stage "07-tpch-triangulation-sf10" '
  cd /opt/ematix/ematix-flow
  TPCH_DATA_DIR=examples/tpch/data/sf10 \
    TPCH_OUT=/opt/ematix/ematix-flow/BENCHMARKS-SF10.md \
    TPCH_TRIALS=20 TPCH_WARMUPS=5 \
    cargo run --release --features triangulation \
    -p ematix-flow-core --example tpch_triangulation_bench 2>&1 | tail -400
' || true
upload /opt/ematix/ematix-flow/BENCHMARKS-SF10.md "07-tpch-triangulation-sf10-BENCHMARKS.md" 2>/dev/null || true

# Stage 08 (`cargo test ... dict_preservation_end_to_end`) was
# dropped per F.2a in docs/CAMPAIGN_HARNESS_FIXES.md: CI covers it
# on every PR, and on the 2026-05-17 campaign it ate ~25 min of EC2
# time before being SIGTERM'd. The behavioural side of dict
# preservation is implicitly tested by stages 06/07 (TPC-H queries
# that GROUP BY string columns engage EnableDictGroupCountRule when
# with_dict_preservation(true) is wired).

# ============================================================
# Phase B: real-S3 S3RunLog driver
# ============================================================

stage "10-s3-runlog-real" "
  cd /opt/ematix/ematix-flow
  # Use python3.12 explicitly — the AL2023 base ships python3.9 as
  # python3 by default; we install python3.12 in userdata for the
  # ematix-flow wheel surface. Pip-install via the same interpreter
  # so boto3 lands in 3.12's site-packages.
  python3.12 -m pip install --quiet --user boto3 \\
    || python3.12 -m pip install --break-system-packages --quiet boto3
  S3_BUCKET='$BUCKET' AWS_REGION='$REGION' FLOW_REPO_ROOT='$REPO_ROOT' \\
    python3.12 $INFRA_DIR/tests/test_s3_runlog_real.py 2>&1 | tail -200
" || true

# ============================================================
# Phase C: Lambda packaging + smoke
# ============================================================

if [[ -n "${LAMBDA_FUNCTION_NAME:-}" ]]; then
  stage "20-lambda-package-build" "
    cd $REPO_ROOT
    python3.12 -m pip install --quiet --user maturin \\
      || python3.12 -m pip install --break-system-packages --quiet maturin
    $INFRA_DIR/lambda/build-package.sh 2>&1 | tail -100
  " || true

  stage "21-lambda-deploy" "
    cd $REPO_ROOT
    $INFRA_DIR/lambda/deploy.sh '$LAMBDA_FUNCTION_NAME' '$REGION' 2>&1 | tail -100
  " || true

  stage "22-lambda-smoke-and-bench" "
    LAMBDA_FUNCTION_NAME='$LAMBDA_FUNCTION_NAME' AWS_REGION='$REGION' \\
      python3.12 $INFRA_DIR/tests/test_lambda_real.py 2>&1 | tail -200
  " || true
fi

# ============================================================
# Phase D: ECR image build + push + K8s submit
# ============================================================

if [[ -n "${EKS_CLUSTER_NAME:-}" && -n "${ECR_REPO_URL:-}" ]]; then
  stage "30-docker-build-push" "
    cd $REPO_ROOT
    $INFRA_DIR/k8s/build-and-push.sh '$ECR_REPO_URL' '$REGION' 2>&1 | tail -200
  " || true

  stage "31-k8s-smoke" "
    python3.12 -m pip install --quiet --user kubernetes pyyaml \\
      || python3.12 -m pip install --break-system-packages --quiet kubernetes pyyaml
    EKS_CLUSTER_NAME='$EKS_CLUSTER_NAME' \\
    ECR_IMAGE='${ECR_REPO_URL}:validation' \\
    AWS_REGION='$REGION' \\
      python3.12 $INFRA_DIR/tests/test_k8s_real.py 2>&1 | tail -200
  " || true
fi

# ============================================================
# Distributed end-to-end (uses Phase B + C + D)
# ============================================================

if [[ -n "${LAMBDA_FUNCTION_NAME:-}" && -n "${EKS_CLUSTER_NAME:-}" && -n "${ECR_REPO_URL:-}" ]]; then
  stage "40-distributed-e2e" "
    S3_BUCKET='$BUCKET' \\
    LAMBDA_FUNCTION_NAME='$LAMBDA_FUNCTION_NAME' \\
    EKS_CLUSTER_NAME='$EKS_CLUSTER_NAME' \\
    ECR_IMAGE='${ECR_REPO_URL}:validation' \\
    AWS_REGION='$REGION' \\
    FLOW_REPO_ROOT='$REPO_ROOT' \\
      python3.12 $INFRA_DIR/tests/test_distributed_e2e.py 2>&1 | tail -200
  " || true
fi

# ============================================================
# Finalise
# ============================================================

stage "99-summary" "
  echo 'campaign complete'
  echo \"results: s3://$BUCKET/$PREFIX/\"
  date -u
"

upload /var/log/ematix-bench.log "00-userdata.log"
upload /var/log/ematix-build.log "00-build.log"
# Push diagnostic taps so a partial/failed campaign is still inspectable
# from S3 after the EC2 (and its EBS) are gone.
upload /tmp/aws-sanity.log "00-aws-sanity.log"
upload /tmp/upload-errors.log "00-upload-errors.log"
echo "results uploaded to s3://$BUCKET/$PREFIX/"
