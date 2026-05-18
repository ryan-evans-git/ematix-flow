# AWS test/bench validation plan

**Goal:** run the tests + benches we can't run on a Mac M-series locally,
against real AWS services, with minimal spend and minimum residue. Total
budget target: **< $5**. Target wall clock for the whole campaign: **one
afternoon**.

## What we can't validate locally

| # | Capability | Why local doesn't cover it |
|---|---|---|
| 1 | `ematix-parquet` AVX2/AVX-512 SIMD path | Mac is ARM (NEON only). The x86 bitpack path compiles in CI but has never executed on real Intel silicon. |
| 2 | TPC-H SF=10+ triangulation on many-core x86 | M-series has ~10-12 perf cores; can't tell apart algorithmic wins from core-count effects above SF=1. |
| 3 | Adaptive predicate-dispatch threshold (Π.14, v0.8.0) | `DEFAULT_THRESHOLD=0.10` was calibrated on M-series. AVX vs NEON unpack timings shift absolute values; we need to confirm the crossover still lives near 0.10 on x86. |
| 4 | Parallel multi-RG decode scaling (Π.15, v0.9.0) | M-series sees the parallel path lose to sequential at every thread count (best: 0.68× speedup at N=4) because of a `ParquetFile.file: Mutex<File>` bottleneck. Need to confirm whether x86's heavier decompress shifts the compute:I/O ratio enough to recover scaling. |
| 5 | `S3RunLog` against real S3 | Today we test against MinIO. Real S3 has different consistency, multipart-upload, and 5xx-retry behaviour. |
| 6 | `LambdaExecutor` against real Lambda | Mock-tested only. No assurance the wheel actually runs in Lambda's Python runtime + ENI cold-start path. |
| 7 | `K8sJobExecutor` against real EKS | Validated against `kind`. EKS adds IRSA, AWS LB controller, image-pull-from-ECR, autoscaler — none of which `kind` models. |
| 8 | Multi-socket NUMA scaling (Π.15 acceptance #1) | **Deferred.** c7i.2xlarge is single-socket. The multi-socket NUMA-pinned-pool validation needs c7i.metal (~$3.50/hr) which is out of scope this campaign. Tracked as a follow-up. |

Items 1, 2, 3, 4 are *bench* deliverables (numbers we want to publish).
Items 5–7 are *correctness* deliverables. Item 8 is on the roadmap.

### Local M-series baseline (2026-05-17) for the parallel scaling bench

```
Sequential                           3.47 ms   1.00×
parallel(N=1)                       12.65 ms   0.27×
parallel(N=2)                        8.56 ms   0.41×
parallel(N=4)                        5.08 ms   0.68×
parallel(N=8)                       11.27 ms   0.31×
parallel(N=14)                       7.66 ms   0.45×
```
Single-NUMA, mutex-bound. On x86 we expect the absolute decompress
work to be heavier (no NEON-fused snappy variant), which may flip
the compute:I/O ratio enough for parallel to win.

## Phasing (sequential, each phase tears down before the next)

### Phase A — compute box: AVX bench + SF=10 triangulation (~$1.50)

- **Instance:** `c7i.2xlarge` (8 vCPU Sapphire Rapids, 16 GB, AVX-512) on **spot**.
  - Spot is ~$0.13/hr vs $0.43/hr on-demand. Spot interruption is
    survivable since results stream to S3 each query; a 4-hr campaign
    has < 5% historical spot eviction rate in `us-east-2`.
  - On-demand fallback automatically if spot pool is empty.
- **Storage:** 100 GB gp3 EBS (deleted with instance).
- **AMI:** Amazon Linux 2023 (Rust toolchain via rustup in userdata).
- **Lifecycle:** spin up → userdata installs Rust + clones repo + builds
  `--release` with `+avx2,+avx512f` → SSM-runs bench harness → uploads
  `BENCHMARKS-AWS.md` to S3 → terminates itself via `shutdown -h now`.
  EC2 `InstanceInitiatedShutdownBehavior=terminate` makes this clean.
- **Tests run:**
  - `cargo test --release --workspace --include-ignored` (full Linux
    integration matrix on x86 toolchain — orthogonal to Mac runs).
  - `cargo bench` on `bitpack_avx2_unit`, `bench_q14_late_mat`,
    `bench_late_mat` from ematix-parquet.
  - TPC-H 22 triangulation at SF=1 and SF=10 (ematix-flow vs DuckDB vs
    Polars), with and without `with_dict_preservation(true)`.
  - Σ.E3b DictGroupCountExec real-data validation: Q1 on SF=10 lineitem
    with `EnableDictGroupCountRule` enabled — verify it fires now that
    the Emat reader preserves dict (the rule was a no-op on materialised
    Utf8 input previously, per [[dict-arrival-blocker]]).
- **Watchdog:** SSM Run Command schedules `terminate-instances` 5 hours
  after launch in case the self-shutdown path fails.
- **Cost ceiling:** 4 hours active = $0.52 spot. EBS $0.10 for 4 hours
  on 100 GB gp3 = ~$0.06. Total < $1.

### Phase B — S3RunLog real-service test (~$0.05)

- **Bucket:** `ematix-flow-test-<8-char-random>` in `us-east-2`.
- **Object lifecycle policy:** auto-delete after 24 hours (belt-and-braces
  against the explicit teardown).
- **Tests run** (still on the Phase A box before it terminates):
  - `cargo test -p ematix-flow-core --features s3 s3_run_log_ --include-ignored`
    pointed at the real bucket via env vars.
  - Manual sanity: write a 200-row RunLog, read it back, list partial,
    list complete.
- **Cost:** a few cents for storage + requests during testing.

### Phase C — Lambda smoke test (~$0.01)

- **Function:** `ematix-flow-test-lambda-<random>`, Python 3.12, x86_64,
  256 MB memory, 1 minute timeout.
- **Package:** built from `ematix-flow-py` wheel + tiny handler that
  invokes `LambdaExecutor`'s expected entrypoint.
- **Test:** invoke once with a synthetic event matching what
  `LambdaExecutor::execute` sends, assert success response.
- **Cost:** essentially zero (10s × 256 MB invocation is well under the
  monthly free tier).
- **Lifecycle:** create → invoke → assert → delete.

### Phase D — EKS K8sJobExecutor smoke test (~$0.50)

This phase is the costliest by control-plane charge but in-scope per
decision 2026-05-16: real EKS, not kind-on-EC2, to cover IRSA + ECR
pull + cluster autoscaler.

- **Cluster:** EKS 1.30, 1× t3.medium node group, public endpoint
  (avoids NAT-gw cost).
- **ECR:** push a single image of the flow worker.
- **Test:** apply a Job manifest produced by `K8sJobExecutor::render_job`,
  poll for completion, assert pod exit code 0.
- **Cost:** $0.10/hr control plane × 2 hours = $0.20; node ~$0.04/hr × 2
  = $0.08; ECR storage pennies. Total < $0.50.
- **Lifecycle:** create cluster → push image → run job → delete cluster
  → delete ECR repo.

### Phase E — teardown verification

A `scripts/aws-teardown-verify.sh` that runs:

```
aws ec2 describe-instances --filters Name=tag:Project,Values=ematix-flow-test --query 'Reservations[].Instances[?State.Name!=`terminated`]'
aws s3 ls | grep ematix-flow-test || true
aws lambda list-functions --query 'Functions[?starts_with(FunctionName, `ematix-flow-test-`)]'
aws eks list-clusters --query 'clusters[?starts_with(@, `ematix-flow-test-`)]'
aws ecr describe-repositories --query 'repositories[?starts_with(repositoryName, `ematix-flow-test-`)]'
```

Each query must return empty. If anything's left, the script aborts and
prints what survives so we delete it explicitly.

## Cross-cutting safeguards

- **Tagging:** every resource carries `Project=ematix-flow-test`,
  `Owner=<your AWS account email>`, `CreatedAt=<ISO timestamp>`,
  `MaxLifetimeHours=24`. Enables a one-liner janitor:
  `aws resourcegroupstaggingapi get-resources --tag-filters Key=Project,Values=ematix-flow-test`.
- **AWS Budgets alert:** $5 monthly threshold with email at 80%. The
  campaign itself stays well under $5 so this is the "I forgot a
  resource" tripwire.
- **No persistent IAM users.** Use a single short-lived IAM role
  assumed by an `aws sso login` session or by `aws-vault` — credentials
  expire automatically.
- **One region:** everything in `us-east-2`. Avoids cross-region
  surprises; cheaper than `us-east-1` for compute.
- **No NAT gateway, no Elastic IP.** EC2 gets a public IP at launch and
  loses it at termination — keeps Phase A's networking cost at $0.
- **No VPC peering, no Transit Gateway, no PrivateLink.** Default VPC is
  fine for test workloads.

## Implementation surface

- `infra/test-validation/` (new directory):
  - `main.tf` — Terraform module that wires the phases.
  - `userdata.sh` — what the Phase A EC2 runs on boot.
  - `bench.sh` — the bench harness driven by SSM.
  - `teardown.sh` — runs Phase E verifications.
- `Makefile` targets (or `xtask` commands):
  - `make aws-up-phase-a` — provision + run Phase A end to end.
  - `make aws-down` — destroy + verify zero residue.

## What we get back

- `docs/BENCHMARKS-AWS.md` — Mac (M-series, NEON) vs c7i.2xlarge (Intel
  Sapphire Rapids, AVX-512) numbers side-by-side, across:
  - the ematix-parquet decode benches,
  - TPC-H 22 SF=1 and SF=10 ematix-flow vs DuckDB vs Polars,
  - DictGroupCountExec fire/no-fire delta with `with_dict_preservation`.
- A signed-off correctness checklist for `S3RunLog`, `LambdaExecutor`,
  and (optionally) `K8sJobExecutor` against the real AWS services.
- Reusable Terraform that can be re-run any time we want a fresh
  validation pass, with a clean teardown step.

## Decisions locked 2026-05-16

| # | Decision | Choice |
|---|---|---|
| 1 | Phase A capacity | **Spot** c7i.2xlarge (on-demand fallback). |
| 2 | Phase D scope | **Run real EKS**, not kind-on-EC2. |
| 3 | Region | `us-east-2` (cheapest). |
| 4 | Linux libc | Amazon Linux 2023 / glibc; musl deferred. |

## Cost roll-up (locked)

| Phase | Service | Estimated cost |
|---|---|---|
| A | EC2 spot c7i.2xlarge + EBS (4 hr) | ~$0.60 |
| B | S3 bucket + requests | ~$0.05 |
| C | Lambda invocations | ~$0.01 |
| D | EKS control plane + 1× t3.medium (2 hr) | ~$0.50 |
| — | Data transfer egress (results to local) | ~$0.01 |
| | **Total** | **~$1.20** |

AWS Budgets alert remains at $5 as the I-forgot-something tripwire.
