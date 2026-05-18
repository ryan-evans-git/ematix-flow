# test-validation infra

Ephemeral AWS environment for running tests + benches we can't run on
a Mac M-series. See [docs/AWS_VALIDATION_PLAN.md](../../docs/AWS_VALIDATION_PLAN.md)
for the campaign design + cost roll-up (~$1.20).

## Setup (one time)

1. Install Terraform ≥ 1.6 and the AWS CLI v2.
2. Create an IAM user (Console → IAM → Users) with programmatic access.
   For the first run, attach `AdministratorAccess`. Tighten later.
3. `cp .env.example .env`, fill in the access key + secret. **`.env` is
   gitignored — never commit it.**
4. `set -a; source .env; set +a` to export them to the shell.

## Workflow

```bash
# from this directory
terraform init                    # downloads providers, no AWS calls
terraform validate                # static-checks the module
terraform plan                    # dry-run — what would change
terraform apply                   # creates resources
# ... run the campaign ...
terraform destroy                 # tears everything down
./scripts/teardown.sh             # verifies zero residue
```

## Phases

Each phase is gated by a `phase_*_enabled` variable so you can run
subsets:

```bash
# Phase A only (EC2 bench box)
terraform apply -var phase_a_enabled=true -var phase_b_enabled=false \
                -var phase_c_enabled=false -var phase_d_enabled=false
```

| Phase | Resources | Cost |
|---|---|---|
| A | EC2 spot c7i.2xlarge, IAM role, security group | ~$0.60 |
| B | S3 bucket with 24-hour lifecycle | ~$0.05 |
| C | Lambda function + IAM role | ~$0.01 |
| D | EKS cluster, node group, ECR repo | ~$0.50 |

## SSH into the Phase A box

Default config doesn't open port 22. Use SSM Session Manager:

```bash
aws ssm start-session --target $(terraform output -raw phase_a_instance_id) --region $(terraform output -raw region)
```

If you want SSH, pass `-var phase_a_ssh_pubkey="$(cat ~/.ssh/id_ed25519.pub)"` to `terraform apply`.

## What the campaign produces

Results stream to `s3://$(terraform output -raw phase_b_bucket)/results/<timestamp>/`:

- `00-host-info.log` — CPU/memory/Rust version baseline
- `01-cargo-test-x86.log` — Linux x86 integration matrix
- `02-emat-parquet-benches.log` — AVX-2/AVX-512 decode benches
- `03-tpch-sf1.log` — SF=1 TPC-H baseline
- `04-tpch-sf10-triangulation.log` — SF=10 ematix-flow vs DuckDB vs Polars
- `05-sigma-e3b-dict-preservation.log` — dict-preservation validation
- `00-userdata.log` — boot + build log
- `99-summary.log` — campaign end marker

After the campaign, sync results locally:

```bash
aws s3 sync s3://$(terraform output -raw phase_b_bucket)/results/ ./results/
```

Then run `terraform destroy` + `./scripts/teardown.sh`.
