# test-validation-distributed infra

4-node TPC-H distributed benchmark cluster (1 coordinator + 3 workers)
for the AWS campaign defined in
[`docs/AWS_CAMPAIGN_2026_05_PLAN.md`](../../docs/AWS_CAMPAIGN_2026_05_PLAN.md).

Each apply brings up a cluster sized for ONE scale factor with ONE
engine pre-installed. To bench three engines on SF=10 you do three
applies (with `terraform destroy` between, OR three separate
workspace dirs).

## Prerequisites

1. Single-node Phase A campaign has run, and `s3://$BUCKET/tpch-data/sf{10,100}/` is populated. (If only SF=10 exists, use `-var scale_factor=10`.)
2. Terraform ≥ 1.6, AWS CLI v2, IAM creds.

## Workflow

```bash
cd infra/test-validation-distributed

# One-time
terraform init

# Bring up an ematix cluster at SF=10
terraform apply -var bench_bucket=<results-bucket>

# Bring up a trino cluster at SF=10
terraform destroy -auto-approve
terraform apply -var bench_bucket=<results-bucket> -var engine=trino

# Bring up a pyspark cluster at SF=10
terraform destroy -auto-approve
terraform apply -var bench_bucket=<results-bucket> -var engine=pyspark

# Step up to SF=100 (c7i.4xlarge automatically)
terraform destroy -auto-approve
terraform apply -var bench_bucket=<results-bucket> -var engine=ematix -var scale_factor=100

# Tear-down when done
terraform destroy -auto-approve
```

## Variables

See `variables.tf`. Key ones:

- `bench_bucket` (required) — S3 bucket with TPC-H data + receives results
- `engine` = `ematix | trino | pyspark | none` — which engine to install via userdata
- `scale_factor` = `10 | 100` — selects c7i.2xlarge or c7i.4xlarge sizing
- `worker_count` = `3` — number of worker nodes (excluding coordinator)
- `use_spot` = `false` — spot can be flaky on c7i.4xlarge; default off

## Architecture

- **Single AZ** — all 4 nodes in the same default-VPC subnet. Distributed
  TPC-H is latency-sensitive; never split across AZs.
- **SSM Session Manager** for operator access — no SSH port open by
  default. Add a pubkey via `-var ssh_pubkey=...` if you need direct SSH.
- **Self-referential security group** — intra-cluster ports all open to
  members of the same SG; external nothing.
- **Shared IAM role** with S3 read on the bench bucket + Glue read/write
  (Trino only uses Glue, but the grant is cheap to share).
- **Engine bootstrap via userdata** — `userdata/<engine>.sh.tftpl` is
  templated per role + coordinator IP. Workers depend on the coordinator
  instance via Terraform implicit ordering so the worker userdata gets
  the right private IP.

## Cost (us-east-2 on-demand)

| Component | Per hour |
|---|---|
| 4× c7i.2xlarge (SF=10) | $1.72 |
| 4× c7i.4xlarge (SF=100) | $3.44 |
| 4× 100 GB gp3 EBS | $0.05 |
| 4× 250 GB gp3 EBS (SF=100) | $0.13 |
| Inter-AZ data transfer | $0 (single AZ) |

Plan budget: ~$30 for the full SF=10 + SF=100 sweep across 3 engines.

## Troubleshooting

- **userdata didn't run** — `aws ssm start-session --target $(terraform
  output -raw coordinator_instance_id)`, then `sudo cat /var/log/cloud-init-output.log`.
- **engine service down** — `systemctl status flow-worker` (ematix),
  `systemctl status trino` (trino), `systemctl status spark-{master,worker}`
  (pyspark). All log to journald.
- **Worker can't reach coordinator** — confirm SG self-rule (`aws ec2
  describe-security-groups --group-ids <sg>`); cluster-internal traffic
  on TCP 1-65535 should be in `IpPermissions`.
