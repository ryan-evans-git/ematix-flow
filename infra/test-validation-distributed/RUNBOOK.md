# Distributed campaign runbook

End-to-end recipe for the AWS distributed TPC-H bench. Follows the
plan at `docs/AWS_CAMPAIGN_2026_05_PLAN.md`.

## Prerequisites

- `aws` CLI v2 + IAM creds (`infra/test-validation/.env` is a fine
  source — `source .env` before running).
- Terraform ≥ 1.6.
- The bench bucket from a previous Phase A run (or stand one up
  manually).

## One-time: upload TPC-H parquet to S3

The cluster nodes pull TPC-H parquet from S3 during bootstrap. Layout:

```
s3://$BUCKET/tpch-data/sf10/<table>/<table>.parquet
s3://$BUCKET/tpch-data/sf100/<table>/<table>.parquet
```

The directory layout is required by Trino's hive connector
(`external_location` must be a directory). PySpark and ematix both
accept the directory layout transparently.

### SF=10 — fast (a few minutes on Phase A's c7i.2xlarge)

```bash
cd infra/test-validation
source .env  # AWS creds + BUCKET
./scripts/gen-sf-data.sh 10 $BUCKET
```

### SF=100 — slow (~45 min); needs a beefier box

SF=100 lineitem is ~75 GB. Run on a c7i.4xlarge with 250 GB EBS, then
terminate.

```bash
# Bring up a one-off generator box (variant of Phase A)
cd infra/test-validation
terraform apply \
  -var phase_a_instance_type=c7i.4xlarge \
  -var phase_a_ebs_size_gb=250 \
  -var phase_b_enabled=true \
  -var phase_c_enabled=false \
  -var phase_d_enabled=false

# SSM in
aws ssm start-session --target $(terraform output -raw phase_a_instance_id)

# (inside the box)
cd /opt/ematix/ematix-flow
sudo -u ec2-user infra/test-validation/scripts/gen-sf-data.sh 100 $BENCH_BUCKET

# Back on your laptop
terraform destroy -auto-approve
```

## SF=10 distributed sweep

Three terraform applies, one per engine. Each runs the engine, then
the bench script writes results to S3.

```bash
cd infra/test-validation-distributed
terraform init

STAMP=$(date -u +%Y%m%dT%H%M%SZ)
echo "Campaign stamp: $STAMP"

# Engine 1: ematix
terraform apply -auto-approve \
  -var bench_bucket=$BUCKET \
  -var scale_factor=10 \
  -var engine=ematix
# Wait for userdata to finish (~5 min for the rust build)
aws ssm start-session --target $(terraform output -raw coordinator_instance_id)
# (inside) — wait for /var/log/ematix-userdata.log to show "done"
# (inside) — run the bench
sudo -u ec2-user bash -c '
  source /etc/profile.d/ematix-env.sh
  cd /opt/ematix/ematix-flow
  EMATIX_PEERS="http://10.0.x.x:50051,http://10.0.y.y:50051,http://10.0.z.z:50051" \
  TPCH_DATA_DIR=/opt/ematix/data/sf10 \
  TPCH_SCALE_FACTOR=10 \
  TPCH_TRIALS=5 TPCH_WARMUPS=2 \
  OUTPUT_PATH=/tmp/ematix-sf10.json \
  ./target/release/examples/tpch_distributed_campaign
  aws s3 cp /tmp/ematix-sf10.json s3://$BENCH_BUCKET/results/'"$STAMP"'/ematix-sf10.json
'
terraform destroy -auto-approve

# Engine 2: trino
terraform apply -auto-approve \
  -var bench_bucket=$BUCKET \
  -var scale_factor=10 \
  -var engine=trino
# SSM into coordinator, run register-tables.sh then bench.py
aws ssm start-session --target $(terraform output -raw coordinator_instance_id)
# (inside)
sudo -u ec2-user /opt/ematix/ematix-flow/infra/distributed-peers/trino/register-tables.sh 10
sudo -u ec2-user python3.12 /opt/ematix/ematix-flow/infra/distributed-peers/trino/bench.py \
  --sf 10 --bucket $BENCH_BUCKET --trials 5 --warmups 2 --stamp $STAMP
terraform destroy -auto-approve

# Engine 3: pyspark
terraform apply -auto-approve \
  -var bench_bucket=$BUCKET \
  -var scale_factor=10 \
  -var engine=pyspark
aws ssm start-session --target $(terraform output -raw coordinator_instance_id)
# (inside)
pip install --user pyspark==3.5.4 boto3
sudo -u ec2-user python3.12 /opt/ematix/ematix-flow/infra/distributed-peers/pyspark/bench.py \
  --sf 10 --bucket $BENCH_BUCKET --trials 5 --warmups 2 --stamp $STAMP
terraform destroy -auto-approve
```

## SF=100 distributed sweep

Same as SF=10 but with `-var scale_factor=100`. Terraform automatically
switches to c7i.4xlarge + 250 GB EBS. Wall-time per engine: ~3-6 hr
depending on engine + query.

## Aggregate + commit

```bash
# Back on your laptop after all 6 runs (3 engines × 2 SFs) are in S3
cd <repo-root>
python3 scripts/aggregate_distributed_bench.py \
  --bucket $BUCKET --stamp $STAMP

git add BENCHMARKS-DISTRIBUTED.md
git commit -m "bench(distributed): publish AWS 2026-05 campaign results"
git push
```

## Cost watchdog

Total wall-time across SF=10 + SF=100:
- 6 applies × ~5 min setup = ~30 min idle compute (4 c7i × $1.72/hr = $0.86)
- 3 engines × ~2 hr SF=10 = ~6 hr × $1.72 = $10
- 3 engines × ~5 hr SF=100 = ~15 hr × $3.44 = $52

Budget ~$70 total. Tear down between engines to keep the cumulative
small.

## Troubleshooting

- **userdata fails on a worker** → workers depend on the coordinator's
  private IP being known at apply time; if you see "coordinator_ip="
  in the rendered userdata, the worker's plan ran before the
  coordinator instance existed. Re-run `terraform apply`; the
  workers will re-bootstrap.
- **Worker can't reach coordinator on the engine's port** → SG self-
  rule failure. `aws ec2 describe-security-groups --group-ids $(terraform
  output -raw security_group_id)` should show TCP 1-65535 from self.
- **Trino "schema not found"** → `register-tables.sh` didn't run.
  It's an explicit coordinator-only step; userdata doesn't auto-run it.
- **PySpark OOMs at SF=100** → bump `SPARK_WORKER_MEMORY` in
  `infra/distributed-peers/pyspark/install.sh` (currently 28g for
  c7i.4xlarge — fine for most queries; Q21 sometimes wants more).
