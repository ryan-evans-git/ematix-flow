# Distributed campaign runbook

End-to-end recipe for the AWS distributed TPC-H bench. Follows the
plan at `docs/AWS_CAMPAIGN_2026_05_PLAN.md`; refreshed 2026-07-04 for
the current kit (campaign binary on the unified production preset,
Spark 4.1.2, Trino 482, provenance-stamped JSON outputs).

Settled protocol: **5 measured trials × 2 untimed warmups per query,
all 22 queries, per engine, per scale factor** (single-node campaigns
use 20×5; distributed stays at 5×2 per the plan). Row counts are
cross-checked across engines by the aggregator — any divergence is a
correctness flag, not a footnote.

## Prerequisites

- `aws` CLI v2 + IAM creds (`aws configure`, or `source
  infra/test-validation/.env` from a previous Phase A run).
- Terraform ≥ 1.6 (kit validated on 1.14).
- The bench bucket from a previous Phase A run (or stand one up
  manually).
- **Push main.** Every node clones ematix-flow from GitHub
  (`-var git_ref=...`, default `main`). Local-only commits are
  invisible to the cluster — verify with
  `git rev-list --count origin/main..main` (must be 0) before you
  spend a cent.

```bash
export BUCKET=<bench-bucket-name>
```

## One-time: upload TPC-H parquet to S3

The cluster nodes pull TPC-H parquet from S3 during bootstrap
(ematix nodes copy to local disk; Spark/Trino read s3a/S3 directly).
Layout:

```
s3://$BUCKET/tpch-data/sf10/<table>/<table>.parquet
s3://$BUCKET/tpch-data/sf100/<table>/<table>.parquet
```

The directory layout is required by Trino's hive connector
(`external_location` must be a directory). PySpark and ematix both
accept the directory layout transparently (ematix userdata flattens on
download).

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
cd infra/test-validation
terraform apply \
  -var phase_a_instance_type=c7i.4xlarge \
  -var phase_a_ebs_size_gb=250 \
  -var phase_b_enabled=true \
  -var phase_c_enabled=false \
  -var phase_d_enabled=false

aws ssm start-session --target $(terraform output -raw phase_a_instance_id)
# (inside the box)
cd /opt/ematix/ematix-flow
sudo -u ec2-user infra/test-validation/scripts/gen-sf-data.sh 100 $BENCH_BUCKET

# Back on your laptop
terraform destroy -auto-approve
```

## Per-engine sweep — the one-command sequence

Three applies per scale factor, one per engine. Tear down between
engines: the cumulative idle cost is what busts budgets, not the runs.

```bash
cd infra/test-validation-distributed
terraform init

STAMP=$(date -u +%Y%m%dT%H%M%SZ)
echo "Campaign stamp: $STAMP"   # ONE stamp for all 6 runs
```

### Engine 1: ematix (repeat with `-var scale_factor=100`)

```bash
terraform apply -auto-approve \
  -var bench_bucket=$BUCKET \
  -var scale_factor=10 \
  -var engine=ematix

# Wait for userdata (rust build + data pull; ~10 min SF10, ~25 min SF100):
# on each node /var/log/ematix-userdata.log ends with "done".
aws ssm start-session --target $(terraform output -raw coordinator_instance_id)

# (inside the coordinator)
sudo -u ec2-user bash -c '
  source /etc/profile.d/ematix-env.sh
  cd /opt/ematix/ematix-flow
  PEERS=$(echo "'"$(terraform output -json worker_private_ips | jq -r 'map("http://\(.):50051") | join(",")')"'")
  EMATIX_PEERS="$PEERS" \
  TPCH_DATA_DIR=/opt/ematix/data/sf10 \
  TPCH_SCALE_FACTOR=10 \
  TPCH_TRIALS=5 TPCH_WARMUPS=2 \
  OUTPUT_PATH=/tmp/ematix-sf10.json \
  ./target/release/examples/tpch_distributed_campaign
  aws s3 cp /tmp/ematix-sf10.json s3://$BENCH_BUCKET/results/'"$STAMP"'/ematix-sf10.json
'
terraform destroy -auto-approve
```

Notes:
- `worker_private_ips` is a terraform output; if you're inside the SSM
  session without terraform state, get the IPs from
  `aws ec2 describe-instances --filters Name=tag:Role,Values=worker
  Name=instance-state-name,Values=running
  --query 'Reservations[].Instances[].PrivateIpAddress' --output text`.
- Do NOT set any `EMAT_*` env: levers are production auto-gated. The
  binary and each flow-worker run solo on their instances, so the
  partition registry senses live=1 → full cores per node.
- The output JSON carries a `provenance` block (instance type, AZ, git
  SHA, `EMAT_*`/`TPCH_*` env, dep versions). Skim it before uploading:
  `jq .provenance /tmp/ematix-sf10.json` — if `git_dirty` is true or
  the SHA isn't the one you meant to measure, stop.

### Engine 2: trino

```bash
terraform apply -auto-approve \
  -var bench_bucket=$BUCKET \
  -var scale_factor=10 \
  -var engine=trino
aws ssm start-session --target $(terraform output -raw coordinator_instance_id)
# (inside)
sudo -u ec2-user /opt/ematix/ematix-flow/infra/distributed-peers/trino/register-tables.sh 10
python3.12 -m pip install --user trino boto3
sudo -u ec2-user python3.12 /opt/ematix/ematix-flow/infra/distributed-peers/trino/bench.py \
  --sf 10 --bucket $BENCH_BUCKET --trials 5 --warmups 2 --stamp $STAMP
terraform destroy -auto-approve
```

### Engine 3: pyspark

```bash
terraform apply -auto-approve \
  -var bench_bucket=$BUCKET \
  -var scale_factor=10 \
  -var engine=pyspark
aws ssm start-session --target $(terraform output -raw coordinator_instance_id)
# (inside) — pyspark client version MUST match the cluster (4.1.2)
python3.12 -m pip install --user pyspark==4.1.2 boto3
sudo -u ec2-user python3.12 /opt/ematix/ematix-flow/infra/distributed-peers/pyspark/bench.py \
  --sf 10 --bucket $BENCH_BUCKET --trials 5 --warmups 2 --stamp $STAMP
terraform destroy -auto-approve
```

### SF=100

Same three blocks with `-var scale_factor=100` (terraform switches to
c7i.4xlarge + 250 GB EBS automatically) and `--sf 100` /
`TPCH_DATA_DIR=/opt/ematix/data/sf100` in the bench invocations.

## TEARDOWN VERIFICATION (run after EVERY destroy — non-negotiable)

`terraform destroy` exiting 0 is a claim, not a fact. Verify nothing
tagged to the project is still alive or billable:

```bash
REGION=us-east-2
# 1. No instances left (running OR stopped — stopped still bills EBS):
aws ec2 describe-instances --region $REGION \
  --filters "Name=tag:Project,Values=ematix-flow-distributed" \
            "Name=instance-state-name,Values=pending,running,stopping,stopped" \
  --query 'Reservations[].Instances[].[InstanceId,State.Name]' --output text
# 2. No orphaned volumes:
aws ec2 describe-volumes --region $REGION \
  --filters "Name=tag:Project,Values=ematix-flow-distributed" \
  --query 'Volumes[].[VolumeId,State]' --output text
# 3. No leftover security groups / IAM roles / instance profiles:
aws ec2 describe-security-groups --region $REGION \
  --filters "Name=tag:Project,Values=ematix-flow-distributed" \
  --query 'SecurityGroups[].GroupId' --output text
aws iam list-roles --query "Roles[?starts_with(RoleName, 'ematix-flow-distributed')].RoleName" --output text
# 4. Region-wide catch-all (anything terraform-managed in the project):
aws resourcegroupstaggingapi get-resources --region $REGION \
  --tag-filters Key=Project,Values=ematix-flow-distributed \
  --query 'ResourceTagMappingList[].ResourceARN' --output text
```

All four must come back EMPTY. The Glue database (`ematix_tpch`,
engine=trino runs only) is destroyed with the stack; the bench bucket
lives outside this module and is expected to persist.

## Aggregate + commit

```bash
# Back on your laptop after all 6 runs (3 engines × 2 SFs) are in S3
cd <repo-root>
python3 scripts/aggregate_distributed_bench.py \
  --bucket $BUCKET --stamp $STAMP

git add BENCHMARKS-DISTRIBUTED.md
git commit -m "bench(distributed): publish AWS campaign results ($STAMP)"
git push
```

The generated doc includes a **Run provenance** table (engine version,
instance type, AZ, git SHA per run) sourced from each JSON's
`provenance` block. If any engine's SHA differs from the others', the
comparison is void — rerun the odd one out.

## Cost estimate (budget ~$70)

On-demand c7i pricing, us-east-2 (verify current rates at
https://aws.amazon.com/ec2/pricing/on-demand/ before apply —
needs-web-verify at execution time):

| Phase | Cluster | $/hr (4 nodes) | Est. wall-hours | Est. cost |
|---|---|---:|---:|---:|
| SF=10 × 3 engines | c7i.2xlarge ×4 (~$0.357/node) | ~$1.43 | ~4.5 (3×~1h run + setup) | ~$6.50 |
| SF=100 × 3 engines | c7i.4xlarge ×4 (~$0.714/node) | ~$2.86 | ~12 (3×~3.5h run + setup) | ~$34 |
| EBS (gp3, 100/250 GB ×4) | — | pennies/hr | — | ~$2 |
| **Total (on-demand)** | | | ~16.5 | **~$43** |

Headroom to the $70 budget ≈ $27 — that absorbs one full SF=100 engine
re-run. Spot (`-var use_spot=true`, bid cap `-var max_spot_price=0.60`)
cuts compute ~60-70% (→ ~$15 total) but c7i.4xlarge spot evictions
mid-Q21 waste more than they save; recommendation: **spot for SF=10,
on-demand for SF=100**. Tear down between engines — the estimate
assumes no idle clusters.

## Troubleshooting

- **userdata fails on a worker** → workers depend on the coordinator's
  private IP being known at apply time; if you see "coordinator_ip="
  in the rendered userdata, the worker's plan ran before the
  coordinator instance existed. Re-run `terraform apply`; the
  workers will re-bootstrap.
- **flow-worker service crash-loops** → check
  `journalctl -u flow-worker`. (The May kit's `--bind 0.0.0.0:50051`
  single-flag form is fixed — it's `--bind 0.0.0.0 --port 50051` now.)
- **campaign binary: "missing parquet"** → the S3→local pull in
  `userdata/ematix.sh.tftpl` didn't finish; check
  `/var/log/ematix-userdata.log` and re-run the copy loop from it.
- **Worker can't reach coordinator on the engine's port** → SG self-
  rule failure. `aws ec2 describe-security-groups --group-ids $(terraform
  output -raw security_group_id)` should show TCP 1-65535 from self.
- **Trino "schema not found"** → `register-tables.sh` didn't run.
  It's an explicit coordinator-only step; userdata doesn't auto-run it.
- **Trino won't start after the 482 bump** → check Java: 482 requires
  Java 25 (`java-25-amazon-corretto-headless`); Java 21/24 are refused
  at startup.
- **PySpark OOMs at SF=100** → bump `SPARK_WORKER_MEMORY` in
  `infra/distributed-peers/pyspark/install.sh` (28g on c7i.4xlarge —
  fine for most queries; Q21 sometimes wants more).
- **PySpark client/cluster version mismatch** → the pip-installed
  `pyspark` on the coordinator must equal the cluster version
  (4.1.2); Spark 4 refuses mixed-version sessions.
