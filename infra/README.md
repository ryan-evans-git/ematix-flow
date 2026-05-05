# `infra/` — Σ.C PR 2 cluster provisioning

Recipes + bootstrap scripts for the multi-host TPC-H benchmark
(ematix-flow-distributed cluster vs PySpark on identical hardware).

Plan: [`docs/PHASE_SIGMA_PLAN.md`](../docs/PHASE_SIGMA_PLAN.md) Σ.C.

## What's in here

| File | Purpose |
|---|---|
| `cloud-init-worker.sh` | EC2 user-data: installs Rust, builds `flow-worker`, runs it as a systemd unit |
| `README.md` | This file — provisioning recipe + run instructions |

## Cluster shape (target)

Per the spike, Σ.C PR 2 runs on **EC2 m6i.4xlarge × 4** (1 coordinator
+ 3 workers) in a single VPC subnet, with all-internal traffic. Same
hardware shape for the PySpark comparison so the gap between the two
engines isn't a hardware artifact.

Why 4 nodes:
- 3 workers gives the distributed planner real fan-out to measure
  against single-node DataFusion;
- 1 coordinator separates client-side overhead from worker-side
  execution time (matters for tiny SF=1 queries);
- m6i.4xlarge × 4 is the same shape the spike's plan recommended +
  has 64 vCPU / 256 GiB total — enough headroom for SF=10 and SF=100
  TPC-H runs.

Smaller / cheaper variants work too; see the "Smaller cluster" note
at the bottom.

## Provisioning recipe (manual + cloud-init)

### 1. Create a security group

Allow:
- TCP 22 from your IP (SSH).
- TCP 50051 from the security group itself (worker → worker
  Arrow Flight).
- TCP 7077 + 7337–7339 from the SG itself (Spark master ↔ workers).
- TCP 8080 from your IP if you want the Spark UI.

### 2. Launch 4 m6i.4xlarge instances

- AMI: Ubuntu 24.04 LTS (`ami-XXXXXXXX` — pick the latest in your
  region).
- Instance type: m6i.4xlarge.
- Storage: 100 GiB gp3 per node.
- Subnet: same one for all 4.
- Security group: the one from step 1.
- IAM: `AmazonS3ReadOnlyAccess` if you're staging TPC-H Parquet on S3.

For the **3 worker nodes**, paste the contents of
`infra/cloud-init-worker.sh` into the user-data field. cloud-init
runs it as root on first boot — installs Rust + builds `flow-worker`
+ starts it as a systemd unit on port 50051. Allow ~10 min for the
first build to finish.

For the **coordinator node**, leave user-data blank; you'll SSH in
and drive the bench from there.

### 3. Stage TPC-H data

Two options:

**a) NFS / S3 share.** Write the SF=10 (or higher) Parquet files to
a share that all 4 nodes can read. Cheapest is a single
`m6i.4xlarge`'s local SSD with NFS exports; for SF=100+ use S3 +
mount via `goofys` / `s3fs` on each node.

**b) Per-node copy.** SCP the data to `/data/tpch/sfN` on each node.
Wasteful at SF≥10 but acceptable for one-off benches.

Generate the data once on the coordinator with:

```sh
cargo run --release -p ematix-flow-core --example tpch_generate -- \
    --sf 10 --out /data/tpch/sf10
```

Each generation is single-machine, so there's no parallelism win
from running it on the cluster — just do it once and copy.

### 4. Bring up Spark (for the comparison)

On the coordinator:

```sh
# Install Spark 3.5.x (matches scripts/bench-tpch-pyspark.py's pin).
wget https://archive.apache.org/dist/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz
tar xf spark-3.5.3-bin-hadoop3.tgz && mv spark-3.5.3-bin-hadoop3 /opt/spark

# Start the master (binds 7077 by default).
/opt/spark/sbin/start-master.sh

# On each of the 3 worker nodes, start a Spark worker pointing at
# the master:
ssh worker-1 "/opt/spark/sbin/start-worker.sh spark://<coordinator-ip>:7077"
ssh worker-2 ...
ssh worker-3 ...
```

The 3 worker nodes now run BOTH `flow-worker` (port 50051, started
by cloud-init) AND a Spark worker (started by the SSH command).
Different processes, different ports; coexist fine.

### 5. Run the bench

From the coordinator:

```sh
cd /opt/ematix-flow
bash scripts/tpch-bench-multi.sh \
    --sf 10 \
    --tpch-data-dir /data/tpch/sf10 \
    --distributed-peers "http://worker-1:50051,http://worker-2:50051,http://worker-3:50051" \
    --pyspark-master "spark://<coordinator-ip>:7077" \
    --output /tmp/bench-results.md
```

Wall-clock: ~30 min for SF=10 (criterion + 5 trials per query for
both engines). SF=100 is ~2-3 hours.

### 6. Capture results + tear down

Copy `/tmp/bench-results.md` back to your laptop, paste the
markdown into `docs/BENCHMARKS.md` (under the Σ.C PR 2 section), and
terminate the EC2 instances.

```sh
aws ec2 terminate-instances --instance-ids i-aaa i-bbb i-ccc i-ddd
```

## Smaller cluster

For a cheaper / smaller dry-run (m6i.large × 4, SF=1):

- Acceptable for verifying the scripts work end-to-end.
- Cost: ~$0.40/hr × 4 × 1 hr ≈ $2 for one full bench run.
- Numbers are NOT representative of production; m6i.large only has
  2 vCPU + 8 GiB so single-node DataFusion will look slow + the
  3-worker overhead will be dominated by tiny per-instance work.

## Future: Terraform

The provisioning recipe above is intentionally manual for the first
PR. A clean Terraform module (`infra/terraform/`) for the full
4-node + SG + S3 setup is a follow-up — once the bench numbers
prove out, automating the spin-up makes sense.

## Known limitations of this PR (Σ.C PR 2 prep)

- **Manual provisioning.** No Terraform; user runs `aws ec2 launch`
  + the SSH bring-up commands themselves.
- **No DNS / discovery.** Peer URLs are hardcoded as
  `http://worker-N:50051` in the bench command. Production
  deployments would use service discovery (k8s pods, Consul, etc.).
- **TLS off.** The Σ.B PR 3 caveat applies: the worker tonic
  endpoint is plaintext HTTP/2. Run on a private subnet + restrict
  via SG.
- **No automatic teardown.** If the bench script crashes mid-run,
  EC2 instances stay up and bill. Set an instance lifecycle policy
  or a `cron @reboot shutdown -h +120` if you want a hard timer.
