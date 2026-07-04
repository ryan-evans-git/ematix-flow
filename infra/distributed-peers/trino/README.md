# Trino 482 peer deployment for the AWS-campaign

This directory deploys **Trino 482** (Apache 2.0, latest stable as of
2026-07, released 2026-06-25) on the same 4-node EC2 cluster (1× coordinator + 3× workers)
that runs ematix-flow distributed and PySpark, as one of the three peers
in the distributed TPC-H comparison described in
`docs/AWS_CAMPAIGN_2026_05_PLAN.md`.

- **Metastore**: AWS Glue Data Catalog (no Hive Metastore Service to
  operate).
- **Storage**: TPC-H parquet at `s3://$BENCH_BUCKET/tpch-data/sf{10,100}/`.
- **Discovery**: workers register with the coordinator's built-in
  discovery service over port 8080.
- **Idempotency**: every script is safe to re-run.

## Layout

| file                | purpose                                                  |
|---------------------|----------------------------------------------------------|
| `install.sh`        | Per-node installer (Java 21 + Trino + systemd unit).     |
| `register-tables.sh`| Coordinator-only — registers Hive schemas + 8 tables.    |
| `bench.py`          | Coordinator-only — 22 queries × N trials, uploads JSON.  |

## Required env

All scripts read these from the environment:

| var            | required for                       | example                       |
|----------------|------------------------------------|-------------------------------|
| `BENCH_BUCKET` | install.sh, register-tables.sh, bench.py | `ematix-bench-2026-05`  |
| `AWS_REGION`   | install.sh (auto-resolved from IMDSv2 if absent), register-tables.sh, bench.py | `us-east-1` |

The IAM role attached to each EC2 node must grant:

- `s3:GetObject`, `s3:ListBucket` on `s3://$BENCH_BUCKET/tpch-data/sf*/*`
- `s3:PutObject`, `s3:GetObject` on `s3://$BENCH_BUCKET/results/*` (bench upload)
- `s3:PutObject`, `s3:DeleteObject`, `s3:GetObject` on
  `s3://$BENCH_BUCKET/tpch-data/sf*/*` (register-tables.sh reorganises
  each table into a `<table>/` subdir — see below)
- `glue:Get*`, `glue:CreateTable`, `glue:CreateDatabase` on the
  `ematix_tpch`-prefixed Glue resources

## Run order

```sh
# 1. on every node (coordinator + 3 workers), via cloud-init or manually:
sudo BENCH_BUCKET="$BENCH_BUCKET" \
  ./install.sh --role coordinator --coordinator-host "$COORDINATOR_IP"
# (workers use --role worker but the same --coordinator-host)

# 2. once the cluster is healthy, from the coordinator only:
BENCH_BUCKET="$BENCH_BUCKET" ./register-tables.sh --sf 10
BENCH_BUCKET="$BENCH_BUCKET" ./register-tables.sh --sf 100   # optional / SF=100 phase

# 3. benchmark, from the coordinator only:
sudo dnf install -y python3-pip
pip install --user trino boto3
python3 bench.py --sf 10  --bucket "$BENCH_BUCKET" --trials 5 --warmups 2
python3 bench.py --sf 100 --bucket "$BENCH_BUCKET" --trials 5 --warmups 2
```

## Data layout caveat

The campaign pipeline uploads TPC-H tables as single files:

    s3://$BENCH_BUCKET/tpch-data/sf10/lineitem.parquet

Trino's Hive connector wants `external_location` to be a *directory*.
`register-tables.sh` reorganises (via `aws s3 mv`) into:

    s3://$BENCH_BUCKET/tpch-data/sf10/lineitem/lineitem.parquet

Re-running the script after this is a no-op (it skips the move if the
`<table>/` prefix is already populated). The single-file source is
removed once moved, so ematix and PySpark scripts that target the
directory layout will still work; scripts targeting the single-file
layout need to be updated. The move only happens on the first run per
scale factor.

## TPC-H dialect notes (Trino vs DataFusion)

The repo's canonical `examples/tpch/queries/q*.sql` are written for
DataFusion's parser. Trino accepts all 22 verbatim (audited on 440, re-checked for 482) — we audited
each one before writing this. Specific points we checked:

| query | DataFusion form                                          | Trino                  |
|-------|----------------------------------------------------------|------------------------|
| Q01   | `date '1998-12-01' - interval '90' day`                  | works as-is            |
| Q07   | `extract(year from l_shipdate)`                          | works as-is (ANSI)     |
| Q08, Q09, Q17 | same `extract(year from ...)` pattern            | works as-is            |
| Q13   | `LEFT OUTER JOIN ... ON ... AND ... NOT LIKE ...`        | works as-is            |
| Q14   | `case when p_type like 'PROMO%' then ... else 0 end`     | works as-is            |
| Q15   | `WITH revenue_s (supplier_no, total_revenue) AS (...)` — CTE with column-list alias | works as-is (Trino supports the column-list CTE form) |
| Q21   | nested `EXISTS` + `NOT EXISTS`                           | works as-is            |
| Q22   | `substring(c_phone from 1 for 2)`                        | works as-is (ANSI)     |

If a future query needs adaptation, add an entry to the `REWRITES` dict
at the top of `bench.py` rather than editing the canonical SQL file. The
adaptation is documented in-place in `bench.py`.

## Output schema

`bench.py` writes JSON matching the shape in the plan doc:

```json
{
  "engine": "trino",
  "version": "482",
  "scale_factor": 10,
  "cluster_size": 4,
  "queries": {
    "Q01": {
      "trials_ms": [27.4, 28.1, 27.7, 27.6, 27.9],
      "median_ms": 27.7,
      "p95_ms": 28.06,
      "first_trial_ms": 27.4,
      "median_trials_3_5_ms": 27.7,
      "rows_returned": 4
    }
  }
}
```

Uploaded to `s3://$BENCH_BUCKET/results/<UTC-stamp>/trino-sf{N}.json` so
`scripts/aggregate_distributed_bench.py` can pick it up alongside the
ematix and PySpark JSONs.

## Troubleshooting

### Worker doesn't register

Symptom: `register-tables.sh` succeeds but queries spread across only
the coordinator's node, or `SELECT * FROM system.runtime.nodes` shows
fewer than 4 entries.

Likely causes:
1. **Security group** doesn't allow port 8080 inter-node. Confirm
   workers can `curl -s http://$COORDINATOR_IP:8080/v1/info` and the
   coordinator can `curl` each worker on 8080.
2. **`discovery.uri` mismatch** — workers' `/opt/trino/etc/config.properties`
   must point at the coordinator's *private* IP (not localhost, not the
   public DNS). Re-run `install.sh --role worker --coordinator-host <ip>`
   with the correct IP; the script rewrites config + restarts.
3. **`node.environment` mismatch** — every node in a Trino cluster must
   share the same `node.environment` value. `install.sh` hard-codes
   `ematix_campaign`, so this only breaks if you've manually edited
   `/opt/trino/etc/node.properties`.

Check the coordinator log:

```sh
sudo tail -f /var/lib/trino/var/log/server.log
```

Look for `INFO Announcement-X Discovery service registered`. If you see
`Failed to register with discovery server` on a worker, it's #1 or #2.

### Glue auth fails

Symptom: `CREATE TABLE` errors with
`com.amazonaws.services.glue.model.AccessDeniedException` or
`Unable to load credentials`.

Likely causes:
1. **Missing IAM role** on the node. From the coordinator:
   `curl -s http://169.254.169.254/latest/meta-data/iam/security-credentials/`
   should return the role name; if empty, the instance profile isn't
   attached.
2. **Role lacks Glue permissions**. The role needs `glue:GetDatabase`,
   `glue:CreateDatabase`, `glue:GetTable`, `glue:CreateTable`,
   `glue:UpdateTable` on the `ematix_tpch*` resources.
3. **Wrong region** — `hive.metastore.glue.region` in
   `/opt/trino/etc/catalog/hive.properties` must match the region the
   Glue database lives in. The installer auto-resolves from IMDSv2, so
   this only breaks if the node is in a different region than the Glue
   database (unusual).

### Query parse errors

Symptom: `bench.py` reports `io.trino.spi.TrinoException: line N:M ...`.

The canonical TPC-H queries in `examples/tpch/queries/` are
DataFusion-dialect. All 22 are verified to work on Trino as-is (440-audited, 482 re-checked; see
the table above). If you hit a parse error on a query that's listed as
"works as-is", the most likely cause is a stale checkout — `git pull`
and confirm the file content matches.

If a *new* dialect difference appears (e.g. after a Trino upgrade), add
an entry to the `REWRITES` dict at the top of `bench.py`:

```python
REWRITES = {
    "Q15": lambda sql: sql.replace("interval '3' month", "interval '3' MONTH"),
}
```

### Query OOM on SF=100

Symptom: `Query exceeded per-node user memory limit of 10GB`.

The installer sets `query.max-memory-per-node=10GB` and `-Xmx13G`,
sized for c7i.2xlarge (16 GB RAM). On c7i.4xlarge (32 GB) for SF=100,
bump both:

```sh
sudo sed -i 's/^-Xmx.*/-Xmx26G/' /opt/trino/etc/jvm.config
sudo sed -i 's/^query.max-memory-per-node=.*/query.max-memory-per-node=22GB/' /opt/trino/etc/config.properties
sudo sed -i 's/^query.max-memory=.*/query.max-memory=80GB/' /opt/trino/etc/config.properties
sudo systemctl restart trino
```

(The campaign's terraform should pass a `--instance-class` flag that
makes the installer pick the right values automatically; that wiring is
in the terraform module, not here.)

### `bench.py` reports "0 rows" for every query

Means the schema was registered but the parquet files aren't visible.
Re-run `register-tables.sh --sf <N>` and confirm the move step ran. Then
from `trino` CLI:

```sh
trino --catalog hive --schema tpch_sf10 --execute "SELECT count(*) FROM lineitem;"
```

SF=10 lineitem is ~60M rows. If it's 0, the `external_location` doesn't
match the actual S3 layout — check `aws s3 ls s3://$BENCH_BUCKET/tpch-data/sf10/lineitem/`.
