#!/usr/bin/env bash
#
# Σ.C PR 2 prep: cluster-mode TPC-H benchmark driver.
#
# Runs Q1 / Q3 / Q6 / Q19 against:
#   - 1-node DataFusion (control)
#   - 3-pod ematix-flow-distributed cluster (this is the new path)
#   - 3-executor PySpark cluster (industry comparison)
#
# All three configurations execute on the SAME hardware against the
# SAME SF=10 (or higher) Parquet files. Output: a markdown table
# pasteable into docs/BENCHMARKS.md.
#
# What this script DOES:
#   - assumes a 4-node cluster is already provisioned (1 coordinator
#     + 3 workers; see infra/README.md for the provisioning recipe)
#   - drives benches against existing services
#   - emits the comparison table
#
# What this script does NOT do:
#   - provision EC2 nodes (manual + cloud-init; see infra/README.md)
#   - bring up flow-worker processes (cloud-init does this; or run
#     the systemd unit manually)
#   - bring up Spark master/workers (run the standard `start-master.sh`
#     / `start-worker.sh` from a Spark distribution)
#
# Usage:
#   tpch-bench-multi.sh --sf 10 \
#       --tpch-data-dir /shared/tpch/sf10 \
#       --distributed-peers http://w1:50051,http://w2:50051,http://w3:50051 \
#       --pyspark-master spark://master:7077 \
#       --output bench-results.md
#
# Defaults if you elide the flags:
#   --sf 1
#   --tpch-data-dir examples/tpch/data/sf1
#   --distributed-peers http://localhost:50051,http://localhost:50052,http://localhost:50053
#   --pyspark-master local[*]      (no real cluster — useful for dry runs)
#   --output  /dev/stdout

set -euo pipefail

# Defaults.
SF=1
TPCH_DATA_DIR="$(cd "$(dirname "$0")/.." && pwd)/examples/tpch/data/sf1"
DISTRIBUTED_PEERS="http://localhost:50051,http://localhost:50052,http://localhost:50053"
PYSPARK_MASTER="local[*]"
OUTPUT="/dev/stdout"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --sf) SF="$2"; shift 2 ;;
        --tpch-data-dir) TPCH_DATA_DIR="$2"; shift 2 ;;
        --distributed-peers) DISTRIBUTED_PEERS="$2"; shift 2 ;;
        --pyspark-master) PYSPARK_MASTER="$2"; shift 2 ;;
        --output) OUTPUT="$2"; shift 2 ;;
        -h|--help)
            sed -n '2,40p' "$0"
            exit 0
            ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RESULTS_DIR="$(mktemp -d -t tpch-bench-XXXXXX)"
trap 'rm -rf "$RESULTS_DIR"' EXIT

echo "==> SF=$SF"
echo "==> TPCH data: $TPCH_DATA_DIR"
echo "==> ematix-flow-distributed peers: $DISTRIBUTED_PEERS"
echo "==> PySpark master: $PYSPARK_MASTER"
echo "==> Results dir: $RESULTS_DIR"
echo

# 1. Single-node DataFusion control. Re-runs the existing criterion
#    bench from Σ.A1 PR 2 against the configured SF.
echo "==> [1/3] single-node DataFusion (criterion bench)"
TPCH_DATA_DIR="$TPCH_DATA_DIR" \
    cargo bench --release -p ematix-flow-core --bench tpch -- \
        --save-baseline "sigma_c_pr2_sf${SF}_datafusion" \
    > "$RESULTS_DIR/datafusion.txt" 2>&1 || {
    echo "DataFusion bench failed; output in $RESULTS_DIR/datafusion.txt" >&2
}

# 2. ematix-flow-distributed cluster. Uses the criterion bench from
#    Σ.C PR 1 against the configured peer URLs.
echo "==> [2/3] ematix-flow-distributed cluster (3 peers)"
TPCH_DATA_DIR="$TPCH_DATA_DIR" \
DISTRIBUTED_PEERS="$DISTRIBUTED_PEERS" \
    cargo bench --release -p ematix-flow-distributed --bench tpch_distributed -- \
        --save-baseline "sigma_c_pr2_sf${SF}_distributed" \
    > "$RESULTS_DIR/distributed.txt" 2>&1 || {
    echo "Distributed bench failed; output in $RESULTS_DIR/distributed.txt" >&2
}

# 3. PySpark cluster. Reuses the script from Σ.A1 PR 4. Pass the
#    cluster master via SPARK_MASTER env var; the script reads it
#    inside its `SparkSession.builder.master(...)` call.
echo "==> [3/3] PySpark cluster"
SPARK_MASTER="$PYSPARK_MASTER" \
TPCH_DATA_DIR="$TPCH_DATA_DIR" \
    python3 "$REPO_ROOT/scripts/bench-tpch-pyspark.py" \
        --data-dir "$TPCH_DATA_DIR" --trials 5 \
    > "$RESULTS_DIR/pyspark.txt" 2>&1 || {
    echo "PySpark bench failed; output in $RESULTS_DIR/pyspark.txt" >&2
}

# 4. Summarize. The summarizer parses each tool's output + emits a
#    comparison table to $OUTPUT. Run as a Python script so the
#    parsing logic stays maintainable.
echo "==> summarizing"
python3 "$REPO_ROOT/scripts/tpch-bench-multi-summarize.py" \
    --sf "$SF" \
    --datafusion-output "$RESULTS_DIR/datafusion.txt" \
    --distributed-output "$RESULTS_DIR/distributed.txt" \
    --pyspark-output "$RESULTS_DIR/pyspark.txt" \
    --output "$OUTPUT"

echo "==> done"
