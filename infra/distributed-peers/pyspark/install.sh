#!/usr/bin/env bash
#
# Phase B4: PySpark standalone-cluster bootstrap.
#
# Runs as root on every node of the 4-node TPC-H benchmark cluster
# (1 master + 3 workers, all Amazon Linux 2023). Idempotent — re-running
# only re-applies config and bounces systemd.
#
# Usage:
#   sudo bash install.sh --role master --master-host 10.0.1.10
#   sudo bash install.sh --role worker --master-host 10.0.1.10
#
# Effect:
#   - Installs OpenJDK 21 (Corretto), Python 3.12, Spark 4.1.2 to /opt/spark
#   - Drops s3a:// JARs (hadoop-aws 3.4.2 + AWS SDK v2 bundle) into /opt/spark/jars
#   - Writes /opt/spark/conf/spark-env.sh + spark-defaults.conf
#   - Installs systemd units (spark-master on master, spark-worker on workers)
#   - Enables + starts the appropriate service(s)

set -euo pipefail

# -----------------------------------------------------------------------------
# Arg parsing
# -----------------------------------------------------------------------------
ROLE=""
MASTER_HOST=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --role)        ROLE="$2"; shift 2 ;;
        --master-host) MASTER_HOST="$2"; shift 2 ;;
        -h|--help)
            grep -E '^# ' "$0" | sed 's/^# \{0,1\}//'
            exit 0 ;;
        *)
            echo "unknown arg: $1" >&2
            exit 2 ;;
    esac
done

if [[ -z "$ROLE" || -z "$MASTER_HOST" ]]; then
    echo "usage: $0 --role master|worker --master-host <ip-or-dns>" >&2
    exit 2
fi
if [[ "$ROLE" != "master" && "$ROLE" != "worker" ]]; then
    echo "--role must be 'master' or 'worker' (got: $ROLE)" >&2
    exit 2
fi

# -----------------------------------------------------------------------------
# Versions — pinned. Changing these requires re-validating the JAR matrix.
#
# 2026-07-04 refresh (owner decision: competitors at latest stable):
#   Spark 3.5.4 → 4.1.2 (latest stable 4.x line; released 2026-05-16,
#   verified at https://archive.apache.org/dist/spark/spark-4.1.2/).
#   Spark 4.1.x bundles Hadoop 3.4.2, so the s3a companion JARs move to
#   hadoop-aws 3.4.2 + the AWS SDK **v2** bundle it was built against
#   (software.amazon.awssdk:bundle:2.29.52, per hadoop-project-3.4.2.pom)
#   — the v1 aws-java-sdk-bundle no longer applies.
#   Java: Spark 4.1 runs on Java 17/21 → Corretto 21 stays. Python 3.10+
#   → python3.12 stays.
# -----------------------------------------------------------------------------
SPARK_VERSION="4.1.2"
HADOOP_LINE="hadoop3"            # Spark's "-bin-hadoop3" build
HADOOP_AWS_VERSION="3.4.2"       # matches Spark 4.1.2's bundled Hadoop
AWS_SDK_V2_VERSION="2.29.52"     # the awssdk bundle hadoop-aws 3.4.2 was built against
SPARK_TGZ="spark-${SPARK_VERSION}-bin-${HADOOP_LINE}.tgz"
SPARK_URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_TGZ}"
SPARK_HOME="/opt/spark"
SPARK_USER="spark"

# -----------------------------------------------------------------------------
# Packages: JDK 21, Python 3.12, basic tools
# -----------------------------------------------------------------------------
echo "==> installing JDK 21 + Python 3.12"
# No `curl` here: AL2023 ships curl-minimal, which provides the curl
# binary and CONFLICTS with the full curl package (dnf hard-fails).
dnf install -y \
    java-21-amazon-corretto-headless \
    python3.12 python3.12-pip \
    tar gzip which procps-ng

# Provide a stable `python3.12` -> ensurepip / venv path
python3.12 -m ensurepip --upgrade >/dev/null 2>&1 || true
python3.12 -m pip install --upgrade pip >/dev/null

# bench.py deps (README's manual step, automated): system-wide, NOT --user —
# cloud-init runs as root but the bench runs as ec2-user.
if [ "$ROLE" = "master" ]; then
    # As ec2-user with --user (the bench's runtime user): a system-wide pip
    # tries to upgrade RPM-owned deps (requests) and hard-fails.
    sudo -u ec2-user python3.12 -m pip install --user --quiet "pyspark==${SPARK_VERSION}" boto3
    # Without SPARK_HOME the pip pyspark runs self-contained: no s3a jars,
    # no spark-defaults.conf (master URL!) — the bench would silently run
    # local-mode. Point it at the real install for every login shell.
    echo "export SPARK_HOME=${SPARK_HOME}" >> /etc/profile.d/ematix-env.sh
fi

# -----------------------------------------------------------------------------
# Spark user
# -----------------------------------------------------------------------------
if ! id "$SPARK_USER" >/dev/null 2>&1; then
    useradd --system --home-dir "$SPARK_HOME" --shell /sbin/nologin "$SPARK_USER"
fi

# -----------------------------------------------------------------------------
# Spark download + unpack (idempotent)
# -----------------------------------------------------------------------------
if [[ ! -x "$SPARK_HOME/bin/spark-submit" ]]; then
    echo "==> downloading Spark $SPARK_VERSION"
    cd /tmp
    curl -fsSL -o "$SPARK_TGZ" "$SPARK_URL"
    tar -xzf "$SPARK_TGZ"
    rm -rf "$SPARK_HOME"
    mv "spark-${SPARK_VERSION}-bin-${HADOOP_LINE}" "$SPARK_HOME"
    rm -f "$SPARK_TGZ"
else
    echo "==> Spark already at $SPARK_HOME, skipping download"
fi

mkdir -p "$SPARK_HOME/logs" "$SPARK_HOME/work"
chown -R "$SPARK_USER:$SPARK_USER" "$SPARK_HOME"

# -----------------------------------------------------------------------------
# Hadoop AWS JARs for s3a:// (Hadoop 3.4.x line = AWS SDK v2 bundle)
# -----------------------------------------------------------------------------
# Guard: the pinned hadoop-aws MUST match the hadoop-client the Spark
# tarball actually bundles, or s3a fails at runtime with linkage errors.
BUNDLED_HADOOP="$(ls "$SPARK_HOME"/jars/hadoop-client-api-*.jar 2>/dev/null \
    | sed -E 's/.*hadoop-client-api-([0-9.]+)\.jar/\1/' | head -1 || true)"
if [[ -n "$BUNDLED_HADOOP" && "$BUNDLED_HADOOP" != "$HADOOP_AWS_VERSION" ]]; then
    echo "!! bundled hadoop-client is $BUNDLED_HADOOP but HADOOP_AWS_VERSION=$HADOOP_AWS_VERSION" >&2
    echo "!! re-pin HADOOP_AWS_VERSION (and the matching awssdk bundle) before proceeding" >&2
    exit 1
fi

MAVEN_BASE="https://repo1.maven.org/maven2"
HADOOP_AWS_JAR="hadoop-aws-${HADOOP_AWS_VERSION}.jar"
AWS_SDK_JAR="bundle-${AWS_SDK_V2_VERSION}.jar"

if [[ ! -f "$SPARK_HOME/jars/$HADOOP_AWS_JAR" ]]; then
    echo "==> fetching $HADOOP_AWS_JAR"
    curl -fsSL -o "$SPARK_HOME/jars/$HADOOP_AWS_JAR" \
        "$MAVEN_BASE/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/${HADOOP_AWS_JAR}"
fi
if [[ ! -f "$SPARK_HOME/jars/$AWS_SDK_JAR" ]]; then
    echo "==> fetching $AWS_SDK_JAR (AWS SDK v2 bundle)"
    curl -fsSL -o "$SPARK_HOME/jars/$AWS_SDK_JAR" \
        "$MAVEN_BASE/software/amazon/awssdk/bundle/${AWS_SDK_V2_VERSION}/${AWS_SDK_JAR}"
fi
chown -R "$SPARK_USER:$SPARK_USER" "$SPARK_HOME/jars"

# -----------------------------------------------------------------------------
# Instance-type-driven sizing
# -----------------------------------------------------------------------------
# IMDSv2 — get a token first, then query instance-type.
IMDS_TOKEN="$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' \
    http://169.254.169.254/latest/api/token 2>/dev/null || true)"
INSTANCE_TYPE="$(curl -fsS -H "X-aws-ec2-metadata-token: ${IMDS_TOKEN}" \
    http://169.254.169.254/latest/meta-data/instance-type 2>/dev/null || echo unknown)"

# Region for s3a — same IMDSv2 token; falls back to us-east-2 (the
# campaign default region) if IMDS is unavailable.
AWS_REGION="$(curl -fsS -H "X-aws-ec2-metadata-token: ${IMDS_TOKEN}" \
    http://169.254.169.254/latest/meta-data/placement/region 2>/dev/null || echo us-east-2)"

# EXECUTOR_MEMORY = the heap ONE executor takes. Spark's default is 1g, so
# without this every executor OOMs on any SF100 join ("Lost task …
# executor N"). BUT oversizing is just as fatal the other way: 10g on a
# 16g box (62%) drove the workers into GC thrash → heartbeat timeouts →
# the master deregistered them → re-registration storm → app stuck at 0
# cores (and the boxes so starved sshd stopped answering). So size the
# executor heap to leave GENEROUS absolute headroom for the worker
# daemon (~1g), OS + parquet page cache (~3-4g), and the ~10% executor
# memory overhead — roughly 55-60% of box RAM, not 75%+. WORKER_MEMORY
# (advertised pool) only needs to cover one executor.
case "$INSTANCE_TYPE" in
    c7i.2xlarge) WORKER_MEMORY="10g"; EXECUTOR_MEMORY="8g";  DRIVER_MEMORY="3g" ;;  # 16g box
    c7i.4xlarge) WORKER_MEMORY="22g"; EXECUTOR_MEMORY="20g"; DRIVER_MEMORY="6g" ;;  # 32g box
    c7i.8xlarge) WORKER_MEMORY="44g"; EXECUTOR_MEMORY="40g"; DRIVER_MEMORY="8g" ;;  # 64g box
    *)
        # Fallback: executor ≈ 55% of detected RAM, worker pool a touch above.
        TOTAL_KB=$(awk '/MemTotal/ {print $2}' /proc/meminfo)
        TOTAL_GB=$(( TOTAL_KB / 1024 / 1024 ))
        EXECUTOR_MEMORY="$(( TOTAL_GB * 55 / 100 ))g"
        WORKER_MEMORY="$(( TOTAL_GB * 60 / 100 ))g"
        DRIVER_MEMORY="4g"
        ;;
esac

# Leave 1 core for the OS / Spark daemon overhead.
NPROC=$(nproc)
WORKER_CORES=$(( NPROC > 1 ? NPROC - 1 : 1 ))

echo "==> instance=$INSTANCE_TYPE  memory=$WORKER_MEMORY  cores=$WORKER_CORES"

# -----------------------------------------------------------------------------
# spark-env.sh
# -----------------------------------------------------------------------------
JAVA_HOME_DIR="$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")"

cat >"$SPARK_HOME/conf/spark-env.sh" <<EOF
#!/usr/bin/env bash
# Written by infra/distributed-peers/pyspark/install.sh — do not edit by hand.
export JAVA_HOME="$JAVA_HOME_DIR"
export SPARK_MASTER_HOST="$MASTER_HOST"
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080
export SPARK_WORKER_PORT=7078
export SPARK_WORKER_WEBUI_PORT=8081
export SPARK_WORKER_MEMORY="$WORKER_MEMORY"
export SPARK_WORKER_CORES=$WORKER_CORES
export PYSPARK_PYTHON=/usr/bin/python3.12
# Executor shuffle scratch on EBS: in STANDALONE mode executors take
# their local dirs from this env var (spark.local.dir in the conf only
# covers the driver — Spark warns about exactly this). AL2023 /tmp is a
# 16GB tmpfs; see the spark.local.dir note in spark-defaults.conf.
export SPARK_LOCAL_DIRS=/opt/spark-scratch
EOF
chmod 0755 "$SPARK_HOME/conf/spark-env.sh"

# -----------------------------------------------------------------------------
# spark-defaults.conf
# -----------------------------------------------------------------------------
cat >"$SPARK_HOME/conf/spark-defaults.conf" <<EOF
# Written by infra/distributed-peers/pyspark/install.sh — do not edit by hand.
spark.master                                   spark://${MASTER_HOST}:7077
spark.serializer                               org.apache.spark.serializer.KryoSerializer
spark.sql.parquet.enableVectorizedReader       true
spark.executor.memory                          ${EXECUTOR_MEMORY}
spark.executor.cores                           ${WORKER_CORES}
spark.driver.memory                            ${DRIVER_MEMORY}
spark.sql.adaptive.enabled                     true
spark.sql.adaptive.coalescePartitions.enabled  true
# 400 initial shuffle partitions (was WORKER_CORES≈16 — far too coarse
# for SF100: each partition held ~1/16 of a shuffled fact relation and
# OOM'd the executor). AQE coalescePartitions merges these back down for
# small stages, so 400 is safe across SF10/SF100.
spark.sql.shuffle.partitions                   400

# SF=100 disk-exhaustion fix (2026-07-10, run 20260707T211533Z): shuffle
# files accumulated across the long-lived bench session until Q05 hit
# "No space left on device" on 1TB workers, killing all later queries.
# Three-part fix: (1) periodicGC drops the running app's collected
# shuffles every 90s (default 30min is far too lazy for SF=100);
# (2)+(3) the standalone worker reaps FINISHED apps' dirs — bench.py now
# recycles its session per query, so each query's shuffle becomes
# reapable minutes after the query ends.
spark.cleaner.periodicGC.interval              90s
spark.worker.cleanup.enabled                   true
spark.worker.cleanup.interval                  120
spark.worker.cleanup.appDataTtl                300

# THE actual SF=100 disk fix (2026-07-10, round 3): AL2023 mounts /tmp as
# tmpfs (RAM-backed, ~16GB) and spark.local.dir defaults to /tmp — every
# shuffle wrote to a 16GB RAM-disk no matter how big the EBS volume was.
# Light queries fit; the heavy five (Q05/Q08/Q09/Q17/Q21) exceed it and
# die with "No space left on device" on their first executions.
spark.local.dir                                /opt/spark-scratch

# s3a access via the EC2 instance profile (no static creds on disk).
# hadoop-aws 3.4.x runs on AWS SDK **v2**: the provider below is the
# v2-native IAM/instance-profile provider (the old v1
# com.amazonaws.auth.InstanceProfileCredentialsProvider class no longer
# exists on the classpath). Region comes from IMDS, not a hardcode —
# the May kit pinned us-east-1 while the campaign runs in us-east-2.
spark.hadoop.fs.s3a.impl                       org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.aws.credentials.provider   org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider
spark.hadoop.fs.s3a.endpoint.region            ${AWS_REGION}
spark.hadoop.fs.s3a.connection.maximum         200
spark.hadoop.fs.s3a.threads.max                64
spark.hadoop.fs.s3a.fast.upload                true
EOF

chown -R "$SPARK_USER:$SPARK_USER" "$SPARK_HOME/conf"

# EBS-backed shuffle scratch (see spark.local.dir note above). /tmp-style
# 1777: the spark user's executors AND the ec2-user driver both create
# blockmgr dirs here.
mkdir -p /opt/spark-scratch
chmod 1777 /opt/spark-scratch

# -----------------------------------------------------------------------------
# systemd units
# -----------------------------------------------------------------------------
cat >/etc/systemd/system/spark-master.service <<EOF
[Unit]
Description=Apache Spark standalone master
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$SPARK_USER
Group=$SPARK_USER
Environment=SPARK_HOME=$SPARK_HOME
Environment=JAVA_HOME=$JAVA_HOME_DIR
EnvironmentFile=-$SPARK_HOME/conf/spark-env.sh
ExecStart=$SPARK_HOME/bin/spark-class org.apache.spark.deploy.master.Master --host $MASTER_HOST --port 7077 --webui-port 8080
Restart=on-failure
RestartSec=5s
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF

cat >/etc/systemd/system/spark-worker.service <<EOF
[Unit]
Description=Apache Spark standalone worker
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$SPARK_USER
Group=$SPARK_USER
Environment=SPARK_HOME=$SPARK_HOME
Environment=JAVA_HOME=$JAVA_HOME_DIR
EnvironmentFile=-$SPARK_HOME/conf/spark-env.sh
ExecStart=$SPARK_HOME/bin/spark-class org.apache.spark.deploy.worker.Worker --webui-port 8081 spark://${MASTER_HOST}:7077
Restart=on-failure
RestartSec=5s
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload

# -----------------------------------------------------------------------------
# Start the right service(s) for the role
# -----------------------------------------------------------------------------
if [[ "$ROLE" == "master" ]]; then
    echo "==> starting spark-master"
    systemctl enable --now spark-master.service
    systemctl restart spark-master.service
    # Master is also a client/submitter node; it does NOT run a worker
    # in this layout (the 3 dedicated workers give us 3 worker slots).
    systemctl disable --now spark-worker.service 2>/dev/null || true
else
    echo "==> starting spark-worker"
    systemctl disable --now spark-master.service 2>/dev/null || true
    systemctl enable --now spark-worker.service
    systemctl restart spark-worker.service
fi

sleep 2
systemctl status "spark-${ROLE}.service" --no-pager || true

echo "==> install.sh complete (role=$ROLE, master=$MASTER_HOST)"
