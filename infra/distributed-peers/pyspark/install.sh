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
dnf install -y \
    java-21-amazon-corretto-headless \
    python3.12 python3.12-pip \
    tar gzip curl which procps-ng

# Provide a stable `python3.12` -> ensurepip / venv path
python3.12 -m ensurepip --upgrade >/dev/null 2>&1 || true
python3.12 -m pip install --upgrade pip >/dev/null

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

case "$INSTANCE_TYPE" in
    c7i.2xlarge) WORKER_MEMORY="12g" ;;
    c7i.4xlarge) WORKER_MEMORY="28g" ;;
    c7i.8xlarge) WORKER_MEMORY="56g" ;;
    *)
        # Fallback: 75% of detected RAM, in whole GiB.
        TOTAL_KB=$(awk '/MemTotal/ {print $2}' /proc/meminfo)
        WORKER_MEMORY="$(( TOTAL_KB * 75 / 100 / 1024 / 1024 ))g"
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
spark.sql.adaptive.enabled                     true
spark.sql.adaptive.coalescePartitions.enabled  true
spark.sql.shuffle.partitions                   ${WORKER_CORES}

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
