#!/usr/bin/env bash
# Trino 482 installer for the AWS-campaign 4-node cluster (1 coordinator + 3
# workers). Runs from cloud-init on every node. Idempotent: re-running on a
# host that already has Trino installed will reconcile config + restart.
#
# 2026-07-04 refresh (owner decision: competitors at latest stable):
#   Trino 440 -> 482 (released 2026-06-25, trino.io/docs/current/release.html).
#   Trino 482 requires Java 25 (min 25.0.1; Java 21/24 refused at startup)
#   -> Corretto 25 (available in the AL2023 repos as
#   java-25-amazon-corretto-headless).
#   jvm.config rebased on the current documented recommendation (adds
#   --add-modules=jdk.incubator.vector; drops the Java-17-era --add-opens
#   list, no longer in the reference config).
#   hive.properties: the legacy hive.s3-file-system-type property was
#   REMOVED upstream; the native S3 filesystem is enabled per-catalog with
#   fs.s3.enabled=true (Trino 482 docs; the fs.native-s3.enabled spelling
#   from the 458-era docs was renamed). Glue metastore properties
#   (hive.metastore=glue, hive.metastore.glue.region,
#   hive.metastore.glue.default-warehouse-dir) verified still current.
#
# Usage (cloud-init):
#   install.sh --role coordinator --coordinator-host <ip>
#   install.sh --role worker      --coordinator-host <ip>
#
# Environment variables read:
#   BENCH_BUCKET   S3 bucket containing tpch-data/ and results/ (required)
#   AWS_REGION     AWS region for Glue + S3 (defaults to ec2 IMDS placement)
#
# What it installs:
#   - Amazon Corretto JDK 25
#   - Trino server 482 → /opt/trino
#   - /opt/trino/etc/{node,jvm,config}.properties
#   - /opt/trino/etc/catalog/hive.properties (Glue metastore)
#   - systemd unit trino.service
#
# Why Glue: avoids running a Hive Metastore Service ourselves. See
# docs/AWS_CAMPAIGN_2026_05_PLAN.md (Resolved risks → Trino + S3 metastore).
set -euo pipefail

ROLE=""
COORDINATOR_HOST=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --role)               ROLE="$2";              shift 2 ;;
        --coordinator-host)   COORDINATOR_HOST="$2";  shift 2 ;;
        -h|--help)
            sed -n '2,/^set -euo/p' "$0" | sed 's/^# \{0,1\}//;/^set -euo/d'
            exit 0
            ;;
        *)
            echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

if [[ -z "$ROLE" || -z "$COORDINATOR_HOST" ]]; then
    echo "usage: $0 --role coordinator|worker --coordinator-host <ip>" >&2
    exit 2
fi
if [[ "$ROLE" != "coordinator" && "$ROLE" != "worker" ]]; then
    echo "--role must be 'coordinator' or 'worker' (got: $ROLE)" >&2
    exit 2
fi
if [[ -z "${BENCH_BUCKET:-}" ]]; then
    echo "BENCH_BUCKET env var is required (S3 bucket name without s3:// prefix)" >&2
    exit 2
fi

# Resolve region from IMDSv2 if not provided. c7i instances support IMDSv2;
# token TTL of 60 s is plenty for one-shot cloud-init.
if [[ -z "${AWS_REGION:-}" ]]; then
    TOKEN="$(curl -s -X PUT 'http://169.254.169.254/latest/api/token' \
        -H 'X-aws-ec2-metadata-token-ttl-seconds: 60')"
    AWS_REGION="$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" \
        http://169.254.169.254/latest/meta-data/placement/region)"
    export AWS_REGION
fi
echo "==> role=$ROLE coordinator-host=$COORDINATOR_HOST region=$AWS_REGION bucket=$BENCH_BUCKET"

TRINO_VERSION=482
TRINO_HOME=/opt/trino
TRINO_DATA=/var/lib/trino
TRINO_USER=trino
NODE_ID_FILE="$TRINO_DATA/node.id"

# --- packages ---------------------------------------------------------------
echo "==> installing Corretto 25 + tools"
# No `curl` here: AL2023 ships curl-minimal, which provides the curl
# binary and CONFLICTS with the full curl package (dnf hard-fails).
sudo dnf install -y java-25-amazon-corretto-headless tar gzip python3 python3-pip uuid

# Create dedicated user. -r = system user, -m = home dir for cli history.
if ! id -u "$TRINO_USER" >/dev/null 2>&1; then
    sudo useradd -r -m -d "$TRINO_DATA" -s /bin/bash "$TRINO_USER"
fi
sudo mkdir -p "$TRINO_HOME" "$TRINO_DATA"
sudo chown -R "$TRINO_USER:$TRINO_USER" "$TRINO_DATA"

# --- Trino server -----------------------------------------------------------
if [[ ! -x "$TRINO_HOME/bin/launcher" ]]; then
    echo "==> downloading Trino $TRINO_VERSION"
    cd /tmp
    # Trino stopped publishing server tarballs to Maven Central after 476;
    # 477+ ship as GitHub release assets (verified live 2026-07-04).
    curl -fsSLO "https://github.com/trinodb/trino/releases/download/${TRINO_VERSION}/trino-server-${TRINO_VERSION}.tar.gz"
    tar -xzf "trino-server-${TRINO_VERSION}.tar.gz"
    sudo cp -a "trino-server-${TRINO_VERSION}/." "$TRINO_HOME/"
    sudo chown -R "$TRINO_USER:$TRINO_USER" "$TRINO_HOME"
    rm -rf "trino-server-${TRINO_VERSION}" "trino-server-${TRINO_VERSION}.tar.gz"
else
    echo "==> Trino already installed at $TRINO_HOME (skip download)"
fi

# --- Trino CLI (only on coordinator; useful for register-tables + bench) ----
if [[ "$ROLE" == "coordinator" && ! -x /usr/local/bin/trino ]]; then
    echo "==> installing Trino CLI"
    # CLI also moved to GitHub releases; the asset is the extensionless
    # executable jar named trino-cli-<version> (per current CLI docs).
    curl -fsSL -o /tmp/trino-cli.jar \
        "https://github.com/trinodb/trino/releases/download/${TRINO_VERSION}/trino-cli-${TRINO_VERSION}"
    sudo install -m 0755 /tmp/trino-cli.jar /usr/local/bin/trino
    rm -f /tmp/trino-cli.jar
fi

# bench.py deps (coordinator-only): trino client + boto3 for result upload.
# As ec2-user with --user (the bench's actual runtime user): a system-wide
# install tries to upgrade RPM-owned deps (requests) and pip hard-fails
# ("RECORD file not found"); root's --user lands in /root/.local, invisible.
if [[ "$ROLE" == "coordinator" ]]; then
    sudo -u ec2-user python3 -m pip install --user --quiet trino boto3
fi

# --- node.properties --------------------------------------------------------
# Stable UUID per host so coordinator/workers can be restarted without
# losing identity. Persist on disk; regenerate only if missing.
if [[ ! -s "$NODE_ID_FILE" ]]; then
    uuidgen | sudo tee "$NODE_ID_FILE" >/dev/null
    sudo chown "$TRINO_USER:$TRINO_USER" "$NODE_ID_FILE"
fi
NODE_ID="$(sudo cat "$NODE_ID_FILE")"

sudo mkdir -p "$TRINO_HOME/etc/catalog"
sudo tee "$TRINO_HOME/etc/node.properties" >/dev/null <<EOF
node.environment=ematix_campaign
node.id=${NODE_ID}
node.data-dir=${TRINO_DATA}
EOF

# --- jvm.config -------------------------------------------------------------
# Heap sized per instance: docs recommend 70-85% of RAM for -Xmx.
#   c7i.2xlarge (16 GB) -> 12G   c7i.4xlarge (32 GB) -> 24G
# If you bump heap, revisit query.max-memory-per-node below.
IMDS_TOKEN="$(curl -s -X PUT 'http://169.254.169.254/latest/api/token' \
    -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' || true)"
INSTANCE_TYPE="$(curl -s -H "X-aws-ec2-metadata-token: $IMDS_TOKEN" \
    http://169.254.169.254/latest/meta-data/instance-type 2>/dev/null || echo unknown)"
case "$INSTANCE_TYPE" in
    # Constraint: max-mem-per-node + heap headroom (default 0.3*Xmx) <= Xmx,
    # i.e. per-node query memory can be at most 0.7*Xmx. 18GB on a 24G heap
    # violated this (18 + 7.2 > 24) and Trino refused to start.
    c7i.2xlarge) TRINO_XMX="12G"; MAX_MEM_PER_NODE="8GB"  ;;
    c7i.4xlarge) TRINO_XMX="24G"; MAX_MEM_PER_NODE="16GB" ;;
    *)           TRINO_XMX="12G"; MAX_MEM_PER_NODE="8GB"  ;;
esac
echo "==> instance=$INSTANCE_TYPE  Xmx=$TRINO_XMX  max-memory-per-node=$MAX_MEM_PER_NODE"

# Flag set = the Trino 482 documented recommendation (deployment docs),
# verbatim except -Xmx sizing. jdk.incubator.vector is REQUIRED for the
# SIMD paths; the Java-17-era --add-opens list is gone from the
# reference config and stays out here.
sudo tee "$TRINO_HOME/etc/jvm.config" >/dev/null <<EOF
-server
-Xmx${TRINO_XMX}
-XX:InitialRAMPercentage=80
-XX:MaxRAMPercentage=80
-XX:G1HeapRegionSize=32M
-XX:+ExplicitGCInvokesConcurrent
-XX:+ExitOnOutOfMemoryError
-XX:+HeapDumpOnOutOfMemoryError
-XX:-OmitStackTraceInFastThrow
-XX:ReservedCodeCacheSize=512M
-XX:PerMethodRecompilationCutoff=10000
-XX:PerBytecodeRecompilationCutoff=10000
-Djdk.attach.allowAttachSelf=true
-Djdk.nio.maxCachedBufferSize=2000000
-Dfile.encoding=UTF-8
--add-modules=jdk.incubator.vector
EOF

# --- config.properties ------------------------------------------------------
# Discovery: workers + coordinator both point at coordinator:8080. The
# coordinator additionally hosts the discovery service.
if [[ "$ROLE" == "coordinator" ]]; then
    sudo tee "$TRINO_HOME/etc/config.properties" >/dev/null <<EOF
coordinator=true
node-scheduler.include-coordinator=false
http-server.http.port=8080
discovery.uri=http://${COORDINATOR_HOST}:8080
query.max-memory=40GB
query.max-memory-per-node=${MAX_MEM_PER_NODE}
EOF
else
    sudo tee "$TRINO_HOME/etc/config.properties" >/dev/null <<EOF
coordinator=false
http-server.http.port=8080
discovery.uri=http://${COORDINATOR_HOST}:8080
query.max-memory=40GB
query.max-memory-per-node=${MAX_MEM_PER_NODE}
EOF
fi

# --- catalog/hive.properties (Glue metastore + S3) --------------------------
# hive.metastore=glue → no HMS to operate. The IAM role on each node grants
# glue:Get* on the ematix_tpch database + s3:GetObject on the bucket.
# Trino 482: the legacy S3 filesystem (hive.s3-file-system-type) is gone;
# the native filesystem is enabled per-catalog with fs.s3.enabled=true
# and authenticates via the default AWS SDK credential chain (instance
# profile — no static creds on disk).
sudo tee "$TRINO_HOME/etc/catalog/hive.properties" >/dev/null <<EOF
connector.name=hive
hive.metastore=glue
hive.metastore.glue.region=${AWS_REGION}
hive.metastore.glue.default-warehouse-dir=s3://${BENCH_BUCKET}/glue-warehouse/
fs.s3.enabled=true
s3.region=${AWS_REGION}
# Don't try to write Hive views as Trino views or vice versa — read-only.
hive.security=allow-all
hive.non-managed-table-writes-enabled=false
EOF

# Permissions on the whole tree.
sudo chown -R "$TRINO_USER:$TRINO_USER" "$TRINO_HOME" "$TRINO_DATA"

# --- systemd unit -----------------------------------------------------------
# Resolve JAVA_HOME from the installed Corretto (path differs per arch/
# package layout on AL2023 — never hardcode it).
JAVA_HOME_DIR="$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")"

sudo tee /etc/systemd/system/trino.service >/dev/null <<EOF
[Unit]
Description=Trino ${TRINO_VERSION} (${ROLE})
After=network-online.target
Wants=network-online.target

[Service]
Type=forking
User=${TRINO_USER}
Group=${TRINO_USER}
Environment=JAVA_HOME=${JAVA_HOME_DIR}
ExecStart=${TRINO_HOME}/bin/launcher start
ExecStop=${TRINO_HOME}/bin/launcher stop
PIDFile=${TRINO_DATA}/var/run/launcher.pid
LimitNOFILE=131072
Restart=on-failure
RestartSec=10
TimeoutStartSec=180

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable trino.service
sudo systemctl restart trino.service

# --- health-check loop ------------------------------------------------------
# /v1/info returns 200 once the node has registered with the discovery
# service. On workers the same endpoint reflects join state.
echo "==> waiting for Trino to come up on localhost:8080"
for i in $(seq 1 60); do
    code="$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8080/v1/info || true)"
    if [[ "$code" == "200" ]]; then
        echo "==> Trino is up (after $((i*5))s)"
        curl -s http://localhost:8080/v1/info
        echo
        exit 0
    fi
    sleep 5
done

echo "!! Trino did not become ready within 5 min; tail of server.log:" >&2
sudo tail -n 80 "$TRINO_DATA/var/log/server.log" >&2 || true
exit 1
