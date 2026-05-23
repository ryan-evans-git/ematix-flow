#!/usr/bin/env bash
# Trino 440 installer for the AWS-campaign 4-node cluster (1 coordinator + 3
# workers). Runs from cloud-init on every node. Idempotent: re-running on a
# host that already has Trino installed will reconcile config + restart.
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
#   - Amazon Corretto JDK 21
#   - Trino server 440 → /opt/trino
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

TRINO_VERSION=440
TRINO_HOME=/opt/trino
TRINO_DATA=/var/lib/trino
TRINO_USER=trino
NODE_ID_FILE="$TRINO_DATA/node.id"

# --- packages ---------------------------------------------------------------
echo "==> installing OpenJDK 21 + tools"
sudo dnf install -y java-21-amazon-corretto-headless tar gzip curl python3-pip uuid

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
    curl -fsSLO "https://repo1.maven.org/maven2/io/trino/trino-server/${TRINO_VERSION}/trino-server-${TRINO_VERSION}.tar.gz"
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
    curl -fsSL -o /tmp/trino-cli.jar \
        "https://repo1.maven.org/maven2/io/trino/trino-cli/${TRINO_VERSION}/trino-cli-${TRINO_VERSION}-executable.jar"
    sudo install -m 0755 /tmp/trino-cli.jar /usr/local/bin/trino
    rm -f /tmp/trino-cli.jar
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
# Heap sized for c7i.2xlarge (16 GB) leaving ~3 GB for OS + Glue clients.
# On c7i.4xlarge (32 GB) the same -Xmx is safe; if you bump it, also bump
# query.max-memory-per-node in config.properties below.
sudo tee "$TRINO_HOME/etc/jvm.config" >/dev/null <<'EOF'
-server
-Xmx13G
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
# Allow Trino's reflective access to internal JDK APIs (required since Java 17+).
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
--enable-native-access=ALL-UNNAMED
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
query.max-memory-per-node=10GB
EOF
else
    sudo tee "$TRINO_HOME/etc/config.properties" >/dev/null <<EOF
coordinator=false
http-server.http.port=8080
discovery.uri=http://${COORDINATOR_HOST}:8080
query.max-memory=40GB
query.max-memory-per-node=10GB
EOF
fi

# --- catalog/hive.properties (Glue metastore + S3) --------------------------
# hive.metastore=glue → no HMS to operate. The IAM role on each node grants
# glue:Get* on the ematix_tpch database + s3:GetObject on the bucket.
sudo tee "$TRINO_HOME/etc/catalog/hive.properties" >/dev/null <<EOF
connector.name=hive
hive.metastore=glue
hive.metastore.glue.region=${AWS_REGION}
hive.metastore.glue.default-warehouse-dir=s3://${BENCH_BUCKET}/glue-warehouse/
hive.s3-file-system-type=TRINO
fs.native-s3.enabled=true
s3.region=${AWS_REGION}
# Don't try to write Hive views as Trino views or vice versa — read-only.
hive.security=allow-all
hive.non-managed-table-writes-enabled=false
EOF

# Permissions on the whole tree.
sudo chown -R "$TRINO_USER:$TRINO_USER" "$TRINO_HOME" "$TRINO_DATA"

# --- systemd unit -----------------------------------------------------------
sudo tee /etc/systemd/system/trino.service >/dev/null <<EOF
[Unit]
Description=Trino ${TRINO_VERSION} (${ROLE})
After=network-online.target
Wants=network-online.target

[Service]
Type=forking
User=${TRINO_USER}
Group=${TRINO_USER}
Environment=JAVA_HOME=/usr/lib/jvm/java-21-amazon-corretto
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
