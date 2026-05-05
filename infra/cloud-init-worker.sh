#!/usr/bin/env bash
#
# Σ.C PR 2: cloud-init bootstrap for an ematix-flow-distributed
# worker node. Runs as root on first boot of each EC2 worker.
#
# Usage (manual): SSH in, then `bash cloud-init-worker.sh`.
# Usage (EC2 user-data): paste the contents of this file into the
# instance's user-data field at launch — cloud-init will execute
# it as root before the instance becomes available.
#
# Effect:
#   - Installs build deps (gcc, cmake, perl, openssl, libsasl, libcurl)
#   - Installs rustup + stable Rust
#   - Clones ematix-flow @ main into /opt/ematix-flow
#   - Builds the `flow-worker` binary in release mode
#   - Installs a systemd unit so the worker survives reboots
#   - Starts the worker on port 50051

set -euo pipefail

EMATIX_FLOW_REPO="${EMATIX_FLOW_REPO:-https://github.com/ryan-evans-git/ematix-flow.git}"
EMATIX_FLOW_REF="${EMATIX_FLOW_REF:-main}"
WORKER_PORT="${WORKER_PORT:-50051}"
INSTALL_DIR="${INSTALL_DIR:-/opt/ematix-flow}"

echo "==> installing build dependencies"
apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    git build-essential pkg-config cmake perl \
    libssl-dev libcurl4-openssl-dev libsasl2-dev \
    ca-certificates curl

echo "==> installing rustup + stable Rust"
if ! command -v rustup >/dev/null 2>&1; then
    curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable
    # shellcheck disable=SC1091
    source "$HOME/.cargo/env"
fi
export PATH="$HOME/.cargo/bin:$PATH"

echo "==> fetching ematix-flow @ ${EMATIX_FLOW_REF}"
mkdir -p "$INSTALL_DIR"
if [ ! -d "$INSTALL_DIR/.git" ]; then
    git clone "$EMATIX_FLOW_REPO" "$INSTALL_DIR"
fi
cd "$INSTALL_DIR"
git fetch origin
git checkout "$EMATIX_FLOW_REF"

echo "==> building flow-worker (release)"
cargo build --release --bin flow-worker -p ematix-flow-distributed
install -m 0755 target/release/flow-worker /usr/local/bin/flow-worker

echo "==> installing systemd unit"
cat >/etc/systemd/system/flow-worker.service <<EOF
[Unit]
Description=ematix-flow distributed batch SQL worker
After=network-online.target
Wants=network-online.target

[Service]
Type=exec
ExecStart=/usr/local/bin/flow-worker --bind 0.0.0.0 --port ${WORKER_PORT}
Restart=on-failure
RestartSec=2s
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now flow-worker.service

echo "==> flow-worker active on :${WORKER_PORT}"
systemctl status flow-worker.service --no-pager || true
