#!/usr/bin/env bash
#
# Local manylinux_2_28 wheel-build sanity check.
#
# Runs the same maturin build that `.github/workflows/release.yml`'s
# `build-linux` job runs, but inside a `quay.io/pypa/manylinux_2_28`
# Docker container on your laptop. Catches the CI-only failure
# modes (missing C headers, glibc constraint mismatches, librdkafka
# / libsasl2 / curl static-link breakage) without burning Actions
# minutes.
#
# Usage:
#   scripts/local-manylinux-build.sh [PY_VERSION]
#
# PY_VERSION defaults to 3.12. Valid: 3.10, 3.11, 3.12, 3.13.
#
# Output: a wheel under `target/wheels/` if the build succeeds.
# Successful exit code = 0. Non-zero on any compile / link / wheel-
# tag error — same gate the CI uses.
#
# Note: heavy Rust crates (datafusion-functions-aggregate, duckdb)
# can OOM the Docker container at default `cargo` parallelism (one
# job per CPU). Limit Docker Desktop memory to ≥8 GB or set
# `CARGO_BUILD_JOBS=2` to reduce concurrent compilations. CI
# runners have 16 GB and don't hit this.
#
# Requires: Docker. ~3GB image pull on first run (cached after).

set -euo pipefail

PY="${1:-3.12}"

case "$PY" in
    3.10|3.11|3.12|3.13) ;;
    *) echo "error: unsupported python version $PY (try 3.10/3.11/3.12/3.13)" >&2; exit 2 ;;
esac

REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
echo "==> repo root: $REPO_ROOT"
echo "==> target python: $PY"

# manylinux_2_28 is what release.yml uses (newer than the default
# manylinux2014 because DuckDB rejects the older C++ ABI). glibc
# 2.28 baseline; wheels built here run on Ubuntu 20.04+, Debian
# 11+, RHEL/AlmaLinux/Rocky 8+, Fedora 30+.
IMAGE="quay.io/pypa/manylinux_2_28_x86_64"

# Strip "3." → "310"/"311"/"312"/"313" for the manylinux Python path.
PY_NODOT="${PY//./}"

docker run --rm \
    -v "$REPO_ROOT:/io" \
    -w /io \
    -e CARGO_TERM_COLOR=always \
    -e RUST_BACKTRACE=1 \
    -e CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-2}" \
    "$IMAGE" \
    bash -c "
        set -euo pipefail
        echo '==> installing perl-core (needed by vendored openssl Configure)'
        dnf install -y perl-core >/dev/null

        echo '==> installing rust toolchain'
        curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable >/dev/null
        source \"\$HOME/.cargo/env\"

        echo '==> selecting python$PY'
        PYBIN=/opt/python/cp${PY_NODOT}-cp${PY_NODOT}/bin
        export PATH=\"\$PYBIN:\$PATH\"
        python --version

        echo '==> installing maturin'
        pip install --quiet --upgrade pip
        pip install --quiet 'maturin>=1.7'

        echo '==> building wheel'
        maturin build --release --interpreter python --target x86_64-unknown-linux-gnu

        echo
        echo '==> built wheels:'
        ls -lh target/wheels/*cp${PY_NODOT}*.whl
    "

echo
echo "✓ manylinux2014 build succeeded for python$PY"
echo "  wheel(s) in $REPO_ROOT/target/wheels/"
