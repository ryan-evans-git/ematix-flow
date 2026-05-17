#!/bin/bash
# Build the Lambda deployment zip:
#   - handler.py at the root
#   - ematix-flow wheel installed at `python/`
#   - boto3 + dependencies bundled
#
# Lambda Python 3.12 runtime expects this layout: anything at the
# zip root is on PYTHONPATH; subdirectories are too if you don't
# nest under `python/`.
#
# Output: $REPO_ROOT/infra/test-validation/.terraform-build/lambda-package.zip
#
# Run from the repo root (or anywhere — the script resolves paths).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_DIR="$(dirname "$SCRIPT_DIR")"
REPO_ROOT="$(cd "$INFRA_DIR/../.." && pwd)"
BUILD_DIR="$INFRA_DIR/.terraform-build"
STAGE_DIR="$BUILD_DIR/lambda-stage"
ZIP_OUT="$BUILD_DIR/lambda-package.zip"

echo "==> Cleaning stage dir"
rm -rf "$STAGE_DIR" "$ZIP_OUT"
mkdir -p "$STAGE_DIR"

# ---- 1. Build the ematix-flow wheel ----
#
# Maturin produces a linux/amd64 wheel when we pass --target. For
# AWS Lambda Python 3.12 we want manylinux2014_x86_64 or newer.
# Easiest: run inside the build container. Skip when --wheel-from-cache
# is passed (CI / repeated runs).
WHEEL_DIR="$REPO_ROOT/target/wheels"
mkdir -p "$WHEEL_DIR"

if [[ "${1:-}" != "--wheel-from-cache" ]]; then
  echo "==> Building ematix-flow Python wheel"
  # Three build paths:
  #   1. Native Linux build: we ARE Linux, no cross-compile needed.
  #      Fastest, most reliable. This is the EC2 campaign path.
  #   2. macOS + zig: cross-compile to x86_64-linux-gnu for Lambda.
  #      For local laptop builds.
  #   3. macOS no-zig: docker maturin container. Slowest fallback.
  #
  # `python3.12 -m maturin` style avoids the `~/.local/bin` PATH
  # issue that bites `--user` pip installs.
  MATURIN="python3.12 -m maturin"
  if ! $MATURIN --version >/dev/null 2>&1; then
    if command -v maturin >/dev/null; then
      MATURIN="maturin"
    else
      echo "ERROR: maturin not available. Install: python3.12 -m pip install --user maturin" >&2
      exit 1
    fi
  fi

  case "$(uname -s)" in
    Linux)
      # Path 1: native build on the same Linux/x86 the wheel will run on.
      (cd "$REPO_ROOT/crates/ematix-flow-py" && \
        $MATURIN build --release --out "$WHEEL_DIR")
      ;;
    Darwin)
      if command -v zig >/dev/null; then
        (cd "$REPO_ROOT/crates/ematix-flow-py" && \
          $MATURIN build --release --zig \
            --target x86_64-unknown-linux-gnu \
            --out "$WHEEL_DIR")
      else
        echo "    zig not found; falling back to docker maturin"
        docker run --rm \
          -v "$REPO_ROOT:/io" \
          -w /io/crates/ematix-flow-py \
          ghcr.io/pyo3/maturin:latest \
          build --release --out /io/target/wheels --manylinux 2014
      fi
      ;;
    *)
      echo "ERROR: unsupported OS $(uname -s)" >&2
      exit 1
      ;;
  esac
fi

# Find the most recent linux wheel.
WHEEL=$(ls -t "$WHEEL_DIR"/ematix_flow-*-cp*-cp*-manylinux*.whl 2>/dev/null | head -1)
if [[ -z "$WHEEL" ]]; then
  WHEEL=$(ls -t "$WHEEL_DIR"/ematix_flow-*-cp*-cp*-linux*.whl 2>/dev/null | head -1)
fi
if [[ -z "$WHEEL" ]]; then
  echo "ERROR: no linux wheel found in $WHEEL_DIR" >&2
  exit 1
fi
echo "    Wheel: $WHEEL"

# ---- 2. Install the wheel + extras into the stage dir ----
#
# Lambda's Python runtime adds the zip root + /opt to sys.path; we
# install everything flat at the root so no PYTHONPATH gymnastics
# are needed.
echo "==> Installing wheel + extras into stage"
# Use python3.12 -m pip — bare `pip` on AL2023 points at the 3.9
# system pip which can't see --python-version 3.12 wheels correctly.
python3.12 -m pip install \
  --target "$STAGE_DIR" \
  --platform manylinux2014_x86_64 \
  --python-version 3.12 \
  --only-binary=:all: \
  --implementation cp \
  --no-deps \
  "$WHEEL"

# Install runtime deps separately so the manylinux platform constraint
# is enforced.
python3.12 -m pip install \
  --target "$STAGE_DIR" \
  --platform manylinux2014_x86_64 \
  --python-version 3.12 \
  --only-binary=:all: \
  --implementation cp \
  boto3==1.40.* \
  || echo "WARN: some deps may not have manylinux wheels"

# ---- 3. Drop the handler at the zip root ----
cp "$SCRIPT_DIR/handler.py" "$STAGE_DIR/handler.py"

# ---- 4. Build the zip ----
echo "==> Building zip"
(cd "$STAGE_DIR" && zip -r9 "$ZIP_OUT" . -x "*.pyc" "__pycache__/*" >/dev/null)

SIZE=$(du -h "$ZIP_OUT" | cut -f1)
echo "==> Done: $ZIP_OUT ($SIZE)"
