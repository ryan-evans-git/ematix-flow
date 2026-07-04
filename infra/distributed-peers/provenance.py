"""Shared run-provenance capture for the distributed campaign bench runners.

The strict LOCAL protocol stamps every result dir with env.json
(scripts/bench/strict_common.sh::capture_env): machine, git SHA, engine
versions, env flags. This module is the campaign-side equivalent for the
PySpark / Trino runners; the ematix runner captures the same shape in
Rust (crates/ematix-flow-distributed/examples/tpch_distributed_campaign.rs).

Every field is best-effort: off-EC2 the IMDS fields are None, a missing
git binary yields None, etc. Provenance must never fail a bench run.

Usage (from infra/distributed-peers/<engine>/bench.py):

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    import provenance
    doc["provenance"] = provenance.collect()
"""

from __future__ import annotations

import os
import platform
import socket
import subprocess
import time
import urllib.request
from pathlib import Path

_IMDS = "http://169.254.169.254/latest"
_ENV_PREFIXES = ("EMAT_", "EMATIX_", "TPCH_", "SPARK_", "TRINO_")
_ENV_EXACT = ("PARTITIONS", "TARGET_PARTITIONS", "RAYON_NUM_THREADS")


def _imds(path: str, timeout: float = 1.0) -> str | None:
    """IMDSv2 lookup with a hard per-call budget. None off-EC2."""
    try:
        tok_req = urllib.request.Request(
            f"{_IMDS}/api/token",
            method="PUT",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"},
        )
        with urllib.request.urlopen(tok_req, timeout=timeout) as r:
            token = r.read().decode()
        req = urllib.request.Request(
            f"{_IMDS}/meta-data/{path}",
            headers={"X-aws-ec2-metadata-token": token},
        )
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read().decode().strip() or None
    except Exception:  # noqa: BLE001 — best-effort by contract
        return None


def _git(repo: Path, *args: str) -> str | None:
    try:
        out = subprocess.run(
            ["git", "-C", str(repo), *args],
            capture_output=True, text=True, timeout=10,
        )
        s = out.stdout.strip()
        return s if out.returncode == 0 and s else None
    except Exception:  # noqa: BLE001
        return None


def collect(repo_root: Path | None = None, extra: dict | None = None) -> dict:
    """Return the campaign provenance dict (schema shared with the Rust
    runner's `provenance` block)."""
    repo = repo_root or Path(__file__).resolve().parents[2]
    dirty = _git(repo, "status", "--porcelain")
    return {
        "captured_at_unix": int(time.time()),
        "hostname": socket.gethostname(),
        "os": platform.system().lower(),
        "arch": platform.machine(),
        "cores": os.cpu_count() or 1,
        "instance_type": _imds("instance-type"),
        "availability_zone": _imds("placement/availability-zone"),
        "git_sha": _git(repo, "rev-parse", "HEAD"),
        "git_branch": _git(repo, "rev-parse", "--abbrev-ref", "HEAD"),
        "git_dirty": bool(dirty) if dirty is not None else False,
        "python": platform.python_version(),
        "emat_env": {
            k: v for k, v in os.environ.items()
            if k.startswith(_ENV_PREFIXES) or k in _ENV_EXACT
        },
        **(extra or {}),
    }
