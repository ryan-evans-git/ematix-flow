"""Real-AWS S3RunLog smoke + micro-bench.

Drives the production S3RunLog class against the real S3 bucket
created by Terraform (no moto). Three things it proves:

  1. PUT/GET/LIST against real S3 work (credentials, region, IAM).
  2. The S3RunLog roundtrip semantics match the moto-mocked tests.
  3. Latency at AWS-typical RTT — surfaces the per-write cost so we
     can compare against the Postgres/MySQL backends.

Usage:

    S3_BUCKET=ematix-flow-test-xxxx-results \\
    AWS_REGION=us-east-2 \\
    python test_s3_runlog_real.py

Exits non-zero if any assertion fails. Prints a JSON summary on
success so the bench harness can scrape it into the campaign log.
"""

from __future__ import annotations

import datetime as _dt
import json
import os
import statistics
import sys
import time
import uuid

# Run-from-anywhere ergonomics: add the repo root to sys.path so the
# `ematix_flow` package imports cleanly when this script is run from
# the EC2 box (where the repo is cloned to /opt/ematix/ematix-flow).
REPO_ROOT = os.environ.get("FLOW_REPO_ROOT", "/opt/ematix/ematix-flow")
if os.path.isdir(REPO_ROOT):
    sys.path.insert(0, os.path.join(REPO_ROOT, "python"))

import boto3
from ematix_flow.run_log.s3 import S3RunLog


def main() -> int:
    bucket = os.environ.get("S3_BUCKET")
    if not bucket:
        print("ERROR: S3_BUCKET env var must be set", file=sys.stderr)
        return 2
    region = os.environ.get("AWS_REGION", "us-east-2")

    # Use a per-run prefix so the artifacts are easy to clean up and
    # so parallel invocations don't collide.
    prefix = f"validation-{uuid.uuid4().hex[:8]}/"

    print(f"==> S3RunLog smoke: bucket={bucket} prefix={prefix} region={region}")
    s3 = boto3.client("s3", region_name=region)
    rl = S3RunLog(bucket, prefix=prefix, client=s3)

    # ---- Stage 1: write 100 entries, measure per-put latency ----
    n = 100
    put_latencies_ms: list[float] = []
    base_t = time.monotonic()
    for i in range(n):
        name = f"pipeline_{i:03d}"
        t0 = time.monotonic()
        rl.record_run(
            name,
            ran_at=_dt.datetime.now(_dt.timezone.utc),
            success=(i % 7) != 0,  # 1 in 7 fails
        )
        put_latencies_ms.append((time.monotonic() - t0) * 1000)
    write_elapsed = time.monotonic() - base_t
    print(
        f"    wrote {n} entries in {write_elapsed:.2f}s "
        f"(avg={statistics.mean(put_latencies_ms):.0f}ms, "
        f"p50={statistics.median(put_latencies_ms):.0f}ms, "
        f"p95={sorted(put_latencies_ms)[int(0.95 * n)]:.0f}ms)"
    )

    # ---- Stage 2: restore + spot-check ----
    t0 = time.monotonic()
    restored = rl.restore_into_process()
    list_elapsed = time.monotonic() - t0
    print(f"    listed + got {len(restored)} entries in {list_elapsed:.2f}s")
    assert len(restored) == n, f"expected {n} entries, got {len(restored)}"
    for i in range(n):
        key = f"pipeline_{i:03d}"
        assert key in restored, f"missing entry {key}"
        ran_at, success = restored[key]
        assert isinstance(ran_at, _dt.datetime)
        assert success == ((i % 7) != 0)

    # ---- Stage 3: idempotent overwrite ----
    rl.record_run(
        "pipeline_000",
        ran_at=_dt.datetime.now(_dt.timezone.utc),
        success=False,
    )
    again = rl.restore_into_process()
    assert again["pipeline_000"][1] is False, "overwrite did not stick"

    # ---- Cleanup ----
    # delete_marker so the next campaign doesn't see our entries.
    # `force_destroy = true` on the bucket would handle this at
    # terraform-destroy time, but tidying now keeps the bucket clean
    # for the rest of the campaign.
    paginator = s3.get_paginator("list_objects_v2")
    to_delete = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            to_delete.append({"Key": obj["Key"]})
    if to_delete:
        # delete_objects caps at 1000 keys/call; we're well under.
        s3.delete_objects(Bucket=bucket, Delete={"Objects": to_delete})
        print(f"    cleaned up {len(to_delete)} test objects")

    summary = {
        "ok": True,
        "n_writes": n,
        "write_elapsed_s": round(write_elapsed, 3),
        "list_elapsed_s": round(list_elapsed, 3),
        "put_avg_ms": round(statistics.mean(put_latencies_ms), 1),
        "put_p50_ms": round(statistics.median(put_latencies_ms), 1),
        "put_p95_ms": round(sorted(put_latencies_ms)[int(0.95 * n)], 1),
        "bucket": bucket,
        "region": region,
    }
    print("=== S3 RUN_LOG SUMMARY ===")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
