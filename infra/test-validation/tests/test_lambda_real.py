"""Real-AWS LambdaExecutor smoke + dispatch bench.

Targets the Lambda function deployed by `lambda/deploy.sh` (which
replaces the Terraform stub with our packaged handler).

Two things it proves:
  1. Sync invocation works end-to-end against real Lambda
     (credentials, IAM, package layout, handler entrypoint).
  2. Async dispatch latency at AWS-typical RTT — surfaces the cost
     of `lambda.invoke(InvocationType=Event)` which is what
     LambdaExecutor uses in production.

Usage:

    LAMBDA_FUNCTION_NAME=ematix-flow-test-xxxx-lambda \\
    AWS_REGION=us-east-2 \\
    python test_lambda_real.py

Exits non-zero if any assertion fails.
"""

from __future__ import annotations

import json
import os
import statistics
import sys
import time

import boto3
from botocore.config import Config


def _synthetic_event(idx: int = 0) -> dict:
    """Event payload matching what LambdaExecutor.build_event_payload emits.

    See python/ematix_flow/executors/lambda_.py for the contract.
    """
    return {
        "pipeline_name": f"validation_pipeline_{idx}",
        "module": "ematix_flow_test.pipelines",
        "claim_token": "validation-claim-token",
        "lease_seconds": 60,
        "run_log_url": "memory://",
        "alerter_urls": [],
        "metrics_url": None,
        "env": {},
        "argv": ["run", "--module", "ematix_flow_test.pipelines",
                 "--pipeline", f"validation_pipeline_{idx}"],
    }


def main() -> int:
    fn = os.environ.get("LAMBDA_FUNCTION_NAME")
    if not fn:
        print("ERROR: LAMBDA_FUNCTION_NAME env var must be set", file=sys.stderr)
        return 2
    region = os.environ.get("AWS_REGION", "us-east-2")

    print(f"==> Lambda smoke: function={fn} region={region}")
    lam = boto3.client(
        "lambda",
        region_name=region,
        config=Config(
            # Tighter retries so a flake fails fast instead of hiding
            # in a 30-sec default-retry loop.
            retries={"max_attempts": 2, "mode": "standard"},
            read_timeout=30,
            connect_timeout=10,
        ),
    )

    # ---- Stage 1: sync invoke, assert handler executed ----
    payload = _synthetic_event(0)
    t0 = time.monotonic()
    resp = lam.invoke(
        FunctionName=fn,
        InvocationType="RequestResponse",  # sync
        Payload=json.dumps(payload).encode(),
        LogType="Tail",
    )
    sync_elapsed_ms = (time.monotonic() - t0) * 1000
    body = json.loads(resp["Payload"].read())
    print(f"    sync invoke: {sync_elapsed_ms:.0f}ms, status={resp['StatusCode']}")
    print(f"    response: {body}")

    if resp.get("FunctionError"):
        print(f"FAIL: Lambda returned FunctionError={resp['FunctionError']}", file=sys.stderr)
        return 1
    if not body.get("ok"):
        print(f"FAIL: handler reported ok=False, body={body}", file=sys.stderr)
        return 1
    if body.get("pipeline_name") != "validation_pipeline_0":
        print(f"FAIL: pipeline_name mismatch", file=sys.stderr)
        return 1
    if not body.get("wheel_ok"):
        print(f"FAIL: wheel_ok=False — ematix-flow wheel did not import", file=sys.stderr)
        return 1

    # ---- Stage 2: async dispatch bench (matches production usage) ----
    n = 50
    latencies_ms: list[float] = []
    base_t = time.monotonic()
    for i in range(n):
        ev = _synthetic_event(i)
        t0 = time.monotonic()
        lam.invoke(
            FunctionName=fn,
            InvocationType="Event",  # async, fire-and-forget
            Payload=json.dumps(ev).encode(),
        )
        latencies_ms.append((time.monotonic() - t0) * 1000)
    total_elapsed = time.monotonic() - base_t
    print(
        f"    async dispatch {n}: total={total_elapsed:.2f}s, "
        f"avg={statistics.mean(latencies_ms):.0f}ms, "
        f"p50={statistics.median(latencies_ms):.0f}ms, "
        f"p95={sorted(latencies_ms)[int(0.95 * n)]:.0f}ms"
    )

    summary = {
        "ok": True,
        "sync_invoke_ms": round(sync_elapsed_ms, 1),
        "n_async": n,
        "async_total_s": round(total_elapsed, 3),
        "async_avg_ms": round(statistics.mean(latencies_ms), 1),
        "async_p50_ms": round(statistics.median(latencies_ms), 1),
        "async_p95_ms": round(sorted(latencies_ms)[int(0.95 * n)], 1),
        "function": fn,
        "region": region,
    }
    print("=== LAMBDA SUMMARY ===")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
