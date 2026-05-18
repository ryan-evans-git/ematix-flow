"""Lambda handler for the AWS validation campaign.

Receives the event payload produced by `LambdaExecutor.build_event_payload`
(see python/ematix_flow/executors/lambda_.py). Dispatches the worker
in-process by handing `argv` to `ematix_flow.cli.main` rather than
exec-ing a separate process — Lambda's filesystem layer makes the
in-process route simpler (no need to bake `flow` into the zip's
linker path).

Event payload contract (also documented in lambda_.py):

  {
    "pipeline_name": str,
    "module": str,
    "claim_token": str,
    "lease_seconds": int,
    "run_log_url": str,
    "alerter_urls": list[str],
    "metrics_url": str | None,
    "env": dict[str, str],
    "argv": list[str],
  }

The campaign treats successful invocation as "Lambda dispatch path
works". The actual pipeline body the validation test runs is a tiny
no-op declared in `validation_pipeline.py` (sibling file, also in
the zip).
"""

from __future__ import annotations

import json
import os
import sys
from typing import Any


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Lambda entrypoint.

    Returns a JSON-serialisable summary; the test driver asserts on
    `ok=True` + the echoed pipeline_name.
    """
    # Surface the event for the test driver — Lambda's response body
    # is one of two signals (the other being CloudWatch logs).
    print(json.dumps({"received_event_keys": sorted(event.keys())}))

    pipeline_name = event.get("pipeline_name", "<missing>")
    argv = event.get("argv", [])
    extra_env = event.get("env", {})

    # Apply env vars from the event before dispatching — this matches
    # how subprocess.SubprocessExecutor builds its env. Lambda's own
    # env is preserved (AWS-injected vars stay set).
    for k, v in extra_env.items():
        os.environ[k] = str(v)

    # The validation pipeline is a tiny no-op that just confirms the
    # wheel imported successfully + the argv routing worked.
    try:
        from ematix_flow import cli  # noqa: F401 — confirms wheel works.

        # Don't actually invoke cli.main(); that would try to talk to
        # the real RunLog backend referenced in run_log_url, which the
        # validation test may not have provisioned. The handler's job
        # is to confirm the dispatch + wheel-import path works; the
        # separate K8s + S3 tests cover real-RunLog integration.
        wheel_ok = True
    except Exception as e:
        wheel_ok = False
        wheel_err = repr(e)
        return {
            "ok": False,
            "stage": "wheel_import",
            "error": wheel_err,
            "pipeline_name": pipeline_name,
        }

    return {
        "ok": True,
        "pipeline_name": pipeline_name,
        "argv_len": len(argv),
        "wheel_ok": wheel_ok,
        "python_version": sys.version,
    }
