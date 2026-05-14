"""LambdaExecutor — fire-and-forget AWS Lambda invocations.

Used by the Ω.W central scheduler when the operator runs on AWS and
wants serverless workers. Each dispatch calls `lambda.invoke(...)`
with `InvocationType="Event"` (async); Lambda runs the function in
the background and the scheduler reads outcome from the RunLog.

Optional dep: `boto3` (install via the `executor-lambda` extra).

Worker-function contract
------------------------
The Lambda function pointed at by `function_name` MUST:

  1. Accept the JSON event payload this executor produces (shape
     below).
  2. Run the worker — typically by execing the `flow` CLI with the
     `argv` field, or by importing `ematix_flow.cli.main` and
     passing `argv` to it.
  3. Write outcome to the RunLog referenced by `run_log_url`.
  4. Release the claim via `RunLog.release(claim_token)` on exit.

Most users will package the Lambda as a container image whose
ENTRYPOINT is the flow CLI; then the handler is a trivial wrapper:

    def handler(event, context):
        import subprocess
        subprocess.check_call(["flow"] + event["argv"], env={**os.environ, **event["env"]})

Event payload shape
-------------------
```json
{
  "pipeline_name": "...",
  "module": "...",
  "claim_token": "...",
  "lease_seconds": 300,
  "run_log_url": "...",
  "alerter_urls": ["...", "..."],
  "metrics_url": "...",
  "env": {"FOO": "bar"},
  "argv": ["run", "--module", "...", "--claim-token", "...", ...]
}
```

The `argv` field is the exact list `SubprocessExecutor` /
`KubernetesJobExecutor` would pass to `flow run` — pre-built so
the Lambda handler can shell out without re-deriving it.

Cancellation
------------
AWS Lambda has no clean cancel for async (`Event`-mode)
invocations — once `invoke` returns, the function runs to completion
or its configured timeout. `cancel()` on this Executor is documented
as a no-op. The scheduler's `sweep_expired_leases` is what recovers
from runaway invocations: the lease expires, the claim is re-marked,
and the next tick re-dispatches.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from .protocol import DispatchError, DispatchHandle, DispatchSpec
from .subprocess import SubprocessExecutor


@dataclass(frozen=True)
class _LambdaInvokeRef:
    """Backend-specific handle for LambdaExecutor.

    Carries the FunctionName + RequestId so operators can find the
    specific invocation in CloudWatch logs / X-Ray traces. Pure
    data — cancel uses neither because Lambda doesn't support it.
    """

    function_name: str
    request_id: str


class LambdaExecutor:
    """AWS Lambda Executor (async / `InvocationType="Event"`).

    Args:
        function_name: Lambda function ARN or short name. ARN is
            recommended in cross-account / cross-region setups.
        client: pre-built `boto3.client("lambda")`. If None, a
            default client is built via `boto3.client("lambda")` —
            you'll want to pass an explicit one when running outside
            AWS or needing region/profile overrides.
        qualifier: optional Lambda version or alias (e.g. "PROD",
            "$LATEST", "5"). When set, the invocation targets that
            qualifier specifically.
    """

    backend_name = "lambda"

    def __init__(
        self,
        *,
        function_name: str,
        client: Any = None,
        qualifier: str | None = None,
    ):
        if client is None:
            try:
                import boto3
            except ImportError as e:
                raise ImportError(
                    "LambdaExecutor requires boto3. Install with "
                    '`pip install "ematix-flow[executor-lambda]"`.'
                ) from e
            client = boto3.client("lambda")
        self._client = client
        self._function_name = function_name
        self._qualifier = qualifier

    def dispatch(self, spec: DispatchSpec) -> DispatchHandle:
        payload = self.build_event_payload(spec)
        kwargs: dict[str, Any] = {
            "FunctionName": self._function_name,
            "InvocationType": "Event",
            "Payload": json.dumps(payload).encode("utf-8"),
        }
        if self._qualifier is not None:
            kwargs["Qualifier"] = self._qualifier
        try:
            response = self._client.invoke(**kwargs)
        except Exception as e:
            # boto3's ClientError hierarchy varies a bit across
            # versions; catch broadly and re-raise as DispatchError
            # so the scheduler can release the claim cleanly.
            raise DispatchError(
                f"LambdaExecutor: invoke {self._function_name!r} failed: "
                f"{type(e).__name__}: {e}"
            ) from e

        # Async invokes return 202 Accepted; anything else is an
        # error code we should surface so the scheduler doesn't
        # think dispatch succeeded.
        status_code = response.get("StatusCode")
        if status_code is not None and status_code != 202:
            payload_str = ""
            payload_field = response.get("Payload")
            if payload_field is not None:
                try:
                    payload_str = payload_field.read().decode("utf-8")
                except Exception:
                    payload_str = "<unreadable>"
            raise DispatchError(
                f"LambdaExecutor: invoke {self._function_name!r} returned "
                f"StatusCode={status_code}; payload={payload_str}"
            )

        request_id = (
            response.get("ResponseMetadata", {}).get("RequestId", "")
            or response.get("RequestId", "")
        )
        return DispatchHandle(
            pipeline_name=spec.pipeline_name,
            backend=self.backend_name,
            ref=_LambdaInvokeRef(
                function_name=self._function_name,
                request_id=request_id,
            ),
        )

    def cancel(self, handle: DispatchHandle) -> None:
        # AWS Lambda has no async-invoke cancel API; once invoke
        # returns, the function runs to completion or timeout. The
        # scheduler's lease-expiry sweep handles recovery.
        return

    # ---- pure helper exposed for tests + advanced operators -------

    @staticmethod
    def build_event_payload(spec: DispatchSpec) -> dict[str, Any]:
        """Construct the JSON event payload the Lambda function
        receives. Pure function — tests verify the shape without
        any AWS round-trip."""
        return {
            "pipeline_name": spec.pipeline_name,
            "module": spec.module,
            "claim_token": spec.claim_token,
            "lease_seconds": spec.lease_seconds,
            "run_log_url": spec.run_log_url,
            "alerter_urls": list(spec.alerter_urls),
            "metrics_url": spec.metrics_url,
            "env": dict(spec.env),
            "argv": SubprocessExecutor._build_run_argv(spec),
        }
