"""Phase Ω.W.5 — LambdaExecutor.

Two layers of coverage, mirroring the Ω.W.4 k8s test structure:

  1. `build_event_payload` (pure): tests verify the JSON event the
     Lambda handler receives — structured fields plus the pre-built
     `argv` matching what SubprocessExecutor / K8sJobExecutor pass.

  2. `dispatch` / `cancel` with a mocked boto3 lambda client: prove
     the executor calls `invoke(InvocationType="Event", Payload=...)`
     with the right body and surfaces non-202 responses + boto3
     errors as `DispatchError`. No AWS round-trip.

A real-AWS integration test (via moto) is deferred to a follow-up.
"""

from __future__ import annotations

import io
import json
from unittest.mock import MagicMock

import pytest

from ematix_flow.executors import (
    DispatchError,
    DispatchSpec,
    LambdaExecutor,
)
from ematix_flow.executors.lambda_ import _LambdaInvokeRef


def _make_spec(**overrides) -> DispatchSpec:
    return DispatchSpec(
        pipeline_name=overrides.pop("pipeline_name", "ingest_events"),
        module=overrides.pop("module", "my_pipelines"),
        claim_token=overrides.pop("claim_token", "abc123def456"),
        lease_seconds=overrides.pop("lease_seconds", 300),
        run_log_url=overrides.pop("run_log_url", "postgres://flow@logdb/history"),
        alerter_urls=overrides.pop("alerter_urls", []),
        metrics_url=overrides.pop("metrics_url", None),
        env=overrides.pop("env", {}),
    )


def _make_executor(client=None) -> LambdaExecutor:
    return LambdaExecutor(
        function_name="arn:aws:lambda:us-east-1:123:function:flow-worker",
        client=client or MagicMock(),
    )


# ---- build_event_payload (pure) -----------------------------------


def test_payload_contains_structured_fields():
    spec = _make_spec(
        pipeline_name="p1",
        module="my_pipelines",
        claim_token="tok-1",
        lease_seconds=600,
        run_log_url="postgres://flow@logdb/history",
        alerter_urls=["stdout://", "slack://x"],
        metrics_url="prometheus://:9100",
        env={"FOO": "bar"},
    )
    p = LambdaExecutor.build_event_payload(spec)
    assert p["pipeline_name"] == "p1"
    assert p["module"] == "my_pipelines"
    assert p["claim_token"] == "tok-1"
    assert p["lease_seconds"] == 600
    assert p["run_log_url"] == "postgres://flow@logdb/history"
    assert p["alerter_urls"] == ["stdout://", "slack://x"]
    assert p["metrics_url"] == "prometheus://:9100"
    assert p["env"] == {"FOO": "bar"}


def test_payload_argv_matches_subprocess_executor():
    """The argv field is the exact list the SubprocessExecutor would
    pass — pre-built so the Lambda handler doesn't have to re-derive
    it. Anchors a contract between the two executor backends."""
    spec = _make_spec()
    p = LambdaExecutor.build_event_payload(spec)
    argv = p["argv"]
    assert argv[0] == "run"
    assert "--module" in argv
    assert "my_pipelines" in argv
    assert "--claim-token" in argv
    assert "abc123def456" in argv
    assert argv[-1] == "ingest_events"


def test_payload_is_json_serializable():
    spec = _make_spec(env={"NESTED": "x", "ESCAPED": 'has "quotes"'})
    p = LambdaExecutor.build_event_payload(spec)
    serialized = json.dumps(p)
    round_trip = json.loads(serialized)
    assert round_trip == p


def test_payload_alerter_metrics_default_to_empty_and_none():
    spec = _make_spec()
    p = LambdaExecutor.build_event_payload(spec)
    assert p["alerter_urls"] == []
    assert p["metrics_url"] is None
    assert p["env"] == {}


# ---- dispatch with mocked boto3 -----------------------------------


def test_dispatch_invokes_lambda_event_async():
    fake_client = MagicMock()
    fake_client.invoke.return_value = {
        "StatusCode": 202,
        "ResponseMetadata": {"RequestId": "req-abc-123"},
    }
    ex = _make_executor(client=fake_client)
    handle = ex.dispatch(_make_spec())

    fake_client.invoke.assert_called_once()
    call = fake_client.invoke.call_args
    assert call.kwargs["FunctionName"].endswith(":function:flow-worker")
    # Async invocation — the scheduler must never wait on Lambda.
    assert call.kwargs["InvocationType"] == "Event"
    # Payload is JSON-encoded bytes.
    payload_bytes = call.kwargs["Payload"]
    payload = json.loads(payload_bytes.decode("utf-8"))
    assert payload["pipeline_name"] == "ingest_events"

    assert handle.backend == "lambda"
    assert isinstance(handle.ref, _LambdaInvokeRef)
    assert handle.ref.request_id == "req-abc-123"


def test_dispatch_passes_qualifier_when_set():
    fake_client = MagicMock()
    fake_client.invoke.return_value = {"StatusCode": 202, "ResponseMetadata": {}}
    ex = LambdaExecutor(
        function_name="flow-worker",
        client=fake_client,
        qualifier="PROD",
    )
    ex.dispatch(_make_spec())
    call = fake_client.invoke.call_args
    assert call.kwargs["Qualifier"] == "PROD"


def test_dispatch_omits_qualifier_when_none():
    fake_client = MagicMock()
    fake_client.invoke.return_value = {"StatusCode": 202, "ResponseMetadata": {}}
    ex = LambdaExecutor(function_name="flow-worker", client=fake_client)
    ex.dispatch(_make_spec())
    call = fake_client.invoke.call_args
    assert "Qualifier" not in call.kwargs


def test_dispatch_boto_error_becomes_dispatch_error():
    fake_client = MagicMock()
    fake_client.invoke.side_effect = RuntimeError("Throttled")
    ex = _make_executor(client=fake_client)
    with pytest.raises(DispatchError, match="invoke .* failed"):
        ex.dispatch(_make_spec())


def test_dispatch_non_202_status_raises():
    """Async invokes always return 202. Anything else (4xx, 5xx) means
    the function definitely didn't accept the invocation — the
    scheduler must release the claim and retry."""
    fake_client = MagicMock()
    fake_client.invoke.return_value = {
        "StatusCode": 429,
        "Payload": io.BytesIO(b'{"errorMessage": "TooManyRequestsException"}'),
        "ResponseMetadata": {},
    }
    ex = _make_executor(client=fake_client)
    with pytest.raises(DispatchError, match="StatusCode=429"):
        ex.dispatch(_make_spec())


def test_dispatch_handle_carries_function_arn_and_request_id():
    fake_client = MagicMock()
    fake_client.invoke.return_value = {
        "StatusCode": 202,
        "ResponseMetadata": {"RequestId": "req-xyz"},
    }
    ex = _make_executor(client=fake_client)
    handle = ex.dispatch(_make_spec())
    assert handle.ref.function_name.endswith(":function:flow-worker")
    assert handle.ref.request_id == "req-xyz"


# ---- cancel --------------------------------------------------------


def test_cancel_is_documented_noop():
    """AWS Lambda has no async-invoke cancel API. cancel() returns
    cleanly; recovery from runaway invocations relies on the
    scheduler's lease-expiry sweep."""
    fake_client = MagicMock()
    ex = _make_executor(client=fake_client)
    fake_client.invoke.return_value = {
        "StatusCode": 202,
        "ResponseMetadata": {"RequestId": "r1"},
    }
    handle = ex.dispatch(_make_spec())
    # Just verify it doesn't raise. Nothing on the boto client should
    # be called either.
    fake_client.invoke.reset_mock()
    ex.cancel(handle)
    fake_client.invoke.assert_not_called()


# ---- protocol conformance ------------------------------------------


def test_lambda_executor_satisfies_protocol():
    from ematix_flow.executors import Executor

    ex = _make_executor()
    assert isinstance(ex, Executor)


# ---- optional-dep gating -------------------------------------------


def test_missing_boto3_raises_loud(monkeypatch):
    """If boto3 isn't installed AND no client is passed in, the
    import error should surface with the install-extra hint —
    not a confusing AttributeError later when the SDK is touched."""
    import builtins

    real_import = builtins.__import__

    def fail_boto3(name, *args, **kwargs):
        if name == "boto3":
            raise ImportError("No module named 'boto3'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fail_boto3)
    with pytest.raises(ImportError, match="executor-lambda"):
        LambdaExecutor(function_name="flow-worker")
