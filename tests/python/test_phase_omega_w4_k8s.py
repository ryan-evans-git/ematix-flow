"""Phase Ω.W.4 — KubernetesJobExecutor.

Two layers of coverage:

  1. Manifest construction (pure): `build_job_manifest()` is a pure
     function; tests verify the produced Job dict against the
     expected shape (apiVersion/kind, namespace, labels, args, env,
     restartPolicy=Never, backoffLimit=0, ttlSecondsAfterFinished).

  2. Dispatch / cancel with a mocked BatchV1Api: prove the executor
     calls `create_namespaced_job` with the right body on dispatch
     and `delete_namespaced_job` on cancel. No real cluster needed
     for the unit suite.

A real-cluster integration test (against a `kind` cluster) is
gated on $EMATIX_FLOW_TEST_K8S=1 — landed in tests/k8s/ as a
follow-up so this PR doesn't depend on Docker-in-Docker.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from ematix_flow.executors import DispatchError, DispatchSpec

# `kubernetes` is an optional dep; skip the whole module if it's
# not installed.
kubernetes = pytest.importorskip(
    "kubernetes",
    reason="install with `pip install ematix-flow[executor-k8s]`",
)


from ematix_flow.executors import KubernetesJobExecutor  # noqa: E402
from ematix_flow.executors.kubernetes import _K8sJobRef  # noqa: E402

# ---- helpers --------------------------------------------------------


def _make_executor(api_client=None) -> KubernetesJobExecutor:
    """Build an executor without touching real cluster config.

    Patch in a MagicMock api_client so __init__ doesn't try
    load_incluster_config / load_kube_config.
    """
    return KubernetesJobExecutor(
        namespace="flow",
        image="ghcr.io/example/ematix-flow-worker:latest",
        api_client=api_client or MagicMock(),
    )


def _make_spec(**overrides) -> DispatchSpec:
    return DispatchSpec(
        pipeline_name=overrides.pop("pipeline_name", "ingest_events"),
        module=overrides.pop("module", "my_pipelines"),
        claim_token=overrides.pop("claim_token", "abc123def456789012345678"),
        lease_seconds=overrides.pop("lease_seconds", 300),
        run_log_url=overrides.pop("run_log_url", "postgres://flow@logdb/history"),
        alerter_urls=overrides.pop("alerter_urls", []),
        metrics_url=overrides.pop("metrics_url", None),
        env=overrides.pop("env", {}),
    )


# ---- manifest construction (pure) -----------------------------------


def test_manifest_basic_shape():
    spec = _make_spec()
    m = KubernetesJobExecutor.build_job_manifest(
        spec=spec,
        job_name="flow-ingest-events-deadbeef",
        namespace="flow",
        image="img",
        service_account=None,
        labels={},
        backoff_limit=0,
        ttl_seconds_after_finished=3600,
    )
    assert m["apiVersion"] == "batch/v1"
    assert m["kind"] == "Job"
    assert m["metadata"]["name"] == "flow-ingest-events-deadbeef"
    assert m["metadata"]["namespace"] == "flow"


def test_manifest_pod_runs_once_only():
    """backoffLimit=0 + restartPolicy=Never — k8s must not double-fire."""
    m = KubernetesJobExecutor.build_job_manifest(
        spec=_make_spec(),
        job_name="j",
        namespace="flow",
        image="img",
        service_account=None,
        labels={},
        backoff_limit=0,
        ttl_seconds_after_finished=3600,
    )
    assert m["spec"]["backoffLimit"] == 0
    assert m["spec"]["template"]["spec"]["restartPolicy"] == "Never"


def test_manifest_ttl_set_when_not_none():
    m = KubernetesJobExecutor.build_job_manifest(
        spec=_make_spec(),
        job_name="j",
        namespace="flow",
        image="img",
        service_account=None,
        labels={},
        backoff_limit=0,
        ttl_seconds_after_finished=7200,
    )
    assert m["spec"]["ttlSecondsAfterFinished"] == 7200


def test_manifest_ttl_omitted_when_none():
    m = KubernetesJobExecutor.build_job_manifest(
        spec=_make_spec(),
        job_name="j",
        namespace="flow",
        image="img",
        service_account=None,
        labels={},
        backoff_limit=0,
        ttl_seconds_after_finished=None,
    )
    assert "ttlSecondsAfterFinished" not in m["spec"]


def test_manifest_args_from_dispatch_spec():
    spec = _make_spec(
        pipeline_name="p1",
        module="my_pipelines",
        claim_token="tok",
        lease_seconds=600,
        run_log_url="memory://",
    )
    m = KubernetesJobExecutor.build_job_manifest(
        spec=spec,
        job_name="j",
        namespace="flow",
        image="img",
        service_account=None,
        labels={},
        backoff_limit=0,
        ttl_seconds_after_finished=3600,
    )
    args = m["spec"]["template"]["spec"]["containers"][0]["args"]
    assert args[0] == "run"
    assert "--module" in args
    assert "my_pipelines" in args
    assert "--claim-token" in args
    assert "tok" in args
    assert "--lease-seconds" in args
    assert "600" in args
    assert args[-1] == "p1"


def test_manifest_env_pulled_from_spec():
    spec = _make_spec(env={"PIPELINE_OWNER": "team-data", "FOO": "bar"})
    m = KubernetesJobExecutor.build_job_manifest(
        spec=spec,
        job_name="j",
        namespace="flow",
        image="img",
        service_account=None,
        labels={},
        backoff_limit=0,
        ttl_seconds_after_finished=3600,
    )
    envs = m["spec"]["template"]["spec"]["containers"][0]["env"]
    by_name = {e["name"]: e["value"] for e in envs}
    assert by_name["PIPELINE_OWNER"] == "team-data"
    assert by_name["FOO"] == "bar"


def test_manifest_service_account_set_when_provided():
    spec = _make_spec()
    m = KubernetesJobExecutor.build_job_manifest(
        spec=spec,
        job_name="j",
        namespace="flow",
        image="img",
        service_account="flow-worker-sa",
        labels={},
        backoff_limit=0,
        ttl_seconds_after_finished=3600,
    )
    assert m["spec"]["template"]["spec"]["serviceAccountName"] == "flow-worker-sa"


def test_manifest_service_account_omitted_when_none():
    spec = _make_spec()
    m = KubernetesJobExecutor.build_job_manifest(
        spec=spec,
        job_name="j",
        namespace="flow",
        image="img",
        service_account=None,
        labels={},
        backoff_limit=0,
        ttl_seconds_after_finished=3600,
    )
    assert "serviceAccountName" not in m["spec"]["template"]["spec"]


def test_manifest_labels_traceability():
    """Every Job carries pipeline + claim-token labels so operators
    can find a specific fire's pod via `kubectl get pods -l ...`."""
    spec = _make_spec(
        pipeline_name="ingest_events",
        claim_token="abc12345def6789",
    )
    m = KubernetesJobExecutor.build_job_manifest(
        spec=spec,
        job_name="j",
        namespace="flow",
        image="img",
        service_account=None,
        labels={"team": "data-platform"},
        backoff_limit=0,
        ttl_seconds_after_finished=3600,
    )
    labels = m["metadata"]["labels"]
    assert labels["ematix-flow/pipeline"] == "ingest_events"
    assert labels["ematix-flow/claim-token-short"] == "abc12345"
    assert labels["team"] == "data-platform"
    # Pod-template labels match (so `kubectl get pods -l` finds them).
    pod_labels = m["spec"]["template"]["metadata"]["labels"]
    assert pod_labels == labels


# ---- job name sanitization -----------------------------------------


def test_job_name_dns1123_compliance():
    """k8s names must be ≤ 63 chars, lower-case, [a-z0-9-]."""
    name = KubernetesJobExecutor._make_job_name("My Pipeline / Long Name")
    assert len(name) <= 63
    assert name.startswith("flow-")
    assert all(c.islower() or c.isdigit() or c == "-" for c in name)


def test_job_name_long_pipeline_truncated():
    name = KubernetesJobExecutor._make_job_name("x" * 200)
    assert len(name) <= 63


def test_job_name_empty_or_punctuation_only_fallback():
    name = KubernetesJobExecutor._make_job_name("///")
    assert "pipeline" in name


def test_job_name_unique_per_call():
    """Same pipeline → different name each call (uuid suffix)."""
    a = KubernetesJobExecutor._make_job_name("p")
    b = KubernetesJobExecutor._make_job_name("p")
    assert a != b


# ---- dispatch with mocked BatchV1Api -------------------------------


def test_dispatch_creates_job_via_api():
    fake_batch = MagicMock()
    ex = _make_executor()
    # Replace the executor's batch client with our mock.
    ex._batch = fake_batch

    spec = _make_spec()
    handle = ex.dispatch(spec)

    assert handle.backend == "kubernetes"
    assert isinstance(handle.ref, _K8sJobRef)
    assert handle.ref.namespace == "flow"
    assert handle.ref.job_name.startswith("flow-ingest-events-")

    # API was called exactly once with the manifest body.
    fake_batch.create_namespaced_job.assert_called_once()
    call = fake_batch.create_namespaced_job.call_args
    assert call.kwargs["namespace"] == "flow"
    body = call.kwargs["body"]
    assert body["apiVersion"] == "batch/v1"
    assert body["kind"] == "Job"
    assert body["metadata"]["name"] == handle.ref.job_name


def test_dispatch_api_failure_raises_dispatch_error():
    fake_batch = MagicMock()
    fake_batch.create_namespaced_job.side_effect = RuntimeError(
        "API server unreachable"
    )
    ex = _make_executor()
    ex._batch = fake_batch

    with pytest.raises(DispatchError, match="failed to create Job"):
        ex.dispatch(_make_spec())


# ---- cancel --------------------------------------------------------


def test_cancel_deletes_job_via_api():
    fake_batch = MagicMock()
    ex = _make_executor()
    ex._batch = fake_batch

    handle = ex.dispatch(_make_spec())
    fake_batch.create_namespaced_job.reset_mock()

    ex.cancel(handle)
    fake_batch.delete_namespaced_job.assert_called_once()
    call = fake_batch.delete_namespaced_job.call_args
    assert call.kwargs["name"] == handle.ref.job_name
    assert call.kwargs["namespace"] == handle.ref.namespace
    # Background propagation cascades to the pod.
    assert call.kwargs["body"].propagation_policy == "Background"


def test_cancel_swallows_api_errors():
    """Cancel is best-effort — a 404 because the Job already
    completed and was GC'd shouldn't raise."""
    fake_batch = MagicMock()
    fake_batch.delete_namespaced_job.side_effect = RuntimeError("404 Not Found")
    ex = _make_executor()
    ex._batch = fake_batch

    handle = ex.dispatch(_make_spec())
    # Must not raise.
    ex.cancel(handle)


def test_cancel_with_wrong_ref_type_is_noop():
    """cancel() called with a SubprocessExecutor's handle should not
    blow up — different backends shouldn't accidentally talk to k8s."""
    from ematix_flow.executors import DispatchHandle

    fake_batch = MagicMock()
    ex = _make_executor()
    ex._batch = fake_batch
    handle = DispatchHandle(
        pipeline_name="p",
        backend="subprocess",
        ref="not-a-k8s-ref",
    )
    ex.cancel(handle)
    fake_batch.delete_namespaced_job.assert_not_called()


# ---- protocol conformance ------------------------------------------


def test_kubernetes_executor_satisfies_protocol():
    from ematix_flow.executors import Executor

    ex = _make_executor()
    assert isinstance(ex, Executor)
