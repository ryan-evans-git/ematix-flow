"""KubernetesJobExecutor — spawn one `batch/v1.Job` per pipeline fire.

Used by the Ω.W central scheduler in multi-host deployments. Each
dispatch creates a Job; the Job's pod runs `flow run ...` exactly
once and exits. The RunLog owns retry (Ω.2), so the Job is
configured with `backoffLimit: 0` + `restartPolicy: Never` to
guarantee single-fire semantics.

Optional dep: the `kubernetes` Python client (install via the
`executor-k8s` extra).

Worker-image contract
---------------------
The image referenced by the Job is the operator's responsibility.
It MUST contain:

  - the `flow` CLI binary on PATH (typically `pip install ematix-flow`)
  - the user's pipelines module importable by name
  - any runtime extras the pipelines depend on

The DispatchSpec's `module` and `env` are passed verbatim — set
`PYTHONPATH` via `env` if the module lives somewhere non-standard.

Manifest shape
--------------
```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: flow-<pipeline>-<uuid8>
  namespace: <namespace>
  labels:
    ematix-flow/pipeline: <pipeline>
    ematix-flow/claim-token-short: <first-8-chars>
spec:
  backoffLimit: 0                # RunLog owns retry; we never want
                                 # k8s to fire the pipeline twice.
  ttlSecondsAfterFinished: 3600  # auto-clean completed Jobs after 1h
  template:
    spec:
      restartPolicy: Never
      serviceAccountName: <sa>?
      containers:
        - name: worker
          image: <image>
          args: ["run", "--module", ..., "--claim-token", ...]
          env: [{name, value}, ...]
```
"""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from typing import Any

from .protocol import DispatchError, DispatchHandle, DispatchSpec
from .subprocess import SubprocessExecutor


@dataclass(frozen=True)
class _K8sJobRef:
    """Backend-specific handle payload for KubernetesJobExecutor.

    Holds the namespace + Job name so cancel() can find the right
    object to delete. Pure data — no live API client references —
    so handles are safe to pass between scheduler ticks.
    """

    namespace: str
    job_name: str


class KubernetesJobExecutor:
    """Spawn `batch/v1.Job` per dispatch.

    Args:
        namespace: target namespace for created Jobs.
        image: container image for the worker (must contain the
            `flow` CLI + the user's module + any runtime extras).
        service_account: optional pod-level service account name.
            Useful when the worker needs IAM-via-IRSA / Workload
            Identity to reach a cloud RunLog backend.
        api_client: pre-built `kubernetes.client.ApiClient`. If
            None, the in-cluster config is loaded (or `~/.kube/config`
            outside the cluster — that's what the library does).
        labels: extra labels to attach to every created Job.
        backoff_limit: k8s retries on container failure. Default 0
            — the RunLog's retry policy is the source of truth. Set
            higher only if your pipelines aren't idempotent (don't —
            fix the pipeline instead).
        ttl_seconds_after_finished: how long completed Jobs linger
            before kubelet GCs them. Default 3600 (1h). Set to None
            to keep Jobs around indefinitely for debugging.
    """

    backend_name = "kubernetes"

    def __init__(
        self,
        *,
        namespace: str,
        image: str,
        service_account: str | None = None,
        api_client: Any = None,
        labels: dict[str, str] | None = None,
        backoff_limit: int = 0,
        ttl_seconds_after_finished: int | None = 3600,
    ):
        try:
            from kubernetes import client, config
        except ImportError as e:
            raise ImportError(
                "KubernetesJobExecutor requires the `kubernetes` client. "
                'Install with `pip install "ematix-flow[executor-k8s]"`.'
            ) from e

        if api_client is None:
            try:
                config.load_incluster_config()
            except config.ConfigException:
                try:
                    config.load_kube_config()
                except Exception as e:
                    raise DispatchError(
                        "KubernetesJobExecutor: could not load k8s config "
                        "(neither in-cluster nor ~/.kube/config). Pass "
                        "api_client= explicitly if you have a non-default "
                        "setup."
                    ) from e
            self._batch = client.BatchV1Api()
        else:
            self._batch = client.BatchV1Api(api_client=api_client)

        self._namespace = namespace
        self._image = image
        self._service_account = service_account
        self._labels = dict(labels or {})
        self._backoff_limit = backoff_limit
        self._ttl = ttl_seconds_after_finished

    def dispatch(self, spec: DispatchSpec) -> DispatchHandle:
        job_name = self._make_job_name(spec.pipeline_name)
        manifest = self.build_job_manifest(
            spec=spec,
            job_name=job_name,
            namespace=self._namespace,
            image=self._image,
            service_account=self._service_account,
            labels=self._labels,
            backoff_limit=self._backoff_limit,
            ttl_seconds_after_finished=self._ttl,
        )
        try:
            self._batch.create_namespaced_job(
                namespace=self._namespace,
                body=manifest,
            )
        except Exception as e:
            # `kubernetes.client.exceptions.ApiException` doesn't always
            # subclass the same hierarchy across versions; catch broadly
            # and re-raise as DispatchError so the scheduler can release
            # the claim. The underlying cause is preserved via `from`.
            raise DispatchError(
                f"KubernetesJobExecutor: failed to create Job "
                f"{self._namespace}/{job_name}: {type(e).__name__}: {e}"
            ) from e
        return DispatchHandle(
            pipeline_name=spec.pipeline_name,
            backend=self.backend_name,
            ref=_K8sJobRef(namespace=self._namespace, job_name=job_name),
        )

    def cancel(self, handle: DispatchHandle) -> None:
        if not isinstance(handle.ref, _K8sJobRef):
            return
        from kubernetes import client

        try:
            self._batch.delete_namespaced_job(
                name=handle.ref.job_name,
                namespace=handle.ref.namespace,
                # Background propagation cascades to the pod —
                # otherwise the Job is removed but the pod lingers.
                body=client.V1DeleteOptions(propagation_policy="Background"),
            )
        except Exception:
            # cancel() is best-effort. The lease expires; the
            # scheduler reclaims and re-dispatches.
            pass

    # ---- pure functions: easy to unit-test without a real cluster ----

    @staticmethod
    def _make_job_name(pipeline: str) -> str:
        """`flow-<sanitized-pipeline>-<8-char-uuid>`, ≤ 63 chars
        (the DNS-1123 limit for k8s names)."""
        sanitized = re.sub(r"[^a-z0-9-]+", "-", pipeline.lower()).strip("-")
        if not sanitized:
            sanitized = "pipeline"
        suffix = "-" + uuid.uuid4().hex[:8]
        # Reserve room for the `flow-` prefix + the uuid suffix.
        max_pipeline_len = 63 - len("flow-") - len(suffix)
        sanitized = sanitized[:max_pipeline_len].rstrip("-") or "pipeline"
        return f"flow-{sanitized}{suffix}"

    @staticmethod
    def build_job_manifest(
        *,
        spec: DispatchSpec,
        job_name: str,
        namespace: str,
        image: str,
        service_account: str | None,
        labels: dict[str, str],
        backoff_limit: int,
        ttl_seconds_after_finished: int | None,
    ) -> dict[str, Any]:
        """Construct a `batch/v1.Job` manifest dict. Exposed at module
        scope so the test suite can verify the manifest without
        hitting an API server.

        Returned as a plain dict so the kubernetes client's
        `create_namespaced_job` accepts it without us depending on
        the model classes (which differ across client versions).
        """
        worker_argv = SubprocessExecutor._build_run_argv(spec)
        env_list = [{"name": k, "value": v} for k, v in sorted(spec.env.items())]
        merged_labels = {
            "ematix-flow/pipeline": spec.pipeline_name[:63],
            "ematix-flow/claim-token-short": spec.claim_token[:8],
            **labels,
        }
        pod_spec: dict[str, Any] = {
            "restartPolicy": "Never",
            "containers": [
                {
                    "name": "worker",
                    "image": image,
                    "args": worker_argv,
                    "env": env_list,
                }
            ],
        }
        if service_account is not None:
            pod_spec["serviceAccountName"] = service_account
        job_spec: dict[str, Any] = {
            "backoffLimit": backoff_limit,
            "template": {
                "metadata": {"labels": merged_labels},
                "spec": pod_spec,
            },
        }
        if ttl_seconds_after_finished is not None:
            job_spec["ttlSecondsAfterFinished"] = ttl_seconds_after_finished
        return {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": job_name,
                "namespace": namespace,
                "labels": merged_labels,
            },
            "spec": job_spec,
        }
