"""URL-based factory for `Executor` instances.

Lets the `flow scheduler` CLI accept a single `--executor <url>`
flag and route to the right concrete backend:

    subprocess://                                  → SubprocessExecutor
    subprocess+python://                           → python_subprocess_executor()
    k8s://<namespace>[?image=...&service-account=...]  → KubernetesJobExecutor
    lambda://<function-name>[?qualifier=...]       → LambdaExecutor

The k8s/lambda schemes raise `ImportError` with the matching extra
hint if the optional dependency isn't installed.
"""

from __future__ import annotations

from urllib.parse import parse_qs, urlparse

from ematix_flow.executors import (
    DispatchError,
    Executor,
    SubprocessExecutor,
    python_subprocess_executor,
)


def executor_from_url(url: str) -> Executor:
    """Parse `url` and return a configured Executor.

    Unknown schemes raise ValueError with a hint at the supported
    set. Missing optional deps raise ImportError pointing at the
    install extra.
    """
    parsed = urlparse(url)
    scheme = parsed.scheme
    query = {k: v[0] for k, v in parse_qs(parsed.query).items()}

    if scheme == "subprocess":
        return SubprocessExecutor()
    if scheme == "subprocess+python":
        return python_subprocess_executor()

    if scheme == "k8s":
        # k8s://<namespace>[?image=...&service-account=...]
        namespace = parsed.netloc or query.pop("namespace", None)
        if not namespace:
            raise ValueError(
                "k8s:// URL must specify a namespace, e.g. k8s://flow"
            )
        image = query.pop("image", None)
        if not image:
            raise ValueError(
                "k8s:// URL must include ?image=<image>; the worker "
                "image is the operator's responsibility."
            )
        kwargs = {
            "namespace": namespace,
            "image": image,
        }
        sa = query.pop("service-account", None) or query.pop("sa", None)
        if sa:
            kwargs["service_account"] = sa
        from ematix_flow.executors import KubernetesJobExecutor

        return KubernetesJobExecutor(**kwargs)

    if scheme == "lambda":
        # lambda://<function-name>[?qualifier=...]
        function_name = parsed.netloc or query.pop("function-name", None)
        if not function_name:
            raise ValueError(
                "lambda:// URL must specify a function name, e.g. "
                "lambda://flow-worker or lambda://<full-arn>"
            )
        kwargs: dict = {"function_name": function_name}
        qualifier = query.pop("qualifier", None)
        if qualifier:
            kwargs["qualifier"] = qualifier
        from ematix_flow.executors import LambdaExecutor

        return LambdaExecutor(**kwargs)

    raise ValueError(
        f"unknown executor URL scheme {scheme!r}; supported: "
        "subprocess://, subprocess+python://, k8s://<ns>?image=..., "
        "lambda://<function-name>"
    )


# Re-export so the scheduler CLI module can wire one error type
# instead of importing two.
__all__ = ["DispatchError", "executor_from_url"]
