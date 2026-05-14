"""SubprocessExecutor — spawn a `flow run` worker in a local subprocess.

The simplest Executor: launches `flow run --module M <pipeline>` with
the W.3 worker-side flags (claim-token, heartbeat-interval, run-log,
alerter, metrics) populated from the DispatchSpec.

Use for local development, single-host deployments, and CI integration
tests. For multi-host distributed dispatch see (forthcoming)
`KubernetesJobExecutor` / `LambdaExecutor`.

Workers run fire-and-forget — `dispatch()` returns once the process
has been started, not when it exits. Result lands in the RunLog;
the scheduler reads it on the next loop tick.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from dataclasses import dataclass

from .protocol import DispatchError, DispatchHandle, DispatchSpec


@dataclass
class _SubprocessRef:
    """Backend-specific handle payload for SubprocessExecutor.

    Carries the Popen so cancel() can SIGTERM the worker. Held
    weakly by DispatchHandle.ref so the scheduler can pass handles
    around without leaking process handles to consumers that don't
    need them.
    """

    popen: subprocess.Popen

    @property
    def pid(self) -> int:
        return self.popen.pid


class SubprocessExecutor:
    """Local-subprocess Executor.

    Args:
        flow_binary: path to the `flow` CLI binary. Default is to
            resolve via `shutil.which("flow")`. Pass an explicit path
            for tests or non-PATH installs.
        python: path to the Python interpreter, when invoking
            `python -m ematix_flow.cli` directly instead of `flow`.
            Defaults to None (use the `flow` binary).
    """

    backend_name = "subprocess"

    def __init__(
        self,
        *,
        flow_binary: str | None = None,
        python: str | None = None,
    ):
        if python is not None:
            # Invoke the CLI through the given interpreter — useful
            # for tests so the worker runs in the same venv as the
            # test process.
            self._argv0 = [python, "-m", "ematix_flow.cli"]
        else:
            resolved = flow_binary or shutil.which("flow")
            if resolved is None:
                raise DispatchError(
                    "SubprocessExecutor: no `flow` binary on PATH and no "
                    "flow_binary= / python= override given. Install the "
                    "ematix-flow wheel or pass python=sys.executable for "
                    "in-process testing."
                )
            self._argv0 = [resolved]

    def dispatch(self, spec: DispatchSpec) -> DispatchHandle:
        argv = list(self._argv0) + self._build_run_argv(spec)
        env = os.environ.copy()
        env.update(spec.env)
        try:
            popen = subprocess.Popen(
                argv,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        except OSError as e:
            raise DispatchError(
                f"SubprocessExecutor: failed to spawn {argv[0]!r}: {e}"
            ) from e
        return DispatchHandle(
            pipeline_name=spec.pipeline_name,
            backend=self.backend_name,
            ref=_SubprocessRef(popen=popen),
        )

    def cancel(self, handle: DispatchHandle) -> None:
        if not isinstance(handle.ref, _SubprocessRef):
            return
        popen = handle.ref.popen
        if popen.poll() is None:
            # SIGTERM first; the worker's heartbeat thread + RunLog
            # write are idempotent so a clean shutdown is preferred.
            popen.terminate()

    @staticmethod
    def _build_run_argv(spec: DispatchSpec) -> list[str]:
        argv = [
            "run",
            "--module",
            spec.module,
            "--claim-token",
            spec.claim_token,
            "--heartbeat-interval",
            # Heartbeat at 1/3 of lease so the scheduler has two
            # missed-heartbeat windows before declaring death.
            str(max(1, spec.lease_seconds // 3)),
            "--lease-seconds",
            str(spec.lease_seconds),
            "--run-log-url",
            spec.run_log_url,
        ]
        for url in spec.alerter_urls:
            argv.extend(["--alerter", url])
        if spec.metrics_url:
            argv.extend(["--metrics", spec.metrics_url])
        argv.append(spec.pipeline_name)
        return argv


# Convenience: the python-fallback Executor that uses the current
# interpreter via `python -m ematix_flow.cli`. Used by the in-process
# test suite so workers run in the same venv with optional extras
# already installed.
def python_subprocess_executor() -> SubprocessExecutor:
    """Build a SubprocessExecutor that runs workers via
    `<sys.executable> -m ematix_flow.cli ...`. Handy for tests +
    development; avoids needing the `flow` binary on PATH."""
    return SubprocessExecutor(python=sys.executable)
