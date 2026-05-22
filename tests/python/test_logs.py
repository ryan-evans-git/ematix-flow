"""Per-run log capture + `flow logs <run_id>`."""
from __future__ import annotations

import os
import sys

import pytest

from ematix_flow import pipeline
from ematix_flow.cli import main
from ematix_flow.logs import (
    capture_pipeline_logs,
    logs_dir,
    prune_old_logs,
    read_run_logs,
)


@pytest.fixture(autouse=True)
def _scratch_logs_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("EMATIX_FLOW_LOGS_DIR", str(tmp_path / "logs"))
    yield


@pytest.fixture(autouse=True)
def _reset_registry():
    pipeline._REGISTRY.clear()
    pipeline._DEPENDS_ON.clear()
    pipeline._UPSTREAM_FRESHNESS.clear()
    pipeline._LAST_RUN.clear()
    pipeline._RETRY_POLICY.clear()
    pipeline._ATTEMPT_STATE.clear()
    yield
    pipeline._REGISTRY.clear()
    pipeline._DEPENDS_ON.clear()
    pipeline._UPSTREAM_FRESHNESS.clear()
    pipeline._LAST_RUN.clear()
    pipeline._RETRY_POLICY.clear()
    pipeline._ATTEMPT_STATE.clear()


class TestCapturePipelineLogs:
    def test_captures_stdout(self) -> None:
        with capture_pipeline_logs("run-1") as path:
            print("hello from pipeline")
        assert path.exists()
        assert "hello from pipeline" in path.read_text()

    def test_captures_stderr(self) -> None:
        with capture_pipeline_logs("run-2") as path:
            print("uh oh", file=sys.stderr)
        assert "uh oh" in path.read_text()

    def test_original_streams_restored(self) -> None:
        before = (sys.stdout, sys.stderr)
        with capture_pipeline_logs("run-3"):
            pass
        assert (sys.stdout, sys.stderr) == before

    def test_atomic_write_no_partial_file_on_error(self) -> None:
        # Even when the wrapped block raises, the file should still
        # land with whatever was captured before the exception (so
        # `flow logs` can show the partial output that led up to the
        # failure).
        with pytest.raises(RuntimeError), capture_pipeline_logs("run-4"):
            print("printed before raise")
            raise RuntimeError("kapow")
        text = read_run_logs("run-4")
        assert text is not None
        assert "printed before raise" in text


class TestReadRunLogs:
    def test_missing_run_returns_none(self) -> None:
        assert read_run_logs("nonexistent-run") is None

    def test_existing_run_returns_text(self) -> None:
        with capture_pipeline_logs("read-test"):
            print("captured line")
        assert read_run_logs("read-test") == "captured line\n"


class TestPruneOldLogs:
    def test_removes_old_files(self, tmp_path) -> None:
        # Create a fresh file and an artificially old one.
        with capture_pipeline_logs("fresh"):
            print("recent")
        old = logs_dir() / "ancient.log"
        old.write_text("old contents")
        # Backdate the old file by 60 days.
        import time
        cutoff_age = time.time() - 60 * 86400
        os.utime(old, (cutoff_age, cutoff_age))
        removed = prune_old_logs(max_age_days=30)
        assert removed == 1
        assert not old.exists()
        assert (logs_dir() / "fresh.log").exists()


class TestFlowLogsCli:
    def test_missing_run_exits_1(self, capsys) -> None:
        rc = main(["logs", "nonexistent"])
        assert rc == 1
        assert "no log file" in capsys.readouterr().err

    def test_prints_log_text(self, capsys) -> None:
        with capture_pipeline_logs("present"):
            print("line one")
            print("line two")
        rc = main(["logs", "present"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "line one" in out
        assert "line two" in out

    def test_tail_limits_output(self, capsys) -> None:
        with capture_pipeline_logs("tail-test"):
            for i in range(10):
                print(f"line-{i}")
        # Drain capsys so we only see what the CLI prints, not the
        # teed lines from the capture context.
        capsys.readouterr()
        rc = main(["logs", "tail-test", "--tail", "3"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "line-7" in out
        assert "line-8" in out
        assert "line-9" in out
        assert "line-0" not in out


class TestExecutorCapturesWhenEnabled:
    def test_capture_off_by_default(self, monkeypatch) -> None:
        # No EMATIX_FLOW_CAPTURE_LOGS → no log file written.
        monkeypatch.delenv("EMATIX_FLOW_CAPTURE_LOGS", raising=False)

        @pipeline.register(name="quiet_sync", schedule="0 * * * *")
        def _fn():
            print("should-not-be-captured")
            return {"ok": True}

        result = pipeline.run_due_with_dag_detailed(["quiet_sync"])
        assert len(result.fired) == 1
        # No log file in the scratch dir.
        assert list(logs_dir().glob("*.log")) == []

    def test_capture_on_creates_log_file(self, monkeypatch) -> None:
        monkeypatch.setenv("EMATIX_FLOW_CAPTURE_LOGS", "1")

        @pipeline.register(name="loud_sync", schedule="0 * * * *")
        def _fn():
            print("captured-payload")
            return {"ok": True}

        pipeline.run_due_with_dag_detailed(["loud_sync"])
        files = list(logs_dir().glob("loud_sync-*.log"))
        assert len(files) == 1
        assert "captured-payload" in files[0].read_text()
