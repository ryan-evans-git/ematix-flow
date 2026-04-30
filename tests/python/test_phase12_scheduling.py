"""Phase 12: scheduling registry + flow CLI."""

from __future__ import annotations

import sys
import textwrap
from datetime import datetime, timezone
from pathlib import Path

import pytest

from ematix_flow import pipeline as p


@pytest.fixture(autouse=True)
def _clean_registry():
    p._REGISTRY.clear()
    yield
    p._REGISTRY.clear()


def test_register_adds_to_registry() -> None:
    @p.register(name="t1", schedule="0 * * * *")
    def fn1() -> dict:
        return {"ok": True}

    pipelines = p.list_pipelines()
    assert any(sp.name == "t1" for sp in pipelines)
    assert next(sp.schedule for sp in pipelines if sp.name == "t1") == "0 * * * *"


def test_register_returns_decorated_function() -> None:
    @p.register(name="t2", schedule="0 * * * *")
    def fn() -> dict:
        return {"called": True}

    # The decorator must not replace the function — calling it directly works.
    assert fn() == {"called": True}


def test_run_pipeline_invokes_registered_fn() -> None:
    calls = {"n": 0}

    @p.register(name="t3", schedule="0 * * * *")
    def fn() -> dict:
        calls["n"] += 1
        return {"ran": True}

    result = p.run_pipeline("t3")
    assert result == {"ran": True}
    assert calls["n"] == 1


def test_run_pipeline_unknown_name_raises() -> None:
    with pytest.raises(KeyError):
        p.run_pipeline("does_not_exist")


def test_register_duplicate_name_raises() -> None:
    @p.register(name="dup", schedule="0 * * * *")
    def a() -> dict:
        return {}

    with pytest.raises(ValueError, match="dup"):

        @p.register(name="dup", schedule="0 * * * *")
        def b() -> dict:
            return {}


# --- is_due -----------------------------------------------------------------


def test_is_due_true_when_cron_fires_in_window() -> None:
    """Hourly cron fires at HH:00; with now=12:00 and interval=60s, due."""
    now = datetime(2026, 4, 30, 12, 0, 0, tzinfo=timezone.utc)
    assert p.is_due("0 * * * *", now, 60) is True


def test_is_due_false_when_cron_does_not_fire_in_window() -> None:
    """Hourly cron fires at HH:00; with now=12:30, not in (12:29, 12:30]."""
    now = datetime(2026, 4, 30, 12, 30, 0, tzinfo=timezone.utc)
    assert p.is_due("0 * * * *", now, 60) is False


def test_is_due_daily_cron_at_midnight() -> None:
    midnight = datetime(2026, 4, 30, 0, 0, 0, tzinfo=timezone.utc)
    one_minute_in = datetime(2026, 4, 30, 0, 0, 30, tzinfo=timezone.utc)
    noon = datetime(2026, 4, 30, 12, 0, 0, tzinfo=timezone.utc)
    assert p.is_due("0 0 * * *", midnight, 60) is True
    assert p.is_due("0 0 * * *", one_minute_in, 60) is True
    assert p.is_due("0 0 * * *", noon, 60) is False


def test_is_due_invalid_cron_string_raises() -> None:
    now = datetime(2026, 4, 30, 12, 0, 0, tzinfo=timezone.utc)
    with pytest.raises(ValueError):
        p.is_due("not a cron expression", now, 60)


# --- CLI --------------------------------------------------------------------


def _write_module(tmp_path: Path, name: str, body: str) -> str:
    path = tmp_path / f"{name}.py"
    path.write_text(textwrap.dedent(body))
    return name


def test_cli_list_subcommand(tmp_path, capsys) -> None:
    mod = _write_module(
        tmp_path,
        "phase12_mod_list",
        """
        from ematix_flow import pipeline as p

        @p.register(name="hourly", schedule="0 * * * *")
        def hourly_job():
            return {"job": "hourly"}
        """,
    )
    sys.path.insert(0, str(tmp_path))
    try:
        from ematix_flow.cli import main

        rc = main(["list", "--module", mod])
        assert rc == 0
        out = capsys.readouterr().out
        assert "hourly" in out
        assert "0 * * * *" in out
    finally:
        sys.path.pop(0)
        sys.modules.pop(mod, None)


def test_cli_run_subcommand_executes_pipeline(tmp_path, capsys) -> None:
    mod = _write_module(
        tmp_path,
        "phase12_mod_run",
        """
        from ematix_flow import pipeline as p

        @p.register(name="manual", schedule="0 * * * *")
        def manual_job():
            return {"job": "manual", "rows_inserted": 5}
        """,
    )
    sys.path.insert(0, str(tmp_path))
    try:
        from ematix_flow.cli import main

        rc = main(["run", "--module", mod, "manual"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "manual" in out
        assert "5" in out
    finally:
        sys.path.pop(0)
        sys.modules.pop(mod, None)


def test_cli_run_due_fires_only_matching_pipelines(tmp_path, capsys) -> None:
    mod = _write_module(
        tmp_path,
        "phase12_mod_due",
        """
        from ematix_flow import pipeline as p

        @p.register(name="hourly", schedule="0 * * * *")
        def hourly_job():
            return {"job": "hourly"}

        @p.register(name="daily", schedule="0 0 * * *")
        def daily_job():
            return {"job": "daily"}
        """,
    )
    sys.path.insert(0, str(tmp_path))
    try:
        from ematix_flow.cli import main

        # 12:00 UTC: hourly fires (00 of the hour); daily does not (0 0 only fires at midnight).
        rc = main(
            [
                "run-due",
                "--module",
                mod,
                "--now",
                "2026-04-30T12:00:00+00:00",
                "--interval",
                "60",
            ]
        )
        assert rc == 0
        out = capsys.readouterr().out
        assert "hourly" in out
        assert "daily" not in out
    finally:
        sys.path.pop(0)
        sys.modules.pop(mod, None)


def test_cli_run_due_with_no_matches_emits_empty_list(tmp_path, capsys) -> None:
    mod = _write_module(
        tmp_path,
        "phase12_mod_nomatch",
        """
        from ematix_flow import pipeline as p

        @p.register(name="daily", schedule="0 0 * * *")
        def daily_job():
            return {"job": "daily"}
        """,
    )
    sys.path.insert(0, str(tmp_path))
    try:
        from ematix_flow.cli import main

        # 13:30 — daily doesn't fire here.
        rc = main(
            [
                "run-due",
                "--module",
                mod,
                "--now",
                "2026-04-30T13:30:00+00:00",
                "--interval",
                "60",
            ]
        )
        assert rc == 0
        out = capsys.readouterr().out
        # Empty result list (no pipelines fired).
        assert "[]" in out
    finally:
        sys.path.pop(0)
        sys.modules.pop(mod, None)
