"""``flow secrets test`` — debug helper for secret-resolver setup.

Resolves a single ``${vault:...}`` / ``${aws:...}`` / ``${gcp:...}``
reference and prints the result (redacted by default; ``--show``
reveals).
"""
from __future__ import annotations

import os

import pytest

from ematix_flow.cli import main


@pytest.fixture(autouse=True)
def _scrub_env():
    """Each test gets a clean slate for the env vars we touch."""
    keys = ("FLOW_TEST_SECRET", "FLOW_TEST_EMPTY", "FLOW_TEST_LONG")
    saved = {k: os.environ.pop(k, None) for k in keys}
    yield
    for k, v in saved.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v


class TestRedactedByDefault:
    def test_redacts_first_2_and_last_2(self, capsys) -> None:
        os.environ["FLOW_TEST_SECRET"] = "hunter2-very-long"
        rc = main(["secrets", "test", "FLOW_TEST_SECRET"])
        assert rc == 0
        captured = capsys.readouterr().out
        assert "hu...ng" in captured
        assert "17 chars" in captured
        # Never leak the raw value in redacted mode.
        assert "hunter2" not in captured

    def test_short_value_fully_masked(self, capsys) -> None:
        os.environ["FLOW_TEST_SECRET"] = "abc"
        rc = main(["secrets", "test", "FLOW_TEST_SECRET"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "***" in out
        assert "abc" not in out


class TestShowFlag:
    def test_show_reveals_full_value(self, capsys) -> None:
        os.environ["FLOW_TEST_SECRET"] = "hunter2"
        rc = main(["secrets", "test", "FLOW_TEST_SECRET", "--show"])
        assert rc == 0
        assert capsys.readouterr().out.strip() == "hunter2"


class TestEmptyAndMissing:
    def test_empty_value(self, capsys) -> None:
        os.environ["FLOW_TEST_SECRET"] = ""
        rc = main(["secrets", "test", "FLOW_TEST_SECRET"])
        assert rc == 0
        assert "<empty>" in capsys.readouterr().out

    def test_unknown_provider_exits_1(self, capsys) -> None:
        # No vault: resolver registered → MissingSecretError → exit 1.
        rc = main(["secrets", "test", "vault:never/registered#k"])
        assert rc == 1
        err = capsys.readouterr().err
        assert "vault" in err.lower()


class TestReferenceForms:
    def test_curly_braced_form_accepted(self, capsys) -> None:
        os.environ["FLOW_TEST_SECRET"] = "abc"
        rc = main(["secrets", "test", "${FLOW_TEST_SECRET}", "--show"])
        assert rc == 0
        assert capsys.readouterr().out.strip() == "abc"

    def test_bare_env_var_accepted(self, capsys) -> None:
        os.environ["FLOW_TEST_SECRET"] = "abc"
        rc = main(["secrets", "test", "FLOW_TEST_SECRET", "--show"])
        assert rc == 0
        assert capsys.readouterr().out.strip() == "abc"
