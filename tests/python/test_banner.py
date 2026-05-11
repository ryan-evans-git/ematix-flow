"""Startup banner: `flow run`/`consume`/`run-due` emit an ASCII banner.

Banner rules:
  * goes to **stderr** so `flow run … | jq` keeps working.
  * suppressed when stderr is not a TTY (default in pytest's capture).
  * always suppressed when `EMATIX_FLOW_NO_BANNER=1` is set,
    even on a TTY.
  * always emitted when `EMATIX_FLOW_BANNER=1` is set (forces it,
    used by the tests below since pytest captures aren't TTYs).
"""

from __future__ import annotations

import io

import pytest

from ematix_flow import __version__
from ematix_flow._banner import format_banner, print_banner


def test_format_banner_contains_brand_and_version() -> None:
    text = format_banner()
    assert "ematix-flow" in text.lower() or "EMATIX" in text
    assert __version__ in text


def test_format_banner_has_block_letters() -> None:
    text = format_banner()
    # ANSI Shadow block letters use these characters extensively.
    assert "█" in text


def test_print_banner_writes_to_provided_stream(monkeypatch) -> None:
    monkeypatch.setenv("EMATIX_FLOW_BANNER", "1")
    buf = io.StringIO()
    print_banner(stream=buf)
    out = buf.getvalue()
    assert "█" in out
    assert __version__ in out


def test_print_banner_suppressed_when_no_banner_env(monkeypatch) -> None:
    monkeypatch.setenv("EMATIX_FLOW_NO_BANNER", "1")
    monkeypatch.delenv("EMATIX_FLOW_BANNER", raising=False)
    buf = io.StringIO()
    print_banner(stream=buf)
    assert buf.getvalue() == ""


def test_print_banner_suppressed_on_non_tty_by_default(monkeypatch) -> None:
    monkeypatch.delenv("EMATIX_FLOW_BANNER", raising=False)
    monkeypatch.delenv("EMATIX_FLOW_NO_BANNER", raising=False)
    buf = io.StringIO()  # StringIO.isatty() is False
    print_banner(stream=buf)
    assert buf.getvalue() == ""


def test_print_banner_force_overrides_non_tty(monkeypatch) -> None:
    monkeypatch.setenv("EMATIX_FLOW_BANNER", "1")
    buf = io.StringIO()
    print_banner(stream=buf)
    assert buf.getvalue() != ""


def test_print_banner_no_banner_beats_force(monkeypatch) -> None:
    """When both env vars are set, NO_BANNER wins — safe default."""
    monkeypatch.setenv("EMATIX_FLOW_BANNER", "1")
    monkeypatch.setenv("EMATIX_FLOW_NO_BANNER", "1")
    buf = io.StringIO()
    print_banner(stream=buf)
    assert buf.getvalue() == ""


@pytest.mark.parametrize("subcommand", ["run", "consume", "run-due"])
def test_cli_long_running_commands_invoke_banner(
    subcommand, monkeypatch, tmp_path, capsys
) -> None:
    """`flow run`, `flow consume`, `flow run-due` print the banner to stderr.

    We force the banner on with EMATIX_FLOW_BANNER=1 (capsys is not a TTY)
    and intentionally fail the command after the banner (unknown module),
    so we don't need to spin up a real pipeline. The banner must be on
    stderr before the error message.
    """
    monkeypatch.setenv("EMATIX_FLOW_BANNER", "1")
    monkeypatch.delenv("EMATIX_FLOW_NO_BANNER", raising=False)

    # Module name that won't import, so the command exits early. The
    # banner fires *before* the user-module import, so this works for
    # all three subcommands without needing a real pipeline.
    from ematix_flow.cli import main

    with pytest.raises((ModuleNotFoundError, SystemExit)):
        if subcommand == "run":
            main(["run", "--module", "ematix_flow._nonexistent_xyz", "foo"])
        elif subcommand == "consume":
            main(["consume", "--module", "ematix_flow._nonexistent_xyz", "foo"])
        else:
            main(["run-due", "--module", "ematix_flow._nonexistent_xyz"])

    captured = capsys.readouterr()
    assert "█" in captured.err, (
        f"banner not on stderr for `flow {subcommand}`; got: {captured.err!r}"
    )


def test_cli_list_does_not_print_banner(monkeypatch, capsys) -> None:
    """Quick read-only subcommands stay quiet — banner is for long-running runs."""
    monkeypatch.setenv("EMATIX_FLOW_BANNER", "1")
    from ematix_flow.cli import main

    with pytest.raises((ModuleNotFoundError, SystemExit)):
        main(["list", "--module", "ematix_flow._nonexistent_xyz"])

    captured = capsys.readouterr()
    assert "█" not in captured.err
    assert "█" not in captured.out
