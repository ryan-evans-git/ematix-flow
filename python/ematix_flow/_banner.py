"""Startup banner for `flow` CLI invocations that kick off long-running work.

Where this fires
----------------
Only on commands that actually launch a pipeline / streaming consumer:
`flow run`, `flow consume`, `flow run-due`. Quick read-only commands
(`flow list`, `flow validate`, `flow connections list`) stay quiet so
they remain pleasant to script around.

Stream + suppression rules
--------------------------
The banner is written to **stderr** so the JSON each command emits on
stdout can still be piped into `jq` / `> result.json` without garbage
prefixed to it.

Defaults to off when stderr isn't a TTY (the common case for CI logs,
captured subprocess output, `2> file.log`) — printing block-letter
ANSI art into a log file is just noise.

Override knobs (env vars):
  * ``EMATIX_FLOW_NO_BANNER=1`` — never print, even on a TTY. Wins
    over the force flag below: if a user has explicitly silenced it,
    nothing should bring it back.
  * ``EMATIX_FLOW_BANNER=1`` — always print, even on non-TTY streams.
    Used in tests (pytest captures aren't TTYs) and by anyone who
    wants the banner in their captured logs.
"""

from __future__ import annotations

import os
import sys
from typing import TextIO

from ematix_flow import __version__

# ANSI Shadow figlet font for "EMATIX". Keeping this hand-built so we
# don't pull in pyfiglet (or any new dep) just for one piece of art.
# Width ≈ 47 cols, fits comfortably in any terminal ≥ 80 cols wide.
_LOGO = r"""
███████╗███╗   ███╗ █████╗ ████████╗██╗██╗  ██╗
██╔════╝████╗ ████║██╔══██╗╚══██╔══╝██║╚██╗██╔╝
█████╗  ██╔████╔██║███████║   ██║   ██║ ╚███╔╝
██╔══╝  ██║╚██╔╝██║██╔══██║   ██║   ██║ ██╔██╗
███████╗██║ ╚═╝ ██║██║  ██║   ██║   ██║██╔╝ ██╗
╚══════╝╚═╝     ╚═╝╚═╝  ╚═╝   ╚═╝   ╚═╝╚═╝  ╚═╝
"""


def format_banner() -> str:
    return (
        f"{_LOGO.rstrip()}\n"
        f"  ematix-flow · v{__version__}\n"
        f"  move data between databases, files, and streams\n"
    )


def _should_print(stream: TextIO) -> bool:
    if os.environ.get("EMATIX_FLOW_NO_BANNER") == "1":
        return False
    if os.environ.get("EMATIX_FLOW_BANNER") == "1":
        return True
    isatty = getattr(stream, "isatty", None)
    return bool(isatty and isatty())


def print_banner(stream: TextIO | None = None) -> None:
    out = stream if stream is not None else sys.stderr
    if not _should_print(out):
        return
    out.write(format_banner())
    out.write("\n")
    flush = getattr(out, "flush", None)
    if flush:
        flush()
