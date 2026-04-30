"""`flow` CLI entry point. Implemented in Phase 12.

Subcommands in v0.1: `flow list`, `flow run`, `flow run-due`.
The long-lived `flow daemon` is deferred to v0.2.
"""

from __future__ import annotations

import sys


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    print("ematix-flow CLI: not yet implemented (Phase 12). Args:", argv)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
