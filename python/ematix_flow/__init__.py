"""ematix-flow: declarative table management and load strategies for Postgres.

See `docs/PRD.md` for the v0.1 design. The Python package is intentionally
empty in Phase 0 — Phase 2 brings in `ManagedTable`/`Column`, Phase 5 brings
in `Pipeline` and the load strategies.
"""

from ematix_flow import _core

__version__ = "0.1.0"

__all__ = ["__version__", "_core"]
