"""ematix-flow: declarative table management and load strategies for Postgres.

See `docs/PRD.md` for the v0.1 design.
"""

from ematix_flow import _core, config, streaming
from ematix_flow.config import connect
from ematix_flow.decorators import ematix
from ematix_flow.markers import natural_key, nullable, pk
from ematix_flow.streaming import run_pipeline

__version__ = "0.1.0"

__all__ = [
    "__version__",
    "_core",
    "config",
    "connect",
    "ematix",
    "natural_key",
    "nullable",
    "pk",
    "run_pipeline",
    "streaming",
]
