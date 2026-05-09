"""ematix-flow: declarative table management and load strategies for Postgres.

See `docs/PRD.md` for the v0.1 design.
"""

from ematix_flow import _core, config, connections, streaming
from ematix_flow.config import connect
from ematix_flow.connections import (
    Connection,
    DeltaLocalConnection,
    DeltaS3Connection,
    DuckDBConnection,
    KafkaConnection,
    KinesisConnection,
    MySQLConnection,
    ObjectStoreLocalConnection,
    ObjectStoreS3Connection,
    PostgresConnection,
    PubSubConnection,
    RabbitMQConnection,
    SchemaRegistryConnection,
    SQLiteConnection,
    get_connection,
    register_connection,
    registered_connections,
)
from ematix_flow.decorators import ematix
from ematix_flow.markers import natural_key, nullable, pk
from ematix_flow.streaming import (
    CDC,
    Aggregation,
    Join,
    Lookup,
    Source,
    StateStore,
    Target,
    Watermark,
    Window,
    run_pipeline,
    run_streaming_pipeline,
)
from ematix_flow.udf import (
    PythonScalarUdfHandle,
    apply_udf_to_batch,
    udf,
)

# Read the installed-package version dynamically so a release bump
# only needs to touch Cargo.toml + pyproject.toml — not this file.
# Falls back to the Rust core's compile-time version if package
# metadata isn't available (editable install before
# `maturin develop`, partial install, etc.); both sources are
# driven by `Cargo.toml` and `pyproject.toml`'s version fields.
try:
    from importlib.metadata import PackageNotFoundError as _PackageNotFoundError
    from importlib.metadata import version as _pkg_version

    try:
        __version__ = _pkg_version("ematix-flow")
    except _PackageNotFoundError:
        __version__ = _core.core_version()
    del _pkg_version, _PackageNotFoundError
except ImportError:
    __version__ = _core.core_version()

__all__ = [
    "CDC",
    "Aggregation",
    "Connection",
    "DeltaLocalConnection",
    "DeltaS3Connection",
    "DuckDBConnection",
    "Join",
    "KafkaConnection",
    "KinesisConnection",
    "Lookup",
    "MySQLConnection",
    "ObjectStoreLocalConnection",
    "ObjectStoreS3Connection",
    "PostgresConnection",
    "PythonScalarUdfHandle",
    "PubSubConnection",
    "RabbitMQConnection",
    "SQLiteConnection",
    "SchemaRegistryConnection",
    "Source",
    "StateStore",
    "Target",
    "Watermark",
    "Window",
    "__version__",
    "_core",
    "apply_udf_to_batch",
    "config",
    "connect",
    "connections",
    "ematix",
    "get_connection",
    "natural_key",
    "nullable",
    "pk",
    "register_connection",
    "registered_connections",
    "run_pipeline",
    "run_streaming_pipeline",
    "streaming",
    "udf",
]
