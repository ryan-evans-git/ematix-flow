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
    SQLiteConnection,
    get_connection,
    register_connection,
    registered_connections,
)
from ematix_flow.decorators import ematix
from ematix_flow.markers import natural_key, nullable, pk
from ematix_flow.streaming import Target, run_pipeline, run_streaming_pipeline

__version__ = "0.1.0"

__all__ = [
    "__version__",
    "_core",
    "config",
    "connect",
    "Connection",
    "connections",
    "DeltaLocalConnection",
    "DeltaS3Connection",
    "DuckDBConnection",
    "ematix",
    "get_connection",
    "KafkaConnection",
    "KinesisConnection",
    "MySQLConnection",
    "natural_key",
    "nullable",
    "ObjectStoreLocalConnection",
    "ObjectStoreS3Connection",
    "pk",
    "PostgresConnection",
    "PubSubConnection",
    "RabbitMQConnection",
    "register_connection",
    "registered_connections",
    "run_pipeline",
    "run_streaming_pipeline",
    "SQLiteConnection",
    "streaming",
    "Target",
]
