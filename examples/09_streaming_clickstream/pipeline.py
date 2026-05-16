"""Streaming pipeline for demo 09: Kafka `clicks` topic → Postgres
`analytics.clicks`.

The decorator captures every kwarg at definition time and registers
the pipeline by name; `flow consume --module pipeline clicks-to-pg`
imports this module (which fires the decorator), looks the pipeline
up by name, and runs it.

Run:
    flow consume --module pipeline clicks-to-pg
"""

from __future__ import annotations

from ematix_flow import KafkaConnection, PostgresConnection, ematix


KAFKA = KafkaConnection(
    name="kafka",
    bootstrap_servers="localhost:9092",
    group_id="ematix-demo-09",
    payload_format="json",
    # Fresh consumer group with no committed offsets reads from the
    # start of the topic — picks up any events the producer emitted
    # before the consumer subscribed. After the first commit, this is
    # ignored and we resume from the committed position.
    auto_offset_reset="earliest",
)

WAREHOUSE = PostgresConnection(
    name="warehouse",
    url="postgres://postgres:postgres@localhost:5434/postgres",
)


@ematix.streaming_pipeline(
    name="clicks-to-pg",
    source=KAFKA,
    source_query="clicks",
    target=WAREHOUSE,
    target_table=("analytics", "clicks"),
    # Cast the ISO-8601 string event_ts JSON field to a microsecond-
    # precision Arrow timestamp so the Postgres binary-COPY target
    # writes it as a real TIMESTAMP column (instead of choking on
    # "error serializing parameter 0").
    transform_sql=(
        "SELECT seq_id, user_id, url, "
        "arrow_cast(event_ts, 'Timestamp(Microsecond, None)') AS event_ts, "
        "referrer "
        "FROM source"
    ),
    idle_pause_ms=200,
)
def clicks_to_pg() -> None:
    """Drains the `clicks` Kafka topic and INSERTs each batch into
    `analytics.clicks`. At-least-once: source offsets advance only
    after the target write commits."""
