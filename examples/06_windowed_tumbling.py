"""Tumbling-window aggregation: count events per user per minute.

Shape: Kafka source -> SQL pre-stage projects the columns we need ->
1-minute tumbling window aggregates per `user_id` -> SQLite target
absorbs the per-window rows.

This example is Python-driven (single source, no join), so the
typed-connection runner handles it directly. For multi-source
shapes (joins) see `08_stream_join.toml`.

Requires:
  - Local Kafka with the `events` topic carrying JSON like
    {"user_id": 1, "amount": 100, "_event_ts": "2026-05-01T..."}
  - Pure-Python: writes to ./example_06.db (SQLite).

Run:
    python examples/06_windowed_tumbling.py
"""

from ematix_flow import (
    Aggregation,
    KafkaConnection,
    SQLiteConnection,
    Window,
    register_connection,
    run_streaming_pipeline,
)


def main() -> None:
    src = KafkaConnection(
        name="kafka",
        bootstrap_servers="localhost:9092",
        group_id="ematix-flow-example-06",
    )
    tgt = SQLiteConnection(name="local", path="example_06.db")
    register_connection(src)
    register_connection(tgt)

    run_streaming_pipeline(
        name="events-per-min",
        source=src,
        source_query="events",
        target=tgt,
        target_table=("main", "events_per_min"),
        # Pre-stage projects + casts the event-time column into the
        # microsecond-precision timestamp the windowed transform
        # requires. arrow_cast is provided by DataFusion.
        transform_sql=(
            "SELECT user_id, amount, "
            "       arrow_cast(_event_ts, 'Timestamp(Microsecond, None)') AS _event_ts "
            "FROM source"
        ),
        window=Window(
            kind="tumbling",
            duration_ms=60_000,
            group_by=("user_id",),
            max_groups_per_window=1_000_000,
            aggregations=[
                Aggregation(agg="count", as_="n"),
                Aggregation(agg="sum",   column="amount", as_="amount_sum"),
                Aggregation(agg="avg",   column="amount", as_="amount_avg"),
            ],
        ),
        metrics_port=9100,  # serves Prometheus /metrics on :9100
    )


if __name__ == "__main__":
    main()
