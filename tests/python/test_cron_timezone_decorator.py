"""Task #558 slices 2+3 — ``timezone=`` flows from decorator → registry → API.

Slice 1 (``is_due(tz=)``) is covered in ``test_cron_timezone.py``.

This file covers:
- ``pipeline.register(timezone="...")`` stores the tz on
  :class:`ScheduledPipeline` and rejects unknown IANA names at
  registration time (not at the first scheduler tick).
- ``pipeline.forecast_next_run`` produces the right UTC instant
  for a tz-anchored cron, including across DST.
- The Web UI ``/api/pipelines`` response surfaces ``timezone`` so
  the SPA can render Next-run in local time instead of UTC.
"""
from __future__ import annotations

from datetime import UTC, datetime

import pytest

from ematix_flow import pipeline


@pytest.fixture(autouse=True)
def _reset_registry():
    """Make every test start with an empty pipeline registry — these
    tests register synthetic pipelines and shouldn't leak into each
    other or into other test modules."""
    pipeline._REGISTRY.clear()
    pipeline._DEPENDS_ON.clear()
    pipeline._UPSTREAM_FRESHNESS.clear()
    pipeline._LAST_RUN.clear()
    pipeline._RETRY_POLICY.clear()
    yield
    pipeline._REGISTRY.clear()
    pipeline._DEPENDS_ON.clear()
    pipeline._UPSTREAM_FRESHNESS.clear()
    pipeline._LAST_RUN.clear()
    pipeline._RETRY_POLICY.clear()


class TestRegisterTimezone:
    def test_timezone_defaults_to_none(self):
        @pipeline.register(name="p_default", schedule="0 9 * * *")
        def _fn():
            return {}

        sp = pipeline._REGISTRY["p_default"]
        assert sp.timezone is None

    def test_timezone_stored_on_scheduled_pipeline(self):
        @pipeline.register(
            name="p_ny",
            schedule="0 9 * * *",
            timezone="America/New_York",
        )
        def _fn():
            return {}

        sp = pipeline._REGISTRY["p_ny"]
        assert sp.timezone == "America/New_York"

    def test_unknown_timezone_rejected_at_registration(self):
        with pytest.raises(ValueError, match="invalid timezone"):

            @pipeline.register(
                name="p_bad",
                schedule="0 9 * * *",
                timezone="Mars/Olympus_Mons",
            )
            def _fn():
                return {}

    def test_empty_string_timezone_rejected(self):
        # Empty string is invalid IANA — fail loudly, don't fall
        # through to a silent UTC default.
        with pytest.raises(ValueError, match="invalid timezone"):

            @pipeline.register(name="p_empty", schedule="0 9 * * *", timezone="")
            def _fn():
                return {}


class TestForecastNextRun:
    def test_none_schedule_returns_none(self):
        assert pipeline.forecast_next_run(None) is None

    def test_utc_default(self):
        now = datetime(2026, 5, 21, 8, 30, 0, tzinfo=UTC)
        # "0 9 * * *" daily at 09:00 UTC → next fire is 2026-05-21 09:00 UTC.
        nxt = pipeline.forecast_next_run("0 9 * * *", now=now)
        assert nxt == datetime(2026, 5, 21, 9, 0, 0, tzinfo=UTC)

    def test_tz_anchored_cron_returns_utc_instant(self):
        # "0 9 * * *" anchored in America/New_York. On 2026-05-21 EDT is
        # UTC-04:00, so 09:00 EDT == 13:00 UTC.
        now = datetime(2026, 5, 21, 8, 30, 0, tzinfo=UTC)
        nxt = pipeline.forecast_next_run(
            "0 9 * * *", now=now, timezone="America/New_York",
        )
        assert nxt is not None
        # Returned as a UTC datetime regardless of input tz.
        assert nxt.tzinfo is not None
        assert nxt.utcoffset() == datetime(2026, 1, 1, tzinfo=UTC).utcoffset()
        assert nxt == datetime(2026, 5, 21, 13, 0, 0, tzinfo=UTC)

    def test_tz_anchored_cron_handles_dst_boundary(self):
        # On 2026-11-01 the US falls back from EDT (UTC-04) to EST
        # (UTC-05) at 02:00 local. "0 9 * * *" on 2026-11-02 should
        # fire at 14:00 UTC (09:00 EST), not 13:00 UTC.
        now = datetime(2026, 11, 2, 8, 30, 0, tzinfo=UTC)
        nxt = pipeline.forecast_next_run(
            "0 9 * * *", now=now, timezone="America/New_York",
        )
        assert nxt == datetime(2026, 11, 2, 14, 0, 0, tzinfo=UTC)


class TestApiSurfacesTimezone:
    """The web ``/api/pipelines`` endpoint must include the pipeline's
    timezone so the Svelte UI can localize Next-run rendering."""

    def test_pipelines_endpoint_returns_timezone_field(self):
        # Skip cleanly if fastapi isn't available in the local env.
        fastapi = pytest.importorskip("fastapi")
        TestClient = pytest.importorskip("fastapi.testclient").TestClient
        del fastapi  # imported for the skip check only

        from ematix_flow.run_log.history import InMemoryRunHistory, RunRecord
        from ematix_flow.web.server import create_app

        # Register a pipeline with a tz so the API can pick it up.
        @pipeline.register(
            name="ny_daily",
            schedule="0 9 * * *",
            timezone="America/New_York",
        )
        def _fn():
            return {}

        history = InMemoryRunHistory()
        history.record_run_record(
            RunRecord(
                run_id="01HQ0000000000000000000001",
                pipeline="ny_daily",
                status="succeeded",
                started_at=datetime(2026, 5, 21, 13, 0, 0, tzinfo=UTC),
                finished_at=datetime(2026, 5, 21, 13, 0, 30, tzinfo=UTC),
                attempt=1,
            )
        )

        client = TestClient(create_app(history=history))
        body = client.get("/api/pipelines").json()
        match = next(p for p in body["pipelines"] if p["name"] == "ny_daily")
        assert match["timezone"] == "America/New_York"
        # `next_run_at` is forecast server-side from cron + tz when
        # the run record's extras don't pre-stash it.
        assert match["next_run_at"] is not None
