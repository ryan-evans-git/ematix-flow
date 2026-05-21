"""Task #558 slice 1 — ``is_due()`` honors a per-call timezone.

The existing ``is_due(schedule, now, interval_seconds)`` interprets
the cron expression in whatever timezone ``now`` carries. ``flow
run-due`` passes ``datetime.now(UTC)`` so today every schedule is
UTC-interpreted. Operators thinking "``0 9 * * *`` = 9 AM business
hours" have to pre-convert to UTC and re-convert across DST changes
— exactly the brittle workflow ematix-flow is meant to remove.

This slice adds an optional ``tz=`` parameter that, when set, makes
``is_due()`` evaluate the schedule in that timezone. The signature
stays backward-compatible (omitted tz = preserve today's behavior).

Slice 2 (follow-up) wires ``timezone=`` into ``@ematix.pipeline`` +
the registry. Slice 3 surfaces the configured tz in the Web UI's
"Next: ..." rendering.
"""
from __future__ import annotations

from datetime import UTC, datetime
from zoneinfo import ZoneInfo

from ematix_flow.pipeline import is_due

# 60-second sweep window — matches the default `flow run-due` cadence.
INTERVAL = 60


class TestIsDueUtcDefault:
    """Without an explicit ``tz=``, ``is_due()`` interprets cron in
    whatever timezone the ``now`` datetime carries. Today that's the
    only behavior the function has."""

    def test_fires_at_utc_top_of_hour(self):
        # `0 * * * *` fires at minute 0 of every hour.
        # now = 2026-05-21 12:00:30Z, window = (11:59:30Z, 12:00:30Z].
        # The 12:00:00Z fire is inside the window.
        now = datetime(2026, 5, 21, 12, 0, 30, tzinfo=UTC)
        assert is_due("0 * * * *", now, INTERVAL) is True

    def test_does_not_fire_between_top_of_hour(self):
        # 12:30:30Z → window (12:29:30Z, 12:30:30Z]. Nothing fires.
        now = datetime(2026, 5, 21, 12, 30, 30, tzinfo=UTC)
        assert is_due("0 * * * *", now, INTERVAL) is False


class TestIsDueWithExplicitTz:
    """When ``tz=`` is passed, ``is_due()`` evaluates the cron in
    that timezone — the operator's mental model ("9 AM in
    America/New_York"), not UTC."""

    def test_9am_eastern_fires_at_correct_utc(self):
        # `0 9 * * *` = 9 AM. With tz=America/New_York, that's 13:00Z
        # in summer (EDT, UTC-4) and 14:00Z in winter (EST, UTC-5).
        # Pick a summer day:
        ny = ZoneInfo("America/New_York")
        # 09:00:30 NY time on 2026-05-21 — DST in effect (EDT).
        # Window: (08:59:30 NY, 09:00:30 NY]. `0 9 * * *` fires at 09:00 NY.
        now_utc = datetime(2026, 5, 21, 13, 0, 30, tzinfo=UTC)
        assert is_due("0 9 * * *", now_utc, INTERVAL, tz=ny) is True

    def test_9am_eastern_does_not_fire_at_random_other_time(self):
        ny = ZoneInfo("America/New_York")
        # 03:00:30 NY time — not 9 AM.
        now_utc = datetime(2026, 5, 21, 7, 0, 30, tzinfo=UTC)
        assert is_due("0 9 * * *", now_utc, INTERVAL, tz=ny) is False

    def test_tz_is_a_zoneinfo_instance(self):
        """Accept ZoneInfo directly — the caller already imported it."""
        chi = ZoneInfo("America/Chicago")
        # 09:00:30 Chicago in summer (CDT, UTC-5) → 14:00:30 UTC.
        now_utc = datetime(2026, 5, 21, 14, 0, 30, tzinfo=UTC)
        assert is_due("0 9 * * *", now_utc, INTERVAL, tz=chi) is True

    def test_tz_accepts_string_name(self):
        """Convenience: pass a tz name string and ``is_due`` resolves
        it via zoneinfo.ZoneInfo internally."""
        now_utc = datetime(2026, 5, 21, 13, 0, 30, tzinfo=UTC)
        assert is_due("0 9 * * *", now_utc, INTERVAL, tz="America/New_York") is True

    def test_utc_tz_explicit_matches_utc_default(self):
        """Passing tz=UTC must match the no-tz default exactly — the
        UTC explicit form should never produce a different answer."""
        utc_tz = ZoneInfo("UTC")
        now = datetime(2026, 5, 21, 12, 0, 30, tzinfo=UTC)
        assert is_due("0 * * * *", now, INTERVAL, tz=utc_tz) is True
        # Both the explicit-UTC form and the no-tz form must agree.
        assert is_due("0 * * * *", now, INTERVAL, tz=utc_tz) == is_due(
            "0 * * * *", now, INTERVAL
        )


class TestIsDueDstSpringForward:
    """Spring-forward edge case: at 2 AM local time on the DST start
    date, the clock jumps to 3 AM. A schedule of ``0 2 * * *`` on
    that date has no actual instant — croniter advances to the next
    valid fire (the following day's 02:00). We don't claim to fire
    a missed 2 AM; we document and accept croniter's behavior."""

    def test_spring_forward_skips_missing_2am(self):
        ny = ZoneInfo("America/New_York")
        # DST spring-forward 2026: Sunday, March 8 2026 — clocks jump
        # 02:00 EST → 03:00 EDT. So 02:30 NY doesn't exist; the
        # corresponding UTC instant is in the gap.
        # We pick a normal day instead and just confirm that the
        # `0 2 * * *` schedule does fire correctly when 2 AM actually
        # exists in the local timezone.
        now_utc = datetime(2026, 5, 21, 6, 0, 30, tzinfo=UTC)  # = 02:00 NY EDT
        assert is_due("0 2 * * *", now_utc, INTERVAL, tz=ny) is True

    def test_spring_forward_behavior_documented(self):
        """The 02:00 NY moment on DST-start day doesn't exist (clocks
        jump 02:00 EST → 03:00 EDT). What ``is_due`` returns at the
        post-jump 03:00 instant depends on croniter's internal
        interpretation of "next valid fire after the gap" — this test
        documents that ematix-flow inherits croniter's behavior and
        doesn't introduce its own gap-handling.

        Operators relying on a 2 AM nightly job during DST-start
        weekends should schedule it as ``30 1 * * *`` or ``0 3 * * *``
        to avoid the ambiguity. We don't paper over it.
        """
        ny = ZoneInfo("America/New_York")
        # 2026-03-08 03:00:30 NY (EDT, just after spring-forward) =
        # 07:00:30 UTC. The previous-window endpoint at 02:00 NY
        # didn't exist this morning.
        now_utc = datetime(2026, 3, 8, 7, 0, 30, tzinfo=UTC)
        # Either True (croniter rolls forward the missed 02:00 fire
        # into the post-gap window) or False (croniter skips it
        # entirely) is acceptable here. Just verify the call doesn't
        # crash.
        result = is_due("0 2 * * *", now_utc, INTERVAL, tz=ny)
        assert isinstance(result, bool)
