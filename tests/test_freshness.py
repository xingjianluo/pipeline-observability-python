"""Freshness / lag rules: boundaries, missing timestamps, clock rollback,
schedule interval revisions."""
from __future__ import annotations

import pytest

from observability.models import (
    EventType,
    FreshnessReason,
    FreshnessStatus,
    IngestEventIn,
    PipelineConfigIn,
)
from tests._support import build_service, explain


def succeed(svc, clock, run, ts, ingest=None, eid=None, dur=5):
    if ingest is None:
        ingest = ts
    clock.set_ms(ingest)
    svc.ingest(IngestEventIn(pipeline="p", run_id=run, event_type=EventType.STARTED,
                             attempt=1, ts_ms=ts - dur, event_id=f"{eid or run}-s"))
    out = svc.ingest(IngestEventIn(pipeline="p", run_id=run, event_type=EventType.SUCCEEDED,
                                   attempt=1, ts_ms=ts, event_id=eid or run, duration_ms=dur))
    return out


def check(svc, clock, now, title):
    clock.set_ms(now)
    out = svc.freshness("p")
    explain(title, {"now_ms": now}, out)
    return out


@pytest.mark.parametrize("elapsed,expected", [
    (0, FreshnessStatus.FRESH),
    (999, FreshnessStatus.FRESH),
    (1000, FreshnessStatus.FRESH),          # elapsed == interval
    (1001, FreshnessStatus.STALE),
    (1200, FreshnessStatus.STALE),          # elapsed == interval + grace
    (1201, FreshnessStatus.LAGGED),
])
def test_fresh_stale_lagged_boundaries(elapsed, expected):
    svc, clock = build_service(start_ms=0)  # interval 1000, grace 200
    succeed(svc, clock, "r1", ts=10_000, ingest=10_000, eid="e1")
    out = check(svc, clock, 10_000 + elapsed, f"elapsed={elapsed}")
    assert out.status == expected
    assert out.elapsed_ms == elapsed
    reason_map = {
        FreshnessStatus.FRESH: FreshnessReason.WITHIN_INTERVAL,
        FreshnessStatus.STALE: FreshnessReason.BEYOND_INTERVAL_WITHIN_GRACE,
        FreshnessStatus.LAGGED: FreshnessReason.BEYOND_GRACE,
    }
    assert out.reason == reason_map[expected]


def test_no_signal_before_any_success():
    svc, clock = build_service(start_ms=0)
    out = check(svc, clock, 5_000, "no-success-yet")
    assert out.status == FreshnessStatus.NO_SIGNAL
    assert out.reason == FreshnessReason.NO_SUCCESS_OUTPUT


def test_missing_timestamp_is_distinguishable_and_not_defaulted():
    svc, clock = build_service(start_ms=0)
    clock.set_ms(10_000)
    svc.ingest(IngestEventIn(pipeline="p", run_id="r", event_type=EventType.SUCCEEDED,
                             attempt=1, ts_ms=None, event_id="no-ts"))
    out = check(svc, clock, 50_000, "missing-ts")
    assert out.status == FreshnessStatus.MISSING_TIMESTAMP
    assert out.reason == FreshnessReason.LAST_SUCCESS_TIMESTAMP_MISSING
    assert out.last_success_ts_ms is None
    # a later success carrying a real timestamp restores evaluation
    succeed(svc, clock, "r2", ts=49_000, ingest=49_000, eid="with-ts")
    out = check(svc, clock, 49_500, "recovered-ts")
    assert out.status == FreshnessStatus.FRESH


def test_client_clock_rollback_is_flagged_and_monotonic_value_kept():
    svc, clock = build_service(start_ms=0)
    succeed(svc, clock, "r1", ts=10_000, ingest=10_000, eid="e1")
    succeed(svc, clock, "r2", ts=9_000, ingest=11_000, eid="e2")  # ts went backwards
    out = check(svc, clock, 12_000, "client-rollback")
    assert out.status == FreshnessStatus.CLOCK_ROLLBACK
    assert out.reason == FreshnessReason.CLOCK_ROLLBACK_DETECTED
    assert out.last_success_ts_ms == 10_000  # never silently decreased
    assert "9000" in out.detail and "10000" in out.detail


def test_server_clock_rollback_is_flagged():
    svc, clock = build_service(start_ms=0)
    succeed(svc, clock, "r1", ts=10_000, ingest=10_000, eid="e1")
    out = check(svc, clock, 5_000, "server-clock-rollback")  # now before ingest time
    assert out.status == FreshnessStatus.CLOCK_ROLLBACK
    assert "server clock moved backwards" in out.detail


def test_schedule_interval_change_uses_revision_without_silent_default():
    svc, clock = build_service(start_ms=0)
    succeed(svc, clock, "r1", ts=10_000, ingest=10_000, eid="e1")
    out = check(svc, clock, 11_100, "interval-v1")
    assert out.status == FreshnessStatus.STALE  # 1100 > 1000, within 1200
    cfg = svc.register_pipeline("p", PipelineConfigIn(
        schedule_interval_ms=5_000, grace_ms=500, failure_alert_threshold=3))
    explain("interval-revision", {"new_interval_ms": 5_000}, cfg)
    assert cfg.revision == 2 and cfg.schedule_interval_ms == 5_000
    # under the new revision the same data is judged with interval=5000
    out = check(svc, clock, 11_100, "interval-v2-now11100")
    assert out.status == FreshnessStatus.FRESH  # elapsed 1100 <= 5000
    assert out.schedule_interval_ms == 5_000
    out = check(svc, clock, 16_000, "interval-v2-now16000")
    assert out.status == FreshnessStatus.LAGGED  # elapsed 6000 > 5000+500
