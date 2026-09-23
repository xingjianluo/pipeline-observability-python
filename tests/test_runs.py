"""Lifecycle ingestion: duplicates, out-of-order delivery, retries, alerts."""
from __future__ import annotations

from observability.models import (
    EventType,
    IngestEventIn,
    IngestionReason,
    RunStatus,
)
from tests._support import build_service, explain


def send(svc, run, att, et, ts, eid=None, dur=None, err=None, t=""):
    payload = {
        "pipeline": "p", "run_id": run, "attempt": att,
        "event_type": et.value if isinstance(et, EventType) else et,
        "ts_ms": ts, "event_id": eid, "duration_ms": dur, "error": err,
    }
    event = IngestEventIn(**payload)
    out = svc.ingest(event)
    explain(t or f"{run}/a{att}/{et}", payload, out)
    return out


def test_duplicate_event_id_is_counted_once():
    svc, clock = build_service()
    r1 = send(svc, "r", 1, EventType.STARTED, 1000, eid="e1", t="start#1")
    r2 = send(svc, "r", 1, EventType.STARTED, 1001, eid="e1", t="start#1-resend-same-id")
    assert r1.accepted and r1.reason == IngestionReason.ACCEPTED
    assert not r2.accepted and r2.reason == IngestionReason.DUPLICATE_EVENT
    assert r2.run_status == RunStatus.RUNNING
    assert svc.stats("p", 0, 10**9).total_finished == 0


def test_duplicate_event_without_id_is_semantically_deduped():
    svc, _ = build_service()
    send(svc, "r", 1, EventType.STARTED, 1000, t="start")
    late = send(svc, "r", 1, EventType.STARTED, 9999, t="start-resend-no-id")
    assert late.reason == IngestionReason.DUPLICATE_LATE_EVENT
    ok = send(svc, "r", 1, EventType.SUCCEEDED, 1010, dur=10, t="success")
    dup_ok = send(svc, "r", 1, EventType.SUCCEEDED, 1010, dur=10, t="success-resend")
    assert ok.accepted and ok.run_status == RunStatus.SUCCEEDED
    assert dup_ok.reason == IngestionReason.DUPLICATE_LATE_EVENT
    w = svc.stats("p", 0, 10**9)
    assert w.total_finished == 1 and w.succeeded == 1


def test_terminal_attempt_never_regresses():
    svc, _ = build_service()
    send(svc, "r", 1, EventType.STARTED, 1000, t="start")
    send(svc, "r", 1, EventType.FAILED, 1020, err="boom", t="failed")
    # a success arriving late for the same attempt must not flip the outcome
    late_success = send(svc, "r", 1, EventType.SUCCEEDED, 1030, t="late-success")
    assert late_success.reason == IngestionReason.ATTEMPT_TERMINAL
    assert late_success.run_status == RunStatus.FAILED
    w = svc.stats("p", 0, 10**9)
    assert (w.total_finished, w.succeeded, w.failed) == (1, 0, 1)


def test_retry_is_distinct_attempt_but_same_run_and_out_of_order_events_ignored():
    svc, clock = build_service(failure_alert_threshold=5)
    send(svc, "r", 1, EventType.STARTED, 1000, eid="s1", t="a1-start")
    send(svc, "r", 1, EventType.FAILED, 1010, eid="f1", t="a1-failed")
    # retry belongs to the same run, brings run back to running
    r2 = send(svc, "r", 2, EventType.STARTED, 1020, eid="s2", t="a2-start")
    assert r2.accepted and r2.attempts == 2 and r2.run_status == RunStatus.RUNNING
    # late finish of the superseded attempt 1 must not count again nor revert
    stale = send(svc, "r", 1, EventType.SUCCEEDED, 1005, eid="late", t="a1-late-success")
    assert stale.reason == IngestionReason.OUT_OF_ORDER_SUPERSEDED
    assert stale.run_status == RunStatus.RUNNING
    done = send(svc, "r", 2, EventType.SUCCEEDED, 1040, eid="ok2", dur=20, t="a2-success")
    assert done.run_status == RunStatus.SUCCEEDED
    w = svc.stats("p", 0, 10**9)
    # two attempt finishes, one run: failure and success each counted once
    assert (w.total_finished, w.succeeded, w.failed) == (2, 1, 1)
    # run-level state still resolves to the highest attempt
    assert svc.store.get_run("p", "r").status == RunStatus.SUCCEEDED


def test_unknown_pipeline_reports_distinguishable_reason():
    svc, _ = build_service()
    out = send(svc, "r", 1, EventType.STARTED, 1000, t="unknown")
    # helper hardcodes pipeline p; craft a real unknown call
    out = svc.ingest(IngestEventIn(pipeline="ghost", run_id="r",
                                   event_type=EventType.STARTED, attempt=1, ts_ms=1000))
    explain("unknown-pipeline", {"pipeline": "ghost"}, out)
    assert not out.accepted and out.reason == IngestionReason.UNKNOWN_PIPELINE


def test_alert_fires_once_then_suppressed_and_rearms_after_success():
    svc, clock = build_service(failure_alert_threshold=3)
    results = []
    for i in range(1, 6):
        clock.set_ms(2000 + i * 10)
        results.append(send(svc, f"r{i}", 1, EventType.FAILED,
                            2000 + i * 10, eid=f"f{i}", t=f"failure#{i}"))
    alerts = [r.alert for r in results if r.alert is not None]
    fired = [a for a in alerts if not a.suppressed]
    suppressed = [a for a in alerts if a.suppressed]
    assert len(fired) == 1 and fired[0].failure_streak == 3
    assert len(suppressed) == 2  # failures 4 and 5 while already firing
    assert len(svc.alerts("p")) == 1
    # a success recovers the streak; three new failures re-arm exactly once
    clock.set_ms(3000)
    send(svc, "ok", 1, EventType.SUCCEEDED, 3000, eid="rec", t="recovery-success")
    assert svc.store.get_pipeline_record("p").failure_streak == 0
    for i in range(3):
        clock.set_ms(3100 + i * 10)
        out = send(svc, f"g{i}", 1, EventType.FAILED, 3100 + i * 10, eid=f"g{i}", t="post-recovery-fail")
    assert out.alert is not None and not out.alert.suppressed
    assert len(svc.alerts("p")) == 2
