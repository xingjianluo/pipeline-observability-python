"""Window aggregation: boundaries, latency stats and concurrent ingestion."""
from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

from observability.models import EventType, IngestEventIn
from tests._support import build_service, explain


def finish(svc, run, attempt, ts, status, dur, eid, ingest=None):
    # Bucketing uses server-side ingestion time; tests move the clock there.
    if ingest is not None:
        svc.clock.set_ms(ingest)
    svc.ingest(IngestEventIn(pipeline="p", run_id=run, event_type=EventType.STARTED,
                             attempt=attempt, ts_ms=ts - dur, event_id=f"{eid}-s"))
    if ingest is not None:
        svc.clock.set_ms(ingest)
    out = svc.ingest(IngestEventIn(pipeline="p", run_id=run, event_type=status,
                                   attempt=attempt, ts_ms=ts, event_id=eid, duration_ms=dur))
    return out


def test_window_is_half_open_and_boundaries_are_deterministic():
    svc, clock = build_service(start_ms=0)
    # finishes land on server ingestion time 100 / 200 / 300
    finish(svc, "r1", 1, 100, EventType.SUCCEEDED, 10, "a", ingest=100)
    finish(svc, "r2", 1, 200, EventType.FAILED, 20, "b", ingest=200)
    finish(svc, "r3", 1, 300, EventType.SUCCEEDED, 30, "c", ingest=300)

    cases = [
        ((100, 300), 2),   # 100 inclusive, 300 exclusive
        ((0, 100), 0),
        ((100, 200), 1),
        ((200, 301), 2),
        ((300, 301), 1),
        ((0, 1000), 3),
    ]
    for (lo, hi), expected in cases:
        w = svc.stats("p", lo, hi)
        explain(f"window[{lo},{hi})", {"window": [lo, hi]}, w)
        assert w.total_finished == expected

    w = svc.stats("p", 0, 1000)
    assert (w.succeeded, w.failed) == (2, 1)
    assert abs(w.success_rate - 2 / 3) < 1e-9
    assert w.avg_duration_ms == 20.0
    assert w.p95_duration_ms == 30


def test_retry_attempts_are_separate_outcomes_in_windows():
    svc, clock = build_service(start_ms=0)
    finish(svc, "r", 1, 100, EventType.FAILED, 5, "r-a1", ingest=100)
    finish(svc, "r", 2, 200, EventType.SUCCEEDED, 7, "r-a2", ingest=200)
    w = svc.stats("p", 0, 1000)
    explain("retry-outcomes", {"run_id": "r"}, w)
    assert w.total_finished == 2 and w.succeeded == 1 and w.failed == 1


def test_concurrent_reports_are_counted_exactly_once():
    svc, clock = build_service(start_ms=0)
    n_per_thread = 200
    n_threads = 8

    def worker(tid: int) -> None:
        for i in range(n_per_thread):
            run = f"t{tid}-r{i}"
            et = EventType.FAILED if i % 2 else EventType.SUCCEEDED
            # no event_id on success: semantic (attempt,type) dedupe still holds
            finish(svc, run, 1, 100 + tid, et, i + 1, f"{tid}-{i}")

    with ThreadPoolExecutor(max_workers=n_threads) as pool:
        list(pool.map(worker, range(n_threads)))

    total = n_threads * n_per_thread
    w = svc.stats("p", 0, 10**9)
    explain("concurrent-total", {"threads": n_threads, "per_thread": n_per_thread}, w)
    assert w.total_finished == total
    assert w.succeeded + w.failed == total
    assert w.succeeded == total // 2
    assert len(svc.store.get_pipeline_record("p").outcomes) == total


def test_concurrent_duplicate_retries_collapse_to_one_outcome():
    svc, clock = build_service(start_ms=0)
    barrier = threading.Barrier(8)

    def worker(_: int) -> None:
        barrier.wait()
        # every thread replays the identical finish for the same run/attempt
        svc.ingest(IngestEventIn(pipeline="p", run_id="hot", event_type=EventType.STARTED,
                                 attempt=1, ts_ms=10, event_id="hot-s"))
        svc.ingest(IngestEventIn(pipeline="p", run_id="hot", event_type=EventType.SUCCEEDED,
                                 attempt=1, ts_ms=20, event_id="hot-f", duration_ms=10))

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(worker, range(8)))

    w = svc.stats("p", 0, 10**9)
    explain("concurrent-duplicates", {"threads": 8, "event_id": "hot-f"}, w)
    assert w.total_finished == 1
    assert w.succeeded == 1
