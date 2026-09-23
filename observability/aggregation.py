"""Fixed-window aggregation and alert listing.

Outcomes are bucketed by the *server-side ingestion time*
(``finish_ingest_ms``) so that client clock skew, out-of-order reports and
duplicate retries cannot move a finish between windows. Each terminal attempt
produces exactly one outcome (see runs.py), hence counts are exact: no
duplicates, no omissions.

Windows are half-open: ``window_start_ms <= t < window_end_ms``.
"""
from __future__ import annotations

from observability.models import (
    Alert,
    RunStatus,
    WindowStatsOut,
)
from observability.store import Store


def percentile_nearest_rank(sorted_values: list[int], pct: float) -> int | None:
    if not sorted_values:
        return None
    n = len(sorted_values)
    rank = max(1, min(n, (pct / 100.0) * n))
    # nearest-rank method: ceil(rank), 1-based
    import math

    idx = min(n - 1, max(0, math.ceil(rank) - 1))
    return sorted_values[idx]


def window_stats(
    store: Store,
    pipeline: str,
    window_start_ms: int,
    window_end_ms: int,
) -> WindowStatsOut:
    if window_end_ms <= window_start_ms:
        raise ValueError("window_end_ms must be greater than window_start_ms")

    prec = store.get_pipeline_record(pipeline)
    if prec is None:
        return WindowStatsOut(
            pipeline=pipeline,
            window_start_ms=window_start_ms,
            window_end_ms=window_end_ms,
            total_finished=0,
            succeeded=0,
            failed=0,
        )

    with store.lock:
        snapshot = list(prec.outcomes)

    selected = [
        o
        for o in snapshot
        if window_start_ms <= o.finish_ingest_ms < window_end_ms
    ]
    # deterministic ordering even though filtering is order independent
    selected.sort(key=lambda o: (o.finish_ingest_ms, o.seq))

    total = len(selected)
    succeeded = sum(1 for o in selected if o.status is RunStatus.SUCCEEDED)
    failed = total - succeeded
    durations = sorted(o.duration_ms for o in selected if o.duration_ms is not None)

    success_rate = (succeeded / total) if total else None
    avg_duration = (sum(durations) / len(durations)) if durations else None
    p95 = percentile_nearest_rank(durations, 95)

    return WindowStatsOut(
        pipeline=pipeline,
        window_start_ms=window_start_ms,
        window_end_ms=window_end_ms,
        total_finished=total,
        succeeded=succeeded,
        failed=failed,
        success_rate=success_rate,
        avg_duration_ms=avg_duration,
        p95_duration_ms=p95,
    )


def list_alerts(store: Store, pipeline: str | None = None) -> list[Alert]:
    with store.lock:
        if pipeline is not None:
            prec = store.get_pipeline_record(pipeline)
            return list(prec.alerts) if prec is not None else []
        result: list[Alert] = []
        for prec in store.pipelines.values():
            result.extend(prec.alerts)
        result.sort(key=lambda a: (a.triggered_at_ms, a.pipeline))
        return result
