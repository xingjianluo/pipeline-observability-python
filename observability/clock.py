"""Clock abstraction so tests can control time deterministically.

All domain time is epoch milliseconds (int). Reports may carry arbitrary
client timestamps, but all *decisions* use the injected clock, which keeps
behavior reproducible in tests and makes clock-rollback scenarios explicit.
"""
from __future__ import annotations

import itertools
import time
from typing import Protocol


class Clock(Protocol):
    def now_ms(self) -> int: ...


class SystemClock:
    def now_ms(self) -> int:
        return time.time_ns() // 1_000_000


class ManualClock:
    """Deterministic clock for tests; callers may move it backwards to
    simulate clock rollback."""

    def __init__(self, initial_ms: int = 0) -> None:
        self._now = int(initial_ms)

    def now_ms(self) -> int:
        return self._now

    def set_ms(self, value_ms: int) -> None:
        self._now = int(value_ms)

    def advance_ms(self, delta_ms: int) -> int:
        self._now += int(delta_ms)
        return self._now


# Process-local monotonic tie-breaker: when several events/finishes share the
# same millisecond timestamp, ingestion order is still total and deterministic
# within a process.
_order_counter = itertools.count(1)


def monotonic_event_order() -> int:
    return next(_order_counter)
