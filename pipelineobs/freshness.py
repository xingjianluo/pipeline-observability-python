"""数据新鲜度与调度滞后判定。

输入缺失或异常时绝不静默套用默认值，而是给出确定且可区分的结论：
- 从未成功产出            => unknown / missing_last_success
- 产出时间戳缺失          => unknown / missing_event_ts
- 未配置调度间隔          => unknown / missing_interval
- 调度间隔 <= 0           => unknown / non_positive_interval
- 评估时间早于产出时间    => unknown / clock_rollback（时钟回拨）
- 调度间隔较上次调小      => unknown / interval_shrunk（需显式确认）

输入正常时（age = evaluated_at - last_success_ts, lag = age - interval）：
- age <= interval                 => fresh
- interval < age <= 2 * interval  => lagging（超出一个调度周期）
- age > 2 * interval              => stale（连续两个周期无产出）
"""
from __future__ import annotations

import threading

from .models import FreshnessResult, FreshnessState, InvalidateReason

_STALE_MULTIPLIER = 2.0


class FreshnessMonitor:
    def __init__(self, lock: threading.RLock | None = None) -> None:
        self._lock = lock or threading.RLock()
        # pipeline_id -> (last_success_ts, last_success_event_ts_present)
        self._last_success: dict[str, tuple[float, bool]] = {}
        # pipeline_id -> 最近一次已确认的调度间隔
        self._intervals: dict[str, float] = {}

    @property
    def lock(self) -> threading.RLock:
        return self._lock

    def record_success(self, pipeline_id: str, event_ts: float | None) -> None:
        """由生命周期成功事件驱动；时间戳可能缺失，需显式记录。"""
        with self._lock:
            if event_ts is None:
                self._last_success[pipeline_id] = (None, False)
            else:
                prev = self._last_success.get(pipeline_id)
                if prev is None or prev[0] is None or event_ts >= prev[0]:
                    self._last_success[pipeline_id] = (event_ts, True)
                else:
                    # 更早的成功时间戳：可能是时钟回拨/乱序，不回退最近产出。
                    self._last_success[pipeline_id] = (prev[0], True)

    def set_interval(self, pipeline_id: str, interval_seconds: float) -> None:
        """设置/确认调度间隔。返回 None；间隔调小会被 evaluate 识别为异常，

        调用方需用 confirm_interval_shrink 显式确认后才按新间隔判定。
        """
        with self._lock:
            self._intervals[pipeline_id] = float(interval_seconds)

    def confirm_interval_shrink(self, pipeline_id: str) -> None:
        """显式确认间隔调小是有意为之，解除 interval_shrunk 阻断。"""
        with self._lock:
            self._confirmed_shrinks.add(pipeline_id)

    # _confirmed_shrinks 延迟初始化以保持构造函数简洁
    @property
    def _confirmed_shrinks(self) -> set[str]:
        if not hasattr(self, "_shrinks"):
            self._shrinks: set[str] = set()
        return self._shrinks

    def evaluate(self, pipeline_id: str, evaluated_at: float,
                 interval_seconds: float | None = None) -> FreshnessResult:
        with self._lock:
            if interval_seconds is not None:
                prev = self._intervals.get(pipeline_id)
                self._intervals[pipeline_id] = float(interval_seconds)
                cur = float(interval_seconds)
            else:
                prev = None
                cur = self._intervals.get(pipeline_id)

            if cur is None:
                return self._invalid(pipeline_id, evaluated_at,
                                     InvalidateReason.MISSING_INTERVAL)
            if cur <= 0:
                return self._invalid(pipeline_id, evaluated_at,
                                     InvalidateReason.NON_POSITIVE_INTERVAL,
                                     interval_seconds=cur)
            if interval_seconds is not None and prev is not None \
                    and cur < prev and pipeline_id not in self._confirmed_shrinks:
                return self._invalid(pipeline_id, evaluated_at,
                                     InvalidateReason.INTERVAL_SHRUNK,
                                     interval_seconds=cur)
            self._confirmed_shrinks.discard(pipeline_id)

            record = self._last_success.get(pipeline_id)
            if record is None:
                return self._invalid(pipeline_id, evaluated_at,
                                     InvalidateReason.MISSING_LAST_SUCCESS,
                                     interval_seconds=cur)
            last_ts, present = record
            if not present or last_ts is None:
                return self._invalid(pipeline_id, evaluated_at,
                                     InvalidateReason.MISSING_EVENT_TS,
                                     interval_seconds=cur)
            if evaluated_at < last_ts:
                return self._invalid(pipeline_id, evaluated_at,
                                     InvalidateReason.CLOCK_ROLLBACK,
                                     last_success_ts=last_ts,
                                     interval_seconds=cur,
                                     age_seconds=evaluated_at - last_ts)

            age = evaluated_at - last_ts
            lag = age - cur
            if age <= cur:
                state, reason = FreshnessState.FRESH, "age_within_interval"
            elif age <= _STALE_MULTIPLIER * cur:
                state, reason = FreshnessState.LAGGING, "age_exceeds_one_interval"
            else:
                state, reason = FreshnessState.STALE, "age_exceeds_two_intervals"

            return FreshnessResult(
                pipeline_id=pipeline_id, state=state, evaluated_at=evaluated_at,
                last_success_ts=last_ts, interval_seconds=cur,
                age_seconds=age, lag_seconds=lag, reason=reason)

    @staticmethod
    def _invalid(pipeline_id: str, evaluated_at: float,
                 reason: InvalidateReason, *,
                 last_success_ts: float | None = None,
                 interval_seconds: float | None = None,
                 age_seconds: float | None = None) -> FreshnessResult:
        return FreshnessResult(
            pipeline_id=pipeline_id, state=FreshnessState.UNKNOWN,
            evaluated_at=evaluated_at, last_success_ts=last_success_ts,
            interval_seconds=interval_seconds, age_seconds=age_seconds,
            lag_seconds=None, reason=reason.value)
