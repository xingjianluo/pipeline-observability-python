"""运行实例生命周期上报与状态收敛。

规则：
1. 去重：同一 (run, attempt, event_type) 的重复上报只计一次。
2. 收敛：运行级状态是“全部尝试”的纯函数，与事件到达顺序无关——
   - 任一尝试 succeeded => 运行 succeeded（终态，永不再变）；
   - 否则以最大 attempt 的尝试状态为准（running / failed）。
3. 乱序：同一尝试的终态一经确定，迟到的冲突终态事件不能将其回退；
   冲突时按事件时间戳决胜，时间戳缺失时按到达顺序决胜。
4. 重试：递增 attempt 表示新尝试，failed 后收到新尝试 started 是合法
   重试（运行重回 running），与原始失败尝试可区分且归属同一 run。
"""
from __future__ import annotations

import threading

from .models import AttemptRecord, EventRecord, EventType, RunEvent, RunState, RunStatus


def canonical_outcome(state: RunState) -> tuple[bool, float, float | None] | None:
    """由运行全部尝试推导顺序无关的终态结论 (succeeded, finished_ts, latency)。

    未进入终态返回 None。成功取最早成功的尝试；失败取最大尝试号的失败尝试。
    批量回补、实时上报与追溯重算共用同一规则，保证结论一致。
    """
    succeeded_attempts = sorted(
        a.attempt for a in state.attempts.values()
        if a.status is RunStatus.SUCCEEDED)
    if succeeded_attempts:
        win = state.attempts[succeeded_attempts[0]]
        return True, win.finished_ts, win.latency
    failed = [a.attempt for a in state.attempts.values()
              if a.status is RunStatus.FAILED]
    if failed:
        win = state.attempts[max(failed)]
        return False, win.finished_ts, win.latency
    return None


class LifecycleManager:
    def __init__(self, lock: threading.RLock | None = None) -> None:
        self._lock = lock or threading.RLock()
        self._runs: dict[tuple[str, str], RunState] = {}
        # 已通过去重、实际进入生命周期的事件留痕，按流水线分桶
        self._journal: dict[str, list[EventRecord]] = {}

    @property
    def lock(self) -> threading.RLock:
        return self._lock

    def journal(self, pipeline_id: str) -> list[EventRecord]:
        """返回该流水线的事件留痕副本（追溯重算的事实来源）。"""
        with self._lock:
            return list(self._journal.get(pipeline_id, ()))

    def ingest(self, event: RunEvent) -> tuple[RunState, str]:
        """提交事件，返回 (最新运行状态, 判定依据)。

        判定依据：first_event / duplicate_event / attempt_started /
        attempt_finished / attempt_terminal_held / retry_started。
        """
        with self._lock:
            key = (event.pipeline_id, event.run_id)
            state = self._runs.get(key)
            if state is None:
                state = RunState(pipeline_id=event.pipeline_id,
                                 run_id=event.run_id)
                self._runs[key] = state
                reason = "first_event"
            else:
                reason = "attempt_updated"

            dedup = f"{event.attempt}:{event.event_type.value}"
            if dedup in state.dedup_keys:
                return state, "duplicate_event"
            state.dedup_keys.add(dedup)

            order_ts = event.event_ts if event.event_ts is not None else event.received_ts
            self._journal.setdefault(event.pipeline_id, []).append(EventRecord(
                pipeline_id=event.pipeline_id, run_id=event.run_id,
                attempt=event.attempt, event_type=event.event_type,
                order_ts=order_ts, ts_present=event.event_ts is not None,
                received_ts=event.received_ts, detail=event.detail))
            if state.last_event_ts is None or order_ts >= state.last_event_ts:
                state.last_event_ts = order_ts
            if state.first_seen_ts is None:
                state.first_seen_ts = event.received_ts

            attempt = state.attempts.get(event.attempt)
            if attempt is None:
                attempt = AttemptRecord(attempt=event.attempt,
                                        status=RunStatus.PENDING)
                state.attempts[event.attempt] = attempt

            old_status = state.status
            applied, apply_reason = self._apply_event(attempt, event)
            self._recompute(state)

            if apply_reason is not None:
                reason = apply_reason
            elif event.event_type is EventType.STARTED \
                    and old_status is RunStatus.FAILED \
                    and state.status is RunStatus.RUNNING:
                reason = "retry_started"
            elif event.event_type is EventType.STARTED:
                reason = "attempt_started"
            elif applied:
                reason = "attempt_finished"
            return state, reason

    @staticmethod
    def _apply_event(attempt: AttemptRecord,
                     event: RunEvent) -> tuple[bool, str | None]:
        """把事件写入尝试记录，返回 (是否实际生效, 判定依据覆盖值)。"""
        ts = event.event_ts if event.event_ts is not None else event.received_ts

        if event.event_type is EventType.STARTED:
            if attempt.status in (RunStatus.SUCCEEDED, RunStatus.FAILED):
                # 终态之后迟到的 started，不能回退。
                return False, "attempt_terminal_held"
            if attempt.started_ts is None or ts < attempt.started_ts:
                attempt.started_ts = ts
            attempt.status = RunStatus.RUNNING
            return True, None

        # 终态事件 succeeded / failed
        terminal = RunStatus.SUCCEEDED if event.event_type is EventType.SUCCEEDED \
            else RunStatus.FAILED
        if attempt.status in (RunStatus.SUCCEEDED, RunStatus.FAILED):
            # 冲突终态：仅当新事件时间更早才覆盖，否则保持原判（不回退）。
            if attempt.finished_ts is not None and ts >= attempt.finished_ts:
                return False, "attempt_terminal_held"
        attempt.finished_ts = ts
        if attempt.started_ts is not None:
            attempt.latency = max(0.0, ts - attempt.started_ts)
        attempt.status = terminal
        if event.detail:
            attempt.detail = event.detail
        return True, None

    @staticmethod
    def _recompute(state: RunState) -> None:
        """由全部尝试重算运行级状态（纯函数，顺序无关）。"""
        if any(a.status is RunStatus.SUCCEEDED for a in state.attempts.values()):
            state.status = RunStatus.SUCCEEDED
            return
        latest = state.attempts[max(state.attempts)]
        state.status = latest.status if latest.status in (
            RunStatus.RUNNING, RunStatus.FAILED) else RunStatus.PENDING

    def get(self, pipeline_id: str, run_id: str) -> RunState | None:
        with self._lock:
            return self._runs.get((pipeline_id, run_id))

    def all_runs(self, pipeline_id: str | None = None) -> list[RunState]:
        with self._lock:
            return [s for s in self._runs.values()
                    if pipeline_id is None or s.pipeline_id == pipeline_id]
