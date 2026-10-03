"""历史事件批量回补：逐条走与单条上报完全相同的处理路径。

语义保证：
- 每条事件都有自己的处理结论（accepted / duplicate / invalid + 原因）；
- 合法事件一律调用 store.apply_event —— 与 POST /events 单条上报同一代码路径，
  因此整批结果 == 把这些事件逐条单独上报；
- 坏数据只影响自己：invalid 事件不进状态机，其余事件照常生效；
- 逐条获取同一把锁而非整批长事务：单条处理与实时上报完全互斥，
  但读请求与其他上报可在事件之间穿插，几千条的批次也不会长时间
  卡住其他请求；
- 单条处理顺序无关且幂等（规范终态 + 去重），因此批次与实时上报并发时
  最终结论与任意逐条顺序一致；重复提交同一批不多算、不重复告警。
"""
from __future__ import annotations

import logging
import math

from .models import (
    BackfillItem,
    BackfillItemResult,
    BackfillItemStatus,
    BackfillResult,
    EventType,
    RunEvent,
)

logger = logging.getLogger("pipelineobs.backfill")

_VALID_EVENT_TYPES = {e.value for e in EventType}

# 去重/终态保持类原因：事件合法，但没有产生新的状态或计数变化
_NO_EFFECT_REASONS = {"duplicate_event", "attempt_terminal_held"}


def _is_real_number(v: object) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool) \
        and not (isinstance(v, float) and (math.isnan(v) or math.isinf(v)))


def validate_item(item: BackfillItem) -> tuple[dict, str | None]:
    """校验并归一化一条事件。返回 (归一化字段, 不合法原因)；合法原因为 None。"""
    pid = item.pipeline_id
    rid = item.run_id
    if not isinstance(pid, str) or not pid:
        return {}, "missing_pipeline_id"
    if not isinstance(rid, str) or not rid:
        return {}, "missing_run_id"
    if not isinstance(item.event_type, str) \
            or item.event_type not in _VALID_EVENT_TYPES:
        return {}, "invalid_event_type"
    if not isinstance(item.attempt, int) or isinstance(item.attempt, bool) \
            or item.attempt < 1:
        return {}, "invalid_attempt"
    if item.event_ts is not None and not _is_real_number(item.event_ts):
        return {}, "invalid_event_ts"
    if item.received_ts is not None and not _is_real_number(item.received_ts):
        return {}, "invalid_received_ts"
    if not isinstance(item.detail, str):
        return {}, "invalid_detail"
    norm = {
        "pipeline_id": pid, "run_id": rid, "attempt": item.attempt,
        "event_type": item.event_type,
        "event_ts": float(item.event_ts) if item.event_ts is not None else None,
        "detail": item.detail,
        "received_ts": (float(item.received_ts)
                        if item.received_ts is not None else None),
    }
    return norm, None


class Backfiller:
    def __init__(self, store) -> None:
        self._store = store

    def ingest_batch(self, items: list[BackfillItem]) -> BackfillResult:
        results: list[BackfillItemResult] = []
        accepted = duplicate = invalid = 0
        # 逐条加锁：与实时上报共用同一把锁，保证“单条处理”原子；
        # 不在整批外持有锁，使读请求与其他上报能在事件之间穿插。
        for index, item in enumerate(items):
            with self._store.lock:
                norm, bad = validate_item(item)
                if bad is not None:
                    invalid += 1
                    results.append(BackfillItemResult(
                        index=index, status=BackfillItemStatus.INVALID,
                        reason=bad))
                    logger.info(
                        "[backfill] idx=%d INVALID input pipeline=%s run=%s "
                        "attempt=%s event=%s ts=%s reason=%s",
                        index, item.pipeline_id, item.run_id, item.attempt,
                        item.event_type, item.event_ts, bad)
                    continue

                recv = norm["received_ts"]
                if recv is None:
                    # 历史回补默认以事件时间作为接收时间，缺失时由时钟补齐
                    recv = norm["event_ts"] if norm["event_ts"] is not None \
                        else self._store._clock()
                event = RunEvent(
                    pipeline_id=norm["pipeline_id"],
                    run_id=norm["run_id"],
                    attempt=norm["attempt"],
                    event_type=EventType(norm["event_type"]),
                    event_ts=norm["event_ts"], received_ts=recv,
                    detail=norm["detail"])
                r = self._store.apply_event(event)
                if r.lifecycle_reason in _NO_EFFECT_REASONS:
                    duplicate += 1
                    status = BackfillItemStatus.DUPLICATE
                else:
                    accepted += 1
                    status = BackfillItemStatus.ACCEPTED
                results.append(BackfillItemResult(
                    index=index, status=status,
                    run_status=r.run.status.value,
                    lifecycle_reason=r.lifecycle_reason,
                    aggregate_reason=r.aggregate_reason))
                logger.info(
                    "[backfill] idx=%d %s input pipeline=%s run=%s "
                    "attempt=%s event=%s ts=%s => run_status=%s "
                    "lifecycle=%s aggregate=%s",
                    index, status.value, norm["pipeline_id"], norm["run_id"],
                    norm["attempt"], norm["event_type"], norm["event_ts"],
                    r.run.status.value, r.lifecycle_reason,
                    r.aggregate_reason)
        result = BackfillResult(
            total=len(items), accepted=accepted, duplicate=duplicate,
            invalid=invalid, results=results)
        logger.info(
            "[backfill] batch done total=%d accepted=%d duplicate=%d "
            "invalid=%d", result.total, result.accepted, result.duplicate,
            result.invalid)
        return result
