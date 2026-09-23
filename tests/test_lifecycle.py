"""生命周期：重复上报、乱序上报、重试尝试、时间戳缺失。"""
import logging

from pipelineobs.lifecycle import LifecycleManager
from pipelineobs.models import EventType, RunEvent, RunStatus

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("test.lifecycle")


def ev(run, attempt, etype, ts, recv=None, detail=""):
    return RunEvent(
        pipeline_id="p", run_id=run, attempt=attempt,
        event_type=EventType(etype), event_ts=ts,
        received_ts=recv if recv is not None else (ts if ts is not None else 0),
        detail=detail)


def submit(lm, event):
    state, reason = lm.ingest(event)
    log.info("INPUT run=%s attempt=%s event=%s ts=%s recv=%s "
             "=> DECISION status=%s reason=%s attempts=%s",
             event.run_id, event.attempt, event.event_type.value,
             event.event_ts, event.received_ts, state.status.value, reason,
             {n: a.status.value for n, a in sorted(state.attempts.items())})
    return state, reason


def test_duplicate_event_not_double_counted():
    lm = LifecycleManager()
    submit(lm, ev("r1", 1, "started", 100))
    submit(lm, ev("r1", 1, "succeeded", 110))
    state, reason = submit(lm, ev("r1", 1, "succeeded", 110))
    assert reason == "duplicate_event"
    assert state.status is RunStatus.SUCCEEDED
    assert len(state.dedup_keys) == 2  # started + succeeded 各一次


def test_out_of_order_failure_does_not_revert_success():
    lm = LifecycleManager()
    submit(lm, ev("r1", 1, "started", 100))
    submit(lm, ev("r1", 1, "succeeded", 110))
    # 迟到/冲突的失败（时间更晚）不能回退成功
    state, reason = submit(lm, ev("r1", 1, "failed", 120))
    assert state.status is RunStatus.SUCCEEDED
    assert reason == "attempt_terminal_held"
    assert state.attempts[1].status is RunStatus.SUCCEEDED


def test_order_independence_same_final_state():
    """乱序与正序提交应收敛到相同终态。"""
    def build(order):
        lm = LifecycleManager()
        for e in order:
            submit(lm, e)
        return lm.get("p", "r1")

    normal = [ev("r1", 1, "started", 100), ev("r1", 1, "failed", 110),
              ev("r1", 2, "started", 120), ev("r1", 2, "succeeded", 130)]
    shuffled = [ev("r1", 2, "succeeded", 130, recv=133),
                ev("r1", 1, "failed", 110, recv=132),
                ev("r1", 2, "started", 120, recv=131),
                ev("r1", 1, "started", 100, recv=130)]
    a, b = build(normal), build(shuffled)
    assert a.status is b.status is RunStatus.SUCCEEDED
    assert sorted(a.attempts) == sorted(b.attempts) == [1, 2]
    assert a.attempts[1].status is b.attempts[1].status is RunStatus.FAILED
    assert a.attempts[2].status is b.attempts[2].status is RunStatus.SUCCEEDED


def test_retry_distinguishable_but_same_run():
    lm = LifecycleManager()
    submit(lm, ev("r9", 1, "started", 100))
    submit(lm, ev("r9", 1, "failed", 110, detail="oom"))
    state, reason = submit(lm, ev("r9", 2, "started", 120))
    assert reason == "retry_started"
    assert state.status is RunStatus.RUNNING
    assert state.attempts[1].status is RunStatus.FAILED
    assert state.attempts[2].status is RunStatus.RUNNING
    submit(lm, ev("r9", 2, "succeeded", 130))
    assert lm.get("p", "r9").status is RunStatus.SUCCEEDED


def test_stale_old_attempt_cannot_change_conclusion():
    lm = LifecycleManager()
    submit(lm, ev("r1", 1, "failed", 110))
    submit(lm, ev("r1", 2, "started", 120))
    submit(lm, ev("r1", 2, "succeeded", 130))
    # attempt1 迟到的 started 不影响已成功的结论
    state, reason = submit(lm, ev("r1", 1, "started", 100, recv=140))
    assert state.status is RunStatus.SUCCEEDED
    assert reason == "attempt_terminal_held"


def test_missing_event_ts_uses_receive_order():
    lm = LifecycleManager()
    # event_ts 缺失：以 received_ts 排序，仍能从 started->failed 收敛
    submit(lm, ev("r1", 1, "started", None, recv=200))
    state, reason = submit(lm, ev("r1", 1, "failed", None, recv=210))
    assert state.status is RunStatus.FAILED
    assert state.attempts[1].finished_ts == 210
    assert reason == "attempt_finished"
