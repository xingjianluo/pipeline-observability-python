"""新鲜度：边界、时钟回拨、缺失时间戳/间隔、间隔调小调大。"""
import logging

import pytest

from pipelineobs.freshness import FreshnessMonitor
from pipelineobs.models import FreshnessState as S

log = logging.getLogger("test.freshness")


def decide(m, pid, at, interval=None):
    r = m.evaluate(pid, at, interval)
    log.info("INPUT pipeline=%s evaluated_at=%s interval=%s "
             "=> DECISION state=%s reason=%s age=%s lag=%s last_success=%s",
             pid, at, interval, r.state.value, r.reason,
             r.age_seconds, r.lag_seconds, r.last_success_ts)
    return r


def test_no_success_is_unknown_distinguishable():
    m = FreshnessMonitor()
    r = decide(m, "p", 100, 10)
    assert r.state is S.UNKNOWN and r.reason == "missing_last_success"


def test_missing_event_ts_is_unknown():
    m = FreshnessMonitor()
    m.record_success("p", None)
    r = decide(m, "p", 100, 10)
    assert r.state is S.UNKNOWN and r.reason == "missing_event_ts"


def test_missing_and_nonpositive_interval():
    m = FreshnessMonitor()
    m.record_success("p", 90)
    r = decide(m, "p", 100, None)
    assert r.reason == "missing_interval"
    r = decide(m, "p", 100, 0)
    assert r.reason == "non_positive_interval"
    r = decide(m, "p", 100, -5)
    assert r.reason == "non_positive_interval"


def test_boundary_fresh_lagging_stale():
    m = FreshnessMonitor()
    m.record_success("p", 100)
    # age == interval 仍 fresh（半开边界）
    assert decide(m, "p", 110, 10).state is S.FRESH
    # age 略超 interval => lagging
    r = decide(m, "p", 110.001, 10)
    assert r.state is S.LAGGING and r.reason == "age_exceeds_one_interval"
    # age == 2*interval 仍 lagging
    assert decide(m, "p", 120, 10).state is S.LAGGING
    # age > 2*interval => stale
    assert decide(m, "p", 120.001, 10).state is S.STALE


def test_clock_rollback_detected_not_silent():
    m = FreshnessMonitor()
    m.record_success("p", 100)
    r = decide(m, "p", 99, 10)
    assert r.state is S.UNKNOWN and r.reason == "clock_rollback"
    assert r.age_seconds == -1.0  # 可解释的负年龄


def test_interval_shrink_requires_confirmation():
    m = FreshnessMonitor()
    m.record_success("p", 100)
    decide(m, "p", 100, 10)
    # 调小间隔：不能静默套新值
    r = decide(m, "p", 105, 5)
    assert r.state is S.UNKNOWN and r.reason == "interval_shrunk"
    # 显式确认后按新间隔判定
    m.confirm_interval_shrink("p")
    r = decide(m, "p", 105, 5)
    assert r.state is S.FRESH  # age=5 == interval
    # 调大间隔不需要确认
    r = decide(m, "p", 105, 8)
    assert r.state is S.FRESH


def test_monotonic_last_success_ignores_older_ts():
    m = FreshnessMonitor()
    m.record_success("p", 100)
    m.record_success("p", 90)  # 乱序的更早成功，不回退
    r = decide(m, "p", 100, 10)
    assert r.last_success_ts == 100
    assert r.state is S.FRESH
