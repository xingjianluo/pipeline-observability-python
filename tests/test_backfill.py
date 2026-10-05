"""历史事件批量回补：乱序、重复、部分失败、与逐条单报等价、跨流水线。"""
import logging

from pipelineobs.models import BackfillItem
from pipelineobs.store import PipelineStore

log = logging.getLogger("test.backfill")


def item(pid, rid, etype, ts=None, attempt=1, **kw):
    return BackfillItem(pid, rid, attempt, etype, ts, **kw)


def _apply_reference(events):
    """参照实现：同一批内容逐条单独上报到全新 store。"""
    ref = PipelineStore()
    for e in events:
        ref.ingest(e.pipeline_id, e.run_id, e.attempt, e.event_type,
                   e.event_ts, e.detail,
                   received_ts=e.received_ts
                   if e.received_ts is not None else
                   (e.event_ts if e.event_ts is not None else 1000))
    return ref


def _snapshot(store):
    snap = {}
    for st in store.list_runs():
        snap[(st.pipeline_id, st.run_id)] = (
            st.status.value,
            tuple(sorted((n, a.status.value, a.started_ts, a.finished_ts,
                          a.latency) for n, a in st.attempts.items())))
    return snap


def test_batch_out_of_order_equals_single_submission():
    ordered = [
        item("p", "r1", "started", 100),
        item("p", "r1", "failed", 110),
        item("p", "r1", "started", 120, attempt=2),
        item("p", "r1", "succeeded", 130, attempt=2),
        item("p", "r2", "failed", 140),
    ]
    shuffled = list(reversed(ordered))

    batch_store = PipelineStore()
    res = batch_store.ingest_batch(shuffled)
    log.info("batch => accepted=%d duplicate=%d invalid=%d",
             res.accepted, res.duplicate, res.invalid)

    ref = _apply_reference(shuffled)
    assert _snapshot(batch_store) == _snapshot(ref)

    # 窗口统计（成功失败数、耗时）也必须一致
    wb, wr = batch_store.window("p", 1000, 1000), ref.window("p", 1000, 1000)
    assert (wb.total_runs, wb.succeeded_runs, wb.failed_runs) == (
        wr.total_runs, wr.succeeded_runs, wr.failed_runs)
    assert wb.latencies == wr.latencies


def test_batch_latency_consistent_across_submission_orders():
    """同一批历史事件换不同提交顺序回补，窗口耗时（avg/p95）与计数完全一致，
    且与按时间顺序逐条上报的结论一致。"""
    import itertools

    base = [
        item("p", "r1", "started", 100),
        item("p", "r1", "failed", 110),            # 尝试1失败，耗时10
        item("p", "r1", "started", 120, attempt=2),
        item("p", "r1", "succeeded", 135, attempt=2),  # 重试成功，耗时15
        item("p", "r2", "started", 200),
        item("p", "r2", "succeeded", 240),         # 成功，耗时40
        item("p", "r3", "started", 300),
        item("p", "r3", "failed", 330),            # 失败，耗时30
    ]
    # 参照：按时间顺序逐条上报
    ref = _apply_reference(base)
    ref_w = ref.window("p", 1000, 1000)
    assert ref_w.avg_latency is not None and ref_w.p95_latency is not None

    orders = [
        list(reversed(base)),                      # 完全逆序（终态全部先到）
        base[::2] + base[1::2],                    # 奇偶交错
        list(base),                                # 时间顺序整批
    ]
    # 再加全排列抽查（8 条事件的全排列 40320 种，抽前 200 种）
    for perm in itertools.islice(itertools.permutations(base), 200):
        orders.append(list(perm))

    for order in orders:
        store = PipelineStore()
        res = store.ingest_batch(order)
        assert res.invalid == 0
        w = store.window("p", 1000, 1000)
        assert (w.total_runs, w.succeeded_runs, w.failed_runs) == (3, 2, 1)
        assert sorted(w.latencies) == sorted(ref_w.latencies)
        assert w.avg_latency == ref_w.avg_latency
        assert w.p95_latency == ref_w.p95_latency
        # 每个运行的耗时都必须有值且一致
        assert _snapshot(store) == _snapshot(ref)
    log.info("latency order-independence verified over %d orders: avg=%s p95=%s",
             len(orders), ref_w.avg_latency, ref_w.p95_latency)


def test_batch_latency_filled_when_terminal_arrives_first():
    """终态事件先回补、started 后补：耗时必须补上而不是一直为空。"""
    store = PipelineStore()
    res = store.ingest_batch([
        item("p", "r1", "succeeded", 110),         # 终态先到
        item("p", "r1", "started", 100),           # started 后补
    ])
    assert res.invalid == 0
    attempt = store.get_run("p", "r1").attempts[1]
    assert attempt.started_ts == 100 and attempt.latency == 10
    w = store.window("p", 1000, 1000)
    assert w.avg_latency == 10 and w.p95_latency == 10
    # 乱序重试同一批：幂等，耗时不被改乱
    res2 = store.ingest_batch([
        item("p", "r1", "started", 100),
        item("p", "r1", "succeeded", 110),
    ])
    assert res2.accepted == 0 and res2.duplicate == 2
    assert store.window("p", 1000, 1000).avg_latency == 10



    store = PipelineStore()
    res = store.ingest_batch([
        item("p", "r1", "started", 100),
        item("p", "r1", "succeeded", 110),
        item("p", "r1", "succeeded", 110),       # 批次内重复
        item("p", "r1", "failed", 120),          # 成功后迟到失败，被保持
    ])
    by_status = [r.status.value for r in res.results]
    assert by_status == ["accepted", "accepted", "duplicate", "duplicate"]
    assert (res.total, res.accepted, res.duplicate, res.invalid) == (4, 2, 2, 0)
    # 成功迟到失败判定依据可区分
    assert res.results[3].lifecycle_reason == "attempt_terminal_held"
    w = store.window("p", 1000, 1000)
    assert w.total_runs == 1 and w.succeeded_runs == 1


def test_batch_partial_failure_does_not_drop_good_events():
    store = PipelineStore()
    res = store.ingest_batch([
        item("", "r0", "succeeded", 100),                    # 缺 pipeline
        item("p", "", "succeeded", 100),                     # 缺 run
        item("p", "r1", "weird", 100),                       # 非法事件类型
        item("p", "r2", "succeeded", 100, attempt=0),        # 非法 attempt
        item("p", "r3", "succeeded", "nope"),                # 非法时间戳
        item("p", "r4", "succeeded", 110),                   # 合法
        item("p", "r5", "failed", 120),                      # 合法
    ])
    assert res.invalid == 5 and res.accepted == 2
    reasons = {r.index: r.reason for r in res.results
               if r.status.value == "invalid"}
    assert reasons == {0: "missing_pipeline_id", 1: "missing_run_id",
                       2: "invalid_event_type", 3: "invalid_attempt",
                       4: "invalid_event_ts"}
    # 合法事件照常生效
    assert store.get_run("p", "r4").status.value == "succeeded"
    assert store.get_run("p", "r5").status.value == "failed"
    assert store.get_run("p", "r1") is None
    w = store.window("p", 1000, 1000)
    assert (w.total_runs, w.succeeded_runs, w.failed_runs) == (2, 1, 1)


def test_duplicate_batch_is_idempotent():
    store = PipelineStore()
    events = [
        item("p", "r1", "started", 100),
        item("p", "r1", "succeeded", 110),
        item("p", "r2", "failed", 120),
    ]
    first = store.ingest_batch(events)
    assert first.accepted == 3
    before = _snapshot(store)
    w_before = store.window("p", 1000, 1000)

    second = store.ingest_batch(events)
    assert second.accepted == 0 and second.duplicate == 3
    assert all(r.status.value == "duplicate" for r in second.results)
    assert _snapshot(store) == before
    w_after = store.window("p", 1000, 1000)
    assert (w_after.total_runs, w_after.succeeded_runs,
            w_after.failed_runs) == (w_before.total_runs,
                                     w_before.succeeded_runs,
                                     w_before.failed_runs)

    # 乱序重试同一批同样幂等
    third = store.ingest_batch(list(reversed(events)))
    assert third.accepted == 0
    assert _snapshot(store) == before


def test_batch_spans_multiple_pipelines_and_runs():
    store = PipelineStore()
    res = store.ingest_batch([
        item("a", "x", "succeeded", 100),
        item("a", "y", "failed", 101),
        item("b", "z", "succeeded", 102),
        item("b", "w", "failed", 103),
    ])
    assert res.accepted == 4
    assert store.window("a", 1000, 1000).total_runs == 2
    assert store.window("b", 1000, 1000).total_runs == 2


def test_batch_failure_then_retry_success_counts_once():
    store = PipelineStore()
    # 失败事件在批次里晚于重试成功事件到达（乱序）
    res = store.ingest_batch([
        item("p", "r1", "succeeded", 130, attempt=2),
        item("p", "r1", "failed", 110, attempt=1),
        item("p", "r1", "started", 100, attempt=1),
        item("p", "r1", "started", 120, attempt=2),
    ])
    assert res.invalid == 0
    w = store.window("p", 1000, 1000)
    assert (w.total_runs, w.succeeded_runs, w.failed_runs) == (1, 1, 0)
    # 终态时间取成功尝试的时间，与到达顺序无关
    assert store.get_run("p", "r1").status.value == "succeeded"
