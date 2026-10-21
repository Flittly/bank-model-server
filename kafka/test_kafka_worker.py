"""Kafka Worker 回归测试。

锁住两类曾经出问题、且极易再次退化的行为：

1. 调用点与 ``execute_risk_level_task`` 的形参个数必须一致。
   历史 bug：worker 传 3 个实参、函数只收 2 个 → 每条消息 100% TypeError，
   且异常被 ``raise`` 抛出 run() → 整个 worker 死掉 + 偏移量不提交 → 毒消息无限重放。
2. 消费失败必须「回写 ERROR 结果 + 提交偏移量 + 继续」，绝不能把异常抛出 ``run()``。

不依赖真实 Kafka：用 ``object.__new__`` 绕过 __init__，对 ``_send_result`` / ``consumer`` 打桩。

运行：
    uv run pytest kafka/test_kafka_worker.py -q
或（无需 pytest）
    uv run python kafka/test_kafka_worker.py
"""
from __future__ import annotations

import inspect
import json
import os
import re
import sys
import types

_KAFKA_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_KAFKA_DIR)
for _path in (_KAFKA_DIR, _ROOT):
    if _path not in sys.path:
        sys.path.insert(0, _path)

import kafka_executor  # noqa: E402
import kafka_worker  # noqa: E402


# --------------------------------------------------------------------------
# 1. 调用点 / 形参一致性（历史 bug 的直接回归）
# --------------------------------------------------------------------------

def test_executor_takes_exactly_two_positional_params():
    params = list(inspect.signature(kafka_executor.execute_risk_level_task).parameters.values())
    assert len(params) == 2, f"execute_risk_level_task 的形参个数变了: {params}"


def test_worker_calls_executor_with_matching_arity():
    src = inspect.getsource(kafka_worker.KafkaModelWorker.run)
    assert "execute_risk_level_task(task, self.worker_id)" in src, \
        "调用点与 execute_risk_level_task(task, worker_id) 的实参不一致"


def test_terrain_preload_is_not_called_from_loop():
    src = inspect.getsource(kafka_worker.KafkaModelWorker.run)
    # 只匹配「调用形式」（方法名后跟左括号），注释里提到方法名不算
    assert not re.search(r'_load_terrain_if_needed\s*\(', src), \
        "主流程不应再调用未接通的地形预下载（会无谓下载，且下载失败会打断 worker）"


# --------------------------------------------------------------------------
# 2. 失败不能打死 worker
# --------------------------------------------------------------------------

def _make_worker():
    """绕过 __init__（不连 Kafka），构造一个可打桩的 worker 实例。"""
    worker = object.__new__(kafka_worker.KafkaModelWorker)
    worker.worker_id = "test-worker"
    worker.sent = []
    worker.committed = []
    worker._send_result = lambda result: worker.sent.append(result)
    worker.consumer = types.SimpleNamespace(commit=lambda msg: worker.committed.append(msg))
    return worker


def test_handle_failure_writes_error_result_and_commits():
    worker = _make_worker()

    worker._handle_failure("MSG-1", {"runId": "r1", "taskId": "t1", "sectionId": "s1"},
                           ValueError("boom"))

    assert len(worker.sent) == 1, worker.sent
    result = worker.sent[0]
    assert result["status"] == "ERROR"
    assert result["runId"] == "r1" and result["sectionId"] == "s1"
    assert "boom" in result["errorMessage"]
    assert worker.committed == ["MSG-1"], "必须提交偏移量，否则同一条消息会无限重放"


def test_handle_failure_never_raises_when_publish_fails():
    worker = _make_worker()

    def _boom(_result):
        raise RuntimeError("kafka 不可用")

    worker._send_result = _boom

    worker._handle_failure("MSG-2", {"runId": "r", "sectionId": "s"}, ValueError("x"))

    assert worker.committed == ["MSG-2"], "回写失败也必须提交偏移量"


def test_handle_failure_never_raises_when_commit_fails():
    worker = _make_worker()

    def _commit_boom(_msg):
        raise RuntimeError("commit 失败")

    worker.consumer = types.SimpleNamespace(commit=_commit_boom)

    worker._handle_failure("MSG-3", {"runId": "r", "sectionId": "s"}, ValueError("x"))

    assert len(worker.sent) == 1


def test_run_loop_does_not_reraise():
    src = inspect.getsource(kafka_worker.KafkaModelWorker.run)
    tail = src.split("except Exception as e:", 1)[-1]
    assert "raise" not in tail, "run() 的异常分支里又出现了 raise——一条坏消息会打死整个 worker"
    assert "_handle_failure" in tail
    assert "continue" in tail


def test_malformed_message_is_guarded():
    src = inspect.getsource(kafka_worker.KafkaModelWorker.run)
    assert "消息不是合法 JSON" in src, "json.loads 没有做保护，畸形消息会打断 worker"


# --------------------------------------------------------------------------
# 2.5 岸段名单不得参与调度（历史 bug：跳过且不 commit → 队头阻塞 + 无人处理）
# --------------------------------------------------------------------------

class _Msg:
    def __init__(self, payload):
        self._payload = payload

    def value(self):
        return json.dumps(self._payload).encode("utf-8")

    def error(self):
        return None


def _make_loop_worker(assigned_banks):
    """构造一个能跑 run() 单轮的 worker：poll 出一句后停止。"""
    worker = object.__new__(kafka_worker.KafkaModelWorker)
    worker.worker_id = "test-worker"
    worker.bootstrap_servers = ["localhost:9092"]
    worker.task_topic = "t"
    worker.result_topic = "r"
    worker.assigned_banks = assigned_banks
    worker._warned_banks = set()
    worker._running = True
    worker.sent = []
    worker.committed = []
    worker._send_result = lambda result: worker.sent.append(result)
    return worker


def _drive_one_message(worker, msg):
    state = {"polled": 0}

    def poll(timeout=1.0):
        state["polled"] += 1
        if state["polled"] == 1:
            return msg
        worker._running = False
        return None

    worker.consumer = types.SimpleNamespace(
        poll=poll, commit=lambda msg: worker.committed.append(msg))
    worker.run()


def test_message_outside_assigned_banks_is_still_processed():
    """名单外岸段必须照常处理——跳过它等于没人处理它，且阻塞该分区后续消息。"""
    worker = _make_loop_worker(assigned_banks=["BANK_A"])

    called = []
    original = kafka_worker.execute_risk_level_task
    kafka_worker.execute_risk_level_task = (
        lambda task, worker_id: called.append(task["bankId"]) or
        {"runId": task["runId"], "sectionId": task["sectionId"], "status": "OK"})
    try:
        _drive_one_message(worker, _Msg({
            "runId": "r1", "taskId": "t1", "sectionId": "s1", "bankId": "BANK_B"}))
    finally:
        kafka_worker.execute_risk_level_task = original

    assert called == ["BANK_B"], "名单外岸段被跳过了，会造成队头阻塞"
    assert len(worker.committed) == 1, "处理完必须提交偏移量"


def test_run_never_skips_on_assigned_banks_mismatch():
    src = inspect.getsource(kafka_worker.KafkaModelWorker.run)
    assert "跳过非分配岸段" not in src, "过滤分支又回来了（会阻塞分区且消息无人处理）"
    assert "_warn_unexpected_bank" in src, "名单不匹配时应告警而非跳过"
    assert "assigned_banks and bank_id not in" in src, "自检分支被误删"


def test_warn_unexpected_bank_is_deduplicated(capsys=None):
    worker = _make_loop_worker(assigned_banks=["BANK_A"])
    worker._warned_banks = set()
    worker._warn_unexpected_bank("BANK_B")
    worker._warn_unexpected_bank("BANK_B")
    assert worker._warned_banks == {"BANK_B"}


# --------------------------------------------------------------------------
# 3. 错误结果字段结构必须与 executor 的错误返回一致（Java 侧按此解析）
# --------------------------------------------------------------------------

def test_error_result_shape_matches_executor():
    class _BoomLauncher:
        def fetch_model_from_API(self, _api):
            raise RuntimeError("模型不可用")

    original = kafka_executor.model.launcher
    kafka_executor.model.launcher = _BoomLauncher()
    try:
        executor_result = kafka_executor.execute_risk_level_task(
            {"payload": {}, "runId": "r", "taskId": "t", "sectionId": "s"}, "w")
    finally:
        kafka_executor.model.launcher = original

    assert executor_result["status"] == "ERROR"

    worker = _make_worker()
    worker._handle_failure("MSG", {"runId": "r", "taskId": "t", "sectionId": "s"}, ValueError("x"))
    handled = worker.sent[0]

    assert set(handled) == set(executor_result), (
        "字段结构不一致，Java 侧可能解析失败\n"
        f"worker  = {sorted(handled)}\n"
        f"executor= {sorted(executor_result)}")


def _run_all():
    tests = [(name, obj) for name, obj in sorted(globals().items())
             if name.startswith('test_') and callable(obj)]
    failed = []
    for name, func in tests:
        try:
            func()
            print(f"  PASS  {name}")
        except AssertionError as exc:
            failed.append(name)
            print(f"  FAIL  {name}: {exc}")
        except Exception as exc:  # noqa: BLE001
            failed.append(name)
            print(f"  ERROR {name}: {type(exc).__name__}: {exc}")
    print(f"\n{len(tests) - len(failed)}/{len(tests)} passed")
    return 1 if failed else 0


if __name__ == '__main__':
    raise SystemExit(_run_all())
