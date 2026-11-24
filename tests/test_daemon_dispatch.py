"""daemon 派发：``duplicate`` 不能被静默吞掉。

修复前：``try_to_run_case`` 只对 ``full`` 做 force 复核，``duplicate`` 直接掉进
「写 waiting_slot + return False」——命令被塞回 deferred_queue 空转，而 case
带着一个已经死掉的持有者（status=2 LOCK、心跳早就没了）永久停在 waiting_slot。
如果持有者其实还活着，这个分支又反过来把正在跑的 case 的进度条打成 pending。
"""

import datetime
import json
import os
import queue
import threading
import time

import config
import model
from model import modelProcess, modelRecovery


def _isolate(monkeypatch, tmp_path) -> str:
    monkeypatch.setattr(config, "DIR_MODEL_CASE", str(tmp_path / "case"))
    monkeypatch.setattr(config, "DIR_GLOBALE_FILE_LOCKER", str(tmp_path / "global.lock"))
    os.makedirs(config.DIR_MODEL_CASE, exist_ok=True)
    return config.DIR_MODEL_CASE


def _create(payload: dict):
    return model.ModelCaseReference.create(
        "/v0/test/dispatch", payload, "Test Model", "/tmp/test.py", []
    )


def _runtime(case_id: str) -> dict:
    with open(
        os.path.join(config.DIR_MODEL_CASE, case_id, "runtime.json"), encoding="utf-8"
    ) as file:
        return json.load(file)


def _events(case_id: str) -> list[dict]:
    path = os.path.join(config.DIR_MODEL_CASE, case_id, "events.log")
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as file:
        return [json.loads(line) for line in file if line.strip()]


def _status_name(case_id: str) -> str:
    directory = os.path.join(config.DIR_MODEL_CASE, case_id, "status")
    return sorted(os.listdir(directory))[0]


def _claim(case_id: str) -> None:
    """模拟「子进程已经起来、mcr_checker 已认领、监控线程在写心跳」。"""
    mcr = model.ModelCaseReference.open_case(case_id)
    mcr.update_status(config.STATUS_LOCK, "OK")
    modelRecovery.touch_heartbeat(case_id)


def _wait_until(predicate, timeout: float = 15.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


def _start_daemon(monkeypatch, spawn_calls: list[list[str]]) -> queue.Queue:
    def fake_launcher(command):
        _claim(command[-1])
        spawn_calls.append(command)
        return object()  # 非 None：spawn 成功

    monkeypatch.setattr(modelProcess, "safe_launcher", fake_launcher)
    process_queue: queue.Queue = queue.Queue()
    threading.Thread(
        target=modelProcess.daemon_process,
        args=(process_queue,),
        daemon=True,
    ).start()
    return process_queue


def test_duplicate_is_consumed_without_downgrading_the_running_case(
    monkeypatch, tmp_path
) -> None:
    """持有者还活着：命令要被消费掉（不空转），且绝不能写 waiting_slot。"""
    _isolate(monkeypatch, tmp_path)
    case = _create({"probe": "duplicate"})
    spawn_calls: list[list[str]] = []
    process_queue = _start_daemon(monkeypatch, spawn_calls)
    command = ["prog.pyc", case.id]

    process_queue.put(command)
    assert _wait_until(lambda: len(spawn_calls) == 1)
    assert _wait_until(lambda: _runtime(case.id)["stage"] == "launching")

    # 同参数再提交一次（case id 由 md5 决定，必然命中同一个 case）
    process_queue.put(command)

    assert _wait_until(
        lambda: any(event.get("stage") == "duplicate_ignored" for event in _events(case.id))
    )
    assert len(spawn_calls) == 1
    assert _runtime(case.id)["stage"] == "launching"  # 没有被打回 waiting_slot
    assert _status_name(case.id) == str(config.STATUS_LOCK)


def test_stale_slot_is_reclaimed_and_redispatched(monkeypatch, tmp_path) -> None:
    """持有者已经死了（LOCK + 心跳过期）：槽位必须收回，命令重新派发。"""
    _isolate(monkeypatch, tmp_path)
    case = _create({"probe": "stale"})
    spawn_calls: list[list[str]] = []
    process_queue = _start_daemon(monkeypatch, spawn_calls)
    command = ["prog.pyc", case.id]

    process_queue.put(command)
    assert _wait_until(lambda: len(spawn_calls) == 1)

    # 把「持有者」做旧：心跳删掉，runtime 与 status 目录的时间推到 60s 前。
    modelRecovery.remove_heartbeat(case.id)
    old = time.time() - 60
    os.utime(os.path.join(config.DIR_MODEL_CASE, case.id, "status"), (old, old))
    runtime_path = os.path.join(config.DIR_MODEL_CASE, case.id, "runtime.json")
    runtime = _runtime(case.id)
    runtime["updated-at"] = (
        datetime.datetime.now() - datetime.timedelta(seconds=60)
    ).isoformat(timespec="milliseconds")
    with open(runtime_path, "w", encoding="utf-8") as file:
        json.dump(runtime, file)
    os.utime(runtime_path, (old, old))

    process_queue.put(command)

    assert _wait_until(lambda: len(spawn_calls) == 2)
    assert any(event.get("stage") == "recovered" for event in _events(case.id))
    assert _runtime(case.id)["stage"] == "launching"


def test_pending_stage_is_not_rewritten_on_every_poll(monkeypatch, tmp_path) -> None:
    """排队中的命令每轮空转都会重走一遍，runtime.json 不能每次都被重写。"""
    _isolate(monkeypatch, tmp_path)
    case = _create({"probe": "pending"})
    mcr = model.ModelCaseReference.open_case(case.id)

    modelProcess.mark_pending_stage(mcr, "waiting_slot", 10, "Waiting for a slot")
    runtime_path = os.path.join(case.directory, "runtime.json")
    assert _runtime(case.id)["stage"] == "waiting_slot"

    # 把 mtime 拨旧，这样「有没有再写一次」可以直接由 mtime 判断
    old = time.time() - 600
    os.utime(runtime_path, (old, old))
    marker = os.stat(runtime_path).st_mtime_ns

    modelProcess.mark_pending_stage(mcr, "waiting_slot", 10, "Waiting for a slot")
    assert os.stat(runtime_path).st_mtime_ns == marker

    # 依赖 id 变了必须写：那是调用方唯一能看到「在等谁」的地方
    modelProcess.mark_pending_stage(
        mcr, "waiting_slot", 10, "Waiting for a slot", meta={"dependency-case-id": "x"}
    )
    assert os.stat(runtime_path).st_mtime_ns != marker
