"""case 锁自愈的单元测试。

背景：case 的状态是 ``case/<id>/status/`` 下的**文件名**（位掩码）。
``LOCK`` 没有持有者记录也没有租约，认领它的进程一死，锁就永远留着，
``/v0/mc/status`` 对它恒回 ``pending``。这里验证「怎么判定它已经过期」。
"""

import json
import os
import time

import config
from model import modelRecovery


def _iso(seconds_ago: float) -> str:
    import datetime

    return (
        datetime.datetime.now() - datetime.timedelta(seconds=seconds_ago)
    ).isoformat(timespec="seconds")


def make_case(
    root: str,
    case_id: str = "a" * 32,
    status: int = config.STATUS_LOCK,
    idle_seconds: float = 3600,
    heartbeat_age: float | None = None,
) -> str:
    """造一个 case 目录，``idle_seconds`` 表示「最近一次有人动它」是多久以前。"""
    directory = os.path.join(root, case_id)
    os.makedirs(os.path.join(directory, "status"), exist_ok=True)
    os.makedirs(os.path.join(directory, "result"), exist_ok=True)

    with open(os.path.join(directory, "identity.json"), "w", encoding="utf-8") as file:
        json.dump(
            {
                "model": "Test Model",
                "model-path": "",
                "url": "/v0/test",
                "json": {},
                "dependencies": [],
            },
            file,
        )
    with open(
        os.path.join(directory, "status", str(status)), "w", encoding="utf-8"
    ) as file:
        file.write("OK")
    with open(os.path.join(directory, "runtime.json"), "w", encoding="utf-8") as file:
        json.dump(
            {
                "case-id": case_id,
                "model": "Test Model",
                "stage": "launching",
                "updated-at": _iso(idle_seconds),
            },
            file,
        )

    # status 目录的 mtime 也是「有人在动它」的证据之一，一起做旧
    stamp = time.time() - idle_seconds
    os.utime(os.path.join(directory, "status"), (stamp, stamp))
    os.utime(directory, (stamp, stamp))

    if heartbeat_age is not None:
        path = modelRecovery.get_heartbeat_path(case_id)
        with open(path, "a", encoding="utf-8"):
            pass
        beat = time.time() - heartbeat_age
        os.utime(path, (beat, beat))

    return directory


def status_of(case_id: str) -> str:
    directory = os.path.join(config.DIR_MODEL_CASE, case_id, "status")
    return sorted(os.listdir(directory))[0]


def runtime_of(case_id: str) -> dict:
    with open(
        os.path.join(config.DIR_MODEL_CASE, case_id, "runtime.json"), encoding="utf-8"
    ) as file:
        return json.load(file)


def test_stale_lock_is_detected_and_reset(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DIR_MODEL_CASE", str(tmp_path))
    case_id = "1" * 32
    make_case(str(tmp_path), case_id, status=config.STATUS_LOCK, idle_seconds=3600)

    assert modelRecovery.stale_reason(case_id) == "lock"

    summary = modelRecovery.recover_stale_cases()

    assert summary["scanned"] == 1
    assert [item["case-id"] for item in summary["recovered"]] == [case_id]
    assert status_of(case_id) == str(config.STATUS_UNLOCK)
    runtime = runtime_of(case_id)
    assert runtime["stage"] == "recovered"
    assert runtime["status"] == "pending"
    assert runtime["meta"]["recovered-from-status"] == config.STATUS_LOCK


def test_fresh_heartbeat_protects_a_running_case(tmp_path, monkeypatch):
    """监控线程还在写心跳 ⇒ 子进程真的在跑（可能正在 import），绝不能重置。"""
    monkeypatch.setattr(config, "DIR_MODEL_CASE", str(tmp_path))
    case_id = "2" * 32
    make_case(
        str(tmp_path),
        case_id,
        status=config.STATUS_LOCK,
        idle_seconds=3600,
        heartbeat_age=0.5,
    )

    assert modelRecovery.stale_reason(case_id) is None
    assert modelRecovery.try_recover_stale_lock(
        modelRecovery.MCR.open_case(case_id)
    ) is False
    assert status_of(case_id) == str(config.STATUS_LOCK)


def test_completed_and_error_cases_are_never_touched(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DIR_MODEL_CASE", str(tmp_path))
    done = "3" * 32
    failed = "4" * 32
    make_case(str(tmp_path), done, status=config.STATUS_UNLOCK | config.STATUS_COMPLETE)
    make_case(str(tmp_path), failed, status=config.STATUS_UNLOCK | config.STATUS_ERROR)

    summary = modelRecovery.recover_stale_cases()

    assert summary["recovered"] == []
    assert status_of(done) == str(config.STATUS_UNLOCK | config.STATUS_COMPLETE)
    assert status_of(failed) == str(config.STATUS_UNLOCK | config.STATUS_ERROR)


def test_unlock_case_needs_no_recovery(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DIR_MODEL_CASE", str(tmp_path))
    case_id = "5" * 32
    make_case(str(tmp_path), case_id, status=config.STATUS_UNLOCK, idle_seconds=99999)

    assert modelRecovery.stale_reason(case_id) is None


def test_running_case_uses_the_longer_threshold(tmp_path, monkeypatch):
    """``LOCK|RUNNING`` 说明子进程进过包装器，可能真的在算，阈值要更宽。"""
    monkeypatch.setattr(config, "DIR_MODEL_CASE", str(tmp_path))
    case_id = "6" * 32
    make_case(
        str(tmp_path),
        case_id,
        status=config.STATUS_LOCK | config.STATUS_RUNNING,
        idle_seconds=120,
    )

    assert modelRecovery.stale_reason(case_id) is None
    assert (
        modelRecovery.stale_reason(case_id, running_seconds=60) == "running"
    )


def test_runtime_takeover_only_handles_plain_lock(tmp_path, monkeypatch):
    """运行期只接管「只有 LOCK」的僵尸；RUNNING 的交给启动自愈。"""
    monkeypatch.setattr(config, "DIR_MODEL_CASE", str(tmp_path))
    running = "7" * 32
    make_case(
        str(tmp_path),
        running,
        status=config.STATUS_LOCK | config.STATUS_RUNNING,
        idle_seconds=99999,
    )

    assert (
        modelRecovery.try_recover_stale_lock(modelRecovery.MCR.open_case(running))
        is False
    )
    assert status_of(running) == str(config.STATUS_LOCK | config.STATUS_RUNNING)


def test_infra_error_is_retryable_but_model_error_is_not(tmp_path, monkeypatch):
    """环境故障（子进程死）可以重跑；模型代码抛异常必须保持缓存直返。"""
    monkeypatch.setattr(config, "DIR_MODEL_CASE", str(tmp_path))

    infra = "8" * 32
    make_case(
        str(tmp_path), infra, status=config.STATUS_UNLOCK | config.STATUS_ERROR
    )
    path = os.path.join(config.DIR_MODEL_CASE, infra, "runtime.json")
    payload = runtime_of(infra)
    payload["stage"] = "error"
    payload["meta"] = {"infra-error": True, "exit-code": 1}
    with open(path, "w", encoding="utf-8") as file:
        json.dump(payload, file)

    model_error = "9" * 32
    make_case(
        str(tmp_path),
        model_error,
        status=config.STATUS_UNLOCK | config.STATUS_ERROR,
    )
    path = os.path.join(config.DIR_MODEL_CASE, model_error, "runtime.json")
    payload = runtime_of(model_error)
    payload["stage"] = "error"
    payload["meta"] = {"traceback": "Traceback\nValueError: bad payload"}
    with open(path, "w", encoding="utf-8") as file:
        json.dump(payload, file)

    assert modelRecovery.is_retryable_error(modelRecovery.MCR.open_case(infra)) is True
    assert (
        modelRecovery.is_retryable_error(modelRecovery.MCR.open_case(model_error))
        is False
    )

    assert modelRecovery.reset_error_for_retry(
        modelRecovery.MCR.open_case(infra), "retry"
    )
    assert status_of(infra) == str(config.STATUS_UNLOCK)

    assert not modelRecovery.reset_error_for_retry(
        modelRecovery.MCR.open_case(model_error), "retry"
    )
    assert status_of(model_error) == str(config.STATUS_UNLOCK | config.STATUS_ERROR)


def test_recovery_summary_is_readable(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DIR_MODEL_CASE", str(tmp_path))
    make_case(str(tmp_path), "b" * 32, status=config.STATUS_LOCK, idle_seconds=7200)

    text = modelRecovery.format_recovery_summary(modelRecovery.recover_stale_cases())

    assert "回收 1 个过期锁" in text
    assert "lock=1" in text


def test_watchdog_mode_only_recovers_half_dead_running_cases(tmp_path, monkeypatch):
    """常驻巡检只碰 `LOCK|RUNNING`；普通 `LOCK` 交给请求路径判定，避免误伤排队中的 case。"""
    monkeypatch.setattr(config, "DIR_MODEL_CASE", str(tmp_path))
    queued = "c1" * 16
    half_dead = "c2" * 16
    make_case(str(tmp_path), queued, status=config.STATUS_LOCK, idle_seconds=7200)
    make_case(
        str(tmp_path),
        half_dead,
        status=config.STATUS_LOCK | config.STATUS_RUNNING,
        idle_seconds=7200,
    )

    summary = modelRecovery.recover_stale_cases(include_lock=False)

    assert [item["case-id"] for item in summary["recovered"]] == [half_dead]
    assert status_of(queued) == str(config.STATUS_LOCK)
    assert status_of(half_dead) == str(config.STATUS_UNLOCK)
