"""子进程监控的单元测试。

修复前：``daemon_process.try_to_run_case`` 只做 ``subprocess.Popen(command)``
然后把返回值丢掉；同文件的 ``monitor_subprocess`` 从未被调用，而且它的
``finally`` 只在「进程还活着」时才写 ERROR，所以即使接上也抓不到「子进程退出码非 0」。
结果就是子进程一死，case 永远停在 LOCK，``/v0/mc/status`` 恒回 pending。
"""

import io
import json
import os

import config
from model import modelProcess


class FakeProcess:
    """替身：``poll`` 直接返回预设退出码，避免测试里真的起进程。"""

    def __init__(self, return_code: int):
        self.return_code = return_code
        self.pid = 4321

    def poll(self):
        return self.return_code

    def wait(self):
        return self.return_code


def make_case(root: str, case_id: str, status: int) -> str:
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
    return directory


def status_of(case_id: str) -> str:
    return sorted(os.listdir(os.path.join(config.DIR_MODEL_CASE, case_id, "status")))[0]


def runtime_of(case_id: str) -> dict:
    with open(
        os.path.join(config.DIR_MODEL_CASE, case_id, "runtime.json"), encoding="utf-8"
    ) as file:
        return json.load(file)


def run_monitor(case_id: str, return_code: int) -> None:
    log_path = modelProcess.get_child_log_path(case_id)
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    with open(log_path, "a", encoding="utf-8") as file:
        file.write("Traceback (most recent call last):\nImportError: libgdal.so\n")
    modelProcess.monitor_case_subprocess(
        FakeProcess(return_code), case_id, log_path, io.StringIO()
    )


def test_nonzero_exit_marks_case_error_with_child_log(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DIR_MODEL_CASE", str(tmp_path))
    case_id = "c" * 32
    make_case(str(tmp_path), case_id, config.STATUS_LOCK)

    run_monitor(case_id, 1)

    assert status_of(case_id) == str(config.STATUS_ERROR | config.STATUS_UNLOCK)
    runtime = runtime_of(case_id)
    assert "exited with code 1" in runtime["message"]
    assert runtime["status"] == "error"
    assert runtime["meta"]["infra-error"] is True
    # 崩溃现场必须落到 case 里，否则事后只能靠猜
    assert "libgdal" in runtime["meta"]["child-log-tail"]


def test_zero_exit_without_status_is_still_an_error(tmp_path, monkeypatch):
    """退出码 0 却没写状态：脚本没跑到包装器，对调用方来说和崩溃等价。"""
    monkeypatch.setattr(config, "DIR_MODEL_CASE", str(tmp_path))
    case_id = "d" * 32
    make_case(str(tmp_path), case_id, config.STATUS_LOCK)

    run_monitor(case_id, 0)

    assert status_of(case_id) == str(config.STATUS_ERROR | config.STATUS_UNLOCK)
    assert "without writing a case status" in runtime_of(case_id)["message"]


def test_monitor_does_not_overwrite_a_completed_case(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DIR_MODEL_CASE", str(tmp_path))
    case_id = "e" * 32
    make_case(str(tmp_path), case_id, config.STATUS_UNLOCK | config.STATUS_COMPLETE)

    run_monitor(case_id, 0)

    assert status_of(case_id) == str(config.STATUS_UNLOCK | config.STATUS_COMPLETE)


def test_mark_case_failure_skips_terminal_cases(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DIR_MODEL_CASE", str(tmp_path))
    case_id = "f" * 32
    make_case(str(tmp_path), case_id, config.STATUS_UNLOCK | config.STATUS_COMPLETE)

    assert modelProcess.mark_case_failure(case_id, "boom") is False
    assert status_of(case_id) == str(config.STATUS_UNLOCK | config.STATUS_COMPLETE)


def test_mark_case_failure_missing_case_is_tolerated(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DIR_MODEL_CASE", str(tmp_path))

    assert modelProcess.mark_case_failure("0" * 32, "boom") is False


def test_spawn_rejects_malformed_command():
    import pytest

    with pytest.raises(ValueError):
        modelProcess.spawn_case_subprocess(["only-program-path"])


def test_monitor_writes_error_when_it_is_watching_a_lock_case(tmp_path, monkeypatch):
    """LOCK（子进程连包装器都没进）被监控判死之后，必须变成 ERROR|UNLOCK，
    否则它会永远停在 LOCK，让 /v0/mc/status 一直回 pending。"""
    monkeypatch.setattr(config, "DIR_MODEL_CASE", str(tmp_path))
    case_id = "1a" * 16
    make_case(str(tmp_path), case_id, config.STATUS_LOCK)

    run_monitor(case_id, 3221225786)  # Windows 下被 Ctrl+C 杀掉的退出码

    runtime = runtime_of(case_id)
    assert status_of(case_id) == str(config.STATUS_ERROR | config.STATUS_UNLOCK)
    assert runtime["meta"]["exit-code"] == 3221225786
