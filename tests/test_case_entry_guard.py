"""子进程入口校验（缺陷：``open_case`` 返回 None 被直接传进模型函数）。

修复前：``__main__`` 里 ``run_risk_level_mcr(None)`` 让 wrapper 的第一行
``mcr.find_status(...)``（在 try 之外）抛 AttributeError，子进程退出码 1，
父进程既写不了 case 状态也看不出是哪个入参没了。
"""

import json
import os

import pytest

import config
from model import caseEntry, modelLauncher
from model.caseEntry import EXIT_CASE_UNAVAILABLE


def make_case(root: str, case_id: str, *, identity: bool = True) -> str:
    directory = os.path.join(root, case_id)
    os.makedirs(os.path.join(directory, "status"), exist_ok=True)
    os.makedirs(os.path.join(directory, "result"), exist_ok=True)
    if identity:
        with open(
            os.path.join(directory, "identity.json"), "w", encoding="utf-8"
        ) as file:
            json.dump({"model": "Test", "url": "/v0/t", "json": {}}, file)
    return directory


def test_available_cases_pass_silently(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(config, "DIR_MODEL_CASE", str(tmp_path))
    make_case(str(tmp_path), "a" * 32)
    make_case(str(tmp_path), "b" * 32)

    caseEntry.require_cases(["a" * 32, "b" * 32], "Test Model")

    assert capsys.readouterr().out == ""


def test_missing_case_directory_exits_with_dedicated_code(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(config, "DIR_MODEL_CASE", str(tmp_path))
    make_case(str(tmp_path), "a" * 32)

    with pytest.raises(SystemExit) as info:
        caseEntry.require_cases(["a" * 32, "b" * 32], "Test Model")

    assert info.value.code == EXIT_CASE_UNAVAILABLE
    out = capsys.readouterr().out
    # 必须指出是第几个、哪个 id、为什么——否则现场只剩一句 AttributeError
    assert "#2" in out
    assert "b" * 32 in out
    assert "does not exist" in out


def test_shell_without_identity_is_reported_as_shell(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(config, "DIR_MODEL_CASE", str(tmp_path))
    # 删除时被句柄占用而跳过的残壳：目录还在，identity.json 没了
    make_case(str(tmp_path), "c" * 32, identity=False)

    with pytest.raises(SystemExit) as info:
        caseEntry.require_cases(["c" * 32], "Test Model")

    assert info.value.code == EXIT_CASE_UNAVAILABLE
    assert "without identity.json" in capsys.readouterr().out


def test_empty_argv_is_rejected(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DIR_MODEL_CASE", str(tmp_path))

    with pytest.raises(SystemExit) as info:
        caseEntry.require_cases(["a" * 32, ""], "Test Model")

    assert info.value.code == EXIT_CASE_UNAVAILABLE


def test_controller_wrapper_rejects_none_case(tmp_path, monkeypatch, capsys):
    """兜底：即使某个脚本漏了入口校验，也不能把 None 传进模型函数。"""
    monkeypatch.setattr(config, "DIR_MODEL_CASE", str(tmp_path))
    executed = []

    @modelLauncher.model_status_controller_sync
    def run_case(mcr):
        executed.append(mcr)

    with pytest.raises(SystemExit) as info:
        run_case(None)

    assert info.value.code == EXIT_CASE_UNAVAILABLE
    assert executed == []
    assert "received mcr=None" in capsys.readouterr().out


def test_monitor_maps_exit_code_to_infra_reason(tmp_path, monkeypatch):
    """父进程要能把这个退出码翻译成「输入 case 不可用」，而不是 code 1。"""
    monkeypatch.setattr(config, "DIR_MODEL_CASE", str(tmp_path))
    case_id = "e" * 32
    directory = make_case(str(tmp_path), case_id)
    with open(os.path.join(directory, "status", str(config.STATUS_LOCK)), "w") as file:
        file.write("OK")

    import io

    from model import modelProcess

    class FakeProcess:
        pid = 1

        def poll(self):
            return EXIT_CASE_UNAVAILABLE

        def wait(self):
            return EXIT_CASE_UNAVAILABLE

    log_path = modelProcess.get_child_log_path(case_id)
    modelProcess.monitor_case_subprocess(FakeProcess(), case_id, log_path, io.StringIO())

    with open(os.path.join(directory, "runtime.json"), encoding="utf-8") as file:
        runtime = json.load(file)
    assert "input case was unavailable" in runtime["message"]
    assert runtime["meta"]["exit-code"] == EXIT_CASE_UNAVAILABLE
    assert runtime["meta"]["infra-error"] is True
