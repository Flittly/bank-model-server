"""case 目录残缺（残壳）时的自愈与 fail-soft 行为。

故障形态：删 case 时只要有一个文件删不掉（Windows 上子进程/监控线程还占着
``child.log`` 或 ``lock``），目录就仍然「存在」但只剩残文件。旧实现把
``os.path.exists(directory)`` 当作「已初始化完毕」，于是 identity.json 与
status/ 永远补不上，随后 get_status() 读 status/ 抛 FileNotFoundError，
HTTP 直接 500；而 case id 是 md5(url+json)，同一断面每次重试都落回同一个
空壳，永久失败且重启不自愈。
"""

import os

import config
import model


def _isolate(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(config, "DIR_MODEL_CASE", str(tmp_path / "case"))
    monkeypatch.setattr(config, "DIR_GLOBALE_FILE_LOCKER", str(tmp_path / "global.lock"))
    os.makedirs(config.DIR_MODEL_CASE, exist_ok=True)


def _create(payload: dict):
    return model.ModelCaseReference.create(
        "/v0/test/shell", payload, "Test Model", "/tmp/test.py", []
    )


def _shell(directory: str, leftovers: list[str]) -> None:
    """把目录删到只剩指定残文件，模拟 rmtree 部分失败。"""
    import shutil

    shutil.rmtree(directory, ignore_errors=True)
    os.makedirs(directory, exist_ok=True)
    for name in leftovers:
        with open(os.path.join(directory, name), "w", encoding="utf-8") as file:
            file.write("residue\n")


def test_shell_with_only_child_log_is_repaired(monkeypatch, tmp_path) -> None:
    _isolate(monkeypatch, tmp_path)

    victim = _create({"a": 1})
    _shell(victim.directory, ["child.log"])

    repaired = _create({"a": 1})

    assert repaired.id == victim.id
    assert repaired.get_status() == config.STATUS_UNLOCK
    assert os.path.exists(os.path.join(repaired.directory, "identity.json"))
    assert os.path.isdir(os.path.join(repaired.directory, "status"))
    assert os.path.isdir(os.path.join(repaired.directory, "time"))
    # 残文件不能被当成「要清理的垃圾」清掉：它可能是排障现场
    assert os.path.exists(os.path.join(repaired.directory, "child.log"))


def test_shell_with_only_lock_is_repaired(monkeypatch, tmp_path) -> None:
    _isolate(monkeypatch, tmp_path)

    victim = _create({"a": 2})
    _shell(victim.directory, ["lock"])

    repaired = _create({"a": 2})

    assert repaired.get_status() == config.STATUS_UNLOCK
    assert os.path.exists(os.path.join(repaired.directory, "identity.json"))


def test_missing_status_only_repairs_without_overwriting_identity(
    monkeypatch, tmp_path
) -> None:
    _isolate(monkeypatch, tmp_path)

    victim = _create({"a": 3})
    identity = os.path.join(victim.directory, "identity.json")
    os.remove(os.path.join(victim.directory, "status", str(config.STATUS_UNLOCK)))
    os.rmdir(os.path.join(victim.directory, "status"))
    mtime_before = os.path.getmtime(identity)

    repaired = _create({"a": 3})

    assert repaired.get_status() == config.STATUS_UNLOCK
    # identity.json 里带 dependencies，而 dependencies 不参与 case id；
    # 覆盖会改掉已完成 case 的依赖关系，所以只能补缺、不能重写。
    assert os.path.getmtime(identity) == mtime_before


def test_completed_case_is_not_reset_to_pending(monkeypatch, tmp_path) -> None:
    _isolate(monkeypatch, tmp_path)

    done = _create({"a": 4})
    done.make_response({"result": 42})
    done.update_status(config.STATUS_UNLOCK | config.STATUS_COMPLETE)
    done.set_runtime(stage="completed", progress=100, status="completed")

    again = _create({"a": 4})

    assert again.get_status() == config.STATUS_UNLOCK | config.STATUS_COMPLETE
    assert (again.get_runtime_info(again.id) or {}).get("stage") == "completed"
    assert (again.make_response() or {}).get("result") == 42


def test_get_status_on_vanished_case_returns_none(monkeypatch, tmp_path) -> None:
    _isolate(monkeypatch, tmp_path)

    ghost = model.ModelCaseReference.__new__(model.ModelCaseReference)
    ghost.id = "0" * 32
    ghost.directory = os.path.join(config.DIR_MODEL_CASE, "0" * 32)
    ghost.local_file_locker = os.path.join(ghost.directory, "lock")

    assert ghost.get_status() == config.STATUS_NONE
    assert ghost.find_status(config.STATUS_COMPLETE) is False


def test_update_status_heals_missing_status_directory(monkeypatch, tmp_path) -> None:
    _isolate(monkeypatch, tmp_path)

    victim = _create({"a": 6})
    import shutil

    shutil.rmtree(os.path.join(victim.directory, "status"))

    victim.update_status(config.STATUS_LOCK | config.STATUS_RUNNING)

    assert victim.get_status() == config.STATUS_LOCK | config.STATUS_RUNNING
