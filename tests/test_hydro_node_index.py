"""水动力节点字典（D2）的 fail-soft 与三条不变量。

设计前提是「任何异常都不比没有这个模块更差」，所以每种失效形态都要断言
``lookup`` 返回「不可用」而不是抛异常。

其中「升序」这条必须靠测试主动构造：顺序错了不会报错，只会让上层把
``nodes[0]`` / ``nodes[-1]`` 当成最小值 / 最大值，把 water_qs 映射到错误的档，
属于静默算错。字符串化、结构残缺同理。
"""

import json
import time

import config
from util import db, hydro_node_index

_KEY = ("Yangtze", "standard", "dc")


def _payload(rows, version: int = 1, age: float = 0.0) -> dict:
    return {"version": version, "refreshed_at": time.time() - age, "rows": rows}


def _install(monkeypatch, tmp_path, payload) -> str:
    path = tmp_path / "hydrodynamic_node_index.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr(config, "DIR_HYDRO_NODE_INDEX", str(path))
    monkeypatch.setattr(config, "MODEL_HYDRO_NODE_INDEX_ENABLED", True)
    monkeypatch.setattr(config, "MODEL_HYDRO_NODE_INDEX_TTL_SECONDS", 300)
    return str(path)


class _FakeContext:
    def __init__(self, rows):
        self._rows = rows

    def __enter__(self):
        return object(), _FakeCursor(self._rows)

    def __exit__(self, *exc_info):
        return False


class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows

    def execute(self, *_args, **_kwargs):
        return None

    def fetchall(self):
        return self._rows


def _stub_db(monkeypatch, rows) -> None:
    monkeypatch.setattr(db, "get_db_cursor", lambda **_kwargs: _FakeContext(rows))


# ---------------------------------------------------------------------------
# 正常路径
# ---------------------------------------------------------------------------


def test_clean_file_is_used(monkeypatch, tmp_path) -> None:
    _install(monkeypatch, tmp_path, _payload([list(_KEY) + [[10000]]]))

    loaded, nodes = hydro_node_index.lookup(*_KEY)

    assert loaded is True
    assert nodes == [10000]


def test_empty_list_is_a_fact_not_a_miss(monkeypatch, tmp_path) -> None:
    """命中但值为空列表 ⇒ 该分组确实没数据，直接返回 []，不能回落（回落会建连）。"""
    _install(monkeypatch, tmp_path, _payload([list(_KEY) + [[]]]))

    loaded, nodes = hydro_node_index.lookup(*_KEY)

    assert loaded is True
    assert nodes == []


def test_missing_key_falls_back(monkeypatch, tmp_path) -> None:
    """「字典没覆盖」≠「库里没数据」⇒ 必须回落 SQL 拿真值。"""
    _install(monkeypatch, tmp_path, _payload([["Other", "standard", "dc", [10000]]]))

    assert hydro_node_index.lookup(*_KEY) == (False, None)


# ---------------------------------------------------------------------------
# 三条不变量：读到脏内容时规范化或被拒绝
# ---------------------------------------------------------------------------


def test_out_of_order_rows_are_sorted(monkeypatch, tmp_path) -> None:
    """不变量 2：顺序错了不报错只会算错，必须被规范化成升序。"""
    _install(monkeypatch, tmp_path, _payload([list(_KEY) + [[84000, 10000, 50000]]]))

    loaded, nodes = hydro_node_index.lookup(*_KEY)

    assert loaded is True
    assert nodes == [10000, 50000, 84000]


def test_duplicate_values_are_collapsed(monkeypatch, tmp_path) -> None:
    """不变量 3：重复值会破坏中点分割的 index 对应关系。"""
    _install(monkeypatch, tmp_path, _payload([list(_KEY) + [[10000, 10000, 84000]]]))

    _, nodes = hydro_node_index.lookup(*_KEY)

    assert nodes == [10000, 84000]


def test_string_numbers_are_coerced_to_int(monkeypatch, tmp_path) -> None:
    """不变量 1：若返回字符串，上层 `i_water_qs <= nodes[0]` 会抛 TypeError。"""
    _install(monkeypatch, tmp_path, _payload([list(_KEY) + [["10000", "84000"]]]))

    _, nodes = hydro_node_index.lookup(*_KEY)

    assert nodes == [10000, 84000]
    assert all(isinstance(node, int) for node in nodes)


def test_non_numeric_value_invalidates_whole_file(monkeypatch, tmp_path) -> None:
    """非数字 ⇒ 整份作废，不允许「用了一半」。"""
    _install(monkeypatch, tmp_path, _payload([list(_KEY) + [["abc"]]]))

    assert hydro_node_index.lookup(*_KEY) == (False, None)


def test_malformed_row_invalidates_whole_file(monkeypatch, tmp_path) -> None:
    _install(
        monkeypatch,
        tmp_path,
        _payload([list(_KEY) + [[10000]], ["Yangtze", "standard", "zc"]]),
    )

    assert hydro_node_index.lookup(*_KEY) == (False, None)


# ---------------------------------------------------------------------------
# 失效形态：全部 fail-soft 回落 SQL
# ---------------------------------------------------------------------------


def test_missing_file_falls_back(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(config, "DIR_HYDRO_NODE_INDEX", str(tmp_path / "nope.json"))
    monkeypatch.setattr(config, "MODEL_HYDRO_NODE_INDEX_ENABLED", True)

    assert hydro_node_index.lookup(*_KEY) == (False, None)


def test_corrupted_json_falls_back(monkeypatch, tmp_path) -> None:
    path = tmp_path / "broken.json"
    path.write_text('{"version":1,"rows":[["Yangtze"', encoding="utf-8")
    monkeypatch.setattr(config, "DIR_HYDRO_NODE_INDEX", str(path))
    monkeypatch.setattr(config, "MODEL_HYDRO_NODE_INDEX_ENABLED", True)

    assert hydro_node_index.lookup(*_KEY) == (False, None)


def test_version_mismatch_falls_back(monkeypatch, tmp_path) -> None:
    _install(monkeypatch, tmp_path, _payload([list(_KEY) + [[10000]]], version=2))

    assert hydro_node_index.lookup(*_KEY) == (False, None)


def test_expired_ttl_falls_back(monkeypatch, tmp_path) -> None:
    _install(monkeypatch, tmp_path, _payload([list(_KEY) + [[10000]]], age=3600))

    assert hydro_node_index.lookup(*_KEY) == (False, None)


def test_disabled_switch_ignores_healthy_file(monkeypatch, tmp_path) -> None:
    _install(monkeypatch, tmp_path, _payload([list(_KEY) + [[10000]]]))
    monkeypatch.setattr(config, "MODEL_HYDRO_NODE_INDEX_ENABLED", False)

    assert hydro_node_index.lookup(*_KEY) == (False, None)


# ---------------------------------------------------------------------------
# refresh_now：写盘、失败保留旧文件、空结果不覆盖
# ---------------------------------------------------------------------------


def _row(domain, set_name, tidal_level, water_qs) -> dict:
    return {
        "dataset_domain": domain,
        "set_name": set_name,
        "tidal_level": tidal_level,
        "water_qs": water_qs,
    }


def test_refresh_writes_usable_file(monkeypatch, tmp_path) -> None:
    path = tmp_path / "node_index.json"
    monkeypatch.setattr(config, "DIR_HYDRO_NODE_INDEX", str(path))
    monkeypatch.setattr(config, "MODEL_HYDRO_NODE_INDEX_ENABLED", True)
    monkeypatch.setattr(config, "MODEL_HYDRO_NODE_INDEX_TTL_SECONDS", 300)
    _stub_db(
        monkeypatch,
        [
            _row("Yangtze", "standard", "dc", "10000"),
            _row("Yangtze", "standard", "1998-2023", "84000"),
        ],
    )

    assert hydro_node_index.refresh_now() is True
    assert hydro_node_index.lookup("Yangtze", "standard", "dc") == (True, [10000])
    assert hydro_node_index.lookup("Yangtze", "standard", "1998-2023") == (
        True,
        [84000],
    )

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["version"] == 1
    assert [row[0:3] for row in payload["rows"]] == [
        ["Yangtze", "standard", "1998-2023"],
        ["Yangtze", "standard", "dc"],
    ]


def test_refresh_failure_keeps_previous_file(monkeypatch, tmp_path) -> None:
    path = _install(monkeypatch, tmp_path, _payload([list(_KEY) + [[10000]]]))
    before = open(path, encoding="utf-8").read()

    def _explode(**_kwargs):
        raise RuntimeError("database is down")

    monkeypatch.setattr(db, "get_db_cursor", _explode)

    assert hydro_node_index.refresh_now() is False
    assert open(path, encoding="utf-8").read() == before
    assert hydro_node_index.lookup(*_KEY) == (True, [10000])


def test_refresh_empty_result_keeps_previous_file(monkeypatch, tmp_path) -> None:
    """空结果通常是「表被清空 / 连错库」，写下去会让所有 key 命中空列表。"""
    path = _install(monkeypatch, tmp_path, _payload([list(_KEY) + [[10000]]]))
    before = open(path, encoding="utf-8").read()
    _stub_db(monkeypatch, [])

    assert hydro_node_index.refresh_now() is False
    assert open(path, encoding="utf-8").read() == before


def test_refresh_skipped_when_disabled(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(config, "DIR_HYDRO_NODE_INDEX", str(tmp_path / "x.json"))
    monkeypatch.setattr(config, "MODEL_HYDRO_NODE_INDEX_ENABLED", False)

    assert hydro_node_index.refresh_now() is False


# ---------------------------------------------------------------------------
# 等价性：字典路径必须与 SQL 路径逐字相同（D2 的核心验收项）
# ---------------------------------------------------------------------------


class _DictCursor(_FakeCursor):
    """db_ops 原 SQL 路径用 dict_cursor=True，按 row["water_qs"] 取值。"""

    pass


def _stub_sql(monkeypatch, water_qs_values) -> None:
    rows = [{"water_qs": value} for value in water_qs_values]
    monkeypatch.setattr(
        db, "get_db_cursor", lambda **_kwargs: _FakeContext(rows)
    )


def test_cached_path_equals_sql_path(monkeypatch, tmp_path) -> None:
    """脏文件（乱序 + 字符串 + 重复）规范化后，结果必须与 SQL 完全相同。"""
    from util import db_ops

    _stub_sql(monkeypatch, ["84000", "10000", "50000"])

    monkeypatch.setattr(config, "MODEL_HYDRO_NODE_INDEX_ENABLED", False)
    sql_result = db_ops.get_available_hydrodynamic_nodes(*_KEY)
    assert sql_result == [10000, 50000, 84000]

    _install(
        monkeypatch,
        tmp_path,
        _payload([list(_KEY) + [["84000", 10000, "50000", 10000]]]),
    )
    monkeypatch.setattr(config, "MODEL_HYDRO_NODE_INDEX_ENABLED", True)
    cached_result = db_ops.get_available_hydrodynamic_nodes(*_KEY)

    assert cached_result == sql_result


def test_db_ops_falls_back_when_file_missing(monkeypatch, tmp_path) -> None:
    from util import db_ops

    _stub_sql(monkeypatch, ["10000"])
    monkeypatch.setattr(config, "DIR_HYDRO_NODE_INDEX", str(tmp_path / "nope.json"))
    monkeypatch.setattr(config, "MODEL_HYDRO_NODE_INDEX_ENABLED", True)

    # 文件不存在 ⇒ 走 SQL，结果与关闭开关时一致。
    assert db_ops.get_available_hydrodynamic_nodes(*_KEY) == [10000]


def test_db_ops_empty_cached_list_does_not_hit_sql(monkeypatch, tmp_path) -> None:
    """命中空列表是事实，不能回落；否则每次都要白建一次连接。"""
    from util import db_ops

    calls = []
    monkeypatch.setattr(
        db,
        "get_db_cursor",
        lambda **_kwargs: calls.append(1) or _FakeContext([{"water_qs": "10000"}]),
    )
    _install(monkeypatch, tmp_path, _payload([list(_KEY) + [[]]]))
    monkeypatch.setattr(config, "MODEL_HYDRO_NODE_INDEX_ENABLED", True)

    assert db_ops.get_available_hydrodynamic_nodes(*_KEY) == []
    assert calls == []
