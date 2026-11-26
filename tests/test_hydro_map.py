"""D3 映射逻辑收敛：4 份重复实现合并为 `util.hydro_map.map_water_qs`。

为什么这些用例不能省：库里 5 个分组**每组只有 1 档**，所以 `len(nodes) == 1`
那个分支之外的三条取档路径（越界夹取 / 命中 / 中点分割）在生产中从未执行过。
它们写错不会抛异常，只会把流量映射到相邻的错误档 —— 只能靠测试钉住。

另一半是结构断言：4 个脚本里必须只剩「引用 + 别名」，不能再长出本地定义，
否则下次改映射又会分叉。
"""

import importlib
from pathlib import Path

import pytest

from util import db_ops, hydro_map

_SCRIPTS = [
    "modelResource/multipleIndicators/riskLevel.py",
    "modelResource/multipleIndicators/antiImpactSpeed.py",
    "modelResource/multipleIndicators/waterLevelFluctuation.py",
    "modelResource/numericalModel/hydrodynamic.py",
]

_ROOT = Path(__file__).resolve().parents[1]


def _fake(mapping, calls=None):
    """按 (domain, set, tidal) 返回预设档位；未配置的 key 返回空（触发回落）。"""

    def fake(dataset_domain, set_name, tidal_level):
        if calls is not None:
            calls.append((dataset_domain, set_name, tidal_level))
        return list(mapping.get((dataset_domain, set_name, tidal_level), []))

    return fake


def test_single_node_collapses_any_water_qs(monkeypatch):
    """库里现状：唯一档 ⇒ 模板填什么流量都被夹成那一档，且 key 只查一次。"""
    calls = []
    monkeypatch.setattr(
        db_ops,
        "get_available_hydrodynamic_nodes",
        _fake({("Yangtze", "standard", "zc"): [10000]}, calls),
    )

    result = hydro_map.map_water_qs("Yangtze", "2023", "standard", "zc", "62000")

    assert result == ("zc", 10000, "Yangtze", "2023", "standard")
    assert calls == [("Yangtze", "standard", "zc")]
    # 非回落分支不得改写 year
    assert result[3] == "2023"


@pytest.mark.parametrize(
    "water_qs, expected",
    [
        ("5000", 10000),  # <= nodes[0]
        ("10000", 10000),
        ("90000", 50000),  # >= nodes[-1]
        ("50000", 50000),
        ("30000", 30000),  # 命中
        ("15000", 10000),  # 中点 19999 左侧
        ("25000", 30000),  # 距 19999 更近，落右侧
        ("35000", 30000),  # 距 39999 更近，落左侧
        ("45000", 50000),  # 中点 39999 右侧
    ],
)
def test_multi_node_branches(monkeypatch, water_qs, expected):
    """多档时的四条路径。中点 = (nodes[i] + nodes[i+1]) / 2 - 1。"""
    monkeypatch.setattr(
        db_ops,
        "get_available_hydrodynamic_nodes",
        _fake({("Yangtze", "standard", "zc"): [10000, 30000, 50000]}),
    )

    _, mapped, _, _, _ = hydro_map.map_water_qs(
        "Yangtze", "2023", "standard", "zc", water_qs
    )

    assert mapped == expected


@pytest.mark.parametrize(
    "tidal_level, expected",
    [
        (0.0, "xc"),
        (1.49, "xc"),
        (1.5, "zc"),  # 边界：`< 1.5` 为假 ⇒ 落 zc
        (2.29, "zc"),
        (2.3, "dc"),  # 边界：`< 2.3` 为假 ⇒ 落 dc
        ("1998-2023", "1998-2023"),  # 字符串原样，不参与分档
    ],
)
def test_tidal_level_mapping(monkeypatch, tidal_level, expected):
    monkeypatch.setattr(
        db_ops,
        "get_available_hydrodynamic_nodes",
        _fake(
            {
                ("Yangtze", "standard", "xc"): [10000],
                ("Yangtze", "standard", "zc"): [10000],
                ("Yangtze", "standard", "dc"): [10000],
                ("Yangtze", "standard", "1998-2023"): [84000],
            }
        ),
    )

    tidal, _, _, _, _ = hydro_map.map_water_qs(
        "Yangtze", "2023", "standard", tidal_level, "5000"
    )

    assert tidal == expected


def test_fallback_rewrites_segment_set_and_year(monkeypatch):
    """首查为空 ⇒ 用 DEFAULT_DOMAIN + standard + year="default" 再查一次。"""
    calls = []
    monkeypatch.setattr(
        db_ops,
        "get_available_hydrodynamic_nodes",
        _fake({("Yangtze", "standard", "zc"): [10000]}, calls),
    )

    result = hydro_map.map_water_qs("NJ", "2018", "other", "zc", "5000")

    assert result == ("zc", 10000, "Yangtze", "default", "standard")
    assert calls == [("NJ", "other", "zc"), ("Yangtze", "standard", "zc")]


def test_raises_when_fallback_also_empty(monkeypatch):
    monkeypatch.setattr(
        db_ops, "get_available_hydrodynamic_nodes", _fake({})
    )

    with pytest.raises(ValueError) as excinfo:
        hydro_map.map_water_qs("NJ", "2018", "other", "zc", "5000")

    message = str(excinfo.value)
    assert "segment=Yangtze" in message
    assert "set=standard" in message
    assert "tidal_level=zc" in message


def test_scripts_share_the_same_function_object():
    """`riskLevel` 里的旧名必须指向同一个对象，否则 monkeypatch 打桩会失灵。"""
    module = importlib.import_module("modelResource.multipleIndicators.riskLevel")

    assert module.get_mapped_tidal_level_and_mapped_water_qs is hydro_map.map_water_qs


@pytest.mark.parametrize("relative_path", _SCRIPTS)
def test_no_local_duplicate_definitions(relative_path):
    """4 个脚本只许引用，不许再存一份实现（含已删死的 get_qs_and_level）。"""
    source = (_ROOT / relative_path).read_text(encoding="utf-8")

    assert "def get_mapped_tidal_level_and_mapped_water_qs" not in source
    assert "def get_qs_and_level" not in source
    assert "from util.hydro_map import map_water_qs" in source
    assert "get_mapped_tidal_level_and_mapped_water_qs = map_water_qs" in source
