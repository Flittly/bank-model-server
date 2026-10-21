"""PQ（造床流量当量）数据接入回归测试。

覆盖 2026-10-20 修复的三类问题：
1. 年份字符串夹取：`max(years[0], min(years[-1], "default"))` 会得到 "2023"，把 2018 年的断面
   算成 2023 年的当量（pq.json["2023"] = 0.0）。
2. 上层把「对齐水动力工况后的 year」传给 PQ，断面自己的年份丢失（"default"）。
3. 数据文件找不到时静默回退，且回退后不留任何来源信息，导致「数据没接上」不可见。
"""

import json
import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import config
from modelResource.multipleIndicators.flowEquivalent import (
    _normalize_year,
    _pick_year,
    _resolve_pq_source,
    compute_flow_equivalent,
)

REAL_PQ = json.load(open(config.DIR_RESOURCE_PQ_TEMPLATE, encoding="utf-8"))
THRESHOLD = [0.5, 1.0, 2.3]


# ------------------------------------------------------------------ 年份归一


@pytest.mark.parametrize(
    "raw,expected",
    [
        (2023, 2023),
        (2018, 2018),
        ("2023", 2023),
        ("2018", 2018),
        ("2023-04-01", 2023),
        ("2018/04/01", 2018),
        ("202311", 2023),
        ("20230401", 2023),
        (" 2023 ", 2023),
        ("default", None),
        ("", None),
        (None, None),
        ("abc", None),
        (True, None),
    ],
)
def test_normalize_year(raw, expected):
    assert _normalize_year(raw) == expected


# ------------------------------------------------------------------ 选年份


def test_pick_year_exact_hit():
    key, requested, exact = _pick_year(REAL_PQ, 2018)
    assert (key, requested, exact) == ("2018", 2018, True)


def test_pick_year_default_uses_latest_but_marks_inexact():
    """请求年份不可识别时取最新年份，但必须标记不精确（旧行为是静默夹取）。"""
    key, requested, exact = _pick_year(REAL_PQ, None)
    assert key == "2023"
    assert requested is None
    assert exact is False


def test_pick_year_out_of_range_takes_nearest():
    key, requested, exact = _pick_year(REAL_PQ, 2035)
    assert (key, requested, exact) == ("2023", 2035, False)
    key, _, exact = _pick_year(REAL_PQ, 2005)
    assert (key, exact) == ("2010", False)


def test_pick_year_rejects_source_without_year_keys():
    with pytest.raises(ValueError):
        _pick_year({"a": 1}, 2020)


# ------------------------------------------------------------------ 数据源解析


def test_resolve_source_falls_back_to_global_file():
    """生产库里 segment 是 YZ/NJ/Yangtze，json/ 下只有 Mzs，必然走 template 兜底。"""
    for segment in ("YZ", "NJ", "Yangtze", "Mzs", None):
        path, source = _resolve_pq_source(segment, 2018)
        assert source in ("segment-year", "segment", "template")
        assert path.endswith(os.path.join("PQ", "pq.json"))


def test_resolve_source_prefers_segment_year_then_segment(tmp_path, monkeypatch):
    root = tmp_path / "json"
    (root / "YZ" / "2018").mkdir(parents=True)
    (root / "YZ" / "2018" / "standard" / "PQ").mkdir(parents=True)
    (root / "YZ" / "2018" / "standard" / "PQ" / "pq.json").write_text(
        json.dumps({"2018": 9.9}), encoding="utf-8"
    )
    monkeypatch.setattr(config, "DIR_RESOURCE_JSON", str(root))

    path, source = _resolve_pq_source("YZ", 2018)
    assert source == "segment-year"
    assert path == str(root / "YZ" / "2018" / "standard" / "PQ" / "pq.json")


def test_resolve_source_raises_when_nothing_exists(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DIR_RESOURCE_JSON", str(tmp_path))
    monkeypatch.setattr(config, "DIR_RESOURCE_PQ_TEMPLATE", str(tmp_path / "nope.json"))
    with pytest.raises(FileNotFoundError) as excinfo:
        _resolve_pq_source("YZ", 2018)
    assert "tried=" in str(excinfo.value)


# ------------------------------------------------------------------ 端到端取值


@pytest.mark.parametrize(
    "year,expected_pq",
    [
        ("2018", 0.29),
        ("2019", 1.68),
        ("2021", 1.35),
        ("2023", 0.0),
        (2023, 0.0),
        ("2023-04-01", 0.0),
        ("202311", 0.0),
    ],
)
def test_compute_pq_follows_section_year(year, expected_pq):
    pq, risk, meta = compute_flow_equivalent("Yangtze", year, THRESHOLD)
    assert pq == expected_pq
    assert meta["year-exact"] is True
    assert meta["source"] == "template"


def test_compute_pq_2018_is_not_downgraded_to_2023():
    """回归：修复前 2018 的断面走到 "default" 再被夹成 2023，PQ 恒为 0.0。"""
    pq_2018, risk_2018, _ = compute_flow_equivalent("YZ", "2018", THRESHOLD)
    pq_2023, risk_2023, _ = compute_flow_equivalent("YZ", "2023", THRESHOLD)
    assert pq_2018 == 0.29
    assert pq_2023 == 0.0
    assert pq_2018 != pq_2023
    # 0.29 < 0.5 => 1 档；0.0 < 0.5 => 1 档（档位相同但取值可区分）
    assert risk_2018 == [1, 0, 0, 0]
    assert risk_2023 == [1, 0, 0, 0]


def test_compute_pq_tiers_follow_thresholds():
    # 用自定义阈值把 pq=0.29 推到 3 档，验证 risk 数组与 PQ 取值确实联动
    pq, risk, _ = compute_flow_equivalent("YZ", "2018", [0.1, 0.2, 0.25])
    assert pq == 0.29
    assert risk == [0, 0, 0, 1]


def test_compute_pq_reports_unresolvable_year_as_inexact():
    pq, _, meta = compute_flow_equivalent("YZ", "default", THRESHOLD)
    assert pq == 0.0
    assert meta["year-exact"] is False
    assert meta["year-requested"] is None
    assert meta["year-used"] == 2023


# ------------------------------------------------------- 溯源信息透传到结果表


def test_every_year_in_file_round_trips():
    """逐年份对齐 pq.json —— 只要计算真的读的是这个文件，这里就成立。

    这条断言同时挡住三种回归：换掉数据源（source 变了）、字符串夹取年份（值错位）、
    静默返回默认值（值对不上文件）。文件被改动而没人同步时也会立刻失败。
    """
    for raw_year, expected in REAL_PQ.items():
        pq, _, meta = compute_flow_equivalent("Yangtze", raw_year, THRESHOLD)
        assert pq == expected, f"year={raw_year} 期望 {expected}，实际 {pq}"
        assert meta["year-used"] == int(raw_year)
        assert meta["year-exact"] is True
        assert meta["source"] == "template"
        assert meta["path"].endswith("resource/json/Mzs/2023/standard/PQ/pq.json")


def test_pq_uses_pre_mapping_year_and_ky_still_uses_mapped(monkeypatch):
    """PQ 必须拿断面自己的年份，不能拿「对齐水动力工况后」的年份。

    生产里 `zc` 潮位 + NJ/YZ 岸段没有水文节点时，映射函数会把 year 改成 `"default"`、
    segment 改成 `"Yangtze"`。旧代码把这个值传给 PQ，`"default"` 被夹成 `"2023"`
    ⇒ 全库 PQ = pq.json["2023"] = 0.0。这里把映射结果打成 `"default"` 来锁定契约：
    PQ 仍收原始年份，Ky/Zd 仍收映射值。
    """
    from modelResource.multipleIndicators import riskLevel as rl

    mapped = ("zc", "30000", "Yangtze", "default", "standard")
    monkeypatch.setattr(
        rl, "get_mapped_tidal_level_and_mapped_water_qs", lambda *a, **k: mapped
    )

    captured = {}

    class _MCR:
        def __init__(self, api):
            self.id = "fake-" + api

    class _Launcher:
        def __init__(self, api):
            self.api = api

        def run(self, payload, deps=None):
            captured[self.api] = payload
            return _MCR(self.api)

    monkeypatch.setattr(
        rl.model.launcher, "fetch_model_from_API", lambda api: _Launcher(api)
    )
    monkeypatch.setattr(
        rl.model.ModelCaseReference, "create", staticmethod(lambda *a, **k: _MCR("risk"))
    )

    rl.PARSING(
        None,
        {
            "segment": "NJ",
            "current-timepoint": "2018-04-01",
            "set": "standard",
            "water-qs": "10000",
            "tidal-level": "zc",
            "bench-id": "demo",
            "ref-id": "demo",
            "hs": 1.0,
            "hc": 1.0,
            "protection-level": "no",
            "control-level": "no",
            "comparison-timepoint": "2012-04-01",
            "risk-thresholds": "NONE",
            "section-geometry": {"coordinates": [[0, 0], [1, 1]]},
            "wRE": "NONE",
            "wNM": "NONE",
            "wGE": "NONE",
            "wRL": "NONE",
        },
        "/tmp",
        [],
    )

    pq_payload = captured[config.API_MI_FLOW_EQUIVALENT]
    assert pq_payload["segment"] == "NJ"
    assert pq_payload["year"] == "2018"

    # 水动力相关的指标仍然用映射后的值（不能被顺手改掉）
    assert captured[config.API_MI_ANTI_IMPACT_SPEED]["year"] == "default"
    assert captured[config.API_MI_WATER_LEVEL_FLUCTUATION]["year"] == "default"


def test_pq_data_quality_reaches_risk_level_matrices(tmp_path):
    """flow_equivalent 的 data-quality 必须落进 matrices，否则结果表里看不出 PQ 来自哪一年。"""
    from modelResource.multipleIndicators.riskLevel import get_risk_level

    specs = {
        "soil_composition.json": {"risk-level": [1, 0, 0, 0], "Dsed": 0.25},
        "slope_protection.json": {"risk-level": [0, 0, 0, 1], "PL": "no"},
        "load_control.json": {"risk-level": [0, 0, 0, 1], "LC": "no"},
        "height_difference.json": {"risk-level": [1, 0, 0, 0], "Zb": 5.0},
        "slope_rate.json": {"risk-level": [1, 0, 0, 0], "Sa": 0.01},
        "nearshore_flush.json": {"risk-level": [1, 0, 0, 0], "Ln": 0.01},
        "flow_equivalent.json": {
            "risk-level": [1, 0, 0, 0],
            "PQ": 0.29,
            "data-quality": {"year-used": 2018, "year-exact": True, "source": "template"},
        },
        "anti_impact_speed.json": {"risk-level": [1, 0, 0, 0], "Ky": 0.9},
        "water_level_fluctuation.json": {"risk-level": [1, 0, 0, 0], "Zd": 0.02},
    }
    paths = []
    for name, content in specs.items():
        path = tmp_path / name
        path.write_text(json.dumps(content, ensure_ascii=False), encoding="utf-8")
        paths.append(str(path))

    _, _, matrices, raw_values = get_risk_level(
        paths,
        [0.25, 0.32, 0.43],
        [0.32, 0.36, 0.32],
        [0.6, 0.2, 0.2],
        [0.32, 0.43, 0.25],
        [0.25, 0.5, 0.75],
        {"protection-level": "no", "control-level": "no"},
    )

    assert raw_values["PQ"] == 0.29
    assert matrices["PQ_data_quality"]["year-used"] == 2018
    assert matrices["indicator_tiers"]["PQ"] == 1
