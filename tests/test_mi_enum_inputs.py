"""PL / LC 两个枚举型指标的契约测试。

覆盖三件事：
1. 枚举 -> 档位映射与前端下拉四处选项逐字对应；
2. 非法值（空串 / None / 未知枚举）必须抛 ValueError，不许静默兜底成第 1 档；
3. 上游 riskLevel 在子模型「不回带 PL/LC」的历史响应下，能从请求入参回退，
   并且输出 indicator_tiers / input_completeness。
"""

import json
import os
import sys

import numpy as np
import pytest

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from modelResource.multipleIndicators.loadControl import compute_load_control
from modelResource.multipleIndicators.riskLevel import _one_hot_tier, get_risk_level
from modelResource.multipleIndicators.slopeProtection import compute_slope_protection


# ---------------------------------------------------------------- 1. 枚举映射

@pytest.mark.parametrize(
    "level,expected",
    [
        ("systemic", [1, 0, 0, 0]),
        ("normal", [0, 1, 0, 0]),
        ("low", [0, 0, 1, 0]),
        ("no", [0, 0, 0, 1]),
    ],
)
def test_protection_level_maps_to_expected_tier(level, expected):
    assert compute_slope_protection(level) == expected


@pytest.mark.parametrize(
    "level,expected",
    [
        ("strict", [1, 0, 0, 0]),
        ("normal", [0, 1, 0, 0]),
        ("low", [0, 0, 1, 0]),
        ("no", [0, 0, 0, 1]),
    ],
)
def test_control_level_maps_to_expected_tier(level, expected):
    assert compute_load_control(level) == expected


def test_enum_input_is_case_and_space_insensitive():
    assert compute_slope_protection("  SYSTEMIC ") == [1, 0, 0, 0]
    assert compute_load_control("\tNo\n") == [0, 0, 0, 1]


# ---------------------------------------------------------------- 2. 非法值

@pytest.mark.parametrize("bad", ["", "   ", None, "unknown", "strong", 0])
def test_invalid_protection_level_raises(bad):
    """兜底成第 1 档等于把「未填报」当成「护岸最强」，必须显式失败。"""
    with pytest.raises(ValueError, match="invalid protection-level"):
        compute_slope_protection(bad)


@pytest.mark.parametrize("bad", ["", "   ", None, "unknown", 0])
def test_invalid_control_level_raises(bad):
    with pytest.raises(ValueError, match="invalid control-level"):
        compute_load_control(bad)


def test_empty_enum_would_break_hstack_without_guard():
    """回归证据：空列表进 np.array 会变成 (1,0)，hstack 必炸。"""
    empty = np.array([[]]).T
    assert empty.shape == (0, 1)
    with pytest.raises(ValueError):
        np.hstack((np.array([[1, 0, 0, 0]]).T, np.array([[1, 0, 0, 0]]).T, empty))


# ---------------------------------------------------------------- 3. 档位反推

@pytest.mark.parametrize(
    "column,expected",
    [
        ([1, 0, 0, 0], 1),
        ([0, 1, 0, 0], 2),
        ([0, 0, 1, 0], 3),
        ([0, 0, 0, 1], 4),
        ([0, 0, 0, 0], None),
    ],
)
def test_one_hot_tier(column, expected):
    assert _one_hot_tier(np.array(column, dtype=float)) == expected


# ---------------------------------------------------------------- 4. 上游回退

def _write_response(directory, name, content):
    path = os.path.join(directory, name)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(content, f, ensure_ascii=False)
    return path


def _build_response_list(tmp_path, pl_response, lc_response):
    """按 get_risk_level 期望的顺序（见 riskLevel.py 的索引注释）造 9 份响应。"""
    specs = [
        ("soil_composition.json", {"risk-level": [1, 0, 0, 0], "Dsed": 0.25}),
        ("slope_protection.json", pl_response),
        ("load_control.json", lc_response),
        ("height_difference.json", {"risk-level": [0, 0, 0, 1], "Zb": 42.0}),
        ("slope_rate.json", {"risk-level": [0, 0, 1, 0], "Sa": 0.6}),
        ("nearshore_flush.json", {"risk-level": [0, 1, 0, 0], "Ln": 0.31}),
        ("flow_equivalent.json", {"risk-level": [1, 0, 0, 0], "PQ": 0.0}),
        ("anti_impact_speed.json", {"risk-level": [1, 0, 0, 0], "Ky": 2.423}),
        ("water_level_fluctuation.json", {"risk-level": [1, 0, 0, 0], "Zd": 0.027}),
    ]
    return [
        _write_response(tmp_path, name, content) for name, content in specs
    ]


WEIGHTS = dict(wRE=[0.25, 0.32, 0.43], wNM=[0.32, 0.36, 0.32], wGE=[0.6, 0.2, 0.2])
WRL = [0.32, 0.43, 0.25]
THRESHOLD = [0.25, 0.5, 0.75]


def test_raw_values_fall_back_to_request_json_for_legacy_cases(tmp_path):
    """历史 Slope Protection / Load Control 响应没有 PL/LC 字段，必须从入参回退。"""
    response_list = _build_response_list(
        tmp_path,
        pl_response={"risk-level": [0, 0, 0, 1]},  # 故意不带 PL
        lc_response={"risk-level": [0, 0, 0, 1]},  # 故意不带 LC
    )
    request_json = {"protection-level": "no", "control-level": "no"}

    _, _, matrices, raw_values = get_risk_level(
        response_list, *[WEIGHTS[k] for k in ("wRE", "wNM", "wGE")],
        WRL, THRESHOLD, request_json,
    )

    assert raw_values["PL"] == "no"
    assert raw_values["LC"] == "no"


def test_raw_values_prefer_model_response_over_request(tmp_path):
    """新 case 模型回带了 PL/LC，应当以模型响应为准。"""
    response_list = _build_response_list(
        tmp_path,
        pl_response={"risk-level": [0, 0, 0, 1], "PL": "no"},
        lc_response={"risk-level": [0, 1, 0, 0], "LC": "normal"},
    )
    request_json = {"protection-level": "systemic", "control-level": "strict"}

    _, _, _, raw_values = get_risk_level(
        response_list, *[WEIGHTS[k] for k in ("wRE", "wNM", "wGE")],
        WRL, THRESHOLD, request_json,
    )

    assert raw_values["PL"] == "no"
    assert raw_values["LC"] == "normal"


def test_indicator_tiers_and_completeness_are_emitted(tmp_path):
    response_list = _build_response_list(
        tmp_path,
        pl_response={"risk-level": [0, 0, 0, 1]},
        lc_response={"risk-level": [0, 0, 0, 1]},
    )
    request_json = {"protection-level": "no", "control-level": "no"}

    _, _, matrices, raw_values = get_risk_level(
        response_list, *[WEIGHTS[k] for k in ("wRE", "wNM", "wGE")],
        WRL, THRESHOLD, request_json,
    )

    tiers = matrices["indicator_tiers"]
    assert tiers["PL"] == 4
    assert tiers["LC"] == 4
    assert tiers["Dsed"] == 1
    assert tiers["Zb"] == 4
    assert set(tiers) == {"Ky", "PQ", "Zd", "Sa", "Ln", "Zb", "Dsed", "PL", "LC"}

    completeness = matrices["input_completeness"]
    assert completeness["total_indicators"] == 9
    assert completeness["resolved_indicators"] == 9
    assert completeness["missing_raw_values"] == []
    assert completeness["enum_inputs"] == ["PL", "LC"]


def test_missing_raw_values_are_reported(tmp_path):
    """子模型连原始量都不回时，input_completeness 必须把缺失项列出来。"""
    response_list = _build_response_list(
        tmp_path,
        pl_response={"risk-level": [0, 0, 0, 1]},
        lc_response={"risk-level": [0, 0, 0, 1]},
    )
    # 不给 request_json，PL/LC 无从回退
    _, _, matrices, raw_values = get_risk_level(
        response_list, *[WEIGHTS[k] for k in ("wRE", "wNM", "wGE")], WRL, THRESHOLD
    )

    assert raw_values["PL"] is None
    assert raw_values["LC"] is None
    assert matrices["input_completeness"]["missing_raw_values"] == ["LC", "PL"]
