"""多指标「恰好等于最高档阈值」的边界回归。

背景：slopeRate / heightDifference / nearshoreFlush 三处第 4 档判定写的是
`elif X > risk_threshold[2]`，而第 3 档用的是 `elif X < risk_threshold[2]`。
两者在 X == risk_threshold[2] 时同时为假，`risk` 保持初始的 `[]`：

    riskLevel._one_hot_tier([]) -> None
    matrices["indicator_tiers"][指标] = None
    matrices["input_completeness"]["resolved_indicators"] 少 1

即「指标恰好压在门槛上」被静默记成「这个指标没有数据」。

修复：把第 4 档改成 `>=`（与 flowEquivalent / riskLevel / soilComposition /
waterLevelFluctuation 的既有写法一致）。不用 `else` 是刻意的 —— NaN 时
`>=` 仍然为假会暴露成缺项，而 `else` 会把「算不出来」静默记成「最高风险」。

antiImpactSpeed 是递减型判定（`> t0 / > t1 / > t2 / <= t2`），互补完整，
不在本次修复范围内，这里也补一条断言把它锁住。
"""

import json
import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from modelResource.multipleIndicators.heightDifference import compute_height_difference
from modelResource.multipleIndicators.nearshoreFlush import compute_nearshore_flush
from modelResource.multipleIndicators.riskLevel import _one_hot_tier
from modelResource.multipleIndicators.slopeRate import compute_slope_rate

# 三档门限：tier1 < t0、tier2 < t1、tier3 < t2、tier4 >= t2
THRESHOLD = [0.1, 0.3, 0.5]


def _write_section(tmp_path, payload):
    out = tmp_path / "result"
    out.mkdir(parents=True, exist_ok=True)
    (out / "section.json").write_text(
        json.dumps(payload, ensure_ascii=False), encoding="utf-8"
    )
    return str(out)


def _assert_single_tier(risk, expected_index):
    assert risk == [1 if i == expected_index else 0 for i in range(4)]


# ------------------------------------------------------------------ Sa（坡比）


@pytest.mark.parametrize(
    "sa,expected_index",
    [
        (0.05, 0),  # 1 档
        (0.1, 1),  # 恰好 t0
        (0.3, 2),  # 恰好 t1
        (0.5, 3),  # 恰好 t2 —— 修复前这里是 []
        (0.9, 3),  # 远高于 t2
    ],
)
def test_slope_rate_tier_boundaries(tmp_path, sa, expected_index):
    # Sa = -min(Sa_v)，所以写 -sa 才能得到 sa
    out = _write_section(tmp_path, {"Sa_v": [-0.01, -sa, -0.02]})
    got_sa, risk = compute_slope_rate(out, THRESHOLD)
    assert got_sa == pytest.approx(sa)
    _assert_single_tier(risk, expected_index)


def test_slope_rate_exact_threshold_is_not_missing(tmp_path):
    """回归：修复前 Sa 恰好等于 t2 时 risk == []，下游记成缺项。"""
    out = _write_section(tmp_path, {"Sa_v": [-0.5]})
    _, risk = compute_slope_rate(out, THRESHOLD)
    assert risk != []
    assert _one_hot_tier(risk) == 4


# --------------------------------------------------------------- Zb（高差）


@pytest.mark.parametrize(
    "zb,expected_index",
    [
        (5.0, 0),
        (10.0, 1),
        (30.0, 2),
        (40.0, 3),  # 恰好 t2 —— 修复前这里是 []
        (80.0, 3),
    ],
)
def test_height_difference_tier_boundaries(tmp_path, zb, expected_index):
    # Zb = -points[deepest_index][2]
    out = _write_section(
        tmp_path,
        {"points": [[0, 0, -1.0], [1, 1, -zb]], "deepest_index": 1},
    )
    got_zb, risk = compute_height_difference(out, [10.0, 20.0, 40.0])
    assert got_zb == pytest.approx(zb)
    _assert_single_tier(risk, expected_index)


def test_height_difference_exact_threshold_is_not_missing(tmp_path):
    out = _write_section(
        tmp_path, {"points": [[0, 0, -40.0]], "deepest_index": 0}
    )
    _, risk = compute_height_difference(out, [10.0, 20.0, 40.0])
    assert risk != []
    assert _one_hot_tier(risk) == 4


# ------------------------------------------------------------- Ln（冲刷）


@pytest.mark.parametrize(
    "depth,expected_index",
    [
        (0.04, 0),  # Ln = 0.04 < 0.05
        (0.05, 1),  # Ln = 0.05 恰好 t0
        (0.10, 2),  # Ln = 0.10 恰好 t1
        (0.20, 3),  # Ln = 0.20 恰好 t2 —— 修复前这里是 []
        (0.60, 3),
    ],
)
def test_nearshore_flush_tier_boundaries(tmp_path, depth, expected_index):
    out = _write_section(
        tmp_path,
        {"points": [[0, 0, -depth], [1, 1, -0.001]], "deepest_index": 0},
    )
    # 固定跨 1 个月，使 Ln 数值上等于 depth
    got_ln, risk = compute_nearshore_flush(
        out, [0.05, 0.10, 0.20], "2020-02-01", "2020-01-01"
    )
    assert got_ln == pytest.approx(depth)
    assert risk == [1 if i == expected_index else 0 for i in range(4)]


def test_nearshore_flush_exact_threshold_is_not_missing(tmp_path):
    out = _write_section(
        tmp_path, {"points": [[0, 0, -0.20]], "deepest_index": 0}
    )
    _, risk = compute_nearshore_flush(out, [0.05, 0.10, 0.20], "2020-02-01", "2020-01-01")
    assert risk != []
    assert _one_hot_tier(risk) == 4


# ------------------------------------------------- 未修复的同族写法也要锁住


def test_anti_impact_speed_inverse_branch_is_complete():
    """递减型判定 `> t0 / > t1 / > t2 / <= t2` 本身互补，恰好等于 t2 时落 4 档。"""
    threshold = [100.0, 50.0, 20.0]
    ky = 20.0
    risk = []
    if ky > threshold[0]:
        risk = [1, 0, 0, 0]
    elif ky > threshold[1]:
        risk = [0, 1, 0, 0]
    elif ky > threshold[2]:
        risk = [0, 0, 1, 0]
    elif ky <= threshold[2]:
        risk = [0, 0, 0, 1]
    assert risk == [0, 0, 0, 1]
