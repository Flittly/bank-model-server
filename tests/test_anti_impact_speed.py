"""Ky（抗冲流速）链路的两个回归点。

背景：所有断面的 Ky 曾恒等于 2.4231。2.4231 是回归式
`Ky = 2.4231 * exp(-0.547 * max_v)` 的**前因子**，恒等于它就等价于 `max_v` 恒为 0。
根因有两层：

1. `geo2proj` 用 `AddPoint(lat, lng)` + `return (GetY, GetX)` 两次交换来抵消 GDAL 3
   对 EPSG:4326 的 lat/lon 轴序，对 4326 恰好正确、**对 3857 会翻成垃圾坐标**；
   而 `section.json` 的 `points_v` 正是 3857，于是「最近水文点」的排序退化成常量，
   所有断面取到同一个点。
2. 该点的 u/v 全是 -99999，`calculate_velocity` 把哨兵直接归零 ⇒ 全 0 ⇒ Ky = 2.4231，
   且这个值还会被判成第 1 档（最轻）。

这里把两处都钉住：投影值、合理区间门禁、公式与分档、全 nodata 的上报字段。
"""

import math
import os
import sys

import pytest

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from modelResource.multipleIndicators.antiImpactSpeed import (
    KY_INTERCEPT,
    compute_anti_impact_speed,
)
from modelResource.numericalModel.flowFieldVelocity import (
    _plausible_in_2437,
    geo2proj,
)


# 长江口某断面的实测坡脚（EPSG:3857），正确投影值按传统轴序算得
FOOT_3857 = (13355639.0, 3767113.0)
FOOT_2437 = (497644.1054826789, 3545103.457202622)
# 旧实现的 3857 分支输出，落在任何一个合理包围盒之外
BROKEN_2437 = (-1080867.8001354896, 9894925.17204124)

# riskLevel 模板里的 Ky 阈值：降序，越高越轻
KY_THRESHOLDS = [1.7, 1.35, 1.0]


def test_geo2proj_keeps_4326_behaviour():
    """4326 一直是正确的，改轴序不能把它改坏。"""
    x, y = geo2proj(120.376326, 32.004437, 4326)
    assert x == pytest.approx(535493.7817, abs=1e-3)
    assert y == pytest.approx(3542445.4559, abs=1e-3)


def test_geo2proj_handles_3857():
    """Ky 恒为 2.4231 的根因：3857 分支曾返回 BROKEN_2437。"""
    x, y = geo2proj(*FOOT_3857, 3857)
    assert x == pytest.approx(FOOT_2437[0], abs=1e-3)
    assert y == pytest.approx(FOOT_2437[1], abs=1e-3)


def test_plausible_range_gate():
    """投影失败要当场抛错，而不是拿垃圾坐标去查最近点。"""
    assert _plausible_in_2437(*FOOT_2437)
    assert not _plausible_in_2437(*BROKEN_2437)


def test_ky_formula_and_tier():
    # 逐时刻取模后最大值为 1.0 ⇒ Ky = 2.4231 * exp(-0.547) ≈ 1.4022
    ky, risk, quality = compute_anti_impact_speed(
        {"us": [1.0, 0.0], "vs": [0.0, 0.0]}, KY_THRESHOLDS
    )
    assert ky == pytest.approx(KY_INTERCEPT * math.exp(-0.547))
    assert ky == pytest.approx(1.4022, abs=1e-4)
    # 1.0 < 1.4022 <= 1.35 不成立、> 1.35 ⇒ 第 2 档
    assert risk == [0, 1, 0, 0]
    assert quality["max_velocity"] == pytest.approx(1.0)
    assert quality["velocity_available"] is True


def test_ky_is_not_constant_across_different_velocities():
    """同一套阈值下，流速不同必须给出不同的 Ky —— 否则说明取数又退化成常量。"""
    values = []
    for speed in (0.2, 1.0, 3.0):
        ky, _, _ = compute_anti_impact_speed(
            {"us": [speed], "vs": [0.0]}, KY_THRESHOLDS
        )
        values.append(round(ky, 6))
    assert len(set(values)) == 3
    # 流速越大 Ky 越小（越不安全）
    assert values == sorted(values, reverse=True)


def test_all_nodata_degrades_to_intercept_and_is_flagged():
    """全时段 nodata 时 Ky 必然是 2.4231；至少要能被识别出来，不能只回一个数。"""
    ky, risk, quality = compute_anti_impact_speed(
        {"us": [-99999.0] * 3, "vs": [-99999.0] * 3}, KY_THRESHOLDS
    )
    assert ky == pytest.approx(KY_INTERCEPT)
    assert risk == [1, 0, 0, 0]  # 2.4231 > 1.7 ⇒ 第 1 档（最轻）
    assert quality["velocity_available"] is False
    assert quality["nodata_samples"] == 3
    assert quality["samples"] == 3
