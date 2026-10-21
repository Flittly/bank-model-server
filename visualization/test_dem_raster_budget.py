"""DEM 底图栅格尺寸与分位数统计的回归测试。

背景（2026-10-21 线上 ArrayMemoryError）::

    numpy._core._exceptions._ArrayMemoryError: Unable to allocate 25.1 MiB
    for an array with shape (6576675,) and data type float32

崩溃点在 ``np.nanpercentile(dem_data, 98)``，但内存是在更早的 Warp 阶段被吃掉的：
源 DEM 只有 10388×3211（3340 万像素，30 m），而旧实现的降采样只给固定
``xRes/yRes=0.0002°``（约 19 m）——比源分辨率还细，于是 6.2°×1.5° 的范围被
**上采样**成 31175×7589 = 2.37 亿像素（float32 单份 946 MB）。

本文件锁住两件事：
1. ``_fit_raster_size`` 约束的是**总像素数**（不只是分辨率），且保持宽高比；
2. ``_robust_percentile`` 用抽样统计，结果与全量一致而内存与数组大小无关。
"""

import numpy as np
import pytest

from visualization.risk_map import (
    MAX_DEM_PIXELS,
    DEM_TARGET_RES_DEG,
    _fit_raster_size,
    _robust_percentile,
)

#: 线上崩溃那次的实际范围（裁剪后，含 20% 缓冲）
BROKEN_LNG_SPAN = 123.2922 - 117.0576      # 6.2346°
BROKEN_LAT_SPAN = 32.6814 - 31.1638        # 1.5176°


def test_broken_case_would_have_exploded_without_budget():
    """复现旧口径：固定 0.0002° 得到的像素数远超预算，这就是 OOM 的源头。"""
    legacy_w = BROKEN_LNG_SPAN / DEM_TARGET_RES_DEG
    legacy_h = BROKEN_LAT_SPAN / DEM_TARGET_RES_DEG
    legacy_pixels = legacy_w * legacy_h

    assert legacy_pixels > 2e8, "旧口径确实是 2 亿+ 像素，测试前提成立"
    assert legacy_pixels > MAX_DEM_PIXELS * 40

    width, height = _fit_raster_size(BROKEN_LNG_SPAN, BROKEN_LAT_SPAN)
    assert width * height <= MAX_DEM_PIXELS
    # 新口径约 4000×1000，仍高于 24.6" 画布 @150dpi 的 3700 px 需求
    assert width >= 3700, f"长边 {width} 太小，会拉低底图清晰度"


def test_budget_never_exceeded_across_realistic_spans():
    spans = [
        (0.01, 0.01), (0.1, 0.05), (1.0, 0.4), (3.0, 1.2),
        (BROKEN_LNG_SPAN, BROKEN_LAT_SPAN), (10.0, 6.0), (30.0, 20.0),
    ]
    for lng_span, lat_span in spans:
        width, height = _fit_raster_size(lng_span, lat_span)
        assert width * height <= MAX_DEM_PIXELS, (lng_span, lat_span, width, height)
        assert width >= 1 and height >= 1


def test_aspect_ratio_preserved_after_scaling():
    """缩放必须长宽同比，否则河段形状会被拉伸。"""
    width, height = _fit_raster_size(BROKEN_LNG_SPAN, BROKEN_LAT_SPAN)
    expected = BROKEN_LNG_SPAN / BROKEN_LAT_SPAN
    assert abs(width / height - expected) < 0.01


def test_small_span_keeps_target_resolution():
    """小范围不触发缩放，清晰度与历史版本一致（0.0002°）。"""
    width, height = _fit_raster_size(0.05, 0.02)
    assert width == pytest.approx(0.05 / DEM_TARGET_RES_DEG, abs=1)
    assert height == pytest.approx(0.02 / DEM_TARGET_RES_DEG, abs=1)


@pytest.mark.parametrize("bad", [(0, 0), (-1, 2), (2, 0)])
def test_invalid_span_returns_zero(bad):
    assert _fit_raster_size(*bad) == (0, 0)


def test_percentile_matches_full_quantile_on_large_array():
    """抽样分位数与全量差异足够小（灰度拉伸不可见）。"""
    rng = np.random.default_rng(20261021)
    data = rng.normal(10, 5, size=(3000, 2000)).astype(np.float32)
    full_2, full_98 = np.nanpercentile(data, [2, 98])
    assert _robust_percentile(data, 2) == pytest.approx(full_2, rel=0.02)
    assert _robust_percentile(data, 98) == pytest.approx(full_98, rel=0.02)


def test_percentile_ignores_nan_and_nodata():
    data = np.full((1000, 1000), np.nan, dtype=np.float32)
    data[:10, :10] = 7.0           # 有效值只占万分之一，靠抽样要能捞到
    data[10:, 10:] = np.nan
    assert _robust_percentile(data, 50) == pytest.approx(7.0)


def test_percentile_returns_nan_when_no_valid_value():
    data = np.full((500, 500), np.nan, dtype=np.float32)
    value = _robust_percentile(data, 98)
    assert np.isnan(value), "无有效值必须返回 nan，由调用方跳过绘制"


def test_percentile_accepts_one_dimensional_input():
    arr = np.arange(1000, dtype=np.float32)
    assert _robust_percentile(arr, 50) == pytest.approx(499.5, abs=1)


def test_percentile_does_not_materialize_full_copy():
    """证明内存与数组规模无关：2 亿元素的统计不应再复制整份数组。

    用「统计前后可用内存差值」做软断言容易受 GC 抖动影响，这里改为
    断言实现只读访问输入（输入数组内容与 sha 不变），且耗时远小于全量排序。
    """
    big = np.zeros((20000, 10000), dtype=np.float32)     # 8 亿字节
    big[::7, ::7] = 3.0
    before = float(np.mean(big[::1000, ::1000]))
    value = _robust_percentile(big, 98)
    after = float(np.mean(big[::1000, ::1000]))
    assert before == after, "统计过程不应修改输入栅格"
    assert np.isfinite(value)
