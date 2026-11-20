"""DEM 预览配色守门。

这个模块的产出是给**人眼**看的图，跑不崩不等于画对了。这里只锁三件容易在重构中
悄悄改坏的事：

- nodata（含 -9999 哨兵与 NaN）必须是纯红，不能被灰度拉伸吞掉；
- 高程要「低暗高亮」，反了不会报错，但会把河槽画成山脊；
- 平坦 DEM / 全空洞 DEM 不能除零或整片涂黑。

不依赖真实 DEM 文件：直接喂构造数组，与 ``test_region_flush_equivalence`` 同一思路。
"""

import os
import sys

import numpy as np

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from model.demPreview import (  # noqa: E402
    PREVIEW_DARK,
    PREVIEW_LIGHT,
    PREVIEW_NODATA,
    colorize,
)


def _all_nodata_pixels_are_red(rgb, mask):
    """掩码内每个像素都等于 nodata 色。"""
    selected = rgb[mask]
    assert selected.size > 0
    expected = np.array(PREVIEW_NODATA, dtype=np.uint8)
    assert (selected == expected).all(), f"期望全为 {PREVIEW_NODATA}，实际出现 {np.unique(selected.reshape(-1, 3), axis=0)}"


def test_sentinel_becomes_red_and_valid_stays_gray():
    dem = np.array(
        [
            [-30.0, -9999.0, -20.0],
            [-9999.0, -10.0, -9999.0],
            [0.0, 5.0, -9999.0],
        ],
        dtype=np.float32,
    )
    rgb, stats = colorize(dem, nodata=-9999.0)

    mask = dem == -9999.0
    _all_nodata_pixels_are_red(rgb, mask)

    valid_mask = ~mask
    valid = rgb[valid_mask]
    # 有效像素必须是灰的（R=G=B），否则说明红色渗到了有效区
    assert (valid[:, 0] == valid[:, 1]).all()
    assert (valid[:, 1] == valid[:, 2]).all()

    # 9 个像素里 -9999 出现 4 次
    assert abs(stats["nodata_ratio"] - 4 / 9) < 1e-6


def test_nan_is_treated_as_nodata():
    dem = np.array([[-5.0, np.nan], [np.nan, -1.0]], dtype=np.float32)
    rgb, _ = colorize(dem, nodata=None)
    _all_nodata_pixels_are_red(rgb, ~np.isfinite(dem))


def test_low_elevation_is_darker_than_high():
    dem = np.linspace(-50.0, 10.0, 100, dtype=np.float32).reshape(10, 10)
    rgb, _ = colorize(dem, nodata=-9999.0)

    darkest = int(rgb[0, 0, 0])   # 最低高程
    brightest = int(rgb[-1, -1, 0])  # 最高高程
    assert darkest < brightest, "高程到灰度的方向反了：低处应更暗"
    assert darkest >= PREVIEW_DARK
    assert brightest <= PREVIEW_LIGHT


def test_flat_dem_does_not_crash_and_is_gray():
    dem = np.full((4, 4), 3.5, dtype=np.float32)
    rgb, stats = colorize(dem, nodata=-9999.0)
    assert stats["nodata_ratio"] == 0.0
    assert (rgb[:, :, 0] == rgb[:, :, 1]).all() and (rgb[:, :, 1] == rgb[:, :, 2]).all()
    # 全部同值 ⇒ 落在拉伸区间内，一定是可辨的中灰而不是极端黑/白
    assert PREVIEW_DARK <= int(rgb[0, 0, 0]) <= PREVIEW_LIGHT


def test_fully_empty_dem_is_all_red():
    dem = np.full((3, 3), -9999.0, dtype=np.float32)
    rgb, stats = colorize(dem, nodata=-9999.0)
    _all_nodata_pixels_are_red(rgb, np.ones((3, 3), dtype=bool))
    assert stats["nodata_ratio"] == 1.0
    assert stats["min"] is None and stats["max"] is None
