"""DEM 预览配色守门。

这个模块的产出是给**人眼**看的图，跑不崩不等于画对了。这里只锁三件容易在重构中
悄悄改坏的事：

- nodata（含 -9999 哨兵与 NaN）必须有明确标记：灰度档是纯红，分层设色档是透明
  （该档顶端本身就是砖红，纯红标空洞分不出来，且九成空白的河道 DEM 会被涂成一片红）；
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


def _rgb_part(rgb):
    """取 RGB 三通道，兼容灰度档的 3 通道与分层设色档的 4 通道。"""
    return np.asarray(rgb)[..., :3]


def _all_nodata_pixels_are_red(rgb, mask):
    """掩码内每个像素都等于 nodata 色。"""
    selected = _rgb_part(rgb)[mask]
    assert selected.size > 0
    expected = np.array(PREVIEW_NODATA, dtype=np.uint8)
    assert (selected == expected).all(), f"期望全为 {PREVIEW_NODATA}，实际出现 {np.unique(selected.reshape(-1, 3), axis=0)}"


def test_sentinel_becomes_transparent_and_valid_stays_gray():
    dem = np.array(
        [
            [-30.0, -9999.0, -20.0],
            [-9999.0, -10.0, -9999.0],
            [0.0, 5.0, -9999.0],
        ],
        dtype=np.float32,
    )
    rgb, stats = colorize(dem, nodata=-9999.0)

    assert rgb.shape == (3, 3, 4), f"灰度档也必须带 alpha，实际 {rgb.shape}"
    mask = dem == -9999.0
    # RGB 侧仍是 nodata 色（alpha 被忽略时也能认出空洞），alpha=0 才是真正的「透明」
    _all_nodata_pixels_are_red(rgb, mask)
    assert (rgb[..., 3][mask] == 0).all(), "空洞像素必须透明"
    assert (rgb[..., 3][~mask] == 255).all(), "有效像素必须不透明"

    valid = rgb[~mask]
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


def test_fully_empty_dem_stays_opaque_red_even_in_hypsometric():
    """「整张都空」是唯一刻意不透明的例外，别顺手统一成透明。

    全透明 PNG 和「图根本没加载出来」在界面上长得一模一样，会把「这份 DEM 没有数据」
    这条信息吞掉。
    """
    from model.demPreview import RAMP_HYPSOMETRIC

    dem = np.full((3, 3), -9999.0, dtype=np.float32)
    rgb, stats = colorize(dem, nodata=-9999.0, ramp=RAMP_HYPSOMETRIC)
    assert rgb.shape[2] == 3, "该分支刻意返回不透明 RGB"
    _all_nodata_pixels_are_red(rgb, np.ones((3, 3), dtype=bool))
    assert stats["nodata_ratio"] == 1.0


# ---------------------------------------------------------------- 分层设色

def test_hypsometric_ramp_goes_blue_bed_to_red_bank():
    """河床（负高程）偏蓝、岸地（正高程）偏红，且随高程单调变暖。

    这个是给人看的：配色反了不会报错，但「深处是红色」会让整张图的读法反过来。
    """
    from model.demPreview import RAMP_HYPSOMETRIC

    dem = np.array([[-55.0, -20.0, 0.0, 4.0, 10.0]], dtype=np.float32)
    rgb, stats = colorize(dem, nodata=None, ramp=RAMP_HYPSOMETRIC)

    assert stats["min"] == -55.0 and stats["max"] == 10.0
    deep, mid, zero, shallow, high = (rgb[0, i].astype(int) for i in range(5))
    # 蓝色分量随高程递减、红色分量递增
    assert deep[2] > deep[0] * 2, f"最深处应当是蓝的，实际 {tuple(deep)}"
    assert high[0] > high[2] * 2, f"最高处应当是红的，实际 {tuple(high)}"
    # 蓝通道不是单调的（中段的浅蓝最「蓝」、顶端又转深砖红），只锁两端的冷暖与中段渐暖
    assert deep[2] > high[2] + 40, (tuple(deep), tuple(high))
    reds = [deep[0], mid[0], zero[0], shallow[0]]
    assert reds == sorted(reds), reds


def test_hypsometric_nodata_is_transparent():
    """分层设色档的 nodata 必须透明（灰度档同理，见上一条）。"""
    from model.demPreview import RAMP_HYPSOMETRIC

    dem = np.array([[5.0, -9999.0], [np.nan, 6.0]], dtype=np.float32)
    rgb, _ = colorize(dem, nodata=-9999.0, ramp=RAMP_HYPSOMETRIC)

    assert rgb.shape == (2, 2, 4), f"分层设色档必须是 RGBA，实际 {rgb.shape}"
    invalid = np.array([[False, True], [True, False]])
    alpha = rgb[..., 3]
    assert (alpha[invalid] == 0).all(), f"空洞像素的 alpha 必须为 0，实际 {alpha[invalid]}"
    assert (alpha[~invalid] == 255).all(), f"有效像素必须完全不透，实际 {alpha[~invalid]}"
    # RGB 侧仍保留 nodata 色：万一将来有人按通道读图，也能认出这是空洞而不是深蓝
    _all_nodata_pixels_are_red(rgb, invalid)


def test_hypsometric_legend_colors_are_not_confusable_with_nodata():
    """自检：调色板里任何一端都不能等于 nodata 色，否则透明与否都会看错。"""
    from model.demPreview import ramp_stops

    nodata = tuple(int(v) for v in PREVIEW_NODATA)
    for _, color in ramp_stops("hypsometric"):
        triple = tuple(int(color[i : i + 2], 16) for i in (1, 3, 5))
        assert triple != nodata, f"色带断点 {color} 与 nodata 色重合"


def test_hypsometric_clamps_out_of_range_elevations():
    """超出断点范围的值取端点色，不出现黑边/反色。"""
    from model.demPreview import RAMP_HYPSOMETRIC

    rgb, _ = colorize(np.array([[-5000.0, 5000.0]], dtype=np.float32), nodata=None, ramp=RAMP_HYPSOMETRIC)
    assert tuple(int(v) for v in rgb[0, 0, :3]) == (8, 48, 107)      # -80 m 的深蓝
    assert tuple(int(v) for v in rgb[0, 1, :3]) == (158, 28, 38)     # 20 m 的砖红
    assert (rgb[..., 3] == 255).all(), "没有空洞时不应出现半透明像素"


def test_unknown_ramp_raises():
    import pytest

    with pytest.raises(ValueError):
        colorize(np.zeros((2, 2), dtype=np.float32), nodata=None, ramp="rainbow")


def test_ramp_stops_are_exposed_for_the_legend():
    from model.demPreview import RAMP_GRAY, RAMP_HYPSOMETRIC, ramp_stops

    assert ramp_stops(RAMP_GRAY) == []
    stops = ramp_stops(RAMP_HYPSOMETRIC)
    assert len(stops) >= 5
    assert [s[0] for s in stops] == sorted(s[0] for s in stops)
    assert all(isinstance(s[1], str) and s[1].startswith("#") and len(s[1]) == 7 for s in stops)


# ---------------------------------------------------------------- 缓存

def test_cache_key_changes_when_the_drawing_style_version_bumps(monkeypatch, tmp_path):
    """改画法必须换键，否则用户会一直看到旧图。

    实测踩过：把分层设色的 nodata 从纯红改成透明后，图例已经写「透明」，地图上仍是
    一整块红 —— 磁盘缓存命中，key 里根本没有「画法」这一维。
    """
    from model import demPreview

    dem = tmp_path / "a.tif"
    dem.write_bytes(b"x")
    before = demPreview._cache_key(dem, 640, demPreview.RAMP_GRAY)
    monkeypatch.setattr(demPreview, "PREVIEW_STYLE_VERSION", demPreview.PREVIEW_STYLE_VERSION + 1)
    after = demPreview._cache_key(dem, 640, demPreview.RAMP_GRAY)
    assert before != after


def test_cache_key_separates_ramps(tmp_path):
    from model.demPreview import RAMP_GRAY, RAMP_HYPSOMETRIC, _cache_key

    dem = tmp_path / "a.tif"
    dem.write_bytes(b"x")
    assert _cache_key(dem, 640, RAMP_GRAY) != _cache_key(dem, 640, RAMP_HYPSOMETRIC)
