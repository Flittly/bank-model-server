"""地理等比例出图的回归测试。

旧实现固定 figsize=(14,10)：东西走向的长河段（任务级合并图实测地理宽高比 9:1）
被纵向拉伸 6 倍以上，河段形状严重失真。本组测试钉死三件事：

1. compute_map_layout 的分档与 clamp；
2. set_aspect 的经度 cos 校正（纬度越高，1° 经度屏幕上必须越窄）；
3. 成图像素宽高比 ≈ 地理宽高比（行为级，端到端）。
"""
import json
import math
import os
import tempfile

from visualization import risk_map
from visualization.risk_map import (
    KM_PER_LAT_DEG, KM_PER_LNG_DEG_EQUATOR,
    compute_map_layout, map_font_scale,
    MAX_EDGE, BASE_SHORT_EDGE, MIN_EDGE, MAX_SANE_RATIO,
)

_EPS = 1e-9


def _ratio(lng_span, lat_span, lat_center):
    """参考实现：地理宽高比（东西 km / 南北 km）。"""
    w = lng_span * KM_PER_LNG_DEG_EQUATOR * math.cos(math.radians(lat_center))
    h = lat_span * KM_PER_LAT_DEG
    return w / h


def test_layout_square_range_keeps_short_edge():
    """近方形范围（比例≈cos32°≈0.85，走竖长分支）：窄边固定在短边基准。"""
    figsize, aspect, ratio, clamped = compute_map_layout(0.10, 0.10, 32.0)
    assert abs(ratio - math.cos(math.radians(32.0))) < 0.02      # 方形范围 + cos 校正
    assert figsize[0] == BASE_SHORT_EDGE                          # 窄边（宽）固定
    assert figsize[1] == BASE_SHORT_EDGE / ratio                  # 长边按比例
    assert clamped is False


def test_layout_truly_square_at_equator():
    """赤道上的方形范围：cos 校正消失，画布也应是方形。"""
    figsize, aspect, ratio, clamped = compute_map_layout(0.10, 0.10, 0.0)
    assert abs(ratio - KM_PER_LNG_DEG_EQUATOR / KM_PER_LAT_DEG) < 0.01
    assert abs(figsize[0] - figsize[1]) < 0.05


def test_layout_wide_range_scales_width():
    ratio = _ratio(0.60, 0.04, 32.0)          # ≈ 12.6:1 的东西向长条
    figsize, aspect, geo_ratio, clamped = compute_map_layout(0.60, 0.04, 32.0)
    assert abs(geo_ratio - ratio) < 0.05
    assert figsize[1] == BASE_SHORT_EDGE       # 短边（高）固定
    assert figsize[0] == BASE_SHORT_EDGE * geo_ratio or figsize[0] == MAX_EDGE
    # 画布宽高比与地理宽高比同向：宽 > 高
    assert figsize[0] > figsize[1]


def test_layout_tall_range_scales_height():
    figsize, aspect, geo_ratio, clamped = compute_map_layout(0.02, 0.30, 32.0)
    assert geo_ratio < 1
    assert figsize[0] == BASE_SHORT_EDGE       # 短边（宽）固定
    assert figsize[1] > figsize[0]


def test_layout_overlong_range_clamps_and_flags():
    ratio = _ratio(1.20, 0.02, 32.0)          # ≈ 56:1 超长
    figsize, aspect, geo_ratio, clamped = compute_map_layout(1.20, 0.02, 32.0)
    assert clamped is True
    assert figsize[0] == MAX_EDGE              # 长边被上限截断
    assert figsize[1] == BASE_SHORT_EDGE
    assert geo_ratio > MAX_SANE_RATIO


def test_layout_edges_never_below_floor():
    """再小的范围，画布边也不低于下限（放不下标题图例的图没有意义）。"""
    figsize, _, _, _ = compute_map_layout(0.001, 0.001, 32.0)
    assert figsize[0] >= MIN_EDGE and figsize[1] >= MIN_EDGE


def test_aspect_applies_cosine_correction():
    """经度每度的屏幕宽度必须随纬度升高按 cos 收窄——这是等比例的核心。"""
    _, aspect_eq, _, _ = compute_map_layout(1.0, 1.0, 0.0)
    _, aspect_60, _, _ = compute_map_layout(1.0, 1.0, 60.0)

    # 赤道处：1°lat 与 1°lng 的真实公里数之比
    assert abs(aspect_eq - KM_PER_LAT_DEG / KM_PER_LNG_DEG_EQUATOR) < 1e-6
    # 60° 纬度处：经度每度只有一半长，aspect 应约等于赤道的 2 倍
    assert abs(aspect_60 / aspect_eq - 1.0 / math.cos(math.radians(60.0))) < 0.01


def test_font_scale_grows_only_for_large_canvas():
    assert map_font_scale((7, 7)) == 1.0
    assert map_font_scale((14, 10)) == 1.0
    assert map_font_scale((20, 7)) == 1.0            # 起步线，仍是 1.0
    assert 1.0 < map_font_scale((28, 7)) < 1.5       # 大图放大
    assert map_font_scale((36, 7)) == 1.5            # 36/20=1.8 超过封顶值


def test_font_scale_capped():
    assert map_font_scale((50, 50)) <= 1.5


# ---------------------------------------------------------------------------
# 行为级：成图像素宽高比 ≈ 地理宽高比
# ---------------------------------------------------------------------------

def _sections_along(lng0, lat0, lng_span, count, level=2, lat_span=0.004):
    """沿东西向布一排断面；lat_span 是单条断面的长度（南北向）。"""
    out = []
    for i in range(count):
        lng = lng0 + lng_span * i / (count - 1)
        out.append({
            'section_id': f'S{i}', 'section_name': f'断面{i}',
            'bank_id': 'B1', 'risk_level': level,
            'start_lng': lng, 'start_lat': lat0,
            'end_lng': lng, 'end_lat': lat0 + lat_span,
        })
    return out


def test_output_pixel_ratio_matches_geographic_ratio():
    """端到端：成图（含标题/图例边饰）的像素宽高比必须接近地理宽高比。

    旧实现固定 14×10 时，3:1 的范围画出来是 1.4:1——河段被纵向拉胖一倍。
    取 3.2:1 的中等范围（未触发长边 clamp，画布完美适配）。
    """
    from PIL import Image

    lng0, lat0, lng_span, lat_span = 119.20, 32.20, 0.24, 0.064
    sections = _sections_along(lng0, lat0, lng_span, 8, lat_span=lat_span)
    banks = [{'bank_id': 'B1', 'bank_name': '甲',
              'coordinates': [[lng0 + lng_span * i / 10.0,
                               lat0 + lat_span / 2] for i in range(11)]}]

    with tempfile.TemporaryDirectory(prefix='layout-') as workdir:
        result = risk_map.generate_risk_distribution_map(
            sections, '等比例回归', output_dir=workdir,
            output_name='ratio.png', banks=banks)
        assert '"success": true' in result, result
        path = json.loads(result)['file_path']
        with Image.open(path) as im:
            px_w, px_h = im.size

    geo_ratio = _ratio(lng_span, lat_span, lat0)
    px_ratio = px_w / px_h
    # 边饰（标题 + 图例 + 图注）会让像素比略小于地理比，但不允许差出 25%
    assert abs(px_ratio - geo_ratio) / geo_ratio < 0.25, \
        f'像素比 {px_ratio:.2f} 偏离地理比 {geo_ratio:.2f} 超过 25%'


def test_overlong_range_still_renders_true_proportion():
    """超长（约 50:1）范围：不崩溃、等比例（不回到 1.4:1 的失真画布）。"""
    from PIL import Image

    lng0, lat0, lng_span, lat_span = 119.00, 32.20, 0.50, 0.009
    sections = _sections_along(lng0, lat0, lng_span, 12, lat_span=lat_span)
    banks = [{'bank_id': 'B1', 'bank_name': '甲',
              'coordinates': [[lng0 + lng_span * i / 10.0,
                               lat0 + lat_span / 2] for i in range(11)]}]

    with tempfile.TemporaryDirectory(prefix='overlong-') as workdir:
        result = risk_map.generate_risk_distribution_map(
            sections, '超长河段', output_dir=workdir,
            output_name='overlong.png', banks=banks)
        assert '"success": true' in result, result
        path = json.loads(result)['file_path']
        with Image.open(path) as im:
            px_w, px_h = im.size

    # 超长时画布长边已 clamp：成图不可能达到 50:1，但绝不能退回接近方形
    px_ratio = px_w / px_h
    assert px_ratio > 3.0, f'超长范围被画成了 {px_ratio:.2f}:1 —— 等比例未生效'
    assert px_w <= MAX_EDGE * 150 + 400           # 长边像素受画布上限约束（留边饰余量）


def test_explicit_figsize_still_keeps_true_proportion():
    """调用方显式传 figsize：坐标轴仍严格等比例——任何输入下都**不允许变形**。

    验证用「数据→像素」变换的各向同性（比总像素比更本质）：地图上东向 1 km
    与北向 1 km 的屏幕像素长度必须相等。总像素比会受标题/图注等固定高度
    边饰稀释（画布越小、范围越扁越明显），那不是变形。
    """
    import matplotlib.pyplot as plt

    lng0, lat0, lng_span, lat_span = 119.20, 32.20, 0.36, 0.036
    sections = _sections_along(lng0, lat0, lng_span, 8, lat_span=lat_span)
    banks = [{'bank_id': 'B1', 'bank_name': '甲',
              'coordinates': [[lng0 + lng_span * i / 10.0,
                               lat0 + lat_span / 2] for i in range(11)]}]

    captured = {}
    original = plt.subplots

    def fake_subplots(*args, **kwargs):
        fig, ax = original(*args, **kwargs)
        captured['fig'], captured['ax'] = fig, ax
        return fig, ax

    plt.subplots = fake_subplots
    try:
        with tempfile.TemporaryDirectory(prefix='fixed-') as workdir:
            result = risk_map.generate_risk_distribution_map(
                sections, '固定尺寸', output_dir=workdir, output_name='fixed.png',
                banks=banks, figsize=(10, 8))
            assert '"success": true' in result, result
    finally:
        plt.subplots = original

    ax = captured['ax']
    fig = captured['fig']
    try:
        fig.canvas.draw()                      # 触发布局解析，变换才有效
        import math as _m
        lng_km_per_deg = KM_PER_LNG_DEG_EQUATOR * _m.cos(_m.radians(lat0))
        origin = ax.transData.transform((lng0, lat0))
        east = ax.transData.transform((lng0 + 1.0 / lng_km_per_deg, lat0))    # 东向 1 km
        north = ax.transData.transform((lng0, lat0 + 1.0 / KM_PER_LAT_DEG))   # 北向 1 km
        px_east = east[0] - origin[0]
        px_north = north[1] - origin[1]
        assert px_east > 0 and px_north > 0
        assert abs(px_east - px_north) / px_east < 0.01, \
            f'东向 1km={px_east:.1f}px vs 北向 1km={px_north:.1f}px —— 固定画布下仍变形'
    finally:
        plt.close(fig)


def _run_all():
    import traceback
    tests = [(name, fn) for name, fn in sorted(globals().items())
             if name.startswith('test_') and callable(fn)]
    passed = failed = 0
    for name, fn in tests:
        try:
            fn()
            print(f'PASS {name}')
            passed += 1
        except Exception:
            print(f'FAIL {name}')
            traceback.print_exc()
            failed += 1
    print(f'\n{passed} passed, {failed} failed')
    assert failed == 0, f'{failed} 个用例失败'


if __name__ == '__main__':
    _run_all()
