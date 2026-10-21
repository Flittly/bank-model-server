"""梯度色带算法的单元测试。

可直接运行（无需 pytest）：
    uv run python -m visualization.test_gradient
也可用 pytest：
    uv run pytest visualization/test_gradient.py
"""
from __future__ import annotations

import math

from visualization import gradient


BANK_LNGS = [117.20, 117.22, 117.24, 117.26, 117.28]
BANK_LAT = 30.85


def _bank_coords(lngs=None, lat=BANK_LAT):
    return [[lng, lat] for lng in (lngs or BANK_LNGS)]


def _section(sid, lng, level, lat=BANK_LAT, distance=None):
    """构造一个断面，（lng, lat）为中点，断面沿经向铺开 0.001 度以便算中点。"""
    return {
        'section_id': sid,
        'section_name': sid,
        'bank_id': 'BANK_A',
        'start_lng': lng,
        'start_lat': lat - 0.0005,
        'end_lng': lng,
        'end_lat': lat + 0.0005,
        'risk_level': level,
        'distance': distance,
    }


def _colors_for(bank_lngs, sections, gap_meters=None, cyclic=False):
    """gap_meters=None 表示走自适应阈值（与线上默认路径一致）。"""
    coords = _bank_coords(bank_lngs)
    stops, items, total, xy = gradient.build_stops(sections, coords, cyclic=cyclic)
    _, arc, _ = gradient.cumulative_lengths(xy)
    threshold = gradient.adaptive_gap_threshold(
        [item['along'] for item in items], override=gap_meters,
        cyclic=cyclic, total=total)
    colors, gaps = gradient.build_vertex_colors(
        xy, arc, total, stops, [item['along'] for item in items], threshold,
        cyclic=cyclic)
    return colors, gaps, stops, items, total


# --------------------------------------------------------------------------
# 1. 位置参数精度
# --------------------------------------------------------------------------

def test_projection_position_is_accurate():
    """断面落在岸段正中时，t 应为 0.5。"""
    sections = [_section('S1', 117.24, 1)]
    _, items, total, _ = gradient.build_stops(sections, _bank_coords())
    assert len(items) == 1
    assert abs(items[0]['t'] - 0.5) < 1e-6, f"t={items[0]['t']}"
    assert items[0]['offset'] < 1.0, f"offset={items[0]['offset']}"
    assert total > 0


def test_projection_uses_geometric_position_not_distance():
    """distance 填了完全离谱的值，位置仍由几何决定。"""
    sections = [_section('S1', 117.24, 1, distance=999999.0)]
    _, items, total, _ = gradient.build_stops(sections, _bank_coords())
    assert abs(items[0]['t'] - 0.5) < 1e-6, "位置不应受 distance 影响"
    assert abs(items[0]['t'] * total - total / 2) < 1e-3


# --------------------------------------------------------------------------
# 2. 方向无关性（坑 1：岸段坐标倒序 + distance 方向相反，颜色必须不变）
# --------------------------------------------------------------------------

def _rgb(color):
    return tuple(int(color[i:i + 2], 16) for i in (1, 3, 5))


def _colors_close(c1, c2, tol=2):
    """颜色近似相等：倒序后弧长是反向累加的，浮点顺序差异会让通道差 1~2 个色阶，
    这是数值噪声而非方向错误（位置差异 < 1e-9 m）。"""
    return all(abs(a - b) <= tol for a, b in zip(_rgb(c1), _rgb(c2)))


def test_gradient_is_direction_independent():
    """岸段坐标倒序、distance 保持不变时，同一地理位置的颜色必须一致。

    这正是「坑 1」要防的事：若用 distance/total 定位，倒序后整条色带会翻转。
    """
    sections = [
        _section('S1', 117.20, 1, distance=0.0),
        _section('S2', 117.24, 2, distance=1116.0),
        _section('S3', 117.28, 4, distance=2233.0),
    ]

    fwd_colors, _, _, _, _ = _colors_for(BANK_LNGS, sections)
    rev_colors, _, _, _, _ = _colors_for(list(reversed(BANK_LNGS)), sections)

    assert len(fwd_colors) == len(rev_colors) == 4
    for index, (forward, backward) in enumerate(zip(reversed(fwd_colors), rev_colors)):
        assert _colors_close(forward, backward), (
            f"第 {index} 段颜色方向不一致\n正序={fwd_colors}\n倒序={rev_colors}")


# --------------------------------------------------------------------------
# 3. 边界：0 / 1 / 2 个断面
# --------------------------------------------------------------------------

def test_zero_sections_gives_solid_default_color():
    colors, gaps, stops, items, _ = _colors_for(BANK_LNGS, [])
    assert stops == [] and items == []
    assert all(c == gradient.DEFAULT_COLOR for c in colors)
    assert not any(gaps), "无断面时应为实线灰，而不是虚线外推"


def test_single_section_paints_whole_bank_with_its_color():
    sections = [_section('S1', 117.24, 3)]
    colors, gaps, _, _, _ = _colors_for(BANK_LNGS, sections)
    from visualization.config import RISK_COLORS
    assert all(c == RISK_COLORS[3] for c in colors), colors
    assert not any(gaps)


def test_two_sections_interpolate_between_them():
    """跨档过渡必须沿等级序走，不得出现浑浊的低饱和中间色。

    回归：早先在 RGB 空间直连，绿(#10b981)→红(#ef4444) 的中点约 #a99987 ——
    一段灰扑扑的土棕，看起来就像「这段没着色」。等级是有序的，1→4 应依次
    经过黄(2)、橙(3)，与图例的四档标尺一致。
    """
    sections = [_section('S1', 117.20, 1), _section('S2', 117.28, 4)]
    colors, _, stops, items, _ = _colors_for(BANK_LNGS, sections)
    assert len(stops) == 2 and len(items) == 2
    assert len(set(colors)) == len(colors), f'各段颜色应互不相同: {colors}'
    for color in colors:
        r, g, b = _rgb(color)
        assert max(r, g, b) - min(r, g, b) >= 40, \
            f'跨档过渡出现浑浊中间色 {color}（饱和度太低，视觉上像没着色）'
    # 起点偏绿、终点偏红
    first_r, first_g, _ = _rgb(colors[0])
    last_r, last_g, _ = _rgb(colors[-1])
    assert first_g > first_r, f'首段应偏绿: {colors[0]}'
    assert last_r > last_g, f'末段应偏红: {colors[-1]}'


def test_lerp_risk_color_walks_the_ordinal_scale():
    """1→4 的中点必须落在**中间档**上，而不是两端直接 RGB 直连。

    断言写成「等于 2 档与 3 档的中点」而不是写死某个色相：调色板 2026-10-21 已对齐
    前端（2 档由黄 #facc15 改为蓝 #3b82f6），写死「黄橙系」会把调色板的正确变更判成失败。
    """
    from visualization.config import RISK_COLORS
    mid = gradient.lerp_risk_color(RISK_COLORS[1], RISK_COLORS[4], 0.5)
    assert mid == gradient.lerp_color(RISK_COLORS[2], RISK_COLORS[3], 0.5), \
        f'1→4 的中点应等于 2 档/3 档的中点，实际 {mid}'
    # 端点在 u=0 / u=1 处必须精确还原，不能被取整带偏
    assert gradient.lerp_risk_color(RISK_COLORS[1], RISK_COLORS[4], 0.0) == RISK_COLORS[1]
    assert gradient.lerp_risk_color(RISK_COLORS[1], RISK_COLORS[4], 1.0) == RISK_COLORS[4]
    # 相邻档之间没有中间档，等价于普通 RGB 插值
    assert gradient.lerp_risk_color(RISK_COLORS[1], RISK_COLORS[2], 0.5) == \
        gradient.lerp_color(RISK_COLORS[1], RISK_COLORS[2], 0.5)
    # 非标准色（如无数据灰）退回普通插值，不抛异常
    assert gradient.lerp_risk_color('#000000', '#ffffff', 0.5) in ('#7f7f7f', '#808080')


# --------------------------------------------------------------------------
# 4. 稀疏区段（坑 3）：阈值按该岸段断面密度自适应
# --------------------------------------------------------------------------

def test_sparse_gap_is_flagged():
    """整条岸段只有两端断面（跨距约 7.6 km），中间全应视为内插区段。"""
    sections = [_section('S1', 117.20, 1), _section('S2', 117.28, 4)]
    _, gaps, _, _, total = _colors_for(BANK_LNGS, sections)
    assert total > 1000
    assert all(gaps), f"整条岸段只有两端断面，中间全应为内插区段: {gaps}"


def test_dense_sections_are_not_flagged():
    """断面间距约 400 m 且铺满整条岸段时，不应出现内插/外推区段。"""
    step = 0.08 / 19  # 岸段全长 117.20→117.28，均分 20 个断面 ≈ 403 m
    sections = [_section(f'S{i}', 117.20 + i * step, 2) for i in range(20)]
    _, gaps, _, _, total = _colors_for(BANK_LNGS, sections)
    assert total > 7000
    assert not any(gaps), f"断面密集时不应有外推区段: {gaps}"


# --------------------------------------------------------------------------
# 5. 颜色工具
# --------------------------------------------------------------------------

def test_color_at_clamps_both_ends():
    stops = [(0.3, '#000000'), (0.7, '#ffffff')]
    assert gradient.color_at(0.0, stops) == '#000000'
    assert gradient.color_at(0.3, stops) == '#000000'
    assert gradient.color_at(1.0, stops) == '#ffffff'
    assert gradient.color_at(0.3, []) == gradient.DEFAULT_COLOR


def test_lerp_color_midpoint():
    assert gradient.lerp_color('#000000', '#ffffff', 0.0) == '#000000'
    assert gradient.lerp_color('#000000', '#ffffff', 1.0) == '#ffffff'
    mid = gradient.lerp_color('#000000', '#ffffff', 0.5)
    assert mid in ('#7f7f7f', '#808080'), mid


# --------------------------------------------------------------------------
# 6. distance 一致性自检只告警、不抛异常
# --------------------------------------------------------------------------

def test_distance_consistency_reports_reversed_direction():
    """distance 与几何方向相反时应报出负相关、且量级比值正常（不再触发单位告警）。"""
    sections = [
        _section('S1', 117.20, 1, distance=7642.0),
        _section('S2', 117.24, 1, distance=3821.0),
        _section('S3', 117.28, 1, distance=0.0),
    ]
    _, items, total, _ = gradient.build_stops(sections, _bank_coords())
    report = gradient.check_distance_consistency(items, total, 'BANK_A')
    assert report['checked'] == 3
    assert report['corr'] is not None and report['corr'] < 0, report
    assert report['ratio'] is not None and 0.5 <= report['ratio'] <= 1.5, report


def test_distance_consistency_flags_unit_mismatch():
    """distance 量级远小于岸段长度时应报出单位可疑。"""
    sections = [
        _section('S1', 117.20, 1, distance=0.0),
        _section('S2', 117.24, 1, distance=0.04),
        _section('S3', 117.28, 1, distance=0.08),
    ]
    _, items, total, _ = gradient.build_stops(sections, _bank_coords())
    report = gradient.check_distance_consistency(items, total, 'BANK_A')
    assert report['ratio'] is not None and report['ratio'] < 0.5, report


def test_distance_consistency_tolerates_missing_distance():
    sections = [_section('S1', 117.20, 1), _section('S2', 117.28, 2)]
    _, items, total, _ = gradient.build_stops(sections, _bank_coords())
    report = gradient.check_distance_consistency(items, total, 'BANK_A')
    assert report['checked'] == 0 and report['corr'] is None


# --------------------------------------------------------------------------
# 7. 局部投影的尺度合理性（坑 2：米制）
# --------------------------------------------------------------------------

def test_total_length_is_metric():
    """117.20→117.28 在 30.85 纬度约 7.67 km，投影总长应落在这个量级。"""
    _, _, total, _ = gradient.build_stops([], _bank_coords())
    expected = math.radians(0.08) * gradient.R_EARTH * math.cos(math.radians(BANK_LAT))
    assert abs(total - expected) < 1.0, f"total={total}, expected={expected}"
    assert 7000 < total < 8500, total


# --------------------------------------------------------------------------
# 8. 闭合环（真实 banks.bank_geometry 实测为首末点重合的闭合线）
# --------------------------------------------------------------------------

RING_LNG0, RING_LNG1 = 117.20, 117.26
RING_LAT0, RING_LAT1 = 30.84, 30.88


def _ring_coords():
    """闭合矩形环：首末点完全相同，与库里 bank_geometry 的形态一致。"""
    return [
        [RING_LNG0, RING_LAT0],
        [RING_LNG1, RING_LAT0],
        [RING_LNG1, RING_LAT1],
        [RING_LNG0, RING_LAT1],
        [RING_LNG0, RING_LAT0],
    ]


def test_is_closed_ring_detects_closed_line():
    assert gradient.is_closed_ring(_ring_coords()) is True


def test_is_closed_ring_leaves_open_line_untouched():
    assert gradient.is_closed_ring(_bank_coords()) is False
    assert gradient.is_closed_ring([[117.2, 30.85], [117.22, 30.85]]) is False


def test_closed_ring_projects_every_section_without_offset():
    """环上每条断面都必须精确投到环上（offset≈0），包括落在闭合边附近的。

    回归：上一版把环「掰直」（丢掉闭合边）后，落在被摘掉那一段上的断面只能被
    拉到最近端点 —— 实测 371-S16 从沿程 14 745 m 被拉到 0 m（偏差 476 m），
    它的颜色于是被画到了接缝处。
    """
    ring = _ring_coords()
    # 断面铺在底边上，其中 LS 紧挨着环的起点（闭合边所在处）
    sections = [
        _section('LS', RING_LNG0 + 0.002, 4, lat=RING_LAT0),
        _section('M1', RING_LNG0 + 0.02, 2, lat=RING_LAT0),
        _section('M2', RING_LNG0 + 0.05, 1, lat=RING_LAT0),
    ]
    stops, items, total, _ = gradient.build_stops(sections, ring, cyclic=True)
    assert len(items) == 3
    for item in items:
        assert item['offset'] < 1.0, f"断面 {item['section_id']} 未落在环上: offset={item['offset']}"
    assert len(stops) == 3


def test_closed_ring_interpolates_across_the_seam():
    """接缝处（环起点，也就是闭合边所在位置）必须正常渐变，不能被压成端点色。

    回归：掰直 + 端点 clamp 时，接缝那段只剩两个断面的纯色，中间真实存在的
    过渡丢失（上一版还把它当「外推」画成半透明虚线，视觉上是一段发灰的缺口）。
    """
    ring = _ring_coords()
    # 断面集中在底边中段，环起点（接缝）附近没有断面
    sections = [
        _section('A', RING_LNG0 + 0.02, 1, lat=RING_LAT0),
        _section('B', RING_LNG0 + 0.04, 4, lat=RING_LAT0),
    ]
    stops, items, _, _ = gradient.build_stops(sections, ring, cyclic=True)
    assert len(stops) == 2 and len(items) == 2
    assert stops[0][1] != stops[1][1], '两端断面颜色应不同才有插值可言'

    # 接缝位置 t≈0：环语义下它夹在「B → 绕接缝 → A」之间，应得到一个过渡色
    seam_color = gradient.color_at_cyclic(0.0, stops)
    assert seam_color not in (stops[0][1], stops[1][1]), \
        f'接缝处被压成端点色 {seam_color}，环上应有渐变'
    # 对照：开放折线的 clamp 语义在 t=0 只会给首色
    assert gradient.color_at(0.0, stops) == stops[0][1]


def test_closed_ring_seam_is_not_flagged_as_gap():
    """环上不存在「实测范围之外」：断面铺满整圈时，接缝那段不该被判成内插/外推。

    同一份几何、同一批断面：按环判定应全部实线；按开放折线判定则接缝处必然被判
    成「超出范围」（这正是「岸段有一段发灰、像是没着色」的成因）。
    """
    ring = _ring_coords()
    mid_lng = (RING_LNG0 + RING_LNG1) / 2.0
    mid_lat = (RING_LAT0 + RING_LAT1) / 2.0
    # 只在右/上/左三条边各布一条断面，**故意空出含环起点的那条底边**：
    # 开放折线语义下，底边整段落在「首个断面之前」，会被判成外推；
    # 环语义下它只是「最后一个断面 → 第一个断面」的正常相邻区间。
    sections = [
        _section('RGT', RING_LNG1, 2, lat=mid_lat),
        _section('TOP', mid_lng, 3, lat=RING_LAT1),
        _section('LFT', RING_LNG0, 4, lat=mid_lat),
    ]
    stops, items, total, xy = gradient.build_stops(sections, ring, cyclic=True)
    assert len(items) == 3
    arc = gradient.cumulative_lengths(xy)[1]
    alongs = [i['along'] for i in items]

    threshold = gradient.adaptive_gap_threshold(alongs, cyclic=True, total=total)
    _, gaps_cyclic = gradient.build_vertex_colors(
        xy, arc, total, stops, alongs, threshold, cyclic=True)
    _, gaps_open = gradient.build_vertex_colors(
        xy, arc, total, stops, alongs, threshold, cyclic=False)

    assert not any(gaps_cyclic), \
        f'断面铺满整圈时不应有内插/外推区段（阈值 {threshold:,.0f} m）: {gaps_cyclic}'
    assert any(gaps_open), \
        f'开放折线语义下接缝处应被判为超出实测范围: {gaps_open}'


def test_color_at_cyclic_wraps_around():
    stops = [(0.25, '#000000'), (0.75, '#ffffff')]
    # 区间内与线性版一致
    assert gradient.color_at_cyclic(0.5, stops) == gradient.color_at(0.5, stops)
    # 环上 0.75→0.25 绕接缝，0.0 刚好在中点 → 灰
    assert gradient.color_at_cyclic(0.0, stops) == gradient.lerp_color('#ffffff', '#000000', 0.5)
    assert gradient.color_at_cyclic(1.0, stops) == gradient.color_at_cyclic(0.0, stops)
    assert gradient.color_at_cyclic(0.1, stops) == gradient.lerp_color('#ffffff', '#000000', 0.7)
    assert gradient.color_at_cyclic(0.5, []) == gradient.DEFAULT_COLOR
    assert gradient.color_at_cyclic(0.5, [(0.3, '#123456')]) == '#123456'


def test_closed_ring_draws_full_geometry():
    """闭合环仍要画满整圈（含那条闭合边）。

    回归：段数与配色数不匹配会让整条岸段被跳过 —— 正是「图上完全没有岸段」的成因之一。
    """
    import matplotlib.pyplot as plt

    from visualization import risk_map

    ring = _ring_coords()
    banks = [{'bank_id': 'BANK_A', 'bank_name': '甲', 'coordinates': ring}]
    # 两条断面都必须可着色（2/3/4）。用 1 级会让整圈不渲染 —— 那是另一条规则，
    # 由 test_ring_with_low_risk_section_leaves_a_gap 覆盖。
    sections = [_section('S1', 117.22, 3, lat=RING_LAT0),
                _section('S2', 117.24, 4, lat=RING_LAT0)]

    fig, ax = plt.subplots()
    try:
        drawn = risk_map._draw_bank_gradients(ax, banks, sections)
        segment_count = sum(len(coll.get_segments()) for coll in ax.collections)
    finally:
        plt.close(fig)

    assert drawn == 1, '闭合环未能绘制（段数与配色数不匹配会被整条跳过）'
    assert segment_count == len(ring) - 1, \
        f'闭合环应画满 {len(ring) - 1} 段（含闭合边），实际 {segment_count}'


# --------------------------------------------------------------------------
# 9. 自适应「内插/外推」阈值
# --------------------------------------------------------------------------

def test_adaptive_threshold_follows_section_density():
    """1 km 级别的断面密度下阈值应放宽到 3 km，而不是固定 500 m。"""
    alongs = [i * 1000.0 for i in range(6)]
    assert gradient.adaptive_gap_threshold(alongs) == 3000.0


def test_adaptive_threshold_has_floor():
    """断面很密时不应把阈值压到 0，下限兜住。"""
    alongs = [i * 100.0 for i in range(6)]
    assert gradient.adaptive_gap_threshold(alongs) == gradient.DEFAULT_GAP_METERS


def test_adaptive_threshold_respects_override():
    alongs = [i * 1000.0 for i in range(6)]
    assert gradient.adaptive_gap_threshold(alongs, override=500) == 500.0
    assert gradient.adaptive_gap_threshold(alongs, override=None) == 3000.0


def test_1km_section_density_is_not_flagged_as_gap():
    """回归：真实数据相邻断面中位间距就是 1000 m。

    修复前用固定 500 m 阈值，会把 76.5% 的区段判成「内插」而整条画成
    虚线+低透明（视觉上等于「岸段没有着色」）。
    """
    lngs = [117.20 + i * 0.011 for i in range(6)]      # 间隔约 1051 m
    sections = [_section(f'S{i}', lng, 1) for i, lng in enumerate(lngs)]
    _, gaps, _, _, _ = _colors_for(lngs, sections)
    assert not any(gaps), f'1 km 密度不该被判为内插/外推: {gaps}'


def _run_all():
    tests = [(name, obj) for name, obj in sorted(globals().items())
             if name.startswith('test_') and callable(obj)]
    failed = []
    for name, func in tests:
        try:
            func()
            print(f"  PASS  {name}")
        except AssertionError as exc:
            failed.append(name)
            print(f"  FAIL  {name}: {exc}")
        except Exception as exc:  # noqa: BLE001
            failed.append(name)
            print(f"  ERROR {name}: {type(exc).__name__}: {exc}")
    print(f"\n{len(tests) - len(failed)}/{len(tests)} passed")
    return 1 if failed else 0


if __name__ == '__main__':
    raise SystemExit(_run_all())
