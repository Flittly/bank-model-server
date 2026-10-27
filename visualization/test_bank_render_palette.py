"""岸段色带的配色与可见性契约（2026-10-21 建立，2026-10-26 修订）。

需求：出图配色与前端页面**完全一致**；色带画 ``risk_level`` 1..4 四档
（绿 / 蓝 / 橙 / 红）。低风险自 2026-10-26 起改为正常着色，
只有未计算（无风险值）**不着色**。

三条契约分别由下面三组测试守着：

1. 调色板逐值等于 `bank-warning-web/src/utils/riskScoring.ts` 的 ``RISK_COLORS``
   （键按「前端显示等级 + 1」对应 ``risk_level``）。这是**跨仓库**契约：
   页面改了色而图没改，用户看到的就是「同一份结果两种颜色」。
2. 逐段着色取的是断面等级的**原始色值**，不做插值 —— 插值会产生不属于任何等级的中间色。
3. 无风险值的等级（None / 0 / 非法）不产生任何绘制段；1..4 全部产生。

几何口径：断面中点投影到岸段折线，按「最近断面」切段（与页面 buildRiskBandSpans 同）。
"""
import re
from pathlib import Path

import json

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.colors import to_hex

import pytest

from visualization import gradient, risk_map
from visualization.config import (
    RISK_COLORS, RISK_LEGEND, RISK_RENDERED_LEVELS, is_rendered_level,
)

#: 前端仓库（与本项目同级）。找不到就跳过调色板契约，不让路径问题伪装成代码失败。
FRONTEND_SCORING = (Path(__file__).resolve().parents[2]
                    / 'bank-warning-web' / 'src' / 'utils' / 'riskScoring.ts')

BANK_LAT = 32.20
BANK_LNG0, BANK_LNG1 = 119.20, 119.60


def _bank():
    n = 41
    coords = [[BANK_LNG0 + (BANK_LNG1 - BANK_LNG0) * i / (n - 1), BANK_LAT] for i in range(n)]
    return [{'bank_id': 'B1', 'bank_name': '甲岸段', 'coordinates': coords}]


def _section(index, lng, level, bank_id='B1'):
    return {
        'section_id': f'S{index}', 'section_name': f'断面{index}', 'bank_id': bank_id,
        'risk_level': level,
        'start_lng': lng, 'start_lat': BANK_LAT - 0.002,
        'end_lng': lng, 'end_lat': BANK_LAT + 0.002,
    }


def _drawn_colors(ax):
    """画到图上的颜色（去重、按绘制顺序）。"""
    out = []
    for collection in ax.collections:
        for rgba in collection.get_colors():
            hexed = to_hex(rgba[:3])
            if hexed not in out:
                out.append(hexed)
    return out


def _draw(sections):
    fig, ax = plt.subplots()
    try:
        drawn = risk_map._draw_bank_gradients(ax, _bank(), sections)
        colors = _drawn_colors(ax)
        segments = sum(len(c.get_segments()) for c in ax.collections)
    finally:
        plt.close(fig)
    return drawn, colors, segments


def _draw_report(sections):
    """只取绘制报告（稀疏区段数 / 未着色断面数）。"""
    fig, ax = plt.subplots()
    report = {}
    try:
        risk_map._draw_bank_gradients(ax, _bank(), sections, report=report)
    finally:
        plt.close(fig)
    return report


def _span_colors(sections):
    """直接拿 ``build_span_colors`` 的逐段结果（含 None = 不画）。"""
    coords = _bank()[0]['coordinates']
    _, items, total, xy = gradient.build_stops(sections, coords)
    _, arc, _ = gradient.cumulative_lengths(xy)
    colors, _ = gradient.build_span_colors(xy, arc, total, items)
    return colors


# ---------------------------------------------------------------------------
# 1. 调色板与前端逐值一致
# ---------------------------------------------------------------------------

def test_palette_equals_frontend_risk_colors():
    """``risk_level`` 1..4 的色值必须逐字等于前端 RISK_COLORS['0'..'3']。"""
    if not FRONTEND_SCORING.exists():
        pytest.skip(f'前端仓库不在预期位置：{FRONTEND_SCORING}')
    source = FRONTEND_SCORING.read_text(encoding='utf-8')
    block = re.search(r'RISK_COLORS[^=]*=\s*\{(.*?)\}', source, re.S)
    assert block, '未能从 riskScoring.ts 解析 RISK_COLORS'
    pairs = dict(re.findall(r"'(\d+)'\s*:\s*'(#[0-9a-fA-F]{6})'", block.group(1)))
    # 前端键是 0..3 的显示等级，0 = 低风险；出图键 = 显示等级 + 1
    expected = {int(k) + 1: v.lower() for k, v in pairs.items() if int(k) <= 3}
    assert len(expected) == 4, pairs
    assert {k: v.lower() for k, v in RISK_COLORS.items()} == expected, (
        '出图调色板与页面不一致：图 %s / 页面 %s' % (RISK_COLORS, expected))


def test_rendered_levels_match_frontend_visibility():
    """页面着色 0..3（= risk_level 1..4），出图必须是同一组。

    图例**不必**与着色档数一致：2026-10-21 起最高档（4，红）按需求不进图例，
    另两档文案改为「高风险 / 中风险」；2026-10-26 补回「低风险」，因为低风险
    重新上色后图面出现了图例里没有的绿色。这里把着色口径与图例口径分开断言。
    """
    assert RISK_RENDERED_LEVELS == (1, 2, 3, 4)
    assert [level for level, _ in RISK_LEGEND] == [3, 2, 1]
    assert [label for _, label in RISK_LEGEND] == ['高风险', '中风险', '低风险']


@pytest.mark.parametrize('level', [1, 2, 3, 4, 1.0, 2.0, '3'])
def test_renderable_levels(level):
    assert is_rendered_level(level) is True


@pytest.mark.parametrize('level', [None, 0, -1, 5, 2.5, 'x', float('nan'), object()])
def test_non_renderable_levels(level):
    assert is_rendered_level(level) is False


# ---------------------------------------------------------------------------
# 2. 色值就是等级原始色，不做插值
# ---------------------------------------------------------------------------

def test_span_color_is_exact_level_color():
    """一等一档：相邻断面 4 级与 2 级，画面只应出现这两个原始色。"""
    sections = [_section(1, 119.20, 4), _section(2, 119.25, 4), _section(3, 119.55, 2)]
    drawn, colors, _ = _draw(sections)

    assert drawn == 1
    assert sorted(colors) == sorted([RISK_COLORS[4], RISK_COLORS[2]]), colors
    # 不能出现任何中间色（插值会产生既不属于 4 级也不属于 2 级的颜色）
    assert len(colors) == 2, f'出现插值色：{colors}'


def test_boundary_sits_at_midpoint_between_adjacent_levels():
    """两档分界必须落在两条断面的中点，而不是渐变过渡带。"""
    sections = [_section(1, 119.20, 4), _section(2, 119.60, 2)]
    _, items, total, xy = gradient.build_stops(
        sections, _bank()[0]['coordinates'], color_of=lambda s: RISK_COLORS[int(s['risk_level'])])
    colors, _ = gradient.build_span_colors(xy, gradient.cumulative_lengths(xy)[1], total, items)

    first_red = max(i for i, c in enumerate(colors) if c == RISK_COLORS[4])
    first_blue = min(i for i, c in enumerate(colors) if c == RISK_COLORS[2])
    assert first_blue == first_red + 1, '分界处不应出现未着色或插值的过渡段'
    mid_along = gradient.cumulative_lengths(xy)[1][first_blue]
    assert mid_along == pytest.approx(total / 2, rel=0.03), (
        f'分界应在中点（{total/2:.0f} m），实际 {mid_along:.0f} m')


# ---------------------------------------------------------------------------
# 3. 低风险着色、无风险值不着色
# ---------------------------------------------------------------------------

def test_low_risk_span_is_drawn_green():
    """4 级 + 1 级：两半都要画，1 级那半边是绿色（2026-10-26 起）。"""
    sections = [_section(1, 119.20, 4), _section(2, 119.30, 4),
                _section(3, 119.50, 1), _section(4, 119.60, 1)]
    drawn, colors, segments = _draw(sections)

    assert drawn == 1
    assert sorted(colors) == sorted([RISK_COLORS[4], RISK_COLORS[1]]), colors
    assert RISK_COLORS[1] == '#10b981', '低风险必须是绿色'
    assert segments >= 40, '整条岸段都应着色，不应再留空'


def test_uncomputed_section_span_is_not_drawn():
    """``risk_level`` 缺失 = 未计算，整段留空。"""
    sections = [_section(1, 119.20, 3), _section(2, 119.40, None), _section(3, 119.60, 3)]
    drawn, colors, segments = _draw(sections)

    assert drawn == 1
    assert colors == [RISK_COLORS[3]]
    assert segments < 40, '中间那段没算出结果，必须留空'


def test_bank_without_any_risk_value_is_skipped():
    """整条都是无风险值 → 连一条 collection 都不应生成。低风险不再属于此类。"""
    drawn, colors, segments = _draw([_section(1, 119.20, None),
                                     _section(2, 119.60, None)])
    assert drawn == 0
    assert colors == [] and segments == 0


def test_bank_with_only_low_risk_is_drawn():
    """整条都是低风险 → 整条绿色，不再被判成「没有可渲染断面」。"""
    drawn, colors, segments = _draw([_section(1, 119.20, 1),
                                     _section(2, 119.60, 1)])
    assert drawn == 1
    assert colors == [RISK_COLORS[1]]
    assert segments >= 40


def test_bank_without_sections_is_skipped():
    """完全没断面（未计算）→ 不画灰线兜底（页面同样不画）。"""
    drawn, colors, segments = _draw([])
    assert drawn == 0
    assert colors == [] and segments == 0


# ---------------------------------------------------------------------------
# 4. 人工无效断面：占住自己那段但不着色（2026-10-27）
# ---------------------------------------------------------------------------

def test_invalid_section_keeps_its_own_span_blank():
    """人工标记无效的断面必须**整段留空**，不能被左右两个有效断面把颜色铺过来。

    病灶（2026-10-27 用户反馈「无效断面最后还是会被渲染上颜色」）：后端最初在 SQL 里
    直接 ``AND manual_invalid = false`` 把无效断面过滤掉。色带是按「最近断面」切段的，
    断面一旦缺席，它原本管辖的那一段就改由左右邻居就近接管 —— 图上被标为无效的那段
    照样有色，只是颜色换成了邻居的。正确做法是保留行、把等级置空，让这段归它自己管。

    本测试同时断言「对照组」：只喂两侧有效断面时整条都会被铺满 —— 说明留空**不是**
    因为几何上恰好没东西可画，而是真的靠无效断面占位。
    """
    invalid = {**_section(2, 119.40, None), 'manual_invalid': True}
    sections = [_section(1, 119.20, 3), invalid, _section(3, 119.60, 3)]

    colors = _span_colors(sections)
    blank = sum(1 for c in colors if c is None)
    assert blank > 0, '无效断面没有让任何一段留空'
    # 无效断面在正中，辖区 = 前后两条断面的中点之间 = 全长的一半
    ratio = blank / len(colors)
    assert 0.4 < ratio < 0.6, f'留空比例应约 1/2，实际 {ratio:.2%}（{blank}/{len(colors)}）'

    # 对照组：把无效断面拿掉 = 旧后端口径，整条被邻居的颜色填满
    fallback = _span_colors([sections[0], sections[2]])
    assert all(c is not None for c in fallback), (
        '对照组不该留空：断面缺席时邻居会接管它那段，这正是被过滤掉之后的后果')


def test_invalid_sections_do_not_shrink_gap_threshold():
    """人工无效断面不参与「间距」统计，否则同岸段别处正常段会被误判成虚线。

    间距阈值的语义是「两个**实测控制点**之间的跨距」：无效/未计算的断面在图上没有
    颜色，不构成控制点。若把它们算进去，中位间距被压小 → 阈值变小 → 这一条岸段上
    另一处本来正常的实线段被画成半透明虚线，用户会以为「标了几条无效，别处也坏了」。
    """
    base = [_section(1, 119.20, 3), _section(2, 119.30, 3),
            _section(3, 119.50, 3), _section(4, 119.60, 3)]
    # 在 119.20~119.30 之间插 5 条人工无效断面（间距 ~1.5 km，远密于 9.4 km 的正常间距）
    filler = [{**_section(10 + i, 119.20 + 0.016 * (i + 1), None), 'manual_invalid': True}
              for i in range(5)]

    assert _draw_report(base)['sparse_segments'] == 0, '对照组：正常间距下不应出现虚线区段'
    report = _draw_report(base + filler)
    assert report['sparse_segments'] == 0, (
        '人工无效断面被算进了间距统计，把阈值压小，119.30~119.50 那段正常实线变成虚线')
    assert report['hidden_sections'] == len(filler)


def test_description_counts_only_rendered_sections(tmp_path):
    """图注的断面数按**可着色**统计，并与报告正文（排除无效）对得上。"""
    sections = [_section(1, 119.20, 3),
                {**_section(2, 119.40, None), 'manual_invalid': True},
                _section(3, 119.60, 3)]
    raw = risk_map.generate_risk_distribution_map(
        sections, output_dir=str(tmp_path), output_name='desc.png', banks=_bank())
    description = json.loads(raw)['description']

    assert '共 2 个断面' in description, description
    assert '另有 1 个未着色' in description and '人工无效 1 个' in description, description
