"""岸段色带的配色与可见性契约（2026-10-21）。

需求：出图配色与前端页面**完全一致**；色带只显示一级/二级/三级风险三档；
低风险（``risk_level`` 1）与未计算（无风险值）**不渲染、不着色**。

三条契约分别由下面三组测试守着：

1. 调色板逐值等于 `bank-warning-web/src/utils/riskScoring.ts` 的 ``RISK_COLORS``
   （键按「前端显示等级 + 1」对应 ``risk_level``）。这是**跨仓库**契约：
   页面改了色而图没改，用户看到的就是「同一份结果两种颜色」。
2. 逐段着色取的是断面等级的**原始色值**，不做插值 —— 插值会产生不属于任何等级的中间色。
3. 不可渲染的等级（1 / 0 / None / 非法）不产生任何绘制段。

几何口径：断面中点投影到岸段折线，按「最近断面」切段（与页面 buildRiskBandSpans 同）。
"""
import re
from pathlib import Path

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
    """页面 only 画显示等级 1..3（= risk_level 2..4），出图必须是同一组。"""
    assert RISK_RENDERED_LEVELS == (2, 3, 4)
    assert [level for level, _ in RISK_LEGEND] == [4, 3, 2]
    assert [label for _, label in RISK_LEGEND] == ['一级风险', '二级风险', '三级风险']


@pytest.mark.parametrize('level', [2, 3, 4, 2.0, '3'])
def test_renderable_levels(level):
    assert is_rendered_level(level) is True


@pytest.mark.parametrize('level', [None, 0, 1, -1, 5, 2.5, 'x', float('nan'), object()])
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
# 3. 低风险 / 未计算不着色
# ---------------------------------------------------------------------------

def test_low_risk_span_is_not_drawn():
    """4 级 + 1 级：只画 4 级那半边，1 级那半边不着色。"""
    sections = [_section(1, 119.20, 4), _section(2, 119.30, 4),
                _section(3, 119.50, 1), _section(4, 119.60, 1)]
    drawn, colors, segments = _draw(sections)

    assert drawn == 1
    assert colors == [RISK_COLORS[4]], f'1 级不应着色，实际出现 {colors}'
    assert segments < 40, '低风险段应留空（不是画成透明色），段数应明显少于整条岸段'


def test_uncomputed_section_span_is_not_drawn():
    """``risk_level`` 缺失 = 未计算，整段留空。"""
    sections = [_section(1, 119.20, 3), _section(2, 119.40, None), _section(3, 119.60, 3)]
    drawn, colors, segments = _draw(sections)

    assert drawn == 1
    assert colors == [RISK_COLORS[3]]
    assert segments < 40, '中间那段没算出结果，必须留空'


def test_bank_without_any_renderable_level_is_skipped():
    """整条都是低风险 / 无值 → 连一条 collection 都不应生成。"""
    for level in (1, None):
        drawn, colors, segments = _draw([_section(1, 119.20, level),
                                         _section(2, 119.60, level)])
        assert drawn == 0, f'level={level} 不应绘制岸段'
        assert colors == [] and segments == 0


def test_bank_without_sections_is_skipped():
    """完全没断面（未计算）→ 不画灰线兜底（页面同样不画）。"""
    drawn, colors, segments = _draw([])
    assert drawn == 0
    assert colors == [] and segments == 0
