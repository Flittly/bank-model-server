"""图例样式契约测试。

契约：
1. 图例底板为白色、不透明度 80%（alpha=0.8）——与 heatmap.py / section_chart.py
   的标注框（facecolor='white', alpha=0.8）同一套视觉约定；
2. 图例文字为深色，在 80% 白底上对比度足够（保证清晰可读）；
3. 图例仍然存在、仍然带边框（不可为了透明把 frameon 关掉）。
"""
import os
import tempfile

import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt

from visualization import risk_map
from visualization.config import RISK_LEGEND

# 与 test_tiles.py 同一套最小数据：一个闭合岸段环 + 一排断面
def _sections(count=8, level=2):
    out = []
    for i in range(count):
        lng = 119.20 + 0.40 * i / (count - 1)
        out.append({
            'section_id': f'S{i}', 'section_name': f'断面{i}',
            'bank_id': 'B1', 'risk_level': level,
            'start_lng': lng, 'start_lat': 32.20,
            'end_lng': lng, 'end_lat': 32.204,
        })
    return out


def _banks():
    return [{'bank_id': 'B1', 'bank_name': '甲',
             'coordinates': [[119.20, 32.20], [119.60, 32.20],
                             [119.60, 32.40], [119.20, 32.40], [119.20, 32.20]]}]


def _draw_capture_legend(workdir):
    """跑一次出图并捕获坐标轴，返回图例对象。"""
    captured = []
    original = plt.subplots

    def fake_subplots(*args, **kwargs):
        fig, ax = original(*args, **kwargs)
        captured.append(ax)
        return fig, ax

    plt.subplots = fake_subplots
    try:
        risk_map.generate_risk_distribution_map(
            _sections(), '图例样式回归', output_dir=workdir,
            output_name='legend.png', banks=_banks())
    finally:
        plt.subplots = original

    assert captured, '未捕获到坐标轴'
    legend = captured[-1].get_legend()
    assert legend is not None, '图上没有图例'
    return legend


def test_legend_frame_alpha_is_80_percent():
    """底板不透明度必须是 0.8（80%），且仍带白底与边框。"""
    workdir = tempfile.mkdtemp(prefix='legend-')
    try:
        legend = _draw_capture_legend(workdir)
    finally:
        plt.close('all')
        for name in os.listdir(workdir):
            os.remove(os.path.join(workdir, name))
        os.rmdir(workdir)

    assert risk_map.LEGEND_FRAME_ALPHA == 0.8, '常量应为 0.8'
    assert legend.get_frame_on(), '图例底板应开启（frameon=True）'

    alpha = legend.get_frame().get_alpha()
    assert alpha is not None and abs(alpha - 0.8) < 1e-9, \
        f'图例底板不透明度应为 0.8，实际 {alpha}'

    # 与同模块其他标注框保持一致的灰边
    assert legend.get_frame().get_edgecolor() is not None, '图例边框颜色不应为空'


def test_legend_text_is_dark_and_readable():
    """80% 白底上的图例文字必须是深色，保证对比度。"""
    workdir = tempfile.mkdtemp(prefix='legend-')
    try:
        legend = _draw_capture_legend(workdir)
        texts = legend.get_texts()
        assert texts, '图例没有文字条目'
        for t in texts:
            r, g, b, _ = matplotlib.colors.to_rgba(t.get_color())
            # 深色判定：三通道都不亮（#1f2937 ≈ 0.12/0.16/0.22）
            assert max(r, g, b) < 0.5, \
                f'图例文字 {t.get_text()!r} 颜色过浅（{t.get_color()}），会看不清'
        labels = [t.get_text() for t in texts]
        # 条目与文案以 config.RISK_LEGEND 为唯一真源，别在这里再抄一份：
        # 2026-10-21 需求：最高档「一级风险」不进图例；原「二级风险」改名「高风险」、
        # 原「三级风险」改名「中风险」；「内插/外推区段」这一项也移除
        # （虚线样式仍照常画在图上，只是不再有图例说明）。
        # 2026-10-26：低风险重新着色，图例补回「低风险」。
        assert labels == [label for _, label in RISK_LEGEND], labels
        assert '一级风险' not in labels, '最高档按需求不进图例'
        assert '内插/外推区段' not in labels, '「内插/外推区段」图例项已移除'
        assert '无数据' not in labels, '「无数据」不是等级，图例里不能出现'
        assert '低风险' in labels, '低风险已着色，图例必须列出对应的绿色'
    finally:
        plt.close('all')
        for name in os.listdir(workdir):
            os.remove(os.path.join(workdir, name))
        os.rmdir(workdir)


if __name__ == '__main__':
    test_legend_frame_alpha_is_80_percent()
    test_legend_text_is_dark_and_readable()
    print('[OK] 图例底板 80% 不透明度 + 深色文字 契约通过')
