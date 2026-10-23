"""DEM 底图缺省关闭的契约（2026-10-21）。

需求原文：「图中不要有 DEM 了，可以不显示 DEM，其他的不变」。

本文件锁三件事：

1. 模块开关 :data:`risk_map.RENDER_DEM_BASEMAP` 为 False；
2. 缺省出图时**根本不去加载 DEM** —— 即便载荷里带了 ``dem_path`` 且文件真实存在，
   ``load_dem_basemap`` 也不应被调用（不读 GDAL、不 Warp、不吃那份内存），
   图上也不应出现任何 DEM 图层；
3. 传 ``render_dem=True`` 时能原样恢复旧行为（否则这个开关就是一句空话）。

⚠ 这里断言的是「DEM 图层与加载调用」，不是「整张图没有 image」：
   遥感影像底图（zorder=-10）是另一个来源，测试里用 monkeypatch 关掉以免依赖网络。
"""

import os
import tempfile

import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt
import numpy as np
import pytest

from visualization import risk_map


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


@pytest.fixture
def no_basemap(monkeypatch):
    """关掉遥感影像底图：避免测试依赖天地图 key / 网络，也让断言只看 DEM。"""
    monkeypatch.setattr(risk_map.basemap_module, 'draw_basemap',
                        lambda *a, **k: {'drawn': False})


def _draw(workdir, dem_path=None, **kwargs):
    """跑一次出图，返回 (结果 JSON 字符串, 唯一坐标轴)。"""
    captured = []
    original = plt.subplots

    def fake_subplots(*args, **kw):
        fig, ax = original(*args, **kw)
        captured.append(ax)
        return fig, ax

    plt.subplots = fake_subplots
    try:
        result = risk_map.generate_risk_distribution_map(
            _sections(), 'DEM 开关回归', output_dir=workdir,
            output_name='dem.png', banks=_banks(),
            dem_path=dem_path, **kwargs)
    finally:
        plt.subplots = original
    assert captured, '未捕获到坐标轴'
    return result, captured[-1]


@pytest.fixture
def workdir():
    path = tempfile.mkdtemp(prefix='dem-off-')
    yield path
    plt.close('all')
    for name in os.listdir(path):
        os.remove(os.path.join(path, name))
    os.rmdir(path)


def test_switch_defaults_off():
    """开关缺省必须是关闭状态 —— 这是「图上不要 DEM」的唯一开关。"""
    assert risk_map.RENDER_DEM_BASEMAP is False


def test_dem_is_not_loaded_even_when_path_exists(no_basemap, monkeypatch, workdir):
    """带 dem_path 且文件存在，也不应该去加载 DEM。"""
    dem_file = os.path.join(workdir, 'fake_dem.tif')
    with open(dem_file, 'wb') as handle:
        handle.write(b'not-a-real-dem')

    called = []

    def boom(*args, **kwargs):
        called.append(args)
        raise AssertionError('DEM 已关闭，不应调用 load_dem_basemap')

    monkeypatch.setattr(risk_map, 'load_dem_basemap', boom)

    result, ax = _draw(workdir, dem_path=dem_file)

    assert called == [], '缺省出图不应加载 DEM'
    assert 'success' in result
    assert len(ax.images) == 0, f'图上不应有 DEM 图层，实际 {len(ax.images)} 层'


def test_legend_has_no_dem_related_entries(no_basemap, workdir):
    """顺带守住图例：最高档不进图例、内插/外推项已移除（见需求 1）。"""
    _, ax = _draw(workdir)
    labels = [t.get_text() for t in ax.get_legend().get_texts()]
    assert labels == ['高风险', '中风险'], labels


def test_render_dem_true_restores_old_behaviour(no_basemap, monkeypatch, workdir):
    """显式传 render_dem=True 时应恢复：调用加载并画出灰色衬底。"""
    dem_file = os.path.join(workdir, 'fake_dem.tif')
    with open(dem_file, 'wb') as handle:
        handle.write(b'not-a-real-dem')

    data = np.linspace(0, 100, 400).reshape(20, 20)
    coords = (np.linspace(119.20, 119.60, 20), np.linspace(32.40, 32.20, 20))
    calls = []

    def fake_load(path, bounds):
        calls.append((path, bounds))
        return data, coords

    monkeypatch.setattr(risk_map, 'load_dem_basemap', fake_load)

    _, ax = _draw(workdir, dem_path=dem_file, render_dem=True)

    assert len(calls) == 1, 'render_dem=True 时应加载一次 DEM'
    assert len(ax.images) == 1, f'应画出 1 层 DEM，实际 {len(ax.images)} 层'
    assert ax.images[0].get_cmap().name == 'gray'
