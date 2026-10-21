"""分幅出图（按经纬度切子图）测试。

契约：
1. compute_tile_grid 均分数学正确；单方向分块（另一维=1）即「只按该方向切」；
2. resolve_split_tiles 支持逐幅自定义边界，缺省的边沿用网格对应边界；
3. 每幅子图都是完整地图（岸段色带 + 图例 + 比例尺 + 指北针 + 取景锁定子图窗口）；
4. 未分块时行为与历史版本完全一致（向后兼容，Java 侧零感知）；
5. 子图里岸段配色与整图一致——颜色按整条几何插值，不随分幅改变。
"""
import json
import math
import os
import shutil
import tempfile

import matplotlib.pyplot as plt

from visualization import risk_map

BBOX = (119.20, 32.20, 119.60, 32.40)          # 西、南、东、北
LNG0, LAT0, LNG_SPAN, LAT_SPAN = 119.20, 32.20, 0.40, 0.20


def _sections(count=8, level=2):
    """沿东西向布一排断面（跨整个 BBOX 宽度）。"""
    out = []
    for i in range(count):
        lng = LNG0 + LNG_SPAN * i / (count - 1)
        out.append({
            'section_id': f'S{i}', 'section_name': f'断面{i}',
            'bank_id': 'B1', 'risk_level': level,
            'start_lng': lng, 'start_lat': LAT0,
            'end_lng': lng, 'end_lat': LAT0 + 0.004,
        })
    return out


def _banks():
    """闭合矩形环（与真实 bank_geometry 同形态），覆盖整个 BBOX。"""
    lng1, lat1 = LNG0 + LNG_SPAN, LAT0 + LAT_SPAN
    return [{'bank_id': 'B1', 'bank_name': '甲',
             'coordinates': [[LNG0, LAT0], [lng1, LAT0], [lng1, lat1],
                             [LNG0, lat1], [LNG0, LAT0]]}]


# ---------------------------------------------------------------------------
# 1. 网格数学
# ---------------------------------------------------------------------------

def test_tile_grid_uniform_division():
    tiles = risk_map.compute_tile_grid(BBOX, 2, 2)
    assert len(tiles) == 4
    west, south, east, north = BBOX
    # 行优先，从西北角开始
    assert (tiles[0]['row'], tiles[0]['col']) == (0, 0)
    assert (tiles[3]['row'], tiles[3]['col']) == (1, 1)
    # 第 (0,0) 幅 = 西北角，西北边界即全局边界
    assert abs(tiles[0]['bbox'][0] - west) < 1e-12
    assert abs(tiles[0]['bbox'][3] - north) < 1e-12
    # 每幅宽 = 总宽 / 2
    for tile in tiles:
        w, s, e, n = tile['bbox']
        assert abs((e - w) - (east - west) / 2) < 1e-12
        assert abs((n - s) - (north - south) / 2) < 1e-12
    # 四幅无缝拼回全局
    assert abs(tiles[0]['bbox'][1] - tiles[2]['bbox'][3]) < 1e-12   # 南北相接
    assert abs(tiles[0]['bbox'][2] - tiles[1]['bbox'][0]) < 1e-12   # 东西相接


def test_tile_grid_single_direction():
    """只按一个方向切：另一维传 1。"""
    wide = risk_map.compute_tile_grid(BBOX, 1, 3)      # 只按经度切 3 幅
    assert len(wide) == 3
    for tile in wide:
        w, s, e, n = tile['bbox']
        assert abs(s - BBOX[1]) < 1e-12 and abs(n - BBOX[3]) < 1e-12, '纬度方向不应被切'
        assert abs((e - w) - (BBOX[2] - BBOX[0]) / 3) < 1e-12

    tall = risk_map.compute_tile_grid(BBOX, 3, 1)      # 只按纬度切 3 幅
    assert len(tall) == 3
    for tile in tall:
        w, s, e, n = tile['bbox']
        assert abs(w - BBOX[0]) < 1e-12 and abs(e - BBOX[2]) < 1e-12, '经度方向不应被切'


def test_tile_grid_identity():
    tiles = risk_map.compute_tile_grid(BBOX, 1, 1)
    assert len(tiles) == 1
    assert tiles[0]['bbox'] == BBOX


def test_tile_grid_rejects_bad_input():
    for rows, cols in [(0, 1), (1, 0), (-1, 2)]:
        try:
            risk_map.compute_tile_grid(BBOX, rows, cols)
        except ValueError:
            continue
        raise AssertionError(f'rows={rows}, cols={cols} 应报错')
    try:
        risk_map.compute_tile_grid((119.6, 32.2, 119.2, 32.4), 2, 2)
    except ValueError:
        return
    raise AssertionError('西>=东 的 bbox 应报错')


# ---------------------------------------------------------------------------
# 2. 逐幅自定义边界
# ---------------------------------------------------------------------------

def test_resolve_tiles_partial_override():
    """只改某一幅的西边界，其余三边沿用该网格格的边界。"""
    custom = [{'row': 0, 'col': 1, 'west': 119.35}]
    tiles = risk_map.resolve_split_tiles(BBOX, 2, 2, custom)
    w, s, e, n = tiles[1]['bbox']          # (0,1) 幅
    assert w == 119.35
    # 其余三边仍是均分网格的值
    grid = risk_map.compute_tile_grid(BBOX, 2, 2)
    assert s == grid[1]['bbox'][1] and e == grid[1]['bbox'][2] and n == grid[1]['bbox'][3]
    # 没提到的幅完全不变
    for i in (0, 2, 3):
        assert tiles[i]['bbox'] == grid[i]['bbox']


def test_resolve_tiles_index_aligned_without_row_col():
    """不写 row/col 时按行优先序号对齐网格。"""
    custom = [{'east': 119.30}]            # 即第 0 幅
    tiles = risk_map.resolve_split_tiles(BBOX, 2, 2, custom)
    assert tiles[0]['bbox'][2] == 119.30
    assert tiles[1]['bbox'][2] != 119.30


def test_resolve_tiles_full_custom_ranges():
    custom = [{'west': 119.21, 'south': 32.21, 'east': 119.31, 'north': 32.31}]
    tiles = risk_map.resolve_split_tiles(BBOX, 1, 2, custom)
    assert tiles[0]['bbox'] == (119.21, 32.21, 119.31, 32.31)


def test_resolve_tiles_rejects_out_of_grid():
    try:
        risk_map.resolve_split_tiles(BBOX, 2, 2, [{'row': 2, 'col': 0}])
    except ValueError:
        return
    raise AssertionError('row=2 超出 2 行网格应报错')


def test_resolve_tiles_rejects_inverted_range():
    try:
        risk_map.resolve_split_tiles(BBOX, 1, 2, [{'west': 119.6}])   # west > 网格 east
    except ValueError:
        return
    raise AssertionError('西>=东 的子图范围应报错')


# ---------------------------------------------------------------------------
# 3. 行为级：子图必须是完整地图
# ---------------------------------------------------------------------------

def _draw_split_capture(workdir, rows, cols, custom=None, with_banks=True):
    """跑一次分幅出图，捕获每幅的坐标轴对象。"""
    axes = []
    original = plt.subplots

    def fake_subplots(*args, **kwargs):
        fig, ax = original(*args, **kwargs)
        axes.append(ax)
        return fig, ax

    banks = _banks() if with_banks else None
    plt.subplots = fake_subplots
    try:
        result = risk_map.generate_risk_map_tiles(
            _sections(), '分幅回归', output_dir=workdir, output_name='tiles.png',
            rows=rows, cols=cols, tiles=custom, banks=banks)
    finally:
        plt.subplots = original
    return json.loads(result), axes


def _is_complete_map(ax, expect_segments=True):
    """一幅「完整地图」的构成：岸段色带 + 图例 + 比例尺 + 指北针 + 边框。"""
    from matplotlib.collections import LineCollection
    from matplotlib.patches import Rectangle
    segments = sum(len(c.get_segments()) for c in ax.collections
                   if isinstance(c, LineCollection))
    if expect_segments:
        assert segments > 0, '子图没有岸段色带'
    assert ax.get_legend() is not None, '子图没有图例'
    scale_rects = [p for p in ax.patches if isinstance(p, Rectangle)]
    assert len(scale_rects) >= 3, f'子图没有比例尺（Rectangle 仅 {len(scale_rects)} 个）'
    assert any('N' in t.get_text() for t in ax.texts), '子图没有指北针标注'
    return segments


def test_split_tiles_are_complete_maps():
    workdir = tempfile.mkdtemp(prefix='tiles-')
    try:
        result, axes = _draw_split_capture(workdir, 2, 1)
        assert result['success'] is True
        assert result['tile_count'] == 2
        assert len(axes) == 2, '2 幅应创建 2 个坐标轴'
        assert len(result['file_paths']) == 2
        for tile_result in result['tiles']:
            assert tile_result['success'] is True
            assert os.path.exists(tile_result['file_path'])
        # 两幅都完整
        assert _is_complete_map(axes[0]) > 0
        assert _is_complete_map(axes[1]) > 0
        # 取景各自锁定子图窗口（南北相邻，不重叠）
        assert abs(axes[0].get_ylim()[0] - axes[1].get_ylim()[1]) < 1e-9
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def test_sections_in_tile_is_edge_inclusive():
    """落在分界线上的断面同时计进相邻两幅。

    这条含边规则（w ≤ lng ≤ e 且 s ≤ lat ≤ n）是**跨语言契约**：
    报告端 `ReportWorkflowService.partitionContains` 必须复刻同一条判定，
    否则会出现「图上标注 N 条、报告里算出 N-1 条」。
    """
    workdir = tempfile.mkdtemp(prefix='tilesedge-')
    try:
        sections = [
            {'section_id': 'L', 'section_name': '左', 'risk_level': 1,
             'start_lng': 119.30, 'start_lat': 32.30, 'end_lng': 119.30, 'end_lat': 32.31},
            {'section_id': 'M', 'section_name': '界', 'risk_level': 2,
             'start_lng': 119.40, 'start_lat': 32.30, 'end_lng': 119.40, 'end_lat': 32.31},
            {'section_id': 'R', 'section_name': '右', 'risk_level': 3,
             'start_lng': 119.50, 'start_lat': 32.30, 'end_lng': 119.50, 'end_lat': 32.31},
        ]
        # 无岸段：包络 = 断面范围 119.30~119.50 扩边 15% → 119.27~119.53，
        # cols=2 的分界线恰好落在 119.40（与断面 M 重合）
        result = json.loads(risk_map.generate_risk_map_tiles(
            sections, '含边判定回归', output_dir=workdir, output_name='edge.png',
            rows=1, cols=2, banks=None))
        assert result['success'] is True
        assert result['tile_count'] == 2

        counts = [tile['sections_in_tile'] for tile in result['tiles']]
        left, right = result['tiles'][0]['bbox'], result['tiles'][1]['bbox']
        assert abs(left[2] - 119.40) < 1e-9 and abs(right[0] - 119.40) < 1e-9, (left, right)
        assert counts == [2, 2], f'分界线上的断面应同时归入两侧，实际 {counts}'
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def test_split_only_along_longitude():
    """只按经度方向切 3 幅：每幅都是完整图，纬度范围不变。"""
    workdir = tempfile.mkdtemp(prefix='tiles1x3-')
    try:
        result, axes = _draw_split_capture(workdir, 1, 3)
        assert result['tile_count'] == 3
        # 纬度方向不分块：每幅的纬度范围 = 全局（扩边后）范围
        base = risk_map.data_extent_with_padding(_sections(), _banks())
        for ax in axes:
            _is_complete_map(ax)
            assert abs(ax.get_ylim()[0] - base[1]) < 1e-9
            assert abs(ax.get_ylim()[1] - base[3]) < 1e-9
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def test_split_naming_and_backward_compat():
    workdir = tempfile.mkdtemp(prefix='tilesname-')
    try:
        # 多幅：_r{r}c{c} 后缀
        result, _ = _draw_split_capture(workdir, 2, 2)
        names = [os.path.basename(p) for p in result['file_paths']]
        assert names == ['tiles_r0c0.png', 'tiles_r0c1.png',
                         'tiles_r1c0.png', 'tiles_r1c1.png'], names

        # 未分块：原名 + 与历史版本一致的返回结构（无 tiles 键）
        single = json.loads(risk_map.generate_risk_map_tiles(
            _sections(), '单幅', output_dir=workdir, output_name='single.png',
            rows=1, cols=1, banks=_banks()))
        assert single['success'] is True
        assert os.path.basename(single['file_path']) == 'single.png'
        assert 'tiles' not in single and 'tile_count' not in single
        assert os.path.exists(os.path.join(workdir, 'single.png'))
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def test_split_colors_match_full_map():
    """分幅不改色：子图与整图里同一岸段的逐段配色必须一致。

    颜色按整条几何插值计算，与显示范围无关——否则同一岸段在不同分幅里
    会变色，等于把「插值岸段」切成了互不一致的几段。
    """
    from matplotlib.collections import LineCollection

    def colors_of(ax):
        found = []
        for coll in ax.collections:
            if isinstance(coll, LineCollection) and len(coll.get_segments()) > 1:
                found.append([tuple(c) for c in coll.get_colors()])
        return found

    workdir = tempfile.mkdtemp(prefix='tilescolor-')
    try:
        full_axes = []
        original = plt.subplots

        def fake_subplots(*args, **kwargs):
            fig, ax = original(*args, **kwargs)
            full_axes.append(ax)
            return fig, ax

        # 整图
        plt.subplots = fake_subplots
        try:
            risk_map.generate_risk_distribution_map(
                _sections(), '整图', output_dir=workdir, output_name='full.png',
                banks=_banks())
        finally:
            plt.subplots = original
        full_colors = colors_of(full_axes[0])

        _, axes = _draw_split_capture(workdir, 1, 2)
        for ax in axes:
            assert colors_of(ax) == full_colors, '分幅后岸段配色与整图不一致'
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def test_bbox_locks_axis_range():
    """bbox 参数：坐标轴范围精确等于传入窗口。"""
    workdir = tempfile.mkdtemp(prefix='tilesbbox-')
    try:
        captured = {}
        original = plt.subplots

        def fake_subplots(*args, **kwargs):
            fig, ax = original(*args, **kwargs)
            captured['ax'] = ax
            return fig, ax

        target = (119.30, 32.24, 119.45, 32.36)
        plt.subplots = fake_subplots
        try:
            result = json.loads(risk_map.generate_risk_distribution_map(
                _sections(), '窗口', output_dir=workdir, output_name='win.png',
                banks=_banks(), bbox=target))
        finally:
            plt.subplots = original
        assert result['success'] is True
        ax = captured['ax']
        assert abs(ax.get_xlim()[0] - target[0]) < 1e-9
        assert abs(ax.get_xlim()[1] - target[2]) < 1e-9
        assert abs(ax.get_ylim()[0] - target[1]) < 1e-9
        assert abs(ax.get_ylim()[1] - target[3]) < 1e-9
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def test_split_counts_sections_per_tile():
    """每幅报告范围内断面数，便于判断空幅。"""
    workdir = tempfile.mkdtemp(prefix='tilescount-')
    try:
        result, _ = _draw_split_capture(workdir, 1, 2)
        counts = [t['sections_in_tile'] for t in result['tiles']]
        assert counts == [4, 4], counts       # 8 条断面均分两幅
        assert all(t['bbox'][2] > t['bbox'][0] for t in result['tiles'])
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def main():
    tests = [(name, obj) for name, obj in sorted(globals().items())
             if name.startswith('test_') and callable(obj)]
    failed = []
    for name, func in tests:
        try:
            func()
            print(f'  PASS  {name}')
        except AssertionError as exc:
            failed.append(name)
            print(f'  FAIL  {name}: {exc}')
        except Exception as exc:                              # noqa: BLE001
            failed.append(name)
            print(f'  ERROR {name}: {type(exc).__name__}: {exc}')
    print(f'\n{len(tests) - len(failed)}/{len(tests)} passed')
    if failed:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
