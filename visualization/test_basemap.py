"""
遥感影像底图（visualization/basemap.py）单元测试

覆盖四类：
  1) Web Mercator 瓦片数学（往返、锚点、覆盖性、缩放级选择）
  2) 配置解析与降级（缺 key / 显式关闭 / 未知 provider / 环境变量取 key）
  3) 拼接与纬度重采样（本地瓦片金字塔 → 图像 + extent，且纬度方向被拉直）
  4) 端到端回归：加底图**不改变** DEM / 岸段 / 断面三层的任何绘制
"""
import os
import shutil
import tempfile

import numpy as np
from PIL import Image

from visualization import basemap, risk_map


# ---------------------------------------------------------------------------
# 1. 瓦片数学
# ---------------------------------------------------------------------------

def test_lnglat_pixel_roundtrip():
    for zoom in (0, 3, 8, 12, 18):
        for lng, lat in ((117.0, 32.0), (-180.0, 0.0), (179.9, -84.0), (0.0, 85.0)):
            px, py = basemap.lnglat_to_pixel(lng, lat, zoom)
            back_lng, back_lat = basemap.pixel_to_lnglat(px, py, zoom)
            assert abs(back_lng - lng) < 1e-9, (zoom, lng, back_lng)
            assert abs(back_lat - lat) < 1e-9, (zoom, lat, back_lat)


def test_mercator_norm_anchors():
    assert abs(basemap.lat_to_mercator_norm(0.0) - 0.5) < 1e-12
    assert abs(basemap.lat_to_mercator_norm(basemap.MAX_MERCATOR_LAT)) < 1e-9
    assert abs(basemap.lat_to_mercator_norm(-basemap.MAX_MERCATOR_LAT) - 1.0) < 1e-9
    # 超范围纬度必须被截断（否则 tan() 会溢出）
    assert abs(basemap.lat_to_mercator_norm(89.9)
               - basemap.lat_to_mercator_norm(basemap.MAX_MERCATOR_LAT)) < 1e-12


def test_tile_extent_known_values():
    west, east, south, north = basemap.tile_extent(0, 0, 1)
    assert abs(west + 180.0) < 1e-9 and abs(east - 0.0) < 1e-9
    assert abs(north - basemap.MAX_MERCATOR_LAT) < 1e-6 and abs(south) < 1e-9

    west, east, south, north = basemap.tile_extent(1, 1, 1)
    assert abs(west) < 1e-9 and abs(east - 180.0) < 1e-9
    assert abs(north) < 1e-9 and abs(south + basemap.MAX_MERCATOR_LAT) < 1e-6


def test_tile_range_covers_requested_bbox():
    bbox = (117.81, 31.55, 119.79, 32.61)
    for zoom in (6, 9, 12):
        x0, y0, x1, y1 = basemap.tile_range(*bbox, zoom)
        west, _, _, north = basemap.tile_extent(x0, y0, zoom)
        _, east, south, _ = basemap.tile_extent(x1, y1, zoom)
        assert west <= bbox[0] and east >= bbox[2], (zoom, west, east)
        assert north >= bbox[3] and south <= bbox[1], (zoom, north, south)


def test_mosaic_pixels_map_back_to_the_right_lnglat():
    """像素级对齐：给定 (lng, lat)，反查到的像素必须落在「该经纬度所属瓦片」的颜色上。

    这是「底图能不能和岸段/断面叠在同一个坐标系里」的核心验证 —— 只比 extent 不够，
    因为 extent 对了但行列顺序反了、或纬度没拉直，一样会整体错位。
    """
    workdir = tempfile.mkdtemp(prefix='bmalign-')
    try:
        bbox = (117.95, 31.95, 118.15, 32.10)
        zoom = 12
        x0, y0, x1, y1 = basemap.tile_range(*bbox, zoom)

        palette = {}
        for x in range(x0, x1 + 1):
            for y in range(y0, y1 + 1):
                color = ((x * 37) % 200 + 20, (y * 53) % 200 + 20, ((x + y) * 29) % 200 + 20)
                palette[(x, y)] = color
                directory = os.path.join(workdir, str(zoom), str(x))
                os.makedirs(directory, exist_ok=True)
                Image.new('RGB', (basemap.TILE_SIZE, basemap.TILE_SIZE), color).save(
                    os.path.join(directory, f'{y}.png'))

        config = basemap.resolve_config({'provider': 'local', 'path': workdir, 'zoom': zoom})
        array, extent = basemap.render_basemap(config, bbox)
        assert array is not None

        rows, cols = array.shape[0], array.shape[1]
        m_north, m_south = extent[3], extent[2]

        checked = 0
        for x in range(x0, x1 + 1):
            for y in range(y0, y1 + 1):
                # 取该瓦片中心经纬度（远离瓦片边界，避免重采样的半像素抖动）
                west, east, south, north = basemap.tile_extent(x, y, zoom)
                lng, lat = (west + east) / 2.0, (south + north) / 2.0
                if not (bbox[0] <= lng <= bbox[2] and bbox[1] <= lat <= bbox[3]):
                    continue

                px, _ = basemap.lnglat_to_pixel(lng, lat, zoom)
                col = int(px) - x0 * basemap.TILE_SIZE
                # 重采样后：行号与纬度线性对应
                row = int(round((m_north - lat) / (m_north - m_south) * (rows - 1)))
                assert 0 <= col < cols and 0 <= row < rows, (lng, lat, col, row)

                got = tuple(int(v) for v in array[row, col])
                assert got == palette[(x, y)], (
                    f'({lng:.5f}, {lat:.5f}) 落在瓦片 {x}/{y}，'
                    f'应得 {palette[(x, y)]}，实得 {got}')
                checked += 1
        assert checked >= 4, f'有效采样点太少（{checked}）'
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def test_choose_zoom_respects_resolution_and_tile_budget():
    bbox = (117.81, 31.55, 119.79, 32.61)
    coarse = basemap.choose_zoom(*bbox, target_px_width=300.0)
    fine = basemap.choose_zoom(*bbox, target_px_width=6000.0)
    assert coarse <= fine, (coarse, fine)

    # 缩放级必须落在配置区间内，且瓦片数不超上限
    z = basemap.choose_zoom(*bbox, target_px_width=6000.0, min_zoom=5, max_zoom=14, max_tiles=64)
    assert 5 <= z <= 14
    assert basemap.tile_count(*bbox, z) <= 64


def test_tile_ground_resolution_shrinks_with_zoom():
    assert (basemap.tile_ground_resolution(13, 32.0)
            < basemap.tile_ground_resolution(12, 32.0))
    # zoom 12、纬度 32°，常识值约 32 m/px
    assert 25.0 < basemap.tile_ground_resolution(12, 32.0) < 40.0


# ---------------------------------------------------------------------------
# 2. 配置解析与降级
# ---------------------------------------------------------------------------

def _clear_key_env():
    saved = {name: os.environ.pop(name, None) for name in ('TIANDITU_TK', 'TIANDITU_KEY')}
    return saved


def _restore_env(saved):
    for name, value in saved.items():
        if value is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = value


def test_resolve_config_absent_or_disabled_returns_none():
    assert basemap.resolve_config(None) is None
    assert basemap.resolve_config({'enabled': False}) is None
    assert basemap.resolve_config({'enabled': 'false'}) is None
    assert basemap.resolve_config({'enabled': 'off'}) is None
    assert basemap.resolve_config('不是对象') is None


def test_tianditu_without_key_is_disabled():
    saved = _clear_key_env()
    try:
        assert basemap.resolve_config({'provider': 'tianditu'}) is None
        assert basemap.resolve_config({'provider': 'tianditu', 'key': ''}) is None
        assert basemap.resolve_config(
            {'provider': 'tianditu', 'key': basemap.TIANDITU_KEY_PLACEHOLDER}) is None
    finally:
        _restore_env(saved)


def test_tianditu_key_from_env_and_auto_enabled():
    saved = _clear_key_env()
    try:
        os.environ['TIANDITU_TK'] = 'tk-abc123'
        config = basemap.resolve_config({'provider': 'tianditu', 'enabled': 'auto'})
        assert config is not None, 'enabled=auto + 有 key 应当启用'
        assert config['key'] == 'tk-abc123'
        assert config['layer'] == 'img'
        assert config['attribution'], '天地图必须带来源标注'
    finally:
        _restore_env(saved)


def test_tianditu_unknown_layer_falls_back_to_img():
    saved = _clear_key_env()
    try:
        os.environ['TIANDITU_TK'] = 'tk-abc123'
        config = basemap.resolve_config({'provider': 'tianditu', 'layer': '不存在'})
        assert config['layer'] == 'img'
    finally:
        _restore_env(saved)


def test_unknown_provider_is_refused():
    assert basemap.resolve_config({'provider': 'google'}) is None
    assert basemap.resolve_config({'provider': 'osm'}) is None
    assert basemap.resolve_config({'provider': 'mapbox'}) is None


def test_custom_provider_needs_url_template_and_needs_no_key():
    assert basemap.resolve_config({'provider': 'custom'}) is None
    config = basemap.resolve_config(
        {'provider': 'custom', 'url_template': 'https://x/{z}/{x}/{y}.png'})
    assert config is not None and config['url_template'].endswith('{y}.png')
    assert basemap.build_tile_url(config, 3, 1, 2) == 'https://x/3/1/2.png'


def test_local_provider_needs_path():
    assert basemap.resolve_config({'provider': 'local'}) is None
    assert basemap.resolve_config({'provider': 'local', 'path': '/tmp/tiles'}) is not None


def test_tianditu_url_is_wmts_and_key_is_maskable():
    config = {'provider': 'tianditu', 'layer': 'img', 'key': 'SECRET-KEY'}
    url = basemap.build_tile_url(config, 12, 3412, 1688, subdomain=3)
    assert url.startswith('https://t3.tianditu.gov.cn/img_w/wmts')
    assert 'TILEMATRIX=12' in url and 'TILEROW=1688' in url and 'TILECOL=3412' in url
    assert 'tk=SECRET-KEY' in url
    masked = basemap._mask_key(url)
    assert 'SECRET-KEY' not in masked and 'tk=***' in masked


# ---------------------------------------------------------------------------
# 3. 拼接与纬度重采样
# ---------------------------------------------------------------------------

def _make_tile_pyramid(root, zoom, x0, y0, x1, y1, color):
    for x in range(x0, x1 + 1):
        directory = os.path.join(root, str(zoom), str(x))
        os.makedirs(directory, exist_ok=True)
        for y in range(y0, y1 + 1):
            Image.new('RGB', (basemap.TILE_SIZE, basemap.TILE_SIZE), color).save(
                os.path.join(directory, f'{y}.png'))


def test_local_tiles_render_with_extent_covering_bbox():
    workdir = tempfile.mkdtemp(prefix='bmlocal-')
    try:
        bbox = (117.90, 31.90, 118.10, 32.10)
        zoom = 12
        x0, y0, x1, y1 = basemap.tile_range(*bbox, zoom)
        _make_tile_pyramid(workdir, zoom, x0, y0, x1, y1, (10, 120, 200))

        config = basemap.resolve_config(
            {'provider': 'local', 'path': workdir, 'zoom': zoom})
        assert config is not None
        array, extent = basemap.render_basemap(config, bbox)

        assert array is not None and extent is not None
        assert array.shape[2] == 3
        assert array.shape[0] == (y1 - y0 + 1) * basemap.TILE_SIZE
        assert extent[0] <= bbox[0] and extent[1] >= bbox[2]
        assert extent[3] >= bbox[3] and extent[2] <= bbox[1]
        assert tuple(array[10, 10]) == (10, 120, 200), '瓦片内容应被原样拼进画布'
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def test_local_tiles_missing_returns_none_instead_of_raising():
    workdir = tempfile.mkdtemp(prefix='bmempty-')
    try:
        config = basemap.resolve_config({'provider': 'local', 'path': workdir, 'zoom': 10})
        array, extent = basemap.render_basemap(config, (117.9, 31.9, 118.1, 32.1))
        assert array is None and extent is None
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def test_invalid_bbox_is_skipped():
    workdir = tempfile.mkdtemp(prefix='bmbbox-')
    try:
        config = basemap.resolve_config({'provider': 'local', 'path': workdir, 'zoom': 10})
        assert basemap.render_basemap(config, (118.0, 32.0, 117.0, 31.0)) == (None, None)
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def test_tile_count_guard_blocks_huge_requests():
    workdir = tempfile.mkdtemp(prefix='bmguard-')
    try:
        config = basemap.resolve_config(
            {'provider': 'local', 'path': workdir, 'zoom': 18, 'max_tiles': 4})
        array, extent = basemap.render_basemap(config, (100.0, 20.0, 120.0, 40.0))
        assert array is None, '超过瓦片数上限必须放弃，而不是把内存/网络打满'
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def test_fetch_budget_bounds_wall_clock_time():
    """慢源不能把出图卡死：预算到点就必须收手（Java 侧起 Python 进程只等 60 s）。"""
    import time

    bbox = (117.95, 31.95, 118.15, 32.10)
    config = basemap.resolve_config({
        'provider': 'custom', 'url_template': 'https://slow.invalid/{z}/{x}/{y}.png',
        'zoom': 12, 'timeout': 5.0, 'retries': 0, 'time_budget': 1.0})
    total = basemap.tile_count(*bbox, 12)

    original = basemap._download
    attempted = []

    def slow_download(url, timeout, retries, deadline=None):
        # 复刻真实实现：预算耗尽前不再发起新请求
        remaining = None if deadline is None else deadline - time.monotonic()
        if remaining is not None and remaining <= 0.3:
            return None
        attempted.append(url)
        time.sleep(timeout if remaining is None else min(timeout, remaining))
        return None

    basemap._download = slow_download
    try:
        start = time.monotonic()
        array, extent = basemap.render_basemap(config, bbox)
        elapsed = time.monotonic() - start
    finally:
        basemap._download = original

    assert array is None and extent is None
    assert elapsed < 1.6, f'取图墙钟耗时 {elapsed:.2f}s，超出 1.0s 预算太多'
    assert len(attempted) < total, f'预算耗尽后不应再发起请求（已发 {len(attempted)}/{total}）'


def test_warp_rows_to_linear_lat_flattens_meridian_nonlinearity():
    """纬度方向非线性必须被拉直。

    行数按真实量级取（zoom 12、1° 纬度 ≈ 14 张瓦片 = 3584 行）：此时不拉直的偏差
    约 5 个像素行，肉眼可见；拉直后应回到半行以内。
    """
    south, north = 31.5, 32.5
    rows = 14 * basemap.TILE_SIZE
    top = basemap.lat_to_mercator_norm(north)
    bottom = basemap.lat_to_mercator_norm(south)
    norms = top + (bottom - top) * (np.arange(rows) / (rows - 1.0))
    src_lat = np.array([basemap.mercator_norm_to_lat(v) for v in norms])

    array = np.repeat(src_lat.reshape(rows, 1, 1), 2, axis=2)      # 每行的值 = 该行纬度
    warped = basemap._warp_rows_to_linear_lat(array, south, north)
    target = np.linspace(src_lat[0], src_lat[-1], rows)

    step = abs(src_lat[0] - src_lat[-1]) / rows
    error = np.max(np.abs(warped[:, 0, 0] - target))
    assert error <= step, f'重采样后仍偏离线性纬度 {error:.6f}°（允许 {step:.6f}°）'

    # 不拉直的偏差应明显大于一个像素行 —— 说明这一步不是多余的
    naive = np.max(np.abs(src_lat - target))
    assert naive > 2 * step, f'原始墨卡托行本身就应该有明显偏差，实测 {naive:.6f}°'


# ---------------------------------------------------------------------------
# 4. 端到端回归：加底图不得改变现有三层
# ---------------------------------------------------------------------------

def _draw_and_capture(workdir, basemap_config):
    """跑一次出图，返回可供对比的各层快照。"""
    import matplotlib.pyplot as plt
    from matplotlib.collections import LineCollection, PathCollection

    sections = [
        {'section_id': 'S1', 'section_name': '断面1', 'risk_level': 1, 'bank_id': 'B1',
         'start_lng': 117.95, 'start_lat': 31.95, 'end_lng': 117.97, 'end_lat': 31.96},
        {'section_id': 'S2', 'section_name': '断面2', 'risk_level': 4, 'bank_id': 'B1',
         'start_lng': 118.05, 'start_lat': 32.05, 'end_lng': 118.07, 'end_lat': 32.06},
    ]
    banks = [{'bank_id': 'B1', 'bank_name': '甲',
              'coordinates': [[117.90 + i * 0.01, 31.90 + i * 0.01] for i in range(12)]
                             + [[117.90, 31.90]]}]

    captured = {}
    original = plt.subplots

    def fake_subplots(*args, **kwargs):
        fig, ax = original(*args, **kwargs)
        captured['fig'], captured['ax'] = fig, ax
        return fig, ax

    plt.subplots = fake_subplots
    try:
        result = risk_map.generate_risk_distribution_map(
            sections, '回归', output_dir=workdir, output_name='reg.png', banks=banks,
            basemap=basemap_config)
    finally:
        plt.subplots = original

    assert '"success": true' in result, result
    ax = captured['ax']
    collections = list(ax.collections)
    Line2D = type(ax.plot([], [])[0])
    return {
        'images': list(ax.get_images()),
        'bank_segments': sum(len(c.get_segments()) for c in collections
                             if isinstance(c, LineCollection)),
        'bank_colors': [tuple(c.get_colors()[0]) for c in collections
                        if isinstance(c, LineCollection)],
        'section_endpoints': sum(len(c.get_offsets()) for c in collections
                                 if isinstance(c, PathCollection)),
        'lines': sorted(tuple(map(tuple, l.get_xydata())) for l in ax.get_lines()
                        if isinstance(l, Line2D)),
        'xlim': ax.get_xlim(),
        'ylim': ax.get_ylim(),
    }


def test_basemap_adds_bottom_layer_without_touching_existing_layers():
    workdir = tempfile.mkdtemp(prefix='bmreg-')
    try:
        plain = _draw_and_capture(workdir, None)

        bbox = (117.85, 31.85, 118.15, 32.15)
        zoom = 12
        x0, y0, x1, y1 = basemap.tile_range(*bbox, zoom)
        _make_tile_pyramid(workdir, zoom, x0, y0, x1, y1, (30, 90, 40))
        with_base = _draw_and_capture(workdir, {
            'provider': 'local', 'path': workdir, 'zoom': zoom})

        assert len(plain['images']) == 0, '不加底图时不该有任何图像层'
        assert len(with_base['images']) == 1, '底图应作为一张图像加入'
        image = with_base['images'][0]
        assert image.get_zorder() == basemap.BASEMAP_ZORDER
        assert image.get_zorder() < 0, '底图 zorder 必须低于 DEM(0)'

        # 岸段色带 / 经纬网 / 取景：一个都不许变
        assert with_base['bank_segments'] == plain['bank_segments'] > 0
        assert with_base['bank_colors'] == plain['bank_colors']
        # 断面线/端点已按需求移除：加不加底图都不应有任何断面图元
        assert with_base['section_endpoints'] == plain['section_endpoints'] == 0
        assert with_base['lines'] == plain['lines']
        assert with_base['xlim'] == plain['xlim']
        assert with_base['ylim'] == plain['ylim']
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def test_draw_basemap_returns_report_only_when_drawn():
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots()
    try:
        assert basemap.draw_basemap(ax, None, (117.0, 31.0, 118.0, 32.0)) == {'drawn': False}
        assert basemap.draw_basemap(ax, {'provider': 'tianditu'}, (117.0, 31.0, 118.0, 32.0)) \
            == {'drawn': False}
    finally:
        plt.close(fig)


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
