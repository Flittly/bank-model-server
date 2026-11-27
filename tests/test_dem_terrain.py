"""DEM 三维地形瓦片（``model.demTerrain``）守门。

这个模块的产物不是给人看的图，而是给 Mapbox **解码后再拿去做几何**的高程数据 ——
差一位就是错 1/256 m，错一层就是整片地形错位。这里锁四件事：

- Terrarium 编码与 Mapbox 解码器逐位对应（含负高程 —— 长江河床高程就是负的）；
- 层级 / 瓦片行列号算得对（错一层 = 瓦片请求全 404，地形静默不显示）；
- 裙边：走廊外不是悬崖，内部小空洞被补平，超出裙片的像素是 NaN；
- 端到端：拿一张构造的小 DEM 真的走一遍 GDAL，解回来的高程等于源值。

纯函数部分不依赖 GDAL；只有最后一个用例会拉起 GDAL（没有 GDAL 则跳过）。
"""

import math
import os
import sys

import numpy as np
import pytest

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from model import demTerrain  # noqa: E402

_HALF_WORLD = 20037508.342789244


# ---------------------------------------------------------------- 编码

def test_terrarium_round_trip_matches_mapbox_decoder():
    heights = np.array([[-35.0789, -0.5, 0.0], [0.00390625, 7.6796, 1234.5678]], dtype=np.float64)
    rgb = demTerrain.encode_terrarium(heights)

    assert rgb.dtype == np.uint8 and rgb.shape == (2, 3, 3)
    back = demTerrain.decode_terrarium(rgb)
    # 量化步长 1/256 m，最大误差半个步长
    assert np.abs(back - heights).max() <= 1.0 / 512

    # 与 Mapbox 的解码公式逐位核对一个已知值：0 m → v = 32768 * 256 = 0x800000
    assert tuple(int(v) for v in demTerrain.encode_terrarium(np.array([[0.0]]))[0, 0]) == (128, 0, 0)


def test_terrarium_rejects_nan():
    with pytest.raises(ValueError):
        demTerrain.encode_terrarium(np.array([[1.0, np.nan]]))


def test_terrarium_clamps_out_of_range():
    rgb = demTerrain.encode_terrarium(np.array([[-99999.0, 99999.0]]))
    back = demTerrain.decode_terrarium(rgb)
    assert -32768.0 <= back[0, 0] <= -32767.0
    assert 32767.0 <= back[0, 1] <= 32768.0


# ---------------------------------------------------------------- 层级与瓦片号

def test_tile_resolution_matches_web_mercator():
    assert demTerrain.tile_resolution(0) == pytest.approx(156543.0339, rel=1e-6)
    assert demTerrain.tile_resolution(12) == pytest.approx(38.2185, rel=1e-4)
    assert demTerrain.tile_resolution(13) == pytest.approx(demTerrain.tile_resolution(12) / 2)


@pytest.mark.parametrize("resolution,expected", [
    (30.0, 12),   # 长江 30 m DEM：z12 = 38 m/px，z13 会白白放大 2.5 倍
    (20.0, 13),
    (10.0, 14),
    (5.0, 15),
    (0.01, demTerrain.MAX_ZOOM_CAP),   # 超清 DEM 被硬上限挡住
    (1000000.0, demTerrain.MIN_ZOOM),   # 极粗的 DEM 被最低层挡住
])
def test_native_zoom_keeps_source_detail(resolution, expected):
    assert demTerrain.native_zoom(resolution) == expected


def test_native_zoom_rejects_bad_resolution():
    with pytest.raises(ValueError):
        demTerrain.native_zoom(0)


def _lonlat_to_3857(lon, lat):
    """测试里独立写一遍墨卡托正算，不复用被测代码。"""
    radius = 6378137.0
    x = math.radians(lon) * radius
    y = radius * math.log(math.tan(math.pi / 4 + math.radians(lat) / 2))
    return x, y


def test_tile_range_matches_xyz_tile_numbers():
    # 南京附近（118.8E, 32.0N）：z12 的 XYZ 瓦片号
    x, y = _lonlat_to_3857(118.8, 32.0)
    n = 2**12
    expect_x = math.floor((118.8 + 180.0) / 360.0 * n)
    lat_rad = math.radians(32.0)
    expect_y = math.floor((1 - math.log(math.tan(lat_rad) + 1 / math.cos(lat_rad)) / math.pi) / 2 * n)

    x0, x1, y0, y1 = demTerrain.tile_range((x - 10, y - 10, x + 10, y + 10), 12)
    assert (x0, x1, y0, y1) == (expect_x, expect_x, expect_y, expect_y)
    assert expect_x == 3399

    # 跨过两张瓦片：东西各伸出半张
    tile_metres = demTerrain.tile_resolution(12) * demTerrain.TILE_SIZE
    x0, x1, y0, y1 = demTerrain.tile_range((x - tile_metres, y - 10, x + tile_metres, y + 10), 12)
    assert (x1 - x0) >= 2


def test_tile_range_grows_toward_south():
    """y 轴朝下：北边是大 y、小行号；边界正好压在瓦片边上也不能多出一行。"""
    x, _ = _lonlat_to_3857(118.8, 32.0)
    tile_metres = demTerrain.tile_resolution(12) * demTerrain.TILE_SIZE
    row = 1662
    top = demTerrain.ORIGIN_Y - row * tile_metres  # 该行瓦片的北边

    # bounds 的顺序是 (minx, miny, maxx, maxy)；两个框上下相邻、都压在格子正中
    _, _, north_row, _ = demTerrain.tile_range((x, top - 1.5 * tile_metres, x, top - 0.5 * tile_metres), 12)
    _, _, south_row, _ = demTerrain.tile_range(
        (x, top - 2.5 * tile_metres, x, top - 1.5 * tile_metres), 12)
    assert north_row == row
    assert south_row == row + 1


def test_bounds_to_lonlat_round_trips():
    lon, lat = 118.8, 32.0
    x, y = _lonlat_to_3857(lon, lat)
    west, south, east, north = demTerrain.bounds_to_lonlat((x - 1000, y - 1000, x + 1000, y + 1000))
    assert west < lon < east
    assert south < lat < north
    assert east - west == pytest.approx(2000 / 6378137.0 * 180 / math.pi, rel=1e-3)


# ---------------------------------------------------------------- 最近邻

def _brute_force_nearest(valid):
    h, w = valid.shape
    seeds = np.argwhere(valid)
    out = np.full((h, w), np.inf)
    for i in range(h):
        for j in range(w):
            out[i, j] = np.hypot(seeds[:, 0] - i, seeds[:, 1] - j).min()
    return out


def test_jump_flood_matches_brute_force():
    rng = np.random.default_rng(7)
    valid = rng.random((14, 14)) < 0.12
    valid[0, 0] = valid[13, 13] = True
    near_row, near_col = demTerrain._jump_flood_nearest(valid)

    truth = _brute_force_nearest(valid)
    got = np.hypot(near_row - np.arange(14)[:, None], near_col - np.arange(14)[None, :])
    # 只要最近点在半个网格以内就必须精确（更远的候选落在裙边之外，权重本来就是 0）
    close = truth <= 7
    assert close.sum() > 50
    assert np.allclose(got[close], truth[close])
    assert valid[near_row.ravel(), near_col.ravel()].all()


def test_jump_flood_keeps_valid_cell_as_itself():
    valid = np.zeros((10, 10), dtype=bool)
    valid[4, 4] = valid[4, 7] = True
    near_row, near_col = demTerrain._jump_flood_nearest(valid)
    assert (near_row[4, 4], near_col[4, 4]) == (4, 4)
    assert (near_row[4, 7], near_col[4, 7]) == (4, 7)


# ---------------------------------------------------------------- 裙边

def _window(size):
    return np.full((size, size), np.nan, dtype=np.float32)


def test_resolve_tile_returns_centre_untouched_when_all_valid():
    size = demTerrain.TILE_SIZE + 2 * 40
    ramp = np.arange(size * size, dtype=np.float32).reshape(size, size) - 5000
    centre = demTerrain.resolve_tile(ramp, apron_px=40)
    np.testing.assert_array_equal(centre, ramp[40:40 + demTerrain.TILE_SIZE, 40:40 + demTerrain.TILE_SIZE])


def test_resolve_tile_rejects_mismatched_window():
    with pytest.raises(ValueError):
        demTerrain.resolve_tile(_window(300), 40)
    # 光晕 42 的两倍不能被 8 整除
    with pytest.raises(ValueError):
        demTerrain.resolve_tile(_window(demTerrain.TILE_SIZE + 2 * 42), 40)


def test_apron_slopes_down_and_ends_in_nan():
    size = demTerrain.TILE_SIZE + 2 * 40
    mean = _window(size)
    h = 40
    # 左半边是 100 m 的台地，右半边全是 nodata
    mean[:, : size // 2] = 100.0
    tile = demTerrain.resolve_tile(mean, apron_px=40)

    row = tile[demTerrain.TILE_SIZE // 2]
    boundary = size // 2 - h  # 边界在瓦片内的列号
    assert np.isfinite(row[:boundary]).all() and (row[:boundary] == 100.0).all()

    apron = row[boundary: boundary + 30].astype(np.float64)
    assert np.isfinite(apron).all()
    # 紧贴边界还是高台地，然后单调下降
    assert apron[0] > 80.0
    assert np.all(np.diff(apron) <= 1e-3)
    assert apron[-1] < apron[0]
    # 40 px 之外没有裙边
    assert np.isnan(row[boundary + 60:]).all()


def test_apron_fills_small_interior_hole_with_neighbour_value():
    size = demTerrain.TILE_SIZE + 2 * 40
    mean = np.full((size, size), 50.0, dtype=np.float32)
    h = 40
    mean[h + 100:h + 106, h + 100:h + 106] = np.nan  # 瓦片正中一个 6×6 的小洞
    tile = demTerrain.resolve_tile(mean, apron_px=40)

    hole = tile[100:106, 100:106]
    assert np.all(np.abs(hole - 50.0) < 2.0)
    assert (tile[99, 100:106] == 50.0).all()


def test_apron_keeps_thin_corridor_exact():
    size = demTerrain.TILE_SIZE + 2 * 40
    mean = _window(size)
    h = 40
    mean[h + 120:h + 130, :] = np.arange(size, dtype=np.float32)[None, :] - 300  # 10 px 宽的斜坡走廊
    tile = demTerrain.resolve_tile(mean, apron_px=8)
    np.testing.assert_array_equal(tile[120:130], mean[h + 120:h + 130, h:h + demTerrain.TILE_SIZE])


def test_resolve_tile_all_invalid_is_all_nan():
    tile = demTerrain.resolve_tile(_window(demTerrain.TILE_SIZE + 2 * 40), 40)
    assert np.isnan(tile).all()


# ---------------------------------------------------------------- 指纹

def test_fingerprint_tracks_content_and_rejects_odd_values(tmp_path):
    a = tmp_path / "a.tif"
    a.write_bytes(b"x" * 10)
    fp1 = demTerrain.source_fingerprint(a)
    assert demTerrain.is_valid_fingerprint(fp1)

    assert not demTerrain.is_valid_fingerprint("../etc")
    assert not demTerrain.is_valid_fingerprint("")
    assert not demTerrain.is_valid_fingerprint(None)
    assert not demTerrain.is_valid_fingerprint("0123456789abcdef0")  # 17 位

    os.utime(a, (a.stat().st_atime, a.stat().st_mtime + 10))
    a.write_bytes(b"y" * 11)
    assert demTerrain.source_fingerprint(a) != fp1


# ---------------------------------------------------------------- 端到端

gdal = pytest.importorskip("osgeo.gdal", reason="需要 GDAL")
from osgeo import osr  # noqa: E402


def _make_synthetic_dem(path, width=900, height=620, pixel=30.0):
    """一张 3857 的小 DEM：平面斜坡 + 左上一块 nodata + 一条 60 px 宽的 nodata 纵沟。

    高程 = col * 0.01 + row * 0.02（0 ~ 21.4 m），是平面 ⇒ 双线性重采样后解析值不变，
    可以逐像素拿公式值对账。
    """
    gdal.UseExceptions()
    driver = gdal.GetDriverByName("GTiff")
    ds = driver.Create(str(path), width, height, 1, gdal.GDT_Float32)
    origin_x, origin_y = _lonlat_to_3857(118.0, 31.5)
    ds.SetGeoTransform((origin_x, pixel, 0.0, origin_y, 0.0, -pixel))
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(3857)
    ds.SetProjection(srs.ExportToWkt())
    rows, cols = np.mgrid[0:height, 0:width]
    elev = (cols * 0.01 + rows * 0.02).astype(np.float32)
    elev[80:180, 700:800] = -99999.0          # 左上一块 nodata
    elev[:, 380:440] = -99999.0               # 纵贯全图的 nodata 沟
    band = ds.GetRasterBand(1)
    band.WriteArray(elev)
    band.SetNoDataValue(-99999.0)
    ds.FlushCache()
    return origin_x, origin_y


def _slope_at(origin_x, origin_y, wx, wy):
    col = (wx - origin_x) / 30.0
    row = (origin_y - wy) / 30.0
    return col, row, col * 0.01 + row * 0.02


def test_build_terrain_pyramid_end_to_end(tmp_path):
    from PIL import Image

    # 本机同时装着 PostGIS，PROJ_LIB 默认指向它那份旧 proj.db；不先纠正，
    # ImportFromEPSG 会直接抛「DATABASE.LAYOUT.VERSION.MINOR」—— 这正是 run.py 启动时做的事。
    from util.gdal_env import configure_gdal_proj_env
    configure_gdal_proj_env()

    dem_path = tmp_path / "synthetic.tif"
    origin_x, origin_y = _make_synthetic_dem(dem_path)
    out_root = tmp_path / "tiles"

    meta = demTerrain.build_terrain_pyramid(
        dem_path, "tiff/TEST/2023/standard/x/synthetic.tif",
        out_root=out_root, min_zoom=10, max_zoom=12, apron_metres=2000.0,
    )

    assert meta["maxzoom"] == 12 and meta["minzoom"] == 10
    assert meta["tile_count"] > 0
    fp = meta["fingerprint"]
    assert demTerrain.is_valid_fingerprint(fp)
    assert (out_root / fp / "metadata.json").exists()

    # 有效高程 0 ~ 21.4 m；沟与角上的 nodata 都不能被当成 -99999 编进去
    assert -1.0 <= meta["min_elevation"] <= 0.5
    assert 20.5 <= meta["max_elevation"] <= 22.0

    # 外包框必须把源 DEM 完整包进去
    b = meta["bounds"]
    assert b["west"] < 118.0 < b["east"]
    assert b["south"] < 31.5 < b["north"]

    # 顶层每张瓦片里离 nodata 较远的点：解回来的高程 = 源斜面上同一位置的解析值
    zoom = 12
    res = demTerrain.tile_resolution(zoom)
    tile_metres = demTerrain.TILE_SIZE * res
    x0, x1, y0, y1 = demTerrain.tile_range(
        (origin_x, origin_y - 620 * 30.0, origin_x + 900 * 30.0, origin_y), zoom)
    checked = 0
    worst = 0.0
    for x in range(x0, x1 + 1):
        for y in range(y0, y1 + 1):
            p = demTerrain.tile_path(out_root, fp, zoom, x, y)
            if not p.exists():
                continue
            height = demTerrain.decode_terrarium(np.asarray(Image.open(p).convert("RGB")))
            for py, px in ((30, 30), (128, 128), (200, 220), (60, 200)):
                wx = x * tile_metres - _HALF_WORLD + (px + 0.5) * res
                wy = _HALF_WORLD - y * tile_metres - (py + 0.5) * res
                col, row, expected = _slope_at(origin_x, origin_y, wx, wy)
                if not (1 <= col < 898 and 1 <= row < 618):
                    continue
                # 只比对纯斜坡区：避开左上的 nodata 块与中间的 nodata 沟
                if 370 <= col < 450 or (col >= 690 and row <= 190):
                    continue
                worst = max(worst, abs(float(height[py, px]) - expected))
                checked += 1
    assert checked >= 4, checked
    assert worst < 0.05, worst

    # nodata 沟里不能是 -99999：沟中心的值只能是两侧地形的延续
    wx = origin_x + 410 * 30.0
    wy = origin_y - 300 * 30.0
    tx = math.floor((wx + _HALF_WORLD) / tile_metres)
    ty = math.floor((_HALF_WORLD - wy) / tile_metres)
    ditch = demTerrain.tile_path(out_root, fp, zoom, tx, ty)
    assert ditch.exists()
    height = demTerrain.decode_terrarium(np.asarray(Image.open(ditch).convert("RGB")))
    assert np.nanmin(height) > -5.0
    assert np.nanmax(height) < 25.0

    # 每一层都得有瓦片：缩小时地形不能「突然消失」
    for z in (10, 11, 12):
        assert any((out_root / fp / str(z)).iterdir())

    # 临时中间件必须清干净
    import config
    leftover = os.path.join(config.DIR_RESOURCE_CACHE, "terrain_tmp")
    assert not os.path.isdir(leftover) or os.listdir(leftover) == []


def test_build_terrain_pyramid_clamps_minzoom_to_grid_span(tmp_path):
    """小栅格 + 要求一路切到 z0：再往下整份 DEM 不足 1 像素，放不下任何东西。

    起因是真实事故：`tiff/Mzs/2023/standard/202304/202304.tif`（3328×3072 px、5 m ⇒ z15）
    要 z0–15，而当时按 GDAL 缩略图级数逐级取，取到不存在的层就拿到 None
    （`'NoneType' object has no attribute 'XSize'`）。现在层级下限由「顶层格网缩几次
    还剩 1 像素」直接算出来，不再依赖 GDAL 建了几级缩略图。
    """
    from util.gdal_env import configure_gdal_proj_env
    configure_gdal_proj_env()

    dem_path = tmp_path / "small.tif"
    _make_synthetic_dem(dem_path, width=400, height=300, pixel=5.0)  # 2 km 宽 ⇒ z15
    out_root = tmp_path / "tiles"

    meta = demTerrain.build_terrain_pyramid(
        dem_path, "tiff/TEST/2023/standard/x/small.tif",
        out_root=out_root, min_zoom=0, max_zoom=15, apron_metres=2000.0,
    )

    assert meta["maxzoom"] == 15
    # 顶层格网连裙边 2048 px ⇒ 缩 11 次只剩 1 px，再往下没有信息：要求 z0，实际落在 z4
    assert meta["minzoom"] == 15 - 11, meta["minzoom"]
    fp = meta["fingerprint"]
    levels = sorted(int(p.name) for p in (out_root / fp).iterdir() if p.is_dir())
    assert levels[0] == meta["minzoom"] and levels[-1] == 15
    # 每一层都有瓦片，而不是只有顶层
    for z in levels:
        assert any((out_root / fp / str(z)).iterdir()), z


def test_build_terrain_pyramid_levels_agree_vertically(tmp_path):
    """同一坐标在各层级解出来的高程必须一致（越粗的层越平滑，但绝不能整体错位）。

    真实事故：低层级瓦片曾经整体偏移最多 255 px —— 长江 DEM 最深处的点（源值 −56.9 m）
    在 z12 解出 −57.1 m、z11 解出 0.0 m，界面上就是「放大缩小地形长得不一样」。
    根因是缩略图的像素原点落在本层瓦片格网的分数位置上，`ReadAsArray` 只能按整数
    像素取窗口；现在每一层都重采样到本层格网，偏移恒为整数。
    """
    from PIL import Image
    from osgeo import gdal, osr

    from util.gdal_env import configure_gdal_proj_env
    configure_gdal_proj_env()

    width, height, pixel = 2000, 1200, 5.0
    origin_x, origin_y = 13390000.0, 3760000.0
    dem_path = tmp_path / "ramp.tif"
    driver = gdal.GetDriverByName("GTiff")
    dataset = driver.Create(str(dem_path), width, height, 1, gdal.GDT_Float32)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(3857)
    srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    dataset.SetProjection(srs.ExportToWkt())
    dataset.SetGeoTransform((origin_x - width * pixel / 2, pixel, 0,
                             origin_y + height * pixel / 2, 0, -pixel))
    # 纯斜坡：解析值已知，且平均重采样不会改变线性场的值（只看偏移，不看平滑）
    col = np.arange(width)[None, :].repeat(height, 0)
    row = np.arange(height)[:, None].repeat(width, 1)
    dataset.GetRasterBand(1).WriteArray((100.0 * col / width + 50.0 * row / height).astype(np.float32))
    dataset = None

    out_root = tmp_path / "tiles"
    meta = demTerrain.build_terrain_pyramid(dem_path, "tiff/T/ramp.tif", out_root=out_root, apron_metres=200.0)
    fp = meta["fingerprint"]

    # 取网格正中心那一点（远离 nodata 边界）
    lng = (origin_x / _HALF_WORLD) * 180.0
    lat = math.degrees(math.atan(math.sinh((origin_y / _HALF_WORLD) * math.pi)))

    def mercator(lng_value: float, lat_value: float, zoom: int) -> tuple[float, float]:
        n = 2 ** zoom
        x = (lng_value + 180.0) / 360.0 * n
        lat_rad = math.radians(lat_value)
        y = (1.0 - math.log(math.tan(lat_rad) + 1 / math.cos(lat_rad)) / math.pi) / 2.0 * n
        return x, y

    def ramp_at(merc_x: float, merc_y: float) -> float:
        """合成 DEM 的解析高程（线性场：按格心取值与按格平均等价）。"""
        col = (merc_x - (origin_x - width * pixel / 2)) / pixel
        row = ((origin_y + height * pixel / 2) - merc_y) / pixel
        return 100.0 * col / width + 50.0 * row / height

    checked = 0
    # 只比「一格明显小于整份 DEM」的层级：再粗下去一格就跨出 DEM 边界了，
    # 格内均值本来就不等于格心值（那是裙边在起作用），比了也不能说明对错。
    for zoom in range(max(meta["minzoom"], meta["maxzoom"] - 8), meta["maxzoom"] + 1):
        fx, fy = mercator(lng, lat, zoom)
        tile_x, tile_y = int(fx), int(fy)
        path = demTerrain.tile_path(out_root, fp, zoom, tile_x, tile_y)
        if not path.exists():
            continue
        px, py = int((fx - tile_x) * 256), int((fy - tile_y) * 256)
        heights = demTerrain.decode_terrarium(np.asarray(Image.open(path).convert("RGB")))
        value = float(heights[py, px])

        # 一格里装的是整格的均值 ⇒ 比对的基准是**该像素中心**的解析值，不是采样点的值。
        # 拿采样点当基准的话，最粗的层级一格就是几公里，光量化就有几十米，测不出错位。
        res = demTerrain.tile_resolution(zoom)
        cell_x = demTerrain.ORIGIN_X + (tile_x * 256 + px + 0.5) * res
        cell_y = demTerrain.ORIGIN_Y - (tile_y * 256 + py + 0.5) * res
        expected = ramp_at(cell_x, cell_y)

        assert abs(value - expected) <= 1.0, f"z{zoom}: {value:.2f}（该格中心应为 {expected:.2f}）"
        checked += 1

    assert checked >= 8, checked


def test_build_terrain_pyramid_missing_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        demTerrain.build_terrain_pyramid(tmp_path / "nope.tif", "k", out_root=tmp_path)


def test_build_terrain_pyramid_without_crs_raises(tmp_path):
    gdal.UseExceptions()
    dem_path = tmp_path / "nocrs.tif"
    driver = gdal.GetDriverByName("GTiff")
    ds = driver.Create(str(dem_path), 64, 64, 1, gdal.GDT_Float32)
    ds.GetRasterBand(1).WriteArray(np.ones((64, 64), dtype=np.float32))
    ds.FlushCache()
    with pytest.raises(ValueError):
        demTerrain.build_terrain_pyramid(dem_path, "k", out_root=tmp_path)

    # 失败路径同样不能留下临时中间件（Windows 下异常攥着句柄时最容易被漏掉）
    leftover = demTerrain.tmp_root()
    assert not leftover.is_dir() or os.listdir(leftover) == []


# ---------------------------------------------------------------- 状态服务（不连库的部分）

def test_terrain_status_row_to_info_hides_details_until_ready():
    from util import terrainService as svc

    pending = svc._row_to_info("k", {"terrain_status": "pending", "terrain_fp": "abc"})
    assert pending == {"tiff_key": "k", "status": "pending", "fingerprint": None, "minzoom": None,
                       "maxzoom": None, "bounds": None, "tile_count": None, "error": None,
                       "generated_at": None}

    unknown = svc._row_to_info("k", {"terrain_status": "weird"})
    assert unknown["status"] == "none"

    missing = svc._row_to_info("k", None)
    assert missing["status"] == "missing"

    failed = svc._row_to_info("k", {"terrain_status": "failed", "terrain_error": "boom"})
    assert failed["error"] == "boom" and failed["fingerprint"] is None


@pytest.mark.parametrize("fp,z,x,y", [
    ("../etc", 12, 1, 1),          # 指纹会被拼进磁盘路径
    ("921cd5752d5b178f0", 12, 1, 1),
    ("", 12, 1, 1),
    ("921cd5752d5b178f", 12, 4096, 1),  # 越界的列号
    ("921cd5752d5b178f", 12, 1, -1),
    ("921cd5752d5b178f", 25, 1, 1),
])
def test_tile_file_rejects_bad_input_before_touching_db(fp, z, x, y):
    """这些输入在查库之前就该被挡掉 —— 否则任何一种都会变成磁盘路径注入。"""
    from util import terrainService as svc

    assert svc.tile_file("tiff/x.tif", fp, z, x, y) is None


# ---------------------------------------------------------------- 行缓存（瓦片接口的瓶颈）

def test_row_cache_serves_repeat_lookups_and_expires(monkeypatch):
    """瓦片接口每张图都要校验一次指纹；不加缓存时每张都要新建 PG 连接（实测 90–100 ms）。

    这条锁两件事：同一个 key 在 TTL 内只查一次库；TTL 过了必须重新查 —— 否则重建完
    金字塔、换了指纹，瓦片接口还会拿着旧行去拼路径。
    """
    from util import terrainService as svc

    svc._row_cache.clear()
    calls: list[str] = []
    monkeypatch.setattr(svc, "_load_row", lambda key: calls.append(key) or {"terrain_fp": "x"})

    assert svc._load_row_cached("k") == {"terrain_fp": "x"}
    assert svc._load_row_cached("k") == {"terrain_fp": "x"}
    assert calls == ["k"], "TTL 内重复取用不应再查库"

    monkeypatch.setattr(svc, "_ROW_CACHE_SECONDS", -1.0)
    svc._load_row_cached("k")
    assert calls == ["k", "k"], "过期后必须重新查库"
    svc._row_cache.clear()


def test_update_invalidates_row_cache(monkeypatch):
    """写库必须立刻失效缓存 —— 30 s 的窗口足够前端把旧指纹的 404 全打一遍。"""
    import contextlib

    from util import terrainService as svc

    class _Cursor:
        def execute(self, *_args, **_kwargs):
            pass

        def fetchone(self):
            return ("ready",)

    @contextlib.contextmanager
    def _fake_cursor():
        yield (None, _Cursor())

    import util.db as db
    monkeypatch.setattr(db, "get_db_cursor", _fake_cursor)

    svc._row_cache["k"] = (0.0, {"terrain_fp": "old"})
    assert svc._update("k", {"terrain_fp": "new"}) == "ready"
    assert "k" not in svc._row_cache
