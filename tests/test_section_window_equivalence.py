"""DEM 窗口读取的等价性回归测试。

背景：``section_view`` 以前每次把整幅 DEM 读进内存（真实长江 DEM 是 33.4M
像元 / 133 MB，DEFLATE 压缩，解压 + 两遍 nodata 掩膜实测约 150 ms 一条断面），
但一条断面只覆盖十几乘二十几格。现在改成只读断面的**整数包络**：100 个采样点
取整后的行/列最小最大值，外扩 ``DEM_WINDOW_PAD`` 格，再夹回栅格。

这个改动只有一个验收标准：**结果必须与全图读取逐位相同**。所以下面的做法是
「同一套数学，只换数组范围」——把 ``_dem_window_box`` 临时替换成整幅栅格，
跑一遍作为基线，再跑窗口版本，逐点比对高程与有效性；端到端再比一次
``section.json`` / ``section.txt`` 的落地内容。

单纯"跑得通"没有意义，``Δz == 0`` 才是契约。
"""

import copy
import os

import numpy as np
import pytest
from osgeo import gdal, osr

from modelResource.riverbedEvolution import sectionView

# 临时 DEM：200x200、30 m、EPSG:2437，谷底在第 100 行（V 形）
ORIGIN_X = 490000.0
ORIGIN_Y = 3548000.0
PIXEL = 30.0
SIZE = 200
NODATA = -99999.0
VALLEY_ROW = 100


def _epsg2437_wkt() -> str:
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(2437)
    return srs.ExportToWkt()


@pytest.fixture(scope="module")
def dem_path(tmp_path_factory) -> str:
    path = str(tmp_path_factory.mktemp("window") / "dem2437.tif")
    dataset = gdal.GetDriverByName("GTiff").Create(
        path, SIZE, SIZE, 1, gdal.GDT_Float32
    )
    dataset.SetProjection(_epsg2437_wkt())
    dataset.SetGeoTransform((ORIGIN_X, PIXEL, 0.0, ORIGIN_Y, 0.0, -PIXEL))
    band = dataset.GetRasterBand(1)
    band.SetNoDataValue(NODATA)
    rows = np.arange(SIZE, dtype=np.float32).reshape(-1, 1)
    profile = -20.0 + ((rows - VALLEY_ROW) * 0.15) ** 2
    band.WriteArray(np.repeat(profile, SIZE, axis=1))
    dataset.FlushCache()
    dataset = None
    return path


def _to_lonlat(row: float, col: float):
    """像素行列 -> 经纬度，用来构造断面几何（GeoJSON 恒为 lon/lat）。"""
    x = ORIGIN_X + col * PIXEL
    y = ORIGIN_Y - row * PIXEL
    src = osr.SpatialReference()
    src.ImportFromEPSG(2437)
    dst = osr.SpatialReference()
    dst.ImportFromEPSG(4326)
    src.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    dst.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    lon, lat = osr.CoordinateTransformation(src, dst).TransformPoint(x, y)[0:2]
    return [lon, lat]


def _section(row0, col0, row1, col1):
    return {
        "type": "LineString",
        "coordinates": [_to_lonlat(row0, col0), _to_lonlat(row1, col1)],
    }


def _full_raster_box(coors, width, height, pad=sectionView.DEM_WINDOW_PAD):
    """基线：把窗口放宽到整幅栅格，复现改动前的全图读取。"""
    return 0, height, 0, width


# 覆盖：内部 / 四边贴边 / 角 / 跨边 / 完全在外
SECTIONS = {
    "interior": _section(80.3, 95.2, 120.7, 104.6),
    "top_edge": _section(0.4, 95.0, 12.3, 104.0),
    "bottom_edge": _section(187.3, 95.0, 199.6, 104.0),
    "left_edge": _section(95.0, 0.3, 105.0, 11.4),
    "right_edge": _section(95.0, 188.2, 105.0, 199.4),
    "top_left_corner": _section(0.5, 0.5, 9.0, 8.0),
    "straddle_right": _section(95.0, 195.0, 105.0, 206.0),
    "straddle_top": _section(-6.0, 95.0, 6.0, 104.0),
    "outside_rows": _section(300.0, 95.0, 340.0, 104.0),
    "outside_cols": _section(95.0, -40.0, 105.0, -20.0),
}


def _sample(dem_path, section, full):
    dataset = gdal.Open(dem_path)
    try:
        if full:
            original = sectionView._dem_window_box
            sectionView._dem_window_box = _full_raster_box
            try:
                return sectionView.get_sample_points_vec3(
                    dataset, copy.deepcopy(section), 100
                )
            finally:
                sectionView._dem_window_box = original
        return sectionView.get_sample_points_vec3(
            dataset, copy.deepcopy(section), 100
        )
    finally:
        dataset = None


def _z_array(points):
    return np.array([[p[0], p[1], p[2]] for p in points], dtype=float)


@pytest.mark.parametrize("name", sorted(SECTIONS))
def test_window_matches_full_raster(dem_path, name):
    """窗口读取与全图读取必须逐点 bit 一致（含 NaN 位置）。"""
    section = SECTIONS[name]
    base, base_scatters, base_step, base_valid = _sample(dem_path, section, full=True)
    win, win_scatters, win_step, win_valid = _sample(dem_path, section, full=False)

    assert win_step == base_step
    assert win_scatters == base_scatters
    assert win_valid == base_valid, f"{name}: 有效性判定漂移"

    a, b = _z_array(base), _z_array(win)
    assert a.shape == b.shape
    assert np.array_equal(a, b, equal_nan=True), (
        f"{name}: 高程出现差异，max |Δz| = "
        f"{np.nanmax(np.abs(a - b)) if a.size else 0}"
    )


@pytest.mark.parametrize("name", ["interior", "top_edge", "right_edge", "top_left_corner"])
def test_window_stays_inside_raster_and_is_small(dem_path, name):
    """窗口必须夹在栅格内、且远小于全图（这就是全部加速来源）。"""
    dataset = gdal.Open(dem_path)
    try:
        width, height = dataset.RasterXSize, dataset.RasterYSize
        _, coors, _, _ = sectionView._section_pipeline(
            dataset, copy.deepcopy(SECTIONS[name]), 100
        )
        box = sectionView._dem_window_box(coors, width, height)
        row0, row1, col0, col1 = box

        assert 0 <= row0 < row1 <= height, f"{name}: 行窗口越界 {box}"
        assert 0 <= col0 < col1 <= width, f"{name}: 列窗口越界 {box}"
        assert (row1 - row0) * (col1 - col0) < width * height / 4
    finally:
        dataset = None


@pytest.mark.parametrize("name", ["outside_rows", "outside_cols"])
def test_window_is_empty_when_section_is_off_raster(dem_path, name):
    """断面完全在栅格外时窗口为空，且所有点判无效（由覆盖率门控报错）。"""
    dataset = gdal.Open(dem_path)
    try:
        _, coors, _, _ = sectionView._section_pipeline(
            dataset, copy.deepcopy(SECTIONS[name]), 100
        )
        box = sectionView._dem_window_box(coors, dataset.RasterXSize, dataset.RasterYSize)
        assert box == (0, 0, 0, 0)

        window = sectionView._load_dem_window(dataset, box)
        assert window.shape == (0, 0)
        assert not any(window.is_valid(row, col) for row, col in [(0, 0), (99, 99)])
    finally:
        dataset = None


def test_offsets_map_full_image_coordinates(dem_path):
    """窗口内取到的值必须等于全图数组在**全图坐标**处的值。"""
    dataset = gdal.Open(dem_path)
    try:
        _, coors, interpolated, _ = sectionView._section_pipeline(
            dataset, copy.deepcopy(SECTIONS["interior"]), 100
        )
        box = sectionView._dem_window_box(coors, dataset.RasterXSize, dataset.RasterYSize)
        row0, row1, col0, col1 = box

        full = _full_raster_box(coors, dataset.RasterXSize, dataset.RasterYSize)
        full_window = sectionView._load_dem_window(dataset, full)
        window = sectionView._load_dem_window(dataset, box)

        assert (window.xoff, window.yoff) == (col0, row0)
        assert window.shape == (row1 - row0, col1 - col0)

        for coor in interpolated:
            for x, y in ((coor[0], coor[1]), (coor[2], coor[3]),
                         (coor[4], coor[5]), (coor[6], coor[7])):
                assert window.value(y, x) == full_window.value(y, x)
                assert window.is_valid(y, x) == full_window.is_valid(y, x)

        if not sectionView._window_covers(window, interpolated,
                                          dataset.RasterXSize, dataset.RasterYSize):
            raise AssertionError("断面包络竟然没覆盖住自己的采样角点")
    finally:
        dataset = None


def test_section_view_output_identical(dem_path, tmp_path):
    """端到端：section.json / section.txt 必须与全图读取逐字节相同。"""
    section = SECTIONS["interior"]

    ref_dir = tmp_path / "reference"
    win_dir = tmp_path / "window"
    ref_dir.mkdir()
    win_dir.mkdir()

    original = sectionView._dem_window_box
    sectionView._dem_window_box = _full_raster_box
    try:
        sectionView.section_view(dem_path, copy.deepcopy(section), str(ref_dir))
    finally:
        sectionView._dem_window_box = original
    sectionView.section_view(dem_path, copy.deepcopy(section), str(win_dir))

    for name in ("section.json", "section.txt"):
        a = (ref_dir / name).read_text(encoding="utf-8")
        b = (win_dir / name).read_text(encoding="utf-8")
        assert a == b, f"{name} 与全图读取不一致"
        assert os.path.getsize(win_dir / name) == os.path.getsize(ref_dir / name)
