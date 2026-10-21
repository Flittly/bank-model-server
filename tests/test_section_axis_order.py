"""断面投影轴序的回归测试。

背景：DEM 是 ``EPSG:2437``（Beijing 1954 / 3-degree Gauss-Kruger CM 120E），
它的权威轴序是 ``(Northing, Easting)``；GeoJSON 的坐标恒为 ``(lon, lat)``。
GDAL 3 起 ``ImportFromEPSG`` / ``ImportFromWkt`` 默认遵循权威轴序，于是
``TransformPoint`` 期望 ``(lat, lon)`` 输入、返回 ``(northing, easting)``。

历史实现只在**入口**把 x/y 对调、**出口**忘了换回来，导致
``prj2imagexy`` 拿 northing 当列号（约 350 万 / 30m ≈ 10 万列，而栅格只有
1 万列宽），越界坐标被 clamp 到 DEM 右下角，那里全是 nodata，最终报出
``DEM_COVERAGE_INSUFFICIENT: 0/100 usable sample points``。

这里用一个临时 GeoTIFF 锁住「东西向必须是 easting、南北向必须是 northing」
这一契约，不依赖 133MB 的真实 DEM 文件。
"""

import copy

import numpy as np
import pytest
from osgeo import gdal, osr

from modelResource.riverbedEvolution import sectionContrast, sectionView

# 临时 DEM：覆盖 (119.95, 32.00) 一带，30m 分辨率，400x400（约 640KB）
# 该点在 EPSG:2437 下约为 (495210, 3541893)
ORIGIN_X = 490000.0
ORIGIN_Y = 3548000.0
PIXEL = 30.0
SIZE = 400
NODATA = -99999.0

# 断面：南北走向，落在临时 DEM 内
SECTION = {
    "type": "LineString",
    "coordinates": [[119.95, 32.00], [119.95, 31.99]],
}


def _epsg2437_wkt() -> str:
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(2437)
    return srs.ExportToWkt()


@pytest.fixture(scope="module")
def dem_path(tmp_path_factory) -> str:
    """写出一个覆盖断面的小 GeoTIFF，高程沿南北向呈 V 形河谷。

    断面是南北走向的，所以地形必须随**行号**变化，否则整条断面等高，
    会在下游的 ``SECTION_PROFILE_DEGENERATE`` 检查处失败（那里要求最深处
    不在首点）。谷底放在第 221 行，正好落在断面覆盖的 203~240 行中间。
    """
    path = str(tmp_path_factory.mktemp("axis") / "dem2437.tif")
    dataset = gdal.GetDriverByName("GTiff").Create(
        path, SIZE, SIZE, 1, gdal.GDT_Float32
    )
    dataset.SetProjection(_epsg2437_wkt())
    dataset.SetGeoTransform((ORIGIN_X, PIXEL, 0.0, ORIGIN_Y, 0.0, -PIXEL))
    band = dataset.GetRasterBand(1)
    band.SetNoDataValue(NODATA)
    rows = np.arange(SIZE, dtype=np.float32).reshape(-1, 1)
    profile = -20.0 + ((rows - 221.0) * 0.15) ** 2
    band.WriteArray(np.repeat(profile, SIZE, axis=1))
    dataset.FlushCache()
    dataset = None
    return path


def test_axis_order_is_easting_northing(dem_path):
    """120°E 必须落在 easting≈500000，而不是被当成北向坐标。"""
    dataset = gdal.Open(dem_path)
    projected = sectionView.convert_geojson_to_2437(copy.deepcopy(SECTION), dataset)
    coords = projected["features"][0]["geometry"]["coordinates"]

    easting, northing = coords[0]
    assert 490000 <= easting <= 510000, f"easting 异常：{easting}（疑似与 northing 互换）"
    assert 3400000 <= northing <= 3600000, f"northing 异常：{northing}"

    # 断面是南北向的：投影后 x 几乎不变、y 变化上千米
    assert abs(coords[1][0] - coords[0][0]) < 100
    assert abs(coords[1][1] - coords[0][1]) > 1000


def test_section_contrast_uses_same_axis_order(dem_path):
    """两份实现曾是同一个 bug 的拷贝，必须同时正确。"""
    dataset = gdal.Open(dem_path)
    # sectionContrast 只识别 Feature 结构，sectionView 两种都支持
    feature = {"geometry": copy.deepcopy(SECTION)}
    b = sectionContrast.convert_geojson_to_2437(copy.deepcopy(feature), dataset)
    a = sectionView.convert_geojson_to_2437(copy.deepcopy(feature), dataset)
    assert (
        a["features"][0]["geometry"]["coordinates"]
        == b["features"][0]["geometry"]["coordinates"]
    )
    easting, northing = b["features"][0]["geometry"]["coordinates"][0]
    assert 490000 <= easting <= 510000, f"easting 异常：{easting}"
    assert 3400000 <= northing <= 3600000, f"northing 异常：{northing}"


def test_fully_covered_section_has_no_nodata(dem_path):
    """栅格全覆盖且全有效时，100 个采样点必须全部有效。"""
    dataset = gdal.Open(dem_path)
    points, scatters, step, valid = sectionView.get_sample_points_vec3(
        dataset, copy.deepcopy(SECTION), 100
    )

    assert len(valid) == 100
    assert all(valid), f"出现 {100 - sum(valid)} 个无效点，说明采样落到了栅格之外"
    assert 0 < step < 200
    assert len(points) == len(scatters) == 100


def test_section_view_accepts_covered_section(dem_path, tmp_path):
    """覆盖良好时 end-to-end 不应抛 DemCoverageError。"""
    result = sectionView.section_view(dem_path, copy.deepcopy(SECTION), str(tmp_path))

    assert result["raw-file"] == "section.json"
    assert (tmp_path / "section.json").exists()
    assert (tmp_path / "section.txt").exists()


def test_section_view_still_rejects_uncovered_section(dem_path, tmp_path):
    """门控不能被这次修复削弱：真正落在 DEM 外的断面仍要报错。"""
    far_away = {
        "type": "LineString",
        "coordinates": [[121.50, 33.00], [121.50, 32.99]],
    }

    with pytest.raises(sectionView.DemCoverageError) as excinfo:
        sectionView.section_view(dem_path, far_away, str(tmp_path))

    assert "DEM_COVERAGE_INSUFFICIENT" in str(excinfo.value)
    assert "0/100 usable sample points" in str(excinfo.value)
