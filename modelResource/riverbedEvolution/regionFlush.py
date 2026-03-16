import os
import sys

module_path = os.path.abspath(os.path.join(os.getcwd()))
if module_path not in sys.path:
    sys.path.append(module_path)

import math
import json
import model
import config
import numpy as np
from PIL import Image
from osgeo import ogr
from osgeo import osr
from osgeo import gdal

from model import ModelCaseReference as MCR
from util.rustfs import resolve_tiff_path, resolve_resource_path


def _debug_log(stage, payload):
    print(
        f"[riverbed-debug][regionFlush][{stage}] "
        f"{json.dumps(payload, ensure_ascii=False, default=str)}",
        flush=True,
    )


def _dataset_debug_info(path):
    base, _ = os.path.splitext(path)
    info = {
        "path": os.path.abspath(path),
        "exists": os.path.exists(path),
        "sidecars": {
            "tfw": os.path.exists(f"{base}.tfw"),
            "aux_xml": os.path.exists(f"{path}.aux.xml"),
            "ovr": os.path.exists(f"{path}.ovr"),
        },
    }
    dataset = gdal.Open(path)
    if dataset is None:
        info["open"] = False
        return info

    band = dataset.GetRasterBand(1)
    info.update(
        {
            "open": True,
            "projection": dataset.GetProjection(),
            "geo_transform": list(dataset.GetGeoTransform()),
            "raster_x_size": dataset.RasterXSize,
            "raster_y_size": dataset.RasterYSize,
            "nodata": band.GetNoDataValue() if band is not None else None,
        }
    )
    dataset = None
    return info


def get_raster_projection(input: str) -> str:
    ds = gdal.Open(input)
    prj = ds.GetProjection()
    ds = None

    return prj


def lonlat2geo(prj, lon, lat):
    prosrs = osr.SpatialReference()
    prosrs.ImportFromWkt(prj)
    geosrs = prosrs.CloneGeogCS()
    ct = osr.CoordinateTransformation(geosrs, prosrs)
    coords = ct.TransformPoint(lat, lon)
    return coords[:2]


def geo2lonlat(prj, x, y):
    prosrs = osr.SpatialReference()
    prosrs.ImportFromWkt(prj)
    geosrs = prosrs.CloneGeogCS()
    ct = osr.CoordinateTransformation(prosrs, geosrs)
    coords = ct.TransformPoint(x, y)
    return coords[:2]


def create_geometry(coords: list[list[int]]):
    ring = ogr.Geometry(ogr.wkbLinearRing)
    for coord in coords:
        ring.AddPoint(coord[0], coord[1])
    poly = ogr.Geometry(ogr.wkbPolygon)
    poly.AddGeometry(ring)

    return poly


def create_mask(poly, prj: str) -> None:
    driver = ogr.GetDriverByName("ESRI Shapefile")
    ds = driver.CreateDataSource("temporary_mask.shp")

    srs = osr.SpatialReference()
    srs.ImportFromWkt(prj)

    layer = ds.CreateLayer("polygon", srs, ogr.wkbPolygon)
    defn = layer.GetLayerDefn()
    feature = ogr.Feature(defn)
    geom = ogr.CreateGeometryFromWkt(poly.ExportToWkt())
    feature.SetGeometry(geom)
    layer.CreateFeature(feature)

    ds = layer = feature = geom = driver = None


def clip_raster(input: str, output_tif: str) -> None:
    gdal.Warp(
        output_tif,
        input,
        format="GTiff",
        cutlineDSName="temporary_mask.shp",
        cropToCutline=True,
    )


def clamp(a, _min, _max):
    return max(min(a, _max), _min)


def clamp_vec3(array, min, max):
    return [
        clamp(array[0], min, max),
        clamp(array[1], min, max),
        clamp(array[2], min, max),
    ]


def jet_color_map(value):
    value = int(value)

    r, g, b = 0, 0, 0

    if value <= 31:
        g, b, r = clamp_vec3([128 + 4 * value, 0, 0], 0, 255)
    elif value <= 95:
        g, b, r = clamp_vec3([255, 4 * (value - 32), 0], 0, 255)
    elif value <= 159:
        g, b, r = clamp_vec3([255 - 4 * (value - 96), 255, 0], 0, 255)
    elif value <= 223:
        g, b, r = clamp_vec3([0, 255, 4 * (value - 160)], 0, 255)
    else:
        g, b, r = clamp_vec3(
            [0, 255 - 4 * (value - 224), 128 + 4 * (value - 224)], 0, 255
        )
    return r, g, b, 255  # 确保颜色值是RGBA格式


def get_tif_attributes(file_path):
    # 打开 TIFF 文件
    dataset = gdal.Open(file_path)

    if not dataset:
        raise FileNotFoundError(f"Cannot open file {file_path}")

    # 获取第一个波段
    band = dataset.GetRasterBand(1)

    # 获取波段的最小值和最大值
    min_value, max_value = band.ComputeRasterMinMax(True)

    nodata = band.GetNoDataValue()

    return min_value, max_value, nodata


def color_mapping(value, min, max, noData):
    r: int = 0
    g: int = 0
    b: int = 0
    a: int = 255

    if int(value) == noData or math.isnan(value):
        a = 0
        return r, g, b, a

    normal_value = (value - min) / (max - min)
    r, g, b, a = jet_color_map(normal_value * 255.0)

    return r, g, b, a


def tif_to_colorPng(input_tif, output_png):
    with Image.open(input_tif) as image:
        image_array = np.array(image).astype(np.float32)
        width, height = image.size

    min, max, nodata = get_tif_attributes(input_tif)
    rgba_array = np.zeros((height, width, 4), dtype=np.uint8)

    for y in range(height):
        for x in range(width):
            pixel = image_array[y, x]
            r, g, b, a = color_mapping(pixel, min, max, nodata)
            # rgba_array[y, x] = np.array([r, g, b, a]).astype(np.uint8)
            rgba_array[y, x] = [r, g, b, a]

    output_image = Image.fromarray(rgba_array, "RGBA")
    output_image.save(output_png)


def get_bound_coords(output_tif: str) -> dict:
    list_coords = {}
    ds: gdal.Dataset = gdal.Open(output_tif)
    info = ds.GetGeoTransform()
    width = ds.RasterXSize
    height = ds.RasterYSize
    offset_width = width * info[1]
    offset_height = height * info[5]
    ul_coord = [info[0], info[3]]
    lr_coord = [info[0] + offset_width, info[3] + offset_height]
    list_coords = {
        "ul": [ul_coord[0], ul_coord[1]],
        "lr": [lr_coord[0], lr_coord[1]],
    }

    return list_coords


def delete_temporary_files() -> None:
    driver = ogr.GetDriverByName("ESRI Shapefile")
    driver.DeleteDataSource("temporary_mask.shp")


# 全江冲淤
def global_flush_func(bench_path, ref_path, output_path, default_value=-9999.0):
    output_tif = os.path.join(output_path, "flush.tif")
    output_png = os.path.join(output_path, "flush.png")
    output_coords = os.path.join(output_path, "flush.json")

    # 打开第一个DEM文件
    dem1_dataset = gdal.Open(bench_path)
    if dem1_dataset is None:
        raise Exception(f"无法打开文件 {bench_path}")

    # 打开第二个DEM文件
    dem2_dataset = gdal.Open(ref_path)
    if dem2_dataset is None:
        raise Exception(f"无法打开文件 {ref_path}")

    # 获取DEM的几何变换参数
    transform1 = dem1_dataset.GetGeoTransform()
    transform2 = dem2_dataset.GetGeoTransform()

    # 确定DEM的交集区域
    min_x = max(transform1[0], transform2[0])
    max_x = min(
        transform1[0] + transform1[1] * dem1_dataset.RasterXSize,
        transform2[0] + transform2[1] * dem2_dataset.RasterXSize,
    )
    min_y = max(
        transform1[3] + transform1[5] * dem1_dataset.RasterYSize,
        transform2[3] + transform2[5] * dem2_dataset.RasterYSize,
    )
    max_y = min(transform1[3], transform2[3])

    # 计算交集区域的像素尺寸
    width = int((max_x - min_x) / min(transform1[1], transform2[1]))
    height = abs(int((max_y - min_y) / min(transform1[5], transform2[5])))

    # 创建输出数据集
    driver = gdal.GetDriverByName("GTiff")
    output_dataset = driver.Create(output_tif, width, height, 1, gdal.GDT_Float32)
    if not output_dataset:
        raise Exception("无法创建输出数据集")

    output_dataset.SetGeoTransform(
        [
            min_x,
            min(transform1[1], transform2[1]),
            0,
            max_y,
            0,
            min(transform1[5], transform2[5]),
        ]
    )

    srs = osr.SpatialReference()
    srs.ImportFromWkt(dem1_dataset.GetProjection())
    output_dataset.SetProjection(srs.ExportToWkt())

    # 获取DEM的波段数据
    band1 = dem1_dataset.GetRasterBand(1)
    band2 = dem2_dataset.GetRasterBand(1)

    # 读取NoData值
    nodata1 = band1.GetNoDataValue()
    nodata2 = band2.GetNoDataValue()

    # 读取和处理数据
    data1 = band1.ReadAsArray()
    data2 = band2.ReadAsArray()

    # 找到交集区域的索引范围
    start_col1 = int((min_x - transform1[0]) / transform1[1])
    end_col1 = int((max_x - transform1[0]) / transform1[1])
    start_row1 = int((max_y - transform1[3]) / transform1[5])
    end_row1 = int((min_y - transform1[3]) / transform1[5])

    start_col2 = int((min_x - transform2[0]) / transform2[1])
    end_col2 = int((max_x - transform2[0]) / transform2[1])
    start_row2 = int((max_y - transform2[3]) / transform2[5])
    end_row2 = int((min_y - transform2[3]) / transform2[5])

    # 相减并处理NoData值
    array = np.full((height, width), fill_value=default_value)
    for row in range(start_row1, end_row1):
        for col in range(start_col1, end_col2):
            # Use relative coordinates (r, c) for the 'array' which has size (height, width)
            # row and col are global coordinates in the source raster
            r = row - start_row1
            c = col - start_col1

            if r >= height or c >= width:
                continue

            val1 = (
                data1[row][col] if row < end_row1 and col < end_col1 else default_value
            )
            val2 = (
                data2[row - (start_row1 - start_row2)][col - (start_col1 - start_col2)]
                if row >= start_row2
                and row < end_row2
                and col >= start_col2
                and col < end_col2
                else default_value
            )

            if (
                val1 == nodata1
                or val2 == nodata2
                or val1 == default_value
                or val2 == default_value
            ):
                array[r, c] = default_value
            else:
                result = val1 - val2
                array[r, c] = result
    output_dataset.GetRasterBand(1).WriteArray(array)

    # 设置输出数据集的NoData值
    output_band = output_dataset.GetRasterBand(1)
    output_band.SetNoDataValue(default_value)

    # 保存并关闭数据集
    output_dataset.FlushCache()
    dem1_dataset = None
    dem2_dataset = None
    output_dataset = None

    tif_to_colorPng(output_tif, output_png)

    prj = get_raster_projection(output_tif)

    bound_coords_pro = get_bound_coords(output_tif)
    bound_coords_geo = {}

    for key in bound_coords_pro:
        coord = bound_coords_pro[key]
        a = geo2lonlat(prj, coord[0], coord[1])
        bound_coords_geo[key] = a[::-1]

    bound_coords_geo["ur"] = [bound_coords_geo["lr"][0], bound_coords_geo["ul"][1]]
    bound_coords_geo["ll"] = [bound_coords_geo["ul"][0], bound_coords_geo["lr"][1]]

    # 写文件
    with open(output_coords, "w") as file_obj:
        json.dump(bound_coords_geo, file_obj)

    _debug_log(
        "output_summary",
        {
            "bench": _dataset_debug_info(bench_path),
            "ref": _dataset_debug_info(ref_path),
            "output_tif": _dataset_debug_info(output_tif),
            "output_png": os.path.abspath(output_png),
            "output_coords": os.path.abspath(output_coords),
        },
    )


# 区域冲淤
def region_flush_func(flush_all_path: str, region_geometry: dict, output_path: str):
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    global_result_directory = os.path.join(flush_all_path, "flush.tif")
    output_coords = os.path.join(output_path, "flush.json")
    output_tif = os.path.join(output_path, "flush.tif")
    output_png = os.path.join(output_path, "flush.png")
    coords_pro = []

    prj = get_raster_projection(global_result_directory)

    # 初始化一个列表来存储所有多边形的坐标
    polygons_coordinates = []

    # 遍历features
    for feature in region_geometry["features"]:
        # 检查geometry类型是否为'Polygon'
        if feature["geometry"]["type"] == "Polygon":
            # 提取坐标
            coordinates = feature["geometry"]["coordinates"][0]  # 取外环的坐标
            polygons_coordinates.append(coordinates)

    for i in range(0, len(polygons_coordinates[0])):
        a = lonlat2geo(
            prj, polygons_coordinates[0][i][0], polygons_coordinates[0][i][1]
        )
        coords_pro.append(a)
    poly = create_geometry(coords_pro)  # 根据裁剪区域点创建面要素
    create_mask(poly, prj)  # 根据面要素创建掩膜
    clip_raster(global_result_directory, output_tif)  # 使用掩膜对全江冲淤结果进行裁剪
    tif_to_colorPng(output_tif, output_png)

    bound_coords_pro = get_bound_coords(output_tif)
    bound_coords_geo = {}
    # 转坐标
    for key in bound_coords_pro:
        coord = bound_coords_pro[key]
        a = geo2lonlat(prj, coord[0], coord[1])
        bound_coords_geo[key] = a[::-1]

    bound_coords_geo["ur"] = [bound_coords_geo["lr"][0], bound_coords_geo["ul"][1]]
    bound_coords_geo["ll"] = [bound_coords_geo["ul"][0], bound_coords_geo["lr"][1]]

    # 写文件
    with open(output_coords, "w") as file_obj:
        json.dump(bound_coords_geo, file_obj)

    # 删除临时文件
    delete_temporary_files()


@model.model_status_controller_sync
def run_global_flush_mcr(mcr: MCR):
    # 动态解析拼接 bench 文件和 ref 文件的实际路径
    segment = mcr.request_json.get("segment", "Mzs")

    # 解析并拼接 bench-id
    bench_time = str(
        mcr.request_json.get(
            "bench-timepoint", mcr.request_json.get("bench-id", "202304")
        )
    ).replace("-", "")
    bench_path = resolve_tiff_path(
        mcr.request_json.get("bench-id"),
        segment=segment,
        timepoint=bench_time,
        set_name="standard",
    )

    ref_time = str(
        mcr.request_json.get("ref-timepoint", mcr.request_json.get("ref-id", "202304"))
    ).replace("-", "")
    ref_path = resolve_tiff_path(
        mcr.request_json.get("ref-id"),
        segment=segment,
        timepoint=ref_time,
        set_name="standard",
    )

    _debug_log(
        "resolved_input",
        {
            "case_id": mcr.id,
            "segment": segment,
            "bench_time": bench_time,
            "ref_time": ref_time,
            "request_bench_id": mcr.request_json.get("bench-id"),
            "request_ref_id": mcr.request_json.get("ref-id"),
            "bench": _dataset_debug_info(bench_path),
            "ref": _dataset_debug_info(ref_path),
        },
    )

    output_path = os.path.join(mcr.directory, "result")

    global_flush_func(bench_path, ref_path, output_path)

    return {
        "case-id": mcr.id,
        "raw-tif": "flush.tif",
        "extent-json": "flush.json",
        "visualization-png": "flush.png",
    }


##########################################################################################


@model.model_status_controller_sync
def run_region_flush_mcr(mcr: MCR):
    if global_mcr is None:
        raise RuntimeError("Global flush case is not available")
    global_output_path = os.path.join(global_mcr.directory, "result")
    region_geometry = mcr.request_json["region-geometry"]
    output_path = os.path.join(mcr.directory, "result")

    _debug_log(
        "region_input",
        {
            "case_id": mcr.id,
            "global_flush_tif": _dataset_debug_info(
                os.path.join(global_output_path, "flush.tif")
            ),
            "region_geometry_type": region_geometry.get("type")
            if isinstance(region_geometry, dict)
            else type(region_geometry).__name__,
        },
    )

    region_flush_func(global_output_path, region_geometry, output_path)

    return {
        "case-id": mcr.id,
        "raw-tif": "flush.tif",
        "extent-json": "flush.json",
        "visualization-png": "flush.png",
    }


##########################################################################################

NAME = "Region Flush"

CATEGORY = "Riverbed Evolution"

CATEGORY_ALIAS = "re"


def PARSING(
    self, request_json: dict, model_path: str, other_dependent_ids: list[str] = []
):
    if request_json["region-geometry"] == "NONE":
        mcr = MCR(
            config.API_RE_REGION_FLUSH,
            request_json,
            NAME,
            model_path,
            other_dependent_ids,
        )
        return [mcr]
    else:
        global_json = {
            "bench-id": request_json["bench-id"],
            "ref-id": request_json["ref-id"],
            "region-geometry": "NONE",
        }
        global_mcr = model.launcher.fetch_model_from_API(
            config.API_RE_REGION_FLUSH
        ).run(global_json, other_dependent_ids)
        region_mcr = MCR(
            config.API_RE_REGION_FLUSH,
            request_json,
            NAME,
            model_path,
            other_dependent_ids + [global_mcr.id],
        )

        return [global_mcr, region_mcr]


def RESPONSING(
    self,
    core_mcr: model.ModelCaseReference,
    default_pre_mcrs: list[model.ModelCaseReference],
    other_pre_mcrs: list[model.ModelCaseReference],
):
    return core_mcr.make_response(
        {"case-id": "TEMPLATE", "raw-tif": "NONE", "extent-json": "NONE"}
    )


##########################################################################################

if __name__ == "__main__":
    v1 = sys.argv[1]  # global model case reference id
    v2 = sys.argv[2] if len(sys.argv) == 3 else None  # region model case reference id

    global_mcr = MCR.open_case(v1)

    if v2 is None:
        # Run model case (Global Flush)
        if global_mcr is not None:
            run_global_flush_mcr(global_mcr)

    else:
        # Run model cased (Region Flush)
        region_mcr = MCR.open_case(v2)
        if region_mcr is not None:
            run_region_flush_mcr(region_mcr)
