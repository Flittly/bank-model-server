import os
import sys
import json
import warnings

module_path = os.path.abspath(os.path.join(os.getcwd()))
if module_path not in sys.path:
    sys.path.append(module_path)

from osgeo import gdal, osr
import util
import model
import config
from math import sqrt
import copy
import numpy as np
from util.rustfs import resolve_tiff_path


def _debug_log(stage, payload):
    print(
        f"[riverbed-debug][sectionView][{stage}] "
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


def _geometry_debug_info(geometry):
    if not isinstance(geometry, dict):
        return {"python_type": type(geometry).__name__}
    crs = geometry.get("crs") or {}
    properties = crs.get("properties") or {}
    return {
        "type": geometry.get("type"),
        "crs": properties.get("name"),
        "has_features": bool(geometry.get("features")),
    }


def euclidean_distance(point1, point2):
    return sqrt((point1[0] - point2[0]) ** 2 + (point1[1] - point2[1]) ** 2)


# specific crs change process
def transform_coordinates(coords, source_srs, target_srs):
    # 创建坐标转换对象
    transform = osr.CoordinateTransformation(source_srs, target_srs)

    # 转换坐标
    transformed_coords = []
    for coord in coords:
        x, y = coord
        transformed_coords.append(transform.TransformPoint(x, y)[0:2])

    return transformed_coords


# change the origin crs of geojson data to 2437
def convert_geojson_to_2437(geojson_data, dataset):
    # 检查GeoJSON的坐标系
    srs = osr.SpatialReference()
    if (
        geojson_data.get("crs")
        and geojson_data["crs"].get("properties")
        and geojson_data["crs"]["properties"].get("name")
    ):
        srs.SetFromUserInput(geojson_data["crs"]["properties"]["name"])
    else:
        # 如果没有坐标系信息，假设是WGS84
        srs.ImportFromEPSG(4326)

    # 获取DEM的坐标系
    target_srs = osr.SpatialReference()
    target_srs.ImportFromWkt(dataset.GetProjection())

    # 要素与要素数据集的区别
    if not geojson_data.get("features"):
        features = []
        # 改为直接从顶层坐标尝试获取，或者从可能的嵌套中提取
        if "geometry" in geojson_data:
            coordinates = geojson_data["geometry"]["coordinates"]
        elif "coordinates" in geojson_data:
            coordinates = geojson_data["coordinates"]
        else:
            raise KeyError(
                "Neither 'geometry' nor 'coordinates' found in geojson_data."
            )
        # swarp coordinates
        for coordinate in coordinates:
            tmp = coordinate[0]
            coordinate[0] = coordinate[1]
            coordinate[1] = tmp
        feature = {
            "type": "Feature",
            "properties": {},
            "geometry": {"type": "LineString", "coordinates": coordinates},
        }
        features.append(feature)
        geojson_data["features"] = features

    features = []
    for feature in geojson_data["features"]:
        # 转换坐标
        transformed_coords = transform_coordinates(
            feature["geometry"]["coordinates"], srs, target_srs
        )

        # 更新Feature的坐标
        feature["geometry"]["coordinates"] = transformed_coords

        # 添加转换后的Feature到列表
        features.append(feature)

    # 更新GeoJSON数据
    geojson_data["features"] = features

    return geojson_data


# get each sample point's pixel coors and the ratio of line and colomn
def prj2imagexy(dataset, sample_points):
    trans = dataset.GetGeoTransform()
    originX = trans[0]
    originY = trans[3]
    pixelWidth = trans[1]
    pixelHeight = trans[5]

    imgCoors = []

    for sample_point in sample_points:
        row = (sample_point[1] - originY) / pixelHeight
        column = (sample_point[0] - originX) / pixelWidth
        imgCoors.append([int(row), int(column), row % 1, column % 1])
    return imgCoors


# get 4 pixel coors to be interpolated
def get_interpolated_pixel_index(image_coors, width, height):
    def clamp(_x, _max):
        return min(_max, max(_x, 0))

    interpolated_pixels = []
    for image_coor in image_coors:
        y = image_coor[0]
        x = image_coor[1]
        ym1 = int(clamp(y - 1, height - 1))
        xm1 = int(clamp(x - 1, width - 1))
        yp1 = int(clamp(y + 1, height - 1))
        xp1 = int(clamp(x + 1, width - 1))
        interpolated_pixel = [x, y, x, y, x, y, x, y]

        ratio_y = image_coor[2]
        ratio_x = image_coor[3]
        if ratio_y < 0.5 and ratio_x < 0.5:
            interpolated_pixel = [xm1, ym1, x, ym1, xm1, y, x, y]
        elif ratio_y < 0.5 and ratio_x > 0.5:
            interpolated_pixel = [x, ym1, xp1, ym1, x, y, xp1, y]
        elif ratio_y > 0.5 and ratio_x < 0.5:
            interpolated_pixel = [xm1, y, x, y, xm1, yp1, x, yp1]
        elif ratio_y > 0.5 and ratio_x > 0.5:
            interpolated_pixel = [x, y, xp1, y, x, yp1, xp1, yp1]
        interpolated_pixels.append(interpolated_pixel)
    return interpolated_pixels


# get the elevation of the 4 pixel coors
def get_elevation(interpolated_coors, dataset):
    Z = []
    band = dataset.GetRasterBand(1)
    dem_data = band.ReadAsArray(0, 0, dataset.RasterXSize, dataset.RasterYSize).astype(
        np.float32
    )
    dem_data = np.array(dem_data)
    for interpolated_coor in interpolated_coors:
        z1 = dem_data[interpolated_coor[1]][interpolated_coor[0]]
        z2 = dem_data[interpolated_coor[3]][interpolated_coor[2]]
        z3 = dem_data[interpolated_coor[5]][interpolated_coor[4]]
        z4 = dem_data[interpolated_coor[7]][interpolated_coor[6]]
        Z.append([z1, z2, z3, z4])
    return Z


# general linear interpolation
def linear_interpolation(A, B, p):
    return A + p * (B - A)


# bilinear interpolation to get the vec3 of sample_points
def bilinear_interpolation(image_coors, Z, sample_points):
    sample_points_vec3 = []
    for index, image_coor in enumerate(image_coors):
        ratio_y = image_coor[2]
        ratio_x = image_coor[3]
        z_values = Z[index]
        p1 = 0
        p2 = 0
        if ratio_y < 0.5 and ratio_x < 0.5:
            p1 = 0.5 + ratio_y
            p2 = 0.5 + ratio_x
        elif ratio_y < 0.5 and ratio_x > 0.5:
            p1 = 0.5 + ratio_y
            p2 = ratio_x - 0.5
        elif ratio_y > 0.5 and ratio_x < 0.5:
            p1 = ratio_y - 0.5
            p2 = 0.5 + ratio_x
        elif ratio_y > 0.5 and ratio_x > 0.5:
            p1 = ratio_y - 0.5
            p2 = ratio_x - 0.5
        z_left = linear_interpolation(z_values[0], z_values[2], p1)
        z_right = linear_interpolation(z_values[1], z_values[3], p1)
        z = linear_interpolation(z_left, z_right, p2)
        sample_points_vec3.append([sample_points[index][0], sample_points[index][1], z])
    return sample_points_vec3


def computeSaIndex(sample_points_vec3, step):
    Sa = []
    for i in range(len(sample_points_vec3) - 1):
        z1 = sample_points_vec3[i][2]
        z2 = sample_points_vec3[i + 1][2]
        Sa.append((z2 - z1) / step)
    return Sa


def computeSaIndex_v(sample_v_points):
    Sa = []
    for i in range(len(sample_v_points) - 1):
        d = util.calculate_distance(
            sample_v_points[i][0],
            sample_v_points[i][1],
            sample_v_points[i + 1][0],
            sample_v_points[i + 1][1],
        )
        # 避免除零错误：如果距离为 0，斜率设为 0 并发出警告
        if d == 0:
            warnings.warn(
                f"Distance between points {i} and {i + 1} is zero, slope set to 0. "
                f"Points: {sample_v_points[i]}, {sample_v_points[i + 1]}",
                RuntimeWarning,
            )
            Sa.append(0.0)
        else:
            Sa.append((sample_v_points[i + 1][2] - sample_v_points[i][2]) / d)
    return Sa


# merged main processes
def get_sample_points_vec3(dataset, section_geometry, num_points=100):
    # change the origin crs of geojson data to 2437
    section_geometry_proj = convert_geojson_to_2437(section_geometry, dataset)
    # sample points in the input section
    sample_points, step = util.divide_line_string(section_geometry_proj, num_points)
    # the pixel coors of sample_points
    image_coors = prj2imagexy(dataset, sample_points)
    # the 4 pixel coors to be interpolated
    width = dataset.RasterXSize
    height = dataset.RasterYSize
    interpolated_coors = get_interpolated_pixel_index(image_coors, width, height)
    # the elevation of the 4 pixel coors
    Z = get_elevation(interpolated_coors, dataset)
    # the vec3 of sample points
    sample_points_vec3 = bilinear_interpolation(image_coors, Z, sample_points)
    return sample_points_vec3, sample_points, step


def sample_by_points(dataset, points):
    # the pixel coors of sample_points
    image_coors = prj2imagexy(dataset, points)
    # the 4 pixel coors to be interpolated
    width = dataset.RasterXSize
    height = dataset.RasterYSize
    interpolated_coors = get_interpolated_pixel_index(image_coors, width, height)
    # the elevation of the 4 pixel coors
    Z = get_elevation(interpolated_coors, dataset)
    # the vec3 of sample points
    sample_points_vec3 = bilinear_interpolation(image_coors, Z, points)
    return sample_points_vec3


def section_view(dem_path, section_geometry, output_path):
    if not os.path.exists(output_path):
        os.mkdir(output_path)

    output_visualization_path = os.path.join(output_path, "section.txt")
    output_raw_path = os.path.join(output_path, "section.json")

    dataset = gdal.Open(dem_path)
    _debug_log(
        "opened_dataset",
        {
            "dem": _dataset_debug_info(dem_path),
            "section_geometry": _geometry_debug_info(section_geometry),
        },
    )

    geometry = copy.deepcopy(section_geometry)
    num_points = 100
    sample_points_vec3, scatters, step = get_sample_points_vec3(
        dataset, geometry, num_points
    )

    # Calculate the slope ratio of 2 adjacent sample points
    Sa = computeSaIndex(sample_points_vec3, step)

    # Get the deepest point
    deepest_index = sample_points_vec3.index(
        min(sample_points_vec3, key=lambda point: point[2])
    )

    # Calculate the points and their heigh that the erosion model focuses
    er_point_coords_verified, step_er_verified = util.divide_point_line(
        scatters, 23, dataset.GetProjection(), deepest_index
    )
    er_point_coords, step_er = util.divide_point_line(
        scatters, 23, dataset.GetProjection()
    )

    points_er_verified = sample_by_points(dataset, er_point_coords_verified)
    points_er = sample_by_points(dataset, er_point_coords)

    # Get the slope ratio at the vertical direction
    start_index = sample_points_vec3.index(
        max(sample_points_vec3[: deepest_index + 1], key=lambda point: point[2])
    )
    start_height = sample_points_vec3[start_index][2]
    end_height = sample_points_vec3[deepest_index][2]
    if start_height > 0:
        start_height = 0

    sample_v_points = []
    sample_v_height = [start_height]

    count = 0
    while -5 * count >= start_height:
        count += 1

    while -5 * count > end_height:
        sample_v_height.append(-5 * count)
        count += 1
    sample_v_height.append(end_height)

    # Preprocess
    # Avoid situation of section shape: "^"
    # Allowed situation of section shapes are "\" and "\^\"
    index = 0
    i = deepest_index
    while i >= 1:
        deep_i = sample_points_vec3[i][2]
        deep_im1 = sample_points_vec3[i - 1][2]

        if deep_im1 >= deep_i:
            i -= 1
        else:
            j = i - 2
            while j >= 0:
                if sample_points_vec3[j][2] > deep_i:
                    break
                j -= 1
            if j == -1:
                break
            else:
                i -= 1

    while i < deepest_index and index < len(sample_v_height):
        p1 = sample_points_vec3[i]
        p2 = sample_points_vec3[i + 1]
        h = sample_v_height[index]
        if h <= p1[2] and h >= p2[2]:
            ratio = (h - p1[2]) / (p2[2] - p1[2])
            x = linear_interpolation(p1[0], p2[0], ratio)
            y = linear_interpolation(p1[1], p2[1], ratio)
            sample_v_points.append([x, y, h])
            index += 1
        else:
            i += 1
    Sa_v = computeSaIndex_v(sample_v_points)
    if not Sa_v:
        slope_foot_index = 1
    else:
        slope_foot_index = Sa_v.index(min(Sa_v)) + 1

    # helper for converting numeric to native python type
    def to_native(obj):
        if isinstance(obj, (np.integer, np.floating)):
            return obj.item()
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return obj

    # Convert all numpy types to python native types in sample_points_vec3
    sample_points_vec3 = [
        [to_native(val) for val in point] for point in sample_points_vec3
    ]
    # Convert Sa list
    Sa = [to_native(val) for val in Sa]
    # Convert sample_v_points
    sample_v_points = [[to_native(val) for val in point] for point in sample_v_points]
    # Convert Sa_v list
    Sa_v = [to_native(val) for val in Sa_v]
    # Convert erosion points
    points_er_verified = [
        [to_native(val) for val in point] for point in points_er_verified
    ]
    points_er = [[to_native(val) for val in point] for point in points_er]

    raw_res = {
        "points": sample_points_vec3,
        "step": step,
        "Sa_h": Sa,
        "points_v": sample_v_points,
        "Sa_v": Sa_v,
        "deepest_index": deepest_index,
        "slope_foot_index": slope_foot_index,
        "points_er_verified": points_er_verified,
        "points_er": points_er,
        "step_er_verified": step_er_verified,
        "step_er": step_er,
    }
    with open(output_raw_path, "w", encoding="utf-8") as f:
        json.dump(raw_res, f, indent=4)

    with open(output_visualization_path, "w", encoding="utf-8") as f:
        for i in range(len(sample_points_vec3)):
            f.write(str(sample_points_vec3[i][2]) + "\n")
        f.write("\n")

        for i in range(len(Sa)):
            f.write(str(Sa[i]) + "\n")

    del dataset

    return {
        "visualization-file": "section.txt",
        "raw-file": "section.json",
        "interval": step,
    }


##########################################################################################


@model.model_status_controller_sync
def run_section_view_mcr(mcr: model.ModelCaseReference):
    # 从请求中解析正确的段和时间，构造真实的 tiff 路径
    segment = mcr.request_json.get("segment", "Mzs")
    time_point = str(mcr.request_json.get("current-timepoint", "202304")).replace(
        "-", ""
    )
    dem_id = mcr.request_json.get("dem-id")

    if dem_id and os.path.isabs(dem_id) and os.path.exists(dem_id):
        dem_path = dem_id
    else:
        # 使用 resolve_tiff_path 函数自动处理本地文件和rustfs下载
        dem_path = resolve_tiff_path(
            dem_id, segment=segment, timepoint=time_point, set_name="standard"
        )

    _debug_log(
        "resolved_input",
        {
            "case_id": mcr.id,
            "segment": segment,
            "time_point": time_point,
            "request_dem_id": dem_id,
            "resolved_dem_path": os.path.abspath(dem_path),
            "section_geometry": _geometry_debug_info(
                mcr.request_json.get("section-geometry")
            ),
            "dem": _dataset_debug_info(dem_path),
        },
    )

    section_geometry = mcr.request_json["section-geometry"]
    output_path = os.path.join(mcr.directory, "result")

    if section_geometry.get("type") == "FeatureCollection":
        raise TypeError("Feature Collection Is Not Allowed")

    result = section_view(dem_path, section_geometry, output_path)

    raw_json_path = os.path.join(output_path, result["raw-file"])
    slope_foot_index = None
    deepest_index = None
    sample_v_count = None
    sa_v_count = None
    if os.path.exists(raw_json_path):
        with open(raw_json_path, "r", encoding="utf-8") as file:
            raw_data = json.load(file)
        slope_foot_index = raw_data.get("slope_foot_index")
        deepest_index = raw_data.get("deepest_index")
        sample_v_count = len(raw_data.get("points_v") or [])
        sa_v_count = len(raw_data.get("Sa_v") or [])

    _debug_log(
        "result_summary",
        {
            "case_id": mcr.id,
            "raw_json_path": raw_json_path,
            "interval": result["interval"],
            "deepest_index": deepest_index,
            "slope_foot_index": slope_foot_index,
            "sample_v_count": sample_v_count,
            "sa_v_count": sa_v_count,
        },
    )

    return {
        "case-id": mcr.id,
        "visualization-txt": result["visualization-file"],
        "raw-json": result["raw-file"],
        "interval": result["interval"],
    }


##########################################################################################

NAME = "Section View"

CATEGORY = "Riverbed Evolution"

CATEGORY_ALIAS = "re"


def PARSING(
    self, request_json: dict, model_path: str, other_dependent_ids: list[str] = []
):
    region_mcr = model.ModelCaseReference.create(
        config.API_RE_SECTION_VIEW, request_json, NAME, model_path, other_dependent_ids
    )

    return [region_mcr]


def RESPONSING(
    self,
    core_mcr: model.ModelCaseReference,
    default_pre_mcrs: list[model.ModelCaseReference],
    other_pre_mcrs: list[model.ModelCaseReference],
):
    return core_mcr.make_response(
        {
            "case-id": "TEMPLATE",
            "raw-json": "NONE",
            "visualization-txt": "NONE",
            "interval": "NONE",
        }
    )


##########################################################################################

if __name__ == "__main__":
    v1 = sys.argv[-1]  # model case id

    section_view_mcr = model.ModelCaseReference.open_case(v1)

    # Run section view model case
    if section_view_mcr is not None:
        run_section_view_mcr(section_view_mcr)
