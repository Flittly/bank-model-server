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


########################################################################
# Nodata guard
#
# The bilinear sampler below has no nodata protection: `get_elevation` hands the
# raw value of the 4 surrounding cells to `bilinear_interpolation`, so a single
# nodata corner (=-9999 here) drags the interpolated elevation down to order
# -1e4. `deepest_index = argmin(z)` is then hijacked by that fake depression and
# lands on index 0, which silently empties `points_v`/`Sa_v` and only surfaces
# much later as an unrelated ValueError/IndexError inside the indicators.
#
# The guard below keeps the computation on the longest *continuously valid*
# stretch of the section and turns the unavoidable failures into an explicit,
# self-describing error instead of a downstream crash.
########################################################################

# Minimum physical length (m) of the longest continuously valid stretch a
# section must have to still be worth evaluating. Override with the
# SECTION_MIN_VALID_LENGTH_M environment variable.
MIN_VALID_LENGTH_M = float(os.environ.get("SECTION_MIN_VALID_LENGTH_M", "100"))
MIN_VALID_POINTS = 2


class DemCoverageError(RuntimeError):
    """Raised when a section cannot be evaluated because too much of it falls on
    DEM nodata cells.

    ``code`` gives the caller a stable reason string, ``detail`` carries the
    measured coverage so the failure can be triaged without re-running.
    """

    def __init__(self, code, message, detail=None):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.detail = detail or {}


def _load_dem_and_valid_mask(dataset):
    """Read band 1 once and return ``(dem, valid)``.

    ``valid`` is a boolean array where ``False`` flags nodata / non-finite
    cells. The nodata value is taken from the band metadata first; when a raster
    declares none, its mask band is consulted so masked rasters are handled too.
    """
    band = dataset.GetRasterBand(1)
    width = dataset.RasterXSize
    height = dataset.RasterYSize
    dem = band.ReadAsArray(0, 0, width, height).astype(np.float32)

    nodata = band.GetNoDataValue()
    has_nodata = nodata is not None and not (
        isinstance(nodata, float) and np.isnan(nodata)
    )
    if has_nodata:
        valid = dem != np.float32(nodata)
    else:
        valid = np.ones(dem.shape, dtype=bool)
        try:
            if band.GetMaskFlags() != gdal.GMF_ALL_VALID:
                mask = band.GetMaskBand().ReadAsArray(0, 0, width, height)
                if mask is not None:
                    valid = np.asarray(mask) > 0
        except Exception:  # pragma: no cover - mask support varies by driver
            pass

    # Non-finite values are never usable regardless of what the metadata says.
    valid &= np.isfinite(dem)
    return dem, valid


def _pixel_quad_validity(interpolated_coors, valid_pixels):
    """A sample point is usable only when *all four* bilinear corners are valid.

    ``bilinear_interpolation`` blends the 4 corner values, so one nodata corner
    is enough to corrupt the result; requiring all four keeps the guard aligned
    with how the elevation is actually produced.

    Coordinates outside the raster count as invalid: ``prj2imagexy`` only clamps
    the +/-1 neighbours, so a sample point beyond the DEM footprint would index
    past the array instead of raising a data error.
    """
    rows, cols = valid_pixels.shape

    def valid_at(row, col):
        return 0 <= row < rows and 0 <= col < cols and bool(valid_pixels[row][col])

    flags = []
    for coor in interpolated_coors:
        flags.append(
            valid_at(coor[1], coor[0])
            and valid_at(coor[3], coor[2])
            and valid_at(coor[5], coor[4])
            and valid_at(coor[7], coor[6])
        )
    return flags


def _longest_valid_run(flags):
    """Inclusive ``(start, end)`` of the longest contiguous run of True.

    Returns ``(0, -1)`` when there is no valid sample at all.
    """
    best_start, best_end = 0, -1
    start = None
    for index, flag in enumerate(flags):
        if flag:
            if start is None:
                start = index
        elif start is not None:
            if index - 1 - start > best_end - best_start:
                best_start, best_end = start, index - 1
            start = None
    if start is not None and len(flags) - 1 - start > best_end - best_start:
        best_start, best_end = start, len(flags) - 1
    return best_start, best_end


def force_traditional_axis_order(srs):
    """Force ``(x, y) = (经度/东向, 纬度/北向)`` axis order on an OGR SRS.

    GDAL 3 respects the authority-defined axis order by default: EPSG:4326 is
    ``(lat, lon)`` and Beijing 1954 / Gauss-Kruger (EPSG:2437) is
    ``(northing, easting)``. GeoJSON stores coordinates as ``(lon, lat)``, so
    without this call ``TransformPoint`` reads the longitude as a latitude and
    hands back ``(northing, easting)``, which ``prj2imagexy`` then treats as
    ``(easting, northing)`` - every sample lands outside the raster and gets
    clamped to its bottom-right corner (pure nodata).
    """
    srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    return srs


# specific crs change process
def transform_coordinates(coords, source_srs, target_srs):
    # GeoJSON 的坐标顺序恒为 (x, y) = (经度, 纬度)，与 CRS 的权威轴序无关，
    # 因此两端都显式声明传统 GIS 轴序，避免入口/出口只换一半。
    force_traditional_axis_order(source_srs)
    force_traditional_axis_order(target_srs)

    # 创建坐标转换对象
    transform = osr.CoordinateTransformation(source_srs, target_srs)

    # 转换坐标
    transformed_coords = []
    for coord in coords:
        x, y = coord[0], coord[1]
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
        # 不再交换 x/y：GeoJSON 已是 (lon, lat)，轴序由 transform_coordinates 统一声明
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


def _safe_dem_value(dem_data, row, col):
    """Read one cell, returning NaN for coordinates outside the raster.

    Sections can sit beyond the DEM footprint (the guard marks those samples as
    invalid); returning NaN here keeps the sampler from raising IndexError.
    """
    if 0 <= row < dem_data.shape[0] and 0 <= col < dem_data.shape[1]:
        return dem_data[row][col]
    return np.float32(np.nan)


# get the elevation of the 4 pixel coors
def get_elevation(interpolated_coors, dataset, dem_data=None):
    if dem_data is None:
        band = dataset.GetRasterBand(1)
        dem_data = band.ReadAsArray(
            0, 0, dataset.RasterXSize, dataset.RasterYSize
        ).astype(np.float32)
    dem_data = np.array(dem_data)
    Z = []
    for interpolated_coor in interpolated_coors:
        z1 = _safe_dem_value(dem_data, interpolated_coor[1], interpolated_coor[0])
        z2 = _safe_dem_value(dem_data, interpolated_coor[3], interpolated_coor[2])
        z3 = _safe_dem_value(dem_data, interpolated_coor[5], interpolated_coor[4])
        z4 = _safe_dem_value(dem_data, interpolated_coor[7], interpolated_coor[6])
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
    # the elevation of the 4 pixel coors (read the raster once, reuse the array)
    dem_data, valid_pixels = _load_dem_and_valid_mask(dataset)
    Z = get_elevation(interpolated_coors, dataset, dem_data)
    # per-sample contribution of the 4 bilinear corners, used by the nodata guard
    point_valid = _pixel_quad_validity(interpolated_coors, valid_pixels)
    # the vec3 of sample points
    sample_points_vec3 = bilinear_interpolation(image_coors, Z, sample_points)
    return sample_points_vec3, sample_points, step, point_valid


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
    sample_points_vec3, scatters, step, point_valid = get_sample_points_vec3(
        dataset, geometry, num_points
    )

    # ---- nodata guard -------------------------------------------------------
    # Keep only the longest continuously valid stretch of the section. Sampling
    # keeps the original spacing (`step`), so the trimmed array stays
    # index-consistent with every downstream consumer of section.json.
    total_points = len(point_valid)
    valid_points = int(sum(point_valid))
    run_start, run_end = _longest_valid_run(point_valid)
    run_points = run_end - run_start + 1 if run_end >= run_start else 0
    run_length_m = step * (run_points - 1) if run_points >= 2 else 0.0
    # `MIN_VALID_LENGTH_M` states how much usable data a section must retain - it is
    # a loss budget, not an absolute requirement. Capping it by the section's own
    # full length keeps the guard a strict no-op for a *complete* profile: a short
    # but fully valid section is what the model was always meant to consume, so it
    # must not be rejected for "only" being 35 m long. Only sections that lost data
    # (run shorter than the whole profile) are held to the absolute floor.
    full_length_m = step * (total_points - 1) if total_points >= 2 else 0.0
    required_length_m = min(MIN_VALID_LENGTH_M, full_length_m)
    data_quality = {
        "sample_points": total_points,
        "valid_points": valid_points,
        "nodata_points": total_points - valid_points,
        "valid_ratio": round(valid_points / total_points, 4) if total_points else 0.0,
        "valid_run_points": run_points,
        "valid_run_length_m": round(float(run_length_m), 2),
        "full_length_m": round(float(full_length_m), 2),
        "min_valid_length_m": MIN_VALID_LENGTH_M,
        "required_length_m": round(float(required_length_m), 2),
        "trimmed": run_points < total_points,
        "trim_range": [run_start, run_end] if run_points else None,
    }

    if run_points < MIN_VALID_POINTS or run_length_m < required_length_m:
        raise DemCoverageError(
            "DEM_COVERAGE_INSUFFICIENT",
            (
                f"the longest continuously valid stretch of this section is only "
                f"{run_length_m:.1f} m ({valid_points}/{total_points} usable sample "
                f"points, {total_points - valid_points} nodata) out of a "
                f"{full_length_m:.1f} m section, below the required "
                f"{required_length_m:.1f} m. The DEM has no data around this section - "
                f"check the DEM coverage / pick a section that falls on valid cells"
            ),
            data_quality,
        )

    if run_points < total_points:
        sample_points_vec3 = sample_points_vec3[run_start : run_end + 1]
        scatters = scatters[run_start : run_end + 1]

    # Calculate the slope ratio of 2 adjacent sample points
    Sa = computeSaIndex(sample_points_vec3, step)

    # Get the deepest point
    deepest_index = sample_points_vec3.index(
        min(sample_points_vec3, key=lambda point: point[2])
    )

    # Calculate the points and their heigh that the erosion model focuses
    if len(scatters) < 2:
        raise SystemError("Section scatter points are insufficient")

    # The vertical-profile walk below starts at `deepest_index` and only moves
    # leftwards, so a deepest sample sitting on the first index leaves no
    # left-bank slope and silently empties `points_v`/`Sa_v`. That is a data
    # geometry problem, not a recoverable one - fail with a clear reason.
    if deepest_index <= 0:
        raise DemCoverageError(
            "SECTION_PROFILE_DEGENERATE",
            (
                f"the deepest valid sample is the first point of the usable stretch "
                f"({data_quality['valid_run_length_m']} m spanning sample "
                f"{run_start}..{run_end} of {total_points}), so no left-bank slope can "
                f"be derived. The usable part of this section is off-centre - extend the "
                f"section across the full channel or check the DEM coverage"
            ),
            data_quality,
        )

    verified_end_index = deepest_index
    if verified_end_index <= 0:
        verified_end_index = 1
    elif verified_end_index >= len(scatters):
        verified_end_index = len(scatters) - 1

    er_point_coords_verified, step_er_verified = util.divide_point_line(
        scatters, 23, dataset.GetProjection(), verified_end_index
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
        # Previously this silently fell back to `slope_foot_index = 1`, which let
        # `heightDifference` report a bogus worst-case risk and only blew up later
        # inside `slopeRate`/`waterLevelFluctuation`/`antiImpactSpeed`.
        if end_height > 0:
            reason = (
                f"the deepest sample of this section is still at {end_height:.2f} m, "
                f"i.e. above the water-surface datum - the section appears to lie on dry "
                f"land / outside the modelled channel rather than crossing the riverbed"
            )
        else:
            reason = (
                f"no point was found between start height {start_height} and bed height "
                f"{end_height} within the usable stretch "
                f"({data_quality['valid_run_length_m']} m)"
            )
        raise DemCoverageError(
            "SECTION_VERTICAL_PROFILE_EMPTY",
            f"the vertical profile of this section is empty: {reason}",
            data_quality,
        )
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
        "data_quality": data_quality,
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


def _failure_code_of(exception):
    """把异常翻成一个稳定的错误码。

    ``DemCoverageError`` 这类业务异常自带 ``.code``（DEM_COVERAGE_INSUFFICIENT /
    SECTION_PROFILE_DEGENERATE / SECTION_VERTICAL_PROFILE_EMPTY），直接用；
    其余异常没有码，退化成类名（TypeError / SystemError / ...），
    至少还能区分「几何格式不对」和「采样点不足」。
    """
    code = getattr(exception, "code", None)
    if isinstance(code, str) and code.strip():
        return code.strip()
    return type(exception).__name__


def _write_failure_response(mcr: model.ModelCaseReference, exception):
    """失败时把原因写进 response.json，而不是只留 RESPONSING 写下的 "NONE"。

    时序上 RESPONSING 在模型子进程启动**之前**就把占位模板落盘了；子进程随后失败时，
    变的只有 status / runtime，response.json 仍是那堆 "NONE"。调用方拿到的是一个
    「看起来正常、内容全空」的响应，只能把 "NONE" 当文件名去取，真正的失败原因
    （DEM 无数据 / 剖面退化）就此在链路里丢失，最后只剩一句 404。

    这里补两个字段：error-code（稳定、可被上游分类）与 error-message（可读原因）。
    """
    try:
        mcr.make_response(
            {
                "case-id": mcr.id,
                "raw-json": "NONE",
                "visualization-txt": "NONE",
                "interval": "NONE",
                "error-code": _failure_code_of(exception),
                "error-message": str(exception),
            }
        )
    except Exception as response_exception:  # pragma: no cover
        # 落盘失败不能盖掉原始异常：那会让调用方看到"写响应失败"而不是真正的原因。
        _debug_log(
            "failure_response_write_failed",
            {"case_id": mcr.id, "error": str(response_exception)},
        )


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

    try:
        result = section_view(dem_path, section_geometry, output_path)
    except Exception as exception:
        # 剖面计算是本模型唯一会因数据质量失败的地方（nodata 覆盖率不足 / 剖面退化 /
        # 垂线剖面为空），失败原因必须落到响应里，否则上游只能看到占位 "NONE"。
        _write_failure_response(mcr, exception)
        raise

    raw_json_path = os.path.join(output_path, result["raw-file"])
    slope_foot_index = None
    deepest_index = None
    sample_v_count = None
    sa_v_count = None
    data_quality = None
    if os.path.exists(raw_json_path):
        with open(raw_json_path, "r", encoding="utf-8") as file:
            raw_data = json.load(file)
        slope_foot_index = raw_data.get("slope_foot_index")
        deepest_index = raw_data.get("deepest_index")
        sample_v_count = len(raw_data.get("points_v") or [])
        sa_v_count = len(raw_data.get("Sa_v") or [])
        data_quality = raw_data.get("data_quality")

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
            "data_quality": data_quality,
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
