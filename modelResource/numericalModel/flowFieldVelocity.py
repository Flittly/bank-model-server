import os
import sys

module_path = os.path.abspath(os.path.join(os.getcwd()))
if module_path not in sys.path:
    sys.path.append(module_path)

from math import sqrt
import model
import config
from osgeo import ogr, osr
import util
from util import db_ops


# 目标投影：Xian 1980 / 3-degree Gauss-Kruger CM 120E。
# `hydrodynamic_points.x/y` 就存这个坐标系，查最近水文点用的也是它。
TARGET_EPSG = 2437


def geo2proj(x, y, EPSG=4326):
    """把 (x, y) 从 EPSG 投影到 EPSG:2437，返回 (东, 北)。

    **必须显式声明传统 GIS 轴序**。GDAL 3 对 EPSG:4326 采用 lat/lon 的 authority
    轴序，旧实现靠 `AddPoint(lat, lng)` + `return (GetY, GetX)` 两次交换把它抵消掉
    —— 对 4326 恰好正确，但**对 EPSG:3857 会翻成垃圾坐标**（3857 本身就是 (x, y)
    轴序，交换不再抵消）：

        旧: geo2proj(13355639.0, 3767113.0, 3857) -> (-1080867.8, 9894925.2)
        应为:                                       (497644.1, 3545103.5)

    而这个函数恰恰会被 `section.json` 里的 3857 坡脚坐标（`points_v`）调用。错误
    坐标让「最近水文点」的 `ORDER BY (x-x0)^2+(y-y0)^2` 被常量偏移主导，排序退化
    成「x 最小 / y 最大」，与断面位置无关 —— 所有断面于是取到同一个水文点，Ky 恒等
    于回归式前因子 2.4231。

    `SetAxisMappingStrategy(OAMS_TRADITIONAL_GIS_ORDER)` 让两种源投影都按 (x, y)
    解释，不再需要任何交换。
    """
    source = osr.SpatialReference()
    source.ImportFromEPSG(EPSG)
    source.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)

    target = osr.SpatialReference()
    target.ImportFromEPSG(TARGET_EPSG)
    target.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)

    point = ogr.Geometry(ogr.wkbPoint)
    point.AddPoint(float(x), float(y))
    point.Transform(osr.CoordinateTransformation(source, target))

    return point.GetX(), point.GetY()


def get_flow_field_velocity(series, lng, lat):
    us = [float(item.get("u") or 0.0) for item in series]
    vs = [float(item.get("v") or 0.0) for item in series]

    return {"us": us, "vs": vs}


def _plausible_in_2437(x, y):
    """(x, y) 是否落在 EPSG:2437 的合理范围内。

    2437 是国内 3 度带高斯-克吕格，长江流域的东坐标约 3e5~7e5、北坐标约 3e6~4e6。
    这里给一个宽松的包围盒，只为拦住「投影失败」这类数量级错误：`(1.3e7, 3.7e6)`
    被当经度投出来会变成 `(-1e6, 9.9e6)`，那样查最近点会退化成一个与位置无关的常量，
    而结果看起来完全正常（Ky 恒等于回归式前因子）。宁可在这里抛错。
    """
    return 1.0e5 <= x <= 1.5e6 and 1.0e6 <= y <= 6.0e6


def get_hydrodynamic_series_by_coordinate(hydrodynamic_mcr, lng, lat):
    _lng = float(lng)
    _lat = float(lat)

    lng_prj = 0.0
    lat_prj = 0.0
    if _lng <= 180.0 and _lat <= 180.0:
        lng_prj, lat_prj = geo2proj(_lng, _lat)
    elif _lng > 10000000.0:
        lng_prj, lat_prj = geo2proj(_lng, _lat, 3857)
    else:
        lng_prj = _lng
        lat_prj = _lat

    if not _plausible_in_2437(lng_prj, lat_prj):
        raise ValueError(
            f"coordinate projection failed: input=({_lng}, {_lat}) "
            f"projected=({lng_prj}, {lat_prj}) is outside the plausible EPSG:2437 "
            f"range of the Yangtze study area"
        )

    nearest_point = db_ops.get_nearest_hydrodynamic_point(
        dataset_domain=hydrodynamic_mcr.request_json["segment"],
        set_name=hydrodynamic_mcr.request_json["set"],
        water_qs=hydrodynamic_mcr.request_json["water-qs"],
        tidal_level=hydrodynamic_mcr.request_json["tidal-level"],
        x=lng_prj,
        y=lat_prj,
    )
    if not nearest_point:
        raise ValueError("No nearest hydrodynamic point found in database")

    return db_ops.get_hydrodynamic_series(nearest_point["id"])


##########################################################################################


@model.model_status_controller_sync
def run_flow_field_velocity_mcr(mcr: model.ModelCaseReference):
    hydrodynamic_mcr = model.ModelCaseReference.open_case(mcr.request_json["case-id"])
    lng = mcr.request_json["lng"]
    lat = mcr.request_json["lat"]

    series = get_hydrodynamic_series_by_coordinate(hydrodynamic_mcr, lng, lat)
    result = get_flow_field_velocity(series, lng, lat)

    return {"case-id": mcr.id, "result": result}


@model.model_status_controller_sync
def run_flow_field_velocities_mcr(mcr: model.ModelCaseReference):
    hydrodynamic_mcr = model.ModelCaseReference.open_case(mcr.request_json["case-id"])
    sample_points = mcr.request_json["sample-points"]

    result = []
    for sample_point in sample_points:
        lng = sample_point["lng"]
        lat = sample_point["lat"]
        series = get_hydrodynamic_series_by_coordinate(hydrodynamic_mcr, lng, lat)
        result.append(get_flow_field_velocity(series, lng, lat))

    return {"case-id": mcr.id, "result": result}


##########################################################################################

NAME = "Flow-Field Velocity"

CATEGORY = "Numerical Model"

CATEGORY_ALIAS = "nm"


def PARSING(
    self, request_json: dict, model_path: str, other_dependent_ids: list[str] = []
):
    flow_field_velocity_mcr = model.ModelCaseReference.create(
        config.API_NM_FLOW_FIELD_VELOCITY,
        request_json,
        NAME,
        model_path,
        other_dependent_ids,
    )

    return [flow_field_velocity_mcr]


def RESPONSING(
    self,
    core_mcr: model.ModelCaseReference,
    default_pre_mcrs: list[model.ModelCaseReference],
    other_pre_mcrs: list[model.ModelCaseReference],
):
    return core_mcr.make_response({"case-id": "TEMPLATE", "result": "NONE"})


##########################################################################################

if __name__ == "__main__":
    v1 = sys.argv[1]  # flow_field_velocity_mcr

    flow_field_velocity_mcr = model.ModelCaseReference.open_case(v1)

    if not "sample-points" in flow_field_velocity_mcr.request_json:
        run_flow_field_velocity_mcr(flow_field_velocity_mcr)
    else:
        run_flow_field_velocities_mcr(flow_field_velocity_mcr)
