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


def geo2proj(lng, lat, EPSG=4326):
    source = osr.SpatialReference()
    source.ImportFromEPSG(EPSG)

    target = osr.SpatialReference()
    target.ImportFromEPSG(2437)

    transform = osr.CoordinateTransformation(source, target)

    point = ogr.Geometry(ogr.wkbPoint)
    point.AddPoint(lat, lng)

    point.Transform(transform)

    lat_prj, lng_prj = point.GetX(), point.GetY()

    return lng_prj, lat_prj


def get_flow_field_velocity(series, lng, lat):
    us = [float(item.get("u") or 0.0) for item in series]
    vs = [float(item.get("v") or 0.0) for item in series]

    return {"us": us, "vs": vs}


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
