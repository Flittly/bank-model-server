import os
import sys

module_path = os.path.abspath(os.path.join(os.getcwd()))
if module_path not in sys.path:
    sys.path.append(module_path)

import model
import config
import json
from math import sqrt, exp
from osgeo import ogr, osr

import re
import util
from util import db_ops


def get_mapped_tidal_level_and_mapped_water_qs(
    segment_name: str, year: str, set_name: str, tidal_level: list | str, water_qs: str
):
    def get_qs_and_level(filename):
        match = re.match(r"(\d+)([a-zA-Z]+)", filename)
        qs = int(match.group(1))
        level = match.group(2)
        return qs, level

    mapped_tidal_level = ""
    if not isinstance(tidal_level, str):
        tidal_level = float(tidal_level)
        mapped_tidal_level = ""
        b1 = 1.5
        b2 = 2.3
        value = tidal_level
        if value < b1:
            mapped_tidal_level = "xc"
        elif value < b2:
            mapped_tidal_level = "zc"
        else:
            mapped_tidal_level = "dc"
    else:
        mapped_tidal_level = tidal_level

    # Get all node
    nodes = db_ops.get_available_hydrodynamic_nodes(
        region_code=segment_name,
        set_name=set_name,
        tidal_level=mapped_tidal_level,
    )

    if not nodes:
        segment_name = "Mzs"
        set_name = "standard"
        year = "default"
        nodes = db_ops.get_available_hydrodynamic_nodes(
            region_code=segment_name,
            set_name=set_name,
            tidal_level=mapped_tidal_level,
        )

    if not nodes:
        raise ValueError(
            f"No available hydrodynamic nodes for segment={segment_name}, set={set_name}, tidal_level={mapped_tidal_level}"
        )

    # map the input water_qs to node
    mapped_water_qs = 0
    i_water_qs = int(water_qs)
    if i_water_qs < nodes[0]:
        mapped_water_qs = nodes[0]
    elif i_water_qs > nodes[-1]:
        mapped_water_qs = nodes[-1]
    else:
        middles = [
            (nodes[i] + (nodes[i + 1] - nodes[i]) / 2 - 1)
            for i in range(len(nodes) - 1)
        ]
        closest_middle = min(middles, key=lambda middle: abs(middle - i_water_qs))
        index = middles.index(closest_middle)

        if i_water_qs - closest_middle < 0:
            mapped_water_qs = nodes[index]
        else:
            mapped_water_qs = nodes[index + 1]

    return mapped_tidal_level, mapped_water_qs, segment_name, year, set_name


def calculate_velocity(x, y):
    if x <= -999.0 or y <= -999.0:
        x = 0.0
        y = 0.0

    return sqrt(x**2 + y**2)


def compute_anti_impact_speed(result, risk_threshold):
    risk = []
    if risk_threshold == "NONE":
        with open(
            config.DIR_RESOURCE_RISKLEVEL_THRESHOLD_TEMPLATE, "r", encoding="utf-8"
        ) as file:
            risk_threshold = json.load(file).get("Ky")

    us = result["us"]
    vs = result["vs"]
    velocities = []
    for i in range(len(us)):
        velocities.append(calculate_velocity(us[i], vs[i]))

    max_v = max(velocities)
    Ky = 2.4231 * exp(-0.547 * max_v)

    if Ky > risk_threshold[0]:
        risk = [1, 0, 0, 0]
    elif Ky > risk_threshold[1]:
        risk = [0, 1, 0, 0]
    elif Ky > risk_threshold[2]:
        risk = [0, 0, 1, 0]
    elif Ky <= risk_threshold[2]:
        risk = [0, 0, 0, 1]

    return Ky, risk


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


def get_flow_field_velocity(raw_paths, lng, lat):
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

    min_distance = float("inf")
    us = []
    vs = []
    index = 0

    # 从第一个时间点中找到距离最近点的索引
    with open(os.path.join(config.DIR_RESOURCE, raw_paths[0]), "r") as file:
        # 跳过前两行
        for _ in range(2):
            next(file)

        # 从第三行开始处理
        for line_index, line in enumerate(file, start=2):
            parts = line.split()
            # 提取坐标和数据
            coords = (float(parts[0]), float(parts[1]))
            distance = util.calculate_distance(lng_prj, lat_prj, coords[0], coords[1])
            # 检查是否是最近的坐标
            if distance < min_distance:
                min_distance = distance
                index = line_index

    # 遍历所有时间点，根据索引找到对应点，获取u,v
    for i in range(26):
        path = os.path.join(config.DIR_RESOURCE, raw_paths[i])
        with open(path, "r") as file:
            # 跳过前两行
            for _ in range(2):
                next(file)

            # 从第三行开始处理
            for line_index, line in enumerate(file, start=2):
                if line_index == index:
                    parts = line.split()
                    us.append(float(parts[-2]))
                    vs.append(float(parts[-1]))

    return {"us": us, "vs": vs}


##########################################################################################


@model.model_status_controller_sync
def run_anti_impact_speed_mcr(mcr: model.ModelCaseReference):
    section_view_output_path = os.path.join(section_view_mcr.directory, "result")
    section_json = os.path.join(section_view_output_path, "section.json")

    slope_foot = []
    with open(section_json, "r", encoding="utf-8") as file:
        data = json.load(file)
        points_v = data.get("points_v")
        slope_foot_index = data.get("slope_foot_index")
        slope_foot = points_v[slope_foot_index]
        # slope_foot = data['points'][data['deepest_index']]

    flow_field_velocity_json = {
        "case-id": hydrodynamic_mcr.id,
        "lng": slope_foot[0],
        "lat": slope_foot[1],
    }

    raw_paths = hydrodynamic_mcr.make_response()["raw-txts"]
    result = get_flow_field_velocity(raw_paths, slope_foot[0], slope_foot[1])

    # flow_field_velocity_mcr = model.ModelCaseReference.create(config.API_NM_FLOW_FIELD_VELOCITY, flow_field_velocity_json, 'Flow-Field Velocity', '/Users/soku/Desktop/bank_model_service/BankModel_Service/resource/model/a323cc8a6d31420c6c26d96d6b68e4e6/a323cc8a6d31420c6c26d96d6b68e4e6.cpython-311.pyc')
    # flow_field_velocity_mcr.update_status(config.STATUS_LOCK)
    # model.run_flow_field_velocity_mcr(flow_field_velocity_mcr)

    # result = []

    # with open(os.path.join(flow_field_velocity_mcr.directory, 'response.json'), 'r', encoding='utf-8') as file:
    #     result = json.load(file).get('result')

    risk_threshold = mcr.request_json["risk-threshold"]

    Ky, risk = compute_anti_impact_speed(result, risk_threshold)

    return {"case-id": mcr.id, "Ky": Ky, "risk-level": risk}


##########################################################################################

NAME = "Anti-Imapcat Speed"

CATEGORY = "Multiple Indicators"

CATEGORY_ALIAS = "mi"


def PARSING(
    self, request_json: dict, model_path: str, other_dependent_ids: list[str] = []
):
    anti_impact_speed_json = request_json
    section_view_json = {
        "dem-id": anti_impact_speed_json["dem-id"],
        "section-geometry": anti_impact_speed_json["section-geometry"],
    }

    segment = request_json["segment"]
    year = request_json["year"]
    set = request_json["set"]
    water_qs = request_json["water-qs"]
    tidal_level = request_json["tidal-level"]
    mapped_tidal_level, mapped_water_qs, segment, year, set = (
        get_mapped_tidal_level_and_mapped_water_qs(
            segment, year, set, tidal_level, water_qs
        )
    )

    map_json = {
        "segment": segment,
        "year": year,
        "set": set,
        "water-qs": f"{mapped_water_qs}",
        "tidal-level": mapped_tidal_level,
    }

    section_view_mcr = model.launcher.fetch_model_from_API(
        config.API_RE_SECTION_VIEW
    ).run(section_view_json, other_dependent_ids)
    hydrodynamic_mcr = model.launcher.fetch_model_from_API(
        config.API_NM_HYDRODYNAMIC
    ).run(map_json, other_dependent_ids)
    anti_impact_speed_mcr = model.ModelCaseReference.create(
        config.API_MI_ANTI_IMPACT_SPEED,
        anti_impact_speed_json,
        NAME,
        model_path,
        other_dependent_ids + [section_view_mcr.id, hydrodynamic_mcr.id],
    )

    return [section_view_mcr, hydrodynamic_mcr, anti_impact_speed_mcr]


def RESPONSING(
    self,
    core_mcr: model.ModelCaseReference,
    default_pre_mcrs: list[model.ModelCaseReference],
    other_pre_mcrs: list[model.ModelCaseReference],
):
    return core_mcr.make_response(
        {"case-id": "TEMPLATE", "Ky": "NONE", "risk-level": "NONE"}
    )


##########################################################################################

if __name__ == "__main__":
    v1 = sys.argv[1]
    v2 = sys.argv[2]
    v3 = sys.argv[3]

    section_view_mcr = model.ModelCaseReference.open_case(v1)
    hydrodynamic_mcr = model.ModelCaseReference.open_case(v2)
    anti_impact_speed_mcr = model.ModelCaseReference.open_case(v3)

    # Run model case (Anti-Impact Speed)
    run_anti_impact_speed_mcr(anti_impact_speed_mcr)
