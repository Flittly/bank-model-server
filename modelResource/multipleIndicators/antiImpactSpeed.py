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
from modelResource.numericalModel.flowFieldVelocity import (
    get_hydrodynamic_series_by_coordinate,
)


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
        dataset_domain=segment_name,
        set_name=set_name,
        tidal_level=mapped_tidal_level,
    )

    if not nodes:
        segment_name = db_ops.DEFAULT_DOMAIN
        set_name = "standard"
        year = "default"
        nodes = db_ops.get_available_hydrodynamic_nodes(
            dataset_domain=segment_name,
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
    if len(nodes) == 1:
        mapped_water_qs = nodes[0]
    elif i_water_qs <= nodes[0]:
        mapped_water_qs = nodes[0]
    elif i_water_qs >= nodes[-1]:
        mapped_water_qs = nodes[-1]
    elif i_water_qs in nodes:
        mapped_water_qs = i_water_qs
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


# 回归式：Ky = 2.4231 * exp(-0.547 * max_v)。2.4231 是**前因子**（max_v -> 0 的取值），
# 不是「算不出来时的默认值」——所以只要看到所有断面 Ky 都恰好等于 2.4231，
# 就等价于「max_v 恒为 0」，即流速全部没取到。
KY_INTERCEPT = 2.4231
KY_DECAY = 0.547

# `hydrodynamic_data` 里 u/v 的缺失哨兵是 -99999
NODATA_LIMIT = -999.0


def calculate_velocity(x, y):
    if x <= NODATA_LIMIT or y <= NODATA_LIMIT:
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
    nodata_samples = 0
    for i in range(len(us)):
        if us[i] <= NODATA_LIMIT or vs[i] <= NODATA_LIMIT:
            nodata_samples += 1
        velocities.append(calculate_velocity(us[i], vs[i]))

    max_v = max(velocities)
    Ky = KY_INTERCEPT * exp(-KY_DECAY * max_v)

    # 「有没有取到流速」必须随结果一起上报，否则下游只能看到一个似真的 Ky。
    data_quality = {
        "samples": len(velocities),
        "nodata_samples": nodata_samples,
        "max_velocity": max_v,
        "velocity_available": nodata_samples < len(velocities),
    }
    if velocities and not data_quality["velocity_available"]:
        # 全时段 nodata：max_v 只能是 0，于是 Ky 退化成前因子 2.4231，再被判成第 1 档
        # （最轻）。「没有数据」被读成「最安全」是最坏的一种静默。这里不抛错 ——
        # riskLevel 会因为拿不到数值矩阵而整条断面失败 —— 所以至少要留下痕迹。
        print(
            f"[anti-impact-speed][warn] all {nodata_samples} samples are nodata, "
            f"Ky falls back to the regression intercept {KY_INTERCEPT}",
            flush=True,
        )

    if Ky > risk_threshold[0]:
        risk = [1, 0, 0, 0]
    elif Ky > risk_threshold[1]:
        risk = [0, 1, 0, 0]
    elif Ky > risk_threshold[2]:
        risk = [0, 0, 1, 0]
    elif Ky <= risk_threshold[2]:
        risk = [0, 0, 0, 1]

    return Ky, risk, data_quality


def get_flow_field_velocity(series):
    us = [float(item.get("u") or 0.0) for item in series]
    vs = [float(item.get("v") or 0.0) for item in series]
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

    series = get_hydrodynamic_series_by_coordinate(
        hydrodynamic_mcr, slope_foot[0], slope_foot[1]
    )
    result = get_flow_field_velocity(series)

    # flow_field_velocity_mcr = model.ModelCaseReference.create(config.API_NM_FLOW_FIELD_VELOCITY, flow_field_velocity_json, 'Flow-Field Velocity', '/Users/soku/Desktop/bank_model_service/BankModel_Service/resource/model/a323cc8a6d31420c6c26d96d6b68e4e6/a323cc8a6d31420c6c26d96d6b68e4e6.cpython-311.pyc')
    # flow_field_velocity_mcr.update_status(config.STATUS_LOCK)
    # model.run_flow_field_velocity_mcr(flow_field_velocity_mcr)

    # result = []

    # with open(os.path.join(flow_field_velocity_mcr.directory, 'response.json'), 'r', encoding='utf-8') as file:
    #     result = json.load(file).get('result')

    risk_threshold = mcr.request_json["risk-threshold"]

    Ky, risk, data_quality = compute_anti_impact_speed(result, risk_threshold)

    print(
        f"[anti-impact-speed][debug] slope_foot=({slope_foot[0]}, {slope_foot[1]}) "
        f"max_v={data_quality['max_velocity']} Ky={Ky} "
        f"nodata={data_quality['nodata_samples']}/{data_quality['samples']}",
        flush=True,
    )

    return {
        "case-id": mcr.id,
        "Ky": Ky,
        "risk-level": risk,
        "data-quality": data_quality,
    }


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
