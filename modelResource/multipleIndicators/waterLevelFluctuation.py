import os
import sys

module_path = os.path.abspath(os.path.join(os.getcwd()))
if module_path not in sys.path:
    sys.path.append(module_path)

import model
import config
import json
from math import sqrt, exp

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
    return sqrt(x**2 + y**2)


def find_nearest_value(input_num, _dict):
    nearest_key = min(_dict.keys(), key=lambda k: abs(int(k) - int(input_num)))
    return _dict[nearest_key]


def compute_water_level_fluctuation(p, water_qs, Z, risk_threshold):
    risk = []
    if risk_threshold == "NONE":
        with open(
            config.DIR_RESOURCE_RISKLEVEL_THRESHOLD_TEMPLATE, "r", encoding="utf-8"
        ) as file:
            risk_threshold = json.load(file).get("Zd")

    B_dict = {
        "10000": "0.027",
        "16500": "0.031",
        "28500": "0.040",
        "35000": "0.045",
        "45000": "0.055",
        "57000": "0.070",
        "62000": "0.077",
        "74000": "0.098",
        "84000": "0.120",
        "92000": "0.141",
        "104000": "0.167",
    }

    B = find_nearest_value(water_qs, B_dict)

    Zmax1 = 0
    Zmax2 = 0
    Zmin1 = 0
    Zmin2 = 0

    flag = 0
    if p[0] > p[1]:
        flag = -1
    else:
        flag = 1
    if flag == -1:
        index = 0
        for i in range(1, len(p) - 1):
            if p[i] < p[i + 1]:
                Zmin1 = p[i]
                index = i
                break
        for i in range(index + 1, len(p) - 1):
            if p[i] > p[i + 1]:
                Zmax1 = p[i]
                index = i
                break
        for i in range(index + 1, len(p) - 1):
            if p[i] < p[i + 1]:
                Zmin2 = p[i]
                index = i
                break
        for i in range(index + 1, len(p) - 1):
            if p[i] > p[i + 1]:
                Zmax2 = p[i]
                index = i
                break
    elif flag == 1:
        index = 0
        for i in range(1, len(p) - 1):
            if p[i] > p[i + 1]:
                Zmax1 = p[i]
                index = i
                break
        for i in range(index + 1, len(p) - 1):
            if p[i] < p[i + 1]:
                Zmin1 = p[i]
                index = i
                break
        for i in range(index + 1, len(p) - 1):
            if p[i] > p[i + 1]:
                Zmax2 = p[i]
                index = i
                break
        for i in range(index + 1, len(p) - 1):
            if p[i] < p[i + 1]:
                Zmin2 = p[i]
                index = i
                break

    A = 0.5 * (Zmax1 + Zmax2 - Zmin1 - Zmin2)
    if Z == 0.0:
        Zd = float(B)
    else:
        Zd = float(B) + A / Z

    if Zd < risk_threshold[0]:
        risk = [1, 0, 0, 0]
    elif Zd < risk_threshold[1]:
        risk = [0, 1, 0, 0]
    elif Zd < risk_threshold[2]:
        risk = [0, 0, 1, 0]
    elif Zd >= risk_threshold[2]:
        risk = [0, 0, 0, 1]

    return Zd, risk


##########################################################################################


@model.model_status_controller_sync
def run_water_level_fluctuation_mcr(mcr: model.ModelCaseReference):
    section_view_output_path = os.path.join(section_view_mcr.directory, "result")
    section_json = os.path.join(section_view_output_path, "section.json")

    slope_foot = []
    with open(section_json, "r", encoding="utf-8") as file:
        data = json.load(file)
        points_v = data.get("points_v")
        slope_foot_index = data.get("slope_foot_index")
        slope_foot = points_v[slope_foot_index]

    min_distance = float("inf")
    raw_paths = hydrodynamic_mcr.make_response()["raw-txts"]

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
            distance = util.calculate_distance(
                slope_foot[0], slope_foot[1], coords[0], coords[1]
            )
            # 检查是否是最近的坐标
            if distance < min_distance:
                min_distance = distance
                index = line_index

    p = []
    Z = 0
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
                    p.append(float(parts[3]))
                    if Z == 0:
                        Z = float(parts[2])

    risk_threshold = mcr.request_json["risk-threshold"]

    water_qs = hydrodynamic_mcr.request_json["water-qs"]
    Zd, risk = compute_water_level_fluctuation(p, water_qs, Z, risk_threshold)

    return {"case-id": mcr.id, "Zd": Zd, "risk-level": risk}


##########################################################################################

NAME = "Water-Level Fluctuation"

CATEGORY = "Multiple Indicators"

CATEGORY_ALIAS = "mi"


def PARSING(
    self, request_json: dict, model_path: str, other_dependent_ids: list[str] = []
):
    water_level_fluctuation_json = request_json
    section_view_json = {
        "dem-id": water_level_fluctuation_json["dem-id"],
        "section-geometry": water_level_fluctuation_json["section-geometry"],
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
    water_level_fluctuation_mcr = model.ModelCaseReference.create(
        config.API_MI_WATER_LEVEL_FLUCTUATION,
        water_level_fluctuation_json,
        NAME,
        model_path,
        other_dependent_ids + [section_view_mcr.id, hydrodynamic_mcr.id],
    )

    return [section_view_mcr, hydrodynamic_mcr, water_level_fluctuation_mcr]


def RESPONSING(
    self,
    core_mcr: model.ModelCaseReference,
    default_pre_mcrs: list[model.ModelCaseReference],
    other_pre_mcrs: list[model.ModelCaseReference],
):
    return core_mcr.make_response(
        {"case-id": "TEMPLATE", "Zd": "NONE", "risk-level": "NONE"}
    )


##########################################################################################

if __name__ == "__main__":
    v1 = sys.argv[1]
    v2 = sys.argv[2]
    v3 = sys.argv[3]

    # Open model case
    section_view_mcr = model.ModelCaseReference.open_case(v1)
    hydrodynamic_mcr = model.ModelCaseReference.open_case(v2)
    water_level_fluctuation_mcr = model.ModelCaseReference.open_case(v3)

    # Run model case (Water-Level Fluctuation)
    run_water_level_fluctuation_mcr(water_level_fluctuation_mcr)
