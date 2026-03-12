import os
import re
import sys
import json
import numpy as np

module_path = os.path.abspath(os.path.join(os.getcwd()))
if module_path not in sys.path:
    sys.path.append(module_path)

import util
import model
import config
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


def get_risk_level(response_list, wRE, wNM, wGE, wRL, risk_threshold):
    with open(
        config.DIR_RESOURCE_RISKLEVEL_THRESHOLD_TEMPLATE, "r", encoding="utf-8"
    ) as file:
        template = json.load(file)
        if risk_threshold == "NONE":
            risk_threshold = template["all"]
        if wRE == "NONE":
            wRE = template["wRE"]
        if wNM == "NONE":
            wNM = template["wNM"]
        if wGE == "NONE":
            wGE = template["wGE"]
        if wRL == "NONE":
            wRL = template["wRL"]

    soil_composition_response = response_list[0]
    slope_protection_response = response_list[1]
    load_control_mcr_response = response_list[2]
    height_difference_response = response_list[3]
    slope_rate_response = response_list[4]
    nearshore_flush_mcr_response = response_list[5]
    flow_equivalent_mcr_response = response_list[6]
    anti_impact_speed_response = response_list[7]
    water_level_fluctuation_response = response_list[8]

    with open(soil_composition_response, "r", encoding="utf-8") as file:
        soil_composition_risk_level_matrix = np.array(
            [json.load(file).get("risk-level")]
        )
    with open(slope_protection_response, "r", encoding="utf-8") as file:
        slope_protection_risk_level_matrix = np.array(
            [json.load(file).get("risk-level")]
        )
    with open(load_control_mcr_response, "r", encoding="utf-8") as file:
        load_control_risk_level_matrix = np.array([json.load(file).get("risk-level")])
    with open(height_difference_response, "r", encoding="utf-8") as file:
        height_difference_risk_level_matrix = np.array(
            [json.load(file).get("risk-level")]
        )
    with open(slope_rate_response, "r", encoding="utf-8") as file:
        slope_rate_risk_level_matrix = np.array([json.load(file).get("risk-level")])
    with open(nearshore_flush_mcr_response, "r", encoding="utf-8") as file:
        nearshore_flush_risk_level_matrix = np.array(
            [json.load(file).get("risk-level")]
        )
    with open(flow_equivalent_mcr_response, "r", encoding="utf-8") as file:
        flow_equivalent_risk_level_matrix = np.array(
            [json.load(file).get("risk-level")]
        )
    with open(anti_impact_speed_response, "r", encoding="utf-8") as file:
        anti_impact_speed_risk_level_matrix = np.array(
            [json.load(file).get("risk-level")]
        )
    with open(water_level_fluctuation_response, "r", encoding="utf-8") as file:
        water_level_fluctuation_risk_level_matrix = np.array(
            [json.load(file).get("risk-level")]
        )

    # matrix transpose
    soil_composition_risk_level_matrix_T = soil_composition_risk_level_matrix.T
    slope_protection_risk_level_matrix_T = slope_protection_risk_level_matrix.T
    load_control_risk_level_matrix_T = load_control_risk_level_matrix.T
    height_difference_risk_level_matrix_T = height_difference_risk_level_matrix.T
    slope_rate_risk_level_matrix_T = slope_rate_risk_level_matrix.T
    nearshore_flush_risk_level_matrix_T = nearshore_flush_risk_level_matrix.T
    flow_equivalent_risk_level_matrix_T = flow_equivalent_risk_level_matrix.T
    anti_impact_speed_risk_level_matrix_T = anti_impact_speed_risk_level_matrix.T
    water_level_fluctuation_risk_level_matrix_T = (
        water_level_fluctuation_risk_level_matrix.T
    )

    # matrix concat
    water_flow_power_matrix = np.hstack(
        (
            anti_impact_speed_risk_level_matrix_T,
            flow_equivalent_risk_level_matrix_T,
            water_level_fluctuation_risk_level_matrix_T,
        )
    )
    riverbed_evolution_matrix = np.hstack(
        (
            slope_rate_risk_level_matrix_T,
            nearshore_flush_risk_level_matrix_T,
            height_difference_risk_level_matrix_T,
        )
    )
    geology_engineering_matrix = np.hstack(
        (
            soil_composition_risk_level_matrix_T,
            slope_protection_risk_level_matrix_T,
            load_control_risk_level_matrix_T,
        )
    )

    # weight matrix
    water_flow_power_weight_matrix = np.array(wNM).reshape(3, 1)
    riverbed_evolution_weight_matrix = np.array(wRE).reshape(3, 1)
    geology_engineering_weight_matrix = np.array(wGE).reshape(3, 1)

    # matrix dot calculation to get 3 result matrixs
    water_flow_power_result_matrix = np.dot(
        water_flow_power_matrix, water_flow_power_weight_matrix
    )
    riverbed_evolution_result_matrix = np.dot(
        riverbed_evolution_matrix, riverbed_evolution_weight_matrix
    )
    geology_engineering_result_matrix = np.dot(
        geology_engineering_matrix, geology_engineering_weight_matrix
    )

    # matrix concat
    concat_matrix = np.hstack(
        (
            water_flow_power_result_matrix,
            riverbed_evolution_result_matrix,
            geology_engineering_result_matrix,
        )
    )

    # final weight matrix
    weight_matrix = np.array(wRL).reshape(3, 1)

    # final result matrix
    result_matrix = np.dot(concat_matrix, weight_matrix)

    risk_value = result_matrix[2] + result_matrix[3]

    risk = []

    if risk_value < risk_threshold[0]:
        risk = [1, 0, 0, 0]
    elif risk_value < risk_threshold[1]:
        risk = [0, 1, 0, 0]
    elif risk_value < risk_threshold[2]:
        risk = [0, 0, 1, 0]
    elif risk_value >= risk_threshold[2]:
        risk = [0, 0, 0, 1]

    return risk_value[0], risk


##########################################################################################


@model.model_status_controller_sync
def run_risk_level_mcr(mcr: model.ModelCaseReference):
    response_list = []
    soil_composition_response = os.path.join(
        soil_composition_mcr.directory, "response.json"
    )
    slope_protection_response = os.path.join(
        slope_protection_mcr.directory, "response.json"
    )
    load_control_mcr_response = os.path.join(
        load_control_mcr.directory, "response.json"
    )
    height_difference_response = os.path.join(
        height_difference_mcr.directory, "response.json"
    )
    slope_rate_response = os.path.join(slope_rate_mcr.directory, "response.json")
    nearshore_flush_mcr_response = os.path.join(
        nearshore_flush_mcr.directory, "response.json"
    )
    flow_equivalent_mcr_response = os.path.join(
        flow_equivalent_mcr.directory, "response.json"
    )
    anti_impact_speed_response = os.path.join(
        anti_impact_speed_mcr.directory, "response.json"
    )
    water_level_fluctuation_response = os.path.join(
        water_level_fluctuation_mcr.directory, "response.json"
    )

    response_list.append(soil_composition_response)
    response_list.append(slope_protection_response)
    response_list.append(load_control_mcr_response)
    response_list.append(height_difference_response)
    response_list.append(slope_rate_response)
    response_list.append(nearshore_flush_mcr_response)
    response_list.append(flow_equivalent_mcr_response)
    response_list.append(anti_impact_speed_response)
    response_list.append(water_level_fluctuation_response)

    risk_threshold = "NONE"
    wRE = "NONE"
    wNM = "NONE"
    wGE = "NONE"
    wRL = "NONE"
    if not mcr.request_json["risk-thresholds"] == "NONE":
        risk_threshold = mcr.request_json["risk-thresholds"]["all"]
    if not mcr.request_json["wRE"] == "NONE":
        wRE = mcr.request_json["wRE"]
    if not mcr.request_json["wNM"] == "NONE":
        wNM = mcr.request_json["wNM"]
    if not mcr.request_json["wGE"] == "NONE":
        wGE = mcr.request_json["wGE"]
    if not mcr.request_json["wRL"] == "NONE":
        wRL = mcr.request_json["wRL"]

    result, risk_level = get_risk_level(
        response_list, wRE, wNM, wGE, wRL, risk_threshold
    )

    return {
        "case-id": mcr.id,
        "multi-indicator-ids": {
            "Dsed": soil_composition_mcr.id,
            "PL": slope_protection_mcr.id,
            "LC": load_control_mcr.id,
            "Zb": height_difference_mcr.id,
            "Sa": slope_rate_mcr.id,
            "Ln": nearshore_flush_mcr.id,
            "PQ": flow_equivalent_mcr.id,
            "Ky": anti_impact_speed_mcr.id,
            "Zd": water_level_fluctuation_mcr.id,
        },
        "result": result,
        "risk-level": risk_level,
    }


##########################################################################################

NAME = "Risk Level"

CATEGORY = "Multiple Indicators"

CATEGORY_ALIAS = "mi"


def PARSING(
    self, request_json: dict, model_path: str, other_dependent_ids: list[str] = []
):
    segment = request_json["segment"]
    year: str = request_json["current-timepoint"].split("-")[0]
    set = request_json["set"]
    water_qs = request_json["water-qs"]
    tidal_level = request_json["tidal-level"]
    mapped_tidal_level, mapped_water_qs, segment, year, set = (
        get_mapped_tidal_level_and_mapped_water_qs(
            segment, year, set, tidal_level, water_qs
        )
    )

    risk_threshold_Dsed = "NONE"
    risk_threshold_Zb = "NONE"
    risk_threshold_Sa = "NONE"
    risk_threshold_Ln = "NONE"
    risk_threshold_PQ = "NONE"
    risk_threshold_Ky = "NONE"
    risk_threshold_Zd = "NONE"

    if not request_json["risk-thresholds"] == "NONE":
        risk_threshold_Dsed = request_json["risk-thresholds"]["Dsed"]
        risk_threshold_Zb = request_json["risk-thresholds"]["Zb"]
        risk_threshold_Sa = request_json["risk-thresholds"]["Sa"]
        risk_threshold_Ln = request_json["risk-thresholds"]["Ln"]
        risk_threshold_PQ = request_json["risk-thresholds"]["PQ"]
        risk_threshold_Ky = request_json["risk-thresholds"]["Ky"]
        risk_threshold_Zd = request_json["risk-thresholds"]["Zd"]

    soil_composition_json = {
        "hs": request_json["hs"],
        "hc": request_json["hc"],
        "risk-threshold": risk_threshold_Dsed,
    }

    slope_protection_json = {"protection-level": request_json["protection-level"]}

    load_control_json = {"control-level": request_json["control-level"]}

    height_difference_json = {
        "dem-id": request_json["bench-id"],
        "section-geometry": request_json["section-geometry"],
        "risk-threshold": risk_threshold_Zb,
    }

    slope_rate_json = {
        "dem-id": request_json["bench-id"],
        "section-geometry": request_json["section-geometry"],
        "risk-threshold": risk_threshold_Sa,
    }

    nearshore_flush_json = {
        "bench-id": request_json["bench-id"],
        "ref-id": request_json["ref-id"],
        "current-timepoint": request_json["current-timepoint"],
        "comparison-timepoint": request_json["comparison-timepoint"],
        "section-geometry": request_json["section-geometry"],
        "risk-threshold": risk_threshold_Ln,
    }

    flow_equivalent_json = {
        "segment": segment,
        "year": year,
        "risk-threshold": risk_threshold_PQ,
    }

    anti_impact_speed_json = {
        "dem-id": request_json["bench-id"],
        "section-geometry": request_json["section-geometry"],
        "segment": segment,
        "year": year,
        "set": set,
        "water-qs": mapped_water_qs,
        "tidal-level": mapped_tidal_level,
        "risk-threshold": risk_threshold_Ky,
    }

    water_level_fluctuation_json = {
        "dem-id": request_json["bench-id"],
        "section-geometry": request_json["section-geometry"],
        "segment": segment,
        "year": year,
        "set": set,
        "water-qs": mapped_water_qs,
        "tidal-level": mapped_tidal_level,
        "risk-threshold": risk_threshold_Zd,
    }

    soil_composition_mcr = model.launcher.fetch_model_from_API(
        config.API_MI_SOIL_COMPOSITION
    ).run(soil_composition_json, other_dependent_ids)
    slope_protection_mcr = model.launcher.fetch_model_from_API(
        config.API_MI_SLOPE_PROTECTION
    ).run(slope_protection_json, other_dependent_ids)
    load_control_mcr = model.launcher.fetch_model_from_API(
        config.API_MI_LOAD_CONTROL
    ).run(load_control_json, other_dependent_ids)
    height_difference_mcr = model.launcher.fetch_model_from_API(
        config.API_MI_HEIGHT_DIFFERENCE
    ).run(height_difference_json, other_dependent_ids)
    slope_rate_mcr = model.launcher.fetch_model_from_API(config.API_MI_SLOPE_RATE).run(
        slope_rate_json, other_dependent_ids
    )
    nearshore_flush_mcr = model.launcher.fetch_model_from_API(
        config.API_MI_NEARSHORE_FLUSH
    ).run(nearshore_flush_json, other_dependent_ids)
    flow_equivalent_mcr = model.launcher.fetch_model_from_API(
        config.API_MI_FLOW_EQUIVALENT
    ).run(flow_equivalent_json, other_dependent_ids)
    anti_impact_speed_mcr = model.launcher.fetch_model_from_API(
        config.API_MI_ANTI_IMPACT_SPEED
    ).run(anti_impact_speed_json, other_dependent_ids)
    water_level_fluctuation_mcr = model.launcher.fetch_model_from_API(
        config.API_MI_WATER_LEVEL_FLUCTUATION
    ).run(water_level_fluctuation_json, other_dependent_ids)
    risk_level_mcr = model.ModelCaseReference.create(
        config.API_MI_RISK_LEVEL,
        request_json,
        NAME,
        model_path,
        other_dependent_ids
        + [
            soil_composition_mcr.id,
            slope_protection_mcr.id,
            load_control_mcr.id,
            height_difference_mcr.id,
            slope_rate_mcr.id,
            nearshore_flush_mcr.id,
            flow_equivalent_mcr.id,
            anti_impact_speed_mcr.id,
            water_level_fluctuation_mcr.id,
        ],
    )

    return [
        soil_composition_mcr,
        slope_protection_mcr,
        load_control_mcr,
        height_difference_mcr,
        slope_rate_mcr,
        nearshore_flush_mcr,
        flow_equivalent_mcr,
        anti_impact_speed_mcr,
        water_level_fluctuation_mcr,
        risk_level_mcr,
    ]


def RESPONSING(
    self,
    core_mcr: model.ModelCaseReference,
    default_pre_mcrs: list[model.ModelCaseReference],
    other_pre_mcrs: list[model.ModelCaseReference],
):
    soil_composition_mcr = default_pre_mcrs[0]
    slope_protection_mcr = default_pre_mcrs[1]
    load_control_mcr = default_pre_mcrs[2]
    height_difference_mcr = default_pre_mcrs[3]
    slope_rate_mcr = default_pre_mcrs[4]
    nearshore_flush_mcr = default_pre_mcrs[5]
    flow_equivalent_mcr = default_pre_mcrs[6]
    anti_impact_speed_mcr = default_pre_mcrs[7]
    water_level_fluctuation_mcr = default_pre_mcrs[8]

    return core_mcr.make_response(
        {
            "case-id": "TEMPLATE",
            "multi-indicator-ids": {
                "Dsed": soil_composition_mcr.id,
                "PL": slope_protection_mcr.id,
                "LC": load_control_mcr.id,
                "Zb": height_difference_mcr.id,
                "Sa": slope_rate_mcr.id,
                "Ln": nearshore_flush_mcr.id,
                "PQ": flow_equivalent_mcr.id,
                "Ky": anti_impact_speed_mcr.id,
                "Zd": water_level_fluctuation_mcr.id,
            },
            "result": "NONE",
            "risk-level": "NONE",
        }
    )


##########################################################################################

if __name__ == "__main__":
    v1 = sys.argv[1]
    v2 = sys.argv[2]
    v3 = sys.argv[3]
    v4 = sys.argv[4]
    v5 = sys.argv[5]
    v6 = sys.argv[6]
    v7 = sys.argv[7]
    v8 = sys.argv[8]
    v9 = sys.argv[9]
    v10 = sys.argv[10]

    soil_composition_mcr = model.ModelCaseReference.open_case(v1)
    slope_protection_mcr = model.ModelCaseReference.open_case(v2)
    load_control_mcr = model.ModelCaseReference.open_case(v3)
    height_difference_mcr = model.ModelCaseReference.open_case(v4)
    slope_rate_mcr = model.ModelCaseReference.open_case(v5)
    nearshore_flush_mcr = model.ModelCaseReference.open_case(v6)
    flow_equivalent_mcr = model.ModelCaseReference.open_case(v7)
    anti_impact_speed_mcr = model.ModelCaseReference.open_case(v8)
    water_level_fluctuation_mcr = model.ModelCaseReference.open_case(v9)
    risk_level_mcr = model.ModelCaseReference.open_case(v10)

    # Run model case (Risk Level)
    run_risk_level_mcr(risk_level_mcr)

    # import time
    # retries = 0
    # max_retries = 100
    # delay = 10
    # while retries < max_retries:
    #     try:
    #         # use run_mcr(sync) to make sure the launch(async) in 'controllers' is finished
    #         model.run_soil_composition_mcr(soil_composition_mcr)
    #         model.run_slope_protection_mcr(slope_protection_mcr)
    #         model.run_load_control_mcr(load_control_mcr)
    #         model.run_height_difference_mcr(height_difference_mcr)
    #         model.run_slope_rate_mcr(slope_rate_mcr)
    #         model.run_nearshore_flush_mcr(nearshore_flush_mcr)
    #         model.run_flow_equivalent_mcr(flow_equivalent_mcr)
    #         model.run_anti_impact_speed_mcr(anti_impact_speed_mcr)
    #         model.run_water_level_fluctuation_mcr(water_level_fluctuation_mcr)

    #         print("所有模型全部成功运行完毕！")
    #         run_risk_level_mcr(risk_level_mcr)
    #         break

    #     except Exception as e:
    #         retries += 1
    #         print(f"第 {retries} 轮查询: 仍在运行中")
    #         if retries < max_retries:
    #             print(f"{delay} 秒后重试...")
    #             time.sleep(delay)
    #         else:
    #             print("达到最大重试次数，终止尝试。")
    #             raise
