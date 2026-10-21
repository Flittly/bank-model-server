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


def _one_hot_tier(column):
    """从某个指标在 4 个档位上的取值（如 [0,0,1,0]）反推它落在第几档；全 0 返回 None。"""
    values = [float(v) for v in column]
    if not any(values):
        return None
    return values.index(max(values)) + 1


def get_risk_level(response_list, wRE, wNM, wGE, wRL, risk_threshold, request_json=None):
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
        soil_composition_data = json.load(file)
        soil_composition_risk_level_matrix = np.array(
            [soil_composition_data.get("risk-level")]
        )
        raw_Dsed = soil_composition_data.get("Dsed")
    with open(slope_protection_response, "r", encoding="utf-8") as file:
        slope_protection_data = json.load(file)
        slope_protection_risk_level_matrix = np.array(
            [slope_protection_data.get("risk-level")]
        )
        # PL/LC 是枚举输入，早期 Slope Protection / Load Control 的响应只回 risk-level、
        # 不回带原始枚举，导致这里恒为 None。旧 case 重跑时从请求入参回退，
        # 新 case 直接用模型回带的字段。
        raw_PL = slope_protection_data.get("PL")
        if raw_PL is None and isinstance(request_json, dict):
            raw_PL = request_json.get("protection-level")
    with open(load_control_mcr_response, "r", encoding="utf-8") as file:
        load_control_data = json.load(file)
        load_control_risk_level_matrix = np.array([load_control_data.get("risk-level")])
        raw_LC = load_control_data.get("LC")
        if raw_LC is None and isinstance(request_json, dict):
            raw_LC = request_json.get("control-level")
    with open(height_difference_response, "r", encoding="utf-8") as file:
        height_difference_data = json.load(file)
        height_difference_risk_level_matrix = np.array(
            [height_difference_data.get("risk-level")]
        )
        raw_Zb = height_difference_data.get("Zb")
    with open(slope_rate_response, "r", encoding="utf-8") as file:
        slope_rate_data = json.load(file)
        slope_rate_risk_level_matrix = np.array([slope_rate_data.get("risk-level")])
        raw_Sa = slope_rate_data.get("Sa")
    with open(nearshore_flush_mcr_response, "r", encoding="utf-8") as file:
        nearshore_flush_data = json.load(file)
        nearshore_flush_risk_level_matrix = np.array(
            [nearshore_flush_data.get("risk-level")]
        )
        raw_Ln = nearshore_flush_data.get("Ln")
    with open(flow_equivalent_mcr_response, "r", encoding="utf-8") as file:
        flow_equivalent_data = json.load(file)
        flow_equivalent_risk_level_matrix = np.array(
            [flow_equivalent_data.get("risk-level")]
        )
        raw_PQ = flow_equivalent_data.get("PQ")
    with open(anti_impact_speed_response, "r", encoding="utf-8") as file:
        anti_impact_speed_data = json.load(file)
        anti_impact_speed_risk_level_matrix = np.array(
            [anti_impact_speed_data.get("risk-level")]
        )
        raw_Ky = anti_impact_speed_data.get("Ky")
    with open(water_level_fluctuation_response, "r", encoding="utf-8") as file:
        water_level_fluctuation_data = json.load(file)
        water_level_fluctuation_risk_level_matrix = np.array(
            [water_level_fluctuation_data.get("risk-level")]
        )
        raw_Zd = water_level_fluctuation_data.get("Zd")

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

    matrices = {
        "water_flow_power_matrix": water_flow_power_matrix.tolist(),
        "riverbed_evolution_matrix": riverbed_evolution_matrix.tolist(),
        "geology_engineering_matrix": geology_engineering_matrix.tolist(),
        "concat_matrix": concat_matrix.tolist(),
        "water_flow_power_result_matrix": water_flow_power_result_matrix.tolist(),
        "riverbed_evolution_result_matrix": riverbed_evolution_result_matrix.tolist(),
        "geology_engineering_result_matrix": geology_engineering_result_matrix.tolist(),
        "weight_matrix": weight_matrix.tolist(),
        "result_matrix": result_matrix.tolist(),
        "risk_value_components": [result_matrix[2][0], result_matrix[3][0]],
    }

    raw_values = {
        "Dsed": raw_Dsed,
        "PL": raw_PL,
        "LC": raw_LC,
        "Zb": raw_Zb,
        "Sa": raw_Sa,
        "Ln": raw_Ln,
        "PQ": raw_PQ,
        "Ky": raw_Ky,
        "Zd": raw_Zd,
    }

    # 输入完备度：回答「这 9 个指标里到底有几个真的在区分断面」。
    # 单断面只能给出本断面的档位；跨断面的「是否为常量」由统计层按 indicator_tiers 判断。
    indicator_tiers = {
        "Ky": _one_hot_tier(water_flow_power_matrix[:, 0]),
        "PQ": _one_hot_tier(water_flow_power_matrix[:, 1]),
        "Zd": _one_hot_tier(water_flow_power_matrix[:, 2]),
        "Sa": _one_hot_tier(riverbed_evolution_matrix[:, 0]),
        "Ln": _one_hot_tier(riverbed_evolution_matrix[:, 1]),
        "Zb": _one_hot_tier(riverbed_evolution_matrix[:, 2]),
        "Dsed": _one_hot_tier(geology_engineering_matrix[:, 0]),
        "PL": _one_hot_tier(geology_engineering_matrix[:, 1]),
        "LC": _one_hot_tier(geology_engineering_matrix[:, 2]),
    }
    matrices["indicator_tiers"] = indicator_tiers
    matrices["input_completeness"] = {
        "total_indicators": len(indicator_tiers),
        "resolved_indicators": sum(1 for t in indicator_tiers.values() if t is not None),
        "missing_raw_values": sorted(k for k, v in raw_values.items() if v is None),
        "enum_inputs": ["PL", "LC"],
    }
    # PQ 溯源：用了哪一年、哪个文件、是否精确命中。全库 PQ 恒为 0.0 时，
    # 只有把这三件事落进结果，才能区分「数据没接上」和「这一年的当量本来就是 0」。
    if isinstance(flow_equivalent_data, dict) and flow_equivalent_data.get("data-quality"):
        matrices["PQ_data_quality"] = flow_equivalent_data["data-quality"]

    return risk_value[0], risk, matrices, raw_values


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

    result, risk_level, matrices, raw_values = get_risk_level(
        response_list, wRE, wNM, wGE, wRL, risk_threshold, mcr.request_json
    )

    input_thresholds = mcr.request_json.get("risk-thresholds")
    resolved_sub = input_thresholds if isinstance(input_thresholds, dict) else None

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
        "matrices": matrices,
        "raw_values": raw_values,
        "thresholds": {
            "risk_threshold": risk_threshold if risk_threshold != "NONE" else None,
            "wRE": wRE if wRE != "NONE" else None,
            "wNM": wNM if wNM != "NONE" else None,
            "wGE": wGE if wGE != "NONE" else None,
            "wRL": wRL if wRL != "NONE" else None,
            "sub_thresholds": resolved_sub,
        },
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

    # PQ 用断面自身的岸段与年份，必须在映射前留一份。
    # get_mapped_* 为了对齐水动力工况会把 segment 改成 DEFAULT_DOMAIN、把 year 改成 "default"
    # （该岸段/潮位组合没有水动力节点时），那是「工况坐标」，不是「断面的年份」。
    # 之前直接把映射后的 year 传给 PQ，导致 2018 年的断面拿到 "default" 再被字符串夹取成 2023，
    # 于是全库 PQ 恒为 pq.json["2023"] = 0.0。
    pq_segment = segment
    pq_year = year

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
        # 用断面原始岸段/年份（映射前），不要用对齐工况后的 segment/year
        "segment": pq_segment,
        "year": pq_year,
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
