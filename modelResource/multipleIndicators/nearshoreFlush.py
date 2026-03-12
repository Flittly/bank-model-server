import os
import sys

module_path = os.path.abspath(os.path.join(os.getcwd()))
if module_path not in sys.path:
    sys.path.append(module_path)

import model
import config
import json
from datetime import datetime


def parse_timepoint(value):
    text = str(value).strip()
    for fmt in ("%Y-%m-%d", "%Y-%m", "%Y%m%d", "%Y%m"):
        try:
            parsed = datetime.strptime(text, fmt)
            if fmt in ("%Y-%m", "%Y%m"):
                return parsed.replace(day=1)
            return parsed
        except ValueError:
            continue
    raise ValueError(f"Unsupported timepoint format: {value!r}")


def months_difference(date1_str, date2_str):
    date1 = parse_timepoint(date1_str)
    date2 = parse_timepoint(date2_str)

    # 计算年和月的差异
    year_diff = date1.year - date2.year
    month_diff = date1.month - date2.month

    # 总的月份差异
    total_month_diff = year_diff * 12 + month_diff
    return total_month_diff


def compute_nearshore_flush(
    section_view_output_path, risk_threshold, current_timepoint, comparison_timepoint
):
    section_json = os.path.join(section_view_output_path, "section.json")
    risk = []
    if risk_threshold == "NONE":
        with open(
            config.DIR_RESOURCE_RISKLEVEL_THRESHOLD_TEMPLATE, "r", encoding="utf-8"
        ) as file:
            risk_threshold = json.load(file).get("Ln")
    with open(section_json, "r", encoding="utf-8") as file:
        data = json.load(file)
        points = data.get("points")
        deepest_index = data.get("deepest_index")
        min_z = points[deepest_index][2]
        time = months_difference(current_timepoint, comparison_timepoint)
        Ln = -min_z / time

        if Ln < risk_threshold[0]:
            risk = [1, 0, 0, 0]
        elif Ln < risk_threshold[1]:
            risk = [0, 1, 0, 0]
        elif Ln < risk_threshold[2]:
            risk = [0, 0, 1, 0]
        elif Ln > risk_threshold[2]:
            risk = [0, 0, 0, 1]
        return Ln, risk


##########################################################################################


@model.model_status_controller_sync
def run_nearshore_flush_mcr(mcr: model.ModelCaseReference):
    section_view_output_path = os.path.join(section_view_mcr.directory, "result")
    risk_threshold = mcr.request_json["risk-threshold"]
    current_timepoint = mcr.request_json["current-timepoint"]
    comparison_timepoint = mcr.request_json["comparison-timepoint"]

    Ln, risk = compute_nearshore_flush(
        section_view_output_path,
        risk_threshold,
        current_timepoint,
        comparison_timepoint,
    )

    return {"case-id": mcr.id, "Ln": Ln, "risk-level": risk}


##########################################################################################

NAME = "Nearshore Flush"

CATEGORY = "Multiple Indicators"

CATEGORY_ALIAS = "mi"


def PARSING(
    self, request_json: dict, model_path: str, other_dependent_ids: list[str] = []
):
    nearshore_flush_json = request_json

    global_json = {
        "bench-id": nearshore_flush_json["bench-id"],
        "ref-id": nearshore_flush_json["ref-id"],
        "region-geometry": "NONE",
    }
    global_mcr = model.launcher.fetch_model_from_API(config.API_RE_REGION_FLUSH).run(
        global_json, other_dependent_ids
    )

    section_view_json = {
        "dem-id": os.path.join(global_mcr.directory, "result", "flush.tif"),
        "section-geometry": nearshore_flush_json["section-geometry"],
    }
    section_view_mcr = model.launcher.fetch_model_from_API(
        config.API_RE_SECTION_VIEW
    ).run(section_view_json, other_dependent_ids + [global_mcr.id])

    nearshore_flush_mcr = model.ModelCaseReference.create(
        config.API_MI_NEARSHORE_FLUSH,
        nearshore_flush_json,
        NAME,
        model_path,
        other_dependent_ids + [global_mcr.id, section_view_mcr.id],
    )

    return [global_mcr, section_view_mcr, nearshore_flush_mcr]


def RESPONSING(
    self,
    core_mcr: model.ModelCaseReference,
    default_pre_mcrs: list[model.ModelCaseReference],
    other_pre_mcrs: list[model.ModelCaseReference],
):
    return core_mcr.make_response(
        {"case-id": "TEMPLATE", "Ln": "NONE", "risk-level": "NONE"}
    )


##########################################################################################

if __name__ == "__main__":
    v1 = sys.argv[1]  # global model case id
    v2 = sys.argv[2]  # section view model case id
    v3 = sys.argv[3]  # nearshore flush model case id

    global_mcr = model.ModelCaseReference.open_case(v1)
    section_view_mcr = model.ModelCaseReference.open_case(v2)
    nearshore_flush_mcr = model.ModelCaseReference.open_case(v3)

    # Run model case (Nearshore Flush)
    run_nearshore_flush_mcr(nearshore_flush_mcr)
