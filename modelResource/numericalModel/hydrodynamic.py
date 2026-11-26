import os
import sys
import json

module_path = os.path.abspath(os.path.join(os.getcwd()))
if module_path not in sys.path:
    sys.path.append(module_path)

import util
import model
import config
from util import db_ops
from util.hydro_map import map_water_qs

# 兼容旧名：PARSING 与 tests/test_flow_equivalent.py 都按这个名字引用/打桩。
get_mapped_tidal_level_and_mapped_water_qs = map_water_qs


##########################################################################################


@model.model_status_controller_sync
def run_hydrodynamic_mcr(mcr: model.ModelCaseReference):
    segment = mcr.request_json["segment"]
    year = mcr.request_json["year"]
    set = mcr.request_json["set"]
    name = mcr.request_json["water-qs"] + mcr.request_json["tidal-level"]
    mapped_path = f"hydrodynamic/{segment}/{year}/{set}/{name}"

    visualization_output_path = f"{mapped_path}/renderResource/"

    bin_content = []
    for i in range(26):
        bin_content.append(os.path.join(visualization_output_path, f"uv_{i}.bin"))

    return {
        "case-id": mcr.id,
        "segment": segment,
        "year": year,
        "set": set,
        "water-qs": mcr.request_json["water-qs"],
        "tidal-level": mcr.request_json["tidal-level"],
        "data-source": "database",
        "visualization-uv-bin": bin_content,
        "visualization-station-bin": os.path.join(
            visualization_output_path, "station.bin"
        ),
        "visualization-description-json": os.path.join(
            visualization_output_path, "flow_field_description.json"
        ),
    }


##########################################################################################

NAME = "Hydrodynamic"

CATEGORY = "Numerical Model"

CATEGORY_ALIAS = "nm"


def PARSING(
    self, request_json: dict, model_path: str, other_dependent_ids: list[str] = []
):
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

    # Identify all required mcrs
    hydrodynamic_mcr = model.ModelCaseReference.create(
        config.API_NM_HYDRODYNAMIC, map_json, NAME, model_path, other_dependent_ids
    )

    return [hydrodynamic_mcr]


def RESPONSING(
    self,
    core_mcr: model.ModelCaseReference,
    default_pre_mcrs: list[model.ModelCaseReference],
    other_pre_mcrs: list[model.ModelCaseReference],
):
    return core_mcr.make_response(
        {
            "case-id": "TEMPLATE",
            "segment": "NONE",
            "year": "NONE",
            "set": "NONE",
            "water-qs": "NONE",
            "tidal-level": "NONE",
            "data-source": "NONE",
            "visualization-uv-bin": "NONE",
            "visualization-station-bin": "NONE",
            "visualization-description-json": "NONE",
        }
    )


##########################################################################################

if __name__ == "__main__":
    v1 = sys.argv[1]  # hydrodynamic model case id

    model.require_cases([v1], NAME)

    hydrodynamic_mcr = model.ModelCaseReference.open_case(v1)

    # Run model case (Hydrodynamic)
    run_hydrodynamic_mcr(hydrodynamic_mcr)
