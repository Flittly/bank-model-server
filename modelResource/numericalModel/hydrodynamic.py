import os
import re
import sys
import json

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


##########################################################################################


@model.model_status_controller_sync
def run_hydrodynamic_mcr(mcr: model.ModelCaseReference):
    segment = mcr.request_json["segment"]
    year = mcr.request_json["year"]
    set = mcr.request_json["set"]
    name = mcr.request_json["water-qs"] + mcr.request_json["tidal-level"]
    mapped_path = f"hydrodynamic/{segment}/{year}/{set}/{name}"

    raw_output_path = f"{mapped_path}/raw/"
    visualization_output_path = f"{mapped_path}/renderResource/"

    content = []
    bin_content = []
    for i in range(26):
        content.append(os.path.join(raw_output_path, f"{i}.txt"))
        bin_content.append(os.path.join(visualization_output_path, f"uv_{i}.bin"))

    return {
        "case-id": mcr.id,
        "raw-txts": content,
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
            "raw-txts": "NONE",
            "visualization-uv-bin": "NONE",
            "visualization-station-bin": "NONE",
            "visualization-description-json": "NONE",
        }
    )


##########################################################################################

if __name__ == "__main__":
    v1 = sys.argv[1]  # hydrodynamic model case id

    hydrodynamic_mcr = model.ModelCaseReference.open_case(v1)

    # Run model case (Hydrodynamic)
    run_hydrodynamic_mcr(hydrodynamic_mcr)
