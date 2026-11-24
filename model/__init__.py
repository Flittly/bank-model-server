import os
import subprocess

import config

from .hydrodynamic_resource import generate_hydrodynamic_resource
from .caseEntry import EXIT_CASE_UNAVAILABLE, require_cases
from .modelCaseReference import ModelCaseReference
from .modelLauncher import ModelLauncher as launcher
from .modelLauncher import model_status_controller_sync


def launch_hydrodynamic_resource_generate(
    segment: str,
    year: str,
    set_name: str,
    name: str,
    temp: bool,
    boundary: str,
) -> None:
    command = [
        "python",
        os.path.join(config.DIR_MODEL, "hydrodynamic_resource.py"),
        segment,
        year,
        set_name,
        name,
        str(temp),
        boundary,
    ]
    subprocess.Popen(command)
