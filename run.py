import os

# Set PROJ_LIB to empty string to avoid conflicts with other PROJ installations (e.g. PostGIS)
os.environ["PROJ_LIB"] = ""

import config
import uvicorn
from model import launcher
from app import create_app
from util import StorageMonitor
from util.gdal_env import configure_gdal_proj_env


configure_gdal_proj_env()


def initialize_work_space():
    os.makedirs(config.DIR_MODEL_CASE, exist_ok=True)
    os.makedirs(os.path.dirname(config.DIR_STORAGE_LOG), exist_ok=True)

    if not os.path.exists(config.DIR_GLOBALE_FILE_LOCKER):
        with open(config.DIR_GLOBALE_FILE_LOCKER, "w") as file:
            pass

    if not os.path.exists(config.DIR_STORAGE_LOG):
        with open(config.DIR_STORAGE_LOG, "w", encoding="utf-8") as file:
            file.write("0\n")

    for key in config.MODEL_REGISTRY:
        launcher.preheat(key)

    StorageMonitor().initialize([config.DIR_ROOT], config.DIR_STORAGE_LOG)


if __name__ == "__main__":
    initialize_work_space()

    app = create_app()

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=config.APP_PORT,
        log_level="debug" if config.APP_DEBUG else "info",
    )
