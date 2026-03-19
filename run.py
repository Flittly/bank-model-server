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


def start_kafka_worker():
    """启动 Kafka Worker（如果启用）"""
    if not config.KAFKA_ENABLED:
        print("[kafka] Kafka 未启用，跳过启动 Worker")
        return None, None

    try:
        from kafka.kafka_worker import start_worker_in_background

        worker, thread = start_worker_in_background()
        print(f"[kafka] Kafka Worker 已在后台启动")
        return worker, thread
    except Exception as e:
        print(f"[kafka] Kafka Worker 启动失败: {e}")
        return None, None


if __name__ == "__main__":
    initialize_work_space()

    # 启动 Kafka Worker
    kafka_worker, kafka_thread = start_kafka_worker()

    app = create_app()

    try:
        uvicorn.run(
            app,
            host="0.0.0.0",
            port=config.APP_PORT,
            log_level="debug" if config.APP_DEBUG else "info",
        )
    finally:
        # 优雅关闭 Kafka Worker
        if kafka_worker is not None:
            print("[kafka] 正在关闭 Kafka Worker...")
            kafka_worker.close()
