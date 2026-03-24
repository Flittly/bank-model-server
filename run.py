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
    os.makedirs(config.DIR_TERRAIN_CACHE, exist_ok=True)  # 创建地形数据缓存目录

    if not os.path.exists(config.DIR_GLOBALE_FILE_LOCKER):
        with open(config.DIR_GLOBALE_FILE_LOCKER, "w") as file:
            pass

    if not os.path.exists(config.DIR_STORAGE_LOG):
        with open(config.DIR_STORAGE_LOG, "w", encoding="utf-8") as file:
            file.write("0\n")

    for key in config.MODEL_REGISTRY:
        launcher.preheat(key)

    StorageMonitor().initialize([config.DIR_ROOT], config.DIR_STORAGE_LOG)


def start_kafka_worker(assigned_banks=None):
    """
    启动 Kafka Worker（如果启用）

    Args:
        assigned_banks: 分配给此 Worker 的岸段 ID 列表
    """
    if not config.KAFKA_ENABLED:
        print("[kafka] Kafka 未启用，跳过启动 Worker")
        return None, None

    try:
        from kafka.kafka_worker import start_worker_in_background

        worker, thread = start_worker_in_background(assigned_banks=assigned_banks)
        print(f"[kafka] Kafka Worker 已在后台启动")
        if assigned_banks:
            print(f"[kafka] 分配岸段: {assigned_banks}")
        return worker, thread
    except Exception as e:
        print(f"[kafka] Kafka Worker 启动失败: {e}")
        return None, None


if __name__ == "__main__":
    import argparse

    # 解析命令行参数
    parser = argparse.ArgumentParser(description="Bank Model Server")
    parser.add_argument(
        "--banks",
        type=str,
        default="",
        help="Kafka Worker 分配的岸段 ID，逗号分隔，例如: BANK_001,BANK_002",
    )
    args = parser.parse_args()

    # 解析岸段列表
    assigned_banks = (
        [b.strip() for b in args.banks.split(",") if b.strip()] if args.banks else None
    )

    initialize_work_space()

    # 启动 Kafka Worker
    kafka_worker, kafka_thread = start_kafka_worker(assigned_banks=assigned_banks)

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
