import os
import time
import threading

# Set PROJ_LIB to empty string to avoid conflicts with other PROJ installations (e.g. PostGIS)
os.environ["PROJ_LIB"] = ""

import config
import uvicorn
from model import launcher
from model.modelRecovery import format_recovery_summary, recover_stale_cases
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

    # 启动自愈：上一个进程被杀掉后，它认领过、却没跑完的 case 会永久停在 LOCK，
    # `/v0/mc/status` 对它恒回 pending，网关只能空转到超时。这里把它重置成 UNLOCK。
    # 必须在 daemon 起来之前做——这一刻本进程不可能持有任何锁，凡是有过期嫌疑的
    # 锁都是上一个进程的遗留，不会误伤正在计算的 case（"真在跑"的有监控心跳）。
    try:
        print(format_recovery_summary(recover_stale_cases()), flush=True)
    except Exception as exc:  # 自愈失败不能挡住启动
        print(f"[case-recovery] 启动自愈失败（已忽略）: {exc}", flush=True)

    for key in config.MODEL_REGISTRY:
        launcher.preheat(key)

    StorageMonitor().initialize([config.DIR_ROOT], config.DIR_STORAGE_LOG)

    start_case_watchdog()


def start_case_watchdog():
    """常驻巡检：服务不重启时，也能救回 `LOCK|RUNNING` 的半死 case。

    覆盖的场景是「daemon 进程死了 / 子进程被外部杀掉，但 uvicorn 还活着」：
    这类 case 的监控线程随之消失，不会再有人把它推进到终态，而服务不重启就
    不会跑到启动自愈。只处理 `LOCK|RUNNING`（真的进入过包装器的），
    普通 `LOCK` 由请求路径上的 `try_recover_stale_lock` 精确判定，避免误伤排队中的 case。
    """
    interval = config.MODEL_CASE_WATCHDOG_SECONDS
    if interval <= 0:
        return None

    def loop():
        while True:
            time.sleep(interval)
            try:
                summary = recover_stale_cases(include_lock=False)
                if summary["recovered"]:
                    print(format_recovery_summary(summary), flush=True)
            except Exception as exc:  # 巡检异常绝不能把线程打死
                print(f"[case-watchdog] 巡检失败（已忽略）: {exc}", flush=True)

    thread = threading.Thread(target=loop, name="case-watchdog", daemon=True)
    thread.start()
    return thread


def start_kafka_worker(assigned_banks=None):
    """
    启动 Kafka Worker（如果启用）

    Args:
        assigned_banks: 期望由本 Worker 处理的岸段 ID 列表。
                        仅用于部署自检（收到名单外岸段时告警），不参与调度；
                        实际范围由 Kafka 分区分配决定（生产者以 bankId 为 key）。
    """
    if not config.KAFKA_ENABLED:
        print("[kafka] Kafka 未启用，跳过启动 Worker")
        return None, None

    try:
        from kafka.kafka_worker import start_worker_in_background

        worker, thread = start_worker_in_background(assigned_banks=assigned_banks)
        print(f"[kafka] Kafka Worker 已在后台启动")
        if assigned_banks:
            print(f"[kafka] 期望岸段（仅自检，不用于过滤）: {assigned_banks}")
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
        help="【仅自检，不再过滤】Kafka Worker 期望处理的岸段 ID，逗号分隔，例如: BANK_001,BANK_002",
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
