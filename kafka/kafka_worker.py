import json
import sys
import os

# 确保 kafka 目录在路径中，以便导入 kafka_executor
kafka_dir = os.path.dirname(os.path.abspath(__file__))
if kafka_dir not in sys.path:
    sys.path.insert(0, kafka_dir)

# 确保项目根目录在路径中
project_root = os.path.dirname(kafka_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import config
from confluent_kafka import Consumer, Producer

from kafka_executor import execute_risk_level_task


class TerrainCache:
    """地形数据缓存管理器"""

    def __init__(self, cache_dir=None):
        self.cache_dir = cache_dir or config.DIR_TERRAIN_CACHE
        os.makedirs(self.cache_dir, exist_ok=True)
        self._cache = {}  # terrain_key -> local_path

    def get_local_path(self, terrain_bucket: str, terrain_key: str) -> str:
        """获取地形数据的本地路径，如果未缓存则从 RustFS 下载"""

        # 检查内存缓存
        if terrain_key in self._cache:
            local_path = self._cache[terrain_key]
            if os.path.exists(local_path):
                print(f"[terrain-cache] 命中缓存: {terrain_key}", flush=True)
                return local_path

        # 生成本地路径
        safe_key = terrain_key.replace("/", "_").replace("\\", "_")
        local_path = os.path.join(self.cache_dir, safe_key)

        # 检查磁盘缓存
        if os.path.exists(local_path):
            print(f"[terrain-cache] 磁盘缓存命中: {terrain_key}", flush=True)
            self._cache[terrain_key] = local_path
            return local_path

        # 从 RustFS 下载
        print(
            f"[terrain-cache] 从 RustFS 下载: {terrain_bucket}/{terrain_key}",
            flush=True,
        )
        self._download_from_rustfs(terrain_bucket, terrain_key, local_path)

        self._cache[terrain_key] = local_path
        return local_path

    def _download_from_rustfs(self, bucket: str, key: str, local_path: str):
        """从 RustFS 下载文件"""
        try:
            import boto3
            from botocore.client import Config

            s3_client = boto3.client(
                "s3",
                endpoint_url=config.RUSTFS_ENDPOINT,
                aws_access_key_id=config.RUSTFS_ACCESS_KEY,
                aws_secret_access_key=config.RUSTFS_SECRET_KEY,
                region_name=config.RUSTFS_REGION,
                use_ssl=config.RUSTFS_SECURE,
                config=Config(signature_version="s3v4"),
            )

            s3_client.download_file(bucket, key, local_path)
            print(f"[terrain-cache] 下载完成: {local_path}", flush=True)

        except Exception as e:
            print(f"[terrain-cache] 下载失败: {e}", flush=True)
            raise

    def clear_cache(self):
        """清空缓存"""
        self._cache.clear()


class KafkaModelWorker:
    def __init__(self, assigned_banks=None) -> None:
        """
        初始化 Kafka Worker

        Args:
            assigned_banks: 分配给此 Worker 的岸段 ID 列表
                           如果为 None，则处理所有岸段
        """
        self.worker_id = config.KAFKA_WORKER_ID
        self.bootstrap_servers = config.KAFKA_BOOTSTRAP_SERVERS
        self.task_topic = config.KAFKA_TASK_TOPIC
        self.result_topic = config.KAFKA_RESULT_TOPIC
        self._running = True

        # 分配的岸段（为空则处理所有岸段）
        self.assigned_banks = assigned_banks or config.KAFKA_ASSIGNED_BANKS
        if self.assigned_banks:
            print(f"[kafka-worker] 只处理岸段: {self.assigned_banks}", flush=True)

        # 地形数据缓存
        self.terrain_cache = TerrainCache()
        self.current_bank_id = None
        self.current_terrain_path = None

        # 当前处理的任务信息
        self.current_run_id = None
        self.current_task_id = None

        # Kafka Consumer
        self.consumer = Consumer(
            {
                "bootstrap.servers": ",".join(self.bootstrap_servers),
                "group.id": config.KAFKA_WORKER_GROUP,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "broker.address.family": "v4",  # 强制使用 IPv4，避免 Windows IPv6 连接问题
            }
        )
        self.consumer.subscribe([self.task_topic])

        # Kafka Producer
        self.producer = Producer(
            {
                "bootstrap.servers": ",".join(self.bootstrap_servers),
                "acks": "all",
                "broker.address.family": "v4",  # 强制使用 IPv4，避免 Windows IPv6 连接问题
            }
        )

    def run(self) -> None:
        print(f"[kafka-worker] 启动 worker_id={self.worker_id}", flush=True)
        print(f"[kafka-worker] bootstrap_servers={self.bootstrap_servers}", flush=True)
        print(f"[kafka-worker] task_topic={self.task_topic}", flush=True)
        print(f"[kafka-worker] result_topic={self.result_topic}", flush=True)

        if self.assigned_banks:
            print(f"[kafka-worker] 分配岸段: {self.assigned_banks}", flush=True)

        while self._running:
            msg = self.consumer.poll(timeout=1.0)

            if msg is None:
                continue
            if msg.error():
                print(f"[kafka-worker] 消费错误 error={msg.error()}", flush=True)
                continue

            task = json.loads(msg.value().decode("utf-8"))
            bank_id = task.get("bankId", "")
            section_id = task.get("sectionId", "")
            run_id = task.get("runId", "")
            task_id = task.get("taskId", "")

            # 检查是否应该处理此岸段
            if self.assigned_banks and bank_id not in self.assigned_banks:
                print(f"[kafka-worker] 跳过非分配岸段: bankId={bank_id}", flush=True)
                # 不提交偏移量，让其他 Worker 处理
                # 注意：这种方式需要手动管理分区分配
                continue

            print(
                f"[kafka-worker] 收到任务 runId={run_id} taskId={task_id} sectionId={section_id} bankId={bank_id}",
                flush=True,
            )

            try:
                # 加载地形数据（如果是新岸段）
                terrain_path = self._load_terrain_if_needed(task)

                # 执行计算
                result = execute_risk_level_task(task, self.worker_id, terrain_path)

                # 发送结果
                self._send_result(result)

                # 提交偏移量
                self.consumer.commit(msg)

            except Exception as e:
                print(f"[kafka-worker] 处理任务失败: {e}", flush=True)
                # 可以选择发送到死信队列或重新抛出让 Kafka 重试
                raise

    def _load_terrain_if_needed(self, task: dict) -> str:
        """如果是新岸段，加载地形数据"""
        bank_id = task.get("bankId", "")
        terrain_bucket = task.get("terrainBucket", config.RUSTFS_BUCKET)
        terrain_key = task.get("terrainKey", "")

        # 如果没有地形文件信息，返回 None
        if not terrain_key:
            print(f"[kafka-worker] 警告: 任务没有地形文件信息", flush=True)
            return None

        # 如果是同一岸段，复用已加载的数据
        if bank_id == self.current_bank_id and self.current_terrain_path:
            return self.current_terrain_path

        # 新岸段，释放旧数据，加载新数据
        print(f"[kafka-worker] 加载新岸段地形数据: bankId={bank_id}", flush=True)
        self.current_terrain_path = self.terrain_cache.get_local_path(
            terrain_bucket, terrain_key
        )
        self.current_bank_id = bank_id

        return self.current_terrain_path

    def _send_result(self, result: dict) -> None:
        key = f"{result['runId']}:{result['sectionId']}"

        def delivery_report(err, msg):
            if err is not None:
                print(f"[kafka-worker] 结果发送失败 error={err}", flush=True)
            else:
                print(
                    f"[kafka-worker] 结果发送成功 topic={msg.topic()} partition={msg.partition()}",
                    flush=True,
                )

        self.producer.produce(
            self.result_topic,
            key=key,
            value=json.dumps(result, ensure_ascii=False).encode("utf-8"),
            on_delivery=delivery_report,
        )
        self.producer.poll(0)
        self.producer.flush()

    def stop(self) -> None:
        """停止 Worker"""
        self._running = False

    def close(self) -> None:
        """关闭连接"""
        self._running = False
        if self.consumer:
            self.consumer.close()
        if self.producer:
            self.producer.flush()


def start_worker_in_background(assigned_banks=None) -> tuple:
    """在后台线程中启动 Kafka Worker，返回 (worker, thread)"""
    import threading

    worker = KafkaModelWorker(assigned_banks=assigned_banks)
    thread = threading.Thread(target=worker.run, daemon=True, name="kafka-worker")
    thread.start()
    return worker, thread


def main() -> None:
    """独立运行模式"""
    import argparse
    from run import initialize_work_space

    if not config.KAFKA_ENABLED:
        print("Kafka 未启用，请设置环境变量 KAFKA_ENABLED=true")
        return

    # 解析命令行参数
    parser = argparse.ArgumentParser(description="Kafka Model Worker")
    parser.add_argument(
        "--banks",
        type=str,
        default="",
        help="分配的岸段 ID，逗号分隔，例如: BANK_001,BANK_002",
    )
    args = parser.parse_args()

    # 解析岸段列表
    assigned_banks = (
        [b.strip() for b in args.banks.split(",") if b.strip()] if args.banks else []
    )

    initialize_work_space()

    worker = KafkaModelWorker(assigned_banks=assigned_banks)
    try:
        worker.run()
    except KeyboardInterrupt:
        print(f"[kafka-worker] 收到停止信号", flush=True)
    finally:
        worker.close()


if __name__ == "__main__":
    main()
