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


class KafkaModelWorker:
    def __init__(self) -> None:
        self.worker_id = config.KAFKA_WORKER_ID
        self.bootstrap_servers = config.KAFKA_BOOTSTRAP_SERVERS
        self.task_topic = config.KAFKA_TASK_TOPIC
        self.result_topic = config.KAFKA_RESULT_TOPIC
        self._running = True

        self.consumer = Consumer(
            {
                "bootstrap.servers": ",".join(self.bootstrap_servers),
                "group.id": config.KAFKA_WORKER_GROUP,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        self.consumer.subscribe([self.task_topic])

        self.producer = Producer(
            {
                "bootstrap.servers": ",".join(self.bootstrap_servers),
                "acks": "all",
            }
        )

    def run(self) -> None:
        print(f"[kafka-worker] 启动 worker_id={self.worker_id}", flush=True)
        print(f"[kafka-worker] bootstrap_servers={self.bootstrap_servers}", flush=True)
        print(f"[kafka-worker] task_topic={self.task_topic}", flush=True)
        print(f"[kafka-worker] result_topic={self.result_topic}", flush=True)

        while self._running:
            msg = self.consumer.poll(timeout=1.0)

            if msg is None:
                continue
            if msg.error():
                print(f"[kafka-worker] 消费错误 error={msg.error()}", flush=True)
                continue

            task = json.loads(msg.value().decode("utf-8"))
            print(
                f"[kafka-worker] 收到任务 runId={task['runId']} taskId={task['taskId']} sectionId={task['sectionId']}",
                flush=True,
            )

            result = execute_risk_level_task(task, self.worker_id)

            self._send_result(result)

            self.consumer.commit(msg)

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


def start_worker_in_background() -> tuple:
    """在后台线程中启动 Kafka Worker，返回 (worker, thread)"""
    import threading

    worker = KafkaModelWorker()
    thread = threading.Thread(target=worker.run, daemon=True, name="kafka-worker")
    thread.start()
    return worker, thread


def main() -> None:
    """独立运行模式"""
    from run import initialize_work_space

    if not config.KAFKA_ENABLED:
        print("Kafka 未启用，请设置环境变量 KAFKA_ENABLED=true")
        return

    initialize_work_space()

    worker = KafkaModelWorker()
    try:
        worker.run()
    except KeyboardInterrupt:
        print(f"[kafka-worker] 收到停止信号", flush=True)
    finally:
        worker.close()


if __name__ == "__main__":
    main()
