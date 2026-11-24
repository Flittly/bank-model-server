import contextlib
import datetime
import json
import os
import shutil
import threading
import time
import traceback
from typing import Any, Iterator, TextIO

import portalocker

import config
import util


class ModelCaseReference:
    @staticmethod
    def _acquire_lock(lock_file: TextIO) -> None:
        """毫秒级轮询地抢排他锁，替代 portalocker.lock(fh, LOCK_EX)。

        为什么不能用 portalocker.lock(fh, LOCK_EX)：它在 Windows 上落到
        msvcrt.LK_LOCK，抢不到锁时不是「立刻再试一次」，而是睡满 1 秒再试。
        写方持锁只有 13~17.5ms，抢锁方却要等 1008ms（隔离实测：对方持锁 50ms，
        LK_LOCK 等 1008ms，LK_NBLCK+1ms 等 71~80ms）。这就是 /v0/mc/status
        发现延迟 890ms、daemon 派发排队 1014~1046ms 的共同根因。

        语义与改造前一致：排他、一直等到拿到锁（上限由
        MODEL_FILE_LOCK_WAIT_LIMIT_SECONDS 控制，默认 30s，0=不限），只把等待
        粒度从 1000ms 换成 MODEL_FILE_LOCK_RETRY_SECONDS。互斥性不变——这里依然
        是排他锁，只是换了个「怎么问」的方式；msvcrt 锁不重入，同一进程嵌套
        加锁会一直等到上限后抛错。
        """
        limit = config.MODEL_FILE_LOCK_WAIT_LIMIT_SECONDS
        deadline = time.perf_counter() + limit if limit > 0 else None
        while True:
            try:
                portalocker.lock(lock_file, portalocker.LOCK_EX | portalocker.LOCK_NB)
                return
            except portalocker.exceptions.AlreadyLocked:
                # 只有「别人正持有」才重试。其余 LockException（fd 非法、段超长）
                # 是硬错误，重试没有意义，直接抛。
                if deadline is not None and time.perf_counter() >= deadline:
                    raise
                time.sleep(config.MODEL_FILE_LOCK_RETRY_SECONDS)

    @staticmethod
    @contextlib.contextmanager
    def _case_lock(path: str) -> Iterator[TextIO]:
        """进入即持锁、退出必解锁，避免各调用点自己写 lock/unlock 漏配对。"""
        with open(path, "a+", encoding="utf-8") as lock_file:
            ModelCaseReference._acquire_lock(lock_file)
            try:
                yield lock_file
            finally:
                portalocker.unlock(lock_file)

    @staticmethod
    def _extract_runtime_error(runtime: dict[str, Any] | None) -> str | None:
        if not isinstance(runtime, dict):
            return None

        meta = runtime.get("meta")
        if isinstance(meta, dict):
            traceback_text = meta.get("traceback")
            if isinstance(traceback_text, str) and traceback_text.strip():
                lines = [line.strip() for line in traceback_text.splitlines() if line.strip()]
                if lines:
                    return lines[-1]

            dependency_error = meta.get("dependency-error")
            if isinstance(dependency_error, str) and dependency_error.strip():
                return dependency_error.strip()

        message = runtime.get("message")
        status = str(runtime.get("status", "")).lower()
        if (
            isinstance(message, str)
            and message.strip()
            and status in {"error", "failed", "failure"}
        ):
            return message.strip()

        return None

    @staticmethod
    def _extract_event_error(events: list[dict[str, Any]] | None) -> str | None:
        if not isinstance(events, list):
            return None

        for event in reversed(events):
            if not isinstance(event, dict):
                continue
            level = str(event.get("level", "")).lower()
            if level != "error":
                continue

            meta = event.get("meta")
            if isinstance(meta, dict):
                traceback_text = meta.get("traceback")
                if isinstance(traceback_text, str) and traceback_text.strip():
                    lines = [line.strip() for line in traceback_text.splitlines() if line.strip()]
                    if lines:
                        return lines[-1]

            message = event.get("message")
            if isinstance(message, str) and message.strip():
                return message.strip()

        return None

    @staticmethod
    def _is_case_chain(text: str) -> bool:
        parts = [item for item in text.split("-") if item]
        if len(parts) < 2:
            return False
        return all(len(item) == 32 and all(ch in "0123456789abcdef" for ch in item) for item in parts)

    @staticmethod
    def _summarize_dependency_chain(case_chain: str) -> str | None:
        summaries: list[str] = []
        for case_id in [item for item in case_chain.split("-") if item]:
            runtime = ModelCaseReference.get_runtime_info(case_id)
            runtime_error = ModelCaseReference._extract_runtime_error(runtime)
            events_error = ModelCaseReference._extract_event_error(
                ModelCaseReference.get_case_events(case_id)
            )
            detail = runtime_error or events_error
            if detail:
                summaries.append(f"{case_id}: {detail}")

        if not summaries:
            return None
        return " | ".join(summaries)

    @staticmethod
    def _read_json_file(
        path: str,
        default: Any = None,
        tolerate_invalid: bool = False,
        retries: int = 0,
    ) -> Any:
        """读 JSON。retries > 0 时只吸收「换名瞬间的共享冲突」，不改其它语义。

        runtime.json / response.json 的写入恒走 _write_json_atomic（os.replace
        原子换名），读者读到的永远是某个完整版本，因此调用方可以不加文件锁。
        但换名那一瞬间读方 open() 会撞上 Windows 共享冲突（实测约 1.3%），
        这是瞬时窗口，退让 0.5ms 重读即可；重试耗尽后仍返回 default。
        """
        content: str | None = None
        for attempt in range(retries + 1):
            if not os.path.exists(path):
                return default
            try:
                with open(path, "r", encoding="utf-8") as file:
                    content = file.read().strip()
                break
            except OSError:
                if attempt >= retries:
                    return default
                time.sleep(0.0005)

        if not content:
            return default

        try:
            return json.loads(content)
        except json.JSONDecodeError:
            if tolerate_invalid:
                return default
            raise

    @staticmethod
    def _write_json_atomic(path: str, payload: Any, durable: bool = True) -> None:
        """原子写：先写 .tmp 再 os.replace 换名，读者永远看不到中间态。

        durable=False 跳过 fsync，只保证「进程崩溃后可见」，不保证「断电后可见」。
        只给 runtime.json 这类进度条用——它在掉电后丢了无害（下次 set_runtime 重写），
        而每次 set_runtime 都是一条派发命令的常驻成本。response.json 等产出物必须 durable。
        """
        directory = os.path.dirname(path)
        os.makedirs(directory, exist_ok=True)
        temp_path = f"{path}.tmp"

        with open(temp_path, "w", encoding="utf-8") as file:
            json.dump(payload, file, indent=4, ensure_ascii=False)
            file.flush()
            if durable:
                os.fsync(file.fileno())

        last_error: PermissionError | None = None
        for _ in range(5):
            try:
                os.replace(temp_path, path)
                return
            except PermissionError as exc:
                last_error = exc
                time.sleep(0.02)

        if last_error is not None:
            raise last_error

    def __init__(
        self,
        request_url: str,
        request_json: dict[str, Any],
        model_name: str,
        model_path: str,
        dependent_case_ids: list[str] | None = None,
    ) -> None:
        dependent_case_ids = dependent_case_ids or []
        self.model_name = model_name
        self.model_path = model_path
        self.request_url = request_url
        self.request_json = request_json
        self.dependencies = dependent_case_ids
        self.id = util.generate_md5(
            request_url + json.dumps(request_json, sort_keys=True)
        )
        self.directory = os.path.join(config.DIR_MODEL_CASE, self.id)
        self.local_file_locker = os.path.join(self.directory, "lock")
        self.timestamp = 0

        # 建 case 要串行：同一份入参的 md5 相同，两个请求同时建会互相踩目录。
        with ModelCaseReference._case_lock(config.DIR_GLOBALE_FILE_LOCKER):
            self.initialize()

    @staticmethod
    def create(
        request_url: str,
        request_json: dict[str, Any],
        model_name: str,
        model_path: str,
        dependent_case_ids: list[str] | None = None,
    ) -> "ModelCaseReference":
        return ModelCaseReference(
            request_url,
            request_json,
            model_name,
            model_path,
            dependent_case_ids,
        )

    @staticmethod
    def has_case(case_id: str) -> bool:
        return os.path.isdir(os.path.join(config.DIR_MODEL_CASE, case_id))

    @staticmethod
    def open_case(case_id: str) -> "ModelCaseReference | None":
        directory = os.path.join(config.DIR_MODEL_CASE, case_id)
        identity_path = os.path.join(directory, "identity.json")
        if not os.path.exists(identity_path):
            return None

        with open(identity_path, "r", encoding="utf-8") as file:
            identity = json.load(file)

        mcr = ModelCaseReference.__new__(ModelCaseReference)
        mcr.model_name = identity.get("model", "Unknown")
        mcr.model_path = identity.get("model-path", "")
        mcr.request_url = identity.get("url", "")
        mcr.request_json = identity.get("json", {})
        mcr.dependencies = identity.get("dependencies", [])
        mcr.id = case_id
        mcr.directory = directory
        mcr.local_file_locker = os.path.join(directory, "lock")
        mcr.timestamp = ModelCaseReference.get_case_time(case_id)
        return mcr

    @staticmethod
    def check_case_status(case_id: str) -> str:
        mcr = ModelCaseReference.open_case(case_id)
        if mcr is None:
            return "not_found"
        status = mcr.get_status()
        if status & config.STATUS_ERROR:
            return "error"
        if status & config.STATUS_COMPLETE:
            return "completed"
        if status & config.STATUS_RUNNING:
            return "running"
        return "pending"

    @staticmethod
    def get_case_response(case_id: str) -> dict[str, Any] | None:
        # 无锁读：response.json 的写入恒走 _write_json_atomic（原子换名），
        # 读者只会拿到完整版本，见 _read_json_file 的说明。
        # ⚠ 任何新增的写入方都必须继续用 _write_json_atomic，否则读方要恢复加锁。
        path = os.path.join(config.DIR_MODEL_CASE, case_id, "response.json")
        response = ModelCaseReference._read_json_file(
            path, default=None, tolerate_invalid=True, retries=3
        )
        return response if isinstance(response, dict) else None

    @staticmethod
    def get_status_log(case_id: str) -> str | None:
        directory = os.path.join(config.DIR_MODEL_CASE, case_id, "status")
        if not os.path.isdir(directory):
            return None
        filenames = util.get_filenames(directory, missing_ok=True)
        if not filenames:
            return None
        with open(os.path.join(directory, filenames[0]), "r", encoding="utf-8") as file:
            return file.read().strip()

    @staticmethod
    def get_runtime_info(case_id: str) -> dict[str, Any] | None:
        # 无锁读。这里原先抢 case/<id>/lock 的 LOCK_EX，实测 GET /v0/mc/status
        # 在这一段耗 1008.6ms（同一接口的 status/、events 两段都只有 0.3ms）：
        # 写方持锁约 17.5ms，而 msvcrt 的唤醒粒度是 1 秒，撞上就等满一个周期。
        # 读方不写 status/、time/，退出锁队列不影响写方之间的互斥。
        # ⚠ 前提同上：runtime.json 的写入必须始终走 _write_json_atomic。
        path = os.path.join(config.DIR_MODEL_CASE, case_id, "runtime.json")
        runtime = ModelCaseReference._read_json_file(
            path, default=None, tolerate_invalid=True, retries=3
        )
        return runtime if isinstance(runtime, dict) else None

    @staticmethod
    def get_case_events(case_id: str, limit: int = 20) -> list[dict[str, Any]]:
        events_path = os.path.join(config.DIR_MODEL_CASE, case_id, "events.log")
        if not os.path.exists(events_path):
            return []
        with open(events_path, "r", encoding="utf-8") as file:
            lines = [line.strip() for line in file.readlines() if line.strip()]
        events = []
        for line in lines[-limit:]:
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                events.append({"level": "info", "message": line})
        return events

    @staticmethod
    def get_simplified_error_log(case_id: str) -> str:
        runtime_error = ModelCaseReference._extract_runtime_error(
            ModelCaseReference.get_runtime_info(case_id)
        )
        if runtime_error:
            return runtime_error

        event_error = ModelCaseReference._extract_event_error(
            ModelCaseReference.get_case_events(case_id)
        )
        if event_error:
            return event_error

        status_log = ModelCaseReference.get_status_log(case_id)
        if status_log:
            status_log = status_log.strip()
            if status_log and status_log != "OK":
                if ModelCaseReference._is_case_chain(status_log):
                    chain_summary = ModelCaseReference._summarize_dependency_chain(status_log)
                    if chain_summary:
                        return chain_summary
                return status_log

        return "Unknown model execution error"

    @staticmethod
    def get_pre_error_cases(case_id: str) -> list[str]:
        error_log = ModelCaseReference.get_status_log(case_id)
        if not error_log:
            return []
        return [value for value in error_log.splitlines()[0].split("-") if value]

    @staticmethod
    def delete_case(case_id: str) -> bool:
        directory = os.path.join(config.DIR_MODEL_CASE, case_id)
        if not os.path.isdir(directory):
            return False
        shutil.rmtree(directory)
        return True

    @staticmethod
    def is_case_locked(case_id: str) -> bool | None:
        mcr = ModelCaseReference.open_case(case_id)
        if mcr is None:
            return None
        return mcr.find_status(config.STATUS_LOCK)

    @staticmethod
    def get_case_time(case_id: str) -> int:
        directory = os.path.join(config.DIR_MODEL_CASE, case_id, "time")
        if not os.path.isdir(directory):
            return 0
        filenames = util.get_filenames(directory)
        if not filenames:
            return 0
        return int(filenames[0])

    @staticmethod
    def generate_pre_error_log(core_case_id: str, error_case_ids: list[str]) -> str:
        return "-".join(error_case_ids)

    @staticmethod
    def is_case_done(case_id: str) -> bool:
        mcr = ModelCaseReference.open_case(case_id)
        if mcr is None:
            return True
        status = mcr.get_status()
        return bool(status & config.STATUS_COMPLETE or status & config.STATUS_ERROR)

    @staticmethod
    def update_case_status(
        case_id: str,
        status: int,
        content: str | None = None,
        mode: str = "a",
    ) -> None:
        mcr = ModelCaseReference.open_case(case_id)
        if mcr is not None:
            mcr.update_status(status, content, mode)

    def _has_complete_layout(self) -> bool:
        """identity.json 与 status/ 里至少一个状态文件都在，才算「已初始化」。"""
        if not os.path.exists(os.path.join(self.directory, "identity.json")):
            return False
        return bool(
            util.get_filenames(
                os.path.join(self.directory, "status"), missing_ok=True
            )
        )

    def initialize(self) -> None:
        # 判据必须是「三样产出都在」，不能是「目录存在」。
        # 删 case 时只要有一个文件删不掉（Windows 上子进程还占着 child.log 就
        # 删不掉，shutil.rmtree(ignore_errors=True) 会跳过它继续删别的），目录就
        # 仍然「存在」，但里面只剩一个残文件，成了空壳。旧代码这时直接 return，
        # identity.json 和 status/ 永远补不上；run() 紧接着问状态，
        # get_status() 读 status/ 抛 FileNotFoundError ⇒ HTTP 500。
        # 而 case id 是 md5(url+json) ⇒ 同一断面每次重试都落回同一个空壳，
        # 永久失败，重启服务也不会自愈。实测样本：
        # case/f052d3b325823bffc507235742b640ef（目录里只剩 child.log）。
        if self._has_complete_layout():
            return

        os.makedirs(self.directory, exist_ok=True)
        with ModelCaseReference._case_lock(self.local_file_locker):
            os.makedirs(os.path.join(self.directory, "time"), exist_ok=True)
            os.makedirs(os.path.join(self.directory, "result"), exist_ok=True)
            status_directory = os.path.join(self.directory, "status")
            os.makedirs(status_directory, exist_ok=True)

            identity_path = os.path.join(self.directory, "identity.json")
            # 只补缺失的 identity.json，绝不覆盖已有的：case id 只由 url+json
            # 决定，dependencies 不在 id 里，覆盖会改掉已完成 case 的依赖关系。
            if not os.path.exists(identity_path):
                with open(identity_path, "w", encoding="utf-8") as file:
                    json.dump(
                        {
                            "model": self.model_name,
                            "model-path": self.model_path,
                            "url": self.request_url,
                            "json": self.request_json,
                            "dependencies": self.dependencies,
                        },
                        file,
                        indent=4,
                    )

            # 状态判据必须在锁内复核：update_status 是「删旧 + 建新」两步，
            # 锁外那一瞬间 status/ 可能是空的，误判成新 case 会把进度打回 pending。
            is_new = not util.get_filenames(status_directory, missing_ok=True)
            if is_new:
                with open(
                    os.path.join(status_directory, str(config.STATUS_UNLOCK)),
                    "w",
                    encoding="utf-8",
                ) as file:
                    file.write("OK")

        if not is_new:
            # 这是已有的 case：只补齐了目录结构，不能重置响应/阶段/时间戳，
            # 否则会把 COMPLETE 的缓存打回 pending，下次又重算一遍。
            return

        self.make_response({})
        self.set_runtime(
            stage="initialized",
            progress=0,
            message="Case initialized",
            status="pending",
        )
        self.update_call_time()

    def update_call_time(self) -> None:
        time_directory = os.path.join(self.directory, "time")
        os.makedirs(time_directory, exist_ok=True)
        current_timestamp = int(datetime.datetime.now().timestamp() * 1000)
        for filename in util.get_filenames(time_directory):
            os.remove(os.path.join(time_directory, filename))
        with open(
            os.path.join(time_directory, str(current_timestamp)), "w", encoding="utf-8"
        ):
            pass
        self.timestamp = current_timestamp

    def get_status(self) -> int:
        # missing_ok：status/ 可能被外部清理删掉，也可能正处在 update_status
        # 的「删旧→建新」之间。读不到状态就是「没有状态」，不能抛
        # FileNotFoundError —— 那会把 GET /v0/mc/status 变成 500，而这里的
        # 每一个调用方（find_status / is_used / is_case_done）本来就把
        # STATUS_NONE 当作正常返回值处理。
        status_directory = os.path.join(self.directory, "status")
        filenames = util.get_filenames(status_directory, missing_ok=True)
        if not filenames:
            return config.STATUS_NONE
        try:
            return int(filenames[0])
        except ValueError:
            return config.STATUS_NONE

    def update_status(self, status: int, content: Any = None, mode: str = "a") -> None:
        # status/ 是「删旧文件 + 建新文件」两步，不是原子换名：不互斥就会出现
        # 两个文件名同时存在（读者按 filenames[0] 取，可能取到旧的那个）。
        with ModelCaseReference._case_lock(self.local_file_locker):
            status_directory = os.path.join(self.directory, "status")
            # 目录被外部清理删掉时自愈：否则下面的 open(..., "w") 会抛
            # FileNotFoundError，把一次正常的状态流转变成 case 失败。
            os.makedirs(status_directory, exist_ok=True)
            filenames = util.get_filenames(status_directory, missing_ok=True)
            try:
                current_status = int(filenames[0]) if filenames else config.STATUS_NONE
            except ValueError:
                current_status = config.STATUS_NONE

            if current_status != status:
                if filenames:
                    os.remove(os.path.join(status_directory, filenames[0]))
                with open(
                    os.path.join(status_directory, str(status)), "w", encoding="utf-8"
                ) as file:
                    if content is not None:
                        file.write(f"{content}\n")
                    else:
                        file.write("OK")
            elif content is not None:
                with open(
                    os.path.join(status_directory, str(status)), mode, encoding="utf-8"
                ) as file:
                    file.write(f"{content}\n")

            self.update_call_time()

    def set_runtime(
        self,
        stage: str,
        progress: int | None = None,
        message: str | None = None,
        status: str | None = None,
        meta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        runtime_path = os.path.join(self.directory, "runtime.json")
        # 这里必须持锁：read-modify-write，两个写方并发会互相丢 timeline 条目。
        with ModelCaseReference._case_lock(self.local_file_locker):
            loaded = ModelCaseReference._read_json_file(
                runtime_path, default={}, tolerate_invalid=True
            )
            current: dict[str, Any] = loaded if isinstance(loaded, dict) else {}

            current.update(
                {
                    "case-id": self.id,
                    "model": self.model_name,
                    "stage": stage,
                    # 毫秒精度：Java 侧算「模型已完成到客户端发现」的延迟要读这个字段。
                    # 唯一的消费方 modelRecovery._iso_age_seconds 用 fromisoformat，
                    # 能解析带小数秒的形式，空闲判定不受影响。
                    "updated-at": datetime.datetime.now().isoformat(
                        timespec="milliseconds"
                    ),
                }
            )
            if config.MODEL_PROFILE_TIMELINE:
                # 唯一的时间线写入点。11 个指标脚本、daemon、请求线程全部经由
                # set_runtime 报阶段，所以只改这一处就能覆盖全部阶段切换，
                # 且不增加任何一次额外的文件读写。
                timeline = current.get("timeline")
                if not isinstance(timeline, list):
                    timeline = []
                timeline.append(
                    {
                        "stage": stage,
                        "at-ns": time.time_ns(),
                        "pid": os.getpid(),
                        "thread": threading.current_thread().name,
                    }
                )
                current["timeline"] = timeline[-config.MODEL_PROFILE_TIMELINE_MAX :]
            if progress is not None:
                current["progress"] = progress
            if message is not None:
                current["message"] = message
            if status is not None:
                current["status"] = status
            if meta is not None:
                current["meta"] = meta

            # durable=False：runtime.json 是进度条，不是账本。每推进一个阶段都写一次，
            # 一次 fsync 实测 1.34ms，而它掉电丢了只是「下一次轮询少看到一个中间态」。
            # 真正不能丢的产出（response.json）走 make_response，仍是 durable=True。
            ModelCaseReference._write_json_atomic(runtime_path, current, durable=False)

        return current

    def append_event(
        self,
        level: str,
        message: str,
        stage: str | None = None,
        meta: dict[str, Any] | None = None,
    ) -> None:
        events_path = os.path.join(self.directory, "events.log")
        event: dict[str, Any] = {
            "time": datetime.datetime.now().isoformat(timespec="seconds"),
            "level": level,
            "message": message,
        }
        if stage is not None:
            event["stage"] = stage
        if meta is not None:
            event["meta"] = meta

        with open(events_path, "a", encoding="utf-8") as file:
            file.write(json.dumps(event, ensure_ascii=False) + "\n")

    def mark_error(self, error: Exception) -> None:
        error_text = "".join(
            traceback.format_exception(type(error), error, error.__traceback__)
        )
        self.set_runtime(
            stage="error",
            progress=100,
            message=str(error),
            status="error",
            meta={"traceback": error_text},
        )
        self.append_event(
            "error", str(error), stage="error", meta={"traceback": error_text}
        )

    def find_status(self, status_flag: int) -> bool:
        return (self.get_status() & status_flag) != 0

    def is_used(self) -> bool:
        return self.find_status(config.STATUS_LOCK) or self.find_status(
            config.STATUS_RUNNING
        )

    def make_response(
        self, content: dict[str, Any] | None = None
    ) -> dict[str, Any] | None:
        response_path = os.path.join(self.directory, "response.json")
        with ModelCaseReference._case_lock(self.local_file_locker):
            if content is None:
                response = ModelCaseReference._read_json_file(
                    response_path, default=None, tolerate_invalid=True
                )
                return response if isinstance(response, dict) else None

            current_response = {}
            loaded = ModelCaseReference._read_json_file(
                response_path, default={}, tolerate_invalid=True
            )
            if isinstance(loaded, dict):
                current_response = loaded

            current_response.update(content)
            current_response["case-id"] = self.id
            current_response["model"] = self.model_name

            ModelCaseReference._write_json_atomic(response_path, current_response)
            return current_response

    def result_packaging(self) -> str:
        result_directory = os.path.join(self.directory, "result")
        archive_without_extension = os.path.join(self.directory, "result")
        util.create_zip_from_folder(result_directory, archive_without_extension)
        return archive_without_extension + ".zip"
