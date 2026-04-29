import datetime
import json
import os
import shutil
import time
import traceback
from typing import Any

import portalocker

import config
import util


class ModelCaseReference:
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
    def _case_lock_path(case_id: str) -> str:
        return os.path.join(config.DIR_MODEL_CASE, case_id, "lock")

    @staticmethod
    def _read_json_file(
        path: str, default: Any = None, tolerate_invalid: bool = False
    ) -> Any:
        if not os.path.exists(path):
            return default

        try:
            with open(path, "r", encoding="utf-8") as file:
                content = file.read().strip()
        except OSError:
            return default

        if not content:
            return default

        try:
            return json.loads(content)
        except json.JSONDecodeError:
            if tolerate_invalid:
                return default
            raise

    @staticmethod
    def _write_json_atomic(path: str, payload: Any) -> None:
        directory = os.path.dirname(path)
        os.makedirs(directory, exist_ok=True)
        temp_path = f"{path}.tmp"

        with open(temp_path, "w", encoding="utf-8") as file:
            json.dump(payload, file, indent=4, ensure_ascii=False)
            file.flush()
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

    @staticmethod
    def _read_case_json_with_lock(
        case_id: str,
        filename: str,
        default: Any = None,
        tolerate_invalid: bool = False,
    ) -> Any:
        path = os.path.join(config.DIR_MODEL_CASE, case_id, filename)
        lock_path = ModelCaseReference._case_lock_path(case_id)
        if not os.path.exists(lock_path):
            return ModelCaseReference._read_json_file(
                path, default=default, tolerate_invalid=tolerate_invalid
            )

        with open(lock_path, "a+", encoding="utf-8") as lock_file:
            portalocker.lock(lock_file, portalocker.LOCK_EX)
            try:
                return ModelCaseReference._read_json_file(
                    path, default=default, tolerate_invalid=tolerate_invalid
                )
            finally:
                portalocker.unlock(lock_file)

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

        with open(config.DIR_GLOBALE_FILE_LOCKER, "a+", encoding="utf-8") as lock_file:
            portalocker.lock(lock_file, portalocker.LOCK_EX)
            self.initialize()
            portalocker.unlock(lock_file)

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
        response = ModelCaseReference._read_case_json_with_lock(
            case_id, "response.json", default=None, tolerate_invalid=True
        )
        return response if isinstance(response, dict) else None

    @staticmethod
    def get_status_log(case_id: str) -> str | None:
        directory = os.path.join(config.DIR_MODEL_CASE, case_id, "status")
        if not os.path.isdir(directory):
            return None
        filenames = util.get_filenames(directory)
        if not filenames:
            return None
        with open(os.path.join(directory, filenames[0]), "r", encoding="utf-8") as file:
            return file.read().strip()

    @staticmethod
    def get_runtime_info(case_id: str) -> dict[str, Any] | None:
        runtime = ModelCaseReference._read_case_json_with_lock(
            case_id, "runtime.json", default=None, tolerate_invalid=True
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

    def initialize(self) -> None:
        if os.path.exists(self.directory):
            return

        os.makedirs(self.directory, exist_ok=True)
        with open(self.local_file_locker, "a+", encoding="utf-8") as lock_file:
            portalocker.lock(lock_file, portalocker.LOCK_EX)
            os.makedirs(os.path.join(self.directory, "time"), exist_ok=True)
            os.makedirs(os.path.join(self.directory, "result"), exist_ok=True)
            os.makedirs(os.path.join(self.directory, "status"), exist_ok=True)

            with open(
                os.path.join(self.directory, "identity.json"), "w", encoding="utf-8"
            ) as file:
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

            with open(
                os.path.join(self.directory, "status", str(config.STATUS_UNLOCK)),
                "w",
                encoding="utf-8",
            ) as file:
                file.write("OK")
            portalocker.unlock(lock_file)

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
        status_directory = os.path.join(self.directory, "status")
        filenames = util.get_filenames(status_directory)
        if not filenames:
            return config.STATUS_NONE
        return int(filenames[0])

    def update_status(self, status: int, content: Any = None, mode: str = "a") -> None:
        with open(self.local_file_locker, "a+", encoding="utf-8") as lock_file:
            portalocker.lock(lock_file, portalocker.LOCK_EX)
            status_directory = os.path.join(self.directory, "status")
            filenames = util.get_filenames(status_directory)
            current_status = int(filenames[0]) if filenames else config.STATUS_NONE

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
            portalocker.unlock(lock_file)

    def set_runtime(
        self,
        stage: str,
        progress: int | None = None,
        message: str | None = None,
        status: str | None = None,
        meta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        runtime_path = os.path.join(self.directory, "runtime.json")
        with open(self.local_file_locker, "a+", encoding="utf-8") as lock_file:
            portalocker.lock(lock_file, portalocker.LOCK_EX)

            loaded = ModelCaseReference._read_json_file(
                runtime_path, default={}, tolerate_invalid=True
            )
            current: dict[str, Any] = loaded if isinstance(loaded, dict) else {}

            current.update(
                {
                    "case-id": self.id,
                    "model": self.model_name,
                    "stage": stage,
                    "updated-at": datetime.datetime.now().isoformat(timespec="seconds"),
                }
            )
            if progress is not None:
                current["progress"] = progress
            if message is not None:
                current["message"] = message
            if status is not None:
                current["status"] = status
            if meta is not None:
                current["meta"] = meta

            ModelCaseReference._write_json_atomic(runtime_path, current)
            portalocker.unlock(lock_file)

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
        with open(self.local_file_locker, "a+", encoding="utf-8") as lock_file:
            portalocker.lock(lock_file, portalocker.LOCK_EX)
            try:
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
            finally:
                portalocker.unlock(lock_file)

    def result_packaging(self) -> str:
        result_directory = os.path.join(self.directory, "result")
        archive_without_extension = os.path.join(self.directory, "result")
        util.create_zip_from_folder(result_directory, archive_without_extension)
        return archive_without_extension + ".zip"
