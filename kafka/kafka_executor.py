import datetime as dt
import time
from typing import Any

import config
import model


def _utc_now() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _normalize_risk_level(risk_vector: Any) -> int:
    if not isinstance(risk_vector, list):
        return 0
    for index, item in enumerate(risk_vector):
        if isinstance(item, (int, float)) and int(item) == 1:
            return index + 1
    return 0


def execute_risk_level_task(task: dict[str, Any], worker_id: str) -> dict[str, Any]:
    payload = task["payload"]
    started_at = _utc_now()
    begin = time.time()

    try:
        mcr = model.launcher.fetch_model_from_API(config.API_MI_RISK_LEVEL).run(payload)
        deadline = time.time() + config.MODEL_SERVICE_DEFAULT_TIMEOUT

        while time.time() < deadline:
            if mcr.find_status(config.STATUS_COMPLETE):
                response = model.ModelCaseReference.get_case_response(mcr.id)
                if response is None:
                    raise RuntimeError("模型计算完成但没有结果")
                return {
                    "runId": task["runId"],
                    "taskId": task["taskId"],
                    "sectionId": task["sectionId"],
                    "status": "SUCCESS",
                    "riskLevel": _normalize_risk_level(response.get("risk-level")),
                    "rawResult": response,
                    "artifactPath": None,
                    "workerId": worker_id,
                    "startedAt": started_at,
                    "completedAt": _utc_now(),
                    "durationMs": int((time.time() - begin) * 1000),
                    "errorMessage": None,
                }
            if mcr.find_status(config.STATUS_ERROR):
                raise RuntimeError(
                    model.ModelCaseReference.get_simplified_error_log(mcr.id)
                )
            time.sleep(config.MODEL_SERVICE_POLL_INTERVAL)

        raise TimeoutError("模型计算超时")

    except Exception as exc:
        return {
            "runId": task["runId"],
            "taskId": task["taskId"],
            "sectionId": task["sectionId"],
            "status": "ERROR",
            "riskLevel": None,
            "rawResult": None,
            "artifactPath": None,
            "workerId": worker_id,
            "startedAt": started_at,
            "completedAt": _utc_now(),
            "durationMs": int((time.time() - begin) * 1000),
            "errorMessage": str(exc),
        }
