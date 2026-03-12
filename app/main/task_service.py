import datetime as dt
import time
from typing import Any

import config
import model
from util import db_ops


RISK_INDICATOR_IDS = ["Dsed", "PL", "LC", "Zb", "Sa", "Ln", "PQ", "Ky", "Zd"]


def _now_string() -> str:
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _normalize_timepoint(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return text
    if len(text) == 6 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-01"
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    if len(text) == 7 and text[4] == "-":
        return f"{text}-01"
    return text


def _normalize_risk_level(risk_vector: Any) -> int:
    if not isinstance(risk_vector, list) or len(risk_vector) != 4:
        return 0
    try:
        values = [float(value) for value in risk_vector]
    except (TypeError, ValueError):
        return 0
    max_value = max(values)
    if max_value <= 0:
        return 0
    return values.index(max_value) + 1


def _build_risk_payload(section: dict[str, Any]) -> dict[str, Any]:
    weights = section.get("weights") or {}
    risk_thresholds = section.get("risk_thresholds") or "NONE"

    payload = {
        "segment": section.get("segment") or section.get("region_code") or "Mzs",
        "set": section.get("set_name") or "standard",
        "current-timepoint": _normalize_timepoint(section.get("current_timepoint")),
        "comparison-timepoint": _normalize_timepoint(
            section.get("comparison_timepoint")
        ),
        "hs": section.get("hs"),
        "hc": section.get("hc"),
        "protection-level": section.get("protection_level"),
        "control-level": section.get("control_level"),
        "section-geometry": section.get("section_geometry") or section.get("geometry"),
        "bench-id": section.get("bench_id"),
        "ref-id": section.get("ref_id"),
        "water-qs": str(section.get("water_qs") or ""),
        "tidal-level": section.get("tidal_level"),
        "risk-thresholds": risk_thresholds,
        "wRE": weights.get("wRE", "NONE"),
        "wNM": weights.get("wNM", "NONE"),
        "wGE": weights.get("wGE", "NONE"),
        "wRL": weights.get("wRL", "NONE"),
    }

    missing_fields = [
        key
        for key, value in payload.items()
        if value in (None, "")
        and key not in {"risk-thresholds", "wRE", "wNM", "wGE", "wRL"}
    ]
    if missing_fields:
        section_id = section.get("section_id") or section.get("id") or "unknown"
        missing_text = ", ".join(sorted(missing_fields))
        raise ValueError(
            f"Section {section_id} missing required fields: {missing_text}"
        )

    return payload


def _execute_model(
    model_api: str, payload: dict[str, Any], timeout_seconds: int
) -> dict[str, Any]:
    mcr = model.launcher.fetch_model_from_API(model_api).run(payload)
    deadline = time.time() + timeout_seconds

    while time.time() < deadline:
        if mcr.find_status(config.STATUS_COMPLETE):
            response = model.ModelCaseReference.get_case_response(mcr.id)
            if response is None:
                raise RuntimeError(f"Model '{model_api}' completed without response")
            return response
        if mcr.find_status(config.STATUS_ERROR):
            raise RuntimeError(
                model.ModelCaseReference.get_simplified_error_log(mcr.id)
            )
        time.sleep(config.MODEL_SERVICE_POLL_INTERVAL)

    raise TimeoutError(f"Model '{model_api}' timed out after {timeout_seconds}s")


def _collect_indicator_details(risk_response: dict[str, Any]) -> dict[str, Any]:
    details: dict[str, Any] = {}
    indicator_ids = risk_response.get("multi-indicator-ids") or {}

    for indicator_name in RISK_INDICATOR_IDS:
        case_id = indicator_ids.get(indicator_name)
        if not case_id:
            continue
        indicator_response = model.ModelCaseReference.get_case_response(case_id) or {}
        details[indicator_name] = {
            "case_id": case_id,
            "response": indicator_response,
            "risk_level": _normalize_risk_level(indicator_response.get("risk-level")),
        }

    return details


def _persist_section_result(
    task: dict[str, Any], section: dict[str, Any], risk_response: dict[str, Any]
) -> int:
    indicators = {
        "case_id": risk_response.get("case-id"),
        "risk_value": risk_response.get("result"),
        "risk_vector": risk_response.get("risk-level"),
        "multi_indicator_ids": risk_response.get("multi-indicator-ids") or {},
        "details": _collect_indicator_details(risk_response),
    }

    return db_ops.create_risk_result(
        task_id=task["task_id"],
        section_id=section["section_id"],
        section_name=section.get("section_name") or "",
        region_code=section.get("region_code") or "",
        bank_id=section.get("bank_id") or "",
        risk_level=_normalize_risk_level(risk_response.get("risk-level")),
        indicators=indicators,
        geometry=section.get("geometry"),
    )


def run_task(task_id: str, timeout_seconds: int) -> dict[str, Any]:
    task = db_ops.get_task(task_id)
    if not task:
        raise FileNotFoundError(f"Task ID ({task_id}) Not Found")

    sections = db_ops.get_cross_sections(task_id_db=task["id"])
    if not sections:
        raise ValueError(f"Task ID ({task_id}) has no cross sections")

    db_ops.update_task_status(
        task_id,
        "running",
        run_started_at=_now_string(),
        clear_run_completed_at=True,
        clear_error_message=True,
    )
    db_ops.delete_risk_results(task_id)

    persisted_result_ids: list[int] = []
    failures: list[dict[str, str]] = []

    for section in sections:
        try:
            payload = _build_risk_payload(section)
            response = _execute_model(
                config.API_MI_RISK_LEVEL, payload, timeout_seconds
            )
            result_id = _persist_section_result(task, section, response)
            persisted_result_ids.append(result_id)
        except Exception as exc:
            failures.append(
                {
                    "section_id": str(
                        section.get("section_id") or section.get("id") or ""
                    ),
                    "section_name": str(section.get("section_name") or ""),
                    "error": str(exc),
                }
            )

    finished_at = _now_string()
    if failures:
        error_message = "; ".join(
            f"{item['section_id']}: {item['error']}" for item in failures
        )
        db_ops.update_task_status(
            task_id,
            "error",
            run_completed_at=finished_at,
            error_message=error_message,
        )
    else:
        db_ops.update_task_status(
            task_id,
            "completed",
            run_completed_at=finished_at,
            clear_error_message=True,
        )

    return {
        "task_id": task_id,
        "status": "error" if failures else "completed",
        "total_sections": len(sections),
        "persisted_results": len(persisted_result_ids),
        "result_ids": persisted_result_ids,
        "failures": failures,
    }


def get_task_results(task_id: str) -> dict[str, Any]:
    task = db_ops.get_task(task_id)
    if not task:
        raise FileNotFoundError(f"Task ID ({task_id}) Not Found")
    return {
        "task_id": task_id,
        "results": db_ops.get_bank_risk_results(task_id=task_id),
    }


def get_bank_result(section_id: str) -> dict[str, Any]:
    result = db_ops.get_bank_risk_result(section_id)
    if not result:
        raise FileNotFoundError(f"Section ID ({section_id}) result not found")
    return result
