import json
import os
import time
from typing import Any

import config
import model
import util
from util import StorageMonitor
from . import task_service


def normalize_model_api(model_api: str) -> str:
    normalized = model_api.strip()
    if not normalized.startswith("/"):
        normalized = "/" + normalized
    if not normalized.startswith(config.API_VERSION + "/"):
        normalized = config.API_VERSION + normalized
    return normalized


def resolve_case_id(case_id: str | None, legacy_id: str | None) -> str:
    resolved = case_id or legacy_id
    if not resolved:
        raise ValueError("Missing case_id")
    return resolved


def health() -> dict[str, Any]:
    return {
        "status": "ok",
        "service": "yangtze-bank-collapse-model-service",
        "models": sorted(config.MODEL_REGISTRY.keys()),
    }


def list_models() -> dict[str, Any]:
    return {
        "models": [
            {"model_api": api, "script": script}
            for api, script in sorted(config.MODEL_REGISTRY.items())
        ]
    }


def predict(
    model_api: str, payload: dict[str, Any], timeout_seconds: int
) -> dict[str, Any]:
    normalized_api = normalize_model_api(model_api)
    mcr = model.launcher.fetch_model_from_API(normalized_api).run(payload)
    deadline = time.time() + timeout_seconds

    while time.time() < deadline:
        if mcr.find_status(config.STATUS_COMPLETE):
            response = model.ModelCaseReference.get_case_response(mcr.id)
            if response is None:
                raise RuntimeError(
                    f"Model '{normalized_api}' completed without response"
                )
            return response
        if mcr.find_status(config.STATUS_ERROR):
            raise RuntimeError(
                model.ModelCaseReference.get_simplified_error_log(mcr.id)
            )
        time.sleep(config.MODEL_SERVICE_POLL_INTERVAL)

    raise TimeoutError(f"Model '{normalized_api}' timed out after {timeout_seconds}s")


def get_model_case_status(case_id: str) -> dict[str, Any]:
    if not model.ModelCaseReference.has_case(case_id):
        raise FileNotFoundError(f"Model Case ID ({case_id}) Not Found")
    return {
        "status": model.ModelCaseReference.check_case_status(case_id),
        "runtime": model.ModelCaseReference.get_runtime_info(case_id),
        "events": model.ModelCaseReference.get_case_events(case_id),
    }


def get_model_case_result(case_id: str) -> dict[str, Any]:
    if not model.ModelCaseReference.has_case(case_id):
        raise FileNotFoundError(f"Model Case ID ({case_id}) Not Found")
    return {"result": model.ModelCaseReference.get_case_response(case_id)}


def get_model_case_error(case_id: str) -> dict[str, Any]:
    if not model.ModelCaseReference.has_case(case_id):
        raise FileNotFoundError(f"Model Case ID ({case_id}) Not Found")
    return {
        "error": model.ModelCaseReference.get_simplified_error_log(case_id),
        "runtime": model.ModelCaseReference.get_runtime_info(case_id),
        "events": model.ModelCaseReference.get_case_events(case_id),
    }


def get_pre_error_cases(case_id: str) -> dict[str, Any]:
    if not model.ModelCaseReference.has_case(case_id):
        raise FileNotFoundError(f"Model Case ID ({case_id}) Not Found")
    return {"case-list": model.ModelCaseReference.get_pre_error_cases(case_id)}


def delete_model_case(case_id: str) -> dict[str, Any]:
    if not model.ModelCaseReference.delete_case(case_id):
        raise FileNotFoundError(f"Model Case ID ({case_id}) Not Found")
    return {"message": "OK"}


def get_model_cases_status(case_ids: list[str]) -> dict[str, str]:
    status_dict: dict[str, str] = {}
    for case_id in case_ids:
        if not model.ModelCaseReference.has_case(case_id):
            raise FileNotFoundError(f"Model Case ID ({case_id}) Not Found")
        status_dict[case_id] = model.ModelCaseReference.check_case_status(case_id)
    return status_dict


def get_model_cases_call_time() -> dict[str, Any]:
    response = {"timestamps": []}
    if not os.path.exists(config.DIR_MODEL_CASE):
        return response

    for case_id in util.get_directories(config.DIR_MODEL_CASE):
        is_locked = model.ModelCaseReference.is_case_locked(case_id)
        if is_locked is None:
            continue
        response["timestamps"].append(
            {
                "id": case_id,
                "time": model.ModelCaseReference.get_case_time(case_id),
                "status": "LOCK" if is_locked else "UNLOCK",
            }
        )

    response["timestamps"].sort(key=lambda case: case["time"], reverse=True)
    return response


def get_model_cases_serialization(case_ids: list[str]) -> dict[str, Any]:
    response = {"serialization-list": []}
    for case_id in case_ids:
        identity_path = os.path.join(config.DIR_MODEL_CASE, case_id, "identity.json")
        if not os.path.exists(identity_path):
            raise FileNotFoundError(f"Model Case ID ({case_id}) Not Found")
        with open(identity_path, "r", encoding="utf-8") as file:
            response["serialization-list"].append(
                {"id": case_id, "serialization": json.load(file)}
            )
    return response


def delete_model_cases(case_ids: list[str]) -> dict[str, Any]:
    for case_id in case_ids:
        if not model.ModelCaseReference.delete_case(case_id):
            raise FileNotFoundError(f"Model Case ID ({case_id}) Not Found")
    return {"message": "OK"}


def get_model_case_file(case_id: str, filename: str) -> str:
    file_path = os.path.join(config.DIR_MODEL_CASE, case_id, "result", filename)
    if not os.path.exists(file_path):
        raise FileNotFoundError("Filename Not Found")
    return file_path


def get_resource_file(directory: str) -> str:
    file_path = os.path.join(config.DIR_RESOURCE, directory)
    if not os.path.exists(file_path):
        raise FileNotFoundError("Filename Not Found")
    return file_path


def get_model_case_zip(case_id: str) -> str:
    mcr = model.ModelCaseReference.open_case(case_id)
    if mcr is None:
        raise FileNotFoundError(f"Model Case ID ({case_id}) Not Found")
    return mcr.result_packaging()


def get_disk_usage() -> dict[str, Any]:
    return {"usage": StorageMonitor().get_size()}


def get_hydrodynamic_resource_list() -> dict[str, Any]:
    response = {"resource": []}
    for segment_name in util.get_directories(config.DIR_RESOURCE_HYDRODYNAMIC):
        segment = {"name": segment_name, "date": []}
        for year_name in util.get_directories(
            os.path.join(config.DIR_RESOURCE_HYDRODYNAMIC, segment_name)
        ):
            year = {"year": year_name, "sets": []}
            for set_name in util.get_directories(
                os.path.join(config.DIR_RESOURCE_HYDRODYNAMIC, segment_name, year_name)
            ):
                set_item = {"name": set_name, "list": []}
                for case_name in util.get_directories(
                    os.path.join(
                        config.DIR_RESOURCE_HYDRODYNAMIC,
                        segment_name,
                        year_name,
                        set_name,
                    ),
                    ["shp", "geojson"],
                ):
                    description_path = os.path.join(
                        config.DIR_RESOURCE_HYDRODYNAMIC,
                        segment_name,
                        year_name,
                        set_name,
                        case_name,
                        "description.json",
                    )
                    with open(description_path, "r", encoding="utf-8") as file:
                        desc = json.load(file)
                    set_item["list"].append({"name": case_name, "temp": desc["temp"]})
                year["sets"].append(set_item)
            segment["date"].append(year)
        response["resource"].append(segment)
    return response


def upload_hydrodynamic_resource(data: dict[str, Any]) -> dict[str, Any]:
    model.launch_hydrodynamic_resource_generate(
        data["segment"],
        data["year"],
        data["set"],
        data["name"],
        bool(data["temp"]),
        data["boundary"],
    )
    return {
        "directory": f"hydrodynamic/{data['segment']}/{data['year']}/{data['set']}/{data['name']}/"
    }


def delete_resource_directory(directory: str) -> dict[str, Any]:
    resource_dir = os.path.join(config.DIR_RESOURCE, os.path.normpath(directory))
    if not os.path.exists(resource_dir):
        raise FileNotFoundError(f"Directory {resource_dir} does not exist")
    util.delete_folder_contents(resource_dir)
    return {"message": "OK"}


def handle_model_runner(api: str, request_json: dict[str, Any]) -> dict[str, Any]:
    print(
        f"[model-runner] incoming api={api} payload={request_json}",
        flush=True,
    )
    try:
        mcr = model.launcher.fetch_model_from_API(api).run(request_json)
        response = mcr.make_response() or {}
        print(
            f"[model-runner] accepted api={api} case_id={mcr.id} response_keys={list(response.keys())}",
            flush=True,
        )
        return response
    except Exception as exc:
        print(
            f"[model-runner] failed api={api} error={exc} payload={request_json}",
            flush=True,
        )
        raise


def run_task(task_id: str, timeout_seconds: int) -> dict[str, Any]:
    return task_service.run_task(task_id, timeout_seconds)


def get_task_results(task_id: str) -> dict[str, Any]:
    return task_service.get_task_results(task_id)


def get_bank_result(section_id: str) -> dict[str, Any]:
    return task_service.get_bank_result(section_id)


def extract_tiff_bounds(tiff_key: str) -> dict[str, Any]:
    """提取 tiff 文件边界并存储到数据库"""
    from util.rustfs import resolve_tiff_path, extract_tiff_bounds
    from util.db_ops import save_tiff_bounds

    # 解析 tiff 路径
    parts = tiff_key.split("/")
    segment = parts[1] if len(parts) > 1 else ""
    timepoint = parts[4] if len(parts) > 4 else None

    # 获取 tiff 文件路径
    tiff_path = resolve_tiff_path(tiff_key, segment=segment, timepoint=timepoint)

    # 提取边界
    bounds = extract_tiff_bounds(tiff_path)

    # 解析 region_code, year
    region_code = segment
    year = parts[2] if len(parts) > 2 else None

    # 存储到数据库
    save_tiff_bounds(
        tiff_key=tiff_key,
        region_code=region_code,
        year=year,
        timepoint=timepoint,
        min_x=bounds["min_x"],
        min_y=bounds["min_y"],
        max_x=bounds["max_x"],
        max_y=bounds["max_y"],
        srid=bounds.get("srid", 3857),
    )

    return {
        "tiff_key": tiff_key,
        "min_x": bounds["min_x"],
        "min_y": bounds["min_y"],
        "max_x": bounds["max_x"],
        "max_y": bounds["max_y"],
        "srid": bounds.get("srid", 3857),
    }
