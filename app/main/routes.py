import json
import os
import zipfile
from typing import Any

from fastapi import APIRouter, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import FileResponse

import config
import util
from . import controllers
from .schemas import PredictRequest, TaskRunRequest

api_router = APIRouter()


def allowed_file(filename: str) -> bool:
    return (
        "." in filename
        and filename.rsplit(".", 1)[1].lower() in config.APP_ALLOWED_EXTENSIONS
    )


def handle_not_found(exc: FileNotFoundError) -> HTTPException:
    return HTTPException(status_code=404, detail=str(exc))


def parse_case_id(case_id: str | None, legacy_id: str | None) -> str:
    try:
        return controllers.resolve_case_id(case_id, legacy_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def parse_case_ids(body: dict[str, Any]) -> list[str]:
    case_ids = body.get("case-ids") or body.get("case_ids") or []
    if not isinstance(case_ids, list) or not case_ids:
        raise HTTPException(status_code=400, detail="Missing case-ids")
    return case_ids


@api_router.get("/api/v1/health")
async def health() -> dict[str, Any]:
    return controllers.health()


@api_router.get("/api/v1/models")
async def list_models() -> dict[str, Any]:
    return controllers.list_models()


@api_router.post("/api/v1/predict")
async def predict(request: PredictRequest) -> dict[str, Any]:
    try:
        return controllers.predict(
            request.model_api, request.payload, request.timeout_seconds
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except TimeoutError as exc:
        raise HTTPException(status_code=504, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@api_router.post("/bank/tasks/{task_id}/run")
async def run_task(task_id: str, request: TaskRunRequest) -> dict[str, Any]:
    try:
        return controllers.run_task(task_id, request.timeout_seconds)
    except FileNotFoundError as exc:
        raise handle_not_found(exc) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except TimeoutError as exc:
        raise HTTPException(status_code=504, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@api_router.get("/bank/tasks/{task_id}/results")
async def get_task_results(task_id: str) -> dict[str, Any]:
    try:
        return controllers.get_task_results(task_id)
    except FileNotFoundError as exc:
        raise handle_not_found(exc) from exc


@api_router.get(f"{config.API_VERSION}/bank/results/{{result_id}}")
async def get_bank_result(result_id: str) -> dict[str, Any]:
    try:
        return controllers.get_bank_result(result_id)
    except FileNotFoundError as exc:
        raise handle_not_found(exc) from exc


@api_router.get(f"{config.API_MC_STATUS}")
async def get_model_case_status(
    case_id: str | None = Query(default=None),
    legacy_id: str | None = Query(default=None, alias="id"),
) -> dict[str, Any]:
    try:
        return controllers.get_model_case_status(parse_case_id(case_id, legacy_id))
    except FileNotFoundError as exc:
        raise handle_not_found(exc) from exc


@api_router.get(f"{config.API_MC_RESULT}")
async def get_model_case_result(
    case_id: str | None = Query(default=None),
    legacy_id: str | None = Query(default=None, alias="id"),
) -> dict[str, Any]:
    try:
        return controllers.get_model_case_result(parse_case_id(case_id, legacy_id))
    except FileNotFoundError as exc:
        raise handle_not_found(exc) from exc


@api_router.get(f"{config.API_MC_ERROR}")
async def get_model_case_error(
    case_id: str | None = Query(default=None),
    legacy_id: str | None = Query(default=None, alias="id"),
) -> dict[str, Any]:
    try:
        return controllers.get_model_case_error(parse_case_id(case_id, legacy_id))
    except FileNotFoundError as exc:
        raise handle_not_found(exc) from exc


@api_router.get(f"{config.API_MC_PRE_ERROR_CASES}")
async def get_pre_error_cases(
    case_id: str | None = Query(default=None),
    legacy_id: str | None = Query(default=None, alias="id"),
) -> dict[str, Any]:
    try:
        return controllers.get_pre_error_cases(parse_case_id(case_id, legacy_id))
    except FileNotFoundError as exc:
        raise handle_not_found(exc) from exc


@api_router.delete(f"{config.API_MC_DELETE}")
async def delete_model_case(
    case_id: str | None = Query(default=None),
    legacy_id: str | None = Query(default=None, alias="id"),
) -> dict[str, Any]:
    try:
        return controllers.delete_model_case(parse_case_id(case_id, legacy_id))
    except FileNotFoundError as exc:
        raise handle_not_found(exc) from exc


@api_router.post(f"{config.API_MCS_STATUS}")
async def get_model_cases_status(body: dict[str, Any]) -> dict[str, str]:
    try:
        return controllers.get_model_cases_status(parse_case_ids(body))
    except FileNotFoundError as exc:
        raise handle_not_found(exc) from exc


@api_router.get(f"{config.API_MCS_TIME}")
async def get_model_cases_call_time() -> dict[str, Any]:
    return controllers.get_model_cases_call_time()


@api_router.post(f"{config.API_MCS_SERIALIZATION}")
async def get_model_cases_serialization(body: dict[str, Any]) -> dict[str, Any]:
    try:
        return controllers.get_model_cases_serialization(parse_case_ids(body))
    except FileNotFoundError as exc:
        raise handle_not_found(exc) from exc


@api_router.post(f"{config.API_MCS_DELETE}")
async def delete_model_cases(body: dict[str, Any]) -> dict[str, Any]:
    try:
        return controllers.delete_model_cases(parse_case_ids(body))
    except FileNotFoundError as exc:
        raise handle_not_found(exc) from exc


@api_router.get(f"{config.API_FS_RESULT_FILE}")
async def get_model_case_file(
    filename: str = Query(alias="filename"),
    case_id: str | None = Query(default=None),
    legacy_id: str | None = Query(default=None, alias="id"),
    legacy_name: str | None = Query(default=None, alias="name"),
):
    try:
        file_path = controllers.get_model_case_file(
            parse_case_id(case_id, legacy_id), legacy_name or filename
        )
        download_name = os.path.basename(file_path)
        return FileResponse(
            file_path, media_type="application/octet-stream", filename=download_name
        )
    except FileNotFoundError as exc:
        raise handle_not_found(exc) from exc


@api_router.get(f"{config.API_FS_RESOURCE_FILE}")
async def get_resource_file(name: str) -> FileResponse:
    try:
        file_path = controllers.get_resource_file(name)
        return FileResponse(
            file_path,
            media_type="application/octet-stream",
            filename=os.path.basename(file_path),
        )
    except FileNotFoundError as exc:
        raise handle_not_found(exc) from exc


@api_router.get(f"{config.API_FS_RESULT_ZIP}")
async def get_model_case_zip(
    case_id: str | None = Query(default=None),
    legacy_id: str | None = Query(default=None, alias="id"),
) -> FileResponse:
    try:
        zip_path = controllers.get_model_case_zip(parse_case_id(case_id, legacy_id))
        return FileResponse(
            zip_path, media_type="application/zip", filename=os.path.basename(zip_path)
        )
    except FileNotFoundError as exc:
        raise handle_not_found(exc) from exc


@api_router.delete(f"{config.API_FS_RESOURCE_DELETE}")
async def delete_resource_directory(directory: str) -> dict[str, Any]:
    try:
        return controllers.delete_resource_directory(directory)
    except FileNotFoundError as exc:
        raise handle_not_found(exc) from exc


@api_router.post(f"{config.API_FS_RESOURCE_ZIP}")
async def upload_resource_zip(
    file: UploadFile = File(...), json_data: str = Form(...)
) -> dict[str, Any]:
    request_json = json.loads(json_data)
    resource_path = os.path.join(config.DIR_RESOURCE, request_json.get("type", ""))
    os.makedirs(resource_path, exist_ok=True)
    if not file.filename or not allowed_file(file.filename):
        raise HTTPException(status_code=400, detail="File type not allowed")
    zip_path = os.path.join(resource_path, file.filename)
    with open(zip_path, "wb") as buffer:
        buffer.write(await file.read())
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(resource_path)
    os.remove(zip_path)
    util.update_size(config.DIR_STORAGE_LOG, util.get_folder_size_in_gb(resource_path))
    return {"message": "OK"}


async def upload_structured_resource(
    file: UploadFile, json_data: str, base_directory: str, extension: str
) -> dict[str, Any]:
    request_json = json.loads(json_data)
    segment = request_json["segment"]
    year = request_json["year"]
    set_name = request_json["set"]
    name = request_json["name"]
    set_path = os.path.join(base_directory, segment, year, set_name)
    os.makedirs(set_path, exist_ok=True)
    if not file.filename or not allowed_file(file.filename):
        raise HTTPException(status_code=400, detail="File type not allowed")
    zip_path = os.path.join(set_path, file.filename)
    root_name, _ = os.path.splitext(file.filename)
    with open(zip_path, "wb") as buffer:
        buffer.write(await file.read())
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(set_path)
        util.remove_ignore_files_and_directories()
    os.remove(zip_path)
    file_path = os.path.join(set_path, root_name)
    target_path = os.path.join(file_path, name + extension)
    if not os.path.exists(target_path):
        raise HTTPException(status_code=400, detail="Files Provided Are Incomplete")
    util.update_size(config.DIR_STORAGE_LOG, util.get_folder_size_in_gb(file_path))
    relative_root = os.path.relpath(file_path, config.DIR_RESOURCE).replace("\\", "/")
    return {"directory": f"{relative_root}/{name}{extension}"}


@api_router.post(f"{config.API_FS_RESOURCE_TIFF}")
async def upload_tiff(
    file: UploadFile = File(...), json_data: str = Form(...)
) -> dict[str, Any]:
    return await upload_structured_resource(
        file, json_data, config.DIR_RESOURCE_TIFF, ".tif"
    )


@api_router.post(f"{config.API_FS_RESOURCE_ADF}")
async def upload_adf(
    file: UploadFile = File(...), json_data: str = Form(...)
) -> dict[str, Any]:
    return await upload_structured_resource(
        file, json_data, config.DIR_RESOURCE_ADF, ".adf"
    )


@api_router.post(f"{config.API_FS_RESOURCE_JSON}")
async def upload_json_resource(
    file: UploadFile = File(...), json_data: str = Form(...)
) -> dict[str, Any]:
    return await upload_structured_resource(
        file, json_data, config.DIR_RESOURCE_JSON, ".json"
    )


@api_router.post(f"{config.API_FS_RESOURCE_GEOJSON}")
async def upload_geojson(
    file: UploadFile = File(...), json_data: str = Form(...)
) -> dict[str, Any]:
    return await upload_structured_resource(
        file, json_data, config.DIR_RESOURCE_GEOJSON, ".geojson"
    )


@api_router.post(f"{config.API_FS_RESOURCE_SHP}")
async def upload_shapefile(
    file: UploadFile = File(...), json_data: str = Form(...)
) -> dict[str, Any]:
    request_json = json.loads(json_data)
    segment = request_json["segment"]
    year = request_json["year"]
    set_name = request_json["set"]
    name = request_json["name"]
    set_path = os.path.join(config.DIR_RESOURCE_SHP, segment, year, set_name)
    os.makedirs(set_path, exist_ok=True)
    if not file.filename or not allowed_file(file.filename):
        raise HTTPException(status_code=400, detail="File Type Not Allowed")
    zip_path = os.path.join(set_path, file.filename)
    root_name, _ = os.path.splitext(file.filename)
    with open(zip_path, "wb") as buffer:
        buffer.write(await file.read())
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(set_path)
        util.remove_ignore_files_and_directories()
    os.remove(zip_path)
    file_path = os.path.join(set_path, root_name)
    shp_path = os.path.join(file_path, name + ".shp")
    required_extensions = [".shp", ".shx", ".prj", ".dbf"]
    if not os.path.exists(shp_path) or not all(
        util.contains_extension(file_path, ext) for ext in required_extensions
    ):
        raise HTTPException(status_code=400, detail="Files Provided Are Incomplete")
    util.update_size(config.DIR_STORAGE_LOG, util.get_folder_size_in_gb(file_path))
    return {"directory": f"shp/{segment}/{year}/{set_name}/{root_name}/{name}.shp"}


@api_router.post(f"{config.API_FS_RESOURCE_HYDRODYNAMIC}")
async def upload_hydrodynamic_zip(
    file: UploadFile = File(...), json_data: str = Form(...)
) -> dict[str, Any]:
    request_json = json.loads(json_data)
    segment = request_json["segment"]
    year = request_json["year"]
    set_name = request_json["set"]
    boundary_path = request_json["boundary"]
    set_path = os.path.join(config.DIR_RESOURCE_HYDRODYNAMIC, segment, year, set_name)
    os.makedirs(set_path, exist_ok=True)
    if not file.filename or not allowed_file(file.filename):
        raise HTTPException(status_code=400, detail="File Type Not Allowed")
    root_name, _ = os.path.splitext(file.filename)
    zip_path = os.path.join(set_path, file.filename)
    with open(zip_path, "wb") as buffer:
        buffer.write(await file.read())
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(set_path)
        util.remove_ignore_files_and_directories()
    os.remove(zip_path)
    file_path = os.path.join(set_path, root_name)
    if not all(
        util.contains_extension(file_path, ext) for ext in [".63", ".64", ".14"]
    ):
        raise HTTPException(status_code=400, detail="Files Provided Are Incomplete")
    return controllers.upload_hydrodynamic_resource(
        {
            "segment": segment,
            "year": year,
            "set": set_name,
            "name": root_name,
            "temp": request_json["temp"],
            "boundary": boundary_path,
        }
    )


@api_router.get(f"{config.API_FS_DISK_USAGE}")
async def get_disk_usage() -> dict[str, Any]:
    return controllers.get_disk_usage()


@api_router.get(f"{config.API_FS_RESOURCE_HYDRODYNAMIC_LIST}")
async def get_hydrodynamic_resource_list() -> dict[str, Any]:
    return controllers.get_hydrodynamic_resource_list()


@api_router.get(f"{config.API_VERSION}" + "/{category}/{model_name}")
async def get_model_runner(
    category: str, model_name: str, request: Request
) -> dict[str, Any]:
    api_path = f"{config.API_VERSION}/{category}/{model_name}"
    try:
        return controllers.handle_model_runner(api_path, dict(request.query_params))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@api_router.post(f"{config.API_VERSION}" + "/{category}/{model_name}")
async def post_model_runner(
    category: str, model_name: str, request: Request
) -> dict[str, Any]:
    api_path = f"{config.API_VERSION}/{category}/{model_name}"
    request_json: dict[str, Any] = {}
    try:
        request_json.update(await request.json())
    except Exception:
        pass
    try:
        form_data = await request.form()
        for key, value in form_data.items():
            if not isinstance(value, UploadFile):
                request_json[key] = value
    except Exception:
        pass
    try:
        return controllers.handle_model_runner(api_path, request_json)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
