from __future__ import annotations

import os
import shutil
import sys
import sysconfig
import tempfile
from pathlib import Path


def _normalize_runtime_path(path: str) -> str:
    if os.name != "nt":
        return path

    try:
        import ctypes

        buffer = ctypes.create_unicode_buffer(260)
        result = ctypes.windll.kernel32.GetShortPathNameW(path, buffer, len(buffer))
        if result:
            return buffer.value
    except Exception:
        pass

    return path


def _ensure_ascii_data_path(path: str, folder_name: str) -> str:
    if os.name != "nt":
        return path

    try:
        path.encode("ascii")
        return path
    except UnicodeEncodeError:
        pass

    source = Path(path)
    target_root = Path(tempfile.gettempdir()) / "bank-model-server-gdal-data"
    target = target_root / folder_name

    if target.exists():
        return str(target)

    target_root.mkdir(parents=True, exist_ok=True)
    shutil.copytree(source, target, dirs_exist_ok=True)
    return str(target)


def _existing_path(*parts: str) -> str | None:
    purelib = Path(sysconfig.get_paths().get("purelib", ""))
    if not purelib:
        return None

    path = purelib.joinpath(*parts)
    if path.exists():
        return str(path)
    return None


def configure_gdal_proj_env() -> None:
    proj_dir = _existing_path("osgeo", "data", "proj")
    if proj_dir is None:
        proj_dir = _existing_path("pyproj", "proj_dir", "share", "proj")

    gdal_data_dir = _existing_path("osgeo", "data", "gdal")

    if proj_dir:
        proj_dir = _ensure_ascii_data_path(proj_dir, "proj")
        proj_dir = _normalize_runtime_path(proj_dir)
        os.environ["PROJ_LIB"] = proj_dir
        os.environ["PROJ_DATA"] = proj_dir

    if gdal_data_dir:
        gdal_data_dir = _ensure_ascii_data_path(gdal_data_dir, "gdal")
        gdal_data_dir = _normalize_runtime_path(gdal_data_dir)
        os.environ["GDAL_DATA"] = gdal_data_dir


if sys.executable:
    configure_gdal_proj_env()
