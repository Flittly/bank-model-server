#!/usr/bin/env python3

from __future__ import annotations

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import config
from util.rustfs import normalize_resource_key, rustfs_configured, upload_resource_file


def iter_tiff_files(base_directory: Path) -> list[Path]:
    return sorted(
        [
            path
            for pattern in ("*.tif", "*.tiff")
            for path in base_directory.rglob(pattern)
            if path.is_file()
        ]
    )


def get_tiff_aux_files(tiff_file: Path) -> list[Path]:
    """
    获取 TIFF 文件相关的辅助文件列表。

    Args:
        tiff_file: TIFF 文件路径

    Returns:
        存在的辅助文件列表
    """
    aux_files = []
    tiff_base = tiff_file.stem
    tiff_name = tiff_file.name

    # 常见的 TIFF 辅助文件扩展名
    aux_patterns = [
        f"{tiff_base}.tfw",
        f"{tiff_name}.aux.xml",
        f"{tiff_name}.ovr",
    ]

    for pattern in aux_patterns:
        aux_file = tiff_file.parent / pattern
        if aux_file.exists() and aux_file.is_file():
            aux_files.append(aux_file)

    return aux_files


def import_tiff(base_directory: Path | None = None) -> list[str]:
    resource_root = Path(config.DIR_RESOURCE)
    source_directory = base_directory or Path(config.DIR_RESOURCE_TIFF) / "Mzs"
    source_directory = source_directory.resolve()

    if not source_directory.exists():
        raise FileNotFoundError(f"TIFF directory not found: {source_directory}")
    if not rustfs_configured():
        raise RuntimeError(
            "RustFS is not configured. Set RUSTFS_ENABLED, RUSTFS_ENDPOINT, "
            "RUSTFS_BUCKET, RUSTFS_ACCESS_KEY and RUSTFS_SECRET_KEY first."
        )

    uploaded_keys: list[str] = []
    tiff_files = iter_tiff_files(source_directory)
    print("=" * 60)
    print(f"Start importing TIFF files from {source_directory}")
    print("=" * 60)

    for tiff_file in tiff_files:
        # 上传主 TIFF 文件
        object_key = normalize_resource_key(tiff_file.relative_to(resource_root))
        upload_resource_file(str(tiff_file), object_key)
        uploaded_keys.append(object_key)
        print(f"Uploaded: {object_key}")

        # 上传相关的辅助文件
        aux_files = get_tiff_aux_files(tiff_file)
        for aux_file in aux_files:
            aux_object_key = normalize_resource_key(aux_file.relative_to(resource_root))
            upload_resource_file(str(aux_file), aux_object_key)
            uploaded_keys.append(aux_object_key)
            print(f"Uploaded: {aux_object_key}")

    print("-" * 60)
    print(f"Imported {len(uploaded_keys)} files to RustFS (including auxiliary files)")
    print("=" * 60)
    return uploaded_keys


if __name__ == "__main__":
    import_tiff()
