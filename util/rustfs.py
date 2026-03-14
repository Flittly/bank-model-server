# pyright: reportMissingImports=false
from __future__ import annotations

import mimetypes
import os
from functools import lru_cache
from pathlib import Path

import config


def _env_flag(name: str, default: bool = False) -> bool:
    """
    从环境变量中读取布尔值标志。
    
    Args:
        name: 环境变量名称
        default: 默认值，当环境变量不存在时返回
    
    Returns:
        bool: 如果环境变量值为 "1", "true", "yes", "on"（不区分大小写）则返回 True，否则返回 False
    """
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def rustfs_configured() -> bool:
    """
    检查 RustFS（基于 S3 兼容的对象存储）是否已正确配置。
    
    通过检查 RUSTFS_ENABLED 环境变量以及必要的 S3 连接参数（endpoint、bucket、access key、secret key）
    来确定 RustFS 是否可用。
    
    Returns:
        bool: 如果所有必要配置都存在且有效，则返回 True，否则返回 False
    """
    if not _env_flag("RUSTFS_ENABLED", False):
        return False
    required = [
        os.getenv("RUSTFS_ENDPOINT", "").strip(),
        os.getenv("RUSTFS_BUCKET", "").strip(),
        os.getenv("RUSTFS_ACCESS_KEY", "").strip(),
        os.getenv("RUSTFS_SECRET_KEY", "").strip(),
    ]
    return all(required)


@lru_cache(maxsize=1)
def get_rustfs_client():
    """
    获取 RustFS S3 客户端实例（使用 LRU 缓存确保单例）。
    
    创建并返回一个 boto3 S3 客户端，用于与 RustFS 对象存储进行交互。
    客户端配置包括 endpoint URL、认证信息、区域、SSL 设置等。
    
    Returns:
        boto3 S3 客户端实例
        
    Raises:
        RuntimeError: 当 RustFS 未配置时抛出异常
    """
    if not rustfs_configured():
        raise RuntimeError("RustFS is not configured")

    import boto3  # type: ignore[import-not-found]
    from botocore.config import Config as BotoConfig  # type: ignore[import-not-found]

    return boto3.session.Session().client(
        "s3",
        endpoint_url=os.getenv("RUSTFS_ENDPOINT", "").strip(),
        aws_access_key_id=os.getenv("RUSTFS_ACCESS_KEY", "").strip(),
        aws_secret_access_key=os.getenv("RUSTFS_SECRET_KEY", "").strip(),
        region_name=os.getenv("RUSTFS_REGION", "us-east-1").strip(),
        use_ssl=_env_flag("RUSTFS_SECURE", False),
        verify=not _env_flag("RUSTFS_SKIP_TLS_VERIFY", False),
        config=BotoConfig(
            signature_version="s3v4",
            s3={
                "addressing_style": os.getenv("RUSTFS_ADDRESSING_STYLE", "path").strip()
                or "path"
            },
        ),
    )


def get_rustfs_bucket() -> str:
    """
    获取 RustFS 存储桶名称。
    
    从 RUSTFS_BUCKET 环境变量中读取存储桶名称。
    
    Returns:
        str: RustFS 存储桶名称
        
    Raises:
        RuntimeError: 当 RUSTFS_BUCKET 环境变量未配置时抛出异常
    """
    bucket = os.getenv("RUSTFS_BUCKET", "").strip()
    if not bucket:
        raise RuntimeError("RUSTFS_BUCKET is not configured")
    return bucket


def get_rustfs_prefix() -> str:
    """
    获取 RustFS 对象键前缀。
    
    从 RUSTFS_PREFIX 环境变量中读取前缀，并去除首尾的斜杠。
    
    Returns:
        str: RustFS 对象键前缀（已清理格式）
    """
    return os.getenv("RUSTFS_PREFIX", "").strip().strip("/")


def normalize_resource_key(resource_path: str | os.PathLike[str]) -> str:
    """
    规范化资源路径，确保安全性并转换为统一格式。
    
    将输入的资源路径转换为相对于资源根目录的相对路径，并进行安全检查，
    防止路径遍历攻击。同时将路径分隔符统一为正斜杠。
    
    Args:
        resource_path: 资源路径（可以是字符串或 PathLike 对象）
        
    Returns:
        str: 规范化后的资源键（相对路径，使用正斜杠分隔）
        
    Raises:
        ValueError: 当路径为空、超出资源目录范围或包含路径遍历序列时抛出异常
    """
    raw = os.fspath(resource_path).strip()
    if not raw:
        raise ValueError("Resource path cannot be empty")

    resource_root = os.path.abspath(config.DIR_RESOURCE)
    if os.path.isabs(raw):
        absolute = os.path.abspath(raw)
        if not absolute.startswith(resource_root):
            raise ValueError(f"Path is outside resource directory: {raw}")
        raw = os.path.relpath(absolute, resource_root)

    normalized = raw.replace("\\", "/").lstrip("/")
    if normalized.startswith("resource/"):
        normalized = normalized[len("resource/") :]
    normalized = str(Path(normalized)).replace("\\", "/")
    if normalized in {"", "."}:
        raise ValueError(f"Invalid resource path: {resource_path}")
    if normalized.startswith("../") or "/../" in normalized:
        raise ValueError(f"Path escapes resource directory: {resource_path}")
    return normalized


def build_tiff_resource_key(
    segment: str,
    timepoint: str | int,
    set_name: str = "standard",
) -> str:
    """
    构建 TIFF 文件的资源键（对象存储路径）。
    
    根据河段名称、时间点和数据集名称构建标准的 TIFF 文件存储路径。
    时间点格式应为 YYYYMM 或 YYYYMMDD，路径结构为：tiff/{segment}/{year}/{set_name}/{timepoint}/{timepoint}.tif
    
    Args:
        segment: 河段标识符
        timepoint: 时间点（格式：YYYYMM 或 YYYYMMDD）
        set_name: 数据集名称，默认为 "standard"
        
    Returns:
        str: TIFF 文件的资源键路径
        
    Raises:
        ValueError: 当时间点格式无效时抛出异常
    """
    normalized_timepoint = str(timepoint).strip().replace("-", "")
    if len(normalized_timepoint) < 6 or not normalized_timepoint[:6].isdigit():
        raise ValueError(f"Invalid TIFF timepoint: {timepoint}")
    year = normalized_timepoint[:4]
    return f"tiff/{segment}/{year}/{set_name}/{normalized_timepoint}/{normalized_timepoint}.tif"


def build_tiff_resource_keys(
    segment: str,
    timepoint: str | int,
    set_name: str = "standard",
) -> list[str]:
    """
    构建 TIFF 文件的多个候选资源键。
    
    为给定的时间点生成主要的资源键，如果时间点是8位（YYYYMMDD格式），
    还会额外生成一个6位（YYYYMM格式）的月度候选键，以支持灵活的文件查找。
    
    Args:
        segment: 河段标识符
        timepoint: 时间点（格式：YYYYMM 或 YYYYMMDD）
        set_name: 数据集名称，默认为 "standard"
        
    Returns:
        list[str]: 候选资源键列表（按优先级排序）
        
    Raises:
        ValueError: 当时间点格式无效时抛出异常
    """
    normalized_timepoint = str(timepoint).strip().replace("-", "")
    if len(normalized_timepoint) < 6 or not normalized_timepoint[:6].isdigit():
        raise ValueError(f"Invalid TIFF timepoint: {timepoint}")

    candidates = [build_tiff_resource_key(segment, normalized_timepoint, set_name)]
    if len(normalized_timepoint) == 8:
        monthly_timepoint = normalized_timepoint[:6]
        monthly_candidate = build_tiff_resource_key(
            segment, monthly_timepoint, set_name
        )
        if monthly_candidate not in candidates:
            candidates.append(monthly_candidate)
    return candidates


def get_local_resource_path(resource_key: str) -> str:
    """
    获取资源在本地文件系统中的完整路径。
    
    将资源键（相对路径）与本地资源目录（config.DIR_RESOURCE）拼接，
    返回完整的本地文件路径。
    
    Args:
        resource_key: 资源键（规范化后的相对路径）
        
    Returns:
        str: 本地资源文件的完整路径
    """
    return os.path.join(config.DIR_RESOURCE, resource_key.replace("/", os.sep))


def get_cached_resource_path(resource_key: str) -> str:
    """
    获取资源在缓存目录中的完整路径。
    
    将资源键与缓存目录（config.DIR_RESOURCE_CACHE/rustfs）拼接，
    返回缓存文件的完整路径。
    
    Args:
        resource_key: 资源键（规范化后的相对路径）
        
    Returns:
        str: 缓存资源文件的完整路径
    """
    return os.path.join(
        config.DIR_RESOURCE_CACHE,
        "rustfs",
        resource_key.replace("/", os.sep),
    )


def get_remote_object_key(resource_key: str) -> str:
    """
    获取远程对象存储中的完整对象键。
    
    如果配置了前缀（RUSTFS_PREFIX），则在资源键前添加前缀，
    否则直接返回原始资源键。
    
    Args:
        resource_key: 资源键（规范化后的相对路径）
        
    Returns:
        str: 远程对象存储中的完整对象键
    """
    prefix = get_rustfs_prefix()
    if not prefix:
        return resource_key
    return f"{prefix}/{resource_key}"


def download_resource(resource_key: str, force: bool = False) -> str:
    """
    从 RustFS 下载资源到本地缓存。
    
    首先检查本地资源目录和缓存目录是否存在该资源，
    如果不存在且 RustFS 已配置，则从远程对象存储下载到缓存目录。
    
    Args:
        resource_key: 资源键（规范化后的相对路径）
        force: 是否强制重新下载（即使本地已存在）
        
    Returns:
        str: 下载后资源文件的本地路径（缓存路径）
        
    Raises:
        FileNotFoundError: 当资源不存在且 RustFS 未配置或下载失败时抛出异常
    """
    normalized_key = normalize_resource_key(resource_key)
    local_path = get_local_resource_path(normalized_key)
    if os.path.exists(local_path) and not force:
        return local_path

    cached_path = get_cached_resource_path(normalized_key)
    if os.path.exists(cached_path) and not force:
        return cached_path

    if not rustfs_configured():
        raise FileNotFoundError(
            f"Resource not found locally and RustFS is not configured: {normalized_key}"
        )

    os.makedirs(os.path.dirname(cached_path), exist_ok=True)
    client = get_rustfs_client()

    files_to_download = [normalized_key]

    if normalized_key.lower().endswith(".tif") or normalized_key.lower().endswith(".tiff"):
        tiff_base = normalized_key.rsplit(".", 1)[0]
        files_to_download.extend(
            [
                f"{tiff_base}.tfw",
                f"{normalized_key}.aux.xml",
                f"{normalized_key}.ovr"
            ]
        )

    for file_key in files_to_download:
        file_cached_path = get_cached_resource_path(file_key)
        if os.path.exists(file_cached_path):
            continue
        try:
            client.download_file(
                get_rustfs_bucket(),
                get_remote_object_key(file_key),
                file_cached_path
            )
        except Exception as exc:
            if os.path.exists(file_cached_path):
                os.remove(file_cached_path)
            raise FileNotFoundError(
                f"RustFS resource download failed: {normalized_key}"
            ) from exc
    return cached_path


def resolve_resource_path(
    resource_id: str | None,
    fallback_key: str | None = None,
) -> str:
    """
    解析资源路径，尝试多种位置查找资源。
    
    按优先级顺序在以下位置查找资源：
    1. 本地资源目录（config.DIR_RESOURCE）
    2. 本地缓存目录
    3. 远程 RustFS 对象存储（如果已配置）
    
    支持提供备选资源键作为回退选项。
    
    Args:
        resource_id: 主要资源标识符（可以是资源键或完整路径）
        fallback_key: 备选资源键（当主资源未找到时使用）
        
    Returns:
        str: 找到的资源文件的本地路径
        
    Raises:
        FileNotFoundError: 当所有位置都找不到资源时抛出异常
    """
    candidates: list[str] = []
    if fallback_key:
        candidates.append(fallback_key)
    if resource_id:
        candidates.append(resource_id)

    seen: set[str] = set()
    for candidate in candidates:
        if not candidate:
            continue
        normalized_key = normalize_resource_key(candidate)
        if normalized_key in seen:
            continue
        seen.add(normalized_key)

        local_path = get_local_resource_path(normalized_key)
        if os.path.exists(local_path):
            return local_path

        cached_path = get_cached_resource_path(normalized_key)
        if os.path.exists(cached_path):
            return cached_path

        if rustfs_configured():
            try:
                return download_resource(normalized_key)
            except FileNotFoundError:
                continue

    joined = ", ".join(seen) if seen else "<empty>"
    raise FileNotFoundError(f"Resource not found: {joined}")


def resolve_tiff_path(
    resource_id: str | None,
    *,
    segment: str,
    timepoint: str | int | None,
    set_name: str = "standard",
) -> str:
    """
    解析 TIFF 文件路径，支持显式指定或基于河段和时间点自动构建。
    
    如果提供了有效的 resource_id 且指向 TIFF 文件，则直接使用；
    否则根据 segment 和 timepoint 参数自动构建候选路径进行查找。
    
    Args:
        resource_id: 显式指定的资源标识符（可选）
        segment: 河段标识符（必需）
        timepoint: 时间点（可选，格式：YYYYMM 或 YYYYMMDD）
        set_name: 数据集名称，默认为 "standard"
        
    Returns:
        str: 找到的 TIFF 文件的本地路径
        
    Raises:
        FileNotFoundError: 当找不到 TIFF 文件时抛出异常
    """
    fallback_key = None
    fallback_candidates: list[str] = []
    normalized_resource_id = None
    if resource_id:
        normalized_resource_id = normalize_resource_key(resource_id)

    explicit_tiff_path = bool(
        normalized_resource_id
        and normalized_resource_id.lower().startswith("tiff/")
        and normalized_resource_id.lower().endswith(".tif")
    )

    if timepoint and not explicit_tiff_path:
        fallback_candidates = build_tiff_resource_keys(segment, timepoint, set_name)
        fallback_key = fallback_candidates[0]

    candidates: list[str | None] = []
    candidates.extend(fallback_candidates)
    candidates.append(normalized_resource_id or resource_id)

    last_error: FileNotFoundError | None = None
    for candidate in candidates:
        if not candidate:
            continue
        try:
            return resolve_resource_path(candidate)
        except FileNotFoundError as exc:
            last_error = exc

    if fallback_key is not None and resource_id:
        return resolve_resource_path(resource_id, fallback_key=fallback_key)
    if last_error is not None:
        raise last_error
    raise FileNotFoundError("Resource not found: <empty>")


def upload_resource_file(
    local_path: str | os.PathLike[str],
    object_key: str | None = None,
) -> str:
    """
    上传本地文件到 RustFS 对象存储。
    
    将指定的本地文件上传到 RustFS，并返回上传后的资源键。
    如果未指定 object_key，则使用本地路径作为资源键。
    
    Args:
        local_path: 本地文件路径
        object_key: 远程对象键（可选，如果未指定则使用本地路径）
        
    Returns:
        str: 上传后的资源键（规范化后的路径）
        
    Raises:
        RuntimeError: 当 RustFS 未配置时抛出异常
        FileNotFoundError: 当本地文件不存在时抛出异常
    """
    if not rustfs_configured():
        raise RuntimeError("RustFS is not configured")

    source = os.fspath(local_path)
    if not os.path.exists(source):
        raise FileNotFoundError(f"Local file not found: {source}")

    resource_key = normalize_resource_key(object_key or source)
    content_type = mimetypes.guess_type(source)[0] or "application/octet-stream"
    client = get_rustfs_client()
    client.upload_file(
        source,
        get_rustfs_bucket(),
        get_remote_object_key(resource_key),
        ExtraArgs={"ContentType": content_type},
    )
    return resource_key