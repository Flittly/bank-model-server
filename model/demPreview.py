"""DEM 栅格预览图：把单波段 DEM 渲染成「灰度高程 + 红色无效区」的 PNG。

为什么要这个模块
----------------
前端双击资源树里的 DEM 时，地图上只有一圈蓝色的「TIF 范围」矩形，看不出这块 DEM
哪里真有高程、哪里是空洞。而模型的 :class:`DemCoverageError` 恰恰是因为断面落在
DEM 的 nodata 上才报的 —— 一张预览图能让人在提交任务之前就看见这件事。

配色约定（与前端 ``demPreviewService.ts`` 的图例逐字对应，改这里必须同步改那边）
------------------------------------------------------------------------------
- 有效高程：低 → 深黑 :data:`PREVIEW_DARK`，高 → 浅灰 :data:`PREVIEW_LIGHT`
  （用「低暗高亮」而不是纯黑白：浅色底图上的纯白会整片消失）
- nodata / 非有限值：纯红 :data:`PREVIEW_NODATA`

为什么必须先 Warp 到 4326
-------------------------
前端用 Mapbox 的 ``image`` source 贴图，它只接受「矩形四角经纬度」这一个定位信息。
DEM 原始坐标系多为 3857 / 地方 2000 系，同一块矩形投到经纬度上不再是矩形，
直接降采样会让图与真实地理位置整体错位。Warp 到 4326 后输出本身就是经纬度轴对齐矩形，
四角坐标即 ``GetGeoTransform`` 算出来的 west/south/east/north。
"""

from __future__ import annotations

import hashlib
import io
import json
import os
from pathlib import Path
from typing import Any

import numpy as np

#: 预览图长边像素上限。1536 对「一眼看清水陆与空洞」足够，且单张 PNG 约 200~600 KB。
PREVIEW_MAX_SIZE = int(os.environ.get("DEM_PREVIEW_MAX_SIZE", "1536"))

#: 有效高程映射后的灰度端点（0~255）。不用 0/255：深黑会糊成一团，纯白在浅色底图上
#: 会整片融掉 —— 这个图是盖在天地图/影像底图上的，顶端必须留出与浅底色的差别。
PREVIEW_DARK = 14
PREVIEW_LIGHT = 208

#: nodata 的颜色 —— 纯红，和灰度区对比最强。
PREVIEW_NODATA = (230, 32, 32)

#: 灰度拉伸用的分位数（低/高）。用分位而非真实 min/max：DEM 里常有单点异常值，
#: 按 min/max 拉伸会把 99% 的地形压成同一个灰。
PREVIEW_LOW_PCT = 2.0
PREVIEW_HIGH_PCT = 98.0

#: 求分位数时的抽样上限（与出图侧 ``DEM_PERCENTILE_SAMPLES`` 同量级）。
PERCENTILE_SAMPLE_CAP = 200_000

#: 无 nodata 声明时的兜底哨兵值（与项目既有 ``-99999`` 约定一致）。
FALLBACK_NODATA = -99999.0


def cache_dir() -> Path:
    """预览图缓存目录：``resource/cache/dem_preview``。"""
    import config

    target = Path(getattr(config, "DIR_RESOURCE_CACHE", Path(config.DIR_RESOURCE) / "cache"))
    target = target / "dem_preview"
    target.mkdir(parents=True, exist_ok=True)
    return target


def _cache_key(path: Path, max_size: int) -> str:
    """按「绝对路径 + 修改时间 + 文件大小 + 尺寸上限」做键。

    mtime/size 必须进键里：同名 tif 被替换后如果仍命中旧缓存，用户看到的会是
    上一份 DEM 的图，而这种错配极难从界面上发现。
    """
    try:
        stat = path.stat()
        fingerprint = f"{path.resolve()}|{stat.st_mtime_ns}|{stat.st_size}|{max_size}"
    except OSError:
        fingerprint = f"{path}|?|?|{max_size}"
    return hashlib.md5(fingerprint.encode("utf-8")).hexdigest()[:20]


def _sample_percentile(values: np.ndarray, q: float) -> float:
    """在抽样后的有效值上求分位数，内存占用与数组大小无关。"""
    if values.size == 0:
        return float("nan")
    if values.size > PERCENTILE_SAMPLE_CAP:
        step = int(np.ceil(values.size / float(PERCENTILE_SAMPLE_CAP)))
        values = values[::step]
    return float(np.percentile(values, q))


def colorize(dem: np.ndarray, nodata: float | None) -> tuple[np.ndarray, dict[str, Any]]:
    """把单波段高程数组转成 ``(H, W, 3)`` 的 uint8 RGB 图。

    纯函数（不碰 GDAL / 文件），便于单测直接喂构造数组。

    Args:
        dem: 二维高程数组，行序与图像一致（第 0 行是北边）。
        nodata: 哨兵值，``None`` 表示只按非有限值判定无效。

    Returns:
        ``(rgb, stats)``；``stats`` 里 ``min``/``max`` 是真实有效高程极值（图例用），
        ``low``/``high`` 是实际用于拉伸的灰度端点。
    """
    arr = np.asarray(dem, dtype=np.float32)

    invalid = ~np.isfinite(arr)
    if nodata is not None:
        try:
            nodata_value = float(nodata)
        except (TypeError, ValueError):
            nodata_value = None
        if nodata_value is not None and np.isfinite(nodata_value):
            # 逐位相等即可：哨兵值是整型写进 float 的，不存在近邻误差的语义
            invalid |= arr == nodata_value

    valid = arr[~invalid]
    if valid.size == 0:
        # 整个 DEM 都是空的（上传错文件 / 裁剪错范围）：整张红，语义上完全正确
        rgb = np.empty(arr.shape + (3,), dtype=np.uint8)
        rgb[...] = PREVIEW_NODATA
        return rgb, {
            "min": None,
            "max": None,
            "low": None,
            "high": None,
            "nodata_ratio": 1.0,
        }

    low = _sample_percentile(valid, PREVIEW_LOW_PCT)
    high = _sample_percentile(valid, PREVIEW_HIGH_PCT)
    if not np.isfinite(low):
        low = float(valid.min())
    if not np.isfinite(high) or high <= low:
        # 平坦 DEM（整片同一高程）：给一个 1.0 的跨度，让它落成中灰而不是被除零打崩
        high = low + 1.0

    # 先把无效像素替换成 0 再算归一化：NaN 直接参与乘加会在 astype(uint8) 时
    # 触发 "invalid value encountered in cast"（结果虽然随后被红色覆盖，但会刷警告，
    # 且依赖"后面一定覆盖"这种隐含约定，重构时极易漏掉）。
    safe = np.where(invalid, 0.0, arr)
    normalized = np.clip((safe - low) / (high - low), 0.0, 1.0)
    gray = (PREVIEW_DARK + normalized * (PREVIEW_LIGHT - PREVIEW_DARK)).astype(np.uint8)
    rgb = np.repeat(gray[:, :, None], 3, axis=2)
    rgb[invalid] = PREVIEW_NODATA

    return rgb, {
        "min": float(valid.min()),
        "max": float(valid.max()),
        "low": low,
        "high": high,
        "nodata_ratio": float(invalid.mean()),
    }


def render_dem_preview(
    tiff_path: str | Path,
    *,
    max_size: int | None = None,
    use_cache: bool = True,
) -> tuple[bytes, dict[str, Any]]:
    """生成 DEM 预览 PNG。

    Args:
        tiff_path: DEM 文件路径。
        max_size: 输出长边像素上限，默认 :data:`PREVIEW_MAX_SIZE`。
        use_cache: 命中磁盘缓存时直接返回。

    Returns:
        ``(png_bytes, meta)``，``meta`` 里的地理范围可直接当 Mapbox ``image`` source 的
        ``coordinates`` 用。

    Raises:
        FileNotFoundError: 文件不存在。
        ValueError: 文件打不开 / 单波段读取失败 / Warp 失败。
    """
    from osgeo import gdal  # 延迟 import：只有真正预览时才拉起 GDAL

    gdal.UseExceptions()

    path = Path(tiff_path)
    if not path.exists():
        raise FileNotFoundError(f"DEM 文件不存在：{path}")

    size = int(max_size or PREVIEW_MAX_SIZE)
    key = _cache_key(path, size)
    png_file = cache_dir() / f"{key}.png"
    meta_file = cache_dir() / f"{key}.json"

    if use_cache and png_file.exists() and meta_file.exists():
        try:
            meta = json.loads(meta_file.read_text(encoding="utf-8"))
            meta["cached"] = True
            return png_file.read_bytes(), meta
        except (OSError, ValueError):
            # 缓存半截损坏（进程被杀在写盘中途）：当没有，重新生成
            pass

    dataset = gdal.Open(str(path))
    if dataset is None:
        raise ValueError(f"无法打开 DEM：{path}")

    try:
        src_w, src_h = dataset.RasterXSize, dataset.RasterYSize
        if src_w <= 0 or src_h <= 0:
            raise ValueError(f"DEM 尺寸非法：{src_w}x{src_h}（{path}）")

        src_band = dataset.GetRasterBand(1)
        if src_band is None:
            raise ValueError(f"DEM 没有第 1 个波段：{path}")
        src_nodata = src_band.GetNoDataValue()

        # 降采样：只缩不放（scale < 1 时保持原尺寸）
        scale = max(src_w, src_h) / float(size)
        ratio = max(scale, 1.0)
        out_w = max(1, int(round(src_w / ratio)))
        out_h = max(1, int(round(src_h / ratio)))

        dst_nodata = FALLBACK_NODATA
        if src_nodata is not None:
            try:
                candidate = float(src_nodata)
                if np.isfinite(candidate):
                    dst_nodata = candidate
            except (TypeError, ValueError):
                pass

        warp_kwargs: dict[str, Any] = {
            "dstSRS": "EPSG:4326",
            "format": "MEM",
            "width": out_w,
            "height": out_h,
            "resampleAlg": "bilinear",
            # 显式给 dstNodata：没有 nodata 声明的 DEM，Warp 出界处会补 0，
            # 0 会被当成「海平面附近的有效高程」而染成深黑，必须按 nodata 标红。
            "dstNodata": dst_nodata,
        }
        if src_nodata is not None:
            warp_kwargs["srcNodata"] = float(src_nodata)

        # 注意：这里**不能**加 targetAlignedPixels —— 它与显式 width/height 互斥，
        # GDAL 会以「对齐像素」为由改写输出尺寸，预览图就和四角坐标对不上了。
        warped = gdal.Warp("", dataset, **warp_kwargs)
        if warped is None:
            raise ValueError(f"DEM 重投影失败（可能缺少坐标系信息）：{path}")

        try:
            band = warped.GetRasterBand(1)
            arr = band.ReadAsArray()
            if arr is None:
                raise ValueError(f"DEM 读取失败：{path}")
            arr = np.asarray(arr, dtype=np.float32)

            rgb, stats = colorize(arr, dst_nodata)

            from PIL import Image

            buffer = io.BytesIO()
            # 不加 optimize：2.4 MP 的 PNG 上 optimize 会多花数百毫秒，压缩率收益不到 10%
            Image.fromarray(rgb, "RGB").save(buffer, format="PNG")
            png_bytes = buffer.getvalue()

            geo = warped.GetGeoTransform()
            # sorted 返回 (小, 大)：经纬度上「小 = 西/南，大 = 东/北」，变量必须按这个顺序接，
            # 否则南北颠倒 —— 图上会整体翻个个儿，而且只在跨纬度对照时才看得出来。
            west, east = sorted((geo[0], geo[0] + geo[1] * warped.RasterXSize))
            south, north = sorted((geo[3], geo[3] + geo[5] * warped.RasterYSize))

            meta: dict[str, Any] = {
                "west": float(west),
                "south": float(south),
                "east": float(east),
                "north": float(north),
                "width": int(warped.RasterXSize),
                "height": int(warped.RasterYSize),
                "source_width": int(src_w),
                "source_height": int(src_h),
                "nodata": float(dst_nodata) if dst_nodata is not None else None,
                "min": stats["min"],
                "max": stats["max"],
                "low": stats["low"],
                "high": stats["high"],
                "nodata_ratio": stats["nodata_ratio"],
                "cached": False,
            }
        finally:
            warped = None
    finally:
        dataset = None

    if use_cache:
        try:
            png_file.write_bytes(png_bytes)
            meta_file.write_text(json.dumps(meta, ensure_ascii=False), encoding="utf-8")
        except OSError as exc:
            # 缓存写不进去只影响下次速度，不影响这次出图
            print(f"[dem-preview] 缓存写入失败（忽略）：{exc}", flush=True)

    return png_bytes, meta
