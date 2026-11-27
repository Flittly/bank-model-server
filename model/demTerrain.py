"""DEM 三维地形瓦片：把一份 GeoTIFF 切成 Mapbox terrain 能直接吃的 Terrarium 金字塔。

为什么要这个模块
----------------
前端用 Mapbox GL 的 ``raster-dem`` 源 + ``setTerrain`` 做三维地形，它**只认一种输入**：
Web 墨卡托（XYZ）瓦片 + Terrarium 编码的 PNG。DEM 原始文件不满足任何一条 —— 多是地方投影
（EPSG:2437 / 3857）、单波段浮点、千万级像素、没有 overview。所以必须在后端一次性预处理。

为什么是「上传/首次使用时预生成」而不是按需切
----------------------------------------------
* 没有 overview 的千万级 DEM 整图读一次就是上百 MB 堆内存，用户拖一下地图会来几十张瓦片，
  按需切等于每次拖动都重算一遍；
* 模型服务的 CPU 是留给断面计算的（``MAX_RUNNING_MODEL_CASE_NUM``），不该被浏览行为抢走；
* 产物是「不可变的静态文件」：路径里带源文件指纹，HTTP 层可以放心设成长缓存。

产物长什么样
------------
``resource/tiles/dem/<指纹>/<z>/<x>/<y>.png`` + 同目录 ``metadata.json``。
指纹 = 「格式版本 + 绝对路径 + mtime + 大小」的 md5 前 16 位（与 :mod:`model.demPreview`
的缓存键同一思路）：源文件被替换、或本模块的算法变了，都会自然落到新目录，旧的留给
调用方清理。

nodata 怎么处理（这一段是本模块最需要读懂的地方）
--------------------------------------------------
Terrarium 没有「无效值」这个概念，Mapbox 也没有 —— 每个像素都必须是一个高程。
长江 DEM 的有效区是**一条蜿蜒的河道走廊**，外面包着大片 nodata，所以边界怎么填直接决定了
三维观感。做法叫「裙边」（apron）：

* 有效像素：原值不动；
* 无效像素：取最近的有效像素的邻域均值，再按到有效区的距离线性衰减到 0（``APRON_METRES``）。

于是走廊边缘不是一刀切的悬崖，而是一圈缓坡，坡脚正好接上 Mapbox 在「没有瓦片」处
假定的 0 米。走廊内部的小空洞同理被最近有效值补平（距离近 ⇒ 衰减权重 ≈ 1）。

裙边的「最近有效值」不逐像素算：先把窗口按 ``APRON_COARSE`` 缩小，在粗网格上做一次
跳洪最近邻，再双线性放大回去。裙边本来就是缓坡，不需要像素级精度；这样每张瓦片只多花
几毫秒，也不用引入 scipy（它不在 ``pyproject`` 里，加了 ``uv sync`` 会连带删锁）。

纯函数（编码、瓦片行列号、裙边）不依赖 GDAL，可以直接喂构造数组做单测
（``tests/test_dem_terrain.py``）；只有 :func:`build_terrain_pyramid` 真正碰 GDAL。
"""

from __future__ import annotations

import json
import math
import os
import re
import shutil
import time
from pathlib import Path
from typing import Any

import numpy as np

#: 产物格式版本。**算法或编码有变就加一** —— 指纹里带着它，改完老金字塔会自动失效。
#: v2：低层级瓦片改为「每层重采样到本层格网」，修掉缩略图分数像素导致的整体偏移。
TERRAIN_FORMAT_VERSION = 2

#: 瓦片边长（像素）。Mapbox 的 ``raster-dem`` 源按 256 请求。
TILE_SIZE = 256

#: 墨卡托世界宽（米）与原点：z0 一张瓦片铺满整个世界。
WORLD_WIDTH = 40075016.6855784
ORIGIN_X = -WORLD_WIDTH / 2
ORIGIN_Y = WORLD_WIDTH / 2

#: 金字塔最低层。从 0 开始只有个位数张瓦片，却能让任何缩放级别下地形都不「突然消失」。
MIN_ZOOM = int(os.environ.get("DEM_TERRAIN_MIN_ZOOM", "0"))

#: 最高层的硬上限（真实 DEM 30 m 对应 z12，10 m 对应 z14）。
MAX_ZOOM_CAP = int(os.environ.get("DEM_TERRAIN_MAX_ZOOM", "16"))

#: 顶层像素预算。超了就逐级降最高层，否则中间栅格会大到把盘写满。
MAX_PIXELS = int(os.environ.get("DEM_TERRAIN_MAX_PIXELS", "160000000"))

#: 裙边宽度（米）。2000 m 对应 z12 的约 52 像素。
APRON_METRES = float(os.environ.get("DEM_TERRAIN_APRON_METRES", "2000"))

#: 裙边最近邻的粗化倍数（见模块说明）。必须是 4 的倍数，才能与 256 对齐。
APRON_COARSE = 8

#: 裙边在低层级不小于这个像素数：高一层的一格是 2.4 km，2000 m 还不到一格，
#: 不兜底的话走廊边缘在 z8 以下会退化成悬崖。
MIN_APRON_PX = 3.0

_FP_RE = re.compile(r"^[0-9a-f]{16}$")


def tile_resolution(zoom: int) -> float:
    """某个缩放级别的地面分辨率（墨卡托米 / 像素）。"""
    return WORLD_WIDTH / (TILE_SIZE * (2**zoom))


def native_zoom(resolution_metres: float, *, min_zoom: int = MIN_ZOOM,
                max_zoom_cap: int = MAX_ZOOM_CAP) -> int:
    """刚好保住原生分辨率的那一层：``z = round(log2(156543 / 分辨率))``。

    取 ``round`` 而不是 ``ceil``：30 m 的 DEM 取 z13 会把像素数放大 2.5 倍，却没有任何
    新信息；取 z12（38 m/像素）对河床尺度（百米级）的地形足够。
    """
    if not (resolution_metres > 0) or not math.isfinite(resolution_metres):
        raise ValueError(f"DEM 像元大小非法：{resolution_metres}")
    zoom = int(round(math.log2((WORLD_WIDTH / TILE_SIZE) / resolution_metres)))
    return max(min_zoom, min(max_zoom_cap, zoom))


def tile_range(bounds: tuple[float, float, float, float], zoom: int) -> tuple[int, int, int, int]:
    """墨卡托外包框在某层盖到的瓦片行列号（含两端）。

    Args:
        bounds: ``(minx, miny, maxx, maxy)``，EPSG:3857 米。
        zoom: 缩放级别。

    Returns:
        ``(x0, x1, y0, y1)``，均为闭区间端点。
    """
    minx, miny, maxx, maxy = bounds
    res = tile_resolution(zoom)
    tile_metres = TILE_SIZE * res
    # 加一个远小于像素的 epsilon 再取整：外包框正好落在瓦片边上时，浮点误差会让
    # ``(ORIGIN_Y - y) / tile`` 算出「整数 − 1e-12」，floor 之后凭空多出一行。
    eps = 1e-9
    x0 = math.floor((minx - ORIGIN_X) / tile_metres + eps)
    x1 = math.floor((maxx - ORIGIN_X) / tile_metres + eps)
    # 纬度方向 y 轴朝下：北边（大 y）对应小行号
    y0 = math.floor((ORIGIN_Y - maxy) / tile_metres + eps)
    y1 = math.floor((ORIGIN_Y - miny) / tile_metres + eps)
    return x0, x1, y0, y1


def bounds_to_lonlat(bounds: tuple[float, float, float, float]) -> tuple[float, float, float, float]:
    """EPSG:3857 外包框 → ``(西, 南, 东, 北)``，给 Mapbox 的 ``bounds`` 用。

    纯三角公式，不走 PROJ：墨卡托 ↔ 经纬度是解析可逆的，少一层环境依赖。
    """
    minx, miny, maxx, maxy = bounds

    def lon(x: float) -> float:
        return x / (WORLD_WIDTH / 2.0) * 180.0

    def lat(y: float) -> float:
        return math.degrees(math.atan(math.sinh(y / (WORLD_WIDTH / (2.0 * math.pi)))))

    return lon(minx), lat(miny), lon(maxx), lat(maxy)


def encode_terrarium(elevation: np.ndarray) -> np.ndarray:
    """高程（米）→ Terrarium RGB（uint8，形状 ``H×W×3``）。

    编码公式（与 Mapbox 的解码器逐位对应）：
    ``v = round((h + 32768) * 256)``，``R = v >> 16``、``G = (v >> 8) & 255``、``B = v & 255``。
    精度 1/256 m，量程 −32768 ~ 32767.996 m。
    """
    arr = np.asarray(elevation, dtype=np.float64)
    if not np.isfinite(arr).all():
        raise ValueError("编码前必须先把 NaN/Inf 处理掉：Terrarium 没有无效值")
    value = np.rint((arr + 32768.0) * 256.0)
    value = np.clip(value, 0, 2**24 - 1).astype(np.uint32)
    rgb = np.empty(arr.shape + (3,), dtype=np.uint8)
    rgb[..., 0] = (value >> 16).astype(np.uint8)
    rgb[..., 1] = ((value >> 8) & 0xFF).astype(np.uint8)
    rgb[..., 2] = (value & 0xFF).astype(np.uint8)
    return rgb


def decode_terrarium(rgb: np.ndarray) -> np.ndarray:
    """Terrarium RGB → 高程（米）。只给单测和校验用。"""
    arr = np.asarray(rgb, dtype=np.float64)
    return (arr[..., 0] * 256.0 + arr[..., 1] + arr[..., 2] / 256.0) - 32768.0


def _block_sum(arr: np.ndarray, factor: int) -> np.ndarray:
    """按 ``factor×factor`` 块求和。输入尺寸必须是 factor 的整数倍。"""
    h, w = arr.shape
    return arr.reshape(h // factor, factor, w // factor, factor).sum(axis=(1, 3))


def _jump_flood_nearest(valid: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """粗网格上的最近有效格行/列号（跳洪法）。

    外面补一圈「无效」边，宽度取网格边长的一半：跳洪的挪移是环绕的，补了这圈之后，
    环绕过来的候选在**真实距离**上一定不是最近的，不会被误选。
    """
    h, w = valid.shape
    pad = max(h, w) // 2 + 1
    big = np.pad(valid, pad, constant_values=False)
    size = big.shape[0]
    large = np.iinfo(np.int32).max
    rows = np.full((size, size), large, dtype=np.int32)
    cols = np.full((size, size), large, dtype=np.int32)
    ys, xs = np.nonzero(big)
    rows[ys, xs] = ys
    cols[ys, xs] = xs

    yy, xx = np.mgrid[0:size, 0:size].astype(np.int32)

    def seed_dist(r: np.ndarray, c: np.ndarray) -> np.ndarray:
        out = np.full((size, size), np.float64(np.inf), dtype=np.float64)
        hit = (r != large) & (c != large)
        # 先算再做掩码：int32 的 large 没转 int64 就平方会溢出成负数
        rr = r[hit].astype(np.int64)
        cc = c[hit].astype(np.int64)
        out[hit] = np.hypot(yy[hit].astype(np.int64) - rr, xx[hit].astype(np.int64) - cc)
        return out

    # 初值必须是「到自己」的距离：有效格是 0，无效格是 inf。
    # 不先放自己进去的话，(0,0) 那一档永远被跳过，有效格最后会被邻居顶掉。
    dist = seed_dist(rows, cols)

    step = max(h, w)
    while step >= 1:
        for dy in (-step, 0, step):
            for dx in (-step, 0, step):
                if dy == 0 and dx == 0:
                    continue
                cand_r = np.roll(rows, (dy, dx), axis=(0, 1))
                cand_c = np.roll(cols, (dy, dx), axis=(0, 1))
                cand_d = seed_dist(cand_r, cand_c)
                better = cand_d < dist
                dist = np.where(better, cand_d, dist)
                rows = np.where(better, cand_r, rows)
                cols = np.where(better, cand_c, cols)
        step //= 2
    # 结果要减掉补边，换回原网格的行列号
    return rows[pad:pad + h, pad:pad + w] - pad, cols[pad:pad + h, pad:pad + w] - pad


def _upsample(arr: np.ndarray, factor: int, size: int) -> np.ndarray:
    """按整数倍双线性放大到 ``size×size``。"""
    n = arr.shape[0]
    t = (np.arange(size) + 0.5) / factor - 0.5
    i0 = np.clip(np.floor(t).astype(np.int64), 0, n - 1)
    i1 = np.clip(i0 + 1, 0, n - 1)
    weight = np.clip(t - i0, 0.0, 1.0)
    rows = arr[i0, :] * (1.0 - weight)[:, None] + arr[i1, :] * weight[:, None]
    return rows[:, i0] * (1.0 - weight)[None, :] + rows[:, i1] * weight[None, :]


def resolve_tile(mean: np.ndarray, apron_px: float, *, coarse: int = APRON_COARSE) -> np.ndarray:
    """由「带光晕的均值窗」算出一张瓦片的高程（中心 ``256×256``）。

    Args:
        mean: ``(256 + 2H) × (256 + 2H)``。有效像素 = 高程均值，无效像素 = NaN。
        apron_px: 裙边宽度（像素）。
        coarse: 粗化倍数，``2H`` 必须是它的整数倍。

    Returns:
        ``256×256`` 的 float32。**超出裙边范围的像素是 NaN**，调用方据此跳过整张瓦片。
    """
    size = mean.shape[0]
    halo = (size - TILE_SIZE) // 2
    if halo < 0 or size != TILE_SIZE + 2 * halo:
        raise ValueError(f"窗口尺寸 {size} 与瓦片 {TILE_SIZE} 对不上")
    if (2 * halo) % coarse != 0:
        raise ValueError(f"光晕 {halo} 的两倍必须能被粗化倍数 {coarse} 整除")
    if mean.shape[0] != mean.shape[1]:
        raise ValueError("窗口必须是方形的")

    mean = np.asarray(mean, dtype=np.float32)
    valid = np.isfinite(mean)
    lo, hi = halo, halo + TILE_SIZE
    if valid.all():
        return mean[lo:hi, lo:hi].copy()

    apron_px = max(float(apron_px), MIN_APRON_PX)

    # 1) 粗网格：每格的有效像素均值
    safe = np.where(valid, mean, 0.0).astype(np.float64)
    count = _block_sum(valid.astype(np.float64), coarse)
    if not (count > 0).any():
        return np.full((TILE_SIZE, TILE_SIZE), np.nan, dtype=np.float32)
    total = _block_sum(safe, coarse)
    coarse_value = np.full(count.shape, np.nan, dtype=np.float64)
    np.divide(total, count, out=coarse_value, where=count > 0)

    # 2) 粗网格最近邻，再放大回原分辨率
    near_row, near_col = _jump_flood_nearest(count > 0)
    n = coarse_value.shape[0]
    grid_y, grid_x = np.mgrid[0:n, 0:n].astype(np.float64)
    distance = np.hypot(grid_y - near_row, grid_x - near_col) * coarse
    value = coarse_value[near_row, near_col]
    value_up = _upsample(value, coarse, size)
    distance_up = _upsample(distance, coarse, size)

    # 减掉粗网格自身的量化误差：紧贴有效区的一圈权重应当接近 1，而不是 0
    distance_up = np.maximum(0.0, distance_up - coarse * 0.75)
    weight = np.clip(1.0 - distance_up / apron_px, 0.0, 1.0)

    filled = value_up * weight
    out = np.where(valid, mean, filled)
    out = np.where(valid | (weight > 0.0), out, np.nan)
    return out[lo:hi, lo:hi].astype(np.float32)


def source_fingerprint(tiff_path: str | Path) -> str:
    """源 DEM 指纹（16 位十六进制）：格式版本 + 绝对路径 + mtime + 大小。

    mtime/size 必须进指纹：同名 tif 被替换后如果还命中旧金字塔，用户看到的是上一份
    DEM 的地形，而这种错配在界面上根本看不出来。
    """
    path = Path(tiff_path)
    try:
        stat = path.stat()
        raw = f"v{TERRAIN_FORMAT_VERSION}|{path.resolve()}|{stat.st_mtime_ns}|{stat.st_size}"
    except OSError:
        raw = f"v{TERRAIN_FORMAT_VERSION}|{path.resolve()}|?|?"
    import hashlib

    return hashlib.md5(raw.encode("utf-8")).hexdigest()[:16]


def is_valid_fingerprint(value: str | None) -> bool:
    """只放行我们自己生成的 16 位十六进制指纹 —— 它会被拼进磁盘路径。"""
    return bool(value) and bool(_FP_RE.match(value))


def tile_dir(root: str | Path, fingerprint: str) -> Path:
    return Path(root) / fingerprint


def tile_path(root: str | Path, fingerprint: str, zoom: int, x: int, y: int) -> Path:
    return tile_dir(root, fingerprint) / str(zoom) / str(x) / f"{y}.png"


def _write_staging(src: Any, staging_path: Path, rows: int = 512) -> None:
    """源 DEM → 双波段暂存 GTiff：波段 1 = 高程（无效处置 0），波段 2 = 有效掩膜（0/1）。

    为什么要拆成两个波段：后面每一级降采样都走 GDAL 的线性重采样，而线性重采样是
    **不认 nodata** 的（不同 GDAL 版本对 NaN 的处理还不一样）。带着「和」与「个数」两个
    波段一起降采样，均值 = 和 ÷ 个数，天然等价于「只对有效像素求平均」，与版本无关。

    逐行条带写入：不把整幅 DEM 读进内存（千万级像素一读就是上百 MB）。
    """
    from osgeo import gdal

    gdal.UseExceptions()
    width, height = src.RasterXSize, src.RasterYSize
    band = src.GetRasterBand(1)
    nodata = band.GetNoDataValue()

    driver = gdal.GetDriverByName("GTiff")
    staging = driver.Create(
        str(staging_path), width, height, 2, gdal.GDT_Float32,
        options=["TILED=YES", "BLOCKXSIZE=256", "BLOCKYSIZE=256", "SPARSE_OK=YES"],
    )
    try:
        staging.SetGeoTransform(src.GetGeoTransform())
        staging.SetProjection(src.GetProjection())
        elev_band = staging.GetRasterBand(1)
        mask_band = staging.GetRasterBand(2)
        for y0 in range(0, height, rows):
            chunk = min(rows, height - y0)
            arr = np.asarray(band.ReadAsArray(0, y0, width, chunk), dtype=np.float32)
            valid = np.isfinite(arr)
            if nodata is not None and math.isfinite(float(nodata)):
                valid &= arr != np.float32(nodata)
            elev_band.WriteArray(np.where(valid, arr, 0.0), 0, y0)
            mask_band.WriteArray(valid.astype(np.float32), 0, y0)
        elev_band.FlushCache()
        mask_band.FlushCache()
    finally:
        staging = None


def _read_padded(band: Any, x: int, y: int, size: int) -> np.ndarray:
    """读 ``size×size`` 窗口，越界部分补 0（对掩膜来说 0 = 无效）。"""
    out = np.zeros((size, size), dtype=np.float32)
    max_x, max_y = band.XSize, band.YSize
    x0, y0 = max(0, x), max(0, y)
    x1, y1 = min(max_x, x + size), min(max_y, y + size)
    if x1 > x0 and y1 > y0:
        out[y0 - y:y1 - y, x0 - x:x1 - x] = band.ReadAsArray(x0, y0, x1 - x0, y1 - y0)
    return out


def _level_tiles(
    bounds: tuple[float, float, float, float], zoom: int, apron_metres: float
) -> tuple[int, int, int, int, float]:
    """某一层要出的瓦片范围（已含裙边圈数）与该层裙边的像素宽度。

    Returns:
        ``(x0, x1, y0, y1, apron_px)``，行列号均为闭区间。
    """
    tx0, tx1, ty0, ty1 = tile_range(bounds, zoom)
    apron_px = max(apron_metres / tile_resolution(zoom), MIN_APRON_PX)
    # 裙边只向外伸 ``ring`` 圈瓦片，再外面对 Mapbox 来说不存在
    ring = int(math.ceil((apron_px + TILE_SIZE) / TILE_SIZE))
    return tx0 - ring, tx1 + ring, ty0 - ring, ty1 + ring, apron_px


def _grid_bounds(zx0: int, zy0: int, zx1: int, zy1: int, zoom: int) -> tuple[float, float, float, float]:
    """一串瓦片在墨卡托下的外包框：``(minx, miny, maxx, maxy)``。"""
    tile_metres = TILE_SIZE * tile_resolution(zoom)
    return (
        ORIGIN_X + zx0 * tile_metres,
        ORIGIN_Y - (zy1 + 1) * tile_metres,
        ORIGIN_X + (zx1 + 1) * tile_metres,
        ORIGIN_Y - zy0 * tile_metres,
    )


def _warp_level(
    src_path: str | Path,
    dst_path: str | Path,
    zoom: int,
    tiles: tuple[int, int, int, int],
    *,
    ratio: float | None = None,
) -> None:
    """把源栅格重采样到「``tiles`` 这一串瓦片拼成的格网」上。

    输出尺寸恰好是 ``瓦片数 × 256``，且第 (0, 0) 个像素的左上角就是
    ``tiles`` 里左上角那张瓦片的左上角 —— 出瓦片时按 ``(x - zx0) * 256`` 取窗口即可，
    不再有分数像素偏移。``ratio`` 只对顶层（源是原始 DEM 分辨率）有意义：降采样超过
    2 倍时双线性会出锯齿，换成平均。
    """
    from osgeo import gdal

    gdal.UseExceptions()
    zx0, zx1, zy0, zy1 = tiles
    gdal.Warp(
        str(dst_path), str(src_path),
        dstSRS="EPSG:3857",
        format="GTiff",
        outputBounds=_grid_bounds(zx0, zy0, zx1, zy1, zoom),
        width=(zx1 - zx0 + 1) * TILE_SIZE,
        height=(zy1 - zy0 + 1) * TILE_SIZE,
        resampleAlg="bilinear" if (ratio is not None and ratio <= 2.0) else "average",
        multithread=True,
        creationOptions=["TILED=YES", "BLOCKXSIZE=256", "BLOCKYSIZE=256",
                         "COMPRESS=DEFLATE", "PREDICTOR=3"],
    )


def build_terrain_pyramid(
    tiff_path: str | Path,
    tiff_key: str,
    *,
    out_root: str | Path | None = None,
    min_zoom: int | None = None,
    max_zoom: int | None = None,
    apron_metres: float | None = None,
) -> dict[str, Any]:
    """把一份 DEM 切成 Terrarium 瓦片金字塔。

    Args:
        tiff_path: DEM 的本地路径（已经是本地文件，不走 RustFS 回源）。
        tiff_key: DEM 的资源键，只写进 ``metadata.json`` 备查。
        out_root: 输出根目录，默认 ``resource/tiles/dem``。
        min_zoom / max_zoom: 显式指定层级（单测用）；默认按原生分辨率推算。
        apron_metres: 裙边宽度，默认取 :data:`APRON_METRES`。

    Returns:
        与 ``metadata.json`` 同内容的字典。

    Raises:
        FileNotFoundError: 文件不存在。
        ValueError: 打不开 / 缺坐标系 / 单波段读取失败 / 重投影失败。
    """
    from osgeo import gdal, osr

    gdal.UseExceptions()
    import config

    started = time.time()
    path = Path(tiff_path)
    if not path.exists():
        raise FileNotFoundError(f"DEM 文件不存在：{path}")

    if out_root is None:
        out_root = os.path.join(config.DIR_RESOURCE, "tiles", "dem")
    tmp_dir = tmp_root() / f"{os.getpid()}_{int(started * 1000)}"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    staging_path = tmp_dir / "staging.tif"

    min_zoom = MIN_ZOOM if min_zoom is None else int(min_zoom)
    apron_metres = APRON_METRES if apron_metres is None else float(apron_metres)

    dataset = None
    try:
        dataset = gdal.Open(str(path))
        if dataset is None:
            raise ValueError(f"无法打开 DEM：{path}")
        if dataset.RasterCount < 1:
            raise ValueError(f"DEM 没有第 1 个波段：{path}")
        if not dataset.GetProjection():
            raise ValueError(f"DEM 缺少坐标系信息，无法切到 Web 墨卡托：{path}")

        fingerprint = source_fingerprint(path)
        out_dir = tile_dir(out_root, fingerprint)
        # 同指纹重建（强制重切）时先把旧目录挪走，而不是原地删：
        # 重建要花几十秒到几分钟，这期间旧瓦片应当继续可读。
        if out_dir.exists():
            shutil.move(str(out_dir), str(tmp_dir / "previous"))

        _write_staging(dataset, staging_path)

        staging = gdal.Open(str(staging_path))
        try:
            srs_3857 = osr.SpatialReference()
            srs_3857.ImportFromEPSG(3857)
            srs_3857.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
            # 传 WKT 而不是 SpatialReference 对象：GDAL 3.6 的绑定只认字符串，
            # WKT 按传统 GIS 轴序（东在前）解析，不用再操心轴序。
            # AutoCreateWarpedVRT 只是算出「重投影后的外包框与原生分辨率」，不搬数据
            warped = gdal.AutoCreateWarpedVRT(staging, None, srs_3857.ExportToWkt())
            try:
                gt = warped.GetGeoTransform()
                resolution = min(abs(gt[1]), abs(gt[5]))
                bounds = (
                    min(gt[0], gt[0] + gt[1] * warped.RasterXSize),
                    min(gt[3], gt[3] + gt[5] * warped.RasterYSize),
                    max(gt[0], gt[0] + gt[1] * warped.RasterXSize),
                    max(gt[3], gt[3] + gt[5] * warped.RasterYSize),
                )
            finally:
                warped = None
        finally:
            staging = None

        # 2) 定最高层：先按原生分辨率，再按像素预算逐级降
        zoom = native_zoom(resolution) if max_zoom is None else int(max_zoom)
        while zoom > min_zoom and _tile_grid(bounds, zoom)[4] > MAX_PIXELS:
            zoom -= 1
        max_zoom_actual = max(min_zoom, zoom)

        tx0, tx1, ty0, ty1, pixels = _tile_grid(bounds, max_zoom_actual)

        # 层级下限：再往下整份 DEM 缩到不足 1 像素，本来也放不下任何东西
        edge0 = _level_tiles(bounds, max_zoom_actual, apron_metres)
        span = max(edge0[1] - edge0[0] + 1, edge0[3] - edge0[2] + 1) * TILE_SIZE
        depth = max(1, int(math.floor(math.log2(max(span, 2)))))
        lowest_zoom = max(min_zoom, max_zoom_actual - depth)

        # 3) 逐层重采样 + 出瓦片。
        #
        # **每一层都重采样到「本层瓦片格网」上**，而不是开一张大栅格、再靠 GDAL 缩略图
        # 逐级读窗口：缩略图的像素原点是本层瓦片格网里的一个分数位置，而 `ReadAsArray`
        # 只能按整数像素取，取出来的窗口会整体偏移最多 255 px，越往低层级偏得越多。
        # 实测同一坐标 z12 是 −57 m、z11 只剩 0 m（偏了将近一整张瓦片），表现为
        # 「缩小时地形变平、放大又冒出来」。按层重采样后本层栅格的 (0,0) 恰好是
        # (zx0, zy0) 这张瓦片的左上角，偏移恒为整数，逐层都严丝合缝。
        tile_count = 0
        low, high = math.inf, -math.inf
        source_path = str(staging_path)
        for z in range(max_zoom_actual, lowest_zoom - 1, -1):
            k = max_zoom_actual - z
            zx0, zx1, zy0, zy1, apron_px = _level_tiles(bounds, z, apron_metres)
            level_path = tmp_dir / f"level-z{z}.tif"
            _warp_level(
                source_path, level_path, z, (zx0, zx1, zy0, zy1),
                ratio=(tile_resolution(z) / resolution) if k == 0 else None,
            )
            # 上一层只用来给本层重采样：出完这一层就删，临时目录里同时最多两张
            Path(source_path).unlink(missing_ok=True)
            source_path = str(level_path)

            level = gdal.Open(source_path)
            try:
                sum_band = level.GetRasterBand(1)
                count_band = level.GetRasterBand(2)

                halo = int(math.ceil(apron_px)) + 2 * APRON_COARSE
                halo += (-halo) % (APRON_COARSE // 2)  # 2*halo 必须能被 APRON_COARSE 整除
                size = TILE_SIZE + 2 * halo
                for x in range(zx0, zx1 + 1):
                    for y in range(zy0, zy1 + 1):
                        # 本层栅格原点就是瓦片 (zx0, zy0) 的左上角 ⇒ 偏移一定是整数
                        ox = (x - zx0) * TILE_SIZE - halo
                        oy = (y - zy0) * TILE_SIZE - halo
                        total = _read_padded(sum_band, ox, oy, size)
                        count = _read_padded(count_band, ox, oy, size)
                        mean = np.full(count.shape, np.nan, dtype=np.float32)
                        np.divide(total, count, out=mean, where=count > 1e-6)
                        if not np.isfinite(mean).any():
                            continue  # 整个光晕窗都没有有效像素 ⇒ 更远的也不必有
                        if z == max_zoom_actual:
                            # 高程极值只统计顶层的**原始有效像素**，裙边是算出来的，不算数
                            low = min(low, float(np.nanmin(mean)))
                            high = max(high, float(np.nanmax(mean)))
                        tile = resolve_tile(mean, apron_px)
                        if not np.isfinite(tile).any():
                            continue
                        tile = np.where(np.isfinite(tile), tile, 0.0)
                        target = tile_path(out_root, fingerprint, z, x, y)
                        target.parent.mkdir(parents=True, exist_ok=True)
                        _write_png(encode_terrarium(tile), target)
                        tile_count += 1
            finally:
                level = None

        west, south, east, north = bounds_to_lonlat(
            _grid_bounds(edge0[0], edge0[2], edge0[1], edge0[3], max_zoom_actual)
        )

        meta = {
            "format_version": TERRAIN_FORMAT_VERSION,
            "tiff_key": tiff_key,
            "fingerprint": fingerprint,
            "minzoom": lowest_zoom,
            "maxzoom": max_zoom_actual,
            "bounds": {"west": west, "south": south, "east": east, "north": north},
            "apron_metres": apron_metres,
            "tile_count": tile_count,
            "pixel_count": pixels,
            "min_elevation": None if low == math.inf else round(low, 3),
            "max_elevation": None if high == -math.inf else round(high, 3),
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "elapsed_sec": round(time.time() - started, 2),
        }
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "metadata.json").write_text(
            json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        return meta
    finally:
        dataset = None
        _remove_workdir(tmp_dir)


def tmp_root() -> Path:
    """切片过程的临时目录（暂存与中间栅格都落在这里）。"""
    import config

    return Path(getattr(config, "DIR_RESOURCE_CACHE", Path(config.DIR_RESOURCE) / "cache")) / "terrain_tmp"


def safe_rmtree(path: Path | str, *, label: str = "") -> bool:
    """尽力删目录，**保证不抛**；返回是否删干净。

    ⚠ 这里绝不能让异常逃出去。清理代码都跑在「业务已经成功、只差收尾」的位置上，
    一个清理失败（Windows 文件占用、某些环境对「一次删太多文件」直接抛 ``SystemExit``
    拦一道）会把整个后台线程打死，任务永远停在 ``pending``。

    实测踩过两次：一次是 270 个临时文件触发守卫（:func:`_remove_workdir`），
    另一次是切完新金字塔后删旧指纹目录（:func:`util.terrainService._drop_superseded`
    280 个瓦片）。``shutil.rmtree(ignore_errors=True)`` 只吞 ``OSError``，
    ``SystemExit`` 一路穿透 —— 所以两处都必须走这个函数。
    """
    import gc

    gc.collect()
    try:
        shutil.rmtree(path, ignore_errors=True)
    except BaseException as exc:  # noqa: BLE001 - 见上：清理失败不得反噬业务
        print(f"[dem-terrain] {label or '目录'}清理未完成（留给 sweep）：{path} — {type(exc).__name__}",
              flush=True)
        return False
    return not Path(path).exists()


def _remove_workdir(path: Path) -> None:
    """删掉切片临时目录（失败只留日志，交给 :func:`sweep_stale_tmp`）。"""
    safe_rmtree(path, label="临时目录")


def sweep_stale_tmp(max_age_hours: float = 6.0) -> int:
    """清掉残留的切片临时目录，返回清掉的个数。

    正常路径下 ``build_terrain_pyramid`` 结束时自己会清；会残留的只有进程被杀
    （部署重启、OOM）这一种情况。按「修改时间超过 N 小时」判定，不按 pid ——
    同一份数据卷上可能有多个模型服务实例在并行切。
    """
    root = tmp_root()
    if not root.is_dir():
        return 0
    removed = 0
    deadline = time.time() - max_age_hours * 3600
    for child in root.iterdir():
        try:
            if child.is_dir() and child.stat().st_mtime < deadline:
                if safe_rmtree(child, label="过期临时目录"):
                    removed += 1
        except OSError:
            continue
    return removed


def _tile_grid(bounds: tuple[float, float, float, float], zoom: int) -> tuple[int, int, int, int, int]:
    """某层的瓦片行列范围 + 像素总数（用来卡预算）。"""
    tx0, tx1, ty0, ty1 = tile_range(bounds, zoom)
    return tx0, tx1, ty0, ty1, (tx1 - tx0 + 1) * (ty1 - ty0 + 1) * TILE_SIZE * TILE_SIZE


def _write_png(rgb: np.ndarray, target: Path) -> None:
    """PNG 先写临时文件再原子替换：绝不让调用方读到半张瓦片。"""
    import tempfile

    from PIL import Image

    fd, tmp = tempfile.mkstemp(prefix=".tile-", suffix=".png", dir=str(target.parent))
    os.close(fd)
    try:
        Image.fromarray(rgb, "RGB").save(tmp, format="PNG")
        os.replace(tmp, target)
    except Exception:
        Path(tmp).unlink(missing_ok=True)
        raise
