"""按「场景」生成流场纹理。

背景与动机
----------
原先 `model/hydrodynamic_resource.py` 那条链路只认 ADCIRC 的 fort.14/63/64：
先算好算例、把三个 fort 文件传到模型服务，才可能出图。产物一律落在
`resource/hydrodynamic/<域>/<年>/<集合>/<工况>/renderResource/`，而且**与任务/DEM 没有任何关系**
—— 那 51 个工况是 2024-08 一次性算出来的固定素材，extent 写死在
经度 120.25~120.98 / 纬度 31.76~32.08（68.7 × 35.4 km），
而任务用的大区 DEM（`tiff/Yangtze/{2018,2023}/standard/1998-20xx/*.tif`）是 310 × 97 km，
流场只盖住包围盒的约 8%。

但**数据其实已经在库里**：`hydrodynamic_points` + `hydrodynamic_data` 里
`Yangtze / water_qs=84000 / tidal_level=1998-2018|1998-2023` 有 118 893 个点 × 26 帧，
覆盖 118.4313~121.7270 / 31.4543~32.3335，与大区 DEM 的边界逐位吻合。
本模块就是补上"从库里点云直接出纹理"这条路。

生成器复用的仍然是那个 C++ `TextureBuilder`，没有重写算法 ——
它真正吃的输入并不是 fort 文件，而是一份 json + 26 个 `N.txt`（列 `X Y H P U V`）
+ 一个矢量边界文件，`hydrodynamic_resource.py` 里那段 fort 解析只是上游适配器。
所以这里只要把点云按范围导出成 26 个 txt，就能生成任意范围的纹理。

"场景"是什么（scene id 的由来）
------------------------------
一个场景 = **一片 DEM 范围** × **一档流量** × **一种潮位** × 分辨率 的一套流场纹理。
所以 id 取的是这四样的哈希，而**不是** `tiff_key` 字符串：

    anchor = "bbox:<west>,<south>,<east>,<north>"   # DEM 经纬度范围，6 位小数
    scene_id = "f-" + sha1(anchor | water_qs | tidal_level | resolution)[:12]

这么定有两个实际原因：
  * 同一片区域的 DEM 往往有多个文件（`2018-Yangtze-uvhH.tif` 与 `2023-Yangtze-uvhH.tif`
    的范围逐位相同，只是地形年份不同），按文件哈希会白白生成两份一模一样的 190 MB 纹理；
  * 一个任务用 `bench_id` + `ref_id` 两个 DEM，它们通常是同一片区域，
    按范围哈希后两个 DEM 指向同一个场景。

`tiff_bounds` 里查不到范围时（DEM 未登记）退化成哈希 `tiff_key`，并在 `scene.json`
里把 `anchor` 记成 `tiff:<key>`，行为可追溯。
`scene.json` 还会记下 `tiff_keys`（所有范围与之相等的已登记 DEM），供前端显示"本场景覆盖哪些 DEM"。

产物结构（按场景 id 一文件夹，对应"按场景组织"的约定）
------------------------------------------------------
    resource/flowField/<scene-id>/
        raw/0..25.txt              导出给生成器的散点
        mask.geojson               边界（= 点云包围盒，保证栅格与网格严格重合）
        texture_builder.json       给 TextureBuilder 的 meta
        scene.json                 场景清单（输入参数、覆盖范围、水域占比…）
        renderResource/
            uv_0..25.png           流速纹理（每格左 u 右 v，各一个 float32 的 4 字节）
            mask_0.png             水域掩膜（每格两个 uint16 大端索引）
            height_0.png           高程灰图（前端暂未消费）
            projection_high/low_mapbox.png
            flow_field_description.json   生成器写出的描述，前端直接消费

关于掩膜（**这一条最容易踩**）
------------------------------
`TextureBuilder` 的掩膜来自矢量边界栅格化，边界只能是一个多边形，做不到"跟着 mesh 走"。
所以这里的做法是：
  1. 先给它一个**矩形**边界（= 点云包围盒）—— 目的只是让它算出正确的 boundary/grid，
     并使 `m_boundary` 与数据范围严格相等（否则掩膜栅格与速度网格会错半格）；
     这个矩形的 `Id` 必须是 **0**（`-i` 时 Id 是"多边形外部"的烧灼值，写 1 会让整张掩膜全通过）；
  2. **生成器跑完后，用点云覆盖重写 `mask_0.png`**：水域 = 该格里真的有点（点云就是水动力 mesh 的节点，
     节点在哪、水就在哪）。
这比用一个粗河道多边形去栅格化更贴近真实计算域，语义也与原产物完全一致
（水域格存自己的坐标、陆域格存一个随机水域格坐标）。

两处已修的生成器缺陷（2026-10-16，都在 `flowFieldTextureBuilder/source/FlowField.cpp`）
----------------------------------------------------------------------------------------
  * `buildBmpMask()` 原来每帧往成员 `m_maskOptions` 上追加 `-ts w h`，第 2 帧起就有两个 `-ts`，
    GDAL 直接拒绝栅格化（`Duplicate argument -ts`），`mask.bmp` 根本不存在；
    改成每帧在副本上追加、用完释放。
  * 构造函数里 `m_flowVelocityRange` / `m_flowPRange` / `m_flowHRange` / `m_terrainRange` /
    `m_boundary` 一个都没初始化，而 `updateFlowUV/updateFlowPH` 是拿它们做累积 min/max，
    实测 `v_max = 1.8e+184` ⇒ `v_norm` **整张纹理全为 0**。现按"反向极值 / 空框"给初值。

用法
----
    # 命令行：第一个参数传 "-" 时按上面的规则自己算 id
    python model/flowFieldSceneResource.py - tiff/Yangtze/2018/standard/1998-2018/2018-Yangtze-uvhH.tif 84000 1998-2018
    # 也可以显式给 id（幂等：scene.json 存在且输入一致就直接复用）
    python model/flowFieldSceneResource.py f-xxxxxxxxxxxx <tiff_key> <water_qs> <tidal_level> [resolution]

Python 侧入口：
    scene_id_for(tiff_key, water_qs, tidal_level, resolution)   算 id（会查 tiff_bounds）
    resolve_scene(tiff_key, water_qs, tidal_level)              只查已生成的场景，不生成
    scenes_for_dem(tiff_key)                                    这片 DEM 下所有已生成的场景
    build_scene(scene_id, ...)                                  生成（已存在则复用）

`tiff_key` 用于两件事：**定场景 id**（按它的经纬度范围）与**核对覆盖范围**
（数据是否真的盖住这个 DEM）；栅格范围本身仍由点云决定。
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import subprocess
import sys
import time
from typing import Any

import numpy as np

import config
import util

# 与 hydrodynamic_resource.py 同一套 UTF-8 处理：必须用 reconfigure 原地改编码，
# 不能新建 TextIOWrapper 覆盖 sys.stdout（会让 pytest 的捕获流在收尾时崩）。
def _ensure_utf8_stdout() -> None:
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8")
        except Exception:
            continue


_ensure_utf8_stdout()

module_path = os.path.abspath(os.path.join(os.getcwd()))
if module_path not in sys.path:
    sys.path.append(module_path)

from util import db  # noqa: E402  (必须在 sys.path 处理之后)


# ==================== 常量 ====================

#: 场景产物根目录。刻意与 `resource/hydrodynamic/**`（那批固定素材）分开，
#: 免得"按需生成"的东西和"一次性素材"混在一个目录里。
SCENE_ROOT = os.path.join(config.DIR_RESOURCE, "flowField")

#: 工作坐标系。点云 `hydrodynamic_points.x/y` 本身存的就是这个坐标系，
#: 而它正好是等角投影（每格在实地近似正方形），所以栅格用它是合适的。
WORKING_EPSG = 2437

#: txt 的列序，必须与 meta 里的 headings 一致。
HEADINGS = ["X", "Y", "H", "P", "U", "V"]

#: 默认水情组合（库里覆盖大区 DEM 的那一套）。
DEFAULT_DOMAIN = "Yangtze"
DEFAULT_SET = "standard"

#: 生成器单帧的散点上限不做限制，但描述里要写明用了多少帧。
MAX_FRAMES = 64


# ==================== 工具 ====================


#: DEM 范围参与哈希时保留的小数位。`tiff_bounds` 存的是 float，不同来源可能差最后几位；
#: round 到 6 位（约 0.1 m）既吸收这种抖动，又不会把两片真正不同的 DEM 并成一个场景。
BOUNDS_PRECISION = 6


def canonical_bounds(bounds) -> str:
    """把 DEM 经纬度包围盒写成稳定的锚点字符串。"""
    return "bbox:" + ",".join(f"{float(v):.{BOUNDS_PRECISION}f}" for v in bounds)


def scene_anchor(tiff_key: str) -> str:
    """场景锚点：优先用 DEM 的经纬度范围，查不到才退回 `tiff_key`。

    返回 `"bbox:<west>,<south>,<east>,<north>"` 或 `"tiff:<tiff_key>"`。
    之所以不直接用 `tiff_key` 当锚点，是因为同一片区域的 DEM 常有多个文件
    （`2018-Yangtze-uvhH.tif` 与 `2023-Yangtze-uvhH.tif` 范围逐位相同），
    按文件哈希会重复生成两份完全一样的纹理。
    """
    bounds = dem_bounds_lonlat(tiff_key)
    return canonical_bounds(bounds) if bounds else f"tiff:{tiff_key}"


def tiff_keys_for_bounds(bounds) -> list[str]:
    """查出所有登记范围与给定包围盒相同的 DEM，供 `scene.json` 记录"本场景覆盖哪些 DEM"。"""
    if not bounds:
        return []
    with db.get_db_cursor() as (_, cursor):
        cursor.execute(
            """
            SELECT tiff_key FROM tiff_bounds
            WHERE round(ST_XMin(geom)::numeric, %s) = round(%s::numeric, %s)
              AND round(ST_YMin(geom)::numeric, %s) = round(%s::numeric, %s)
              AND round(ST_XMax(geom)::numeric, %s) = round(%s::numeric, %s)
              AND round(ST_YMax(geom)::numeric, %s) = round(%s::numeric, %s)
            ORDER BY tiff_key
            """,
            (
                BOUNDS_PRECISION, float(bounds[0]), BOUNDS_PRECISION,
                BOUNDS_PRECISION, float(bounds[1]), BOUNDS_PRECISION,
                BOUNDS_PRECISION, float(bounds[2]), BOUNDS_PRECISION,
                BOUNDS_PRECISION, float(bounds[3]), BOUNDS_PRECISION,
            ),
        )
        return [str(row[0]) for row in cursor.fetchall()]


def _hash_scene_id(anchor: str, water_qs: str, tidal_level: str, resolution: int) -> str:
    spec = "|".join([anchor, str(water_qs), str(tidal_level), str(int(resolution))])
    return "f-" + hashlib.sha1(spec.encode("utf-8")).hexdigest()[:12]


def scene_id_for(tiff_key: str, water_qs: str, tidal_level: str, resolution: int = 1024) -> str:
    """由「DEM 范围 + 水情 + 分辨率」推一个稳定的场景 id。

    同一组输入永远得到同一个 id ⇒ 多个任务共用同一片 DEM 时不会重复生成。
    用哈希而不是拼接字符串，是因为锚点里带逗号与路径分隔符，直接拼会变成非法目录名。
    """
    return _hash_scene_id(scene_anchor(tiff_key), water_qs, tidal_level, resolution)


def scene_dir(scene_id: str) -> str:
    return os.path.join(SCENE_ROOT, scene_id)


def _builder_path() -> str:
    """TextureBuilder 可执行文件位置。

    与 `hydrodynamic_resource.py` 里拼的路径保持一致（build/windows|macos/TextureBuilder）。
    目录不存在时报错要说清是"没构建"，而不是让它报 302/文件找不到。
    """
    platform = util.get_os()
    if platform == "Windows":
        path = os.path.join(config.DIR_MODEL, "flowFieldTextureBuilder", "build", "windows", "TextureBuilder")
    elif platform == "MacOS":
        path = os.path.join(config.DIR_MODEL, "flowFieldTextureBuilder", "build", "macos", "TextureBuilder")
    else:
        path = os.path.join(config.DIR_MODEL, "flowFieldTextureBuilder", "build", "TextureBuilder")
    if not os.path.exists(path) and not os.path.exists(path + ".exe"):
        raise FileNotFoundError(
            f"没找到流场纹理生成器：{path}\n"
            "Windows 上先跑 model/flowFieldTextureBuilder/build-windows.bat 构建。"
        )
    return path


def _builder_env() -> dict[str, str]:
    """给生成器准备运行时环境。

    两个坑：
    1. 它链接的是 OSGeo4W 的 GDAL，运行时要能找到 gdal3xx.dll ⇒ 把 OSGeo4W\\bin 加到 PATH；
    2. PROJ 的数据目录必须显式指定。默认搜索会命中 PostgreSQL/PostGIS 自带的那个老
       `proj.db`（layout MINOR=2），而 GDAL 3.12 要求 >= 4，于是
       `OGRCreateCoordinateTransformation` 直接失败 —— 表面现象是 extent 变成 NaN。
       这里优先用 pyproj 自带的数据目录（版本一定够新）。
    """
    env = os.environ.copy()
    osgeo_bin = os.environ.get("OSGEO4W_ROOT", r"C:\OSGeo4W") + os.sep + "bin"
    if os.path.isdir(osgeo_bin):
        env["PATH"] = osgeo_bin + os.pathsep + env.get("PATH", "")
    proj_share = None
    try:
        import pyproj

        proj_share = pyproj.datadir.get_data_dir()
    except Exception:
        proj_share = None
    if not proj_share:
        candidate = os.path.join(osgeo_bin, "..", "share", "proj")
        proj_share = candidate if os.path.isdir(candidate) else None
    if proj_share:
        env["PROJ_DATA"] = proj_share
        env["PROJ_LIB"] = proj_share
    return env


# ==================== 取数 ====================


def load_points(
    *,
    dataset_domain: str = DEFAULT_DOMAIN,
    set_name: str = DEFAULT_SET,
    water_qs: str,
    tidal_level: str,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """取一批水动力点。

    Returns:
        `(point_ids, x, y)` —— `x/y` 是工作坐标系（EPSG:2437）下的米。
        直接取库里的 `x/y` 列，而不是拿 `geom`（4326）再投影：
        这两个列本来就是同一批坐标的投影结果，少一次 12 万点的投影换算。
    """
    with db.get_db_cursor() as (_, cursor):
        cursor.execute(
            """
            SELECT id, x, y
            FROM hydrodynamic_points
            WHERE dataset_domain = %s
              AND set_name = %s
              AND water_qs = %s
              AND tidal_level = %s
              AND temp = FALSE
            ORDER BY id
            """,
            (dataset_domain, set_name, water_qs, tidal_level),
        )
        rows = cursor.fetchall()
    if not rows:
        raise ValueError(
            f"库里没有这组水情的数据：{dataset_domain}/{set_name}/{water_qs}/{tidal_level}"
        )
    point_ids = np.array([r[0] for r in rows], dtype=np.int64)
    x = np.array([float(r[1]) for r in rows], dtype=np.float64)
    y = np.array([float(r[2]) for r in rows], dtype=np.float64)
    return point_ids, x, y


def load_series(
    point_ids: np.ndarray,
    *,
    dataset_domain: str = DEFAULT_DOMAIN,
    set_name: str = DEFAULT_SET,
    water_qs: str,
    tidal_level: str,
) -> tuple[np.ndarray, dict[str, np.ndarray]]:
    """取这批点的时间序列。

    Returns:
        `(time_steps, fields)`，`fields` 含 `h/p/u/v`，形状都是 `(帧数, 点数)`。
        **必须按帧拼**：生成器是一帧一个 txt，所以这里一次把矩阵摆好，
        免得后面 26 次分别查库。

    两个实现要点：
    * 用 **服务端游标**（`conn.cursor(name=...)`）。大区那一套是 118 893 × 26 ≈ 309 万行，
      普通游标会把这 309 万行全缓存到客户端内存里（GB 量级）。
    * 用 JOIN 而不是 `WHERE point_id = ANY(...)` —— 后者的 ARRAY 字面量要拼 12 万个 id。
    """
    ids = [int(v) for v in point_ids]
    index_of = {pid: i for i, pid in enumerate(ids)}
    n = len(ids)
    fields: dict[str, np.ndarray] = {}

    with db.get_db_connection() as conn:
        with conn.cursor(name="flow_scene_series") as cursor:
            cursor.itersize = 200000
            cursor.execute(
                """
                SELECT hd.point_id, hd.time_step, hd.h, hd.p, hd.u, hd.v
                FROM hydrodynamic_data hd
                JOIN hydrodynamic_points hp ON hp.id = hd.point_id
                WHERE hp.dataset_domain = %s
                  AND hp.set_name = %s
                  AND hp.water_qs = %s
                  AND hp.tidal_level = %s
                  AND hp.temp = FALSE
                ORDER BY hd.time_step
                """,
                (dataset_domain, set_name, water_qs, tidal_level),
            )
            steps: list[int] = []
            step_index: dict[int, int] = {}
            for point_id, time_step, h, p, u, v in cursor:
                step = int(time_step)
                fi = step_index.get(step)
                if fi is None:
                    fi = len(steps)
                    step_index[step] = fi
                    steps.append(step)
                    for key in ("h", "p", "u", "v"):
                        fields.setdefault(key, []).append(np.zeros(n, dtype=np.float64))
                slot = index_of.get(int(point_id))
                if slot is None:
                    continue
                fields["h"][fi][slot] = float(h)
                fields["p"][fi][slot] = float(p)
                fields["u"][fi][slot] = float(u)
                fields["v"][fi][slot] = float(v)

    if not steps:
        raise ValueError("库里这组水情没有任何时间序列")
    if len(steps) > MAX_FRAMES:
        raise ValueError(f"帧数过多：{len(steps)} > {MAX_FRAMES}")
    stacked = {key: np.stack(value, axis=0) for key, value in fields.items()}
    return np.array(steps, dtype=np.int64), stacked


def dem_bounds_lonlat(tiff_key: str) -> tuple[float, float, float, float] | None:
    """取 DEM 的经纬度包围盒，仅用于核对覆盖范围。取不到就返回 None（不阻塞生成）。"""
    if not tiff_key:
        return None
    with db.get_db_cursor() as (_, cursor):
        cursor.execute(
            """
            SELECT ST_XMin(geom), ST_YMin(geom), ST_XMax(geom), ST_YMax(geom)
            FROM tiff_bounds
            WHERE tiff_key = %s
            ORDER BY id DESC
            LIMIT 1
            """,
            (tiff_key,),
        )
        row = cursor.fetchone()
    if not row or row[0] is None:
        return None
    return tuple(float(v) for v in row)  # type: ignore[return-value]


# ==================== 栅格几何（必须与 C++ 侧逐位一致） ====================


def grid_shape(x_min: float, x_max: float, y_min: float, y_max: float, resolution: int) -> tuple[int, int, float]:
    """复刻 `FlowField::buildTextures()` 里的网格算法。

    C++ 那边是：
        dl = max(dx, dy)
        ds = dl / resolution
        nx = dx / ds ; ny = dy / ds          # 长边正好 = resolution 格
        dd = max(dx/nx, dy/ny)               # 两个方向共用同一步长
    这里**必须一模一样**，否则掩膜会和速度纹理错位。
    返回 `(nx, ny, dd)`。任何一处不等，后面 `assert_grid_shape()` 会当场报出来。
    """
    dx = x_max - x_min
    dy = y_max - y_min
    if dx <= 0 or dy <= 0:
        raise ValueError(f"点云包围盒退化：dx={dx}, dy={dy}")
    dl = dx if dx > dy else dy
    ds = dl / float(resolution)
    nx = int(dx / ds)
    ny = int(dy / ds)
    if nx < 2 or ny < 2:
        raise ValueError(f"网格太小：{nx} x {ny}，请提高 resolution")
    dd = max(dx / nx, dy / ny)
    return nx, ny, dd


# ==================== 掩膜 ====================


def coverage_mask(
    x: np.ndarray,
    y: np.ndarray,
    x_min: float,
    y_min: float,
    nx: int,
    ny: int,
    dd: float,
) -> np.ndarray:
    """点云覆盖 → 水域掩膜。形状 `(ny, nx)`，`iy = 0` 在最南。

    判定就是"这一格里有没有点"。散点就是水动力 mesh 的节点，节点在哪水就在哪 ——
    比拿一个粗河道多边形去栅格化更贴近真实计算域。
    """
    ix = np.floor((x - x_min) / dd).astype(np.int64)
    iy = np.floor((y - y_min) / dd).astype(np.int64)
    np.clip(ix, 0, nx - 1, out=ix)
    np.clip(iy, 0, ny - 1, out=iy)
    water = np.zeros((ny, nx), dtype=bool)
    water[iy, ix] = True
    return water


def fill_single_cell_holes(water: np.ndarray) -> np.ndarray:
    """把被水域完全包围的单格空洞补上。

    303 m 的格子比 mesh 稀疏得多，河道边缘偶尔会漏一格；那一格会让粒子在里面
    立刻判越界重生，观感上是一小片闪烁。只补"8 邻域里 >=7 个是水"的孤立陆格，
    不会把河道加宽。
    """
    ny, nx = water.shape
    padded = np.pad(water, 1, constant_values=False)
    neighbour_count = np.zeros_like(water, dtype=np.int8)
    for dy in (-1, 0, 1):
        for dx in (-1, 0, 1):
            if dx == 0 and dy == 0:
                continue
            neighbour_count += padded[1 + dy : 1 + dy + ny, 1 + dx : 1 + dx + nx]
    return water | (~water & (neighbour_count >= 7))


def write_mask_png(path: str, water: np.ndarray, *, seed: int = 20261016) -> dict[str, Any]:
    """把水域判定写成生成器约定的掩膜 PNG。

    编码（与 C++ 侧 `toTextureGRID_uv` 完全一致）：
    * 水域格 → 两个 16 位**大端**索引 `(ix, iy)`，即自己的坐标；
    * 陆域格 → 一个**随机水域格**的坐标（"粒子飘上岸就重生"的查找表）。
    * `iy` 是**网格行号，0 在最南**；PNG 第 0 行是最北 ⇒ 写到 `ny-1-iy` 行。
      这一条与消费端着色器里的翻转互为镜像，改任何一边都要同时改另一边。
    """
    from PIL import Image

    ny, nx = water.shape
    iy, ix = np.nonzero(water)
    if ix.size == 0:
        raise ValueError("掩膜里一个水域格都没有，检查点云是否落进了包围盒")
    rng = np.random.default_rng(seed)
    pick = rng.integers(0, ix.size, size=ny * nx)

    mask = np.empty((ny, nx, 4), dtype=np.uint8)
    # 先整张铺成"随机水域格"
    mask[:, :, 0] = (ix[pick] >> 8).astype(np.uint8).reshape(ny, nx)
    mask[:, :, 1] = (ix[pick] & 0xFF).astype(np.uint8).reshape(ny, nx)
    mask[:, :, 2] = (iy[pick] >> 8).astype(np.uint8).reshape(ny, nx)
    mask[:, :, 3] = (iy[pick] & 0xFF).astype(np.uint8).reshape(ny, nx)
    # 再覆盖成"自指"（水域格）
    mask[iy, ix, 0] = (ix >> 8).astype(np.uint8)
    mask[iy, ix, 1] = (ix & 0xFF).astype(np.uint8)
    mask[iy, ix, 2] = (iy >> 8).astype(np.uint8)
    mask[iy, ix, 3] = (iy & 0xFF).astype(np.uint8)

    # iy=0 在最南 ⇒ 翻到 ny-1 行（PNG 第 0 行在北）
    flipped = mask[::-1]
    Image.fromarray(flipped, mode="RGBA").save(path, optimize=True)
    return {
        "water_cells": int(ix.size),
        "total_cells": int(nx * ny),
        "water_ratio": round(float(ix.size) / float(nx * ny), 6),
    }


def write_mask_geojson(path: str, bounds: tuple[float, float, float, float]) -> None:
    """写一个矩形边界文件。

    它**只用来让生成器算出正确的 boundary 与网格**（那个 C++ 只认矢量文件，
    没有"直接给范围"的入口）。真正的掩膜在生成之后由点云覆盖重写。
    坐标直接写工作坐标系的米（不是经纬度），因为生成器是拿 `vector_mask` 的
    envelope 去和数据坐标做比较的，两边必须同坐标系。

    `Id` 必须是 **0**，不能是 1 —— 这是 2026-10-16 实测出来的语义（不是猜的）：

        FlowField::createVectorMask() 给的选项是
            -i -a Id -l <layer> -init 255 -of BMP -a_srs <wkt> -ot byte
        实测（C:\\OSGeo4W\\bin\\gdal_rasterize，同一份选项表）：
            多边形**内部** → 保留 -init 的 255
            多边形**外部** → 烧成属性 `Id` 的值
        而 bmpMaskPass = m_bmpMask[...] 取的是"非零即通过"。
        所以：Id=0 时「多边形内=255=水(通过)、外=0=陆(不通过)」，语义正确；
             Id=1 时外部也变成 1，**整张图全通过**，掩膜彻底失效。
        参考实现（liuchang）的边界 `bjm_1998.shp` 只有一个字段 `Id: Integer`，
        值为 0 —— 与此完全一致。

    注意我们这个矩形是**整个点云包围盒**，没有"外部"，所以生成器内部的 bmpMask
    实际是全 255（全通过）。这是有意接受的：它只影响生成器自己写的那张
    `mask_0.png`（我们随后用点云覆盖重写）与它内部的"随机重生池"，
    不影响落在水域格里的速度值。换成贴合水域的窄多边形反而会让 `m_boundary`
    缩小 ⇒ 网格缩小 ⇒ 丢掉边缘数据点、也不再覆盖整个 DEM，与需求相悖。
    """
    x_min, y_min, x_max, y_max = bounds
    ring = [
        [x_min, y_min],
        [x_max, y_min],
        [x_max, y_max],
        [x_min, y_max],
        [x_min, y_min],
    ]
    payload = {
        "type": "FeatureCollection",
        "name": "mask",
        "features": [
            {
                "type": "Feature",
                # Id 必须为 0：详见函数 docstring（-i 时它是"多边形外部"的烧灼值）。
                "properties": {"Id": 0, "note": "flow field scene bounds (working CRS metres)"},
                "geometry": {"type": "Polygon", "coordinates": [ring]},
            }
        ],
    }
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False)


# ==================== 导出与调用生成器 ====================


def write_frame_texts(raw_dir: str, x: np.ndarray, y: np.ndarray, fields: dict[str, np.ndarray]) -> int:
    """导出 26 个 `N.txt`。

    格式（生成器的真实输入契约，与 `mergeOutput_data` 一致）：
        第 1 行 点数
        第 2 行 列名
        之后每行 `X Y H P U V`
    """
    os.makedirs(raw_dir, exist_ok=True)
    frames = fields["u"].shape[0]
    for fi in range(frames):
        lines = [str(x.size), " ".join(HEADINGS)]
        lines.extend(
            f"{x[i]:.4f} {y[i]:.4f} {fields['h'][fi, i]:.4f} {fields['p'][fi, i]:.4f} "
            f"{fields['u'][fi, i]:.4f} {fields['v'][fi, i]:.4f}"
            for i in range(x.size)
        )
        with open(os.path.join(raw_dir, f"{fi}.txt"), "w", encoding="utf-8") as fh:
            fh.write("\n".join(lines))
            fh.write("\n")
    return frames


def write_builder_meta(path: str, *, raw_dir: str, out_dir: str, resource_url: str,
                       mask_path: str, resolution: int, frames: int) -> None:
    payload = {
        "input_url": raw_dir.replace("\\", "/") + "/",
        "output_url": out_dir.replace("\\", "/") + "/",
        "resource_url": resource_url,
        "column_num": len(HEADINGS),
        "headings": HEADINGS,
        "coordinates": ["X", "Y"],
        "velocities": ["U", "V"],
        "dem": ["P", "H"],
        "outlier": -99999.0,
        "resolution": int(resolution),
        "vector_mask": mask_path.replace("\\", "/"),
        "source_space": str(WORKING_EPSG),
        "target_space": ["mapbox"],
        "files": [{"in": f"{i}.txt", "sn": str(i)} for i in range(frames)],
    }
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=4)


def run_builder(meta_path: str, log=print) -> None:
    command = [_builder_path(), meta_path]
    log(f"[flow-scene] 调用生成器：{os.path.basename(command[0])} {meta_path}")
    proc = subprocess.run(command, env=_builder_env(), capture_output=True, text=True, encoding="utf-8", errors="replace")
    for line in (proc.stdout or "").splitlines():
        log(f"[TextureBuilder] {line}")
    for line in (proc.stderr or "").splitlines():
        log(f"[TextureBuilder!] {line}")
    if proc.returncode != 0:
        raise RuntimeError(f"TextureBuilder 退出码 {proc.returncode}")


# ==================== 主流程 ====================


def build_scene(
    scene_id: str,
    *,
    tiff_key: str = "",
    water_qs: str,
    tidal_level: str,
    resolution: int = 1024,
    dataset_domain: str = DEFAULT_DOMAIN,
    set_name: str = DEFAULT_SET,
    temp: bool = False,
    force: bool = False,
    log=print,
) -> dict[str, Any]:
    """生成一个场景的全部纹理，返回场景清单。

    已经生成过（`scene.json` 存在且输入一致）时直接返回，不重复跑生成器 ——
    一个场景可能要跑好几分钟（104 次 Xpand），而且同一份 DEM 会被多个任务复用。
    要强制重跑（例如改了生成器、或想核对产物）传 `force=True`。
    """
    import shutil

    root = scene_dir(scene_id)
    raw_dir = os.path.join(root, "raw")
    out_dir = os.path.join(root, "renderResource")
    resource_url = f"flowField/{scene_id}/renderResource/"
    manifest_path = os.path.join(root, "scene.json")

    if os.path.exists(manifest_path) and not temp and not force:
        with open(manifest_path, "r", encoding="utf-8") as fh:
            cached = json.load(fh)
        if (
            cached.get("water_qs") == water_qs
            and cached.get("tidal_level") == tidal_level
            and int(cached.get("resolution") or 0) == int(resolution)
        ):
            log(f"[flow-scene] 场景 {scene_id} 已存在，复用")
            return cached

    started = time.time()
    log(f"[flow-scene] 场景 {scene_id}：取点云 {dataset_domain}/{water_qs}/{tidal_level}")
    point_ids, x, y = load_points(
        dataset_domain=dataset_domain,
        set_name=set_name,
        water_qs=water_qs,
        tidal_level=tidal_level,
    )
    log(f"[flow-scene] 点数 {x.size}，包围盒 x=[{x.min():.1f}, {x.max():.1f}] y=[{y.min():.1f}, {y.max():.1f}]")

    steps, fields = load_series(
        point_ids,
        dataset_domain=dataset_domain,
        set_name=set_name,
        water_qs=water_qs,
        tidal_level=tidal_level,
    )
    log(f"[flow-scene] 帧数 {steps.size}（time_step {steps.min()}..{steps.max()}）")

    x_min, x_max = float(x.min()), float(x.max())
    y_min, y_max = float(y.min()), float(y.max())
    nx, ny, dd = grid_shape(x_min, x_max, y_min, y_max, resolution)
    log(f"[flow-scene] 网格 {nx} x {ny}，格距 {dd:.2f} m")

    frames = write_frame_texts(raw_dir, x, y, fields)
    mask_geojson = os.path.join(root, "mask.geojson")
    write_mask_geojson(mask_geojson, (x_min, y_min, x_max, y_max))

    meta_path = os.path.join(root, "texture_builder.json")
    write_builder_meta(
        meta_path,
        raw_dir=raw_dir,
        out_dir=out_dir,
        resource_url=resource_url,
        mask_path=mask_geojson,
        resolution=resolution,
        frames=frames,
    )

    if os.path.isdir(out_dir):
        shutil.rmtree(out_dir)
    os.makedirs(out_dir, exist_ok=True)
    run_builder(meta_path, log=log)

    description_path = os.path.join(out_dir, "flow_field_description.json")
    with open(description_path, "r", encoding="utf-8") as fh:
        description = json.load(fh)

    # 交叉校验：生成器算出来的网格必须与我们的复刻一致，否则掩膜一定会错位。
    size = description.get("texture_size", {}).get("area_mask")
    if not size or int(size[0]) != nx or int(size[1]) != ny:
        raise AssertionError(
            f"网格不一致：生成器给出 {size}，本模块算出 {(nx, ny)}。"
            "掩膜会与速度纹理错位，先对齐 grid_shape() 与 FlowField::buildTextures()。"
        )

    water = coverage_mask(x, y, x_min, y_min, nx, ny, dd)
    filled = fill_single_cell_holes(water)
    stats = write_mask_png(os.path.join(out_dir, "mask_0.png"), filled)
    log(
        f"[flow-scene] 掩膜重写完成：水域格 {stats['water_cells']} / {stats['total_cells']}"
        f"（{stats['water_ratio'] * 100:.2f}%），补空洞 {int(filled.sum() - water.sum())} 格"
    )

    # 覆盖核对：数据范围 vs DEM 范围。数据没盖住 DEM 时必须能一眼看见。
    coverage = describe_coverage(x_min, x_max, y_min, y_max, tiff_key)
    if coverage:
        log(
            f"[flow-scene] 覆盖：数据 / DEM 的包围盒重叠 {coverage['overlap_ratio'] * 100:.1f}%"
        )

    # 场景锚点与它覆盖的 DEM。scene_id 就是由 anchor + 水情哈希出来的，
    # 所以这几个字段既能自证 id 的来历，也让前端能回答"这个场景属于哪片 DEM"。
    anchor = scene_anchor(tiff_key)
    dem_bounds = dem_bounds_lonlat(tiff_key)
    tiff_keys = tiff_keys_for_bounds(dem_bounds) if dem_bounds else ([tiff_key] if tiff_key else [])
    if tiff_key and tiff_key not in tiff_keys:
        tiff_keys.append(tiff_key)
    # 自证：传进来的 id 必须与锚点算出来的一致，否则产物落在一个谁也解析不到的目录里。
    expected_id = _hash_scene_id(anchor, water_qs, tidal_level, resolution)
    if scene_id != expected_id:
        raise AssertionError(
            f"场景 id 与锚点不匹配：给定 {scene_id}，按 anchor={anchor} 算出 {expected_id}。"
            "调用方应该用 scene_id_for() 取 id，不要在外部手拼。"
        )

    manifest = {
        "scene_id": scene_id,
        "anchor": anchor,
        "dataset_domain": dataset_domain,
        "set_name": set_name,
        "water_qs": water_qs,
        "tidal_level": tidal_level,
        "tiff_key": tiff_key,
        "tiff_keys": tiff_keys,
        "dem_bounds": list(dem_bounds) if dem_bounds else None,
        "resolution": resolution,
        "frames": frames,
        "time_steps": [int(v) for v in steps],
        "point_count": int(x.size),
        "working_epsg": WORKING_EPSG,
        "grid": {"nx": nx, "ny": ny, "cell_size_m": round(dd, 3)},
        "bounds_working": {"x_min": x_min, "y_min": y_min, "x_max": x_max, "y_max": y_max},
        "mask": stats,
        "extent": description.get("extent"),
        "flow_boundary": description.get("flow_boundary"),
        "texture_size": description.get("texture_size"),
        "coverage": coverage,
        "temp": temp,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "elapsed_seconds": round(time.time() - started, 1),
    }
    with open(manifest_path, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, ensure_ascii=False, indent=2)
    log(f"[flow-scene] 场景 {scene_id} 完成，用时 {manifest['elapsed_seconds']} s")
    return manifest


def describe_coverage(
    x_min: float, x_max: float, y_min: float, y_max: float, tiff_key: str
) -> dict[str, Any] | None:
    """把数据包围盒与 DEM 包围盒都换算到经纬度后比对，给出重叠比例。"""
    dem = dem_bounds_lonlat(tiff_key)
    if not dem:
        return None
    try:
        from pyproj import Transformer

        transformer = Transformer.from_crs(WORKING_EPSG, 4326, always_xy=True)
        # 四角都转一次取外包：等角投影下边界是弯的，只转对角会漏。
        lons, lats = [], []
        for px in (x_min, x_max):
            for py in (y_min, y_max):
                lon, lat = transformer.transform(px, py)
                lons.append(lon)
                lats.append(lat)
        data_box = (min(lons), min(lats), max(lons), max(lats))
    except Exception:
        return None
    d_w, d_s, d_e, d_n = dem
    i_w, i_s = max(data_box[0], d_w), max(data_box[1], d_s)
    i_e, i_n = min(data_box[2], d_e), min(data_box[3], d_n)
    if i_e <= i_w or i_n <= i_s:
        return {
            "dem_bounds": list(dem),
            "data_bounds": list(data_box),
            "overlap_ratio": 0.0,
            "note": "数据与 DEM 完全不相交",
        }
    overlap = (i_e - i_w) * (i_n - i_s)
    dem_area = (d_e - d_w) * (d_n - d_s)
    return {
        "dem_bounds": [round(v, 6) for v in dem],
        "data_bounds": [round(v, 6) for v in data_box],
        "overlap_ratio": round(overlap / dem_area, 6) if dem_area > 0 else 0.0,
    }


def list_scenes() -> list[dict[str, Any]]:
    """列出已经生成好的场景（读各自的 scene.json）。供接口层使用。"""
    if not os.path.isdir(SCENE_ROOT):
        return []
    scenes = []
    for name in sorted(os.listdir(SCENE_ROOT)):
        manifest_path = os.path.join(SCENE_ROOT, name, "scene.json")
        if not os.path.isfile(manifest_path):
            continue
        try:
            with open(manifest_path, "r", encoding="utf-8") as fh:
                manifest = json.load(fh)
        except Exception:
            continue
        if manifest.get("temp"):
            continue
        # 没有 anchor 的产物是"按 tiff_key 字符串哈希"那一版规则留下的，已经解析不到，
        # 也就没有消费方；不列出来，免得前端下拉里出现一个点了必然 404 的场景。
        if not manifest.get("anchor"):
            continue
        scenes.append(manifest)
    return scenes


def resolve_scene(
    tiff_key: str,
    water_qs: str,
    tidal_level: str,
    resolution: int = 1024,
) -> dict[str, Any] | None:
    """查一个已生成的场景；没有就返回 `None`。

    **刻意不生成**：这条路径会被 HTTP 请求调用，而生成一次要 60 s 以上、产物 190 MB，
    绝不能挂在请求里同步做。要生成走 `build_scene()`（或离线批量脚本）。
    """
    scene_id = scene_id_for(tiff_key, water_qs, tidal_level, resolution)
    manifest_path = os.path.join(scene_dir(scene_id), "scene.json")
    if not os.path.isfile(manifest_path):
        return None
    try:
        with open(manifest_path, "r", encoding="utf-8") as fh:
            manifest = json.load(fh)
    except Exception:
        return None
    if manifest.get("temp"):
        return None
    return manifest


def scenes_for_dem(tiff_key: str) -> list[dict[str, Any]]:
    """这片 DEM 下所有已生成的场景（可能有多档流量/潮位）。

    匹配用的是**范围锚点**，所以同范围的另一个 DEM 文件也能命中 —— 这正是"流场属于任务所属 DEM"
    这条语义的落点。
    """
    if not tiff_key:
        return []
    anchor = scene_anchor(tiff_key)
    return [m for m in list_scenes() if m.get("anchor") == anchor]


# ==================== CLI ====================

if __name__ == "__main__":
    # 与 hydrodynamic_resource.py 一样的传参风格：位置参数，便于被 subprocess.Popen 直接拉起。
    # 第一个参数传 "-" 表示"按 DEM 范围自己算 id"，避免外部去复刻哈希规则。
    _scene_id = sys.argv[1]
    _tiff_key = sys.argv[2]
    _water_qs = sys.argv[3]
    _tidal_level = sys.argv[4]
    _resolution = int(sys.argv[5]) if len(sys.argv) > 5 else 1024
    if _scene_id in ("-", "auto"):
        _scene_id = scene_id_for(_tiff_key, _water_qs, _tidal_level, _resolution)
        print(f"[flow-scene] 自动场景 id：{_scene_id}", flush=True)
    build_scene(
        _scene_id,
        tiff_key=_tiff_key,
        water_qs=_water_qs,
        tidal_level=_tidal_level,
        resolution=_resolution,
    )
