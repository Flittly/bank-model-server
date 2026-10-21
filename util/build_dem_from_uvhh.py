#!/usr/bin/env python3
"""从「1998 大洪水」水动力 txt 的地形列构建栅格 DEM（GeoTIFF）。

背景
----
`#资料/.../相关资料/水文条件数据/1998大洪水-{2018,2023}地形/<子目录>/*.txt` 是
水动力模型的**非结构网格节点结果**，列序（GBK，见 `0格式说明.txt`）：

    坐标X  坐标Y  地形  流速U  流速V  水位  水深（85基面）

每个文件 = 一个时间帧，26 个文件 = 26 帧，网格相同、地形时不变（各帧地形中位一致）。
坐标参考为 EPSG:2437（北京54 / 3 度带 CM 120E，FE 500000）。

要点
----
1. `地形 == 水位 - 水深`（误差 ≤ 0.01 m，三位独立四舍五入所致），所以地形列本身
   没有额外信息；本脚本仍直接取第 3 列。
2. **干节点不可用**：`水深 <= 0.10` 的节点（约占 14%）其“地形”实为 `水位 - 0.10`
   的数值填充，与真实 DEM 相比系统性偏高约 1.5 m。故只取 `水深 > --dry-threshold`
   的湿节点作为**插值源**；湿区口径用 **26 帧的并集**（= 最大淹没范围），而不是
   单帧，因为网格节点几乎恒定湿润，但并集才是这个模型真正算过的域。
3. 插值方法：Delaunay 线性三角网（`gdal.Grid algorithm=linear`），在节点处精确。
4. ⚠️ `gdal.Grid` 的 linear 会把**凸包之外**的区域一并外推填满（实测 nodata 占比 0%）。
   而凸包本身也不可用：这批节点是一条 367 km 的蜿蜒河带，其凸包面积高达
   36,405 km²（接近整个外包矩形），会把大片陆地包进来。
   因此本脚本用**节点自身的 Delaunay 三角网覆盖域**作掩膜：
   保留**最长边 ≤ --max-edge** 的三角形，其余（跨河、跨凸包凹口的大三角形）丢弃。
   理由与实测：
   - 这批节点是按“横断面/网链”排布的曲线网格：沿链间距中位 ~40 m，
     跨链间距中位 ~315 m、P90 ~660 m；正常网格单元的最长边也就几百米。
   - 最长边阈值 1000 m 时保留 95.4% 的三角形，覆盖 2,666 km²；关键是**掩膜内
     任意一格到最近真实节点的距离最大只有 592 m（P95 = 324 m）**，即掩膜紧贴
     观测，不存在远场外推。
   - 阈值再小（如 500~700 m）会把孤立横断面切掉，河带上出现成片虚线状空洞。
5. 掩膜外写 nodata；另外再做一次合理值域夹紧（防凸包边角的外推值漏进来）。
6. 输出 EPSG:3857、Float32、nodata = -9999，与 `resource/tiff` 下既有 DEM 保持一致。
   分辨率默认 30 m：源网格沿链间距约 40 m、跨链约 315 m，10 m 只是假精度。


用法
----
    # 2018 地形
    uv run python util/build_dem_from_uvhh.py --year 2018 \
        --out resource/tiff/Yangtze/2018/standard/1998-2018/2018-Yangtze-uvhH.tif

    # 2023 地形，网格对齐到 2018 的产物（两个 DEM 必须逐格同网格）
    uv run python util/build_dem_from_uvhh.py --year 2023 \
        --out resource/tiff/Yangtze/2023/standard/1998-2023/2023-Yangtze-uvhH.tif \
        --align-to resource/tiff/Yangtze/2018/standard/1998-2018/2018-Yangtze-uvhH.tif

这两个 DEM 的用途是「岸床演变 / 冲淤」：`modelResource/riverbedEvolution/regionFlush.py`
里的 `global_flush_func(bench, ref)` 会把两者相减，而该函数是**按行列号直接对读**的
（不做重采样），所以两套 DEM 必须分辨率一致、原点对齐——`--align-to` 就是干这个的。
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sys
import tempfile
import time
from pathlib import Path

import numpy as np
from osgeo import gdal, ogr, osr

gdal.UseExceptions()

# ---------------------------------------------------------------- 源数据定位

SOURCE_ROOT = Path(
    "E:/Graduate/graduate-1/workspace/#资料/yangtze-bank-warning-system/"
    "相关资料/水文条件数据"
)

# 年份 -> (目录名, 子目录名)；子目录名来自数据方，2023 那套还带 0格式说明.txt
YEAR_LAYOUT = {
    "2018": ("1998大洪水-2018地形", "XYZUVhH"),
    "2023": ("1998大洪水-2023地形", "UVZUVhH"),
}

# 列序（0-based）：坐标X 坐标Y 地形 流速U 流速V 水位 水深
COL = {"x": 0, "y": 1, "z": 2, "u": 3, "v": 4, "wl": 5, "depth": 6}

SRC_EPSG = 2437
DST_EPSG = 3857
NODATA = -9999.0

# 数据帧文件名形如 "210-1998-2018-xg-uvhH.txt"；2023 目录里还有 "0格式说明.txt" 需排除
FRAME_RE = re.compile(r"^\d+-\d{4}-\d{4}-")


def frame_files(year: str) -> list[Path]:
    """返回该年份的 26 个时间帧文件（按段号排序，只取第 0 帧即地形帧）。"""
    if year not in YEAR_LAYOUT:
        raise SystemExit(f"未知年份 {year}，可选：{sorted(YEAR_LAYOUT)}")
    top, sub = YEAR_LAYOUT[year]
    d = SOURCE_ROOT / top / sub
    if not d.is_dir():
        raise SystemExit(f"源目录不存在：{d}")
    files = sorted(
        (p for p in d.glob("*.txt") if FRAME_RE.match(p.name)),
        key=lambda p: int(p.name.split("-", 1)[0]),
    )
    if not files:
        raise SystemExit(f"源目录没有数据文件：{d}")
    return files


def load_terrain(year: str, dry_threshold: float) -> tuple[np.ndarray, dict]:
    """读取全部时间帧，返回湿区并集节点的 (x, y, z) 与统计信息。

    - 高程取第 0 帧（地形不随时间变化，首末帧中位一致）。
    - 湿区取 26 帧的**并集**（最大淹没范围）；这既是插值的可信域，
      也是掩膜的取点口径。
    """
    files = frame_files(year)
    frame = files[0]
    t0 = time.time()
    arr = np.loadtxt(frame, skiprows=1, encoding="gbk")
    x, y, z = arr[:, COL["x"]], arr[:, COL["y"]], arr[:, COL["z"]]
    depth, wl = arr[:, COL["depth"]], arr[:, COL["wl"]]

    wet0 = depth > dry_threshold
    wet_union = wet0.copy()
    per_frame_wet = [int(wet0.sum())]
    for p in files[1:]:
        d = np.loadtxt(p, skiprows=1, encoding="gbk")[:, COL["depth"]]
        w = d > dry_threshold
        per_frame_wet.append(int(w.sum()))
        wet_union |= w

    n_all, n_wet = len(arr), int(wet_union.sum())

    # 自检：地形 == 水位 - 水深（±0.01 为两位小数独立四舍五入的容差）
    dev = np.abs(z - (wl - depth))
    identity_rate = float((dev <= 0.0101).mean())

    # 自检：地形在 26 帧之间应恒定（抽首尾两帧比对中位）
    last = np.loadtxt(files[-1], skiprows=1, encoding="gbk")
    med_first = float(np.median(z))
    med_last = float(np.median(last[:, COL["z"]]))

    info = {
        "frame": frame.name,
        "frames_total": len(files),
        "nodes_total": n_all,
        "nodes_wet_frame0": int(wet0.sum()),
        "nodes_wet_union": n_wet,
        "per_frame_wet_min": min(per_frame_wet),
        "per_frame_wet_max": max(per_frame_wet),
        "wet_ratio": n_wet / n_all,
        "terrain_min": float(z[wet_union].min()),
        "terrain_max": float(z[wet_union].max()),
        "terrain_median": float(np.median(z[wet_union])),
        "identity_rate": identity_rate,
        "terrain_median_first_frame": med_first,
        "terrain_median_last_frame": med_last,
        "read_seconds": time.time() - t0,
    }
    print(
        "[读源] {}：{} 帧，取第 0 帧 {} 的高程；节点 {} → 湿区并集 {}（{:.1f}%）；"
        "地形范围 [{:.2f}, {:.2f}] 中位 {:.2f}".format(
            year, info["frames_total"], frame.name, n_all, n_wet,
            100 * info["wet_ratio"], info["terrain_min"], info["terrain_max"],
            info["terrain_median"],
        )
    )
    print(
        "[自检] 地形==水位-水深 符合率 {:.2f}%；首帧/末帧地形中位 {:.2f} / {:.2f}；"
        "单帧湿节点 {:,}~{:,}".format(
            100 * identity_rate, med_first, med_last,
            info["per_frame_wet_min"], info["per_frame_wet_max"],
        )
    )

    return np.column_stack([x[wet_union], y[wet_union], z[wet_union]]), info


# ---------------------------------------------------------------- 坐标转换


def to_3857(pts: np.ndarray) -> np.ndarray:
    """把 (x, y, z) 的 x/y 从 EPSG:2437 转到 EPSG:3857，z 原样保留。"""
    src = osr.SpatialReference()
    src.ImportFromEPSG(SRC_EPSG)
    src.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    dst = osr.SpatialReference()
    dst.ImportFromEPSG(DST_EPSG)
    dst.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    ct = osr.CoordinateTransformation(src, dst)

    t0 = time.time()
    out = np.empty_like(pts)
    for i in range(len(pts)):
        X, Y, _ = ct.TransformPoint(float(pts[i, 0]), float(pts[i, 1]))
        out[i, 0] = X
        out[i, 1] = Y
        out[i, 2] = pts[i, 2]
    print(f"[投影] {len(pts)} 点 2437 → 3857，用时 {time.time() - t0:.1f}s")
    return out


# ---------------------------------------------------------------- 网格定义


def resolve_grid(
    pts3857: np.ndarray, res: float, align_to: Path | None
) -> tuple[float, float, float, float, int, int, osr.SpatialReference, dict]:
    """确定栅格边界与尺寸。

    返回 (x0, y_bottom, x1, y_top, width, height, srs, geom_info)。
    对齐模式下把格点吸附到参考栅格的同一套格网上，保证两个 DEM 逐格一一对应。
    """
    xmin, ymin = float(pts3857[:, 0].min()), float(pts3857[:, 1].min())
    xmax, ymax = float(pts3857[:, 0].max()), float(pts3857[:, 1].max())

    srs = osr.SpatialReference()
    srs.ImportFromEPSG(DST_EPSG)

    if align_to is not None:
        ref = gdal.Open(str(align_to))
        if ref is None:
            raise SystemExit(f"无法打开对齐参考栅格：{align_to}")
        gt = ref.GetGeoTransform()
        if abs(gt[1] - res) > 1e-6:
            raise SystemExit(
                f"参考栅格分辨率 {gt[1]} 与本脚本 --resolution {res} 不一致，无法对齐"
            )
        ref_srs = osr.SpatialReference()
        ref_srs.ImportFromWkt(ref.GetProjection())
        if ref_srs.GetAuthorityCode(None) != srs.GetAuthorityCode(None):
            raise SystemExit(
                "参考栅格坐标系 {} 与 EPSG:3857 不一致".format(
                    ref_srs.GetAuthorityCode(None)
                )
            )
        anchor_x, anchor_y = gt[0], gt[3]  # 参考栅格的左上角即格网锚点
    else:
        anchor_x = 0.0
        anchor_y = np.ceil(ymax / res) * res

    x0 = anchor_x + np.floor((xmin - anchor_x) / res) * res
    x1 = anchor_x + np.ceil((xmax - anchor_x) / res) * res
    y_top = anchor_y - np.floor((anchor_y - ymax) / res) * res
    y_bottom = anchor_y - np.ceil((anchor_y - ymin) / res) * res

    width = int(round((x1 - x0) / res))
    height = int(round((y_top - y_bottom) / res))

    geom = {
        "res": res,
        "anchor": [float(anchor_x), float(anchor_y)],
        "x0": float(x0),
        "x1": float(x1),
        "y_bottom": float(y_bottom),
        "y_top": float(y_top),
        "width": width,
        "height": height,
        "aligned_to": str(align_to) if align_to else None,
    }
    print(
        "[网格] res={:.0f} m x0={:.1f} y0={:.1f}{}  ->  {} x {} = {:.1f}M 格".format(
            res, x0, y_bottom,
            "（已吸附参考栅格）" if align_to else "",
            width, height, width * height / 1e6,
        )
    )
    return x0, y_bottom, x1, y_top, width, height, srs, geom


# ---------------------------------------------------------------- 域掩膜


def delaunay_domain_mask(
    pts3857: np.ndarray, x0: float, y_top: float, res: float, width: int, height: int,
    max_edge: float, max_dist: float, sieve_cells: int,
) -> tuple[np.ndarray, dict]:
    """用节点自身的 Delaunay 三角网覆盖域作掩膜。

    为什么不用缓冲膨胀：这批节点是按“横断面/网链”排布的曲线网格，沿链间距中位
    ~40 m，而跨链间距中位 ~315 m、P90 ~660 m、P99 ~1,340 m。任何“膨胀 k 格”的
    半径要么跨不过跨链空隙（留下条纹空洞），要么一跨就外溢到河道之外（实测
    dilate8+erode7 的掩膜达 1,789 km²，且全是岸上陆地）。

    为什么不用凸包：这是一条 367 km 的蜿蜒河带，凸包面积 36,405 km²，接近整个
    外包矩形，会把大片陆地包进来。

    正确做法：三角网是节点之间的自然插值单元，**保留“正常大小”的三角形**即可——
    正常网格单元的最长边只有几百米；跨河、跨凸包凹口的大三角形会被剔除。

    参数
    ----
    max_edge  : 三角形最长边上限（米）。1000 m 时保留 95.4% 的三角形、覆盖
                2,666 km²，且**掩膜内任意一格到最近真实节点的距离最大仅 592 m
                （P95 = 324 m）**，即掩膜紧贴观测，没有远场外推。
                调小（如 500~700）会切掉孤立的横断面，河带上出现成片虚线空洞。
    max_dist  : 到最近节点的距离上限（米），0 表示关闭。作为保险丝，
                把被 max_edge 放进来的个别远格剔掉。
    sieve_cells: 小于该格数的连通小块/小洞交给 GDAL SieveFilter 归并。
    """
    t0 = time.time()
    mp = ogr.Geometry(ogr.wkbMultiPoint)
    for i in range(len(pts3857)):
        g = ogr.Geometry(ogr.wkbPoint)
        g.AddPoint_2D(float(pts3857[i, 0]), float(pts3857[i, 1]))
        mp.AddGeometry(g)
    tri = mp.DelaunayTriangulation()
    n_sub = tri.GetGeometryCount()
    print(f"[掩膜] Delaunay 完成：{n_sub:,} 个子几何（{time.time() - t0:.1f}s）")

    xs = np.empty((n_sub, 3), dtype=np.float64)
    ys = np.empty((n_sub, 3), dtype=np.float64)
    longest = np.empty(n_sub, dtype=np.float64)
    kept = 0
    for i in range(n_sub):
        g = tri.GetGeometryRef(i)
        if g.GetGeometryType() != ogr.wkbPolygon:
            continue
        ring = g.GetGeometryRef(0)
        if ring.GetPointCount() < 4:
            continue
        x = (ring.GetX(0), ring.GetX(1), ring.GetX(2))
        y = (ring.GetY(0), ring.GetY(1), ring.GetY(2))
        e = max(
            ((x[a] - x[b]) ** 2 + (y[a] - y[b]) ** 2) ** 0.5
            for a, b in ((0, 1), (1, 2), (2, 0))
        )
        xs[kept] = x
        ys[kept] = y
        longest[kept] = e
        kept += 1
    xs, ys, longest = xs[:kept], ys[:kept], longest[:kept]
    sel = longest <= max_edge

    mask = _rasterize_triangles(xs[sel], ys[sel], x0, y_top, res, width, height)
    tri_area_km2 = float(_triangle_area(xs[sel], ys[sel]).sum() / 1e6)

    dist_max = dist_p95 = 0.0
    if max_dist > 0:
        dist = distance_to_nodes(pts3857, x0, y_top, res, width, height)
        cut = int((mask & ~(dist <= max_dist)).sum())
        mask = mask & (dist <= max_dist)
        if mask.any():
            d = dist[mask]
            dist_max, dist_p95 = float(d.max()), float(np.percentile(d, 95))
        print("[掩膜] 到最近节点距离 > {:.0f} m 被剔掉 {:,} 格".format(max_dist, cut))

    removed = 0
    if sieve_cells > 0:
        before = int(mask.sum())
        mask, _ = _sieve(mask, sieve_cells)
        removed = int(mask.sum()) - before

    stats = {
        "triangles_total": int(kept),
        "triangles_kept": int(sel.sum()),
        "triangles_kept_ratio": float(sel.mean()),
        "max_edge_m": float(max_edge),
        "hull_area_km2": float(_triangle_area(xs, ys).sum() / 1e6),
        "domain_area_km2": tri_area_km2,
        "raster_cells": int(mask.sum()),
        "raster_area_km2": float(mask.sum() * res * res / 1e6),
        "max_dist_to_node_m": dist_max,
        "p95_dist_to_node_m": dist_p95,
        "sieve_net_cells": removed,
    }
    print(
        "[掩膜] 最长边 <= {:.0f} m 保留三角形 {:,}/{:,}（{:.1f}%）；三角网面积 {:.1f} km²；"
        "栅格化后域内 {:,} 格 = {:.1f} km²（占 bbox {:.2f}%）".format(
            max_edge, int(sel.sum()), kept, 100 * sel.mean(), tri_area_km2,
            int(mask.sum()), stats["raster_area_km2"], 100 * mask.mean(),
        )
    )
    if max_dist > 0:
        print(
            "[掩膜] 域内格到最近节点: P95 {:.0f} m  最大 {:.0f} m".format(
                dist_p95, dist_max
            )
        )
    if sieve_cells > 0:
        print("[掩膜] Sieve 归并净变化 {:+d} 格".format(removed))
    return mask, stats


def _triangle_area(xs: np.ndarray, ys: np.ndarray) -> np.ndarray:
    return np.abs(
        0.5 * ((xs[:, 1] - xs[:, 0]) * (ys[:, 2] - ys[:, 0])
               - (xs[:, 2] - xs[:, 0]) * (ys[:, 1] - ys[:, 0]))
    )


def _rasterize_triangles(
    xs: np.ndarray, ys: np.ndarray, x0: float, y_top: float, res: float,
    width: int, height: int,
) -> np.ndarray:
    """把三角形联合体栅格化成布尔掩膜（调用方自备目标格网的左上角与分辨率）。"""
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(DST_EPSG)
    ds = gdal.GetDriverByName("MEM").Create("", width, height, 1, gdal.GDT_Byte)
    ds.SetGeoTransform((x0, res, 0.0, y_top, 0.0, -res))
    ds.SetProjection(srs.ExportToWkt())
    band = ds.GetRasterBand(1)
    band.Fill(0)

    vds = ogr.GetDriverByName("MEM").CreateDataSource("tri")
    lyr = vds.CreateLayer("tri", srs=srs, geom_type=ogr.wkbPolygon)
    defn = lyr.GetLayerDefn()
    lyr.StartTransaction()
    for i in range(len(xs)):
        ring = ogr.Geometry(ogr.wkbLinearRing)
        for k in range(3):
            ring.AddPoint_2D(float(xs[i, k]), float(ys[i, k]))
        ring.AddPoint_2D(float(xs[i, 0]), float(ys[i, 0]))
        poly = ogr.Geometry(ogr.wkbPolygon)
        poly.AddGeometry(ring)
        f = ogr.Feature(defn)
        f.SetGeometry(poly)
        lyr.CreateFeature(f)
        f = None
    lyr.CommitTransaction()
    gdal.RasterizeLayer(ds, [1], lyr, burn_values=[1])
    out = band.ReadAsArray().astype(bool)
    ds = None
    vds = None
    return out


def distance_to_nodes(
    pts3857: np.ndarray, x0: float, y_top: float, res: float, width: int, height: int,
) -> np.ndarray:
    """每个栅格到最近节点的距离（米）。用 GDAL 的 proximity 算子，避免自己循环。"""
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(DST_EPSG)
    gt = (x0, res, 0.0, y_top, 0.0, -res)
    drv = gdal.GetDriverByName("MEM")
    src = drv.Create("", width, height, 1, gdal.GDT_Byte)
    src.SetGeoTransform(gt)
    src.SetProjection(srs.ExportToWkt())
    b = src.GetRasterBand(1)
    b.Fill(0)
    col = np.floor((pts3857[:, 0] - x0) / res).astype(np.int64)
    row = np.floor((y_top - pts3857[:, 1]) / res).astype(np.int64)
    ok = (col >= 0) & (col < width) & (row >= 0) & (row < height)
    arr = np.zeros((height, width), dtype=np.uint8)
    arr[row[ok], col[ok]] = 1
    b.WriteArray(arr)

    dst = drv.Create("", width, height, 1, gdal.GDT_Float32)
    dst.SetGeoTransform(gt)
    dst.SetProjection(srs.ExportToWkt())
    out_b = dst.GetRasterBand(1)
    # 注意签名是 ComputeProximity(srcBand, proximityBand, options)，没有 mask 波段位
    gdal.ComputeProximity(b, out_b, ["VALUES=1", "DISTUNITS=GEO"])
    out = out_b.ReadAsArray()
    src = None
    dst = None
    return out


def _sieve(mask: np.ndarray, min_cells: int) -> tuple[np.ndarray, int]:
    """用 GDAL SieveFilter 归并小于 min_cells 的连通区块（造临时栅格处理后读回）。

    SieveFilter 把小块并进最大邻块：值为 1 的小岛被抹掉、值为 0 的小洞被填上，
    两者都是我们要的。
    """
    tmpdir = Path(tempfile.mkdtemp(prefix="dem_mask_"))
    tmp = tmpdir / "mask.tif"
    h, w = mask.shape
    ds = gdal.GetDriverByName("GTiff").Create(str(tmp), w, h, 1, gdal.GDT_Byte)
    ds.SetGeoTransform((0.0, 1.0, 0.0, float(h), 0.0, -1.0))
    band = ds.GetRasterBand(1)
    band.WriteArray(mask.astype(np.uint8))
    gdal.SieveFilter(band, None, band, min_cells, 8)
    out = band.ReadAsArray().astype(bool)
    ds = None
    shutil.rmtree(tmpdir, ignore_errors=True)
    return out, 0


# ---------------------------------------------------------------- 插值


def write_points_geojson(pts3857: np.ndarray, path: Path, srs: osr.SpatialReference) -> None:
    drv = ogr.GetDriverByName("GeoJSON")
    if path.exists():
        drv.DeleteDataSource(str(path))
    ds = drv.CreateDataSource(str(path))
    lyr = ds.CreateLayer("pts", srs=srs, geom_type=ogr.wkbPoint)
    lyr.CreateField(ogr.FieldDefn("z", ogr.OFTReal))
    defn = lyr.GetLayerDefn()
    lyr.StartTransaction()
    for k in range(len(pts3857)):
        f = ogr.Feature(defn)
        g = ogr.Geometry(ogr.wkbPoint)
        g.AddPoint_2D(float(pts3857[k, 0]), float(pts3857[k, 1]))
        f.SetGeometry(g)
        f.SetField("z", float(pts3857[k, 2]))
        lyr.CreateFeature(f)
        f = None
    lyr.CommitTransaction()
    ds = None


def build(out: Path, year: str, res: float, max_edge: float, max_dist: float,
          dry_threshold: float, align_to: Path | None, workdir: Path,
          sieve_cells: int, force: bool = False) -> dict:
    t_start = time.time()
    out.parent.mkdir(parents=True, exist_ok=True)
    workdir.mkdir(parents=True, exist_ok=True)

    pts2437, info = load_terrain(year, dry_threshold)
    pts = to_3857(pts2437)
    x0, y_bottom, x1, y_top, width, height, srs, geom = resolve_grid(pts, res, align_to)

    gj = workdir / f"domain_points_{year}.geojson"
    if force and gj.exists():
        gj.unlink()
    if not gj.exists():
        t0 = time.time()
        write_points_geojson(pts, gj, srs)
        print(f"[点集] GeoJSON {gj.name} {gj.stat().st_size / 1e6:.1f} MB（{time.time() - t0:.1f}s）")
    else:
        print(f"[点集] 复用 {gj.name}")

    mask, mask_stats = delaunay_domain_mask(
        pts, x0, y_top, res, width, height, max_edge, max_dist, sieve_cells
    )

    t0 = time.time()
    tmp = workdir / f"grid_{year}_{int(res)}m_raw.tif"
    opts = gdal.GridOptions(
        format="GTiff",
        outputType=gdal.GDT_Float32,
        width=width,
        height=height,
        outputBounds=(x0, y_bottom, x1, y_top),
        outputSRS=srs,
        algorithm="linear:nodata={:.0f}".format(NODATA),
        layers=["pts"],
        zfield="z",
    )
    if tmp.exists():
        tmp.unlink()
    ds = gdal.Grid(str(tmp), str(gj), options=opts)
    ds = None
    print(f"[插值] Delaunay 线性三角网完成，用时 {time.time() - t0:.1f}s")

    ds = gdal.Open(str(tmp))
    band = ds.GetRasterBand(1)
    grid = band.ReadAsArray()
    gt = ds.GetGeoTransform()
    proj = ds.GetProjection()
    ds = None

    # 域外 → nodata；再做一次合理性夹紧，防止凸包外推的边角值漏进掩膜
    valid = mask & (grid >= info["terrain_min"] - 1.0) & (grid <= info["terrain_max"] + 1.0)
    clipped = int((mask & ~valid).sum())
    grid = np.where(valid, grid, np.float32(NODATA))
    if clipped:
        print(f"[掩膜] 域内因超出节点值域被剔掉的格：{clipped:,}")

    if out.exists():
        out.unlink()
    drv = gdal.GetDriverByName("GTiff")
    ds = drv.Create(
        str(out), width, height, 1, gdal.GDT_Float32,
        options=["TILED=YES", "COMPRESS=DEFLATE", "PREDICTOR=3", "BIGTIFF=IF_SAFER"],
    )
    ds.SetGeoTransform(gt)
    ds.SetProjection(proj)
    b = ds.GetRasterBand(1)
    b.SetNoDataValue(NODATA)
    b.WriteArray(grid.astype(np.float32))
    b.FlushCache()
    b.ComputeStatistics(False)  # 生成 .aux.xml 统计
    ds = None

    ok = grid != NODATA
    stats = {
        "year": year,
        "output": str(out),
        "size_mb": out.stat().st_size / 1e6,
        "width": width,
        "height": height,
        "resolution": res,
        "nodata_ratio": float(1 - ok.mean()),
        "value_min": float(grid[ok].min()),
        "value_max": float(grid[ok].max()),
        "value_median": float(np.median(grid[ok])),
        "domain_area_km2": float(ok.sum() * res * res / 1e6),
        "clipped_cells": clipped,
        "mask": mask_stats,
        "source": info,
        "grid": geom,
        "geotransform": list(gt),
        "elapsed_seconds": time.time() - t_start,
    }
    print(
        "[输出] {}  {:.1f} MB  nodata {:.1f}%  有效 {:.1f} km²  值域 [{:.2f}, {:.2f}] 中位 {:.2f}".format(
            out, stats["size_mb"], 100 * stats["nodata_ratio"], stats["domain_area_km2"],
            stats["value_min"], stats["value_max"], stats["value_median"],
        )
    )
    meta = out.with_suffix(".meta.json")
    meta.write_text(json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[输出] 元数据 {meta}")
    print(f"[完成] 总用时 {stats['elapsed_seconds']:.1f}s")
    return stats


def main() -> None:
    ap = argparse.ArgumentParser(description="从 1998 大洪水 txt 的地形列构建栅格 DEM")
    ap.add_argument("--year", required=True, choices=sorted(YEAR_LAYOUT))
    ap.add_argument("--out", required=True, type=Path, help="输出 GeoTIFF 路径")
    ap.add_argument("--resolution", type=float, default=30.0, help="栅格分辨率（米，默认 30）")
    ap.add_argument("--max-edge", type=float, default=1000.0,
                    help="域掩膜保留的三角形最长边上限（米，默认 1000；"
                         "调小会切掉孤立横断面、留虚线空洞）")
    ap.add_argument("--max-dist", type=float, default=500.0,
                    help="掩膜格到最近节点的距离上限（米，默认 500；0 表示关闭）")
    ap.add_argument("--dry-threshold", type=float, default=0.1001,
                    help="水深大于该值才算湿节点（默认 0.1001）")
    ap.add_argument("--sieve", type=int, default=100,
                    help="归并小于该格数的连通小块/小洞（默认 100，0 表示不处理）")
    ap.add_argument("--align-to", type=Path, default=None, help="对齐到参考栅格的网格")
    ap.add_argument("--workdir", type=Path, default=None, help="中间产物目录")
    ap.add_argument("--force", action="store_true", help="忽略缓存的 GeoJSON，重新生成")
    args = ap.parse_args()

    workdir = args.workdir or args.out.parent / ".build"
    build(args.out, args.year, args.resolution, args.max_edge, args.max_dist,
          args.dry_threshold, args.align_to, workdir, args.sieve, args.force)


if __name__ == "__main__":
    sys.exit(main())
