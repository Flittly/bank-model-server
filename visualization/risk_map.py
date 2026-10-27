"""
岸段风险分布图生成器
在地图上按断面风险等级沿程插值显示岸段风险分布
支持遥感影像底图、比例尺、指北针

图层自下而上（zorder）：
    -10  遥感影像底图（basemap.py，可选）
      1  经纬网
      2  岸段风险色带（由断面风险等级沿程插值着色；断面本身不画）

⚠ DEM 灰度底图（原 zorder=0）自 2026-10-21 起**缺省不画**，见
  :data:`RENDER_DEM_BASEMAP`；保留相关函数与 ``dem_path`` 参数只为兼容载荷。

断面数据仍参与：岸段色带的插值 stop、稀疏区段判定、取景范围与统计口径
都来自断面——只是不再把断面线/端点画到图上。
"""
import os
import json
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.collections import LineCollection
from matplotlib.patches import Rectangle
import numpy as np
from typing import List, Optional

from . import basemap as basemap_module
from . import gradient
from .config import (
    RISK_COLORS, CHART_STYLE, RISK_LEGEND,
    risk_color, is_rendered_level,
    ensure_output_dir, save_result
)

#: 岸段色带线宽 / 稀疏区段线宽
BANK_LINEWIDTH = 6.0
BANK_GAP_LINEWIDTH = 4.0

#: 岸段无实测控制区段（内插/外推）的虚线样式
GAP_LINESTYLE = 'dashed'

# ---------------------------------------------------------------------------
# DEM 底图的栅格尺寸与统计口径
#
# 两道硬约束，缺一不可（2026-10-21 修 ArrayMemoryError）：
#   ① 输出像素总数上限。旧实现只给 Warp 一个固定 xRes/yRes=0.0002°（≈19 m），
#      它约束的是「分辨率」而不是「总量」——对 30 m 源 DEM 而言这个值比源还细，
#      于是 6.2°×1.5° 的范围被上采样成 31175×7589 = 2.37 亿像素，
#      float32 单份就 946 MB，直接吃掉进程内存。
#   ② 分位数只能在抽样后的有效值上算。旧实现在整个 2.37 亿元素数组上调
#      np.nanpercentile，numpy 内部还要再复制一份 1D 数组，内存翻倍后必然失败。
#
# 底图只是 alpha=0.6 的灰度衬底：24.6" 画布 @150 dpi ≈ 3700 px，栅格再细也看不出来，
# 所以 400 万像素（≈ 4000×1000）已远超肉眼可辨。
# ---------------------------------------------------------------------------

#: Warp 输出像素总数上限（float32 下约 16 MB）
MAX_DEM_PIXELS = 4_000_000
#: 精细范围下仍保留的目标分辨率（度）。约 20 m，与历史输出的观感一致。
DEM_TARGET_RES_DEG = 0.0002
#: 计算 vmin/vmax 时参与统计的抽样点上限（与全量相比差异远小于 1%）
DEM_PERCENTILE_SAMPLES = 200_000

#: 是否把 DEM 灰度高程衬底画到图上。
#:
#: 2026-10-21 需求：**图上不再显示 DEM**（「可以不显示 DEM，其他的不变」）。
#: 置 False 后连 ``load_dem_basemap`` 都不会被调用 —— 不再读 GDAL、不再 Warp、
#: 不再吃那份内存，但仍保留 ``dem_path`` 参数与相关函数：Java 侧载荷里的
#: ``dem_path`` 不动（免得改协议），将来要恢复只改这一个开关。
#: 其余图层（遥感影像底图 / 岸段色带 / 经纬网 / 比例尺 / 指北针 / 图例）一律不变。
RENDER_DEM_BASEMAP = False


def _fit_raster_size(lng_span: float, lat_span: float,
                     max_pixels: int = MAX_DEM_PIXELS) -> tuple:
    """按「目标分辨率 + 总像素上限」算出 Warp 输出尺寸。

    先按 :data:`DEM_TARGET_RES_DEG` 取尺寸（保持小范围时的历史清晰度不变），
    超出 :data:`max_pixels` 再整体等比缩放 —— 长宽同时缩小，地理宽高比不变。

    返回 ``(width, height)``；范围非法（非正、方向反了）时返回 ``(0, 0)``，
    调用方应放弃尺寸约束，交给 GDAL 按源分辨率自行决定。
    """
    lng_span, lat_span = float(lng_span), float(lat_span)
    if not (lng_span > 0 and lat_span > 0) or max_pixels <= 0:
        return 0, 0
    width = max(1, int(np.ceil(lng_span / DEM_TARGET_RES_DEG)))
    height = max(1, int(np.ceil(lat_span / DEM_TARGET_RES_DEG)))
    if width * height > max_pixels:
        scale = (max_pixels / float(width * height)) ** 0.5
        width = max(1, int(width * scale))
        height = max(1, int(height * scale))
    return width, height


def _robust_percentile(data, q: float, max_samples: int = DEM_PERCENTILE_SAMPLES) -> float:
    """在「抽样后的有效值」上求分位数，内存占用与数组大小无关。

    nodata 已在载入时转成 NaN，因此先按步长抽样再剔除 NaN 即可。
    抽样是规则的网格步进（不是随机采样），对高程这种空间连续场，
    其分位数与全量结果的差异远小于灰度拉伸的可见阈值。

    无任何有效值时返回 ``nan``，由调用方决定跳过绘制。
    """
    arr = np.asarray(data)
    if arr.size == 0:
        return float('nan')
    if arr.ndim > 1:
        ratio = arr.size / float(max(1, max_samples))
        step = max(1, int(np.ceil(ratio ** (1.0 / arr.ndim))))
        sample = arr[::step, ::step]
    else:
        step = max(1, int(np.ceil(arr.size / float(max(1, max_samples)))))
        sample = arr[::step]
    valid = np.asarray(sample)[np.isfinite(sample)]
    if valid.size == 0:
        return float('nan')
    return float(np.percentile(valid, q))


def _bank_coords(bank) -> List[tuple]:
    """从 bank dict 取 [[lng, lat], ...]；非法项跳过。"""
    if not isinstance(bank, dict):
        return []
    coords = bank.get('coordinates')
    if not coords:
        return []
    out = []
    for point in coords:
        try:
            out.append((float(point[0]), float(point[1])))
        except (TypeError, ValueError, IndexError):
            continue
    return out


def _safe_level(section):
    try:
        return int(section.get('risk_level'))
    except (TypeError, ValueError):
        return None


def _draw_bank_gradients(ax, banks, sections, gap_meters=None, report=None) -> int:
    """把岸段折线按断面风险等级着色，画在断面之下（zorder=2）。

    配色与可见性口径**与页面完全一致**（色值真源 `riskScoring.ts`，
    可见性口径 `sectionRenderVisibility.ts`）：

    * ``risk_level`` 1/2/3/4 四档**全部着色**（绿 / 蓝 / 橙 / 红）——
      低风险自 2026-10-26 起改为正常画绿色，与页面同一口径；
    * 只有缺失/未知（未计算、计算失败）**不画** —— 不是画成透明色，
      而是整段不生成 LineCollection，让底图自然透出。「没数」不是第 5 个等级；
    * 每条断面管辖「与前后断面中点之间」的那段岸线，区间内**纯色**
      （见 ``gradient.build_span_colors``）；因此换色是硬边，
      与页面逐像素同款。

    返回成功绘制的岸段数。缺数据时逐项打印原因，不静默跳过——「图上没有岸段」
    必须能在日志里看出是哪一环缺的（载荷没带 / 几何点太少 / 坐标非法 / 全是无风险值）。

    闭合环（``banks.bank_geometry`` 实测首末点重合）按**环**处理，不掰直：
    几何原样绘制、颜色按环分区、稀疏判定按环计算。理由见 gradient.is_closed_ring。

    :param gap_meters: 「内插/外推」区段的间距阈值（米）；
                       None 表示按该岸段断面的实际密度自适应（推荐，见 adaptive_gap_threshold）。
    :param report: 可选 dict；会写入 ``sparse_segments``（实画的虚线区段数），
                   仅用于日志核对（2026-10-21 起图例不再列「内插/外推区段」一项）。
                   另写入 ``hidden_sections``（因**无风险值**而未着色的断面数；
                   2026-10-26 起低风险不再计入这里）。
    """
    if not banks:
        print('[viz] 载荷未包含 banks（旧载荷或后端未取到岸段几何），跳过岸段色带', flush=True)
        return 0

    by_bank = {}
    for section in sections:
        by_bank.setdefault(section.get('bank_id'), []).append(section)

    drawn = 0
    sparse_total = 0
    hidden_total = 0
    for bank in banks:
        bank_id = bank.get('bank_id') if isinstance(bank, dict) else None
        bank_sections = by_bank.get(bank_id, [])
        # 环必须原样使用（含那条闭合边）。掰直会把这条边从折线上摘掉，
        # 落在其上的断面只能被拉到最近端点（实测偏差 476 m）。
        coords = _bank_coords(bank)
        if len(coords) < 2:
            print(f'[viz] 岸段 {bank_id} 坐标点不足（{len(coords)} 个 <2），跳过', flush=True)
            continue
        is_ring = gradient.is_closed_ring(coords)
        if is_ring:
            print(f'[viz] 岸段 {bank_id} 为闭合环（{len(coords)} 顶点），按环分区着色'
                  f'（含闭合边，接缝处按环取最近断面）', flush=True)

        _, items, total, xy = gradient.build_stops(
            bank_sections, coords, color_of=risk_color, cyclic=is_ring)
        if total <= 0:
            print(f'[viz] 岸段 {bank_id} 折线长度为 0，跳过', flush=True)
            continue
        if not items:
            # 未计算 / 无风险值：页面同样整条不着色，这里也不画
            print(f'[viz] 岸段 {bank_id} 名下无可定位断面（{len(bank_sections)} 条），'
                  f'整条不渲染（未计算段不着色）', flush=True)
            continue

        gradient.check_distance_consistency(items, total, bank_id)
        far = max(items, key=lambda it: it['offset'])
        if far['offset'] > gradient.DEFAULT_OFFSET_WARN_METERS:
            print(f'[viz] 岸段 {bank_id} 断面 {far["section_id"]} 中点距折线 '
                  f'{far["offset"]:.0f} m：该断面可能不在岸段线上（几何或断面数据可疑），'
                  f'位置按最近点处理', flush=True)

        hidden = sum(1 for item in items if not is_rendered_level(item.get('risk_level')))
        hidden_total += hidden

        # 自适应间距阈值只按**可着色**断面统计：无效/未计算的断面在图上没有颜色，
        # 不构成实测控制点，把它们算进去会压低中位间距→阈值变小→同岸段别处正常段
        # 被误判成「内插」画成虚线（与 gradient.build_span_colors 内的 measured 同口径）。
        threshold = gradient.adaptive_gap_threshold(
            [item['along'] for item in items
             if is_rendered_level(item.get('risk_level'))],
            override=gap_meters, cyclic=is_ring, total=total)
        _, arc, _ = gradient.cumulative_lengths(xy)
        colors, gaps = gradient.build_span_colors(
            xy, arc, total, items, threshold, cyclic=is_ring)

        # 几何原样使用：闭合环的末点就是首点，段数天然等于「环上的边数」，
        # 既不用补段也不用重新排序（掰直 + 补段那套已经废弃）。
        segments = [[coords[i], coords[i + 1]] for i in range(len(coords) - 1)]
        if len(segments) != len(colors):
            print(f'[viz] 岸段 {bank_id} 段数({len(segments)})与配色数({len(colors)})不一致，跳过',
                  flush=True)
            continue

        # color is None = 该段归「无风险值」断面管辖 ⇒ 不画（不着色）。
        # 2026-10-26 起低风险（risk_level 1）也会给出绿色，不再落进这个分支。
        visible = [(seg, color, gap) for seg, color, gap in zip(segments, colors, gaps)
                   if color is not None]
        if not visible:
            print(f'[viz] 岸段 {bank_id} 全部断面无风险值（{len(items)} 条），'
                  f'整条不渲染', flush=True)
            continue

        solid = [(seg, color) for seg, color, gap in visible if not gap]
        sparse = [(seg, color) for seg, color, gap in visible if gap]

        if solid:
            ax.add_collection(LineCollection(
                [seg for seg, _ in solid], colors=[c for _, c in solid],
                linewidths=BANK_LINEWIDTH, alpha=0.75, zorder=2, capstyle='round'))
        if sparse:
            ax.add_collection(LineCollection(
                [seg for seg, _ in sparse], colors=[c for _, c in sparse],
                linewidths=BANK_GAP_LINEWIDTH, alpha=0.4,
                linestyles=GAP_LINESTYLE, zorder=2, capstyle='round'))
            sparse_total += len(sparse)
        drawn += 1

    if hidden_total:
        print(f'[viz] {hidden_total} 条断面无风险值，按页面口径不着色', flush=True)
    if report is not None:
        report['sparse_segments'] = sparse_total
        report['hidden_sections'] = hidden_total
    return drawn


def load_dem_basemap(dem_path: str, bounds: tuple):
    """加载 DEM 底图（支持坐标系转换、裁剪、降采样）"""
    try:
        from osgeo import gdal, osr
        print(f"[viz] opening DEM with GDAL: {dem_path}", flush=True)
        ds = gdal.Open(dem_path)
        if ds is None:
            print("[viz] GDAL returned None, file might be corrupted", flush=True)
            return None, None
        
        # 获取原始坐标系
        src_srs = osr.SpatialReference()
        src_srs.ImportFromWkt(ds.GetProjection())
        
        # 目标坐标系：WGS84
        dst_srs = osr.SpatialReference()
        dst_srs.ImportFromEPSG(4326)
        
        # 检查是否需要转换
        needs_reproject = not src_srs.IsSame(dst_srs)
        print(f"[viz] DEM CRS: EPSG:{src_srs.GetAuthorityCode(None)}, needs_reproject: {needs_reproject}", flush=True)
        
        # 裁剪范围（加 20% 缓冲）
        min_lng, max_lng, min_lat, max_lat = bounds
        lng_pad = (max_lng - min_lng) * 0.2
        lat_pad = (max_lat - min_lat) * 0.2
        crop_min_lng = min_lng - lng_pad
        crop_max_lng = max_lng + lng_pad
        crop_min_lat = min_lat - lat_pad
        crop_max_lat = max_lat + lat_pad
        
        print(f"[viz] cropping to: lng=[{crop_min_lng:.4f}, {crop_max_lng:.4f}], lat=[{crop_min_lat:.4f}, {crop_max_lat:.4f}]", flush=True)
        
        # 使用 GDAL Warp 进行重投影+裁剪+降采样
        warp_options = {
            'dstSRS': 'EPSG:4326',
            'format': 'MEM',
            'outputBounds': [crop_min_lng, crop_min_lat, crop_max_lng, crop_max_lat],
            'targetAlignedPixels': True,
        }
        
        # 需要降采样（像素太多）
        ds_info = gdal.Info(ds, format='json')
        src_pixels = ds_info.get('size', [0, 0])
        if src_pixels[0] * src_pixels[1] > 10_000_000:  # 超过1000万像素
            # 关键：约束的是**输出像素总数**，不是固定分辨率。
            # 旧实现给 xRes/yRes=0.0002°（≈19 m），比 30 m 源还细 ⇒ 对 6.2°×1.5°
            # 的范围反而上采样到 2.37 亿像素（946 MB/份），随即 OOM。
            out_w, out_h = _fit_raster_size(crop_max_lng - crop_min_lng,
                                           crop_max_lat - crop_min_lat)
            if out_w > 0 and out_h > 0:
                warp_options['width'] = out_w
                warp_options['height'] = out_h
                # -tap（targetAlignedPixels）只能和 -tr（固定分辨率）一起用，
                # 与 width/height 同时给会被 GDAL 直接拒绝：
                # "-tap option cannot be used without using -tr"
                warp_options.pop('targetAlignedPixels', None)
                x_res = (crop_max_lng - crop_min_lng) / out_w
                y_res = (crop_max_lat - crop_min_lat) / out_h
                ground_m = x_res * 111320 * np.cos(np.radians((crop_min_lat + crop_max_lat) / 2))
                print(f"[viz] downsampling DEM (too large: {src_pixels[0]}x{src_pixels[1]}) "
                      f"→ {out_w}x{out_h}（{out_w * out_h / 1e6:.1f} M px，≈{ground_m:.0f} m/px）",
                      flush=True)
        
        if needs_reproject or True:  # 总是用 Warp 进行裁剪
            print("[viz] reprojecting and cropping DEM...", flush=True)
            ds = gdal.Warp('', ds, **warp_options)
            if ds is None:
                print("[viz] warp failed", flush=True)
                return None, None
            print("[viz] warp done", flush=True)
        
        # 获取地理变换参数
        gt = ds.GetGeoTransform()
        print(f"[viz] DEM geo_transform: {gt}", flush=True)
        
        # 读取高程数据
        band = ds.GetRasterBand(1)
        # 先转 float32：若源是整型，np.where(cond, np.nan, data) 会静默升成
        # float64（8 B/px，是 float32 的两倍），大栅格上同样是 OOM 来源。
        data = band.ReadAsArray().astype(np.float32, copy=False)
        nodata = band.GetNoDataValue()
        print(f"[viz] DEM shape: {data.shape}, nodata: {nodata}", flush=True)
        
        # 计算经纬度范围
        rows, cols = data.shape
        lngs = gt[0] + np.arange(cols) * gt[1]
        lats = gt[3] + np.arange(rows) * gt[5]
        print(f"[viz] DEM extent: lng=[{lngs[0]:.6f}, {lngs[-1]:.6f}], lat=[{lats[-1]:.6f}, {lats[0]:.6f}]", flush=True)
        
        # 处理 nodata：原地改写，不再复制整幅栅格
        if nodata is not None and not (isinstance(nodata, float) and np.isnan(nodata)):
            data[data == nodata] = np.nan

        # 检查有效数据比例
        valid_count = int(np.count_nonzero(~np.isnan(data)))
        total_count = data.size
        print(f"[viz] DEM valid pixels: {valid_count}/{total_count} "
              f"({valid_count/max(1, total_count)*100:.1f}%)", flush=True)
        
        ds = None
        return data, (lngs, lats)
    except ImportError:
        print("[viz] GDAL not available, skipping DEM basemap", flush=True)
        return None, None
    except Exception as e:
        print(f"[viz] failed to load DEM: {e}", flush=True)
        return None, None


# ---------------------------------------------------------------------------
# 地理等比例出图：动态画布尺寸
#
# 旧实现固定 figsize=(14, 10)，而长江岸段多是东西走向的窄长条——实测任务级
# 合并图的地理宽高比可达 9:1，被 1.4:1 的固定画布纵向拉伸 6 倍以上，河段
# 形状严重失真。
#
# 修正分两件事，缺一不可：
#   ① ax.set_aspect(纬度每度屏幕高 / 经度每度屏幕宽)：把「1°纬度 vs 1°经度」
#      的屏幕长度比锁定为两者真实公里数之比（经度按中心纬度做 cos 校正）。
#      此后无论画布多大，图上 1 km 横向 = 1 km 纵向，地理形状永不失真。
#   ② 画布尺寸跟随范围：短边固定，长边按地理宽高比放大，让地图尽量占满
#      画布而不是被 aspect 压成中间一条带、两侧大片留白。
# ---------------------------------------------------------------------------

#: 所有图的「窄方向」边长（英寸）。地图主体 + 标题/图例的最小可读尺寸。
BASE_SHORT_EDGE = 7.0
#: 画布任一边上限（英寸）。150 dpi 下 36" = 5400 px，再大文件难以嵌入文档。
MAX_EDGE = 36.0
#: 画布任一边下限（英寸）。再小放不下标题与图例。
MIN_EDGE = 6.0
#: 超过该宽高比后，等比例单图已经是不可读的「细窄条」→ 提示分段出图。
MAX_SANE_RATIO = MAX_EDGE / BASE_SHORT_EDGE      # ≈ 5.1
#: 字号从该边长开始放大（150 dpi 下 9pt≈19px，在 3000px+ 宽的图里显得过小）。
FONT_SCALE_FROM_EDGE = 20.0
#: 字号放大上限（避免图例比地图还抢眼）。
FONT_SCALE_MAX = 1.5

#: 图例底板不透明度 = 0.8（80%）。与 heatmap.py / section_chart.py 的标注框 alpha 一致。
#: 别再往下调：低于 ~0.75 时底图纹理开始透上来干扰文字。
LEGEND_FRAME_ALPHA = 0.8

#: 每纬度度的真实公里数（子午线平均长度）。
KM_PER_LAT_DEG = 110.574
#: 每经度度在赤道的公里数。
KM_PER_LNG_DEG_EQUATOR = 111.32


def compute_map_layout(lng_span: float, lat_span: float, lat_center: float,
                       short_edge: float = BASE_SHORT_EDGE,
                       max_edge: float = MAX_EDGE):
    """按地理真实比例计算画布尺寸与坐标轴 aspect。

    :param lng_span: 经度跨距（度）
    :param lat_span: 纬度跨距（度）
    :param lat_center: 范围中心纬度（用于经度的 cos 长度校正）
    :return: (figsize, aspect, geo_ratio, clamped)
        figsize   —— (宽, 高) 英寸，两边均已 clamp 到 [MIN_EDGE, max_edge]
        aspect    —— 传给 ax.set_aspect 的值（1°纬度屏幕高 / 1°经度屏幕宽）
        geo_ratio —— 地理宽高比（东西 km / 南北 km）
        clamped   —— 长边是否被上限截断（True = 等比例保持，但图已偏「窄条」）
    """
    import math

    lng_km_per_deg = KM_PER_LNG_DEG_EQUATOR * math.cos(math.radians(lat_center))
    width_km = abs(lng_span) * lng_km_per_deg
    height_km = abs(lat_span) * KM_PER_LAT_DEG
    geo_ratio = width_km / height_km if height_km > 0 else float('inf')

    # set_aspect 语义：y 每数据单位 / x 每数据单位 的屏幕尺寸比。
    # 等比例 ⇔ (1°lat 屏幕高)/(1°lng 屏幕宽) = 110.574 / (111.32·cosφ)
    aspect = KM_PER_LAT_DEG / lng_km_per_deg if lng_km_per_deg > 0 else 1.0

    # 画布：短边固定，长边按地理比例放大
    if geo_ratio >= 1:
        width, height = short_edge * geo_ratio, short_edge
        clamped = width > max_edge
    else:
        width, height = short_edge, short_edge / geo_ratio
        clamped = height > max_edge
    width = min(max(width, MIN_EDGE), max_edge)
    height = min(max(height, MIN_EDGE), max_edge)
    return (width, height), aspect, geo_ratio, clamped


def map_font_scale(figsize: tuple) -> float:
    """画布变大时适度放大字号：长边每超过 20" 起步 ×1，线性放大，封顶 1.5×。"""
    long_edge = max(figsize)
    if long_edge <= FONT_SCALE_FROM_EDGE:
        return 1.0
    factor = long_edge / FONT_SCALE_FROM_EDGE
    return min(factor, FONT_SCALE_MAX)


def data_extent_with_padding(sections: List[dict], banks: Optional[List[dict]],
                             pad: float = 0.15) -> tuple:
    """按「断面端点 + 岸段几何」算数据包络，再四边扩边。

    岸段几何必须纳入：否则岸段环会比断面包络大，被裁到画布之外。
    """
    all_lngs, all_lats = [], []
    for s in sections:
        all_lngs.extend([s['start_lng'], s.get('end_lng', s['start_lng'])])
        all_lats.extend([s['start_lat'], s.get('end_lat', s['start_lat'])])
    for bank in (banks or []):
        for lng, lat in _bank_coords(bank):
            all_lngs.append(lng)
            all_lats.append(lat)
    if not all_lngs:
        return (0.0, 0.0, 1.0, 1.0)
    lng_min, lng_max = min(all_lngs), max(all_lngs)
    lat_min, lat_max = min(all_lats), max(all_lats)
    lng_pad = (lng_max - lng_min) * pad
    lat_pad = (lat_max - lat_min) * pad
    return (lng_min - lng_pad, lat_min - lat_pad,
            lng_max + lng_pad, lat_max + lat_pad)


def compute_tile_grid(bbox: tuple, rows: int, cols: int) -> List[dict]:
    """把 bbox 按 行×列 均分为 rows*cols 个子图范围。

    行序自北向南（第 0 行在最上/最北），与读图习惯一致：第 (0,0) 幅是左上角，
    第 (rows-1, cols-1) 幅是右下角。序号按行优先展开。

    任一方向为 1 即该方向不分块：例如 (rows=1, cols=3) = 只按经度方向切成 3 幅；
    (rows=2, cols=1) = 只按纬度方向切成 2 幅。
    """
    west, south, east, north = (float(v) for v in bbox)
    if not (east > west and north > south):
        raise ValueError(f'bbox 非法: {list(bbox)}')
    if rows < 1 or cols < 1:
        raise ValueError(f'分块数必须 >=1: rows={rows}, cols={cols}')
    tiles = []
    for r in range(rows):
        for c in range(cols):
            w = west + (east - west) * c / cols
            e = west + (east - west) * (c + 1) / cols
            # 第 0 行取最北的一段
            n = north - (north - south) * r / rows
            s = north - (north - south) * (r + 1) / rows
            tiles.append({'row': r, 'col': c, 'bbox': (w, s, e, n)})
    return tiles


def resolve_split_tiles(bbox: tuple, rows: int, cols: int,
                        custom: Optional[List[dict]] = None) -> List[dict]:
    """生成最终子图列表：均分网格 + 逐幅自定义边界覆盖。

    :param custom: 自定义项列表，每项可含 row/col（0 起，缺省按行优先序号对齐网格）
                   与 west/south/east/north（缺省的边沿用该网格格的对应边界）。
                   例：只改第 2 幅的西边界 → [{"row":0,"col":1,"west":119.30}]
    """
    grid = compute_tile_grid(bbox, rows, cols)
    if not custom:
        return grid

    by_index = {(t['row'], t['col']): t for t in grid}
    for i, item in enumerate(custom):
        if not isinstance(item, dict):
            raise ValueError(f'split.tiles[{i}] 必须是对象，得到 {type(item).__name__}')
        key = (int(item.get('row', i // cols)), int(item.get('col', i % cols)))
        base = by_index.get(key)
        if base is None:
            raise ValueError(f'split.tiles[{i}] 的 row/col={key} 超出 {rows}×{cols} 网格')
        w, s, e, n = base['bbox']
        edges = {
            'west': float(item['west']) if item.get('west') is not None else w,
            'south': float(item['south']) if item.get('south') is not None else s,
            'east': float(item['east']) if item.get('east') is not None else e,
            'north': float(item['north']) if item.get('north') is not None else n,
        }
        base['bbox'] = (edges['west'], edges['south'], edges['east'], edges['north'])
    for tile in grid:
        w, s, e, n = tile['bbox']
        if not (e > w and n > s):
            raise ValueError(f"子图 ({tile['row']},{tile['col']}) 范围非法: {list(tile['bbox'])}")
    return grid


def _tile_output_name(stem: str, row: int, col: int, multi: bool) -> str:
    """单幅（未分块）时保持原名，多幅时加 _r{r}c{c} 后缀。"""
    if not multi:
        return stem
    dot = stem.rfind('.')
    if dot > 0:
        return f'{stem[:dot]}_r{row}c{col}{stem[dot:]}'
    return f'{stem}_r{row}c{col}'


def generate_risk_map_tiles(risk_data: List[dict],
                            title: str = "岸段风险分布图",
                            output_dir: Optional[str] = None,
                            output_name: Optional[str] = None,
                            rows: int = 1,
                            cols: int = 1,
                            tiles: Optional[List[dict]] = None,
                            dem_path: Optional[str] = None,
                            banks: Optional[List[dict]] = None,
                            gap_meters: Optional[float] = None,
                            basemap: Optional[dict] = None,
                            render_dem: bool = RENDER_DEM_BASEMAP) -> str:
    """分幅出图：把一幅完整地图按经纬度切成 rows×cols 幅子图。

    每幅子图都是**完整的地图**——各自的等比例画布、标题、图例、比例尺、指北针、
    经纬网、底图与岸段插值色带一样不缺；只有显示范围不同（子图窗口），
    越界内容由 matplotlib 自动裁剪，岸段颜色仍按整条几何插值计算（不受分幅影响）。
    DEM 底图与单图同口径：缺省不画（见 :data:`RENDER_DEM_BASEMAP`）。

    :param rows: 纬度方向分块数（1 = 不分）
    :param cols: 经度方向分块数（1 = 不分）
    :param tiles: 逐幅自定义边界（见 resolve_split_tiles），缺省 = 均分
    :return: JSON。未分块时与 generate_risk_distribution_map 的返回完全一致；
             分块时含 file_paths / tiles 明细。
    """
    # 未分块：走原单图路径，返回结构保持向后兼容（Java 侧零感知）
    if rows == 1 and cols == 1 and not tiles:
        return generate_risk_distribution_map(
            risk_data, title, output_dir, output_name,
            dem_path=dem_path, banks=banks, gap_meters=gap_meters,
            basemap=basemap, render_dem=render_dem)

    sections = [item for item in risk_data
                if item.get('start_lng') and item.get('start_lat')]
    if not sections:
        return json.dumps({'success': False, 'error': '没有有效的坐标数据'}, ensure_ascii=False)

    base_bbox = data_extent_with_padding(sections, banks)
    try:
        grid = resolve_split_tiles(base_bbox, rows, cols, tiles)
    except ValueError as exc:
        return json.dumps({'success': False, 'error': f'分幅参数错误: {exc}'},
                          ensure_ascii=False)

    stem = output_name or 'risk_distribution_map.png'
    results = []
    for tile in grid:
        r, c = tile['row'], tile['col']
        w, s, e, n = tile['bbox']
        # 只数**可着色**断面：人工标为无效的断面在 Java 侧已被 risk_level 置空，
        # 报告正文/分区表（Step 1 文本、RiskSegmentSummaryService）也早已排除它们。
        # 若这里仍按几何全量计数，报告分区表会出现一个比正文更大的数字 ——
        # 「同一份成果两套断面数」是本项目反复踩过的坑，两个口径必须同源。
        inside = sum(1 for sec in sections
                     if w <= sec['start_lng'] <= e and s <= sec['start_lat'] <= n
                     and is_rendered_level(sec.get('risk_level')))
        print(f"[viz] 分幅 ({r},{c}) lng=[{w:.4f},{e:.4f}] lat=[{s:.4f},{n:.4f}]"
              f" 范围内断面 {inside} 条", flush=True)
        single = json.loads(generate_risk_distribution_map(
            risk_data, title,
            output_dir, _tile_output_name(stem, r, c, multi=True),
            dem_path=dem_path, banks=banks, gap_meters=gap_meters,
            basemap=basemap, bbox=tile['bbox'], render_dem=render_dem))
        single['row'] = r
        single['col'] = c
        single['bbox'] = [w, s, e, n]
        single['sections_in_tile'] = inside
        results.append(single)

    ok = [item for item in results if item.get('success')]
    if not ok:
        return json.dumps({'success': False, 'error': '所有子图均生成失败',
                           'tiles': results}, ensure_ascii=False)
    return json.dumps({
        'success': True,
        'chart_type': 'risk_distribution',
        'title': title,
        'tile_count': len(results),
        'file_path': ok[0]['file_path'],
        'file_paths': [item['file_path'] for item in ok],
        'tiles': results,
        'description': f'共 {len(results)} 幅子图（{rows} 行 × {cols} 列），'
                       f'每幅含完整图面；范围内断面合计 '
                       f'{sum(item.get("sections_in_tile", 0) for item in results)} 条',
    }, ensure_ascii=False)


def generate_risk_distribution_map(
    risk_data: List[dict],
    title: str = "岸段风险分布图",
    output_dir: Optional[str] = None,
    output_name: Optional[str] = None,
    figsize: Optional[tuple] = None,
    dem_path: Optional[str] = None,
    banks: Optional[List[dict]] = None,
    gap_meters: Optional[float] = None,
    basemap: Optional[dict] = None,
    bbox: Optional[tuple] = None,
    render_dem: bool = RENDER_DEM_BASEMAP
) -> str:
    """
    生成岸段风险分布图（图上只画插值后的岸段色带，不画断面线/断面点）

    Args:
        risk_data: 风险数据列表（断面）。断面不直接绘制，但驱动岸段插值、
                   稀疏判定、取景范围与统计口径
        title: 图表标题
        output_dir: 输出目录
        output_name: 输出文件名
        figsize: 图表大小。**None（缺省）= 按河段地理范围动态计算并锁定等比例**
                 （推荐，见 compute_map_layout）；显式传入时尊重调用方，
                 但坐标轴仍会设置等比例 aspect——固定画布只会带来留白，不会变形。
        dem_path: DEM 文件路径。**当前缺省不画**（见 :data:`RENDER_DEM_BASEMAP`）：
                   保留该参数只为兼容 Java 侧载荷，传了也不会加载 DEM。
        render_dem: 是否把 DEM 灰度高程衬底画到图上。缺省 False = 不显示 DEM
                   （2026-10-21 需求）。传 True 恢复旧行为，其余图层完全不受影响。
        banks: 岸段列表 [{bank_id, bank_name, coordinates:[[lng,lat],...]}]，
               提供时把岸段按断面风险等级做沿程颜色插值后绘制（图面主体）
        gap_meters: 「内插/外推」区段的间距阈值（米）。缺省 None = 按该岸段断面的实际
                    密度自适应（推荐）；显式传值则覆盖自适应。相邻断面跨距超过阈值、
                    或落在断面范围之外的区段，会用虚线+低透明度弱化显示。
        basemap: 遥感影像底图配置（可选，见 visualization.basemap.resolve_config）。
                 在最底层（zorder=-10）垫一层影像，**不改变**岸段色带等上层任何
                 绘制参数。缺 key / 网络不通 / 未配置时自动跳过。
        bbox: 显示范围 (west, south, east, north)，分幅出图的子图窗口。
                None（缺省）= 按断面+岸段几何包络自动计算并扩边 15%；
                传入时坐标轴范围精确锁定为该 bbox，越界内容由 matplotlib 裁掉，
                但岸段插值仍按整条几何计算——颜色不受分幅影响。

    Returns:
        JSON 格式的生成结果
    """
    plt.rcParams.update(CHART_STYLE)
    
    # 准备数据
    sections = []
    for item in risk_data:
        if item.get('start_lng') and item.get('start_lat'):
            sections.append(item)
    
    if not sections:
        return json.dumps({'success': False, 'error': '没有有效的坐标数据'}, ensure_ascii=False)
    
    # ---------------------------------------------------------------
    # 显示范围：缺省 = 断面+岸段包络扩边 15%；
    # 分幅出图时由 bbox 指定子图窗口（见 generate_risk_map_tiles）。
    # ---------------------------------------------------------------
    if bbox is not None:
        lng_min, lat_min, lng_max, lat_max = (float(v) for v in bbox)
        if not (lng_max > lng_min and lat_max > lat_min):
            return json.dumps({'success': False,
                               'error': f'非法的显示范围 bbox={list(bbox)}'},
                              ensure_ascii=False)
        print(f"[viz] 使用指定显示范围 lng=[{lng_min:.6f}, {lng_max:.6f}], "
              f"lat=[{lat_min:.6f}, {lat_max:.6f}]", flush=True)
    else:
        lng_min, lat_min, lng_max, lat_max = data_extent_with_padding(sections, banks)
    
    # ---------------------------------------------------------------
    # 画布尺寸 + 地理等比例：按范围动态计算（旧实现固定 14×10，
    # 东西走向的长河段被纵向拉伸数倍，河段形状失真——本次修复的核心）
    # ---------------------------------------------------------------
    lat_center = (lat_min + lat_max) / 2
    auto_figsize, aspect, geo_ratio, edge_clamped = compute_map_layout(
        lng_max - lng_min, lat_max - lat_min, lat_center)
    if figsize is None:
        figsize = auto_figsize
    font_scale = map_font_scale(figsize)
    print(f"[viz] 地理范围宽高比 {geo_ratio:.2f}:1 → 画布 "
          f"{figsize[0]:.1f}\"×{figsize[1]:.1f}\"（等比例 aspect={aspect:.3f}，"
          f"字号 ×{font_scale:.2f}）", flush=True)
    if geo_ratio > MAX_SANE_RATIO:
        print(f"[viz] 提示：本图地理宽高比 {geo_ratio:.1f}:1 超过 {MAX_SANE_RATIO:.1f}:1，"
              f"等比例单图会是很窄的长条。建议改用按岸段分段出图"
              f"（Java 侧 generateRiskMapBatch 的 per_bank 模式），"
              f"每段单独成图可读性更好", flush=True)

    # 创建图表
    fig, ax = plt.subplots(figsize=figsize)
    # adjustable='box'：通过收缩坐标轴盒子（而非拉伸数据）满足比例——
    # 这是「宁可留白、绝不变形」的实现基础
    ax.set_aspect(aspect, adjustable='box')

    # ---------------------------------------------------------------
    # 最底层：遥感影像底图（zorder=-10）
    # 只做「加一层」：岸段色带等上层绘制参数一概不动。
    # ---------------------------------------------------------------
    basemap_report = basemap_module.draw_basemap(
        ax, basemap, (lng_min, lat_min, lng_max, lat_max),
        target_px_width=figsize[0] * fig.get_dpi() * 0.8)
    if basemap_report.get('drawn'):
        bm_extent = basemap_report['extent']
        print(f"[viz] 遥感影像底图已作为最底层绘制（zorder={basemap_module.BASEMAP_ZORDER}），"
              f"覆盖 lng=[{bm_extent[0]:.4f}, {bm_extent[1]:.4f}] "
              f"lat=[{bm_extent[2]:.4f}, {bm_extent[3]:.4f}]", flush=True)
    else:
        print("[viz] 本次未绘制遥感影像底图（未配置 / 缺 key / 取图失败），"
              "岸段色带等图层不受影响", flush=True)

    # 绘制 DEM 底图（缺省关闭：RENDER_DEM_BASEMAP=False ⇒ 图上不出现 DEM）
    if not render_dem:
        print("[viz] DEM 底图已关闭（RENDER_DEM_BASEMAP=False），本图不含 DEM 高程底图", flush=True)
    elif dem_path and os.path.exists(dem_path):
        print(f"[viz] loading DEM: {dem_path}", flush=True)
        dem_data, dem_coords = load_dem_basemap(dem_path, (lng_min, lng_max, lat_min, lat_max))
        if dem_data is not None:
            dem_lngs, dem_lats = dem_coords
            print(f"[viz] DEM loaded: shape={dem_data.shape}", flush=True)
            print(f"[viz] DEM extent: lng=[{dem_lngs[0]:.6f}, {dem_lngs[-1]:.6f}], lat=[{dem_lats[-1]:.6f}, {dem_lats[0]:.6f}]", flush=True)
            
            # 检查 DEM 是否覆盖风险数据范围
            dem_covers = (dem_lngs[0] <= lng_min <= dem_lngs[-1]) and (dem_lngs[0] <= lng_max <= dem_lngs[-1]) and \
                         (dem_lats[-1] <= lat_min <= dem_lats[0]) and (dem_lats[-1] <= lat_max <= dem_lats[0])
            print(f"[viz] DEM covers risk data: {dem_covers}", flush=True)
            
            # 绘制 DEM 底图（灰度，增强对比度）
            # 抽样统计：全量 nanpercentile 会在大栅格上再复制一份数组（2.37 亿 float32
            # 时直接 OOM），而灰度拉伸只需要 2% / 98% 两个分位，抽样 20 万点足够。
            vmin = _robust_percentile(dem_data, 2)
            vmax = _robust_percentile(dem_data, 98)
            if not np.isfinite(vmin) or not np.isfinite(vmax):
                print("[viz] DEM 无有效高程值，跳过 DEM 底图（不影响岸段/断面图层）", flush=True)
            else:
                if vmax <= vmin:      # 全平 DEM：给一个 1 m 的窗口，避免 imshow 归一化退化
                    vmin, vmax = vmin - 1.0, vmax + 1.0
                ax.imshow(
                    dem_data,
                    extent=[dem_lngs[0], dem_lngs[-1], dem_lats[-1], dem_lats[0]],
                    cmap='gray',
                    vmin=vmin, vmax=vmax,
                    alpha=0.6,
                    aspect='auto',
                    zorder=0
                )
                print(f"[viz] DEM basemap rendered (stretch {vmin:.2f}~{vmax:.2f} m)", flush=True)
        else:
            print("[viz] failed to load DEM data", flush=True)
    else:
        print(f"[viz] no DEM path or file not exists: {dem_path}", flush=True)
    
    # 绘制经纬网（简洁版）
    draw_graticule(ax, lng_min, lng_max, lat_min, lat_max, font_scale=font_scale)

    # 绘制岸段风险色带（在断面之下，zorder=2）
    bank_report = {}
    drawn_banks = _draw_bank_gradients(ax, banks, sections, gap_meters, report=bank_report)
    sparse_segments = bank_report.get('sparse_segments', 0)
    # 稀疏（内插/外推）区段仍按虚线绘制，只是不再进图例（2026-10-21 需求）；
    # 这里把条数打进日志，便于排查「图上虚线怎么没了/怎么多了」。
    print(f"[viz] bank gradients drawn: {drawn_banks}, "
          f"sparse segments: {sparse_segments}", flush=True)

    # 断面不再单独绘制（需求：图上只显示插值后的岸段色带）。
    # 断面数据仍驱动岸段插值、稀疏判定与取景范围——只是不画断面线和端点。

    # 设置坐标轴（不显示默认标签，用经纬网的标签）
    ax.set_xlim(lng_min, lng_max)
    ax.set_ylim(lat_min, lat_max)
    ax.set_xlabel('')
    ax.set_ylabel('')
    ax.set_xticks([])
    ax.set_yticks([])
    
    # 标题
    ax.set_title(title, fontsize=16 * font_scale, fontweight='bold', pad=15)
    
    # 添加图例（左上角）
    # 图例条目与文案由 config.RISK_LEGEND 唯一决定（见该常量注释：最高档不进图例、
    # 「内插/外推区段」这一项已按需求移除）。低风险自 2026-10-26 起在图上着绿色，
    # 图例必须列出来 —— 图上出现图例里没有的颜色等于没法查证。
    # 「无风险值」仍按页面口径整段不画，所以不进图例：列进去等于承诺一种图上不存在的颜色。
    #
    # ⚠ 稀疏区段的虚线样式仍在图上绘制（_draw_bank_gradients），只是不再进图例；
    #   不要因为这里没列就把 GAP_LINESTYLE 一起删掉。
    legend_patches = [
        mpatches.Patch(color=RISK_COLORS[level], label=label)
        for level, label in RISK_LEGEND
    ]
    # 图例底板透明度 80%（alpha=0.8，保留 20% 透出底图），与 heatmap.py / section_chart.py
    # 的标注框（facecolor='white', alpha=0.8, edgecolor='gray'）保持同一套视觉约定；
    # 白底 80% 不透明足以压住底图纹理，保证图例文字（默认黑色）清晰可读。
    legend = ax.legend(
        handles=legend_patches,
        loc='upper left',
        fontsize=9 * font_scale,
        frameon=True,
        facecolor='white',
        framealpha=LEGEND_FRAME_ALPHA,
        edgecolor='gray',
        bbox_to_anchor=(0.01, 0.99)
    )
    # 显式锁定文字颜色，避免任何主题/rcParams 下被改成浅色而降低读性
    for text in legend.get_texts():
        text.set_color('#1f2937')
    
    # 统计各等级数量（用于返回描述）
    risk_counts = {}
    for s in sections:
        level = _safe_level(s)
        risk_counts[level] = risk_counts.get(level, 0) + 1
    high_risk_count = sum(count for level, count in risk_counts.items()
                          if level is not None and level >= 2)

    # 描述里的断面数按**可着色**统计：人工标为无效的断面（risk_level 被 Java 侧置空）
    # 在图上不着色、在报告正文里也已排除（RiskSegmentSummaryService 同口径），
    # 这里若照旧写 len(sections) 会与正文数字对不上，看起来像漏算。
    rendered_count = sum(1 for s in sections if is_rendered_level(_safe_level(s)))
    hidden_count = len(sections) - rendered_count
    invalid_count = sum(1 for s in sections if s.get('manual_invalid'))
    hidden_note = ''
    if hidden_count:
        hidden_note = f"，另有 {hidden_count} 个未着色"
        if invalid_count:
            hidden_note += f"（其中人工无效 {invalid_count} 个）"
    
    # 添加比例尺（左下角）
    add_scale_bar(ax, lng_min, lng_max, lat_min, lat_max, font_scale=font_scale)

    # 添加指北针（右上角）
    add_north_arrow(ax, x=0.95, y=0.78, font_scale=font_scale)
    
    # 添加边框
    for spine in ax.spines.values():
        spine.set_linewidth(1.5)
        spine.set_color('black')
    
    plt.tight_layout()

    # 保存图片
    output_dir = ensure_output_dir(output_dir)
    if output_name is None:
        output_name = 'risk_distribution_map.png'
    file_path = os.path.join(output_dir, output_name)
    plt.savefig(file_path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()

    bank_note = f"，岸段 {drawn_banks} 条" if drawn_banks else ""
    basemap_note = "，含遥感影像底图" if basemap_report.get('drawn') else ""
    return save_result(
        output_dir, 'risk_distribution', file_path,
        title, f"共 {rendered_count} 个断面{bank_note}，高风险(>=2) {high_risk_count} 个"
               f"{hidden_note}{basemap_note}"
    )


def draw_graticule(ax, lng_min, lng_max, lat_min, lat_max, font_scale: float = 1.0):
    """绘制简洁的经纬网（只在边框显示刻度）"""
    lng_range = lng_max - lng_min
    lat_range = lat_max - lat_min
    
    # 选择合适的间隔
    if lng_range < 0.1:
        lng_step = 0.01
    elif lng_range < 0.5:
        lng_step = 0.05
    elif lng_range < 2:
        lng_step = 0.1
    else:
        lng_step = 0.5
    
    if lat_range < 0.1:
        lat_step = 0.01
    elif lat_range < 0.5:
        lat_step = 0.05
    elif lat_range < 2:
        lat_step = 0.1
    else:
        lat_step = 0.5
    
    # 绘制经度刻度（底部）
    lng = np.floor(lng_min / lng_step) * lng_step
    while lng <= lng_max:
        if lng_min <= lng <= lng_max:
            ax.plot([lng, lng], [lat_min, lat_min + lat_range * 0.02],
                   color='black', linewidth=1)
            ax.text(lng, lat_min - lat_range * 0.01, f'{lng:.2f}°',
                   ha='center', va='top', fontsize=8 * font_scale, color='black')
        lng += lng_step

    # 绘制纬度刻度（左侧）
    lat = np.floor(lat_min / lat_step) * lat_step
    while lat <= lat_max:
        if lat_min <= lat <= lat_max:
            ax.plot([lng_min, lng_min + lng_range * 0.02], [lat, lat],
                   color='black', linewidth=1)
            ax.text(lng_min - lng_range * 0.01, lat, f'{lat:.2f}°',
                   ha='right', va='center', fontsize=8 * font_scale, color='black')
        lat += lat_step


def add_scale_bar(ax, lng_min, lng_max, lat_min, lat_max, font_scale: float = 1.0):
    """添加比例尺（左下角）。

    等比例出图后的关键约束：
      * 横向长度**必须**保持数据坐标——比例尺的度量意义就在于此
        （scale_km 换算成经度跨度后，屏幕长度与真实公里数严格成比例）；
      * 纵向的位置/条高/文字改用画布比例（blended transform）——
        窄长河段图的纬度跨距很小，若条高仍按 lat_range 比例取，
        比例尺会缩成一条看不清的细线。
    """
    from matplotlib.transforms import blended_transform_factory

    center_lat = (lat_min + lat_max) / 2
    lng_range = lng_max - lng_min

    # 横向数据坐标、纵向画布比例：画条与文字只受纵向用后者
    trans = blended_transform_factory(ax.transData, ax.transAxes)

    # 1度经度在中心纬度的距离（km）
    lng_to_km = 111.32 * np.cos(np.radians(center_lat))

    # 计算合适的比例尺长度
    total_km = lng_range * lng_to_km
    if total_km < 1:
        scale_km = 0.1
    elif total_km < 5:
        scale_km = 0.5
    elif total_km < 20:
        scale_km = 2
    elif total_km < 50:
        scale_km = 5
    elif total_km < 100:
        scale_km = 10
    else:
        scale_km = 50

    # 比例尺位置（左下角，纵向按画布比例）
    scale_lng = lng_min + lng_range * 0.05
    scale_y = 0.07

    # 比例尺长度（经度单位）与条高（画布比例）
    scale_lng_len = scale_km / lng_to_km
    bar_height = 0.012
    segments = 3
    seg_len = scale_lng_len / segments

    for i in range(segments):
        color = 'black' if i % 2 == 0 else 'white'
        rect = Rectangle(
            (scale_lng + i * seg_len, scale_y),
            seg_len, bar_height,
            transform=trans,
            facecolor=color, edgecolor='black', linewidth=1
        )
        ax.add_patch(rect)

    # 刻度线
    for i in range(segments + 1):
        x = scale_lng + i * seg_len
        ax.plot([x, x], [scale_y, scale_y - 0.006],
                transform=trans, color='black', linewidth=1)

    # 文字标注
    ax.text(
        scale_lng + scale_lng_len / 2,
        scale_y - 0.018,
        f'{scale_km} km',
        transform=trans,
        ha='center', va='top', fontsize=9 * font_scale, fontweight='bold'
    )


def add_north_arrow(ax, x=0.95, y=0.78, length=0.10, font_scale: float = 1.0):
    """添加经典箭头式指北针（右上角）：竖直实心箭头指向正北，顶端标注 N。

    整体用 axes-fraction 坐标绘制——不受 set_aspect 等比例与画布宽高比影响，
    无论图多宽多扁，箭头始终是竖直的细杆加实心三角头，不会变形。
    """
    ax.annotate(
        '',
        xy=(x, y + length), xytext=(x, y),
        xycoords='axes fraction',
        arrowprops=dict(
            arrowstyle='-|>',              # 细杆 + 实心三角头（经典样式）
            color='black',
            linewidth=1.8 * font_scale,
            mutation_scale=22 * font_scale,
            shrinkA=0, shrinkB=0,
        ),
        annotation_clip=False,
        zorder=55,
    )
    ax.text(
        x, y + length + 0.012, 'N',
        transform=ax.transAxes,
        ha='center', va='bottom',
        fontsize=13 * font_scale, fontweight='bold', color='black',
        zorder=55,
    )
