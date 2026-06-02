"""
岸段风险分布图生成器
在地图上显示各断面的风险等级分布
支持 DEM 底图、比例尺、指北针
"""
import os
import json
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import Rectangle
import numpy as np
from typing import List, Optional

from .config import (
    RISK_COLORS, RISK_LABELS, CHART_STYLE,
    ensure_output_dir, save_result
)


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
        
        # 如果需要降采样（像素太多）
        ds_info = gdal.Info(ds, format='json')
        src_pixels = ds_info.get('size', [0, 0])
        if src_pixels[0] * src_pixels[1] > 10_000_000:  # 超过1000万像素
            warp_options['xRes'] = 0.0002  # 约20米分辨率
            warp_options['yRes'] = 0.0002
            print(f"[viz] downsampling DEM (too large: {src_pixels[0]}x{src_pixels[1]})", flush=True)
        
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
        data = band.ReadAsArray()
        nodata = band.GetNoDataValue()
        print(f"[viz] DEM shape: {data.shape}, nodata: {nodata}", flush=True)
        
        # 计算经纬度范围
        rows, cols = data.shape
        lngs = gt[0] + np.arange(cols) * gt[1]
        lats = gt[3] + np.arange(rows) * gt[5]
        print(f"[viz] DEM extent: lng=[{lngs[0]:.6f}, {lngs[-1]:.6f}], lat=[{lats[-1]:.6f}, {lats[0]:.6f}]", flush=True)
        
        # 处理 nodata
        if nodata is not None:
            data = np.where(data == nodata, np.nan, data)
        
        # 检查有效数据比例
        valid_count = np.count_nonzero(~np.isnan(data))
        total_count = data.size
        print(f"[viz] DEM valid pixels: {valid_count}/{total_count} ({valid_count/total_count*100:.1f}%)", flush=True)
        
        ds = None
        return data, (lngs, lats)
    except ImportError:
        print("[viz] GDAL not available, skipping DEM basemap", flush=True)
        return None, None
    except Exception as e:
        print(f"[viz] failed to load DEM: {e}", flush=True)
        return None, None


def generate_risk_distribution_map(
    risk_data: List[dict],
    title: str = "岸段风险分布图",
    output_dir: Optional[str] = None,
    output_name: Optional[str] = None,
    figsize: tuple = (14, 10),
    dem_path: Optional[str] = None
) -> str:
    """
    生成岸段风险分布图
    
    Args:
        risk_data: 风险数据列表
        title: 图表标题
        output_dir: 输出目录
        output_name: 输出文件名
        figsize: 图表大小
        dem_path: DEM 文件路径（可选，用于底图）
        
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
    
    # 计算坐标范围
    all_lngs = []
    all_lats = []
    for s in sections:
        all_lngs.extend([s['start_lng'], s.get('end_lng', s['start_lng'])])
        all_lats.extend([s['start_lat'], s.get('end_lat', s['start_lat'])])
    
    lng_min, lng_max = min(all_lngs), max(all_lngs)
    lat_min, lat_max = min(all_lats), max(all_lats)
    
    # 扩边（15%）
    lng_pad = (lng_max - lng_min) * 0.15
    lat_pad = (lat_max - lat_min) * 0.15
    lng_min -= lng_pad
    lng_max += lng_pad
    lat_min -= lat_pad
    lat_max += lat_pad
    
    # 创建图表
    fig, ax = plt.subplots(figsize=figsize)
    
    # 绘制 DEM 底图
    print(f"[viz] dem_path parameter: {dem_path}", flush=True)
    print(f"[viz] risk data extent: lng=[{lng_min:.6f}, {lng_max:.6f}], lat=[{lat_min:.6f}, {lat_max:.6f}]", flush=True)
    if dem_path and os.path.exists(dem_path):
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
            vmin = np.nanpercentile(dem_data, 2)
            vmax = np.nanpercentile(dem_data, 98)
            ax.imshow(
                dem_data,
                extent=[dem_lngs[0], dem_lngs[-1], dem_lats[-1], dem_lats[0]],
                cmap='gray',
                vmin=vmin, vmax=vmax,
                alpha=0.6,
                aspect='auto',
                zorder=0
            )
            print("[viz] DEM basemap rendered", flush=True)
        else:
            print("[viz] failed to load DEM data", flush=True)
    else:
        print(f"[viz] no DEM path or file not exists: {dem_path}", flush=True)
    
    # 绘制经纬网（简洁版）
    draw_graticule(ax, lng_min, lng_max, lat_min, lat_max)
    
    # 绘制岸线断面
    for section in sections:
        risk_level = section.get('risk_level', 1)
        color = RISK_COLORS.get(risk_level, '#95a5a6')
        
        start_lng = section['start_lng']
        start_lat = section['start_lat']
        end_lng = section.get('end_lng', start_lng)
        end_lat = section.get('end_lat', start_lat)
        
        # 绘制断面线段
        ax.plot(
            [start_lng, end_lng],
            [start_lat, end_lat],
            color=color,
            linewidth=3,
            alpha=0.9,
            solid_capstyle='round',
            zorder=3
        )
        
        # 绘制端点
        ax.scatter(
            [start_lng, end_lng],
            [start_lat, end_lat],
            c=color,
            s=40,
            zorder=4,
            edgecolors='white',
            linewidth=1.5
        )
    
    # 设置坐标轴（不显示默认标签，用经纬网的标签）
    ax.set_xlim(lng_min, lng_max)
    ax.set_ylim(lat_min, lat_max)
    ax.set_xlabel('')
    ax.set_ylabel('')
    ax.set_xticks([])
    ax.set_yticks([])
    
    # 标题
    ax.set_title(title, fontsize=16, fontweight='bold', pad=15)
    
    # 添加图例（左上角）
    legend_patches = [
        mpatches.Patch(color=RISK_COLORS[level], label=RISK_LABELS[level])
        for level in sorted(RISK_LABELS.keys())
    ]
    ax.legend(
        handles=legend_patches,
        loc='upper left',
        fontsize=9,
        frameon=True,
        framealpha=0.9,
        edgecolor='gray',
        bbox_to_anchor=(0.01, 0.99)
    )
    
    # 统计各等级数量（用于返回描述）
    risk_counts = {}
    for s in sections:
        level = s.get('risk_level', 1)
        risk_counts[level] = risk_counts.get(level, 0) + 1
    
    # 添加比例尺（左下角）
    add_scale_bar(ax, lng_min, lng_max, lat_min, lat_max)
    
    # 添加指北针（右上角）
    add_north_arrow(ax, x=0.95, y=0.90)
    
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
    
    return save_result(
        output_dir, 'risk_distribution', file_path,
        title, f"共 {len(sections)} 个断面，高风险 {risk_counts.get(4, 0)} 个"
    )


def draw_graticule(ax, lng_min, lng_max, lat_min, lat_max):
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
                   ha='center', va='top', fontsize=8, color='black')
        lng += lng_step
    
    # 绘制纬度刻度（左侧）
    lat = np.floor(lat_min / lat_step) * lat_step
    while lat <= lat_max:
        if lat_min <= lat <= lat_max:
            ax.plot([lng_min, lng_min + lng_range * 0.02], [lat, lat],
                   color='black', linewidth=1)
            ax.text(lng_min - lng_range * 0.01, lat, f'{lat:.2f}°',
                   ha='right', va='center', fontsize=8, color='black')
        lat += lat_step


def add_scale_bar(ax, lng_min, lng_max, lat_min, lat_max):
    """添加比例尺（左下角）"""
    center_lat = (lat_min + lat_max) / 2
    lng_range = lng_max - lng_min
    
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
    
    # 比例尺位置
    scale_lng = lng_min + lng_range * 0.05
    scale_lat = lat_min + (lat_max - lat_min) * 0.08
    
    # 比例尺长度（经度单位）
    scale_lng_len = scale_km / lng_to_km
    
    # 绘制比例尺
    bar_height = (lat_max - lat_min) * 0.008
    segments = 3
    seg_len = scale_lng_len / segments
    
    for i in range(segments):
        color = 'black' if i % 2 == 0 else 'white'
        rect = Rectangle(
            (scale_lng + i * seg_len, scale_lat),
            seg_len, bar_height,
            facecolor=color, edgecolor='black', linewidth=1
        )
        ax.add_patch(rect)
    
    # 刻度线
    for i in range(segments + 1):
        x = scale_lng + i * seg_len
        ax.plot([x, x], [scale_lat, scale_lat - bar_height * 0.5], 
                color='black', linewidth=1)
    
    # 文字标注
    ax.text(
        scale_lng + scale_lng_len / 2,
        scale_lat - bar_height * 2,
        f'{scale_km} km',
        ha='center', va='top', fontsize=9, fontweight='bold'
    )


def add_north_arrow(ax, x=0.95, y=0.25):
    """添加指北针（右上角，图例下方）"""
    ax.text(
        x, y + 0.06, 'N',
        transform=ax.transAxes,
        ha='center', va='bottom',
        fontsize=14, fontweight='bold', color='black',
        zorder=55
    )
    ax.add_patch(
        plt.Polygon(
            [(x, y + 0.05),
             (x - 0.015, y),
             (x, y + 0.02),
             (x + 0.015, y)],
            closed=True,
            facecolor='black',
            edgecolor='black',
            transform=ax.transAxes,
            zorder=55
        )
    )
