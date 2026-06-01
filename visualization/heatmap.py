"""
冲淤变化热力图生成器
显示河床高程变化的热力图
"""
import os
import json
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import numpy as np
from typing import List, Optional, Tuple

from .config import CHART_STYLE, ensure_output_dir, save_result


def generate_scour_heatmap(
    grid_data: List[List[float]],
    title: str = "冲淤变化热力图",
    x_label: str = "距离 (m)",
    y_label: str = "河宽 (m)",
    unit: str = "m",
    output_dir: Optional[str] = None,
    output_name: Optional[str] = None,
    figsize: tuple = (14, 8),
    cmap: str = 'RdBu',
    vmin: Optional[float] = None,
    vmax: Optional[float] = None
) -> str:
    """
    生成冲淤变化热力图
    
    Args:
        grid_data: 二维网格数据（高程变化值）
        title: 图表标题
        x_label: X轴标签
        y_label: Y轴标签
        unit: 单位
        output_dir: 输出目录
        output_name: 输出文件名
        figsize: 图表大小
        cmap: 颜色映射
        vmin: 最小值
        vmax: 最大值
        
    Returns:
        JSON 格式的生成结果
    """
    plt.rcParams.update(CHART_STYLE)
    
    data = np.array(grid_data)
    
    if data.size == 0:
        return json.dumps({'success': False, 'error': '网格数据为空'}, ensure_ascii=False)
    
    # 自动计算范围
    if vmin is None:
        vmin = np.nanpercentile(data, 5)
    if vmax is None:
        vmax = np.nanpercentile(data, 95)
    
    # 创建图表
    fig, ax = plt.subplots(figsize=figsize)
    
    # 绘制热力图
    im = ax.imshow(
        data,
        cmap=cmap,
        aspect='auto',
        origin='lower',
        vmin=vmin,
        vmax=vmax,
        interpolation='bilinear'
    )
    
    # 添加颜色条
    cbar = plt.colorbar(im, ax=ax, label=f'高程变化 ({unit})', shrink=0.8)
    
    # 设置标签
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    ax.set_title(title, fontsize=16, fontweight='bold', pad=20)
    
    # 添加零线
    if vmin < 0 < vmax:
        ax.contour(data, levels=[0], colors='black', linewidths=1, linestyles='--', alpha=0.5)
    
    # 添加统计信息
    stats = {
        'mean': float(np.nanmean(data)),
        'std': float(np.nanstd(data)),
        'min': float(np.nanmin(data)),
        'max': float(np.nanmax(data)),
        'erosion_area': float(np.sum(data < -0.1) / data.size * 100),
        'deposition_area': float(np.sum(data > 0.1) / data.size * 100)
    }
    
    stats_text = (
        f"平均变化: {stats['mean']:.3f} {unit}\n"
        f"冲刷面积: {stats['erosion_area']:.1f}%\n"
        f"淤积面积: {stats['deposition_area']:.1f}%"
    )
    
    ax.text(
        0.02, 0.98, stats_text,
        transform=ax.transAxes,
        fontsize=10,
        verticalalignment='top',
        bbox=dict(boxstyle='round', facecolor='white', alpha=0.8, edgecolor='gray')
    )
    
    plt.tight_layout()
    
    # 保存图片
    output_dir = ensure_output_dir(output_dir)
    if output_name is None:
        output_name = 'scour_heatmap.png'
    file_path = os.path.join(output_dir, output_name)
    plt.savefig(file_path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()
    
    return save_result(
        output_dir, 'scour_heatmap', file_path,
        title, f"冲刷面积 {stats['erosion_area']:.1f}%，淤积面积 {stats['deposition_area']:.1f}%"
    )


def generate_scour_heatmap_from_profiles(
    profiles: List[dict],
    title: str = "冲淤变化热力图",
    output_dir: Optional[str] = None,
    output_name: Optional[str] = None
) -> str:
    """
    从断面剖面数据生成冲淤热力图
    
    Args:
        profiles: 断面剖面数据列表，每项包含 x_coords, elevation
        title: 图表标题
        output_dir: 输出目录
        output_name: 输出文件名
        
    Returns:
        JSON 格式的生成结果
    """
    if not profiles:
        return json.dumps({'success': False, 'error': '没有断面数据'}, ensure_ascii=False)
    
    # 构建网格数据
    max_points = max(len(p.get('x_coords', [])) for p in profiles)
    grid_data = []
    
    for profile in profiles:
        elevation = profile.get('elevation', [])
        if len(elevation) > 0:
            # 插值到统一长度
            if len(elevation) != max_points:
                x_old = np.linspace(0, 1, len(elevation))
                x_new = np.linspace(0, 1, max_points)
                elevation = np.interp(x_new, x_old, elevation)
            grid_data.append(elevation)
    
    if not grid_data:
        return json.dumps({'success': False, 'error': '无法构建网格数据'}, ensure_ascii=False)
    
    return generate_scour_heatmap(
        grid_data, title, "断面距离", "断面编号",
        output_dir=output_dir, output_name=output_name
    )
