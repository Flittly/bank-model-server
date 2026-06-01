"""
断面对比图生成器
对比不同时间点的断面形态变化
"""
import os
import json
import matplotlib.pyplot as plt
import numpy as np
from typing import List, Optional

from .config import CHART_STYLE, ensure_output_dir, save_result


def generate_section_comparison(
    sections: List[dict],
    title: str = "断面对比图",
    output_dir: Optional[str] = None,
    output_name: Optional[str] = None,
    figsize: tuple = (14, 8),
    show_deepest: bool = True,
    show_slope_foot: bool = True
) -> str:
    """
    生成断面对比图
    
    Args:
        sections: 断面数据列表，每项包含:
            - section_name: 断面名称
            - x_coords: X坐标列表（水平距离）
            - elevation: 高程列表
            - deepest_index: 最深点索引（可选）
            - slope_foot_index: 坡脚点索引（可选）
            - timepoint: 时间点（可选，用于区分不同时间）
        title: 图表标题
        output_dir: 输出目录
        output_name: 输出文件名
        figsize: 图表大小
        show_deepest: 是否显示最深点
        show_slope_foot: 是否显示坡脚点
        
    Returns:
        JSON 格式的生成结果
    """
    plt.rcParams.update(CHART_STYLE)
    
    if not sections:
        return json.dumps({'success': False, 'error': '没有断面数据'}, ensure_ascii=False)
    
    # 颜色列表
    colors = ['#3498db', '#e74c3c', '#2ecc71', '#f39c12', '#9b59b6', '#1abc9c']
    
    # 创建图表
    fig, ax = plt.subplots(figsize=figsize)
    
    # 绘制各断面
    for i, section in enumerate(sections):
        x_coords = section.get('x_coords', [])
        elevation = section.get('elevation', [])
        
        if not x_coords or not elevation:
            continue
        
        color = colors[i % len(colors)]
        label = section.get('section_name', f'断面{i+1}')
        timepoint = section.get('timepoint', '')
        
        if timepoint:
            label = f"{label} ({timepoint})"
        
        # 绘制断面线
        ax.plot(x_coords, elevation, color=color, linewidth=2, label=label, alpha=0.8)
        
        # 填充断面下方
        ax.fill_between(x_coords, elevation, alpha=0.1, color=color)
        
        # 标记最深点
        deepest_idx = section.get('deepest_index')
        if show_deepest and deepest_idx is not None and 0 <= deepest_idx < len(x_coords):
            ax.scatter(
                x_coords[deepest_idx], elevation[deepest_idx],
                c=color, s=100, marker='v', zorder=5, edgecolors='black', linewidth=1.5
            )
            ax.annotate(
                f'最深点\n{elevation[deepest_idx]:.2f}m',
                xy=(x_coords[deepest_idx], elevation[deepest_idx]),
                xytext=(0, -30), textcoords='offset points',
                fontsize=9, ha='center',
                arrowprops=dict(arrowstyle='->', color=color),
                bbox=dict(boxstyle='round', facecolor='white', alpha=0.8, edgecolor=color)
            )
        
        # 标记坡脚点
        slope_foot_idx = section.get('slope_foot_index')
        if show_slope_foot and slope_foot_idx is not None and 0 <= slope_foot_idx < len(x_coords):
            ax.scatter(
                x_coords[slope_foot_idx], elevation[slope_foot_idx],
                c=color, s=80, marker='^', zorder=5, edgecolors='black', linewidth=1.5
            )
            ax.annotate(
                f'坡脚\n{elevation[slope_foot_idx]:.2f}m',
                xy=(x_coords[slope_foot_idx], elevation[slope_foot_idx]),
                xytext=(0, 20), textcoords='offset points',
                fontsize=9, ha='center',
                arrowprops=dict(arrowstyle='->', color=color),
                bbox=dict(boxstyle='round', facecolor='white', alpha=0.8, edgecolor=color)
            )
    
    # 设置图表属性
    ax.set_xlabel('水平距离 (m)')
    ax.set_ylabel('高程 (m)')
    ax.set_title(title, fontsize=16, fontweight='bold', pad=20)
    ax.legend(loc='best', fontsize=10)
    ax.grid(True, alpha=0.3, linestyle='--')
    
    # 设置Y轴比例适当
    ax.set_aspect('equal')
    
    plt.tight_layout()
    
    # 保存图片
    output_dir = ensure_output_dir(output_dir)
    if output_name is None:
        output_name = 'section_comparison.png'
    file_path = os.path.join(output_dir, output_name)
    plt.savefig(file_path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()
    
    return save_result(
        output_dir, 'section_comparison', file_path,
        title, f"对比 {len(sections)} 个断面"
    )


def generate_section_comparison_by_id(
    section_id: str,
    profiles: List[dict],
    title: Optional[str] = None,
    output_dir: Optional[str] = None,
    output_name: Optional[str] = None
) -> str:
    """
    按断面ID生成时间序列对比图
    
    Args:
        section_id: 断面ID
        profiles: 该断面在不同时间点的剖面数据
        title: 图表标题
        output_dir: 输出目录
        output_name: 输出文件名
        
    Returns:
        JSON 格式的生成结果
    """
    if not profiles:
        return json.dumps({'success': False, 'error': f'没有找到断面 {section_id} 的数据'}, ensure_ascii=False)
    
    sections = []
    for profile in profiles:
        # 解析剖面数据
        profile_data = profile.get('profile_data')
        if isinstance(profile_data, str):
            try:
                profile_data = json.loads(profile_data)
            except:
                continue
        
        if not profile_data:
            continue
        
        points = profile_data.get('points', [])
        if not points:
            continue
        
        # 提取坐标和高程
        x_coords = []
        elevation = []
        for point in points:
            if len(point) >= 3:
                x_coords.append(point[0])
                elevation.append(point[2])
        
        sections.append({
            'section_name': profile.get('section_name', section_id),
            'x_coords': x_coords,
            'elevation': elevation,
            'deepest_index': profile.get('deepest_index'),
            'slope_foot_index': profile.get('slope_foot_index'),
            'timepoint': profile.get('timepoint', '')
        })
    
    if title is None:
        title = f"断面 {section_id} 时间序列对比"
    
    if output_name is None:
        output_name = f'section_{section_id}_comparison.png'
    
    return generate_section_comparison(
        sections, title, output_dir, output_name
    )
