"""
可视化配置和工具函数
"""
import os
import sys
import json
from typing import Optional
from dataclasses import dataclass, asdict

# 强制 UTF-8 输出（Windows 兼容）
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

# 中文字体配置
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'WenQuanYi Micro Hei', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False

# 风险等级颜色配置
RISK_COLORS = {
    1: '#2ecc71',  # 绿色 - 低风险
    2: '#f39c12',  # 橙色 - 中低风险
    3: '#e74c3c',  # 红色 - 中高风险
    4: '#8e44ad',  # 紫色 - 高风险
}

RISK_LABELS = {
    1: '低风险',
    2: '中低风险',
    3: '中高风险',
    4: '高风险',
}

# 图表样式配置
CHART_STYLE = {
    'figure.figsize': (12, 8),
    'figure.dpi': 150,
    'font.size': 12,
    'axes.titlesize': 14,
    'axes.labelsize': 12,
}

# 输出目录
DEFAULT_OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'output')


@dataclass
class RiskData:
    """风险评估数据"""
    section_id: str
    section_name: str
    risk_level: int
    risk_value: float
    bank_name: Optional[str] = None
    start_lng: Optional[float] = None
    start_lat: Optional[float] = None
    end_lng: Optional[float] = None
    end_lat: Optional[float] = None
    ky: Optional[float] = None
    pq: Optional[float] = None
    zd: Optional[float] = None
    sa: Optional[float] = None
    ln: Optional[float] = None
    zb: Optional[float] = None
    dsed: Optional[float] = None


@dataclass 
class SectionProfile:
    """断面剖面数据"""
    section_id: str
    section_name: str
    x_coords: list
    y_coords: list
    elevation: list
    deepest_index: Optional[int] = None
    slope_foot_index: Optional[int] = None
    interval: Optional[float] = None


def ensure_output_dir(output_dir: Optional[str] = None) -> str:
    """确保输出目录存在"""
    out = output_dir or DEFAULT_OUTPUT_DIR
    os.makedirs(out, exist_ok=True)
    return out


def save_result(output_path: str, chart_type: str, file_path: str, 
                title: str, description: str) -> str:
    """保存生成结果为 JSON"""
    result = {
        'success': True,
        'chart_type': chart_type,
        'file_path': file_path,
        'title': title,
        'description': description
    }
    
    result_path = os.path.splitext(file_path)[0] + '.json'
    with open(result_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    
    return json.dumps(result, ensure_ascii=False)
