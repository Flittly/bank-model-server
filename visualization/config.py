"""
可视化配置和工具函数
"""
import os
import sys
import json
from typing import Optional
from dataclasses import dataclass, asdict

# 强制 UTF-8 输出（Windows 兼容）
# 用 reconfigure 原地调整编码，不要新建 TextIOWrapper 覆盖 sys.stdout：
# 覆盖会让原流失去引用，在 pytest 等已经替换过 sys.stdout 的场景下，
# 旧流被 GC 关闭后会在测试框架收尾时抛 "I/O operation on closed file"。
if sys.platform == 'win32':
    for _stream in (sys.stdout, sys.stderr):
        try:
            _stream.reconfigure(encoding='utf-8')
        except Exception:
            pass

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

# 中文字体配置
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'WenQuanYi Micro Hei', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False

# 风险等级颜色配置
# 键 = bank_risk_results.risk_level，即模型侧 one-hot 索引+1，取值 1..4（1 最低、4 最高）。
# 依据：kafka_executor._normalize_risk_level 与 TaskExecutionService.toRiskLevel 均为
#       「one-hot 中为 1 的下标 + 1」；0 表示解析失败/未知。
# ⚠ 别照抄前端 ResultPage 的 0-3 表：前端是对 indicators.result（0~1 浮点）自行分档
#   （<0.25→0, <0.5→1, <0.75→2, else→3），输入不同、起点不同。
#   两套语义同向（数值越大风险越高），所以这里的键 = 前端 level + 1，色值保持一致。
#
# 色值唯一真源：`bank-warning-web/src/utils/riskScoring.ts` 的 RISK_COLORS
# （2026-10-20 定稿：一级红 / 二级橙 / 三级蓝 / 低风险绿）。出图与页面必须逐值相同，
# 否则同一份结果在报告里和在页面上是两种颜色。改色请两边一起改。
RISK_COLORS = {
    1: '#10b981',  # 绿色 - 低风险（页面同样不着色，见 RISK_RENDERED_LEVELS）
    2: '#3b82f6',  # 蓝色 - 三级风险（前端 level 1）
    3: '#f97316',  # 橙色 - 二级风险（前端 level 2）
    4: '#ef4444',  # 红色 - 一级风险（前端 level 3）
}

RISK_LABELS = {
    1: '低风险',
    2: '一般风险',
    3: '高风险',
    4: '极高风险',
}

#: 出图时**真正着色**的等级。与页面口径一致（sectionRenderVisibility.ts）：
#:   - `risk_level` 1 = 低风险（页面显示等级 0）→ 不渲染、不着色；
#:   - 缺失 / 0 / 非整数 = 未计算或计算失败 → 不渲染、不着色。
#: 只画 2/3/4，即页面上的一级、二级、三级风险。
RISK_RENDERED_LEVELS = (2, 3, 4)

#: 图例条目（等级从高到低，与页面「一级/二级/三级」的叫法一致）。
RISK_LEGEND = (
    (4, '一级风险'),
    (3, '二级风险'),
    (2, '三级风险'),
)

#: 等级缺失/未知（0 或 NULL）时的兜底色
DEFAULT_RISK_COLOR = '#94a3b8'

#: 出图支持的最高风险等级
RISK_LEVEL_MAX = 4


def risk_color(section_or_level) -> str:
    """取风险颜色。可传断面 dict（读 risk_level）或直接传等级值。

    0 / None / 非法值视为「未知」，返回灰色兜底；
    超过 4 的值按最高档处理并告警，避免静默画成灰色而看不出数据异常。
    """
    level = section_or_level.get('risk_level') if isinstance(section_or_level, dict) else section_or_level
    try:
        level = int(level)
    except (TypeError, ValueError):
        return DEFAULT_RISK_COLOR
    if level > RISK_LEVEL_MAX:
        import logging
        logging.getLogger(__name__).warning(
            '风险等级 %s 超出出图口径 1-%d，已按最高档着色', level, RISK_LEVEL_MAX)
        level = RISK_LEVEL_MAX
    if level < 1:
        return DEFAULT_RISK_COLOR
    return RISK_COLORS.get(level, DEFAULT_RISK_COLOR)


def is_rendered_level(level) -> bool:
    """该等级是否要在图上着色。

    只认 :data:`RISK_RENDERED_LEVELS`（2/3/4）。``None``、0、非整数、越界一律 False ——
    刻意不把非法值兜底成某个等级：兜底等于把「没算」画成「算出来了」，
    而页面（sectionRenderVisibility.ts）正是把这类断面整段留空的。
    """
    if level is None or isinstance(level, bool):
        return False
    try:
        value = float(level)
    except (TypeError, ValueError):
        return False
    if not (value == value and value not in (float('inf'), float('-inf'))):  # NaN / inf
        return False
    if value != int(value):
        return False
    return int(value) in RISK_RENDERED_LEVELS

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
