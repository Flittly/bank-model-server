"""
可视化工具主入口
支持命令行调用和 Python 模块导入
"""
import os
import sys
import json
import argparse
from typing import Optional

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from visualization.risk_map import generate_risk_distribution_map
from visualization.heatmap import generate_scour_heatmap, generate_scour_heatmap_from_profiles
from visualization.section_chart import generate_section_comparison, generate_section_comparison_by_id


def main():
    parser = argparse.ArgumentParser(description='河岸崩塌风险评估可视化工具')
    subparsers = parser.add_subparsers(dest='command', help='可用命令')
    
    # 风险分布图
    risk_parser = subparsers.add_parser('risk-map', help='生成风险分布图')
    risk_parser.add_argument('--data', required=True, help='风险数据 JSON 文件路径')
    risk_parser.add_argument('--title', default='岸段风险分布图', help='图表标题')
    risk_parser.add_argument('--output', help='输出目录')
    risk_parser.add_argument('--name', help='输出文件名')
    
    # 冲淤热力图
    heatmap_parser = subparsers.add_parser('heatmap', help='生成冲淤热力图')
    heatmap_parser.add_argument('--data', required=True, help='网格数据 JSON 文件路径')
    heatmap_parser.add_argument('--title', default='冲淤变化热力图', help='图表标题')
    heatmap_parser.add_argument('--output', help='输出目录')
    heatmap_parser.add_argument('--name', help='输出文件名')
    
    # 断面对比图
    section_parser = subparsers.add_parser('section', help='生成断面对比图')
    section_parser.add_argument('--data', required=True, help='断面数据 JSON 文件路径')
    section_parser.add_argument('--title', help='图表标题')
    section_parser.add_argument('--output', help='输出目录')
    section_parser.add_argument('--name', help='输出文件名')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # 读取数据文件
    with open(args.data, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # 执行对应命令
    if args.command == 'risk-map':
        # 新格式：包含 sections 和 dem_path
        if isinstance(data, dict) and 'sections' in data:
            risk_data = data['sections']
            dem_path = data.get('dem_path')
        else:
            # 兼容旧格式
            risk_data = data
            dem_path = None
        
        result = generate_risk_distribution_map(
            risk_data=risk_data,
            title=args.title,
            output_dir=args.output,
            output_name=args.name,
            dem_path=dem_path
        )
    elif args.command == 'heatmap':
        result = generate_scour_heatmap(
            grid_data=data,
            title=args.title,
            output_dir=args.output,
            output_name=args.name
        )
    elif args.command == 'section':
        result = generate_section_comparison(
            sections=data,
            title=args.title,
            output_dir=args.output,
            output_name=args.name
        )
    else:
        print(json.dumps({'success': False, 'error': f'未知命令: {args.command}'}, ensure_ascii=False))
        return
    
    print(result)


# Python API 接口
def generate_risk_map(risk_data: list, title: str = '岸段风险分布图', 
                      output_dir: Optional[str] = None) -> str:
    """生成风险分布图"""
    return generate_risk_distribution_map(risk_data, title, output_dir)


def generate_heatmap(grid_data: list, title: str = '冲淤变化热力图',
                     output_dir: Optional[str] = None) -> str:
    """生成冲淤热力图"""
    return generate_scour_heatmap(grid_data, title, output_dir)


def generate_section_chart(sections: list, title: str = '断面对比图',
                          output_dir: Optional[str] = None) -> str:
    """生成断面对比图"""
    return generate_section_comparison(sections, title, output_dir)


if __name__ == '__main__':
    main()
