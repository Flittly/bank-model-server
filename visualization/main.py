"""
可视化工具主入口
支持命令行调用和 Python 模块导入
"""
import os
import sys
import json
import argparse

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from visualization.risk_map import (
    generate_risk_distribution_map, generate_risk_map_tiles,
)
from visualization.heatmap import generate_scour_heatmap, generate_scour_heatmap_from_profiles
from visualization.section_chart import generate_section_comparison, generate_section_comparison_by_id


def _basemap_from_cli(args) -> dict | None:
    """把命令行底图参数拼成配置；命令行没提底图时返回 None（沿用载荷里的配置）。"""
    if getattr(args, 'no_basemap', False):
        return {'enabled': False}

    touched = any([
        getattr(args, 'basemap', False),
        args.basemap_provider, args.basemap_layer, args.basemap_zoom,
        args.basemap_path, args.basemap_url, args.basemap_alpha,
    ])
    if not touched:
        return None

    if args.basemap_provider:
        provider = args.basemap_provider
    elif args.basemap_url:
        provider = 'custom'
    elif args.basemap_path:
        provider = 'local'
    else:
        provider = 'tianditu'

    config = {'enabled': True, 'provider': provider}
    if args.basemap_layer:
        config['layer'] = args.basemap_layer
    if args.basemap_zoom:
        config['zoom'] = args.basemap_zoom
    if args.basemap_path:
        config['path'] = args.basemap_path
    if args.basemap_url:
        config['url_template'] = args.basemap_url
    if args.basemap_alpha is not None:
        config['alpha'] = args.basemap_alpha
    return config


def main():
    parser = argparse.ArgumentParser(description='河岸崩塌风险评估可视化工具')
    subparsers = parser.add_subparsers(dest='command', help='可用命令')
    
    # 风险分布图
    risk_parser = subparsers.add_parser('risk-map', help='生成风险分布图')
    risk_parser.add_argument('--data', required=True, help='风险数据 JSON 文件路径')
    risk_parser.add_argument('--title', default='岸段风险分布图', help='图表标题')
    risk_parser.add_argument('--output', help='输出目录')
    risk_parser.add_argument('--name', help='输出文件名')
    # 遥感影像底图（可选，画在最底层，不影响 DEM/岸段/断面）
    risk_parser.add_argument('--basemap', action='store_true',
                             help='在最底层叠加遥感影像底图（默认天地图影像；'
                                  'key 取环境变量 TIANDITU_TK）')
    risk_parser.add_argument('--no-basemap', action='store_true',
                             help='强制关闭底图（覆盖载荷里的 basemap 配置）')
    risk_parser.add_argument('--basemap-provider', choices=['tianditu', 'local', 'custom'],
                             help='底图来源：tianditu（默认）/ local（本地瓦片目录）/ custom（URL 模板）')
    risk_parser.add_argument('--basemap-layer', default=None,
                             help='天地图图层：img(影像) / cia(影像注记) / vec / cva / ter / cta')
    risk_parser.add_argument('--basemap-zoom', type=int, default=None,
                             help='手动指定瓦片缩放级（缺省按成图分辨率自适应）')
    risk_parser.add_argument('--basemap-path', default=None,
                             help='provider=local 时的本地瓦片根目录（<root>/<z>/<x>/<y>.jpg）')
    risk_parser.add_argument('--basemap-url', default=None,
                             help='provider=custom 时的瓦片 URL 模板，含 {z} {x} {y}')
    risk_parser.add_argument('--basemap-alpha', type=float, default=None,
                             help='底图不透明度 0~1（缺省 1.0）')
    # 分幅出图：按经纬度范围把一幅图切成 rows×cols 幅子图
    risk_parser.add_argument('--split-rows', type=int, default=1,
                             help='纬度方向分块数（1=不分，缺省 1）')
    risk_parser.add_argument('--split-cols', type=int, default=1,
                             help='经度方向分块数（1=不分，缺省 1）')
    risk_parser.add_argument('--split-tiles', default=None,
                             help='逐幅自定义边界 JSON 文件：[{"row":0,"col":1,'
                                  '"west":119.3,"south":32.2,...}, ...]，'
                                  '缺省的边沿用均分网格的对应边界')
    
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
        # 新格式：包含 sections / banks / dem_path / basemap / options
        if isinstance(data, dict) and 'sections' in data:
            risk_data = data['sections']
            dem_path = data.get('dem_path')
            banks = data.get('banks')
            options = data.get('options') or {}
            basemap_cfg = data.get('basemap')
        else:
            # 兼容旧格式
            risk_data = data
            dem_path = None
            banks = None
            options = {}
            basemap_cfg = None
        if basemap_cfg is None:
            basemap_cfg = options.get('basemap')

        # 命令行显式给了底图参数就以命令行为准（便于本地单独验证底图取图）
        cli_basemap = _basemap_from_cli(args)
        if cli_basemap is not None:
            print('[viz] 底图配置来自命令行，覆盖载荷中的 basemap', flush=True)
            basemap_cfg = cli_basemap

        # gap_warn_meters 不给就用自适应阈值（按该岸段断面的实际间距决定），
        # 固定阈值会把采样本来就稀疏的岸段整条误判成「内插」而画成虚线。
        gap_override = options.get('gap_warn_meters')

        # 分幅出图：载荷 split 键提供 rows/cols/tiles，命令行参数覆盖之。
        # rows×cols 均分；tiles 逐幅自定义边界（缺省的边沿用网格对应边界）。
        split = data.get('split') if isinstance(data, dict) else {}
        split = split or {}
        split_rows = int(split.get('rows') or 1)
        split_cols = int(split.get('cols') or 1)
        split_tiles = split.get('tiles')
        if args.split_rows != 1 or args.split_cols != 1:
            split_rows, split_cols = args.split_rows, args.split_cols
        if args.split_tiles:
            with open(args.split_tiles, 'r', encoding='utf-8') as handle:
                split_tiles = json.load(handle)

        if split_rows < 1 or split_cols < 1:
            result = json.dumps({'success': False,
                                 'error': f'分块数必须 >=1: rows={split_rows}, cols={split_cols}'},
                                ensure_ascii=False)
        elif split_rows == 1 and split_cols == 1 and not split_tiles:
            # 未分块：走原单图路径，行为与历史版本完全一致
            result = generate_risk_distribution_map(
                risk_data=risk_data,
                title=args.title,
                output_dir=args.output,
                output_name=args.name,
                dem_path=dem_path,
                banks=banks,
                gap_meters=float(gap_override) if gap_override else None,
                basemap=basemap_cfg,
            )
        else:
            result = generate_risk_map_tiles(
                risk_data=risk_data,
                title=args.title,
                output_dir=args.output,
                output_name=args.name,
                rows=split_rows,
                cols=split_cols,
                tiles=split_tiles,
                dem_path=dem_path,
                banks=banks,
                gap_meters=float(gap_override) if gap_override else None,
                basemap=basemap_cfg,
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


if __name__ == '__main__':
    main()
