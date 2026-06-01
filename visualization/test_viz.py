"""
可视化模块测试脚本
"""
import json
from visualization.risk_map import generate_risk_distribution_map
from visualization.heatmap import generate_scour_heatmap
from visualization.section_chart import generate_section_comparison


def test_risk_map():
    """测试风险分布图"""
    print("测试风险分布图...")
    
    risk_data = [
        {
            "section_id": "sec-001",
            "section_name": "断面1",
            "risk_level": 3,
            "risk_value": 0.2236,
            "bank_name": "长江南京段",
            "start_lng": 119.87,
            "start_lat": 32.22,
            "end_lng": 119.90,
            "end_lat": 32.23
        },
        {
            "section_id": "sec-002",
            "section_name": "断面2",
            "risk_level": 1,
            "risk_value": 0.1500,
            "bank_name": "长江南京段",
            "start_lng": 119.91,
            "start_lat": 32.24,
            "end_lng": 119.93,
            "end_lat": 32.25
        },
        {
            "section_id": "sec-003",
            "section_name": "断面3",
            "risk_level": 4,
            "risk_value": 0.3100,
            "bank_name": "长江南京段",
            "start_lng": 119.94,
            "start_lat": 32.26,
            "end_lng": 119.96,
            "end_lat": 32.27
        }
    ]
    
    result = generate_risk_distribution_map(risk_data, "测试风险分布图")
    print(result)
    print()


def test_heatmap():
    """测试冲淤热力图"""
    print("测试冲淤热力图...")
    
    import numpy as np
    grid_data = np.random.randn(10, 20).tolist()
    
    result = generate_scour_heatmap(grid_data, "测试冲淤热力图")
    print(result)
    print()


def test_section_comparison():
    """测试断面对比图"""
    print("测试断面对比图...")
    
    sections = [
        {
            "section_name": "2023年断面",
            "x_coords": [0, 10, 20, 30, 40, 50],
            "elevation": [35, 32, 28, 25, 30, 34],
            "deepest_index": 3,
            "slope_foot_index": 1
        },
        {
            "section_name": "2024年断面",
            "x_coords": [0, 10, 20, 30, 40, 50],
            "elevation": [35, 31, 26, 23, 29, 33],
            "deepest_index": 3,
            "slope_foot_index": 1
        }
    ]
    
    result = generate_section_comparison(sections, "测试断面对比图")
    print(result)
    print()


if __name__ == '__main__':
    print("=" * 50)
    print("可视化模块测试")
    print("=" * 50)
    print()
    
    test_risk_map()
    test_heatmap()
    test_section_comparison()
    
    print("=" * 50)
    print("测试完成！请查看 visualization/output 目录")
    print("=" * 50)
