#!/usr/bin/env python3
"""
参数导入脚本：将 Resource 文件夹中的参数信息导入到 basic_params 表中
"""

import os
import sys
import json
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

import config
from util import db_ops


def normalize_timepoint(value: str) -> str:
    text = str(value).strip()
    if len(text) == 6 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-01"
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    if len(text) == 7 and text[4] == "-":
        return f"{text}-01"
    return text


def load_json_file(file_path: Path) -> dict:
    """加载 JSON 文件"""
    if not file_path.exists():
        raise FileNotFoundError(f"JSON file not found: {file_path}")

    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


def import_parameters():
    """导入所有参数到 basic_params 表"""
    print("=" * 60)
    print("开始导入参数到 basic_params 表")
    print("=" * 60)

    # 基础参数配置
    region_code = "Mzs"
    year = "2023"
    set_name = "standard"

    # 加载风险阈值和权重模板
    risk_level_template_path = (
        Path(config.DIR_RESOURCE)
        / "json"
        / region_code
        / year
        / set_name
        / "RiskLevel"
        / "template.json"
    )
    print(f"\n加载风险参数模板: {risk_level_template_path}")
    risk_level_data = load_json_file(risk_level_template_path)

    # 分离风险阈值和权重
    risk_thresholds = {}
    weights = {}

    for key, value in risk_level_data.items():
        if key.startswith("w"):
            weights[key] = value
        else:
            risk_thresholds[key] = value

    print(f"  - 风险阈值: {list(risk_thresholds.keys())}")
    print(f"  - 权重参数: {list(weights.keys())}")

    # 加载 PQ 数据
    pq_path = (
        Path(config.DIR_RESOURCE)
        / "json"
        / region_code
        / year
        / set_name
        / "PQ"
        / "pq.json"
    )
    print(f"\n加载 PQ 数据: {pq_path}")
    pq_data = load_json_file(pq_path)
    print(f"  - 年份数据: {list(pq_data.keys())}")

    # 定义参数组合（从 hydrodynamic 文件夹获取）
    hydro_path = (
        Path(config.DIR_RESOURCE) / "hydrodynamic" / region_code / "default" / set_name
    )
    print(f"\n扫描水动力数据目录: {hydro_path}")

    param_combinations = []
    if hydro_path.exists():
        for item in hydro_path.iterdir():
            if item.is_dir() and not item.name.startswith("."):
                # 解析目录名，例如 "10000dc" -> water_qs="10000", tidal_level="dc"
                dir_name = item.name
                # 分离数字和字母部分
                import re

                match = re.match(r"(\d+)([a-z]+)", dir_name)
                if match:
                    water_qs = match.group(1)
                    tidal_level = match.group(2)
                    param_combinations.append(
                        {
                            "water_qs": water_qs,
                            "tidal_level": tidal_level,
                            "dir_name": dir_name,
                        }
                    )
                    print(f"  - 发现参数组合: {water_qs} + {tidal_level}")

    # 如果没有找到，使用默认组合
    if not param_combinations:
        print("  - 未找到水动力数据，使用默认参数组合")
        param_combinations = [
            {"water_qs": "10000", "tidal_level": "dc", "dir_name": "10000dc"},
            {"water_qs": "10000", "tidal_level": "xc", "dir_name": "10000xc"},
            {"water_qs": "10000", "tidal_level": "zc", "dir_name": "10000zc"},
            {"water_qs": "104000", "tidal_level": "dc", "dir_name": "104000dc"},
        ]

    # 定义默认的河段和时间点
    segments = ["Mzs"]
    timepoints = ["202304"]
    comparison_timepoint = "201904"

    imported_count = 0
    skipped_count = 0

    print(f"\n开始导入参数...")
    print("-" * 60)

    for segment in segments:
        for timepoint in timepoints:
            for combo in param_combinations:
                current_timepoint = normalize_timepoint(timepoint)
                comparison_timepoint_normalized = normalize_timepoint(
                    comparison_timepoint
                )

                # 生成唯一的 param_id
                param_id = f"PARAM_{segment}_{timepoint}_{combo['water_qs']}_{combo['tidal_level']}"
                param_name = (
                    f"{segment} {timepoint} {combo['water_qs']} {combo['tidal_level']}"
                )

                # 检查是否已存在
                existing = db_ops.get_basic_param(param_id)
                if existing:
                    print(f"⏭️  参数已存在，跳过: {param_id}")
                    skipped_count += 1
                    continue

                # 创建参数记录
                try:
                    param_id_db = db_ops.create_basic_param(
                        param_id=param_id,
                        param_name=param_name,
                        segment=segment,
                        current_timepoint=current_timepoint,
                        set_name=set_name,
                        water_qs=combo["water_qs"],
                        tidal_level=combo["tidal_level"],
                        bench_id=os.path.join(
                            "tiff",
                            segment,
                            year,
                            set_name,
                            timepoint,
                            f"{timepoint}.tif",
                        ),
                        ref_id=os.path.join(
                            "tiff", segment, "2019", "standard", "201904", "201904.tif"
                        ),
                        hs=0.5,
                        hc=2.0,
                        protection_level="systemic",
                        control_level="strict",
                        comparison_timepoint=comparison_timepoint_normalized,
                        risk_thresholds=risk_thresholds,
                        weights=weights,
                        other_params={
                            "pq_data": pq_data,
                            "hydro_dir": combo.get("dir_name"),
                        },
                    )
                    print(f"✅ 成功导入参数: {param_id} (数据库 ID: {param_id_db})")
                    imported_count += 1
                except Exception as e:
                    print(f"❌ 导入参数失败 {param_id}: {e}")

    # 导入一个通用的默认参数模板
    print("\n" + "-" * 60)
    default_param_id = "PARAM_DEFAULT_TEMPLATE"
    if not db_ops.get_basic_param(default_param_id):
        try:
            db_ops.create_basic_param(
                param_id=default_param_id,
                param_name="默认参数模板",
                segment="Mzs",
                current_timepoint=normalize_timepoint("202304"),
                set_name="standard",
                water_qs="10000",
                tidal_level="zc",
                bench_id=os.path.join(
                    "tiff", "Mzs", "2023", "standard", "202304", "202304.tif"
                ),
                ref_id=os.path.join(
                    "tiff", "Mzs", "2019", "standard", "201904", "201904.tif"
                ),
                hs=0.5,
                hc=2.0,
                protection_level="systemic",
                control_level="strict",
                comparison_timepoint=normalize_timepoint("201904"),
                risk_thresholds=risk_thresholds,
                weights=weights,
                other_params={"pq_data": pq_data, "is_default": True},
            )
            print(f"✅ 成功导入默认参数模板: {default_param_id}")
            imported_count += 1
        except Exception as e:
            print(f"❌ 导入默认参数模板失败: {e}")
    else:
        print(f"⏭️  默认参数模板已存在，跳过: {default_param_id}")
        skipped_count += 1

    print("\n" + "=" * 60)
    print(f"导入完成！")
    print(f"  - 成功导入: {imported_count} 个参数")
    print(f"  - 跳过已存在: {skipped_count} 个参数")
    print("=" * 60)


def list_imported_params():
    """列出已导入的参数"""
    print("\n" + "=" * 60)
    print("已导入的参数列表")
    print("=" * 60)
    params = db_ops.get_basic_params()
    if not params:
        print("没有找到已导入的参数")
        return

    for param in params:
        print(f"  - {param['param_id']}: {param['param_name']}")
    print(f"\n总计: {len(params)} 个参数")


if __name__ == "__main__":
    import_parameters()
    list_imported_params()
