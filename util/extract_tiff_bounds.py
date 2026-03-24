"""
批量提取 tiff 边界并存储到数据库
用法: uv run extract_tiff_bounds.py
"""

import sys
import os

# 确保项目根目录在路径中
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import config
from util.rustfs import extract_tiff_bounds
from util.db_ops import save_tiff_bounds


def extract_all_tiff_bounds():
    """提取 resource/tiff 目录下所有 tiff 文件的边界"""
    tiff_dir = os.path.join(config.DIR_RESOURCE, "tiff")

    if not os.path.exists(tiff_dir):
        print(f"tiff 目录不存在: {tiff_dir}")
        return

    count = 0
    for root, dirs, files in os.walk(tiff_dir):
        for file in files:
            if file.endswith(".tif") or file.endswith(".tiff"):
                tiff_path = os.path.join(root, file)

                # 构建 tiff_key (相对路径)
                tiff_key = os.path.relpath(tiff_path, config.DIR_RESOURCE).replace(
                    "\\", "/"
                )

                # 解析 region_code, year, timepoint
                parts = tiff_key.split("/")
                region_code = parts[1] if len(parts) > 1 else None
                year = parts[2] if len(parts) > 2 else None
                timepoint = parts[4] if len(parts) > 4 else None

                try:
                    print(f"提取边界: {tiff_key}")
                    bounds = extract_tiff_bounds(tiff_path)

                    save_tiff_bounds(
                        tiff_key=tiff_key,
                        region_code=region_code,
                        year=year,
                        timepoint=timepoint,
                        min_x=bounds["min_x"],
                        min_y=bounds["min_y"],
                        max_x=bounds["max_x"],
                        max_y=bounds["max_y"],
                        geom_wkt=bounds["geom_wkt"],
                    )
                    count += 1
                    print(
                        f"  -> 成功: ({bounds['min_x']:.2f}, {bounds['min_y']:.2f}) - ({bounds['max_x']:.2f}, {bounds['max_y']:.2f})"
                    )
                except Exception as e:
                    print(f"  -> 失败: {e}")

    print(f"\n完成! 共提取 {count} 个 tiff 文件的边界")


if __name__ == "__main__":
    extract_all_tiff_bounds()
