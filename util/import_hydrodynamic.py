#!/usr/bin/env python3
"""
水动力数据导入脚本 - 高性能批量插入版
使用 psycopg2 的 executemany 批量插入，大幅提升速度
"""

import os
import sys
import re
import time
from pathlib import Path

project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

import config
from util import db


def get_temp_flag(item: Path) -> bool:
    description_path = item / "description.json"
    if not description_path.exists():
        return False
    try:
        import json

        with open(description_path, "r", encoding="utf-8") as f:
            return bool(json.load(f).get("temp", False))
    except Exception:
        return False


def parse_single_file_for_point(file_path: Path, point_key: tuple) -> dict:
    """
    只解析一个文件中特定坐标点的数据
    返回: {time_step: (h, p, u, v)}
    """
    x_target, y_target = point_key
    result = {}

    with open(file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()
        if len(lines) < 3:
            return result

        try:
            time_step = int(file_path.stem)
        except ValueError:
            return result

        for line in lines[2:]:
            line = line.strip()
            if not line:
                continue
            parts = line.split()
            if len(parts) >= 6:
                try:
                    x = float(parts[0])
                    y = float(parts[1])
                    if abs(x - x_target) < 0.0001 and abs(y - y_target) < 0.0001:
                        h = float(parts[2])
                        p = float(parts[3])
                        u = float(parts[4])
                        v = float(parts[5])
                        result[time_step] = (h, p, u, v)
                        break
                except (ValueError, IndexError):
                    continue
    return result


def get_points_from_first_file(raw_dir: Path, max_points: int = 0) -> list:
    """
    从第一个文件中获取所有坐标点列表
    """
    txt_files = sorted(
        raw_dir.glob("*.txt"), key=lambda f: int(f.stem) if f.stem.isdigit() else 999
    )
    if not txt_files:
        return []

    first_file = txt_files[0]
    points = []

    with open(first_file, "r", encoding="utf-8") as f:
        lines = f.readlines()
        if len(lines) < 3:
            return []

        for line in lines[2:]:
            line = line.strip()
            if not line:
                continue
            parts = line.split()
            if len(parts) >= 2:
                try:
                    x = float(parts[0])
                    y = float(parts[1])
                    points.append((x, y))
                    if max_points > 0 and len(points) >= max_points:
                        break
                except (ValueError, IndexError):
                    continue
    return points


def import_single_flow_optimized(
    hydro_path: Path,
    flow_level: str,
    region_code: str,
    set_name: str,
    max_points: int = 0,
):
    """
    优化版：导入单个流量级的所有潮位数据
    使用批量插入大幅提升速度
    """
    tidal_levels = ["dc", "xc", "zc"]
    folders = [f"{flow_level}{tl}" for tl in tidal_levels]

    total_points = 0
    total_data = 0
    overall_start = time.time()

    for dir_name in folders:
        folder_start = time.time()
        print(f"\n处理: {dir_name}")

        item = hydro_path / dir_name
        temp_flag = get_temp_flag(item)
        raw_dir = item / "raw"
        if not raw_dir.exists():
            print(f"  跳过: raw文件夹不存在")
            continue

        txt_files = sorted(
            raw_dir.glob("*.txt"),
            key=lambda f: int(f.stem) if f.stem.isdigit() else 999,
        )
        if not txt_files:
            print(f"  跳过: 没有数据文件")
            continue

        max_time_step = max(int(f.stem) for f in txt_files if f.stem.isdigit())
        print(f"  时段范围: 0-{max_time_step}")

        print(f"  读取坐标点列表...", end="", flush=True)
        points = get_points_from_first_file(raw_dir, max_points)
        print(f"  发现 {len(points)} 个点")

        if not points:
            continue

        print(f"  准备批量插入...", end="", flush=True)
        prepare_start = time.time()

        point_records = []
        point_map = {}

        for idx, (x, y) in enumerate(points):
            point_id = (
                f"HYDRO_{region_code}_{set_name}_{flow_level}_{dir_name[-2:]}_{x}_{y}"
            )
            point_records.append(
                {
                    "point_id": point_id,
                    "region_code": region_code,
                    "set_name": set_name,
                    "water_qs": flow_level,
                    "tidal_level": dir_name[-2:],
                    "temp": temp_flag,
                    "x": x,
                    "y": y,
                }
            )
            point_map[(x, y)] = idx

        prepare_time = time.time() - prepare_start
        print(f"  完成 ({prepare_time:.1f}秒)")

        print(f"  插入点数据...", end="", flush=True)
        insert_points_start = time.time()

        inserted_point_ids = []
        with db.get_db_cursor() as (conn, cursor):
            for pr in point_records:
                try:
                    cursor.execute(
                        """
                        INSERT INTO hydrodynamic_points (
                            point_id, region_code, set_name, water_qs, tidal_level, temp, x, y, geom
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s,
                            ST_SetSRID(ST_MakePoint(%s, %s), 4326)
                        )
                        ON CONFLICT (point_id) DO NOTHING
                        RETURNING id
                        """,
                        (
                            pr["point_id"],
                            pr["region_code"],
                            pr["set_name"],
                            pr["water_qs"],
                            pr["tidal_level"],
                            pr["temp"],
                            pr["x"],
                            pr["y"],
                            pr["x"],
                            pr["y"],
                        ),
                    )
                    result = cursor.fetchone()
                    if result:
                        inserted_point_ids.append(result[0])
                    else:
                        inserted_point_ids.append(None)
                except Exception:
                    inserted_point_ids.append(None)

        insert_points_time = time.time() - insert_points_start
        actual_inserted = len([pid for pid in inserted_point_ids if pid is not None])
        print(f"  完成 ({insert_points_time:.1f}秒) - 新插入 {actual_inserted} 个点")

        if actual_inserted == 0:
            print(f"  所有点已存在，跳过数据插入")
            continue

        print(f"  读取时段数据并批量插入...", end="", flush=True)
        insert_data_start = time.time()

        data_batch = []
        BATCH_SIZE = 5000
        data_inserted = 0

        for time_step in range(max_time_step + 1):
            file_path = raw_dir / f"{time_step}.txt"
            if not file_path.exists():
                continue

            file_data = {}
            with open(file_path, "r", encoding="utf-8") as f:
                lines = f.readlines()
                for line in lines[2:]:
                    line = line.strip()
                    if not line:
                        continue
                    parts = line.split()
                    if len(parts) >= 6:
                        try:
                            x = float(parts[0])
                            y = float(parts[1])
                            h = float(parts[2])
                            p = float(parts[3])
                            u = float(parts[4])
                            v = float(parts[5])
                            file_data[(x, y)] = (h, p, u, v)
                        except (ValueError, IndexError):
                            continue

            for idx, (x, y) in enumerate(points):
                point_id_db = inserted_point_ids[idx]
                if point_id_db is None:
                    continue

                hpv = file_data.get((x, y))
                if hpv:
                    h, p, u, v = hpv
                else:
                    h, p, u, v = (None, None, None, None)

                data_batch.append((point_id_db, time_step, h, p, u, v))

                if len(data_batch) >= BATCH_SIZE:
                    with db.get_db_cursor() as (conn, cursor):
                        cursor.executemany(
                            """
                            INSERT INTO hydrodynamic_data (point_id, time_step, h, p, u, v)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            """,
                            data_batch,
                        )
                    data_inserted += len(data_batch)
                    data_batch = []

        if data_batch:
            with db.get_db_cursor() as (conn, cursor):
                cursor.executemany(
                    """
                    INSERT INTO hydrodynamic_data (point_id, time_step, h, p, u, v)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    data_batch,
                )
            data_inserted += len(data_batch)

        insert_data_time = time.time() - insert_data_start
        print(f"  完成 ({insert_data_time:.1f}秒) - 插入 {data_inserted} 条数据")

        folder_time = time.time() - folder_start
        print(f"  {dir_name} 总耗时: {folder_time:.1f}秒")

        total_points += actual_inserted
        total_data += data_inserted

    overall_time = time.time() - overall_start
    print(f"\n流量级 {flow_level} 完成！")
    print(f"  总耗时: {overall_time:.1f}秒")
    print(f"  导入点: {total_points} 个")
    print(f"  导入数据: {total_data} 条")

    return total_points, total_data


def import_hydrodynamic_data():
    """导入所有水动力数据"""
    print("=" * 60)
    print("水动力数据导入工具 - 高性能版")
    print("=" * 60)

    region_code = "Mzs"
    set_name = "standard"

    hydro_path = (
        Path(config.DIR_RESOURCE) / "hydrodynamic" / region_code / "default" / set_name
    )
    print(f"\n扫描目录: {hydro_path}")

    if not hydro_path.exists():
        print(f"❌ 目录不存在: {hydro_path}")
        return

    from collections import defaultdict

    folders_by_flow = defaultdict(list)
    all_folders = []

    for item in hydro_path.iterdir():
        if item.is_dir() and not item.name.startswith("."):
            match = re.match(r"(\d+)([a-z]+)", item.name)
            if match:
                water_qs = match.group(1)
                tidal_level = match.group(2)
                folders_by_flow[water_qs].append(item.name)
                all_folders.append(item.name)

    flow_levels = sorted(folders_by_flow.keys(), key=int)

    print(f"\n发现 {len(flow_levels)} 个流量级:")
    for i, flow in enumerate(flow_levels, 1):
        tidal_levels = sorted(folders_by_flow[flow])
        print(f"  {i}. {flow} ({', '.join(tidal_levels)})")

    print("\n请选择导入模式:")
    print("  1. 导入单个流量级")
    print("  2. 导入全部流量级")

    choice = input("\n请输入选项 (1-2): ").strip()

    selected_flows = []
    max_points = 0

    if choice == "1":
        flow_choice = input(
            "请输入要导入的流量级序号 (1-{}): ".format(len(flow_levels))
        ).strip()
        try:
            idx = int(flow_choice) - 1
            if 0 <= idx < len(flow_levels):
                selected_flows = [flow_levels[idx]]
                print(f"\n已选择流量级: {selected_flows[0]}")

                max_points_input = input("\n限制导入的点数 (直接回车不限制): ").strip()
                if max_points_input:
                    max_points = int(max_points_input)
            else:
                print("❌ 无效的序号")
                return
        except ValueError:
            print("❌ 无效的输入")
            return

    elif choice == "2":
        selected_flows = flow_levels
        confirm = (
            input("确认要导入全部 {} 个流量级？(y/n): ".format(len(selected_flows)))
            .strip()
            .lower()
        )
        if confirm != "y":
            print("已取消")
            return
    else:
        print("❌ 无效的选项")
        return

    total_points = 0
    total_data = 0
    overall_start = time.time()

    for flow in selected_flows:
        points, data = import_single_flow_optimized(
            hydro_path, flow, region_code, set_name, max_points
        )
        total_points += points
        total_data += data

    overall_time = time.time() - overall_start

    print("\n" + "=" * 60)
    print(f"全部完成！")
    print(f"  总耗时: {overall_time:.1f} 秒")
    print(f"  导入点: {total_points} 个")
    print(f"  导入数据: {total_data} 条")
    print("=" * 60)


def list_imported_hydrodynamic_data():
    """列出已导入的水动力数据统计"""
    print("\n" + "=" * 60)
    print("已导入的水动力数据统计")
    print("=" * 60)

    region_code = "Mzs"
    set_name = "standard"

    from util import db_ops

    points = db_ops.get_hydrodynamic_points(region_code=region_code, set_name=set_name)

    if not points:
        print("没有找到已导入的水动力数据")
        return

    flow_count = {}
    flow_tidal = {}

    for point in points:
        flow = point["water_qs"]
        tidal = point["tidal_level"]

        if flow not in flow_count:
            flow_count[flow] = 0
            flow_tidal[flow] = []

        flow_count[flow] += 1
        if tidal not in flow_tidal[flow]:
            flow_tidal[flow].append(tidal)

    print("\n按流量级统计:")
    for flow in sorted(flow_count.keys(), key=int):
        tidal_sorted = sorted(flow_tidal[flow])
        print(f"  - {flow}: {flow_count[flow]} 个点 (潮位: {', '.join(tidal_sorted)})")

    print(f"\n总计: {len(points)} 个坐标点")


if __name__ == "__main__":
    import_hydrodynamic_data()
    list_imported_hydrodynamic_data()
