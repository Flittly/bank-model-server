#!/usr/bin/env python3
"""
1998 大洪水水动力数据导入（2018 地形 / 2023 地形 两套）

════════════════════════════════════════════════════════════════════════
一、源数据格式
════════════════════════════════════════════════════════════════════════
每套目录 26 个帧文件，文件名形如 `210-1998-2018-xg-uvhH.txt` … `235-...`
（前缀 210–235 = 帧号；stem 不是纯数字，所以不能沿用 import_hydrodynamic.py
 那套 int(stem) 的排序/取 max 逻辑）。

文件内容（空格分隔）：
    第 1 行   头部标识（单个整数，与数据行数不符，含义待提供方确认）——跳过
    第 2 行起  7 列：X  Y  地形  U  V  水位  水深
                      1   2   3    4  5   6    7
    坐标 X/Y 单位 m，投影 EPSG:2437（Beijing1954 / 3°带 CM 120°E）；
    地形/水位/水深 均为 1985 国家高程基面的 m。
    已验证恒等式：地形 ≡ 水位 − 水深（纯舍入级偏差）⇒ 三列只有 2 个自由度。

════════════════════════════════════════════════════════════════════════
二、列映射（关键：两边列序不同，映射错误整库报废）
════════════════════════════════════════════════════════════════════════
平台 raw 的表头是 `X Y H P U V`（见 model/hydrodynamic_resource.py:185-189，
H=fort.14 水深，P=fort.63 水位，U/V=fort.64 流速），而源数据是
`X Y 地形 U V 水位 水深`——**顺序完全不同**。正确映射：

    平台 h（水深）  ←  源第 7 列（水深）
    平台 p（水位）  ←  源第 6 列（水位）
    平台 u          ←  源第 4 列（U）
    平台 v          ←  源第 5 列（V）
    源第 3 列（地形）→  丢弃（≡ 水位 − 水深，冗余）

⚠ 若照抄 import_hydrodynamic.py 的 `x y h p u v` 位置解析，会把「地形」
  写进 h、「U」写进 p，风险计算直接全错。

h 是否动态写入：源数据的「水深」逐帧变化，而平台既有数据 h 是静态
（fort.14 地形）。全仓检索确认 h 在模型里**只有一个消费点**
（modelResource/multipleIndicators/waterLevelFluctuation.py:224 的
 `Z = float(item.get("h") or 0.0)`，且在 `if Z == 0` 保护下只取首帧），
所以逐帧写入动态水深对计算**完全等价**，却保留了原始信息 → 按原样写入。

════════════════════════════════════════════════════════════════════════
三、导入标签
════════════════════════════════════════════════════════════════════════
    dataset_domain = Yangtze            （与已统一的 DEFAULT_DOMAIN 一致）
    set_name       = standard
    water_qs       = 84000              （数据方已确认的边界流量档）
    tidal_level    = 1998-2018 / 1998-2023   （用该字段区分两套地形版本；
                                             它是唯一区分维度，且 VARCHAR(20) 放得下）
    time_step      = 0 .. 25            （源帧号 210-235 归一化为 0 基连续序号，
                                         与平台既有 0..51 的口径一致）

幂等：points 用 ON CONFLICT (point_id) DO NOTHING；
      data 用 ON CONFLICT (point_id, time_step) DO NOTHING。
      重复执行不会产生重复行，也不会报唯一约束错误。

用法：
    # 预演（只统计、不写库）
    uv run python -m util.import_hydrodynamic_1998 --dry-run

    # 实际导入（两套，各 26 帧）
    uv run python -m util.import_hydrodynamic_1998 --yes

    # 小样本试跑
    uv run python -m util.import_hydrodynamic_1998 --limit-points 2000 --limit-frames 3 --yes
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from psycopg2.extras import execute_values  # noqa: E402

from util import db  # noqa: E402
from util.db_ops import DEFAULT_DOMAIN  # noqa: E402

# ── 源数据根目录（rar 解压后的两个目录）────────────────────────────────────
SOURCE_ROOT = Path(
    r"E:\Graduate\graduate-1\workspace\#资料\yangtze-bank-warning-system"
    r"\相关资料\水文条件数据"
)

# 一套数据 = (地形版本, 子目录名)；tidal_level 用「1998-<地形年>」表达
DATASETS = [
    ("2018", "1998大洪水-2018地形/XYZUVhH"),
    ("2023", "1998大洪水-2023地形/UVZUVhH"),
]

SRID_SOURCE = 2437  # Beijing1954 / 3°带 CM 120°E（源 X/Y 实测反算吻合）
SENTINEL = -99999.0  # 平台既有数据中 p/u/v 的缺测哨兵值（源数据不含）
BATCH_POINTS = 2000
BATCH_DATA = 5000
FRAME_RE = re.compile(r"^(\d+)-")


# ══════════════════════════════════════════════════════════════════════════
# 源数据读取
# ══════════════════════════════════════════════════════════════════════════
def list_frames(src_dir: Path) -> list[Path]:
    """按帧号前缀数值升序返回帧文件（跳过 0格式说明.txt）。"""
    frames = []
    for p in src_dir.glob("*.txt"):
        m = FRAME_RE.match(p.name)
        if m:
            frames.append((int(m.group(1)), p))
    frames.sort(key=lambda t: t[0])
    return [p for _, p in frames]


def read_frame(path: Path, limit: int = 0) -> list[tuple]:
    """
    读取一帧，返回 [(x, y, u, v, p, h), ...]
    列序：1=X 2=Y 3=地形 4=U 5=V 6=水位 7=水深
    映射后：u<-4, v<-5, p<-6(水位), h<-7(水深)；第 3 列丢弃。
    """
    out: list[tuple] = []
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        f.readline()  # 跳过头部标识
        for line in f:
            parts = line.split()
            if len(parts) < 7:
                continue
            try:
                x = float(parts[0])
                y = float(parts[1])
                u = float(parts[3])
                v = float(parts[4])
                p = float(parts[5])  # 水位
                h = float(parts[6])  # 水深
            except ValueError:
                continue
            out.append((x, y, u, v, p, h))
            if limit and len(out) >= limit:
                break
    return out


def coord_key(x: float, y: float) -> str:
    """坐标的规范化字符串（源数据 X/Y 为 2 位小数），用于拼 point_id。"""
    return f"{x:.2f}_{y:.2f}"


def build_point_id(domain: str, set_name: str, qs: str, tidal: str, x: float, y: float) -> str:
    return f"HYDRO_{domain}_{set_name}_{qs}_{tidal}_{coord_key(x, y)}"


# ══════════════════════════════════════════════════════════════════════════
# 导入
# ══════════════════════════════════════════════════════════════════════════
def import_one_dataset(
    *,
    terrain_year: str,
    rel_dir: str,
    domain: str,
    set_name: str,
    water_qs: str,
    user_id: int,
    limit_points: int,
    limit_frames: int,
    dry_run: bool,
) -> dict:
    tidal_level = f"1998-{terrain_year}"
    src_dir = SOURCE_ROOT / rel_dir

    print("=" * 76)
    print(f"数据集：{tidal_level}   （{rel_dir}）")
    print("=" * 76)

    if not src_dir.is_dir():
        print(f"  ✗ 源目录不存在：{src_dir}")
        return {"ok": False, "tidal_level": tidal_level}

    frames = list_frames(src_dir)
    if limit_frames:
        frames = frames[:limit_frames]
    if not frames:
        print("  ✗ 未找到帧文件")
        return {"ok": False, "tidal_level": tidal_level}

    print(f"  帧文件 {len(frames)} 个：{frames[0].name} … {frames[-1].name}")

    # ── 1) 从首帧取坐标清单 ────────────────────────────────────────────
    t0 = time.time()
    first = read_frame(frames[0], limit_points)
    if not first:
        print("  ✗ 首帧无有效数据行")
        return {"ok": False, "tidal_level": tidal_level}
    n_points = len(first)
    print(f"  坐标点 {n_points:,} 个（读取首帧 {time.time() - t0:.1f}s）")

    # point_id 长度红线：VARCHAR(100)
    sample_pid = build_point_id(domain, set_name, water_qs, tidal_level, first[0][0], first[0][1])
    if len(sample_pid) > 100:
        print(f"  ✗ point_id 超长（{len(sample_pid)} > 100）：{sample_pid}")
        return {"ok": False, "tidal_level": tidal_level}
    print(f"  point_id 样例（{len(sample_pid)} 字符）：{sample_pid}")

    # 坐标唯一性自检
    keys = {coord_key(r[0], r[1]) for r in first}
    if len(keys) != n_points:
        print(f"  ⚠ 首帧存在重复坐标：{n_points:,} 行 / {len(keys):,} 个唯一坐标")

    if dry_run:
        # 预演：把剩余帧也读一遍，给出真实行数与取值范围，但不写库
        stats = _scan_range(frames, limit_points)
        print(f"  待写入点：{n_points:,}")
        print(f"  待写入数据：{n_points:,} × {len(frames)} = {n_points * len(frames):,} 行")
        print(f"  源值域（{len(frames)} 帧）：{stats}")
        return {
            "ok": True,
            "tidal_level": tidal_level,
            "points": n_points,
            "rows": n_points * len(frames),
            "dry_run": True,
        }

    # ── 2) 插点 ────────────────────────────────────────────────────────
    t0 = time.time()
    point_rows = []
    for x, y, *_ in first:
        point_rows.append(
            (
                build_point_id(domain, set_name, water_qs, tidal_level, x, y),
                domain,
                set_name,
                water_qs,
                tidal_level,
                False,  # temp
                x,
                y,
                x,  # geom: ST_MakePoint(x, y)
                y,
                user_id,
            )
        )

    with db.get_db_cursor() as (conn, cur):
        for i in range(0, len(point_rows), BATCH_POINTS):
            chunk = point_rows[i : i + BATCH_POINTS]
            execute_values(
                cur,
                """
                INSERT INTO hydrodynamic_points
                    (point_id, dataset_domain, set_name, water_qs, tidal_level,
                     temp, x, y, geom, user_id)
                VALUES %s
                ON CONFLICT (point_id) DO NOTHING
                """,
                chunk,
                template=(
                    "(%s,%s,%s,%s,%s,%s,%s,%s,"
                    f" ST_Transform(ST_SetSRID(ST_MakePoint(%s,%s), {SRID_SOURCE}), 4326),"
                    " %s)"
                ),
                page_size=BATCH_POINTS,
            )
    print(f"  插点完成 {time.time() - t0:.1f}s（行数 {len(point_rows):,}，冲突忽略）")

    # ── 3) 取回 point_id → id 映射（按四元组查询，走索引）──────────────
    with db.get_db_cursor(dict_cursor=True) as (conn, cur):
        cur.execute(
            """
            SELECT id, point_id FROM hydrodynamic_points
            WHERE dataset_domain = %s AND set_name = %s
              AND water_qs = %s AND tidal_level = %s
            """,
            (domain, set_name, water_qs, tidal_level),
        )
        id_map = {r["point_id"]: r["id"] for r in cur.fetchall()}
    print(f"  取回 point_id→id 映射 {len(id_map):,} 条")

    # 按坐标序构建 (x,y) → db id 的数组，与逐帧行顺序对齐
    pid_to_db = id_map
    ordered_ids = [
        pid_to_db.get(build_point_id(domain, set_name, water_qs, tidal_level, x, y))
        for x, y, *_ in first
    ]
    missing = sum(1 for v in ordered_ids if v is None)
    if missing:
        print(f"  ⚠ 有 {missing:,} 个点在库中未取到 id，这些点的数据将被跳过")

    # ── 4) 逐帧插数据 ──────────────────────────────────────────────────
    total_rows = 0
    t0 = time.time()
    for step, path in enumerate(frames):
        rows = read_frame(path, limit_points)
        payload = []
        for (x, y, u, v, p, h), db_id in zip(rows, ordered_ids):
            if db_id is None:
                continue
            payload.append((db_id, step, h, p, u, v, user_id))

        with db.get_db_cursor() as (conn, cur):
            for i in range(0, len(payload), BATCH_DATA):
                execute_values(
                    cur,
                    """
                    INSERT INTO hydrodynamic_data
                        (point_id, time_step, h, p, u, v, user_id)
                    VALUES %s
                    ON CONFLICT (point_id, time_step) DO NOTHING
                    """,
                    payload[i : i + BATCH_DATA],
                    page_size=BATCH_DATA,
                )
        total_rows += len(payload)
        print(
            f"    帧 {step:>2}/{len(frames) - 1}  {path.name:<34} "
            f"写入 {len(payload):>7,} 行  （累计 {total_rows:,}）"
        )

    print(f"  数据写入完成 {time.time() - t0:.1f}s，共 {total_rows:,} 行")
    return {
        "ok": True,
        "tidal_level": tidal_level,
        "points": n_points,
        "rows": total_rows,
    }


def _scan_range(frames: list[Path], limit_points: int) -> str:
    """预演用：扫全部帧给出 h/p/u/v 的取值范围。"""
    import math

    mn = {"h": math.inf, "p": math.inf, "u": math.inf, "v": math.inf}
    mx = {"h": -math.inf, "p": -math.inf, "u": -math.inf, "v": -math.inf}
    sentinel = 0
    for path in frames:
        for _x, _y, u, v, p, h in read_frame(path, limit_points):
            for k, val in (("h", h), ("p", p), ("u", u), ("v", v)):
                if val == SENTINEL:
                    sentinel += 1
                    continue
                if val < mn[k]:
                    mn[k] = val
                if val > mx[k]:
                    mx[k] = val
    return (
        f"h {mn['h']:.3f}~{mx['h']:.3f} | p {mn['p']:.3f}~{mx['p']:.3f} | "
        f"u {mn['u']:.3f}~{mx['u']:.3f} | v {mn['v']:.3f}~{mx['v']:.3f} | "
        f"哨兵 {sentinel}"
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="1998 大洪水水动力数据导入")
    ap.add_argument("--domain", default=DEFAULT_DOMAIN, help=f"数据集域（默认 {DEFAULT_DOMAIN}）")
    ap.add_argument("--set-name", default="standard")
    ap.add_argument("--water-qs", default="84000", help="流量档（数据方已确认为 84000）")
    ap.add_argument("--limit-points", type=int, default=0, help="每个数据集的点数上限（0=不限）")
    ap.add_argument("--limit-frames", type=int, default=0, help="每套的帧数上限（0=全部）")
    ap.add_argument("--dry-run", action="store_true", help="只统计不写库")
    ap.add_argument("--yes", action="store_true", help="跳过交互确认")
    ap.add_argument(
        "--only",
        default="",
        help="只导入指定地形版本（2018 / 2023），逗号分隔",
    )
    args = ap.parse_args()

    only = {s.strip() for s in args.only.split(",") if s.strip()}
    targets = [d for d in DATASETS if not only or d[0] in only]
    if not targets:
        print(f"✗ --only={args.only} 未匹配任何数据集（可选 2018/2023）")
        return 2

    with db.get_db_cursor(dict_cursor=True) as (conn, cur):
        cur.execute("SELECT id FROM users WHERE username = 'admin'")
        row = cur.fetchone()
    if not row:
        print("✗ 未找到 admin 用户，无法确定 user_id")
        return 2
    user_id = row["id"]

    print("=" * 76)
    print("1998 大洪水水动力数据导入")
    print("=" * 76)
    print(f"  源根目录   : {SOURCE_ROOT}")
    print(f"  dataset_domain : {args.domain}")
    print(f"  set_name       : {args.set_name}")
    print(f"  water_qs       : {args.water_qs}")
    print(f"  user_id        : {user_id} (admin)")
    print(f"  SRID           : {SRID_SOURCE} → 4326")
    print(f"  数据集         : {', '.join(f'1998-{d[0]}' for d in targets)}")
    print(f"  模式           : {'DRY-RUN（不写库）' if args.dry_run else '实际导入'}")
    print()

    if not args.dry_run and not args.yes:
        ans = input("确认写入数据库？(y/N): ").strip().lower()
        if ans != "y":
            print("已取消")
            return 1

    t_all = time.time()
    results = []
    for terrain_year, rel_dir in targets:
        results.append(
            import_one_dataset(
                terrain_year=terrain_year,
                rel_dir=rel_dir,
                domain=args.domain,
                set_name=args.set_name,
                water_qs=args.water_qs,
                user_id=user_id,
                limit_points=args.limit_points,
                limit_frames=args.limit_frames,
                dry_run=args.dry_run,
            )
        )
        print()

    print("=" * 76)
    print("汇总")
    print("=" * 76)
    ok = 0
    for r in results:
        if r.get("ok"):
            ok += 1
            tag = "（预演）" if r.get("dry_run") else ""
            print(
                f"  ✓ {r['tidal_level']:<12} 点 {r['points']:>7,}  数据 {r['rows']:>9,} {tag}"
            )
        else:
            print(f"  ✗ {r['tidal_level']:<12} 失败")
    print(f"\n  成功 {ok}/{len(results)}，总耗时 {time.time() - t_all:.1f}s")
    return 0 if ok == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
