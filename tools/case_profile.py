"""把 model case 的端到端耗时拆成阶段——需要服务以 APP_MODEL_PROFILE=1 启动。

一条 case 由三个执行体接力完成，各自只写自己的那一段：

    initialized          HTTP 请求线程：建目录、写 identity.json 完成
    waiting_slot          daemon：并发槽已满，进 deferred 队列
    waiting_dependencies  daemon：父 case 在等指标子 case
    launching             daemon：决定派发，准备起子进程
    starting              子进程：import 全部依赖完成、进入包装器
    running               子进程：开始执行模型代码
    writing_response      子进程：算完，开始写 response.json
    completed             子进程：写盘 + 状态收尾完成

其中 launching → starting 就是"每个 case 都要重付一次的入场费"（起解释器 + import
gdal/numpy/matplotlib + open_case）。这段有多大，直接决定常驻解释器池值不值得做。

用法：
    .venv/Scripts/python.exe tools/case_profile.py                # 按接口汇总
    .venv/Scripts/python.exe tools/case_profile.py --case 0dd82e  # 单个 case 明细
    .venv/Scripts/python.exe tools/case_profile.py --api height   # 只看某类接口
    .venv/Scripts/python.exe tools/case_profile.py --last 30      # 只看最近 N 个

注意：timeline 只在开关打开时写入。关闭开关后，曾经开过的 case 会保留旧
timeline，而 stage 继续演进——所以下面用「最后一条 timeline 的 stage 必须等于
runtime.stage」来剔除这种过期数据，而不是直接拿来算。
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CASE_DIR = os.path.join(ROOT, "case")

NS_PER_MS = 1_000_000


def _load(path: str) -> dict | None:
    try:
        with open(path, encoding="utf-8") as handle:
            data = json.load(handle)
        return data if isinstance(data, dict) else None
    except (OSError, ValueError):
        return None


def _percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(round((len(ordered) - 1) * q)))
    return ordered[index]


def _api_name(api: str) -> str:
    return api.rstrip("/").rsplit("/", 1)[-1] or "?"


def collect(limit: int | None) -> tuple[list[dict], int, int]:
    """返回 (可用样本, 无 timeline 数, 过期 timeline 数)。"""
    if not os.path.isdir(CASE_DIR):
        raise SystemExit(f"找不到 case 目录：{CASE_DIR}")

    entries: list[dict] = []
    no_timeline = 0
    stale = 0

    for cid in os.listdir(CASE_DIR):
        case_path = os.path.join(CASE_DIR, cid)
        runtime = _load(os.path.join(case_path, "runtime.json"))
        if not runtime:
            continue
        timeline = runtime.get("timeline")
        if not isinstance(timeline, list) or len(timeline) < 2:
            no_timeline += 1
            continue
        # 开关关闭后 stage 仍会推进，但 timeline 不再追加 —— 这种样本的首尾差
        # 是假的（少算了尾部），必须剔除而不是当成"很快"。
        if timeline[-1].get("stage") != runtime.get("stage"):
            stale += 1
            continue

        identity = _load(os.path.join(case_path, "identity.json")) or {}
        points = [
            point
            for point in timeline
            if isinstance(point, dict) and isinstance(point.get("at-ns"), int)
        ]
        if len(points) < 2:
            continue

        segments = []
        for prev, nxt in zip(points, points[1:]):
            segments.append(
                {
                    "from": prev.get("stage", "?"),
                    "to": nxt.get("stage", "?"),
                    "ms": (nxt["at-ns"] - prev["at-ns"]) / NS_PER_MS,
                    "same_process": prev.get("pid") == nxt.get("pid"),
                }
            )

        entries.append(
            {
                "case_id": cid,
                "api": _api_name(str(identity.get("url", "?"))),
                "stage": runtime.get("stage"),
                "total_ms": (points[-1]["at-ns"] - points[0]["at-ns"]) / NS_PER_MS,
                "updated_at": runtime.get("updated-at", ""),
                "points": points,
                "segments": segments,
            }
        )

    entries.sort(key=lambda item: item["updated_at"], reverse=True)
    if limit:
        entries = entries[:limit]
    return entries, no_timeline, stale


def print_detail(entries: list[dict], keyword: str) -> None:
    matched = [item for item in entries if item["case_id"].startswith(keyword)]
    if not matched:
        matched = [item for item in entries if keyword in item["case_id"]]
    if not matched:
        raise SystemExit(f"没有匹配 '{keyword}' 的 case")

    for item in matched[:3]:
        print(f"\n=== case {item['case_id']} · {item['api']} · 最终 {item['stage']} ===")
        print(f"    首尾墙钟 {item['total_ms']:.1f} ms（不含 HTTP 往返与响应返回）\n")
        print(f"    {'阶段':22s} {'距上一阶段':>12s}  {'pid':>7s}  线程")
        print(f"    {'-' * 62}")
        first = item["points"][0]
        print(
            f"    {first.get('stage', '?'):22s} {'—':>12s}  "
            f"{first.get('pid', 0):7d}  {first.get('thread', '')}"
        )
        for segment, point in zip(item["segments"], item["points"][1:]):
            print(
                f"    {point.get('stage', '?'):22s} {segment['ms']:10.1f}ms  "
                f"{point.get('pid', 0):7d}  {point.get('thread', '')}"
            )


def print_summary(entries: list[dict], api_filter: str | None) -> None:
    if api_filter:
        entries = [item for item in entries if api_filter in item["api"]]
    if not entries:
        raise SystemExit(
            "没有可用的 timeline 样本。\n"
            "请确认服务是以 APP_MODEL_PROFILE=1 启动的，且之后跑过任务。"
        )

    grouped: dict[str, list[dict]] = defaultdict(list)
    for item in entries:
        grouped[item["api"]].append(item)

    # 全局面板：不按接口分，直接看整体哪一段最贵
    all_segments: dict[str, list[float]] = defaultdict(list)
    for item in entries:
        for segment in item["segments"]:
            all_segments[f"{segment['from']} → {segment['to']}"].append(segment["ms"])

    print(f"\n样本 {len(entries)} 个 case，{len(grouped)} 类接口")
    print("\n=== 全局面板（所有接口合并）===")
    print(f"    {'阶段转换':34s} {'n':>5s} {'p50':>10s} {'p90':>10s} {'max':>10s}")
    print(f"    {'-' * 74}")
    for key, values in sorted(
        all_segments.items(), key=lambda kv: -_percentile(kv[1], 0.5)
    ):
        print(
            f"    {key:34s} {len(values):5d} "
            f"{_percentile(values, 0.5):9.1f}ms {_percentile(values, 0.9):9.1f}ms "
            f"{max(values):9.1f}ms"
        )

    entrance = [
        segment["ms"]
        for item in entries
        for segment in item["segments"]
        if segment["from"] == "launching" and segment["to"] == "starting"
    ]
    calc = [
        segment["ms"]
        for item in entries
        for segment in item["segments"]
        if segment["from"] == "running" and segment["to"] == "writing_response"
    ]
    if entrance:
        print(
            f"\n    入场费（launching→starting）: p50 {_percentile(entrance, 0.5):.1f} ms "
            f"· p90 {_percentile(entrance, 0.9):.1f} ms  （n={len(entrance)}）"
        )
    if calc:
        print(
            f"    纯计算（running→writing）  : p50 {_percentile(calc, 0.5):.1f} ms "
            f"· p90 {_percentile(calc, 0.9):.1f} ms  （n={len(calc)}）"
        )

    print("\n=== 按接口 ===")
    for api, items in sorted(grouped.items(), key=lambda kv: -len(kv[1])):
        totals = [item["total_ms"] for item in items]
        print(
            f"\n[{api}]  n={len(items)}  "
            f"首尾中位数 {_percentile(totals, 0.5):.1f} ms  "
            f"p90 {_percentile(totals, 0.9):.1f} ms"
        )
        per_segment: dict[str, list[float]] = defaultdict(list)
        for item in items:
            for segment in item["segments"]:
                per_segment[f"{segment['from']} → {segment['to']}"].append(segment["ms"])
        base = _percentile(totals, 0.5) or 1.0
        for key, values in sorted(
            per_segment.items(), key=lambda kv: -_percentile(kv[1], 0.5)
        ):
            share = _percentile(values, 0.5) / base * 100
            print(
                f"    {key:34s} n={len(values):<4d} "
                f"p50={_percentile(values, 0.5):8.1f}ms  "
                f"p90={_percentile(values, 0.9):8.1f}ms  {share:5.1f}%"
            )


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description="按阶段拆解 model case 耗时（需 APP_MODEL_PROFILE=1）"
    )
    parser.add_argument("--case", help="只看某个 case（支持 id 前缀）")
    parser.add_argument("--api", help="只看接口名包含该子串的样本，如 height")
    parser.add_argument("--last", type=int, help="只取最近 N 个 case")
    args = parser.parse_args(argv)

    entries, no_timeline, stale = collect(args.last)

    if not entries:
        print(
            "没有可用的 timeline 样本。\n"
            f"  扫描到 {no_timeline} 个无 timeline 的 case（未开埋点）\n"
            f"  剔除 {stale} 个过期样本（开关关闭后 stage 仍在推进）\n\n"
            "请用 APP_MODEL_PROFILE=1 重启模型服务，跑一条任务后重试。",
            file=sys.stderr,
        )
        return 1

    if args.case:
        print_detail(entries, args.case)
    else:
        print_summary(entries, args.api)

    if no_timeline or stale:
        print(
            f"\n（另有 {no_timeline} 个 case 无 timeline、{stale} 个已过期，未计入）"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
