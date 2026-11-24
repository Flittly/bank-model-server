"""对模型服务做一次端到端耗时采样：提交一个 case，然后按固定间隔轮询直到完成。

行为刻意与 Java 网关一致（ModelGatewayService）：
    POST /v0/{category}/{model}  → 拿 case-id（异步）
    GET  /v0/mc/status?case_id=  → 每 poll-ms 问一次，直到 completed/error

采样时会对 payload 做微扰，让 md5 变、真正重算（命中缓存就测不到耗时了）：
    带 section-geometry 的 → 第一个坐标 +1e-7 度（约 1cm，不影响计算能否成功）
    不带几何的（PL/LC/Dsed/PQ） → 追加一个 _bench 键，模型只读已知字段，多余键被忽略

用法：
    .venv/Scripts/python.exe tools/case_bench.py --api re/section-view -n 5
    .venv/Scripts/python.exe tools/case_bench.py --all -n 3 --poll-ms 200
"""

from __future__ import annotations

import argparse
import copy
import json
import os
import shutil
import sys
import time
import urllib.request
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CASE_DIR = os.path.join(ROOT, "case")

DEFAULT_APIS = [
    "mi/load-control",
    "re/section-view",
    "mi/height-difference",
    "mi/slope-rate",
    "mi/anti-impact-speed",
    "mi/water-level-fluctuation",
    "mi/nearshore-flush",
    "mi/risk-level",
]

# 本地服务不走代理，否则企业代理会把 127.0.0.1 也拦掉
_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def _load(path: str) -> dict | None:
    try:
        with open(path, encoding="utf-8") as handle:
            data = json.load(handle)
        return data if isinstance(data, dict) else None
    except (OSError, ValueError):
        return None


def request_json(url: str, payload: dict | None = None, timeout: float = 120) -> dict:
    body = json.dumps(payload).encode("utf-8") if payload is not None else None
    headers = {"Content-Type": "application/json"} if body else {}
    req = urllib.request.Request(url, data=body, headers=headers)
    with _OPENER.open(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def snapshot_cases() -> set[str]:
    if not os.path.isdir(CASE_DIR):
        return set()
    return {name for name in os.listdir(CASE_DIR) if os.path.isdir(os.path.join(CASE_DIR, name))}


def find_sample(api_suffix: str) -> dict | None:
    """挑一个已完成、同接口的历史 case，复制它的 payload 当模板。"""
    newest: tuple[float, dict] | None = None
    for cid in os.listdir(CASE_DIR):
        case_path = os.path.join(CASE_DIR, cid)
        identity = _load(os.path.join(case_path, "identity.json"))
        if not identity:
            continue
        if not str(identity.get("url", "")).endswith(api_suffix):
            continue
        runtime = _load(os.path.join(case_path, "runtime.json")) or {}
        if runtime.get("stage") != "completed":
            continue
        mtime = os.path.getmtime(os.path.join(case_path, "identity.json"))
        payload = identity.get("json")
        if not isinstance(payload, dict):
            continue
        if newest is None or mtime > newest[0]:
            newest = (mtime, payload)
    return newest[1] if newest else None


def perturb(payload: dict, tag: str) -> tuple[dict, str]:
    """让 md5 一定变化，否则会命中缓存、测出几毫秒的假数据。

    两件事一起做：坐标 +1e-7（进入子 payload，保证下游指标也真算），
    再挂一个 _bench 标记（模型只读已知键，多余键被忽略，但会改变 md5）。
    """
    mutated = copy.deepcopy(payload)
    mode = "_bench"
    geometry = mutated.get("section-geometry")
    if isinstance(geometry, dict):
        coords = geometry.get("coordinates")
        if isinstance(coords, list) and coords and isinstance(coords[0], list):
            coords[0][0] = round(float(coords[0][0]) + 1e-7, 10)
            mode = "geometry+1e-7"
    mutated["_bench"] = tag
    return mutated, mode


def run_once(base_url: str, api: str, payload: dict, poll_ms: float) -> dict:
    started = time.perf_counter()
    submit = request_json(f"{base_url}/v0/{api}", payload)
    submit_ms = (time.perf_counter() - started) * 1000.0

    case_id = submit.get("case-id") or submit.get("case_id") or submit.get("id")
    if not case_id:
        raise RuntimeError(f"提交未返回 case-id：{submit}")

    polls = 0
    status = ""
    runtime: dict = {}
    poll_costs: list[float] = []
    poll_started = time.perf_counter()
    while True:
        polls += 1
        tick = time.perf_counter()
        state = request_json(f"{base_url}/v0/mc/status?case_id={case_id}")
        poll_costs.append((time.perf_counter() - tick) * 1000.0)
        status = str(state.get("status", "")).lower()
        runtime = state.get("runtime") or {}
        if status in ("completed", "error"):
            break
        if time.perf_counter() - started > 300:
            raise TimeoutError(f"{case_id} 超时未完成，最后状态 {status}")
        time.sleep(poll_ms / 1000.0)

    total_ms = (time.perf_counter() - started) * 1000.0

    # 模型侧写下 completed 的时刻 → 客户端看到的时刻，两者之差就是"算完了但没人知道"的白等
    lag_ms = None
    updated = runtime.get("updated-at")
    if isinstance(updated, str):
        try:
            lag_ms = (datetime.now() - datetime.fromisoformat(updated)).total_seconds() * 1000
        except ValueError:
            lag_ms = None

    return {
        "case_id": case_id,
        "api": api,
        "status": status,
        "submit_ms": submit_ms,
        "total_ms": total_ms,
        "polls": polls,
        "lag_ms": lag_ms,
        "poll_p50": percentile(poll_costs, 0.5),
        "poll_max": max(poll_costs) if poll_costs else 0.0,
        "poll_sum": sum(poll_costs),
    }


def percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(round((len(ordered) - 1) * q)))]


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="模型服务端到端耗时采样")
    parser.add_argument("--base-url", default="http://127.0.0.1:8088")
    parser.add_argument("--api", action="append", help="接口后缀，如 re/section-view，可重复")
    parser.add_argument("--all", action="store_true", help="跑内置的 8 个接口")
    parser.add_argument("-n", "--repeat", type=int, default=5, help="每个接口采样次数")
    parser.add_argument("--poll-ms", type=float, default=50, help="轮询间隔（默认 50ms）")
    parser.add_argument("--cleanup", action="store_true", help="结束时删除本次新增的 case 目录")
    args = parser.parse_args(argv)

    apis = DEFAULT_APIS if args.all else (args.api or [])
    if not apis:
        parser.error("请用 --api 指定接口，或用 --all 跑内置列表")

    before = snapshot_cases()
    results: list[dict] = []
    skipped: list[str] = []

    for api in apis:
        template = find_sample(api)
        if not template:
            skipped.append(api)
            print(f"[skip] {api}：case/ 里没有已完成的同类样本可作模板")
            continue
        print(f"\n[{api}]")
        for index in range(args.repeat):
            payload, mode = perturb(template, f"bench-{int(time.time() * 1000)}-{index}")
            try:
                row = run_once(args.base_url, api, payload, args.poll_ms)
            except Exception as exc:  # noqa: BLE001 — 单次失败不该中断整轮采样
                print(f"  #{index + 1} 失败：{exc}")
                continue
            results.append(row)
            lag = f"{row['lag_ms']:6.1f}ms" if row["lag_ms"] is not None else "     —"
            print(
                f"  #{index + 1} {row['status']:9s} 总 {row['total_ms']:8.1f}ms  "
                f"提交 {row['submit_ms']:6.1f}ms  轮询 {row['polls']:3d} 次 "
                f"(p50 {row['poll_p50']:5.1f}/max {row['poll_max']:6.1f}ms)  "
                f"发现延迟 {lag}  [{mode}]"
            )

    if not results:
        print("\n没有成功的采样。", file=sys.stderr)
        return 1

    print("\n=== 汇总 ===")
    print(
        f"  {'接口':26s} {'n':>3s} {'p50':>10s} {'p90':>10s} "
        f"{'提交p50':>10s} {'status p50':>11s} {'发现延迟p50':>12s}"
    )
    by_api: dict[str, list[dict]] = {}
    for row in results:
        by_api.setdefault(row["api"], []).append(row)
    for api, rows in by_api.items():
        totals = [r["total_ms"] for r in rows]
        submits = [r["submit_ms"] for r in rows]
        lags = [r["lag_ms"] for r in rows if r["lag_ms"] is not None]
        poll_p50 = [r["poll_p50"] for r in rows]
        print(
            f"  {api:26s} {len(rows):3d} {percentile(totals, 0.5):9.1f}ms "
            f"{percentile(totals, 0.9):9.1f}ms {percentile(submits, 0.5):9.1f}ms "
            f"{percentile(poll_p50, 0.5):10.1f}ms "
            f"{percentile(lags, 0.5) if lags else 0:11.1f}ms"
        )

    created = snapshot_cases() - before
    print(f"\n本次新增 case 目录 {len(created)} 个")
    if args.cleanup and created:
        removed = 0
        for cid in created:
            target = os.path.join(CASE_DIR, cid)
            if os.path.isdir(target):
                shutil.rmtree(target, ignore_errors=True)
                removed += 1
        print(f"已清理 {removed} 个")
    elif created:
        print("（未清理；加 --cleanup 可删除本次新增的目录）")

    if skipped:
        print(f"跳过 {len(skipped)} 个接口：{', '.join(skipped)}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
