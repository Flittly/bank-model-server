"""把「永远停在 LOCK 的 case」救回来。

背景（状态机）
--------------
case 的状态不是数据库里的一行，而是 ``case/<id>/status/`` 目录下的**一个文件名**，
文件名是十进制位掩码（见 ``config.py``）：

======  =============  ============================================
值      含义           谁写
======  =============  ============================================
1       UNLOCK         创建时；跑完/失败时与 COMPLETE/ERROR 一起
2       LOCK           ``mcr_checker`` 入队前认领
4       RUNNING        子进程进入 ``model_status_controller_sync``
8       COMPLETE       子进程写完 response.json
32      ERROR          子进程抛异常，或父进程发现它死了
======  =============  ============================================

``LOCK`` 的语义只有一句「这个 case 已经有人认领，别人别抢」，但它
**既不记录持有者、也没有租约（lease）**。于是认领者（父进程排队的那个子进程）
一旦死掉，没有任何人会来解开这把锁：``status/2`` 就永远躺在那里。
``/v0/mc/status`` 对 LOCK 恒回 ``pending``，Java 网关只能每秒轮询到 600s 超时。

本模块只做两件事
----------------
1. 判定一把锁是否已经过期——判据是「还有没有活的监控进程在写心跳」；
2. 把过期锁重置成 UNLOCK，让下一次请求（或启动自愈）能重新入队。

心跳文件 ``case/<id>/monitor.heartbeat`` 由 ``modelProcess.spawn_case_subprocess``
起的监控线程每 ``MODEL_CASE_MONITOR_INTERVAL_SECONDS`` 秒刷新一次，
子进程退出时删除。因此：

* 心跳新鲜  ⇒ 子进程真的在跑（哪怕正在 import 阶段、还没写 RUNNING），一律不动；
* 心跳缺失/很旧 ⇒ 持有者已消失，这把锁是僵尸，可以安全重置。
"""

from __future__ import annotations

import datetime
import os
from typing import Any

import config
from .modelCaseReference import ModelCaseReference as MCR

HEARTBEAT_FILENAME = "monitor.heartbeat"

TERMINAL_STATUS = config.STATUS_COMPLETE | config.STATUS_ERROR
ACTIVE_STATUS = config.STATUS_LOCK | config.STATUS_RUNNING


def get_heartbeat_path(case_id: str) -> str:
    return os.path.join(config.DIR_MODEL_CASE, case_id, HEARTBEAT_FILENAME)


def touch_heartbeat(case_id: str) -> None:
    """刷新心跳（监控线程每轮调用一次，失败也不抛，心跳永远不是关键路径）。"""
    try:
        path = get_heartbeat_path(case_id)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "a", encoding="utf-8"):
            pass
    except OSError:
        pass


def remove_heartbeat(case_id: str) -> None:
    try:
        os.remove(get_heartbeat_path(case_id))
    except OSError:
        pass


def _iso_age_seconds(stamp: Any, now: float) -> float | None:
    if not isinstance(stamp, str) or not stamp.strip():
        return None
    try:
        moment = datetime.datetime.fromisoformat(stamp.strip())
    except ValueError:
        return None
    return now - moment.timestamp()


def case_idle_seconds(case_id: str) -> float | None:
    """这个 case 最近一次「有人在动它」距今多少秒。

    取三处证据里**最新**的那个（即年龄最小），宁可判「还活着」也不误杀：
    ``runtime.json`` 的 ``updated-at``、心跳文件 mtime、status 目录 mtime。
    全都拿不到时返回 None，调用方据此放弃（没有证据就不动）。
    """
    now = datetime.datetime.now().timestamp()
    ages: list[float] = []

    runtime = MCR.get_runtime_info(case_id) or {}
    runtime_age = _iso_age_seconds(runtime.get("updated-at"), now)
    if runtime_age is not None:
        ages.append(runtime_age)

    for path in (
        get_heartbeat_path(case_id),
        os.path.join(config.DIR_MODEL_CASE, case_id, "status"),
    ):
        try:
            ages.append(now - os.path.getmtime(path))
        except OSError:
            continue

    if not ages:
        return None
    return max(0.0, min(ages))


def stale_reason(
    case_id: str,
    *,
    lock_seconds: int | None = None,
    running_seconds: int | None = None,
) -> str | None:
    """返回 ``"lock"`` / ``"running"`` 表示这把锁已过期，None 表示别碰。"""
    mcr = MCR.open_case(case_id)
    if mcr is None:
        return None

    status = mcr.get_status()
    if status & TERMINAL_STATUS:
        return None  # 已经跑完/已失败，没有锁可救
    if not (status & ACTIVE_STATUS):
        return None  # 没有被认领（UNLOCK），本来就能重新入队

    idle = case_idle_seconds(case_id)
    if idle is None:
        return None  # 没有任何时间证据，保守不动

    if status & config.STATUS_RUNNING:
        limit = (
            running_seconds
            if running_seconds is not None
            else config.MODEL_CASE_STALE_RUNNING_SECONDS
        )
        return "running" if idle >= limit else None

    limit = (
        lock_seconds
        if lock_seconds is not None
        else config.MODEL_CASE_STALE_LOCK_SECONDS
    )
    return "lock" if idle >= limit else None


def reset_case_lock(case_id: str, reason: str, message: str) -> bool:
    """把过期锁重置为 UNLOCK（status/1），并在 runtime/events 里留下现场。"""
    mcr = MCR.open_case(case_id)
    if mcr is None:
        return False

    previous_status = mcr.get_status()
    if previous_status & TERMINAL_STATUS:
        return False

    idle = case_idle_seconds(case_id)
    meta = {
        "recovered-from-status": previous_status,
        "recovered-reason": reason,
        "idle-seconds": None if idle is None else round(idle, 1),
    }
    try:
        mcr.set_runtime(
            stage="recovered",
            progress=0,
            message=message,
            status="pending",
            meta=meta,
        )
        mcr.append_event("warning", message, stage="recovered", meta=meta)
    except OSError as exc:  # 目录被删/被占用时不阻断启动
        print(
            f"[case-recovery] failed to rewrite runtime case={case_id} error={exc}",
            flush=True,
        )
        return False

    mcr.update_status(config.STATUS_UNLOCK, "OK")
    remove_heartbeat(case_id)
    return True


def try_recover_stale_lock(
    mcr: MCR, *, lock_seconds: int | None = None
) -> bool:
    """运行期用：只接管「只有 LOCK、子进程根本没进包装器」的僵尸锁。

    ``LOCK|RUNNING`` 不在运行期接管——那可能是真的在算，交给启动自愈。
    """
    if mcr is None:
        return False

    reason = stale_reason(mcr.id, lock_seconds=lock_seconds)
    if reason != "lock":
        return False

    idle = case_idle_seconds(mcr.id)
    idle_text = "unknown" if idle is None else f"{idle:.0f}s"
    message = (
        f"Recovered stale lock before re-queue (idle {idle_text}, "
        f"no live monitor heartbeat)"
    )
    if not reset_case_lock(mcr.id, reason, message):
        return False

    print(
        f"[case-recovery] recovered stale lock case={mcr.id} idle={idle_text}",
        flush=True,
    )
    return True


def recover_stale_cases(
    *,
    lock_seconds: int | None = None,
    running_seconds: int | None = None,
    include_lock: bool = True,
    include_running: bool = True,
) -> dict[str, Any]:
    """启动自愈：扫一遍 ``case/``，把所有过期锁重置为 UNLOCK。

    只在服务启动、daemon 还没起来的时候调用——那一刻本进程不可能持有任何锁，
    所以任何「有过期嫌疑的锁」都是上一个进程留下的。
    ``include_lock=False`` 用于常驻巡检：只救 ``LOCK|RUNNING``（daemon 死了但服务还在），
    普通 ``LOCK`` 交给请求路径上的 ``try_recover_stale_lock`` 精确判定。
    """
    summary: dict[str, Any] = {
        "scanned": 0,
        "recovered": [],
        "failed": [],
    }
    root = config.DIR_MODEL_CASE
    if not os.path.isdir(root):
        return summary

    for name in sorted(os.listdir(root)):
        directory = os.path.join(root, name)
        if not os.path.isdir(directory):
            continue
        if not os.path.exists(os.path.join(directory, "identity.json")):
            continue  # 不是 case 目录（比如全局锁文件 lock）

        summary["scanned"] += 1
        reason = stale_reason(
            name, lock_seconds=lock_seconds, running_seconds=running_seconds
        )
        if reason is None:
            continue
        if reason == "lock" and not include_lock:
            continue
        if reason == "running" and not include_running:
            continue

        idle = case_idle_seconds(name)
        idle_text = "unknown" if idle is None else f"{idle:.0f}s"
        label = "lock" if reason == "lock" else "running lock"
        message = (
            f"Recovered stale {label} on service startup "
            f"(idle {idle_text}, no live monitor heartbeat)"
        )
        if reset_case_lock(name, reason, message):
            summary["recovered"].append(
                {
                    "case-id": name,
                    "reason": reason,
                    "idle-seconds": None if idle is None else round(idle, 1),
                }
            )
        else:
            summary["failed"].append(name)

    return summary


def format_recovery_summary(summary: dict[str, Any]) -> str:
    recovered = summary.get("recovered") or []
    if not recovered:
        return (
            f"[case-recovery] 启动自愈完成：扫描 {summary.get('scanned', 0)} 个 case，"
            "没有需要回收的过期锁"
        )

    by_reason: dict[str, int] = {}
    for item in recovered:
        by_reason[item["reason"]] = by_reason.get(item["reason"], 0) + 1
    detail = ", ".join(f"{key}={value}" for key, value in sorted(by_reason.items()))
    return (
        f"[case-recovery] 启动自愈完成：扫描 {summary.get('scanned', 0)} 个 case，"
        f"回收 {len(recovered)} 个过期锁（{detail}）"
    )


def is_retryable_error(mcr: MCR) -> bool:
    """这个 ERROR 是「环境/进程」造成的，还是「模型代码」造成的？

    case id = md5(url + json)，同一个断面重跑一定命中同一个 case。如果一把
    ERROR 也当成终态缓存直返，那么子进程被 Ctrl+C 杀掉一次，这个断面就
    **永久失败**了（除非手工清缓存）。所以这里区分两类 ERROR：

    * 基础设施类（子进程退出码异常/起不来/依赖因此失败）⇒ 允许重跑；
    * 模型代码类（包装器捕获到真实异常，meta 里有 traceback）⇒ 保持缓存直返，
      因为同样的输入必然得到同样的异常，重跑只是浪费算力。
    """
    if not (mcr.get_status() & config.STATUS_ERROR):
        return False

    runtime = MCR.get_runtime_info(mcr.id) or {}
    meta = runtime.get("meta")
    if isinstance(meta, dict):
        if meta.get("infra-error") or "exit-code" in meta:
            return True

    if str(runtime.get("stage") or "") == "dependency_error":
        return True

    return False


def reset_error_for_retry(mcr: MCR, message: str) -> bool:
    """把「基础设施类 ERROR」退回 UNLOCK，让本次请求重新排一次。"""
    if not is_retryable_error(mcr):
        return False

    meta = {"retry-from-status": mcr.get_status(), "retry-reason": message}
    try:
        mcr.set_runtime(
            stage="retrying",
            progress=0,
            message=message,
            status="pending",
            meta=meta,
        )
        mcr.append_event("warning", message, stage="retrying", meta=meta)
    except OSError as exc:
        print(
            f"[case-recovery] failed to reset error case={mcr.id} error={exc}",
            flush=True,
        )
        return False

    mcr.update_status(config.STATUS_UNLOCK, "OK")
    remove_heartbeat(mcr.id)
    print(f"[case-recovery] retry after infra failure case={mcr.id}", flush=True)
    return True
