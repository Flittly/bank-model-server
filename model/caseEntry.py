"""模型子进程的入口校验：入参 case 不可用时就地失败。

为什么需要这一层
----------------
19 个模型脚本的 ``__main__`` 都是同一个套路：

.. code-block:: python

    v10 = sys.argv[10]
    risk_level_mcr = model.ModelCaseReference.open_case(v10)
    run_risk_level_mcr(risk_level_mcr)

``open_case`` 的契约是「拿不到就返回 None」（它靠 ``identity.json`` 判存在，
缓存被清理、目录成了空壳、排队期间被删，都会返回 None）。而 ``run_*_mcr``
被 ``model_status_controller_sync`` 包住，wrapper 的第一行是
``mcr.find_status(...)``，而且**在 try 之外**——于是子进程直接抛
``AttributeError: 'NoneType' object has no attribute 'find_status'``、
退出码 1，既没有 case 状态可写，父进程的 ``mark_case_failure`` 也因为
``open_case`` 返回 None 而记不了账。现场只剩一句 AttributeError，
看不出是哪个 argv 位置、哪一类缺失。

所以入口统一问一句：这些 case 现在真的可用吗？不可用就带着明细退出
``EXIT_CASE_UNAVAILABLE``。父进程的监控线程按这个退出码把原因写成
「输入 case 不可用」，而不是笼统的 "exited with code 1"。
"""

from __future__ import annotations

import os

import config

# 专用退出码：与 Python 自身的 1（未捕获异常）/ 2（用法错误）区分开，
# 父进程据此判定「这条命令的入参已经失效」，属于可重跑的基础设施类故障。
EXIT_CASE_UNAVAILABLE = 3


def _diagnose(case_id: str | None) -> str | None:
    """返回不可用的原因；可用时返回 None。"""
    if not case_id:
        return "empty case id"
    directory = os.path.join(config.DIR_MODEL_CASE, case_id)
    if not os.path.isdir(directory):
        return "case directory does not exist (deleted while the command was queued)"
    if not os.path.exists(os.path.join(directory, "identity.json")):
        # 残壳：目录还在，但 identity.json 已经没了（删除时被句柄占用跳过的）。
        try:
            contents = sorted(os.listdir(directory))
        except OSError:
            contents = ["<unreadable>"]
        return f"case directory exists without identity.json (shell, contents={contents})"
    return None


def require_cases(case_ids: list[str | None], model_label: str = "model") -> None:
    """校验一组 case id 全部可用，否则打印明细并以 ``EXIT_CASE_UNAVAILABLE`` 退出。

    只在子进程的 ``__main__`` 里调用，位置必须在**第一次 open_case 之前**。
    这里用 ``SystemExit``：它不继承 ``Exception``，不会被模型脚本里的
    ``except Exception`` 吞掉。
    """
    problems: list[tuple[int, str | None, str]] = []
    for position, case_id in enumerate(case_ids, start=1):
        reason = _diagnose(case_id)
        if reason is not None:
            problems.append((position, case_id, reason))

    if not problems:
        return

    print(
        f"[case-entry][fatal] {model_label}: {len(problems)} of {len(case_ids)} "
        f"input case(s) unavailable, refusing to run",
        flush=True,
    )
    for position, case_id, reason in problems:
        print(
            f"[case-entry][fatal]   #{position} case={case_id!r} reason={reason}",
            flush=True,
        )
    print(f"[case-entry][fatal] argv={list(case_ids)}", flush=True)
    raise SystemExit(EXIT_CASE_UNAVAILABLE)
