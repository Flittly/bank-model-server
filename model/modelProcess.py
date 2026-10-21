import os
import sys
import time
import queue
import config
import threading
import subprocess
import multiprocessing

from .modelCaseReference import ModelCaseReference as MCR
from .modelRecovery import (
    remove_heartbeat,
    touch_heartbeat,
)

# 子进程的 stdout/stderr 合并写进这个文件。崩溃的现场（ImportError / GDAL 报错 /
# Segmentation fault 之前的最后一行）只能从这里拿到——子进程如果在 import 阶段就死了，
# 它连 case 的包装器都没进，case 目录里不会留下任何异常信息。
CHILD_LOG_FILENAME = "child.log"
CHILD_LOG_TAIL_LINES = 15
CHILD_LOG_TAIL_CHARS = 1200
CHILD_LOG_MAX_BYTES = 2 * 1024 * 1024  # 只保留一代 .1 备份，避免无限膨胀

TERMINAL_STATUS = config.STATUS_COMPLETE | config.STATUS_ERROR


def get_child_log_path(case_id: str) -> str:
    return os.path.join(config.DIR_MODEL_CASE, case_id, CHILD_LOG_FILENAME)


def read_child_log_tail(case_id: str, lines: int = CHILD_LOG_TAIL_LINES) -> str:
    path = get_child_log_path(case_id)
    if not os.path.exists(path):
        return ""
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as file:
            content = file.readlines()
    except OSError:
        return ""
    tail = "".join(content[-lines:]).strip()
    if len(tail) > CHILD_LOG_TAIL_CHARS:
        tail = tail[-CHILD_LOG_TAIL_CHARS:]
    return tail


def prepare_child_log(case_id: str) -> str:
    path = get_child_log_path(case_id)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        if os.path.exists(path) and os.path.getsize(path) > CHILD_LOG_MAX_BYTES:
            os.replace(path, path + ".1")
    except OSError:
        pass
    return path


def mark_case_failure(case_id: str, reason: str, meta: dict | None = None) -> bool:
    """把还没走到终态的 case 标成 ERROR|UNLOCK，并写清原因。

    这是「子进程死了没人记账」的补丁：不这么做，case 会停在 LOCK，
    ``/v0/mc/status`` 永远回 pending，调用方只能干等到超时。
    """
    mcr = MCR.open_case(case_id)
    if mcr is None:
        return False
    if mcr.get_status() & TERMINAL_STATUS:
        return False  # 已经完成或已经报过错，别覆盖真实结果

    payload = dict(meta or {})
    payload.setdefault("child-log", get_child_log_path(case_id))
    # 打上「基础设施故障」的标记：重跑时据此决定是「重新入队」还是「直接返回缓存的错误」。
    # 子进程被杀/起不来是环境问题，重试有意义；模型代码自己抛异常是输入问题，重试没意义。
    payload["infra-error"] = True
    tail = read_child_log_tail(case_id)
    if tail:
        payload.setdefault("child-log-tail", tail)

    try:
        mcr.set_runtime(
            stage="error",
            progress=100,
            message=reason,
            status="error",
            meta=payload,
        )
        mcr.append_event("error", reason, stage="error", meta=payload)
    except OSError as exc:
        print(
            f"[model-runner][error] failed to write error state case={case_id} error={exc}",
            flush=True,
        )

    mcr.update_status(config.STATUS_ERROR | config.STATUS_UNLOCK, reason, "w")
    remove_heartbeat(case_id)
    return True


def monitor_case_subprocess(
    process: subprocess.Popen,
    case_id: str,
    child_log_path: str,
    log_file,
) -> None:
    """看护一个模型子进程直到它退出，并把「它是怎么死的」写回 case。

    ``model_status_controller_sync`` 只能记录**函数体内部**的异常。子进程如果在
    import、``__main__`` 取参、或进程被系统杀掉时退出，包装器根本没执行，
    case 就会停在 LOCK——这正是原来会永久卡住的那一类。所以必须由父进程兜底。
    """
    timeout_seconds = config.MODEL_CASE_CHILD_TIMEOUT_SECONDS
    started_at = time.time()
    reason: str | None = None
    return_code: int | None = None

    try:
        while True:
            return_code = process.poll()
            if return_code is not None:
                break
            # 心跳：证明「这个 case 还有一个活的监控线程」。恢复逻辑靠它区分
            # 「真的在跑」和「主人已经死掉的僵尸锁」。
            touch_heartbeat(case_id)
            if timeout_seconds > 0 and time.time() - started_at > timeout_seconds:
                print(
                    f"[model-runner][error] child timeout case={case_id} after {timeout_seconds}s, killing",
                    flush=True,
                )
                process.kill()
                return_code = process.wait()
                reason = (
                    f"Model subprocess exceeded the time limit "
                    f"({timeout_seconds}s) and was killed"
                )
                break
            time.sleep(config.MODEL_CASE_MONITOR_INTERVAL_SECONDS)

        if reason is None:
            if return_code != 0:
                reason = f"Model subprocess exited with code {return_code}"
            else:
                mcr = MCR.open_case(case_id)
                if mcr is not None and not (mcr.get_status() & TERMINAL_STATUS):
                    # 退出码 0 却没写状态：脚本没跑到包装器（比如 __main__ 取参失败后
                    # 静默 return），对调用方来说和崩溃没有区别，同样要记账。
                    reason = (
                        "Model subprocess exited normally without writing a case status"
                    )

        if reason is not None:
            print(f"[model-runner][error] case={case_id} {reason}", flush=True)
            tail = read_child_log_tail(case_id)
            if tail:
                print(f"[model-runner][error] child log tail:\n{tail}", flush=True)
            mark_case_failure(
                case_id,
                reason,
                {
                    "exit-code": return_code,
                    "child-log": child_log_path,
                    "child-log-tail": tail,
                },
            )
    except Exception as exc:  # 监控线程本身绝不能静默死掉
        print(
            f"[model-runner][error] monitor crashed case={case_id} error={exc}",
            flush=True,
        )
        mark_case_failure(case_id, f"Model subprocess monitor crashed: {exc}")
    finally:
        remove_heartbeat(case_id)
        try:
            log_file.close()
        except OSError:
            pass


def spawn_case_subprocess(command: list[str]) -> subprocess.Popen | None:
    """启动一个模型子进程，并把它交给监控线程看护。

    ``command`` 形如 ``[program_path, case_id_1, ..., case_id_n]``，
    最后一个是核心 case（子进程只执行核心 case，其余只是它的输入依赖）。
    """
    if len(command) < 2:
        raise ValueError(f"Invalid model command: {command}")

    core_case_id = command[-1]
    exec_command = [sys.executable, *command]
    child_log_path = prepare_child_log(core_case_id)

    log_file = open(child_log_path, "a", encoding="utf-8", errors="replace")
    log_file.write(
        f"\n===== [{time.strftime('%Y-%m-%d %H:%M:%S')}] spawn args={exec_command[1:]}\n"
    )
    log_file.flush()

    # Ctrl+C 在 Windows 控制台是发给**整个进程组**的：停 uvicorn 时会把正在跑的
    # 模型子进程一起带走，留下没人推进的 LOCK。放进独立进程组后父进程退出不再波及它，
    # 子进程可以自己跑完（跑完会正常写 COMPLETE），或者由监控线程独立看护。
    creationflags = 0
    if os.name == "nt":
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP

    try:
        process = subprocess.Popen(
            exec_command,
            stdin=subprocess.DEVNULL,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            creationflags=creationflags,
        )
    except OSError as exc:
        log_file.write(f"[spawn failed] {exc}\n")
        log_file.close()
        print(
            f"[model-runner][error] spawn failed case={core_case_id} error={exc}",
            flush=True,
        )
        mark_case_failure(
            core_case_id,
            f"Failed to start model subprocess: {exc}",
            {"child-log": child_log_path},
        )
        return None

    log_file.write(f"===== child pid={process.pid} =====\n")
    log_file.flush()
    touch_heartbeat(core_case_id)

    threading.Thread(
        target=monitor_case_subprocess,
        args=(process, core_case_id, child_log_path, log_file),
        name=f"case-monitor-{core_case_id[:8]}",
        daemon=True,
    ).start()
    return process


def safe_launcher(command: list[str]) -> subprocess.Popen | None:
    """带看护地启动模型子进程（``try_to_run_case`` 的唯一出口）。"""
    return spawn_case_subprocess(command)


def daemon_process(process_queue: queue.Queue[any]):
    print("Daemon process is working", flush=True)

    task_queue = set()
    deferred_queue = []

    def update_task_queue():
        # Remove completed or error model cases
        task_queue.intersection_update(
            {id for id in task_queue if not MCR.is_case_done(id)}
        )

    def try_to_run_case(command):
        id = command[-1]
        update_task_queue()
        mcr = MCR.open_case(id)

        if (
            len(task_queue) <= config.MAX_RUNNING_MODEL_CASE_NUM
            and id not in task_queue
        ):
            task_queue.add(id)
            if mcr is not None:
                mcr.set_runtime(
                    stage="launching",
                    progress=20,
                    message="Launching model subprocess",
                    status="running",
                )
                mcr.append_event(
                    "info", "Launching model subprocess", stage="launching"
                )

            # 关键：句柄不再丢弃。safe_launcher 会起一个监控线程盯着退出码，
            # 子进程非正常退出时把 case 标成 ERROR（否则它会永远停在 LOCK）。
            if safe_launcher(command) is None:
                task_queue.discard(id)
            return True
        else:
            if mcr is not None:
                mcr.set_runtime(
                    stage="waiting_slot",
                    progress=10,
                    message="Waiting for an available execution slot",
                    status="pending",
                )
            return False

    # Return 0: False, 1: True, 2: Error
    def ready_for_core_case(command: list[str]):
        is_ready = 1
        core_mcr_id = command[-1]
        other_mcr_ids = command[1:-1]

        # Run pre cases if they are not used
        for id in other_mcr_ids:
            mcr = MCR.open_case(id)
            if not mcr:
                is_ready = 0
                break
            current_status = mcr.get_status()
            if (current_status & config.STATUS_ERROR) == config.STATUS_ERROR:
                error_log = MCR.get_simplified_error_log(id)
                core_mcr = MCR.open_case(core_mcr_id)
                if core_mcr is not None:
                    core_mcr.set_runtime(
                        stage="dependency_error",
                        progress=100,
                        message="A dependent case failed",
                        status="error",
                        meta={"dependency-case-id": id, "dependency-error": error_log},
                    )
                    core_mcr.append_event(
                        "error",
                        f"Dependent case failed: {id}",
                        stage="dependency_error",
                        meta={"dependency-error": error_log},
                    )

                MCR.update_case_status(
                    core_mcr_id,
                    config.STATUS_ERROR | config.STATUS_UNLOCK,
                    error_log,
                    "w",
                )
                is_ready = 2
                break

            if (current_status & config.STATUS_COMPLETE) != config.STATUS_COMPLETE:
                core_mcr = MCR.open_case(core_mcr_id)
                if core_mcr is not None:
                    core_mcr.set_runtime(
                        stage="waiting_dependencies",
                        progress=10,
                        message="Waiting for dependent cases to complete",
                        status="pending",
                        meta={"dependency-case-id": id},
                    )
                is_ready = 0
                break

        return is_ready

    def execute_(command: list[str]):
        if command == "STOP":
            return False

        check_code = ready_for_core_case(command)
        if check_code == 1:
            if not try_to_run_case(command):
                deferred_queue.append(command)
        elif check_code == 0:
            deferred_queue.append(command)

        return True

    def waiting():
        command = process_queue.get()
        process_queue.put(command)

    # Task loop
    while True:
        try:
            command = process_queue.get_nowait()

            if not execute_(command):
                print("Daemon process stopped", flush=True)
                break

        except queue.Empty:
            while deferred_queue:
                process_queue.put(deferred_queue.pop(0))
            waiting()


####################################################################################################


class Process_Queue:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.initialize()
        return cls._instance

    def initialize(self):
        self.queue = multiprocessing.Queue()
        daemon = multiprocessing.Process(
            target=daemon_process, args=(self.queue,), daemon=True
        )
        daemon.start()

    def put(self, command):
        self.queue.put(command)
