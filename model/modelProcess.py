import os
import sys
import time
import queue
import config
import threading
import collections
import subprocess
import multiprocessing

from .modelCaseReference import ModelCaseReference as MCR
from .caseEntry import EXIT_CASE_UNAVAILABLE
from .modelRecovery import (
    case_idle_seconds,
    remove_heartbeat,
    touch_heartbeat,
    try_recover_stale_lock,
)
from util import hydro_node_index

# 子进程的 stdout/stderr 合并写进这个文件。崩溃的现场（ImportError / GDAL 报错 /
# Segmentation fault 之前的最后一行）只能从这里拿到——子进程如果在 import 阶段就死了，
# 它连 case 的包装器都没进，case 目录里不会留下任何异常信息。
CHILD_LOG_FILENAME = "child.log"
CHILD_LOG_TAIL_LINES = 15
CHILD_LOG_TAIL_CHARS = 1200
CHILD_LOG_MAX_BYTES = 2 * 1024 * 1024  # 只保留一代 .1 备份，避免无限膨胀

TERMINAL_STATUS = config.STATUS_COMPLETE | config.STATUS_ERROR


def mark_pending_stage(
    mcr: MCR | None,
    stage: str,
    progress: int,
    message: str,
    meta: dict | None = None,
) -> None:
    """把 case 落到「等槽位 / 等依赖」这类 pending 阶段，重复调用不重复写盘。

    daemon 每轮空转都会为同一条排队命令重走一次
    ``execute_`` → ``try_to_run_case``（槽位满）或 ``ready_for_core_case``
    （依赖没完），而 ``set_runtime`` 每次都要 读-改-写 runtime.json 再原子换名。
    实测这条忙等路径每 ~6ms 写一次、160ms 就把 24 条 timeline 上限写满：
    进度没有任何变化，盘却被刷穿。

    只有「阶段/状态/meta 都一致」时才跳过；依赖 id 变了、或从 running 退回
    pending，都会照常写一条。
    """
    if mcr is None:
        return
    current = MCR.get_runtime_info(mcr.id) or {}
    if (
        current.get("stage") == stage
        and current.get("status") == "pending"
        and current.get("meta") == meta
    ):
        return
    mcr.set_runtime(
        stage=stage,
        progress=progress,
        message=message,
        status="pending",
        meta=meta,
    )


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
        # case 在排队期间被删（或只剩空壳）：这里没有落笔的地方，只能把话说清楚。
        # 调用方轮询 /v0/mc/status 会拿到 not_found，不会永远 pending。
        print(
            f"[model-runner][error] cannot record failure case={case_id}: "
            "case directory or identity.json is gone (deleted while queued?)",
            flush=True,
        )
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
            if return_code == EXIT_CASE_UNAVAILABLE:
                # 子进程入口校验没过：入参 case 在排队期间被删/成了空壳。
                # 这是环境问题（可重跑），不是模型代码问题，所以照样标 infra-error。
                reason = (
                    "Model input case was unavailable when the subprocess started "
                    "(deleted or never initialized while queued)"
                )
            elif return_code != 0:
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


def _start_hydro_node_index_refresher() -> None:
    """起一个后台线程，定时刷新水动力节点字典（D2）。

    挂在这里是因为 daemon 是全系统唯一「活得比 HTTP 请求久、也比 case 子进程久」
    的常驻对象，也是唯一有资格替子进程查库的角色。刷新线程崩了不带走 daemon，
    刷新失败只打日志 —— daemon 不能因为一次数据库抖动就死。
    """
    if not config.MODEL_HYDRO_NODE_INDEX_ENABLED:
        print("[hydro-node-index] 开关关闭，case 将直接查库", flush=True)
        return

    def worker() -> None:
        while True:
            started = time.monotonic()
            try:
                hydro_node_index.refresh_now()
            except Exception as error:  # noqa: BLE001 - 双保险，refresh_now 内部已兜底
                print(f"[hydro-node-index] 刷新线程异常: {error}", flush=True)
            # 用已耗时抵扣间隔，避免「间隔 + 刷新耗时」逐轮漂移。
            elapsed = time.monotonic() - started
            time.sleep(
                max(1.0, config.MODEL_HYDRO_NODE_INDEX_REFRESH_SECONDS - elapsed)
            )

    threading.Thread(target=worker, name="hydro-node-index", daemon=True).start()


def daemon_process(process_queue: queue.Queue[any]):
    print("Daemon process is working", flush=True)
    # 字典预取线程：case 子进程读文件替代建连查库。
    _start_hydro_node_index_refresher()

    # task_queue 记录「已占用的执行槽位」。daemon 目前单线程，这把锁是零争用的；
    # 但槽位判断是典型的 check-then-act，将来派发改并发时这层互斥必须已经在。
    task_queue = set()
    task_queue_lock = threading.Lock()
    # deque 而非 list：原来 deferred_queue.pop(0) 是 O(n) 且非线程安全，
    # 命令积压时每轮空转都要把整个列表搬一遍。
    deferred_queue: collections.deque[list[str]] = collections.deque()
    last_scan_at = 0.0

    def update_task_queue(force: bool = False) -> None:
        """清理已结束的 case，把槽位还回来。

        单次 is_case_done 实测 0.155ms（open_case + 读 status 目录名），32 个就是 4.9ms。
        原来每条命令都全量扫一遍，是派发路径上一笔纯浪费，所以改成时间节流：
        距上次扫描不足 MODEL_DAEMON_SCAN_INTERVAL_SECONDS 时直接跳过。

        节流的副作用是 task_queue 会短暂残留已完成的 id、看起来「槽位满」，
        所以 reserve_slot 判满时用 force=True 精确复核一次，不会误拒任务。
        """
        nonlocal last_scan_at
        now = time.monotonic()
        if not force and (now - last_scan_at) < config.MODEL_DAEMON_SCAN_INTERVAL_SECONDS:
            return
        last_scan_at = now
        with task_queue_lock:
            # 先快照再遍历：并发下直接迭代 task_queue 会抛
            # RuntimeError: Set changed size during iteration
            stale = [id for id in list(task_queue) if MCR.is_case_done(id)]
            for id in stale:
                task_queue.discard(id)

    def reserve_slot(case_id: str) -> str:
        """原子地占一个执行槽位，返回 ok / duplicate / full。

        原写法把「还有没有名额」的判断和「把 id 加进 task_queue」隔开了两次写盘的距离。
        单线程下没人插队所以安全；多线程下 8 个 worker 会同时读到「还剩 1 个」而一起放行。
        这里把判断与占用收进同一把锁。

        顺带修了差一：上限是 MAX 个槽位，原来的 `len(task_queue) <= MAX`
        会放行第 MAX+1 个。
        """
        with task_queue_lock:
            if case_id in task_queue:
                return "duplicate"
            if len(task_queue) >= config.MAX_RUNNING_MODEL_CASE_NUM:
                return "full"
            task_queue.add(case_id)
            return "ok"

    def release_slot(case_id: str) -> None:
        with task_queue_lock:
            task_queue.discard(case_id)

    # 同一 case 的「重复提交被忽略」痕迹限流：重复提交可能很密（前端重试），
    # 每写一条 event 都是一次追加写盘。
    duplicate_notice_at: dict[str, float] = {}

    def notice_duplicate(case_id: str, mcr: MCR | None) -> None:
        now = time.monotonic()
        if now - duplicate_notice_at.get(case_id, 0.0) < 60.0:
            return
        duplicate_notice_at[case_id] = now
        idle = case_idle_seconds(case_id)
        idle_text = "unknown" if idle is None else f"{idle:.0f}s"
        print(
            f"[model-runner] duplicate command ignored case={case_id} "
            f"(already dispatched, idle={idle_text})",
            flush=True,
        )
        if mcr is None:
            return
        try:
            mcr.append_event(
                "warning",
                "Duplicate submission ignored: this case is already dispatched",
                stage="duplicate_ignored",
                meta={"idle-seconds": None if idle is None else round(idle, 1)},
            )
        except OSError:
            pass

    def try_to_run_case(command):
        id = command[-1]
        update_task_queue()
        mcr = MCR.open_case(id)

        slot = reserve_slot(id)
        if slot == "full":
            # 节流窗口内残留的已结束 case 会造成假「满」，精确复核一次再判。
            update_task_queue(force=True)
            slot = reserve_slot(id)

        if slot == "duplicate":
            # duplicate 有两个来源，必须先分开，不能一起当成「满」：
            # ① 槽位里那个 id 其实已经跑完了，只是节流窗口内还没被清掉；
            # ② 真的还有一个子进程在跑（同一参数重复提交，md5 相同的 case）。
            update_task_queue(force=True)
            slot = reserve_slot(id)

        if slot == "duplicate":
            # ②里还要再分一次：持有槽位的子进程可能早就死了，case 停在 LOCK、
            # 心跳也停了。这时槽位是僵尸占位，而旧代码在这个分支既不 force 复核
            # 也不重新入队 ⇒ 命令被静默丢弃，case 永远停在 waiting_slot + status=2。
            # try_recover_stale_lock 只接管「只有 LOCK、没有 RUNNING」且心跳过期的，
            # 真在算的一律不动。
            if try_recover_stale_lock(mcr):
                print(
                    f"[model-runner] reclaimed stale slot case={id}, re-dispatching",
                    flush=True,
                )
                release_slot(id)
                slot = reserve_slot(id)
            else:
                # 真的在跑：这次提交已经被满足了（调用方轮询能看到终态），命令要
                # 消费掉，不能再进 deferred_queue 空转。也绝不能写 waiting_slot——
                # 那会把正在跑的 case 的进度条打回 pending。
                notice_duplicate(id, mcr)
                return True

        if slot == "ok":
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
                release_slot(id)
            return True

        mark_pending_stage(
            mcr,
            "waiting_slot",
            10,
            "Waiting for an available execution slot",
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
                # 依赖没跑完时命令会一直留在 deferred_queue，每轮回灌都会走到这里。
                # 用 mark_pending_stage 去重，否则同一条命令每轮都重写一次 runtime.json。
                mark_pending_stage(
                    core_mcr,
                    "waiting_dependencies",
                    10,
                    "Waiting for dependent cases to complete",
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

    # Task loop
    while True:
        try:
            command = process_queue.get_nowait()
        except queue.Empty:
            while deferred_queue:
                process_queue.put(deferred_queue.popleft())
            # 阻塞等新命令。原写法是 get() 取到队首后立刻 put() 放回队尾，
            # [A, B, C] 会变成 [B, C, A] —— FIFO 顺序被打乱，先提交的可能后执行。
            # 这里直接取出来交给下一轮执行，顺序不再经过队列。
            command = process_queue.get()

        if not execute_(command):
            print("Daemon process stopped", flush=True)
            break


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
