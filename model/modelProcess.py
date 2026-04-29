import sys
import time
import copy
import queue
import config
import threading
import subprocess
import multiprocessing
from .modelCaseReference import ModelCaseReference as MCR


def monitor_subprocess(args: list[str]):
    ids = copy.deepcopy(args)[1:]
    args.insert(0, sys.executable)

    try:
        process = subprocess.Popen(args)

        while True:
            return_code = process.poll()

            if return_code is None:
                pass
            else:
                break
            time.sleep(3)

    except OSError as e:
        print(f"Error Happened When Model Case Is Running: {e}", flush=True)

    finally:
        if process.poll() is None:
            process.kill()
            for id in ids:
                mcr = MCR.open_case(id)
                if mcr.find_status(config.STATUS_LOCK):
                    mcr.update_status(
                        config.STATUS_ERROR | config.STATUS_UNLOCK,
                        "Unexpected Error Happend!",
                    )


def safe_launcher(command: list[str]):
    threading.Thread(target=monitor_subprocess, args=(command,)).start()


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
            command.insert(0, sys.executable)

            subprocess.Popen(command)
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
