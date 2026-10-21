import os
import sys
import util
import model
import types
import config
import marshal
import traceback
import functools
import py_compile
from typing import cast
from .modelProcess import Process_Queue
from .modelCaseReference import ModelCaseReference as MCR
from .modelRecovery import reset_error_for_retry, try_recover_stale_lock

######################################## AOP ########################################


def model_status_controller_sync(func):
    @functools.wraps(func)
    def wrapper(mcr: MCR, *args, **kwargs):
        # Return if mcr is unlocked (COMPLETE or ERROR)
        if mcr.find_status(config.STATUS_COMPLETE) or mcr.find_status(
            config.STATUS_ERROR
        ):
            return

        # Run
        try:
            mcr.set_runtime(
                stage="starting",
                progress=5,
                message="Model execution is starting",
                status="running",
            )
            mcr.append_event("info", "Model execution started", stage="starting")
            mcr.update_status(config.STATUS_LOCK | config.STATUS_RUNNING)
            mcr.set_runtime(
                stage="running",
                progress=30,
                message="Model code is running",
                status="running",
            )
            result = func(mcr, *args, **kwargs)
            mcr.set_runtime(
                stage="writing_response",
                progress=90,
                message="Writing model response",
                status="running",
            )
            mcr.make_response(result)
            mcr.update_status(config.STATUS_UNLOCK | config.STATUS_COMPLETE)
            mcr.set_runtime(
                stage="completed",
                progress=100,
                message="Model execution completed",
                status="completed",
            )
            mcr.append_event("info", "Model execution completed", stage="completed")
            util.update_size(
                config.DIR_STORAGE_LOG, util.get_folder_size_in_gb(mcr.directory)
            )
            return

        except Exception as e:
            mcr.mark_error(e)
            mcr.update_status(config.STATUS_UNLOCK | config.STATUS_ERROR, e, "w")
            util.update_size(
                config.DIR_STORAGE_LOG, util.get_folder_size_in_gb(mcr.directory)
            )
            traceback.print_exc()
            return

    return wrapper


######################################## Utils ########################################


def get_pyc_filename(script_name: str):
    python_version = f"{sys.version_info.major}{sys.version_info.minor}"

    pyc_filename = f"{script_name}.cpython-{python_version}.pyc"
    return pyc_filename


def compile_model_script_to_pyc(model_api: str, script_path: str):
    code_md5 = util.generate_md5(model_api)
    pyc_filename = get_pyc_filename(code_md5)
    compiled_path = os.path.join(config.DIR_RESOURCE_MODEL, code_md5)
    pyc_file_path = os.path.join(compiled_path, pyc_filename)

    script_modified_time = os.path.getmtime(script_path)

    if os.path.exists(compiled_path):
        try:
            with open(
                os.path.join(compiled_path, "time.txt"), "r", encoding="utf-8"
            ) as file:
                program_modified_time = float(file.readline())

            existing_pyc_files = [
                f for f in os.listdir(compiled_path) if f.endswith(".pyc")
            ]
            if existing_pyc_files:
                existing_version = (
                    existing_pyc_files[0].split(".")[-2].replace("cpython-", "")
                )
                current_version = f"{sys.version_info.major}{sys.version_info.minor}"

                if (
                    existing_version != current_version
                    or program_modified_time != script_modified_time
                ):
                    for old_pyc in existing_pyc_files:
                        os.remove(os.path.join(compiled_path, old_pyc))
                else:
                    return
        except FileNotFoundError:
            pass

    if not os.path.exists(compiled_path):
        os.makedirs(compiled_path)

    with open(os.path.join(compiled_path, "time.txt"), "w", encoding="utf-8") as file:
        file.write(str(script_modified_time))

    py_compile.compile(script_path, cfile=pyc_file_path)
    print(
        f"Model Program ({os.path.basename(script_path)}) Is Compiled to .pyc",
        flush=True,
    )


def load_code_from_pyc(pyc_path: str):
    with open(pyc_path, "rb") as f:
        f.read(16)  # Skip the .pyc header (timestamp, magic number, etc.)
        code_obj = marshal.load(f)
    return code_obj


######################################## Model Launcher ########################################

# TODO: Need to check whether mcrs is valid
# @model_status_controller_async
# def mcr_checker(mcrs: list[MCR]):
#     pass


def mcr_checker(mcrs: list[MCR]):
    core_mcr = mcrs[-1]
    other_mcrs = mcrs[:-1]

    # Check if core model case is being used or used (simplified)
    core_status = core_mcr.get_status()
    if (core_status & config.STATUS_COMPLETE) or (core_status & config.STATUS_ERROR):
        return False
    if (core_status & config.STATUS_LOCK) or (core_status & config.STATUS_RUNNING):
        # LOCK 没有持有者记录、也没有租约：认领它的子进程一死，锁永远不会被释放。
        # 以前这里直接 return False ⇒ 同一个参数重跑必然撞回那把僵尸锁（case id 是
        # md5(url+json)），连重启服务都没用。现在先问一句「还有活的监控进程在写心跳吗」，
        # 真过期就把它重置成 UNLOCK 重新入队。
        if not try_recover_stale_lock(core_mcr):
            return False
        core_status = core_mcr.get_status()

    # Check if any other model cases is error (simplified)
    error_case_ids = []
    for mcr in other_mcrs:
        mcr_status = mcr.get_status()
        if mcr_status & config.STATUS_ERROR:
            error_case_ids.append(mcr.id)

    if len(error_case_ids):
        # Simplified error log
        error_log = "-".join(error_case_ids)
        core_mcr.update_status(
            config.STATUS_ERROR | config.STATUS_UNLOCK, error_log, "w"
        )
        return False

    # Lock any model case if it has not run yet (simplified)
    for mcr in mcrs:
        mcr_status = mcr.get_status()
        if not (
            (mcr_status & config.STATUS_LOCK)
            or (mcr_status & config.STATUS_RUNNING)
            or (mcr_status & config.STATUS_COMPLETE)
            or (mcr_status & config.STATUS_ERROR)
        ):
            mcr.update_status(config.STATUS_LOCK)

    return True


class ModelLauncher:
    def __init__(self, api: str):
        self.api = api
        self.id = util.generate_md5(api)

        if api not in config.MODEL_REGISTRY:
            raise KeyError(
                f"API '{api}' not found in MODEL_REGISTRY. Available APIs: {list(config.MODEL_REGISTRY.keys())}"
            )

        self.script_path = os.path.join(
            config.DIR_TRIGGER_RESOURCE, config.MODEL_REGISTRY[api]
        )
        self.program_path = os.path.join(
            config.DIR_RESOURCE_MODEL, self.id, get_pyc_filename(self.id)
        )

        compile_model_script_to_pyc(self.api, self.script_path)

        code_obj = load_code_from_pyc(self.program_path)

        local_namespace = {"MCR": MCR, "model": model, "config": config}
        exec(code_obj, local_namespace, local_namespace)

        self.name = (
            local_namespace["NAME"] if "NAME" in local_namespace else "Unknown Name"
        )
        self.category = (
            local_namespace["CATEGORY"]
            if "CATEGORY" in local_namespace
            else "Unknown Category"
        )
        self.category_alias = (
            local_namespace["CATEGORY_ALIAS"]
            if "CATEGORY_ALIAS" in local_namespace
            else "Unknown Category Alias"
        )
        self.parse = (
            types.MethodType(local_namespace["PARSING"], self)
            if "PARSING" in local_namespace
            else lambda request_json, model_path, other_dependent_ids=None: []
        )
        self.response = (
            types.MethodType(local_namespace["RESPONSING"], self)
            if "RESPONSING" in local_namespace
            else lambda core_mcr,
            default_pre_mcrs,
            other_pre_mcrs: core_mcr.make_response({})
        )

    @staticmethod
    def preheat(api: str):
        id = util.generate_md5(api)
        script_path = os.path.join(
            config.DIR_TRIGGER_RESOURCE, config.MODEL_REGISTRY[api]
        )

        compile_model_script_to_pyc(api, script_path)

    def run(self, request_json: dict, other_dependent_ids: list[str] = []) -> MCR:
        print(
            f"[launcher] start api={self.api} model={self.name} request={request_json} deps={other_dependent_ids}",
            flush=True,
        )
        other_pre_mcrs: list[MCR] = cast(
            list[MCR],
            [mcr for id in other_dependent_ids if (mcr := MCR.open_case(id))],
        )
        try:
            default_pre_mcrs: list[MCR] = cast(
                list[MCR],
                self.parse(request_json, self.program_path, other_dependent_ids),
            )
        except Exception as exc:
            print(
                f"[launcher] parse failed api={self.api} model={self.name} error={exc} request={request_json}",
                flush=True,
            )
            raise
        if not default_pre_mcrs:
            raise RuntimeError(f"Model '{self.api}' did not create a core case")

        mcrs: list[MCR] = other_pre_mcrs + default_pre_mcrs
        core_mcr: MCR = mcrs[-1]
        print(
            f"[launcher] parsed api={self.api} core_case={core_mcr.id} all_cases={[m.id for m in mcrs]}",
            flush=True,
        )

        if core_mcr.find_status(config.STATUS_COMPLETE) or core_mcr.find_status(
            config.STATUS_ERROR
        ):
            # ERROR 也是"缓存命中直接返回"。但子进程被 Ctrl+C 杀掉、或依赖被它带崩，
            # 这类失败跟输入无关，属于环境问题——此时必须允许重跑，否则这个断面
            # （case id 由 md5(url+json) 决定）会一直失败下去，除非手工清缓存。
            if core_mcr.find_status(config.STATUS_ERROR) and reset_error_for_retry(
                core_mcr, "Retry after an infrastructure failure"
            ):
                print(
                    f"[launcher] retry infra-error api={self.api} core_case={core_mcr.id}",
                    flush=True,
                )
            else:
                print(
                    f"[launcher] skip api={self.api} core_case={core_mcr.id} status={core_mcr.get_status()}",
                    flush=True,
                )
                return core_mcr

        # Check whether core model case is valid for running
        if mcr_checker(mcrs):
            # Launch model
            command = [self.program_path] + [mcr.id for mcr in mcrs]
            Process_Queue().put(command)
            print(
                f"[launcher] queued api={self.api} core_case={core_mcr.id} command={command}",
                flush=True,
            )

            # Make default response
            self.response(default_pre_mcrs[-1], default_pre_mcrs[:-1], other_pre_mcrs)
            print(
                f"[launcher] response prepared api={self.api} core_case={core_mcr.id}",
                flush=True,
            )
        else:
            print(
                f"[launcher] checker blocked api={self.api} core_case={core_mcr.id} status={core_mcr.get_status()}",
                flush=True,
            )

        return core_mcr

    @staticmethod
    def fetch_model_from_API(api: str):
        return ModelLauncher(api)
