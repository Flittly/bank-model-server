"""解释器启动期的最小引导：只为 GDAL/PROJ 设置环境变量。

⚠ 这里**不要**写成 ``from util.gdal_env import configure_gdal_proj_env``。
那种写法会先执行 ``util/__init__.py``，而它 eagerly 导入 ``.geo``
（→ ``osgeo.gdal_array`` → numpy）与 ``.storage``（→ portalocker），
实测给**每一个**模型 case 子进程平白增加约 160 ms 启动成本，
而本文件真正需要的只是 ``os.environ`` 的几次赋值。

因此改为按文件路径直接加载 ``util/gdal_env.py``（它只依赖 stdlib），
绕开包初始化。行为与原来完全等价，只是不再顺带把整个 util 包拉起来。
"""

import importlib.util as _importlib_util
import pathlib as _pathlib
import sys as _sys

_BOOTSTRAP_MODULE = "_bank_model_gdal_env"


def _load_gdal_env():
    if _BOOTSTRAP_MODULE in _sys.modules:
        return

    path = _pathlib.Path(__file__).resolve().parent / "util" / "gdal_env.py"
    if path.is_file():
        try:
            spec = _importlib_util.spec_from_file_location(_BOOTSTRAP_MODULE, path)
            if spec is not None and spec.loader is not None:
                module = _importlib_util.module_from_spec(spec)
                _sys.modules[_BOOTSTRAP_MODULE] = module
                # gdal_env 在模块末尾自行调用 configure_gdal_proj_env()，
                # 所以 exec_module 即完成配置，无需再显式调用一次。
                spec.loader.exec_module(module)
                return
        except Exception:  # pragma: no cover - 回退到常规导入
            _sys.modules.pop(_BOOTSTRAP_MODULE, None)

    # 兜底：路径推断失败时退回原写法。慢，但行为一致，
    # 绝不静默跳过 —— 否则 GDAL 找不到 PROJ 数据会报成别的错。
    from util.gdal_env import configure_gdal_proj_env  # noqa: F401


_load_gdal_env()
