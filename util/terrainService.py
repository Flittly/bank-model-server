"""DEM 三维地形的「状态机 + 后台切片 + 托管」。

切片算法在 :mod:`model.demTerrain`，这个模块只管它**什么时候跑、跑到哪一步了、谁可以取**：

- 状态记在 ``tiff_bounds`` 的 ``terrain_*`` 列上（一行一份 DEM）；
- 单线程 worker 按队列逐份切，不抢断面计算的 CPU；
- 上传登记后自动入队，首次使用时发现「没切 / 过期了」也会补切；
- 删 DEM 时同步删金字塔。

为什么状态放在 ``tiff_bounds`` 而不是单独建表
----------------------------------------------
归属过滤（``user_id``）与资源清单都走这张表：Java 网关只要拿到这一行，就同时知道
「这份 DEM 归谁」和「地形切到哪一步了」，不需要再查第二处、也不存在两处不同步。

为什么不是「请求来了现切」
------------------------
同一时刻只会有一份 DEM 挂在地图上，但一份金字塔要切几秒到几分钟；浏览行为
（拖一下地图 = 几十张瓦片）不能等它。所以必须是**提前切好 + 不可变文件**。

线程模型
--------
进程内一个 :class:`queue.SimpleQueue` + 一个 daemon 线程。进程重启后队列就空了，
所以启动时把库里 ``pending`` 的行重新入队、``building`` 的行打回 ``pending``
（上个进程死在半路上的）。多个服务实例共用同一份数据卷时，各自都会认领自己
看到的 pending 行 —— 最坏情况是重复切一份相同产物，不会写坏数据。
"""

from __future__ import annotations

import json
import os
import queue
import threading
import time
import traceback
from pathlib import Path
from typing import Any

import config

STATUS_PENDING = "pending"    # 已入队，等待切片
STATUS_BUILDING = "building"  # 正在切片
STATUS_READY = "ready"        # 金字塔可用
STATUS_FAILED = "failed"      # 上次切片失败（error 里有原因）

#: 上传登记后是否自动切片。关掉后地形改为「第一次用到时现切」。
BUILD_ON_UPLOAD = os.environ.get("DEM_TERRAIN_BUILD_ON_UPLOAD", "1").strip().lower() not in ("0", "false", "no")

WORKER_ENABLED = os.environ.get("DEM_TERRAIN_WORKER_ENABLED", "1").strip().lower() not in ("0", "false", "no")

_COLUMNS: tuple[tuple[str, str], ...] = (
    ("terrain_status", "VARCHAR(20)"),
    ("terrain_fp", "VARCHAR(16)"),
    ("terrain_minzoom", "INTEGER"),
    ("terrain_maxzoom", "INTEGER"),
    ("terrain_bounds", "JSONB"),
    ("terrain_tile_count", "INTEGER"),
    ("terrain_error", "TEXT"),
    ("terrain_generated_at", "TIMESTAMP"),
)

_queue: "queue.SimpleQueue[str]" = queue.SimpleQueue()
_lock = threading.Lock()
_queued: set[str] = set()
_columns_ready = False
_columns_checked = False


def terrain_tile_root() -> str:
    """金字塔根目录：``resource/tiles/dem/<指纹>/``。

    放在 ``resource/`` 下 —— 部署时它是宿主机挂载卷，容器重建后瓦片还在。
    """
    return os.path.join(config.DIR_RESOURCE, "tiles", "dem")


# ------------------------------------------------------------------ 表结构

def ensure_terrain_columns(*, force: bool = False) -> bool:
    """给 ``tiff_bounds`` 补上 ``terrain_*`` 列（幂等）。返回列是否可用。

    项目的库表变更不走 Flyway，靠手工执行 SQL；但少几列的后果是**静默**的
    （地形永远「生成中」）。所以这里自己 ``ADD COLUMN IF NOT EXISTS``，
    失败就打一条能看懂的日志并返回 False —— 调用方据此把地形功能整体降级，
    不影响上传、预览、计算这些既有链路。
    """
    global _columns_ready, _columns_checked
    if _columns_ready and not force:
        return True
    if _columns_checked and not force:
        return _columns_ready

    from util.db import get_db_cursor

    statements = [
        f"ALTER TABLE tiff_bounds ADD COLUMN IF NOT EXISTS {name} {ddl};"
        for name, ddl in _COLUMNS
    ]
    try:
        with get_db_cursor() as (_conn, cursor):
            for statement in statements:
                cursor.execute(statement)
        _columns_ready = True
    except Exception as exc:  # 列加不上不能挡住任何既有功能
        _columns_ready = False
        print(f"[dem-terrain] tiff_bounds 补列失败，三维地形不可用：{exc}", flush=True)
    _columns_checked = True
    return _columns_ready


# ------------------------------------------------------------------ 基本读写

def resolve_dem_path(tiff_key: str) -> str:
    """``tiff_key`` → 本地文件路径，与 :func:`controllers.preview_dem` 同一套解析规则。"""
    from util.rustfs import resolve_tiff_path

    parts = tiff_key.split("/")
    segment = parts[1] if len(parts) > 1 else ""
    timepoint = parts[4] if len(parts) > 4 else None
    return resolve_tiff_path(tiff_key, segment=segment, timepoint=timepoint)


def _public_status(row: dict[str, Any]) -> str:
    status = row.get("terrain_status")
    return status if status in (STATUS_PENDING, STATUS_BUILDING, STATUS_READY, STATUS_FAILED) else "none"


def _row_to_info(tiff_key: str, row: dict[str, Any] | None) -> dict[str, Any]:
    """把 ``tiff_bounds`` 的一行转成对外状态。非 ready 时不暴露指纹与层级。"""
    if row is None:
        return {"tiff_key": tiff_key, "status": "missing"}

    status = _public_status(row)
    ready = status == STATUS_READY
    return {
        "tiff_key": tiff_key,
        "status": status,
        "fingerprint": row.get("terrain_fp") if ready else None,
        "minzoom": row.get("terrain_minzoom") if ready else None,
        "maxzoom": row.get("terrain_maxzoom") if ready else None,
        "bounds": row.get("terrain_bounds") if ready else None,
        "tile_count": row.get("terrain_tile_count") if ready else None,
        "error": row.get("terrain_error") if status == STATUS_FAILED else None,
        "generated_at": row.get("terrain_generated_at").isoformat(sep="T")
        if ready and row.get("terrain_generated_at") else None,
    }


def _load_row(tiff_key: str) -> dict[str, Any] | None:
    from util.db import get_db_cursor

    with get_db_cursor() as (_conn, cursor):
        cursor.execute("SELECT * FROM tiff_bounds WHERE tiff_key = %s LIMIT 1", (tiff_key,))
        row = cursor.fetchone()
    return None if row is None else dict(zip(_select_columns(cursor), row))


#: 行缓存的存活时间。取 30 s：足够覆盖「一次缩放把几十张瓦片全拉一遍」的突发，
#: 又短到重建/删除后不会继续发已失效的指纹（况且指纹对不上时下游本来也会 404）。
_ROW_CACHE_SECONDS = 30.0
_row_cache: dict[str, tuple[float, dict[str, Any] | None]] = {}


def _load_row_cached(tiff_key: str) -> dict[str, Any] | None:
    """``_load_row`` 的短期缓存版本。

    ⚠ 瓦片接口**每张图**都要校验一次「指纹是否是当前值、层级是否在范围内」。实测这一步
    直接查库要 **90–100 ms**（每次新建 PG 连接，本项目没做池化），而一张图本身只有几 KB
    ⇒ 一次缩放要拉几十张瓦片时，光校验就是几秒，前端看起来像「卡住」。
    缓存 30 s 后同样的校验降到微秒级；写库处统一失效（见 :func:`_update`）。
    """
    now = time.time()
    hit = _row_cache.get(tiff_key)
    if hit is not None and now - hit[0] < _ROW_CACHE_SECONDS:
        return hit[1]
    row = _load_row(tiff_key)
    _row_cache[tiff_key] = (now, row)
    return row


def _select_columns(cursor) -> list[str]:
    return [d[0] for d in cursor.description]


def _update(tiff_key: str, fields: dict[str, Any], *, where_status: str | None = None) -> str | None:
    """按 tiff_key 更新 ``terrain_*`` 列。

    Args:
        where_status: 给了就只在该行当前处于这个状态时才改（乐观锁），用于「认领」。

    Returns:
        被更新那行的 ``terrain_status``；没有行被更新则 None。
    """
    if not fields:
        return None
    from util.db import get_db_cursor

    assignments = ", ".join(f"{name} = %s" for name in fields)
    values = list(fields.values())
    sql = f"UPDATE tiff_bounds SET {assignments}"
    if where_status is not None:
        sql += " WHERE tiff_key = %s AND terrain_status = %s"
        values += [tiff_key, where_status]
    else:
        sql += " WHERE tiff_key = %s"
        values.append(tiff_key)
    sql += " RETURNING terrain_status"

    with get_db_cursor() as (_conn, cursor):
        cursor.execute(sql, values)
        row = cursor.fetchone()
    # 写成功就失效缓存：否则重建完成后 30 s 内还在发旧指纹的路径（那些文件已经不在）
    _row_cache.pop(tiff_key, None)
    return row[0] if row else None


def _pyramid_is_intact(fingerprint: str | None) -> bool:
    if not fingerprint:
        return False
    return (Path(terrain_tile_root()) / fingerprint / "metadata.json").is_file()


# ------------------------------------------------------------------ 入队 / 状态

def _enqueue(tiff_key: str) -> None:
    with _lock:
        if tiff_key in _queued:
            return
        _queued.add(tiff_key)
    _queue.put(tiff_key)


def enqueue_build(tiff_key: str) -> bool:
    """把一份 DEM 标记为待切并入队。

    Returns:
        是否真的入队了（已是 ready 且金字塔完好时不动，返回 False）。
    """
    if not ensure_terrain_columns():
        return False
    row = _load_row(tiff_key)
    if row is None:
        return False
    status = _public_status(row)
    if status in (STATUS_READY, STATUS_BUILDING) and _pyramid_is_intact(row.get("terrain_fp")):
        return False
    # 已在队列里等着的也不重复改状态（否则前端会看到状态在 pending 之间来回跳）
    if status == STATUS_PENDING:
        _enqueue(tiff_key)
        return True
    _update(tiff_key, {"terrain_status": STATUS_PENDING, "terrain_error": None})
    _enqueue(tiff_key)
    return True


def request_build(tiff_key: str) -> dict[str, Any]:
    """前端点开三维地形时调用：没切过 / 失败过 / 过期了就开始切，否则什么都不做。"""
    if not tiff_key:
        raise ValueError("tiff_key 不能为空")
    if not ensure_terrain_columns():
        raise RuntimeError("三维地形未就绪：tiff_bounds 缺少 terrain_* 列")
    tiff_path = resolve_dem_path(tiff_key)  # 文件不在就让它抛 FileNotFoundError
    from model.demTerrain import source_fingerprint

    current = source_fingerprint(tiff_path)
    row = _load_row(tiff_key)
    if row is not None and _public_status(row) == STATUS_READY \
            and row.get("terrain_fp") == current and _pyramid_is_intact(current):
        return _row_to_info(tiff_key, row)  # 已经是最新的：不重复切

    if not WORKER_ENABLED:
        raise RuntimeError("DEM_TERRAIN_WORKER_ENABLED 已关闭，后台切片不可用")

    # 过期的 ready 瓦片先留着可读（旧指纹目录还在），等新金字塔切好再整体切换
    fields: dict[str, Any] = {"terrain_status": STATUS_PENDING, "terrain_error": None}
    if row is not None and _public_status(row) == STATUS_PENDING:
        _enqueue(tiff_key)
        return _row_to_info(tiff_key, _load_row(tiff_key))
    _update(tiff_key, fields)
    _enqueue(tiff_key)
    return _row_to_info(tiff_key, _load_row(tiff_key))


def get_status(tiff_key: str) -> dict[str, Any]:
    """当前状态。顺手做一次「过期检查」：源文件换了 / 金字塔目录没了就重新入队。"""
    if not tiff_key:
        raise ValueError("tiff_key 不能为空")
    if not ensure_terrain_columns():
        return {"tiff_key": tiff_key, "status": "unavailable", "error": "tiff_bounds 缺少 terrain_* 列"}

    row = _load_row(tiff_key)
    if row is None:
        return _row_to_info(tiff_key, None)
    if _public_status(row) != STATUS_READY:
        return _row_to_info(tiff_key, row)

    from model.demTerrain import source_fingerprint

    try:
        tiff_path = resolve_dem_path(tiff_key)
        current = source_fingerprint(tiff_path)
    except FileNotFoundError:
        return _row_to_info(tiff_key, row)  # 文件暂时找不到：原样报 ready，等调用方自己处理

    if row.get("terrain_fp") != current or not _pyramid_is_intact(current):
        # 源文件被替换，或者金字塔目录被手工删了 ⇒ 重新切；这期间旧瓦片照常可读
        if WORKER_ENABLED:
            _update(tiff_key, {"terrain_status": STATUS_PENDING, "terrain_error": None})
            _enqueue(tiff_key)
            return _row_to_info(tiff_key, _load_row(tiff_key))
    return _row_to_info(tiff_key, row)


def tile_file(tiff_key: str, fingerprint: str, zoom: int, x: int, y: int) -> Path | None:
    """一张瓦片的本地路径；拿不到（未就绪 / 指纹不符 / 越界）一律返回 None。

    指纹必须与库里的当前值一致：URL 里的指纹是浏览器长缓存的依据，
    对不上的瓦片宁可 404 也不能发出去。
    """
    from model.demTerrain import is_valid_fingerprint, tile_path

    if not is_valid_fingerprint(fingerprint):
        return None
    try:
        zoom, x, y = int(zoom), int(x), int(y)
    except (TypeError, ValueError):
        return None
    if zoom < 0 or zoom > 24 or x < 0 or y < 0 or x >= 2**zoom or y >= 2**zoom:
        return None
    if not ensure_terrain_columns():
        return None
    row = _load_row_cached(tiff_key)
    if row is None or _public_status(row) != STATUS_READY or row.get("terrain_fp") != fingerprint:
        return None
    if zoom < int(row.get("terrain_minzoom") or 0) or zoom > int(row.get("terrain_maxzoom") or 0):
        return None
    path = tile_path(terrain_tile_root(), fingerprint, zoom, x, y)
    return path if path.is_file() else None


def on_dem_registered(tiff_key: str) -> None:
    """上传登记成功后的回调。**任何异常都不许往上抛** —— 地形是锦上添花，上传不是。"""
    if not BUILD_ON_UPLOAD:
        return
    try:
        enqueue_build(tiff_key)
    except Exception as exc:
        print(f"[dem-terrain] 上传后入队失败（已忽略）：{tiff_key}: {exc}", flush=True)


def delete_terrain(tiff_key: str, fingerprint: str | None = None) -> bool:
    """删 DEM 时把金字塔一起删掉。返回是否真的删了目录。

    指纹优先取调用方传来的（删 DEM 的流程里行还在时读到的）；没传就查库。
    """
    from model.demTerrain import is_valid_fingerprint, safe_rmtree

    if fingerprint is None:
        try:
            row = _load_row(tiff_key)
            fingerprint = (row or {}).get("terrain_fp")
        except Exception:
            return False
    if not is_valid_fingerprint(fingerprint):
        return False

    target = Path(terrain_tile_root()) / fingerprint
    if not target.is_dir():
        return False
    # 一份金字塔就是几百个文件，删它同样会撞上批量删除守卫 ⇒ 必须走 safe_rmtree，
    # 否则 SystemExit 会从这个 HTTP 请求线程里穿出去。
    return safe_rmtree(target, label=f"金字塔 {fingerprint} ")


# ------------------------------------------------------------------ worker

def _claim(tiff_key: str) -> bool:
    """把一行从 pending 改成 building；改不动就说明别人在切（或已经被改过状态）。"""
    return _update(tiff_key, {"terrain_status": STATUS_BUILDING},
                   where_status=STATUS_PENDING) == STATUS_BUILDING


def build_one(tiff_key: str) -> dict[str, Any]:
    """同步切一份 DEM（worker 线程体；单测也可以直接调）。"""
    from model.demTerrain import build_terrain_pyramid

    tiff_path = resolve_dem_path(tiff_key)
    meta = build_terrain_pyramid(tiff_path, tiff_key, out_root=terrain_tile_root())
    bounds = meta.get("bounds")
    _update(tiff_key, {
        "terrain_status": STATUS_READY,
        "terrain_fp": meta["fingerprint"],
        "terrain_minzoom": meta["minzoom"],
        "terrain_maxzoom": meta["maxzoom"],
        "terrain_bounds": json.dumps(bounds) if bounds is not None else None,
        "terrain_tile_count": meta.get("tile_count"),
        "terrain_error": None,
        "terrain_generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    })
    _drop_superseded(tiff_key, meta["fingerprint"])
    return meta


def _drop_superseded(tiff_key: str, keep: str) -> None:
    """新金字塔切好后，删掉同一份 DEM 的旧指纹目录（源文件被替换过才会存在）。

    删旧目录会一次撞上几百个文件，某些环境会因此抛 ``SystemExit`` —— 走
    :func:`model.demTerrain.safe_rmtree` 保证删不掉也不反噬（旧目录留着无害，
    :func:`sweep_stale_tmp` 与下一次切片都会兜底）。
    """
    from model.demTerrain import safe_rmtree

    root = Path(terrain_tile_root())
    if not root.is_dir():
        return
    for child in root.iterdir():
        if child.name == keep or not child.is_dir():
            continue
        meta_path = child / "metadata.json"
        try:
            if meta_path.is_file() and json.loads(meta_path.read_text(encoding="utf-8")).get("tiff_key") == tiff_key:
                safe_rmtree(child, label=f"旧金字塔 {child.name} ")
        except (OSError, ValueError):
            continue


def _worker_loop() -> None:
    while True:
        tiff_key = _queue.get()
        try:
            if not _claim(tiff_key):
                continue
            started = time.time()
            print(f"[dem-terrain] 开始切片：{tiff_key}", flush=True)
            try:
                meta = build_one(tiff_key)
                print(f"[dem-terrain] 切片完成：{tiff_key} 瓦片 {meta.get('tile_count')} 张，"
                      f"耗时 {time.time() - started:.1f}s", flush=True)
            except Exception as exc:
                detail = f"{type(exc).__name__}: {exc}"
                traceback.print_exc()
                try:
                    _update(tiff_key, {"terrain_status": STATUS_FAILED,
                                       "terrain_error": detail[:2000]})
                except Exception as inner:  # 连失败都记不下去，只能打日志
                    print(f"[dem-terrain] 写失败状态时出错：{inner}", flush=True)
                print(f"[dem-terrain] 切片失败：{tiff_key} {detail}", flush=True)
        except BaseException as exc:  # worker 绝不能被单条消息打死（kafka_worker 踩过这个坑）
            # 连 SystemExit 也接住：线程一死就再没人消费队列，任务全部停在 pending，
            # 而界面上只会显示「切片中…」永远不动（实测踩过：清理临时目录触发环境守卫）。
            if isinstance(exc, KeyboardInterrupt):
                raise
            print(f"[dem-terrain] worker 异常（已忽略，继续下一条）：{exc}", flush=True)
            traceback.print_exc()
        finally:
            with _lock:
                _queued.discard(tiff_key)


def start_worker() -> threading.Thread | None:
    """启动后台切片线程（进程启动时调一次）。未启用或表结构补不上时返回 None。"""
    if not WORKER_ENABLED:
        print("[dem-terrain] 后台切片未启用（DEM_TERRAIN_WORKER_ENABLED=0）", flush=True)
        return None
    if not ensure_terrain_columns(force=True):
        return None

    from model.demTerrain import sweep_stale_tmp

    try:
        swept = sweep_stale_tmp()
        if swept:
            print(f"[dem-terrain] 清理残留的切片临时目录 {swept} 个", flush=True)
    except Exception as exc:
        print(f"[dem-terrain] 清理临时目录失败（已忽略）：{exc}", flush=True)

    try:
        from util.db import get_db_cursor

        with get_db_cursor() as (_conn, cursor):
            # 上个进程死在半路上的：building → pending；再加上本来就排着队的
            cursor.execute(
                "UPDATE tiff_bounds SET terrain_status = %s WHERE terrain_status = %s "
                "RETURNING tiff_key",
                (STATUS_PENDING, STATUS_BUILDING),
            )
            stale = [row[0] for row in cursor.fetchall()]
            cursor.execute("SELECT tiff_key FROM tiff_bounds WHERE terrain_status = %s", (STATUS_PENDING,))
            pending = [row[0] for row in cursor.fetchall()]
    except Exception as exc:
        print(f"[dem-terrain] 启动时恢复待切队列失败：{exc}", flush=True)
        return None

    for tiff_key in dict.fromkeys(stale + pending):
        _enqueue(tiff_key)

    thread = threading.Thread(target=_worker_loop, name="dem-terrain-builder", daemon=True)
    thread.start()
    print(f"[dem-terrain] 后台切片已启动，队列 {len(set(stale + pending))} 份", flush=True)
    return thread
