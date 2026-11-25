"""水动力节点映射的进程外缓存（D2，见 docs/model-db-p0-design.md）。

`db_ops.get_available_hydrodynamic_nodes` 的答案是「某个工况下有哪些 water_qs 档」，
全库只有 5 组（484,044 行数据的 DISTINCT 结果）。但算这个答案要扫 8 万条索引条目，
而且每个 case 子进程还要为它单独新建一条 PG 连接 —— Windows 上约 60ms，8 路并发 167ms。

case 进程只活 ~400ms，进程内的 lru_cache 和连接池都活不过它（下一个 case 是全新进程），
所以缓存只能放在进程外：常驻 daemon 定时查一次写成 JSON，子进程读文件（0.37ms）。

## 设计前提

本模块**绝不允许让调用失败**。任何异常都当作「缓存不可用」返回，调用方回落原 SQL。
最坏情况必须与没有这个模块时逐字相同 —— 这是 fail-soft，不是 best-effort。

## 三条不变量

与 db_ops 的 `sorted((int(row["water_qs"]) for row in rows))` 严格一致，缺一即错：

1. **类型必须是 int**。若返回字符串，上层 `i_water_qs <= nodes[0]` 会抛 TypeError。
2. **必须升序**。上层把 `nodes[0]` 当最小值、`nodes[-1]` 当最大值。顺序错了不会报错，
   只会把 water_qs 映射到错误的档 —— **静默算错**，这是最危险的失效方式。
3. **必须去重**。重复值会破坏中点分割（`middles` 与 index 的对应关系）。

## 数据假设：append-only

唯一写入者是 util/import_hydrodynamic.py 与 util/import_hydrodynamic_1998.py，
两者都是 `ON CONFLICT (point_id) DO NOTHING`，从不 UPDATE / DELETE。
所以已存在的分组不会消失、water_qs 值不会变，TTL 只用于防「文件腐坏 / daemon 停摆」，
不用于防「数据旧」。若将来出现改删路径，必须缩短 TTL 或在写入处调用 refresh_now()。
"""

import json
import os
import tempfile
import time
from typing import Dict, List, Optional, Tuple

import config

# 文件格式版本。结构变化时 +1，旧文件会被判为不可用（回落 SQL），不会被误读。
_VERSION = 1

# Windows 上 daemon 正在 os.replace 时，读侧会拿到 PermissionError（实测三档写频率
# 都出现过），而读到半个 JSON 的情况实测为 0（os.replace 的目录项替换是原子的）。
# 3 次 × 1ms 后实测 0 次失败；仍拿不到就判不可用、回落 SQL。
_READ_RETRIES = 3
_READ_RETRY_SLEEP = 0.001

# 写侧重试比读侧宽：Windows 不允许 replace 一个正被读句柄打开的文件
# （不共享 FILE_SHARE_DELETE），只能等读者读完。生产节奏下读者只持有 0.06ms，
# 5 × 10ms 足够跨过；压测里 4 线程不间断读才会失败，那属于人为极端。
_WRITE_RETRIES = 5
_WRITE_RETRY_SLEEP = 0.01

_REFRESH_SQL = """
    SELECT dataset_domain, set_name, tidal_level, water_qs
    FROM hydrodynamic_points
    WHERE temp = FALSE
    GROUP BY dataset_domain, set_name, tidal_level, water_qs
    ORDER BY dataset_domain, set_name, tidal_level, water_qs
"""


def lookup(
    dataset_domain: str, set_name: str, tidal_level: str
) -> Tuple[bool, Optional[List[int]]]:
    """读进程外字典，返回 (是否命中, 节点列表)。

    (True, [...])  字典里有这个 key，值是升序去重的 int 列表。**可能是空列表**：
                   该分组确实没有数据，直接返回它，不要回落 —— 回落反而会建连。
    (False, None)  字典不可用：开关关闭 / 文件缺失 / 过期 / 损坏 / 版本不符 /
                   **没有这个 key**。调用方必须回落原 SQL。

    最后一档要强调：「字典没覆盖」≠「库里没数据」。若对未覆盖的 key 返回空列表，
    上层会走 DEFAULT_DOMAIN 回落并把 year 改写成 "default"，等于拿另一个工况的
    数据去算 —— 结果错得很像对的。
    """
    if not config.MODEL_HYDRO_NODE_INDEX_ENABLED:
        return False, None

    index = _load()
    if index is None:
        return False, None

    nodes = index.get((str(dataset_domain), str(set_name), str(tidal_level)))
    if nodes is None:
        return False, None
    # 拷一份：调用方可能就地排序/追加，别污染下一次命中。
    return True, list(nodes)


def refresh_now() -> bool:
    """查库并把全量字典原子写盘。成功返回 True。

    供 daemon 的刷新线程与导入脚本收尾调用。**任何异常都不外抛**：
    daemon 不能因为一次数据库抖动而死；导入脚本也不该因为缓存刷新失败而失败
    （数据已经写进库了，缓存最坏等一个刷新间隔的兜底）。

    实现上是「每次刷新新建一条连接」而不是「持一条长连接」。60s 一次、建连约 60ms，
    占空比 0.1%，但省掉了连接保活、断线重连、事务状态残留三类状态机。
    """
    if not config.MODEL_HYDRO_NODE_INDEX_ENABLED:
        return False

    try:
        rows = _query_rows()
    except Exception as error:  # noqa: BLE001 - 缓存刷新不允许把异常扇出去
        print(f"[hydro-node-index] 查询失败，保留旧文件: {error}", flush=True)
        return False

    if not rows:
        # 空结果通常意味着「表被清空 / 连错了库」。写下去会让所有 key 都命中空列表，
        # 保留旧文件更安全 —— 最坏 TTL 到期后回落 SQL，不会静默返回空。
        print("[hydro-node-index] 查询结果为空，保留旧文件", flush=True)
        return False

    return _write_rows(rows)


# ---------------------------------------------------------------------------
# 内部实现
# ---------------------------------------------------------------------------


def _load() -> Optional[Dict[Tuple[str, str, str], List[int]]]:
    payload = _read_payload()
    if payload is None:
        return None
    return _parse(payload)


def _read_payload():
    for attempt in range(_READ_RETRIES):
        try:
            with open(config.DIR_HYDRO_NODE_INDEX, "r", encoding="utf-8") as file:
                return json.load(file)
        except FileNotFoundError:
            # 还没写过（daemon 未启动或首启竞态）。不存在就没人写，重试无意义。
            return None
        except OSError:
            if attempt == _READ_RETRIES - 1:
                return None
            time.sleep(_READ_RETRY_SLEEP)
        except (json.JSONDecodeError, ValueError, TypeError):
            # 语法/编码错：重试还是错，直接判不可用。
            return None
    return None


def _parse(payload) -> Optional[Dict[Tuple[str, str, str], List[int]]]:
    if not isinstance(payload, dict):
        return None
    if payload.get("version") != _VERSION:
        return None

    refreshed_at = payload.get("refreshed_at")
    # bool 是 int 的子类，True 会被当成 1 而“永远过期”，显式排除。
    if isinstance(refreshed_at, bool) or not isinstance(refreshed_at, (int, float)):
        return None
    if time.time() - float(refreshed_at) > config.MODEL_HYDRO_NODE_INDEX_TTL_SECONDS:
        return None

    rows = payload.get("rows")
    if not isinstance(rows, list):
        return None

    index: Dict[Tuple[str, str, str], List[int]] = {}
    for row in rows:
        # 任何一行结构不对 ⇒ 整份文件作废（fail-closed 回 SQL）。
        # 不允许「用了一半」：半份字典比没有字典更难排查。
        if not isinstance(row, (list, tuple)) or len(row) != 4:
            return None
        domain, set_name, tidal_level, nodes = row
        if not isinstance(nodes, (list, tuple)):
            return None
        try:
            # 规范化到 sorted(set(int))，三条不变量由此保证，
            # 与 db_ops 原实现逐字等价。非数字（含空串）直接判整份不可用。
            normalized = sorted({int(value) for value in nodes})
        except (TypeError, ValueError):
            return None
        index[(str(domain), str(set_name), str(tidal_level))] = normalized

    return index


def _query_rows() -> List[list]:
    # 延迟导入：db 依赖 config，本模块也可能在 config 初始化期被 import。
    from util import db

    grouped: Dict[Tuple[str, str, str], set] = {}
    with db.get_db_cursor(dict_cursor=True) as (conn, cursor):
        cursor.execute(_REFRESH_SQL)
        for row in cursor.fetchall():
            key = (row["dataset_domain"], row["set_name"], row["tidal_level"])
            grouped.setdefault(key, set()).add(int(row["water_qs"]))

    return [
        [domain, set_name, tidal_level, sorted(nodes)]
        for (domain, set_name, tidal_level), nodes in sorted(grouped.items())
    ]


def _write_rows(rows: List[list]) -> bool:
    payload = {"version": _VERSION, "refreshed_at": time.time(), "rows": rows}
    path = config.DIR_HYDRO_NODE_INDEX
    directory = os.path.dirname(path)

    try:
        os.makedirs(directory, exist_ok=True)
    except OSError as error:
        print(f"[hydro-node-index] 建目录失败: {error}", flush=True)
        return False

    for attempt in range(_WRITE_RETRIES):
        temporary = None
        try:
            # 临时文件名必须唯一：daemon 的刷新线程与导入脚本的 refresh_now()
            # 可能同时写（实测两个写者共用固定 .tmp 会互相 PermissionError）。
            descriptor, temporary = tempfile.mkstemp(
                dir=directory, prefix=".hydro_node_index-", suffix=".tmp"
            )
            with os.fdopen(descriptor, "w", encoding="utf-8") as file:
                json.dump(payload, file, ensure_ascii=False, separators=(",", ":"))
            # 原子替换：读者要么看到旧文件、要么看到完整新文件，不会读到半个 JSON。
            os.replace(temporary, path)
            return True
        except OSError as error:
            if temporary is not None:
                # 不留垃圾：失败时清掉自己这一份临时文件。
                try:
                    os.unlink(temporary)
                except OSError:
                    pass
            if attempt == _WRITE_RETRIES - 1:
                # 保留旧文件（TTL 内仍可用），下一轮刷新再试。
                print(f"[hydro-node-index] 写盘失败，保留旧文件: {error}", flush=True)
                return False
            time.sleep(_WRITE_RETRY_SLEEP)
    return False
