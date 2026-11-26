"""水动力工况映射（D3，见 `docs/model-db-p0-design.md` §D3）。

## 为什么收在这里

`get_mapped_tidal_level_and_mapped_water_qs` 原先在 4 个脚本里各存一份
（riskLevel / antiImpactSpeed / waterLevelFluctuation / hydrodynamic），
用 AST 逐个取源码算 md5，4 份**逐字节相同**（`108a20270c8b`，2270 字节）。
同一段逻辑抄 4 份，改一处漏三处的风险大于它的便利，故合并。

顺带删掉 4 份函数体内嵌的 `get_qs_and_level()`：它是**从未被调用**的死代码
（全仓 grep 只有 4 个 `def`、0 个调用点；潮位映射实际走的是下面的
`isinstance(tidal_level, str)` 分支，正则那条路早就不通了）。删掉后
`import re` 在 4 个脚本里也一并未使用，一并移除。

## 行为契约（必须逐字保持，勿"顺手优化"）

1. `tidal_level` 是字符串 ⇒ 原样作为工况键；不是字符串 ⇒ 强转 float 后按
   `< 1.5 → xc` / `< 2.3 → zc` / 其余 `dc` 分档。传 `None` 会在这句抛
   `TypeError`，与旧实现一致。
2. 第一次查库为空 ⇒ 回落 `DEFAULT_DOMAIN` + `set_name="standard"` + `year="default"`
   再查一次。**`year` 被改写是旧行为**，上游 `riskLevel.PARSING` 已用映射前的
   副本（`pq_segment` / `pq_year`）规避 PQ 拿错年份；本次重构不动这个语义。
3. 两次都为空 ⇒ `ValueError`（文案含三个键，排查用）。
4. 取档规则：`len(nodes)==1` 直接取唯一档；否则 `<= nodes[0]` / `>= nodes[-1]` /
   命中 / 中点分割四分支。

⚠ 当前库里 5 个分组**每组只有 1 档**，所以第 4 条实际永远走第一个分支，
其余三个分支在生产中从未执行过 —— 测试里必须单独钉住（见
`tests/test_hydro_map.py`），否则它们只会在"以后补导了别的档位"时才第一次运行。

## 参数说明

`year` 只透传、不参与任何查询：`get_available_hydrodynamic_nodes` 的键是
`(dataset_domain, set_name, tidal_level)`，没有年份维度。保留形参是为了让 4 处
调用点的位置参数不变（少改一处就少一处出错机会）。
"""

from util import db_ops


def map_water_qs(
    segment_name: str,
    year: str,
    set_name: str,
    tidal_level: list | str,
    water_qs: str,
):
    """把模板里的流量档映射到该工况真实存在的档位，并返回对齐后的工况坐标。

    返回 `(mapped_tidal_level, mapped_water_qs, segment_name, year, set_name)`。
    后三项可能被回落分支改写，调用方若不希望被改写，须在调用前另存副本。
    """
    mapped_tidal_level = ""
    if not isinstance(tidal_level, str):
        tidal_level = float(tidal_level)
        mapped_tidal_level = ""
        b1 = 1.5
        b2 = 2.3
        value = tidal_level
        if value < b1:
            mapped_tidal_level = "xc"
        elif value < b2:
            mapped_tidal_level = "zc"
        else:
            mapped_tidal_level = "dc"
    else:
        mapped_tidal_level = tidal_level

    # Get all node
    nodes = db_ops.get_available_hydrodynamic_nodes(
        dataset_domain=segment_name,
        set_name=set_name,
        tidal_level=mapped_tidal_level,
    )

    if not nodes:
        segment_name = db_ops.DEFAULT_DOMAIN
        set_name = "standard"
        year = "default"
        nodes = db_ops.get_available_hydrodynamic_nodes(
            dataset_domain=segment_name,
            set_name=set_name,
            tidal_level=mapped_tidal_level,
        )

    if not nodes:
        raise ValueError(
            f"No available hydrodynamic nodes for segment={segment_name}, set={set_name}, tidal_level={mapped_tidal_level}"
        )

    # map the input water_qs to node
    mapped_water_qs = 0
    i_water_qs = int(water_qs)
    if len(nodes) == 1:
        mapped_water_qs = nodes[0]
    elif i_water_qs <= nodes[0]:
        mapped_water_qs = nodes[0]
    elif i_water_qs >= nodes[-1]:
        mapped_water_qs = nodes[-1]
    elif i_water_qs in nodes:
        mapped_water_qs = i_water_qs
    else:
        middles = [
            (nodes[i] + (nodes[i + 1] - nodes[i]) / 2 - 1)
            for i in range(len(nodes) - 1)
        ]
        closest_middle = min(middles, key=lambda middle: abs(middle - i_water_qs))
        index = middles.index(closest_middle)

        if i_water_qs - closest_middle < 0:
            mapped_water_qs = nodes[index]
        else:
            mapped_water_qs = nodes[index + 1]

    return mapped_tidal_level, mapped_water_qs, segment_name, year, set_name
