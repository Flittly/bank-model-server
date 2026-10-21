import os
import re
import sys
import json

module_path = os.path.abspath(os.path.join(os.getcwd()))
if module_path not in sys.path:
    sys.path.append(module_path)

import config
import model

# PQ（造床流量当量）的数据源是「一份按年份排列的序列」，不分岸段。
# 真实文件：resource/json/Mzs/<年>/standard/PQ/pq.json，其中 Mzs/<年> 只是它所在的层级，
# 文件内部才是 2010–2023 的全套年份。因此解析顺序是：
#   ① json/<segment>/<year>/standard/PQ/pq.json   —— 将来若出现按岸段/年份拆分的文件，优先用
#   ② json/<segment>/standard/PQ/pq.json          —— 按岸段但年份共享的文件
#   ③ config.DIR_RESOURCE_PQ_TEMPLATE             —— 全局兜底（就是上面那个文件）
# 三条都不存在时必须报错，不能静默返回某个默认值（否则会得出「所有断面 PQ 都一样」的假结果）。
PQ_SUBDIR = ("standard", "PQ", "pq.json")

_YEAR_PREFIX = re.compile(r"^(\d{4})(?!\d)")
_YEAR_SEPARATORS = re.compile(r"[-/.]")


def _normalize_year(value):
    """把 2023 / "2023" / "2023-04-01" / "202311" / "2023.4" 归一成 int 年份。

    无法识别（如 "default"、""、None）一律返回 None —— 由调用方决定降级策略，
    绝不再用字符串比较去「夹取」，那会把 "default" 静默变成 "2023"。
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        year = int(value)
        return year if 1900 <= year <= 2999 else None

    text = str(value).strip()
    if not text:
        return None

    # "2023"、"2023-04-01"、"2023.4"：前 4 位是年份且第 5 位不是数字
    matched = _YEAR_PREFIX.match(text)
    if matched:
        return int(matched.group(1))

    # "202311"、"20230401"："2023" 后面还有数字，按紧凑日期解析
    compact = _YEAR_SEPARATORS.sub("", text)
    if compact.isdigit() and len(compact) in (6, 8):
        return int(compact[:4])

    return None


def _resolve_pq_source(segment, year):
    """按 ①→②→③ 找 PQ 数据文件，返回 (路径, 来源标识, 实际使用的年份目录名)。

    返回来源标识是为了让上游能看出「这条结果用的是全局兜底文件」还是「岸段专属文件」，
    之前所有断面都静默走兜底、且没有任何输出，才让「数据没接上」完全不可见。
    """
    year_dir = str(year) if year not in (None, "") else None
    candidates = []
    if segment:
        if year_dir:
            candidates.append(
                ("segment-year", os.path.join(config.DIR_RESOURCE_JSON, str(segment), year_dir, *PQ_SUBDIR))
            )
        candidates.append(
            ("segment", os.path.join(config.DIR_RESOURCE_JSON, str(segment), *PQ_SUBDIR))
        )
    candidates.append(("template", config.DIR_RESOURCE_PQ_TEMPLATE))

    for source, path in candidates:
        if os.path.exists(path):
            return path, source

    raise FileNotFoundError(
        "PQ data source not found, segment=%r, year=%r, tried=%s"
        % (segment, year, [path for _, path in candidates])
    )


def _pick_year(pq_dict, requested_year):
    """在 PQ 序列里挑出最贴合请求年份的键。

    返回 (键, 请求年份 int 或 None, 是否精确命中)。
    请求年份无法识别时取序列里最新的年份（保持旧行为的最优解），但会标记为不精确，
    以便上层把它写进 data-quality 而不是当成真值。
    """
    year_map = {}
    for key in pq_dict.keys():
        parsed = _normalize_year(key)
        if parsed is not None:
            year_map[parsed] = key
    if not year_map:
        raise ValueError("PQ data source has no valid year key: %s" % sorted(pq_dict.keys()))

    available = sorted(year_map)
    if requested_year is None:
        return year_map[available[-1]], None, False
    if requested_year in year_map:
        return year_map[requested_year], requested_year, True

    nearest = min(available, key=lambda candidate: (abs(candidate - requested_year), -candidate))
    return year_map[nearest], requested_year, False


def compute_flow_equivalent(segment, year, risk_threshold):
    """计算 PQ 及其风险档位。

    返回 (PQ, risk, meta)。meta 里带数据来源、实际年份、是否精确命中，
    上游写进响应后可以直接判断「这条结果的 PQ 到底来自哪一年、哪个文件」。
    """
    if risk_threshold in (None, "NONE"):
        with open(config.DIR_RESOURCE_RISKLEVEL_THRESHOLD_TEMPLATE, "r", encoding="utf-8") as file:
            risk_threshold = json.load(file)["PQ"]

    # 先用归一化后的年份去找文件（"2023-04-01" / "202311" 都是 <年 2023 目录>），
    # 归一化失败才回退成原始值，让 _resolve_pq_source 决定跳过「按年」这一层候选。
    requested = _normalize_year(year)
    path, source = _resolve_pq_source(segment, requested if requested is not None else year)
    with open(path, "r", encoding="utf-8") as file:
        PQ_dict = json.load(file)

    year_key, requested, exact = _pick_year(PQ_dict, requested)
    PQ = PQ_dict[year_key]

    risk = []
    if PQ < risk_threshold[0]:
        risk = [1, 0, 0, 0]
    elif PQ < risk_threshold[1]:
        risk = [0, 1, 0, 0]
    elif PQ < risk_threshold[2]:
        risk = [0, 0, 1, 0]
    elif PQ >= risk_threshold[2]:
        risk = [0, 0, 0, 1]

    meta = {
        "source": source,
        "path": os.path.relpath(path, config.DIR_ROOT).replace("\\", "/"),
        "year-requested": requested,
        "year-used": int(year_key),
        "year-exact": exact,
    }
    print(
        "[flow-equivalent][debug] segment=%r year=%r -> path=%s source=%s year_used=%s exact=%s PQ=%s"
        % (segment, year, meta["path"], source, year_key, exact, PQ)
    )
    return PQ, risk, meta


##########################################################################################

@model.model_status_controller_sync
def run_flow_equivalent_mcr(mcr: model.ModelCaseReference):

    segment = mcr.request_json['segment']
    year = mcr.request_json['year']
    risk_threshold = mcr.request_json['risk-threshold']

    PQ, risk, meta = compute_flow_equivalent(segment, year, risk_threshold)

    return {
        'case-id': mcr.id,
        'PQ': PQ,
        'risk-level': risk,
        'year': meta['year-used'],
        'data-source': meta['source'],
        'data-quality': {
            'year-requested': meta['year-requested'],
            'year-used': meta['year-used'],
            'year-exact': meta['year-exact'],
            'source': meta['source'],
            'path': meta['path'],
        },
    }

##########################################################################################

NAME = 'Flow Equivalent'

CATEGORY = 'Multiple Indicators'

CATEGORY_ALIAS = 'mi'

def PARSING(self, request_json: dict, model_path: str, other_dependent_ids: list[str]=[]):

    flow_equivalent_mcr = model.ModelCaseReference.create(config.API_MI_FLOW_EQUIVALENT, request_json, NAME, model_path, other_dependent_ids)
    
    return [flow_equivalent_mcr]

def RESPONSING(self, core_mcr: model.ModelCaseReference, default_pre_mcrs: list[model.ModelCaseReference], other_pre_mcrs: list[model.ModelCaseReference]):
    
    return core_mcr.make_response({
                                    'case-id': 'TEMPLATE',
                                    'PQ': 'NONE',
                                    'risk-level': 'NONE'
                                })
    
##########################################################################################

if __name__ == '__main__':

    v1 = sys.argv[1]    # flow equivalent model case reference id

    mcr = model.ModelCaseReference.open_case(v1)

    # Run model case (Flow Equivalent)
    run_flow_equivalent_mcr(mcr)
