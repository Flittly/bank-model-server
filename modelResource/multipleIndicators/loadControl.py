import os
import sys

module_path = os.path.abspath(os.path.join(os.getcwd()))
if module_path not in sys.path:
    sys.path.append(module_path)

import model
import config

# 工程扰动控制等级枚举 -> 档位。1 档 = 控制最严格 = 风险最低；4 档 = 无控制 = 风险最高。
# 注意：与 Slope Protection 用的是两套字符串（这里用 strict 表示「强」，那边用 systemic）。
CONTROL_LEVEL_TIERS = {
    "strict": 1,
    "normal": 2,
    "low": 3,
    "no": 4,
}


def normalize_level(level):
    return level.strip().lower() if isinstance(level, str) else ""


def compute_load_control(control_level):
    """把工程扰动控制等级枚举映射成 one-hot 档位向量 [1档,2档,3档,4档]。

    非法值（None / 空串 / 未知枚举）必须抛异常，不允许静默兜底：
    兜底成第 1 档等于把「未填报」当成「控制最严格」，会让风险等级凭空偏低且无从察觉。
    """
    level = normalize_level(control_level)
    if level not in CONTROL_LEVEL_TIERS:
        raise ValueError(
            "invalid control-level: %r; expect one of %s"
            % (control_level, sorted(CONTROL_LEVEL_TIERS))
        )

    risk = [0, 0, 0, 0]
    risk[CONTROL_LEVEL_TIERS[level] - 1] = 1
    return risk
    
##########################################################################################

@model.model_status_controller_sync
def run_load_control_mcr(mcr: model.ModelCaseReference):
    
    control_level = mcr.request_json['control-level']

    risk = compute_load_control(control_level)

    # LC 是枚举输入、没有连续量，早期版本只回 risk-level，导致上游 riskLevel.py 的
    # raw_values["LC"] 恒为 None、结果页只能显示 N/A。这里显式回带原始枚举与档位。
    level = normalize_level(control_level)

    return {
        
        'case-id': mcr.id,
        'risk-level': risk,
        'LC': level,
        'risk-tier': CONTROL_LEVEL_TIERS[level],
    }
    
##########################################################################################

NAME = 'Load Control'

CATEGORY = 'Multiple Indicators'

CATEGORY_ALIAS = 'mi'
 
def PARSING(self, request_json: dict, model_path: str, other_dependent_ids: list[str]=[]):

    load_control_mcr = model.ModelCaseReference.create(config.API_MI_LOAD_CONTROL, request_json, NAME, model_path, other_dependent_ids)
    
    return [load_control_mcr]

def RESPONSING(self, core_mcr: model.ModelCaseReference, default_pre_mcrs: list[model.ModelCaseReference], other_pre_mcrs: list[model.ModelCaseReference]):
    
    return core_mcr.make_response({
                                    'case-id': 'TEMPLATE',
                                    'risk-level': 'NONE'
                                })
    
##########################################################################################

if __name__ == '__main__':

    v1 = sys.argv[1]    # load control model case reference id

    mcr = model.ModelCaseReference.open_case(v1)

    # Run model case (Load Control)
    run_load_control_mcr(mcr)
