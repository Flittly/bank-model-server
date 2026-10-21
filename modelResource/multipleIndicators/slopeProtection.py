import os
import sys

module_path = os.path.abspath(os.path.join(os.getcwd()))
if module_path not in sys.path:
    sys.path.append(module_path)

import model
import config

# 护岸等级枚举 -> 档位。1 档 = 护岸最强 = 风险最低；4 档 = 无护岸 = 风险最高。
PROTECTION_LEVEL_TIERS = {
    "systemic": 1,
    "normal": 2,
    "low": 3,
    "no": 4,
}


def normalize_level(level):
    return level.strip().lower() if isinstance(level, str) else ""


def compute_slope_protection(protection_level):
    """把护岸等级枚举映射成 one-hot 档位向量 [1档,2档,3档,4档]。

    非法值（None / 空串 / 未知枚举）必须抛异常，不允许静默兜底：
    兜底成第 1 档等于把「未填报」当成「护岸最强」，会让风险等级凭空偏低且无从察觉，
    这正是本次 PL/LC 问题的同款错误。
    """
    level = normalize_level(protection_level)
    if level not in PROTECTION_LEVEL_TIERS:
        raise ValueError(
            "invalid protection-level: %r; expect one of %s"
            % (protection_level, sorted(PROTECTION_LEVEL_TIERS))
        )

    risk = [0, 0, 0, 0]
    risk[PROTECTION_LEVEL_TIERS[level] - 1] = 1
    return risk
    
##########################################################################################

@model.model_status_controller_sync
def run_slope_protection_mcr(mcr: model.ModelCaseReference):
    
    protection_level = mcr.request_json['protection-level']

    risk = compute_slope_protection(protection_level)

    # PL 是枚举输入、没有连续量，早期版本只回 risk-level，导致上游 riskLevel.py 的
    # raw_values["PL"] 恒为 None、结果页只能显示 N/A。这里显式回带原始枚举与档位。
    level = normalize_level(protection_level)

    return {
        
        'case-id': mcr.id,
        'risk-level': risk,
        'PL': level,
        'risk-tier': PROTECTION_LEVEL_TIERS[level],
    }
    
##########################################################################################

NAME = 'Slope Protection'

CATEGORY = 'Multiple Indicators'

CATEGORY_ALIAS = 'mi'
 
def PARSING(self, request_json: dict, model_path: str, other_dependent_ids: list[str]=[]):

    slope_protection_mcr = model.ModelCaseReference.create(config.API_MI_SLOPE_PROTECTION, request_json, NAME, model_path, other_dependent_ids)
    
    return [slope_protection_mcr]

def RESPONSING(self, core_mcr: model.ModelCaseReference, default_pre_mcrs: list[model.ModelCaseReference], other_pre_mcrs: list[model.ModelCaseReference]):
    
    return core_mcr.make_response({
                                    'case-id': 'TEMPLATE',
                                    'risk-level': 'NONE'
                                })

##########################################################################################

if __name__ == '__main__':

    v1 = sys.argv[1]    # slope protection model case reference id

    mcr = model.ModelCaseReference.open_case(v1)

    # Run model case (Slope Protection)
    run_slope_protection_mcr(mcr)
