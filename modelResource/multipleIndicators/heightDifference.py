import os
import sys

module_path = os.path.abspath(os.path.join(os.getcwd()))
if module_path not in sys.path:
    sys.path.append(module_path)

import model
import config
import json
    
def compute_height_difference(section_view_output_path, risk_threshold):

    section_json = os.path.join(section_view_output_path, 'section.json')
    risk = []
    if risk_threshold == "NONE":
        with open(config.DIR_RESOURCE_RISKLEVEL_THRESHOLD_TEMPLATE, 'r', encoding='utf-8') as file: 
            risk_threshold = json.load(file).get("Zb")
    with open(section_json, 'r', encoding='utf-8') as file:
        data = json.load(file)
        points = data.get('points')
        deepest_index = data.get('deepest_index')
        Zb = -points[deepest_index][2]
        if Zb < risk_threshold[0]:
            risk = [1,0,0,0]
        elif Zb < risk_threshold[1]:
            risk = [0,1,0,0]
        elif Zb < risk_threshold[2]:
            risk = [0,0,1,0]
        elif Zb >= risk_threshold[2]:
            risk = [0,0,0,1]
        return Zb, risk
    
##########################################################################################

@model.model_status_controller_sync
def run_height_difference_mcr(mcr: model.ModelCaseReference):
    
    section_view_output_path = os.path.join(section_view_mcr.directory, 'result')
    risk_threshold = mcr.request_json['risk-threshold']
    
    Zb, risk = compute_height_difference(section_view_output_path, risk_threshold)
    
    return {
        
        "case-id": mcr.id,
        "Zb": Zb,
        "risk-level": risk
    }
    
##########################################################################################

NAME = 'Height Difference'

CATEGORY = 'Multiple Indicators'

CATEGORY_ALIAS = 'mi'
 
def PARSING(self, request_json: dict, model_path: str, other_dependent_ids: list[str]=[]):

    height_difference_json = request_json
    section_view_json = {
        'dem-id': height_difference_json['dem-id'],
        'section-geometry': height_difference_json['section-geometry']
    }

    section_view_mcr = model.launcher.fetch_model_from_API(config.API_RE_SECTION_VIEW).run(section_view_json, other_dependent_ids)
    height_difference_mcr = model.ModelCaseReference.create(config.API_MI_HEIGHT_DIFFERENCE, height_difference_json, NAME, model_path, other_dependent_ids + [section_view_mcr.id])
    
    return [section_view_mcr, height_difference_mcr]

def RESPONSING(self, core_mcr: model.ModelCaseReference, default_pre_mcrs: list[model.ModelCaseReference], other_pre_mcrs: list[model.ModelCaseReference]):
    
    return core_mcr.make_response({
                                    'case-id': 'TEMPLATE',
                                    'Zb': 'NONE',
                                    'risk-level': 'NONE'
                                })
    
##########################################################################################

if __name__ == '__main__':
    
    v1 = sys.argv[1]    
    v2 = sys.argv[2]    
    
    section_view_mcr = model.ModelCaseReference.open_case(v1)
    height_difference_mcr = model.ModelCaseReference.open_case(v2)
    
    # Run model case (Height Difference)
    run_height_difference_mcr(height_difference_mcr)
        