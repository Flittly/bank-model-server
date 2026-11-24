import os
import sys
import json

module_path = os.path.abspath(os.path.join(os.getcwd()))
if module_path not in sys.path:
    sys.path.append(module_path)

import model
import config

def compute_soil_composition(hs, hc, risk_threshold):
    if risk_threshold == "NONE":
        with open(config.DIR_RESOURCE_RISKLEVEL_THRESHOLD_TEMPLATE, 'r', encoding='utf-8') as file: 
            risk_threshold = json.load(file).get("Dsed")
    D_sed = hs / hc

    risk = []

    if D_sed < risk_threshold[0]:
        risk = [1,0,0,0]
    elif D_sed < risk_threshold[1]:
        risk = [0,1,0,0]
    elif D_sed < risk_threshold[2]:
        risk = [0,0,1,0]
    elif D_sed >= risk_threshold[2]:
        risk = [0,0,0,1]

    return D_sed, risk
    
##########################################################################################

@model.model_status_controller_sync
def run_soil_composition_mcr(mcr: model.ModelCaseReference):
    
    hs = mcr.request_json['hs']
    hc = mcr.request_json['hc']
    risk_threshold = mcr.request_json['risk-threshold']

    D_sed, risk = compute_soil_composition(hs, hc, risk_threshold)

    return {
        
        'case-id': mcr.id,
        'Dsed': D_sed,
        'risk-level': risk
    }
    
##########################################################################################

NAME = 'Soil Composition'

CATEGORY = 'Multiple Indicators'

CATEGORY_ALIAS = 'mi'
 
def PARSING(self, request_json: dict, model_path: str, other_dependent_ids: list[str]=[]):

    soil_composition_mcr = model.ModelCaseReference.create(config.API_MI_SOIL_COMPOSITION, request_json, NAME, model_path, other_dependent_ids)
    
    return [soil_composition_mcr]

def RESPONSING(self, core_mcr: model.ModelCaseReference, default_pre_mcrs: list[model.ModelCaseReference], other_pre_mcrs: list[model.ModelCaseReference]):
    
    return core_mcr.make_response({
                                    'case-id': 'TEMPLATE',
                                    'Dsed': 'NONE',
                                    'risk-level':'NONE'
                                })

##########################################################################################

if __name__ == '__main__':

    v1 = sys.argv[1]    # soil composition model case id

    model.require_cases([v1], NAME)

    mcr = model.ModelCaseReference.open_case(v1)

    # Run model case (Soil composition)
    run_soil_composition_mcr(mcr)
    