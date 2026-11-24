import sys
import os
module_path = os.path.abspath(os.path.join(os.getcwd()))
if module_path not in sys.path:
    sys.path.append(module_path)

from modelResource.erosionModel.BSTEM_xls import set_bank_geometry, initVX, compute_layer_slice_area
import modelResource.erosionModel.Excel_source_codes.xcel_fx as xcel_fx
import model
import config
import json

def erosion_model(x_values, z_values, intToePoint, bankLayerThickNess, channelFlowParams, boolTension=False):

    print('x_values:', x_values)
    print('z_values:', z_values)
    print('index_toe:', intToePoint)

    bankLayerThickNess = []
    if bankLayerThickNess == "NONE":
        with open(os.path.join(config.DIR_RESOURCE_EROSIONMODEL, 'template.json'), 'r', encoding='utf-8') as file: 
            bankLayerThickNess = json.load(file).get("bankLayerThickNess")

    # view_bank(x_values, z_values)
    initVX(os.path.join(config.DIR_TRIGGER_RESOURCE, 'erosionModel', 'Excel_source_codes', 'worksheets') + '/')
    # set_bank_geometry(x_values, z_values, intToePoint)
    set_bank_geometry(x_values, z_values, intToePoint)
    xcel_fx.cells_reset()
    # compute_layer_slice_area(x_values, z_values, intToePoint, bankLayerThickNess, -29.1, 26.92, 130, 0.0000255, 1.5, 720, list_own_friction_angle = list_own_friction_angle, list_own_list_own_phi_b = list_own_list_own_phi_b)
    fos = compute_layer_slice_area(x_values, z_values, intToePoint, bankLayerThickNess=bankLayerThickNess, boolTension=boolTension, channelFlowParams=channelFlowParams)
    return fos

##########################################################################################
    
@model.model_status_controller_sync
def run_erosion_model_mcr(mcr: model.ModelCaseReference):
    
    x_values = []
    z_values = []
    # bool_tension = mcr.request_json['bool-tension']
    bankLayerThickNess = mcr.request_json['bank-layer-thickness']

    # if (not config.APP_DEBUG) and mcr.request_json['dem-id'] != "NONE":
    #     section_view_output_path = os.path.join(section_view_mcr.directory, 'result')
    #     section_json = os.path.join(section_view_output_path, 'section.json')
    #     with open(section_json, 'r', encoding='utf-8') as file:
    #         data = json.load(file)
    #         points_er_verified = data.get('points_er_verified')
    #         step_er_verified = data.get('step_er_verified')
    #         current_distance = 0
    #         for point in points_er_verified:
    #             z_values.append(point[2])
    #             x_values.append(current_distance)
    #             current_distance += step_er_verified
    #     min_value = min(z_values)
    #     index_toe = z_values.index(min_value)
    # else:
    x_values = mcr.request_json['x-values']
    z_values = mcr.request_json['z-values']
    index_toe = mcr.request_json['index-toe']
    # bool_tension = False
    flow_elevation = mcr.request_json['flow-elevation']
    bankLayerThickNess = mcr.request_json['bank-layer-thickness']
    # x_values = [0.00,24.39,28.62,32.69,36.76,39.15,41.55,45.84,50.13,51.10,52.78,56.79,60.81,64.83,68.85,72.86,73.67,77.68,84.45,91.65,98.49,102.88,109.54]
    # z_values = [3.43,3.43,1.14,-1.46,-4.07,-4.07,-5.24,-7.41,-9.58,-11.57,-15.01,-17.72,-20.43,-23.14,-25.85,-28.56,-29.10,-29.10,-29.10,-29.10,-29.10,-29.10,-29.10]
    # index_toe = 17
    # bool_tension = False
    # bankLayerThickNess = [1.5, 6, 7.5, 15, 10]
    
    fos, fs, seEle, ssAngle, fX1, fZ1, fX2, fZ2 = erosion_model(x_values, z_values, index_toe, bankLayerThickNess, channelFlowParams=[130, 0.000025, flow_elevation, 720])
    
    return {
        "case-id": mcr.id,
        "fos": fos,
        "see": seEle,
        "ssa": ssAngle,
        "point1": [fX1, fZ1],
        "point2": [fX2, fZ2]
    }

##########################################################################################

NAME = 'BSTEM'

CATEGORY = 'Erosion Model'

CATEGORY_ALIAS = 'em'
 
def PARSING(self, request_json: dict, model_path: str, other_dependent_ids: list[str]=[]):

    bstem_mcr = model.ModelCaseReference.create(config.API_EM_BSTEM, request_json, NAME, model_path, other_dependent_ids)
    
    return [bstem_mcr]

def RESPONSING(self, core_mcr: model.ModelCaseReference, default_pre_mcrs: list[model.ModelCaseReference], other_pre_mcrs: list[model.ModelCaseReference]):
    
    return core_mcr.make_response({
                                    'case-id': 'TEMPLATE',
                                    'fos': 'None',
                                    'see': 'None',
                                    'ssa': 'None',
                                    'point1': 'None',
                                    'point2': 'None'
                                })

##########################################################################################

if __name__ == '__main__':
    
    v1 = sys.argv[1]    # erosion model case reference id
    
    model.require_cases([v1], NAME)

    mcr = model.ModelCaseReference.open_case(v1)

    # if (not config.APP_DEBUG) and mcr.request_json['dem-id'] != "NONE":
        
    #     section_view_json = {
    #         "dem-id": mcr.request_json['dem-id'],
    #         "section-geometry": mcr.request_json['section-geometry']
    #     }
    #     section_view_mcr = model.ModelCaseReference.create(config.API_RE_SECTION_VIEW, section_view_json, 'Section View')
    #     model.run_section_view_mcr(section_view_mcr)
    
    # Run model case (Erosion Model)
    run_erosion_model_mcr(mcr)
