import os
import sys
import stat
import shutil
import subprocess

module_path = os.path.abspath(os.path.join(os.getcwd()))
if module_path not in sys.path:
    sys.path.append(module_path)
    
import config
from model import (
    ModelCaseReference as MCR,
    generate_hydrodynamic_resource,
    model_status_controller_sync,
    require_cases,
)

def chomd_x(file_path):
    
    os.chmod(file_path, os.stat(file_path).st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

def real_hydrodynamic(segment_name: str, year: str, set_name: str, case_name: str, temp: bool, boundary: str):

    PATH_ADCPREP_PROTOTYPE = os.path.join(config.DIR_RESOURCE_MODEL, 'adcprep')
    PATH_PADCIRC_PROTOTYPE = os.path.join(config.DIR_RESOURCE_MODEL, 'padcirc')

    path_start = os.path.join(config.DIR_RESOURCE_HYDRODYNAMIC, segment_name, year, set_name, case_name)
    path_adcprep = os.path.join(path_start, 'adcprep')
    path_padcirc = os.path.join(path_start, 'padcirc')
    shutil.copyfile(PATH_ADCPREP_PROTOTYPE, path_adcprep)
    shutil.copyfile(PATH_PADCIRC_PROTOTYPE, path_padcirc)
    chomd_x(path_adcprep)
    chomd_x(path_padcirc)

    # PASS 1
    with subprocess.Popen([path_adcprep], cwd=path_start, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as process:
        
        stdout, stderr = process.communicate(input=f'{config.CAL_CORE_NUM}\n1\nfort.14\n')

        print(stdout)
        print(stderr)
        
    # PASS 2
    with subprocess.Popen(['./adcprep'], cwd=path_start, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as process:
        
        stdout, stderr = process.communicate(input=f'{config.CAL_CORE_NUM}\n2\n')

        print(stdout)
        print(stderr)

    # PASS 3
    with subprocess.Popen(['mpirun', '-np', f'{config.CAL_CORE_NUM}', './padcirc'], cwd=path_start, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1) as process:
        
        for line in iter(process.stdout.readline, ''):
            print(line.strip())
        for line in iter(process.stderr.readline, ''):
            print(line.strip())
                
    return_code = process.wait()
    print(f'Process finished with return code {return_code}')
    
    # PASS 4
    generate_hydrodynamic_resource(segment_name, year, set_name, case_name, temp, boundary)

##########################################################################################

@model_status_controller_sync
def run_real_hydrodynamic_mcr(mcr: MCR):
    
    segment = mcr.request_json['segment']
    year = str(mcr.request_json['year'])
    name = mcr.request_json['name']
    set = mcr.request_json['set']
    temp = mcr.request_json['temp']
    boundary = mcr.request_json['boundary']
    
    real_hydrodynamic(segment, year, set, name, temp, boundary)
    
    return {
        'case-id': mcr.id,
        'status': 'OK'
    }

##########################################################################################

NAME = 'Real Hydrodynamic'

CATEGORY = 'Numerical Model'

CATEGORY_ALIAS = 'nm'
 
def PARSING(self, request_json: dict, model_path: str, other_dependent_ids: list[str]=[]):
    
    saved_path = os.path.join(config.DIR_RESOURCE_HYDRODYNAMIC, request_json['segment'], str(request_json['year']), request_json['set'], request_json['name'])
    required_files = ['fort.13', 'fort.14', 'fort.15', 'fort.19', 'fort.20']
    if not os.path.exists(saved_path):
        
        os.makedirs(saved_path)
        for filename in required_files:
            file = request_json.get(filename)
            if file:
                file_path = os.path.join(saved_path, file.filename)
                file.save(file_path)
            del request_json[filename]
    
    else:
        for filename in required_files:
            del request_json[filename]
        
    real_hydro_mcr = MCR(config.API_NM_REAL_HYDRODYNAMIC, request_json, NAME, model_path, other_dependent_ids)
    
    return [real_hydro_mcr]

def RESPONSING(self, core_mcr: MCR, default_pre_mcrs: list[MCR], other_pre_mcrs: list[MCR]):
    
    return core_mcr.make_response({
                    'case-id': 'TEMPLATE',
                    'status': 'RUNNING'
                })

##########################################################################################

if __name__ == '__main__':
    
    v1 = sys.argv[1]
    
    require_cases([v1], NAME)

    real_hydro_mcr = MCR.open_case(v1)
    
    run_real_hydrodynamic_mcr(real_hydro_mcr)
