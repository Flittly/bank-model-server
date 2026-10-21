import os
import re
import sys
import json
import shutil
import struct
import subprocess
from osgeo import osr

def _ensure_utf8_stdout() -> None:
    """Windows 控制台默认编码不是 UTF-8，直接 print 中文会乱码甚至抛 UnicodeEncodeError。

    这里必须用 ``reconfigure`` **原地**调整编码，而不是 ``sys.stdout = io.TextIOWrapper(...)``
    新建包装器覆盖：新建会让原对象失去引用，在 pytest 等已经把 sys.stdout 换成捕获流的场景下，
    旧流被 GC 关闭后会抛 "ValueError: I/O operation on closed file"，
    导致整个测试框架在 session 收尾时崩溃（本仓库曾因此无法跑 pytest）。
    """
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding='utf-8')  # type: ignore[union-attr]
        except Exception:
            # 流不支持 reconfigure（如被测试框架替换/包装过）时静默跳过，
            # 这是增强项，不应影响导入方。
            continue


_ensure_utf8_stdout()

module_path = os.path.abspath(os.path.join(os.getcwd()))
if module_path not in sys.path:
    sys.path.append(module_path)

import util
import config
    
######################################## Visualization Resource Builder ########################################

class Station:
    
    def __init__(self, sourceX, sourceY, uvs):
        self.sourceCoord = (sourceX, sourceY)
        self.uvs = uvs
        self.targetCoord = (0, 0)

def validate(num):
    
    absolute = abs(num)
    mini = 0.0009
    maxi = 999
    if (absolute < mini or absolute > maxi):
        return 0.0
    else:
        return round(num, 3)

def init_station(fort14file, stationCount):
    
    with open(fort14file, 'r') as f14:
        # jump
        f14.readline()
        f14.readline()
        # read
        stations = []
        stationIndex = 0
        while (stationIndex < stationCount):
            line = re.split(r'\s+', f14.readline().strip())
            line = [float(data) for data in line]
            stations.append(Station(line[1], line[2], []))
            stationIndex = stationIndex + 1

    print('==== Stations Initialization Completed ====', flush=True)
    return stations


def update_station(fort64file, stations, stationCount):
    
    with open(fort64file, 'r') as f64:
        # jump
        f64.readline()
        f64.readline()
        # read
        timeCount = 0
        totalCount = 26

        while (timeCount < totalCount):
            # jump
            f64.readline()
            stationIndex = 0
            while (stationIndex < stationCount):
                line = re.split(r'\s+', f64.readline().strip())
                line = [validate(float(data)) for data in line]
                uv = (line[1], line[2])
                stations[stationIndex].uvs.append(uv)
                stationIndex = stationIndex + 1

            timeCount = timeCount + 1
    print('==== Stations Update Completed ====', flush=True)
    return stations

def station_coord_transform(stations):
    
    sourceEPSG = 2437
    targetEPSG = 4326

    source = osr.SpatialReference()
    target = osr.SpatialReference()
    source.ImportFromEPSG(sourceEPSG)
    target.ImportFromEPSG(targetEPSG)
    transformation = osr.CoordinateTransformation(source, target)

    for station in stations:
        coords = transformation.TransformPoint(station.sourceCoord[1], station.sourceCoord[0])
        station.targetCoord = (coords[1], coords[0])

    print('====station coords transform finish====', flush=True)
    return stations


def write_bin(stations, binDir):
    
    if not os.path.exists(binDir):
        os.makedirs(binDir)
        print(f"Directory '{binDir}' Build Succeeded", flush=True)

    station_coord_transform(stations)

    stationFile = "station.bin"
    with open(os.path.join(binDir, stationFile), 'wb') as bin_file:
        for station in stations:
            position = struct.pack('ff', station.targetCoord[0], station.targetCoord[1])
            bin_file.write(position)
    print('==== station.bin Written====', flush=True)

    timeCount = 26
    for i in range(timeCount):
        fileName = f"uv_{i}.bin"
        with open(os.path.join(binDir, fileName), 'wb') as uv_bin_file:
            for station in stations:
                uv = struct.pack("ff", station.uvs[i][0], station.uvs[i][1])
                uv_bin_file.write(uv)
    print('==== uv.bin Written ====', flush=True)
    
######################################## Model Case Runner ########################################

def read_14_data(filename):
    sum = 0
    data = []
    temp = []
    with open(filename, 'r') as file:
        for i, line in enumerate(file):
            parts = line.strip().split()  
            if i == 0:
                continue
            elif i == 1:
                sum = int(parts[1]) # 获取总节点数
            elif i - 1 <= sum:
                data.append((int(parts[0]), float(parts[1]), float(parts[2]), float(parts[3])))
    return data, sum

def read_63_64_data(filename, sum):
    data = []
    temp = []
    with open(filename, 'r') as file:
        for i, line in enumerate(file):
            parts = line.strip().split()  
            if i < 3:
                continue
            else:
                if float(parts[0]) >= 1 and float(parts[0]) < sum:
                    temp.append((int(parts[0]), float(parts[1]), float(parts[2]) if os.path.basename(filename) == 'fort.64' else None))
                    
                elif float(parts[0]) == sum:
                    temp.append((int(parts[0]), float(parts[1]), float(parts[2]) if os.path.basename(filename) == 'fort.64' else None))
                    data.append(temp)
                    temp = []
                else:
                    pass
                
    return data 

def mergeOutput_data(fort14_data, fort63_data, fort64_data, num, sum, output_path):
    for i in range(num):
        path = os.path.join(output_path, f'{i}.txt')
        with open(path, 'w') as file:
            file.write(str(sum) + '\n')
            file.write('X' + ' ' + 'Y' + ' ' + 'H' + ' ' + 'P' + ' ' + 'U' + ' ' + 'V' + "\n")
            for j in range(sum):
                fort14_data_string = ' '.join(map(str, fort14_data[j][1:4]))
                fort64_data_string = ' '.join(map(str, fort64_data[i][j][1:3]))
                file.write(fort14_data_string + ' ' + str(fort63_data[i][j][1]) + ' ' + fort64_data_string + "\n")      

def hydrodynamic(fort14_data_path, fort63_data_path, fort64_data_path, raw_output_path, visualization_output_path):

    if not os.path.exists(raw_output_path):
        os.makedirs(raw_output_path)
    if not os.path.exists(visualization_output_path):
        os.makedirs(visualization_output_path)

    fort14_data, sum = read_14_data(fort14_data_path)
    fort63_data = read_63_64_data(fort63_data_path, sum)
    fort64_data = read_63_64_data(fort64_data_path, sum)
    mergeOutput_data(fort14_data, fort63_data, fort64_data, len(fort63_data), sum, raw_output_path)
    
    # Generate rendering resource
    empty_stations = init_station(fort14_data_path, sum)
    stations = update_station(fort64_data_path, empty_stations, sum)
    write_bin(stations, visualization_output_path)
    
def generate_rendering_resource(segment_name: str, year: str, set_name: str, case_name: str, set_path: str, input_path: str, output_path: str, boundary_path: str):

    platform = util.get_os()
    if platform == 'Windows':
        return

    files = []
    for i in range(26):
        files.append({
            "in": f"{i}.txt",
            "sn": f"{i}"
        })
        
    boundary_file = os.path.join(config.DIR_RESOURCE, boundary)
    
    description_dict = {
        "input_url": input_path,
        "output_url": output_path,
        "resource_url": f'hydrodynamic/{segment_name}/{year}/{set_name}/{case_name}/renderResource/',
        "column_num": 6,
        "headings": ["X", "Y", "H", "P", "U", "V"],
        "coordinates": ["X", "Y"],
        "velocities": ["U", "V"],
        "dem": ["P", "H"],
        "attribute": "",
        "resolution": 1024,
        "outlier": -99999.0,
        "vector_mask": boundary_file,
        "source_space": "2437",
        "target_space": ["mapbox"],
        "files": files
    }

    # 指定要写入的JSON文件名
    json_filename = os.path.join(output_path, 'meta.json')

    # 将字典写入JSON文件
    with open(json_filename, 'w', encoding='utf-8') as json_file:
        json.dump(description_dict, json_file, ensure_ascii=False, indent=4)
    
    textureBuilder = ''
    platform = util.get_os()
    if platform == 'Windows':
        textureBuilder = os.path.join(config.DIR_MODEL, 'flowFieldTextureBuilder', 'build', 'windows', 'TextureBuilder')
    elif platform == 'MacOS':
        textureBuilder = os.path.join(config.DIR_MODEL, 'flowFieldTextureBuilder', 'build', 'macos', 'TextureBuilder')
    else:
        textureBuilder = os.path.join(config.DIR_MODEL, 'flowFieldTextureBuilder', 'build', 'TextureBuilder')
        
        
    command = [
        textureBuilder,
        json_filename
    ]
    proc = subprocess.Popen(command)
    return_code = proc.wait()
    
    if return_code == 0:
        print("Generate Flow Field Texture successfully.", flush=True)
    else:
        print(f"Flow Field Texture Builder ended with return code: {return_code}", flush=True)

def generate_hydrodynamic_resource(segment: str, year: str, set: str, name: str, temp: bool, boundary: str):
    
    set_path = os.path.join(config.DIR_RESOURCE_HYDRODYNAMIC, segment, year, set)
    mapped_path = os.path.join(set_path, name)
    raw_output_path = os.path.join(mapped_path, 'raw/')
    visualization_output_path = os.path.join(mapped_path, 'renderResource/') 

    fort14_data_path = os.path.join(mapped_path, 'fort.14')
    fort63_data_path = os.path.join(mapped_path, 'fort.63')
    fort64_data_path = os.path.join(mapped_path, 'fort.64')

    hydrodynamic(fort14_data_path, fort63_data_path, fort64_data_path, raw_output_path, visualization_output_path)
    generate_rendering_resource(segment, year, set, name, set_path, raw_output_path, visualization_output_path, boundary)
    
    with open(os.path.join(mapped_path, 'description.json'), 'w', encoding='utf-8') as file:
        json.dump({
                'temp': temp,
            }, file, indent=4, ensure_ascii=False)
    
    # os.remove(fort14_data_path)
    # os.remove(fort63_data_path)
    # os.remove(fort64_data_path)
    
    keep_files = {'description.json'}
    keep_folders = {'raw', 'renderResource'}
    
    for item in os.listdir(mapped_path):
        item_path = os.path.join(mapped_path, item)
        
        if os.path.isfile(item_path) and item not in keep_files:
            os.remove(item_path)
        
        elif os.path.isdir(item_path) and item not in keep_folders:
            shutil.rmtree(item_path)
            
    util.update_size(config.DIR_STORAGE_LOG, util.get_folder_size_in_gb(mapped_path))
    
#######################################################################################

if __name__ == '__main__':
    
    segment = sys.argv[1]
    year = sys.argv[2]
    set = sys.argv[3]
    name = sys.argv[4]
    v5 = sys.argv[5]
    boundary = sys.argv[6]
    
    if v5 == 'False':
        temp = False
    else:
        temp = True
    
    generate_hydrodynamic_resource(segment, year, set, name, temp, boundary)
    