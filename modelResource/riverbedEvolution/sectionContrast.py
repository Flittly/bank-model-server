import os
import sys
import copy
from osgeo import gdal

module_path = os.path.abspath(os.path.join(os.getcwd()))
if module_path not in sys.path:
    sys.path.append(module_path)

import util
import model
import config
import numpy as np
from osgeo import gdal, osr
from util.rustfs import resolve_resource_path

# specific crs change process
def transform_coordinates(coords, source_srs, target_srs):
    # 创建坐标转换对象
    transform = osr.CoordinateTransformation(source_srs, target_srs)
    
    # 转换坐标
    transformed_coords = []
    for coord in coords:
        x, y = coord
        transformed_coords.append(transform.TransformPoint(x, y)[0:2])
    
    return transformed_coords

# get each sample point's pixel coors and the ratio of line and colomn 
def prj2imagexy(dataset, sample_points):
    trans = dataset.GetGeoTransform()
    originX = trans[0]
    originY = trans[3]
    pixelWidth = trans[1]
    pixelHeight = trans[5]

    imgCoors = []

    for sample_point in sample_points:
        row = (sample_point[1] - originY) / pixelHeight
        column = (sample_point[0] - originX) / pixelWidth
        imgCoors.append([int(row), int(column), row % 1, column % 1])
    return imgCoors

# change the origin crs of geojson data to 2437 
def convert_geojson_to_2437(geojson_data, dataset):
    
    # 检查GeoJSON的坐标系
    srs = osr.SpatialReference()
    if geojson_data.get('crs') and geojson_data['crs'].get('properties') and geojson_data['crs']['properties'].get('name'):
        srs.SetFromUserInput(geojson_data['crs']['properties']['name'])
    else:
        # 如果没有坐标系信息，假设是WGS84
        srs.ImportFromEPSG(4326)

    # 获取DEM的坐标系
    target_srs = osr.SpatialReference()
    target_srs.ImportFromWkt(dataset.GetProjection())
    
    # 要素与要素数据集的区别
    if not geojson_data.get('features'):
        features = []
        coordinates = geojson_data['geometry']['coordinates']
        # swarp coordinates
        for coordinate in coordinates:
            tmp = coordinate[0]
            coordinate[0] = coordinate[1]
            coordinate[1] = tmp
        feature = {
            "type": "Feature", 
            "properties": {},
            "geometry": {
                "type": "LineString",
                "coordinates": coordinates
            } 
        }
        features.append(feature)
        geojson_data['features'] = features

    features = []
    for feature in geojson_data['features']:
        # 转换坐标
        transformed_coords = transform_coordinates(feature['geometry']['coordinates'], srs, target_srs)
        
        # 更新Feature的坐标
        feature['geometry']['coordinates'] = transformed_coords
        
        # 添加转换后的Feature到列表
        features.append(feature)
    
    # 更新GeoJSON数据
    geojson_data['features'] = features

    return geojson_data

# get 4 pixel coors to be interpolated
def get_interpolated_pixel_index(image_coors, width, height):

    def clamp(_x, _max):
        return min(_max, max(_x, 0))

    interpolated_pixels = []
    for image_coor in image_coors:
        y = image_coor[0]
        x = image_coor[1]
        ym1 = int(clamp(y - 1, height - 1))
        xm1 = int(clamp(x - 1, width - 1))
        yp1 = int(clamp(y + 1, height - 1))
        xp1 = int(clamp(x + 1, width - 1))

        ratio_y = image_coor[2]
        ratio_x = image_coor[3]
        if ratio_y < 0.5 and ratio_x < 0.5:
            interpolated_pixel = [xm1, ym1, x, ym1, xm1, y, x, y]
        elif ratio_y < 0.5 and ratio_x > 0.5:
            interpolated_pixel = [x, ym1, xp1, ym1, x, y, xp1, y]
        elif ratio_y > 0.5 and ratio_x < 0.5:
            interpolated_pixel = [xm1, y, x, y, xm1, yp1, x, yp1]
        elif ratio_y > 0.5 and ratio_x > 0.5:
            interpolated_pixel = [x, y, xp1, y, x, yp1, xp1, yp1]
        interpolated_pixels.append(interpolated_pixel)
    return interpolated_pixels

# get the elevation of the 4 pixel coors
def get_elevation(interpolated_coors, dataset):
    Z = []
    band = dataset.GetRasterBand(1)
    dem_data = band.ReadAsArray(0, 0, dataset.RasterXSize, dataset.RasterYSize).astype(np.float32)
    dem_data = np.array(dem_data)
    for interpolated_coor in interpolated_coors:
        z1 = dem_data[interpolated_coor[1]][interpolated_coor[0]]
        z2 = dem_data[interpolated_coor[3]][interpolated_coor[2]]
        z3 = dem_data[interpolated_coor[5]][interpolated_coor[4]]
        z4 = dem_data[interpolated_coor[7]][interpolated_coor[6]]
        Z.append([z1, z2, z3, z4])
    return Z

# merged main processes
def get_sample_points_vec3(dataset, section_geometry, num_points=100):
    
    # change the origin crs of geojson data to 2437
    section_geometry_proj = convert_geojson_to_2437(section_geometry, dataset)
    # sample points in the input section
    sample_points, step = util.divide_line_string(section_geometry_proj, num_points)
    # the pixel coors of sample_points
    image_coors = prj2imagexy(dataset, sample_points)
    # the 4 pixel coors to be interpolated
    width = dataset.RasterXSize
    height = dataset.RasterYSize
    interpolated_coors = get_interpolated_pixel_index(image_coors, width, height)
    # the elevation of the 4 pixel coors
    Z = get_elevation(interpolated_coors, dataset)
    # the vec3 of sample points
    sample_points_vec3 = bilinear_interpolation(image_coors, Z, sample_points)
    return sample_points_vec3, sample_points, step

# general linear interpolation
def linear_interpolation(A, B, p):
    return A + p * (B - A)

# bilinear interpolation to get the vec3 of sample_points
def bilinear_interpolation(image_coors, Z, sample_points):

    sample_points_vec3 = []
    for index, image_coor in enumerate(image_coors):
        ratio_y = image_coor[2]
        ratio_x = image_coor[3]
        z_values =  Z[index]
        p1 = 0
        p2 = 0
        if ratio_y < 0.5 and ratio_x < 0.5:
            p1 = 0.5 + ratio_y
            p2 = 0.5 + ratio_x
        elif ratio_y < 0.5 and ratio_x > 0.5:
            p1 = 0.5 + ratio_y
            p2 = ratio_x - 0.5
        elif ratio_y > 0.5 and ratio_x < 0.5:
            p1 = ratio_y - 0.5
            p2 = 0.5 + ratio_x
        elif ratio_y > 0.5 and ratio_x > 0.5:
            p1 = ratio_y - 0.5
            p2 = ratio_x - 0.5
        z_left = linear_interpolation(z_values[0], z_values[2], p1)
        z_right = linear_interpolation(z_values[1], z_values[3], p1)
        z = linear_interpolation(z_left, z_right, p2)
        sample_points_vec3.append([sample_points[index][0], sample_points[index][1], z])
    return sample_points_vec3

def get_contrast_result(raster_arr, section_geometry):

    # deepcopy
    geometry = copy.deepcopy(section_geometry)
    result = []
    # 遍历每个dem
    for i in range(len(raster_arr)):
        values_arr = []
        # 打开dem
        dataset = gdal.Open(raster_arr[i])
        sample_points_vec3, scatters, step = get_sample_points_vec3(dataset, geometry)
        for sample_point_vec3 in sample_points_vec3:
            values_arr.append(sample_point_vec3[2])
        result.append(values_arr)
        del dataset
        geometry = copy.deepcopy(section_geometry)
    return result, step

def section_contrast(benchmark_path, refer_path, raster_path, section_geometry, result_path):

    raster_path = os.path.join(raster_path, 'flush.tif')

    if not os.path.exists(result_path):
        os.makedirs(result_path)
    result_path = os.path.join(result_path, 'section_contrast.txt')

    raster_arr = [benchmark_path, refer_path]
    contrast_result, interval = get_contrast_result(raster_arr, section_geometry)

    dataset = gdal.Open(raster_path)
    
    geometry = copy.deepcopy(section_geometry)
    flush_result, scatters, step = get_sample_points_vec3(dataset, geometry)

    with open(result_path, "w", encoding='utf-8') as file:
        for i in range(2):
            for j in range(len(contrast_result[i])):
                file.write(str(contrast_result[i][j]) + '\n')
            file.write('\n')

        for i in range(len(flush_result)):
            file.write(str(flush_result[i][2]) + '\n')
    del dataset

    return interval

##########################################################################################
    
@model.model_status_controller_sync
def run_section_contrast_mcr(mcr: model.ModelCaseReference):
    
    bench_path = resolve_resource_path(mcr.request_json['bench-id'])
    ref_path = resolve_resource_path(mcr.request_json['ref-id'])
    global_output_path = os.path.join(global_mcr.directory, 'result')
    section_geometry = mcr.request_json['section-geometry']
    output_path = os.path.join(mcr.directory, 'result')
    
    interval = section_contrast(bench_path, ref_path, global_output_path, section_geometry, output_path)
    
    return {
        
        "case-id": mcr.id,
        "raw-txt": 'section_contrast.txt',
        "interval": interval
    }

##########################################################################################

NAME = 'Section Contrast'

CATEGORY = 'Riverbed Evolution'

CATEGORY_ALIAS = 're'
 
def PARSING(self, request_json: dict, model_path: str, other_dependent_ids: list[str]=[]):
    
    region_json = request_json
    global_json = {
        'bench-id': region_json['bench-id'],
        'ref-id': region_json['ref-id'],
        'region-geometry': 'NONE'
    }
    global_mcr = model.launcher.fetch_model_from_API(config.API_RE_REGION_FLUSH).run(global_json, other_dependent_ids)
    region_mcr = model.ModelCaseReference.create(config.API_RE_SECTION_CONTRAST, region_json, NAME, model_path, other_dependent_ids + [global_mcr.id])
    
    return [global_mcr, region_mcr]

def RESPONSING(self, core_mcr: model.ModelCaseReference, default_pre_mcrs: list[model.ModelCaseReference], other_pre_mcrs: list[model.ModelCaseReference]):
    
    return core_mcr.make_response({
                    'case-id': 'TEMPLATE',
                    'raw-txt': 'NONE',
                    'interval': 'NONE'
                })

##########################################################################################

if __name__ == '__main__':
    
    v1 = sys.argv[1]    # global model case reference id
    v2 = sys.argv[2]    # region model case reference id
    
    global_mcr = model.ModelCaseReference.open_case(v1)
    region_mcr = model.ModelCaseReference.open_case(v2)
    
    # Run model case (Section Contrast)
    run_section_contrast_mcr(region_mcr, global_mcr.id)
