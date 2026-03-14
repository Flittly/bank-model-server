import os
import sys

module_path = os.path.abspath(os.path.join(os.getcwd()))
if module_path not in sys.path:
    sys.path.append(module_path)

import math
import json
import model
import config
import numpy as np
from PIL import Image
from osgeo import gdal
from osgeo import ogr
from osgeo import osr
from util.rustfs import resolve_resource_path

def get_bound_coords(output_tif: str) -> dict:
    list_coords = {}
    ds: gdal.Dataset = gdal.Open(output_tif)
    info = ds.GetGeoTransform()
    width = ds.RasterXSize
    height = ds.RasterYSize
    offset_width = width*info[1]
    offset_height = height*info[5]
    ul_coord = [info[0], info[3]]
    lr_coord = [info[0] + offset_width, info[3]+offset_height]
    list_coords = {'ul': [ul_coord[0], ul_coord[1]],
                   'lr': [lr_coord[0], lr_coord[1]], }

    return list_coords

  
def clamp(a, _min, _max):
    
    return max(min(a, _max), _min)

def clamp_vec3(array, min, max):
    
    return [ clamp(array[0], min, max), clamp(array[1], min, max), clamp(array[2], min, max) ]

def jet_color_map(value):
    
    value = int(value)

    r, g, b = 0, 0, 0
    
    if value <= 31:
        g, b, r = clamp_vec3([128 + 4 * value, 0, 0], 0, 255)
    elif value <= 95:
        g, b, r = clamp_vec3([255, 4 * (value - 32), 0], 0, 255)
    elif value <= 159:
        g, b, r = clamp_vec3([255 - 4 * (value - 96), 255, 0], 0, 255)
    elif value <= 223:
        g, b, r = clamp_vec3([0, 255, 4 * (value - 160)], 0, 255)
    else:
        g, b, r = clamp_vec3([0, 255 - 4 * (value - 224), 128 + 4 * (value - 224)], 0, 255)
    return r, g, b, 255  # 确保颜色值是RGBA格式

def get_tif_attributes(file_path):
    
    # 打开 TIFF 文件
    dataset = gdal.Open(file_path)
    
    if not dataset:
        raise FileNotFoundError(f"Cannot open file {file_path}")

    # 获取第一个波段
    band = dataset.GetRasterBand(1)
    
    # 获取波段的最小值和最大值
    min_value, max_value = band.ComputeRasterMinMax(True)

    nodata = band.GetNoDataValue()
    
    return min_value, max_value, nodata

def color_mapping(value, min, max, noData):
    
    r: int = 0
    g: int = 0
    b: int = 0
    a: int = 255

    if int(value) == noData or math.isnan(value):
        a = 0
        return r, g, b, a
    
    normal_value = (value - min) / (max - min)
    r, g, b, a = jet_color_map(normal_value * 255.0)

    return r, g, b, a

def tif_to_colorPng(input_tif, output_png):
    
    with Image.open(input_tif) as image:
        image_array = np.array(image).astype(np.float32)
        width, height = image.size

    min,max,nodata = get_tif_attributes(input_tif)
    rgba_array = np.zeros((height, width, 4), dtype=np.uint8)

    for y in range(height):
        for x in range(width):
            pixel = image_array[y, x]
            r, g, b, a = color_mapping(pixel, min, max, nodata)
            # rgba_array[y, x] = np.array([r, g, b, a]).astype(np.uint8)
            rgba_array[y, x] = [r, g, b, a]

    output_image = Image.fromarray(rgba_array, 'RGBA')
    output_image.save(output_png)


def geo2lonlat(prj, x, y):
    prosrs = osr.SpatialReference()
    prosrs.ImportFromWkt(prj)
    geosrs = prosrs.CloneGeogCS()
    ct = osr.CoordinateTransformation(prosrs, geosrs)
    coords = ct.TransformPoint(x, y)
    return coords[:2]


def delete_temporary_files() -> None:
    driver = ogr.GetDriverByName('ESRI Shapefile')
    driver.DeleteDataSource('temp_shp.shp')
    os.remove('temp_tif.tif')


def compute_volume(depth, region_geometry, in_raster_path, output_path):

    if not os.path.exists(output_path):
        os.makedirs(output_path)
    output_coords = os.path.join(output_path, 'volume.json')
    volume_path = os.path.join(output_path, 'volume.txt')
    result_path = os.path.join(output_path, 'volume.tif')
    visual_path = os.path.join(output_path, 'volume.png')

    # 获取栅格图像各种元数据
    ds_raster: gdal.Dataset = gdal.Open(in_raster_path)
    prj = ds_raster.GetProjection()
    geo = ds_raster.GetGeoTransform()
    area = abs(geo[1] * geo[5])

    # 地理坐标转投影坐标
    prosrs: osr.SpatialReference = osr.SpatialReference()
    prosrs.ImportFromWkt(prj)
    geosrs: osr.SpatialReference = prosrs.CloneGeogCS()
    # 设置地图坐标策略
    prosrs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    geosrs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    ct: osr.CoordinateTransformation = osr.CoordinateTransformation(
        geosrs, prosrs)
    

    # 初始化一个列表来存储所有多边形的坐标
    coords_geo = []

    # 遍历features
    for feature in region_geometry['features']:
        # 检查geometry类型是否为'Polygon'
        if feature['geometry']['type'] == 'Polygon':
            # 提取坐标
            coordinates = feature['geometry']['coordinates'][0]  # 取外环的坐标
            coords_geo.extend(coordinates)

    # 坐标转换
    coords_pro = list()
    for coords in coords_geo:
        coords_trans = ct.TransformPoint(
            float(coords[0]), float(coords[1]))
        coords_pro.append(coords_trans)
    coords_pro.append(coords_pro[0])
    ds_raster = prosrs = geosrs = coords_geo = ct = None

    # 根据坐标点建立掩膜 geometry
    ring: ogr.Geometry = ogr.Geometry(ogr.wkbLinearRing)
    for coords in coords_pro:
        ring.AddPoint(coords[0], coords[1])
    poly: ogr.Geometry = ogr.Geometry(ogr.wkbPolygon)
    poly.AddGeometry(ring)
    ring = coords = None

    # 根据掩膜 geometry 建立 shp 文件
    driver_shp: ogr.Driver = ogr.GetDriverByName('ESRI Shapefile')
    ds_shp: ogr.DataSource = driver_shp.CreateDataSource(
        'temp_shp.shp')
    srs: osr.SpatialReference = osr.SpatialReference()
    srs.ImportFromWkt(prj)
    layer: ogr.Layer = ds_shp.CreateLayer('polygon', srs, ogr.wkbPolygon)
    layer.CreateField(ogr.FieldDefn('ID', ogr.OFTString))
    defn: ogr.FeatureDefn = layer.GetLayerDefn()
    feature: ogr.Feature = ogr.Feature(defn)
    geom: ogr.Geometry = ogr.CreateGeometryFromWkt(poly.ExportToWkt())
    feature.SetGeometry(geom)
    layer.CreateFeature(feature)
    poly = driver_shp = ds_shp = srs = layer = defn = feature = geom = None

    # 根据 shp 文件裁剪栅格图像
    # 之前 ds 必须赋 None, 否则一直占用, 读不出来
    gdal.Warp('temp_tif.tif',
              in_raster_path,
              cutlineDSName='temp_shp.shp',
              cropToCutline=True,
              dstNodata=-9999,
              )

    # 读取栅格图像原始数据
    ds_raster: gdal.Dataset = gdal.Open('temp_tif.tif')
    band: gdal.Band = ds_raster.GetRasterBand(1)
    min = band.ComputeRasterMinMax()[0]
    max = band.ComputeRasterMinMax()[1]
    # 栅格图像输出为 numpy 数组
    tif_array = band.ReadAsArray()
    # 提取符合条件的像素点
    tif_array_extract = np.where(
        (tif_array < -1 * depth) & (tif_array > -9999), tif_array, -9999)
    tif_array_calculate = np.where(
        (tif_array < -1 * depth) & (tif_array > -9999), (tif_array + depth) * area, 0)
    volume = tif_array_calculate.sum() * -1

    projection = ds_raster.GetProjection()
    transform = ds_raster.GetGeoTransform()
    ds_raster = None

    # 保存为tif
    driver = gdal.GetDriverByName('GTiff')
    new_dataset = driver.Create(result_path, tif_array_extract.shape[1], tif_array_extract.shape[0], 1, gdal.GDT_Float32)
    new_dataset.SetProjection(projection)
    new_dataset.SetGeoTransform(transform)
    new_band = new_dataset.GetRasterBand(1)
    new_band.WriteArray(tif_array_extract)

    new_band.SetNoDataValue(-9999)
    new_dataset = None

    tif_to_colorPng(result_path, visual_path)

    bound_coords_pro = get_bound_coords(result_path)
    bound_coords_geo = {}

    for key in bound_coords_pro:
        coord = bound_coords_pro[key]
        a = geo2lonlat(prj, coord[0], coord[1])
        bound_coords_geo[key] = a[::-1]

    bound_coords_geo['ur'] = [
        bound_coords_geo['lr'][0], bound_coords_geo['ul'][1]]
    bound_coords_geo['ll'] = [
        bound_coords_geo['ul'][0], bound_coords_geo['lr'][1]]

    volume_file = open(volume_path, 'w', encoding='utf8')
    volume_file.write(str(volume))
    volume_file.close()

    with open(output_coords, 'w') as file_obj:
        json.dump(bound_coords_geo, file_obj)
    delete_temporary_files()

    return {
        'raw_json':'volume.json',
        'raw_txt':'volume.txt',
        'raw_tif':'volume.tif',
        'visualize-file':'volume.png',
    }

##########################################################################################

@model.model_status_controller_sync
def run_river_volume_mcr(mcr: model.ModelCaseReference):

    dem_path = resolve_resource_path(mcr.request_json['dem-id'])
    depth = mcr.request_json['water-depth']
    region_geometry = mcr.request_json['region-geometry']
    output_path = os.path.join(mcr.directory, 'result')

    compute_volume(depth, region_geometry, dem_path, output_path)
    
    return {
        
        'case-id': mcr.id,
        'extent-json': 'volume.json',
        'raw-tif': 'volume.tif',
        'visualization-png': 'volume.png',
        'volume-summary-txt': 'volume.txt'
    }

##########################################################################################

NAME = 'River Volume'

CATEGORY = 'Riverbed Evolution'

CATEGORY_ALIAS = 're'
 
def PARSING(self, request_json: dict, model_path: str, other_dependent_ids: list[str]=[]):
    
    river_volume_mcr = model.ModelCaseReference.create(config.API_RE_RIVER_VOLUME, request_json, NAME, model_path, other_dependent_ids)
    
    return [river_volume_mcr]

def RESPONSING(self, core_mcr: model.ModelCaseReference, default_pre_mcrs: list[model.ModelCaseReference], other_pre_mcrs: list[model.ModelCaseReference]):
    
    return core_mcr.make_response({
                    'case-id': 'TEMPLATE',
                    'raw-tif': 'NONE',
                    'extent-json': 'NONE',
                    'visualization-png': 'NONE',
                    'volume-summary-txt': 'NONE',
                })

##########################################################################################

if __name__ == '__main__':

    v1 = sys.argv[1]    # river volume model case reference id

    mcr = model.ModelCaseReference.open_case(v1)

    # Run model case (River Volume)
    run_river_volume_mcr(mcr)


