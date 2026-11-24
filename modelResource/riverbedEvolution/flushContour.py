import os
import sys

module_path = os.path.abspath(os.path.join(os.getcwd()))
if module_path not in sys.path:
    sys.path.append(module_path)

import util
import model
import config
from osgeo import gdal, ogr, osr

def flush_all_contour(flush_all_path, output_path):
    
    if not os.path.exists(output_path):
        os.makedirs(output_path)
  
    # 定义输入和输出文件的完整路径
    flush_all_path = os.path.join(flush_all_path, 'flush.tif')
    output_shp_path = os.path.join(output_path, 'flush_contour.shp')
    output_json_path = os.path.join(output_path, 'flush_contour.geojson')

    # 打开DEM文件
    dem_dataset = gdal.Open(flush_all_path, gdal.GA_ReadOnly)
    if dem_dataset is None:
        raise Exception('无法打开DEM文件')

    band = dem_dataset.GetRasterBand(1)

    # 创建输出Shapefile的驱动
    shapefile_driver = ogr.GetDriverByName('ESRI Shapefile')

    # 创建Shapefile数据集
    shapefile_dataset = shapefile_driver.CreateDataSource(output_shp_path)

    srs = osr.SpatialReference()
    srs.ImportFromWkt(dem_dataset.GetProjection())

    # 创建等高线图层
    layer = shapefile_dataset.CreateLayer('contours', srs, ogr.wkbLineString)

    # 定义等高线字段
    field_name = 'elevation'
    field_defn = ogr.FieldDefn(field_name, ogr.OFTReal)
    layer.CreateField(field_defn)

    # 定义等高线间隔
    contour_interval = 5.0
    # 基准等高线
    contour_base = float(band.ComputeRasterMinMax()[0])

    # 调用gdal.ContourGenerate生成等高线
    gdal.ContourGenerate(
        band,
        contour_interval,  # 等高线间隔
        contour_base,  # 基准高度，确保是浮点数
        [],  # 传递一个空的序列
        False,  # 不使用NoData值
        -9999.0,  # NoData值，确保是浮点数
        layer,
        0,
        0
    )

    # 不需要关闭数据集，因为DataSource会自动保存并关闭
    del dem_dataset
    del shapefile_dataset
    
    util.convert_shp_to_geojson(output_shp_path, output_json_path)

def flush_contour(flush_all_contour_path, region_geometry, output_path):
    
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    flush_all_contour_path = os.path.join(flush_all_contour_path, 'flush_contour.shp')
    output_shp_path = os.path.join(output_path, 'flush_contour.shp')
    output_json_path = os.path.join(output_path, 'flush_contour.geojson')
    
    # 初始化一个列表来存储所有多边形的坐标
    polygons_coordinates = []

    # 遍历features
    for feature in region_geometry['features']:
        # 检查geometry类型是否为'Polygon'
        if feature['geometry']['type'] == 'Polygon':
            # 提取坐标
            coordinates = feature['geometry']['coordinates'][0]  # 取外环的坐标
            polygons_coordinates.extend(coordinates)

    shp_driver = ogr.GetDriverByName('ESRI Shapefile')
    shapefile = shp_driver.Open(flush_all_contour_path)
    # shapefile = ogr.Open(flush_all_contour_path)
    layer = shapefile.GetLayer()

    # 获取投影信息
    spatial_ref = layer.GetSpatialRef()

    # 定义未投影坐标的空间参考系统（WGS84）
    source_srs = osr.SpatialReference()
    source_srs.ImportFromEPSG(4326)  # EPSG:4326 是 WGS84 的代码

    # 创建坐标转换对象
    transform = osr.CoordinateTransformation(source_srs, spatial_ref)

    # 进行坐标转换  
    # 创建多边形几何对象
    ring = ogr.Geometry(ogr.wkbLinearRing)
    for lon, lat in polygons_coordinates:
        point = ogr.Geometry(ogr.wkbPoint)
        point.AddPoint(lat, lon)
        point.Transform(transform)
        ring.AddPoint(point.GetX(), point.GetY())

    polygon = ogr.Geometry(ogr.wkbPolygon)
    polygon.AddGeometry(ring)

    # 创建一个新的图层来存储裁剪结果
    clipped_datasource  = ogr.GetDriverByName('ESRI Shapefile').CreateDataSource(output_shp_path)
    clipped_layer = clipped_datasource.CreateLayer('contour', geom_type=ogr.wkbLineString, srs=spatial_ref)

    # 复制字段定义
    layer_defn = layer.GetLayerDefn()
    for i in range(layer_defn.GetFieldCount()):
        field_defn = layer_defn.GetFieldDefn(i)
        clipped_layer.CreateField(field_defn)

    # 使用 multi_polygon 进行裁剪
    for feature in layer:
        geom = feature.GetGeometryRef()
        if geom.Intersects(polygon):
            clipped_geom = geom.Intersection(polygon)
            if clipped_geom:
                new_feature = ogr.Feature(clipped_layer.GetLayerDefn())
                new_feature.SetGeometry(clipped_geom)
                for i in range(feature.GetFieldCount()):
                    new_feature.SetField(i, feature.GetField(i))
                clipped_layer.CreateFeature(new_feature)
                new_feature = None  # 清理内存
            
    del shapefile
    del clipped_layer
    del clipped_datasource
    
    util.convert_shp_to_geojson(output_shp_path, output_json_path)

##########################################################################################

@model.model_status_controller_sync
def run_global_contour_mcr(mcr: model.ModelCaseReference):
    
    flush_all_path = os.path.join(global_flush_mcr.directory, 'result')
    output_path = os.path.join(mcr.directory, 'result')
    
    flush_all_contour(flush_all_path, output_path)
    
    return {
        
        'case-id': mcr.id,
        'raw-shp': 'flush_contour.shp',
        'visualization-geojson': 'flush_contour.geojson'
    }

@model.model_status_controller_sync
def run_region_contour_mcr(mcr: model.ModelCaseReference):
    
    flush_all_contour_path = os.path.join(global_contour_mcr.directory, 'result') 
    region_geometry = mcr.request_json['region-geometry']
    output_path = os.path.join(mcr.directory, 'result')
    flush_contour(flush_all_contour_path, region_geometry, output_path)
    
    return {
        
        'case-id': mcr.id,
        'raw-shp': 'flush_contour.shp',
        'visualization-geojson': 'flush_contour.geojson'
    }

##########################################################################################

NAME = 'Flush Contour'

CATEGORY = 'Riverbed Evolution'

CATEGORY_ALIAS = 're'
 
def PARSING(self, request_json: dict, model_path: str, other_dependent_ids: list[str]=[]):
    
    region_json = request_json
    global_json = {
        'bench-id': region_json['bench-id'],
        'ref-id': region_json['ref-id'],
        'region-geometry': 'NONE'
    }
    global_flush_mcr = model.launcher.fetch_model_from_API(config.API_RE_REGION_FLUSH).run(global_json, other_dependent_ids)
    
    if region_json['region-geometry'] == 'NONE':
        
        global_contour_mcr = model.ModelCaseReference.create(config.API_RE_FLUSH_CONTOUR, global_json, NAME, model_path, other_dependent_ids + [global_flush_mcr.id])
        return [global_flush_mcr, global_contour_mcr]
    else:
        global_contour_mcr = model.launcher.fetch_model_from_API(config.API_RE_FLUSH_CONTOUR).run(global_json, other_dependent_ids)
        region_contour_mcr = model.ModelCaseReference.create(config.API_RE_FLUSH_CONTOUR, region_json, NAME, model_path, other_dependent_ids + [global_flush_mcr.id, global_contour_mcr.id])
    
        return [global_flush_mcr, global_contour_mcr, region_contour_mcr]

def RESPONSING(self, core_mcr: model.ModelCaseReference, default_pre_mcrs: list[model.ModelCaseReference], other_pre_mcrs: list[model.ModelCaseReference]):
    
    return core_mcr.make_response({
                    'case-id': 'TEMPLATE',
                    'raw-shp': 'NONE',
                    'visualization-geojson': 'NONE'
                })

##########################################################################################

if __name__ == '__main__':
    
    v1 = sys.argv[1]    # global flush model case reference id
    v2 = sys.argv[2]    # global contour model case reference id
    v3 = sys.argv[3] if len(sys.argv) == 4 else None    # region contour model case reference id
    
    model.require_cases([v1, v2] + ([v3] if v3 else []), NAME)

    global_flush_mcr = model.ModelCaseReference.open_case(v1)
    global_contour_mcr = model.ModelCaseReference.open_case(v2)
    
    if v3 is None:
        # Run model case (Global Flush Contour)
        run_global_contour_mcr(global_contour_mcr)
    else:
        # Run model case (Region Flush Contour)
        region_contour_mcr = model.ModelCaseReference.open_case(v3)
        run_region_contour_mcr(region_contour_mcr)
    
    
