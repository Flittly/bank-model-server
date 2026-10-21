//
// Created by Yucheng Soku on 2023/7/14.
//

#ifndef FLOWFIELD_BUILDER_FLOWFIELD_H
#define FLOWFIELD_BUILDER_FLOWFIELD_H

#include <string>
#include <iostream>
#include <cstdlib>
#include <fstream>
#include <vector>
#include <cstdio>

#include "nlohmann/json.hpp"
using Json = nlohmann::json;
#include <ogr_core.h>
#include "gdal_utils.h"
#include "ogrsf_frmts.h"
#include "gdal_priv.h"
#include "ogr_geometry.h"
#include "gridxtyp.h"
#include "scatdata.h"
#include "surfgrid.h"
#include "xpand.h"
#include "contour.h"

enum Platform_Types {
    Mapbox = 0,
    CESIUM,
    EPSG,

    PLATFORM_COUNT
};

struct FlowFiledFile
{
    std::string in; // Input File Name
    std::string sn; // Serial Number
};

union Texel {
    float f32;
    uint8_t rgba8[4];
};

class FlowField
{
private:
    std::string m_descriptionPath;
    std::string m_inUrl;
    std::string m_outUrl;
    std::string m_resourceUrl;
    // std::string m_caseID;
    // std::string m_casePath;
    std::string m_xName;
    std::string m_yName;
    std::string m_uName;
    std::string m_vName;
    std::string m_pName;
    std::string m_hName;
    std::string m_vMask;
    int m_attributeIndex[6];
    int m_headingNum;
    int m_columnNum;
    double m_outlier;
    double m_resolution;
    double m_boundary[4];
    double m_flowVelocityRange[4];
    double m_flowPRange[2];
    double m_flowHRange[2];
    double m_terrainRange[2];

    std::string m_sourceSpace;
    std::vector<std::string> m_targetSpaces;

    GDALDataset* m_ds;
    // GDAL 3.9 起 OGRLayer::GetSpatialRef() 返回 const 指针，必须跟着加 const，
    // 否则在 GDAL 3.12（OSGeo4W 现装版本）下 MSVC 直接报 C2440。exportToWkt 本身是 const，
    // 所以这里加 const 不影响任何调用。
    const OGRSpatialReference* m_sRef;
    OGRGeometry* m_vectorMask;
    char** m_maskOptions;
    std::vector<char> m_bmpMask;

    std::vector<FlowFiledFile> m_files;
    Json m_result;

public:
    std::string urlPath;
    std::string resultPath;

private:
    void updateFlowUV(double u, double v);
    void updateFlowPH(double p, double h);
    bool toTextureGRID_elevation(ScatData& sc_p, ScatData& sc_h, SurfaceGrid& grd_p, SurfaceGrid& grd_h, const char* textureFile, const char* maskFile, bool isFirst);
    bool toTextureGRID_uv(ScatData& sc_u, ScatData& sc_v, SurfaceGrid& grd_u, SurfaceGrid& grd_v, const char* textureFile, const char* maskFile, bool isFirst);
    void MercatorCoordsFromLonLat(OGRCoordinateTransformation* poCT, double& lon, double& lat);
    void EPSGFromLonLat(OGRCoordinateTransformation* poCT, double& lon, double& lat);
    void CesiumWgs84ToCartesian(OGRCoordinateTransformation* poCT, double& lon, double& lat, double& altitude);
    OGRGeometry* createVectorMask();
    void buildBmpMask(int width, int height);
    void readBmpMask();
    void closeDataSet();

public:
    explicit FlowField(const char* descriptionPath);
    ~FlowField();
    static FlowField* Create(const char* descriptionPath);
    void preprocess();
    void buildProjectionTexture(int width, int height, size_t index);
    void buildTextures();
    void finish();
    size_t getTargetSpaceNum() {return m_targetSpaces.size();}
};

#endif //FLOWFIELD_BUILDER_FLOWFIELD_H
