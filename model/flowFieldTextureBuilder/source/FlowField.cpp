//
// Created by Yucheng Soku on 2023/7/14.
//
#include "FlowField.h"

#include "cesium_cartesian3.h"
#define STB_IMAGE_IMPLEMENTATION
#include "stb_image.h"
#define STB_IMAGE_WRITE_IMPLEMENTATION
#include "stb_image_write.h"


void FlowField::updateFlowUV(double u, double v)
{
    // update uMin
    m_flowVelocityRange[0] = u < m_flowVelocityRange[0] ? u : m_flowVelocityRange[0];
    // update uMax
    m_flowVelocityRange[2] = u > m_flowVelocityRange[2] ? u : m_flowVelocityRange[2];
    // update vMin
    m_flowVelocityRange[1] = v < m_flowVelocityRange[1] ? v : m_flowVelocityRange[1];
    // update vMax
    m_flowVelocityRange[3] = v > m_flowVelocityRange[3] ? v : m_flowVelocityRange[3];
}

void FlowField::updateFlowPH(double p, double h)
{

    // update pMin
    m_flowPRange[0] = p < m_flowPRange[0] ? p : m_flowPRange[0];
    // update pMax
    m_flowPRange[1] = p > m_flowPRange[1] ? p : m_flowPRange[1];
    // update hMin
    m_flowHRange[0] = h < m_flowHRange[0] ? h : m_flowHRange[0];
    // update hMax
    m_flowHRange[1] = h > m_flowHRange[1] ? h : m_flowHRange[1];
    // update eMin
    m_terrainRange[0] = (p - h) < m_terrainRange[0] ? (p - h) : m_terrainRange[0];
    // update eMax
    m_terrainRange[1] = (p - h) > m_terrainRange[1] ? (p - h) : m_terrainRange[1];
}

bool FlowField::toTextureGRID_elevation(ScatData& sc_p, ScatData& sc_h, SurfaceGrid& grd_p, SurfaceGrid& grd_h, const char* textureFile, const char* maskFile, bool isFirst)
{
    if (!isFirst) return true;

    std::vector<uint8_t> texture;
    texture.resize(grd_p.xsize() * grd_p.ysize(), 0);

    double elevation;
    uint8_t r;
    size_t index;
    int nx = grd_p.xsize();
    int ny = grd_p.ysize();
    bool bmpMaskPass = true;

    for (size_t iy = 0; iy < ny; ++iy)
    {
        for (size_t ix = 0; ix < nx; ++ix)
        {

            bmpMaskPass = m_bmpMask[(ny - iy - 1) * nx * 3 + ix * 3];

            r = 0;
            if (bmpMaskPass && grd_p.z(ix, iy) != m_outlier && grd_h.z(ix, iy) != m_outlier) {
                elevation = (grd_p.z(ix, iy) - grd_h.z(ix, iy) - m_terrainRange[0]) / (m_terrainRange[1] - m_terrainRange[0]);
            } else {
                elevation = 1.0;
            }

            r = uint8_t(elevation * 255.0);
            texture[(ny - iy - 1) * nx + ix + 0] = r;
        }
    }

    m_result["texture_size"]["dem"][0] = nx;  m_result["texture_size"]["dem"][1] = ny;

    int status1 = stbi_write_png(textureFile, nx, ny, 1, texture.data(), 0);
    return status1;
}


bool FlowField::toTextureGRID_uv(ScatData& sc_u, ScatData& sc_v, SurfaceGrid& grd_u, SurfaceGrid& grd_v, const char* textureFile, const char* maskFile, bool isFirst)
{
    std::vector<size_t> validAddress;
    std::vector<uint8_t> texture, mask, rebirthPool;
    texture.resize(grd_u.xsize() * 2 * grd_u.ysize() * 4, 0);
    mask.resize(grd_u.xsize() * grd_u.ysize() * 4, 0);

    double u, v;
    uint8_t r, g, b, a;
    size_t index;
    Texel rgba_u, rgba_v;
    int nx = grd_u.xsize();
    int ny = grd_u.ysize();
    bool bmpMaskPass = true;

    // Test valid addresses
    for (size_t iy = 0; iy < ny; ++iy)
    {
        for (size_t ix = 0; ix < nx; ++ix)
        {

            u = (grd_u.z(ix, iy) - m_flowVelocityRange[0]) / (m_flowVelocityRange[2] - m_flowVelocityRange[0]);
            v = (grd_v.z(ix, iy) - m_flowVelocityRange[1]) / (m_flowVelocityRange[3] - m_flowVelocityRange[1]);

            bmpMaskPass = m_bmpMask[(ny - iy - 1) * nx * 3 + ix * 3];

            if (bmpMaskPass && u != m_outlier && u >= 0.0 && v != m_outlier && v >= 0.0)
            {
                validAddress.emplace_back(ix);
                validAddress.emplace_back(iy);
            }
            else m_bmpMask[(ny - iy - 1) * nx * 3 + ix * 3] = 0;
        }
    }

    for (size_t iy = 0; iy < ny; ++iy)
    {
        for (size_t ix = 0; ix < nx; ++ix)
        {
            rgba_u.f32 = 0.0, rgba_v.f32 = 0.0;
            r = g = b = a = 0;
            u = (grd_u.z(ix, iy) - m_flowVelocityRange[0]) / (m_flowVelocityRange[2] - m_flowVelocityRange[0]);
            v = (grd_v.z(ix, iy) - m_flowVelocityRange[1]) / (m_flowVelocityRange[3] - m_flowVelocityRange[1]);

            bmpMaskPass = m_bmpMask[(ny - iy - 1) * nx * 3 + ix * 3];

            // Mask pass
            if (bmpMaskPass)
            {
                rgba_u.f32 = u, rgba_v.f32 = v;

                r = int(ix) >> 8;
                g = int(ix) & 0xff;
                b = int(iy) >> 8;
                a = int(iy) & 0xff;

                if (isFirst)
                {
                    mask[(ny - iy - 1) * nx * 4 + ix * 4 + 0] = r;
                    mask[(ny - iy - 1) * nx * 4 + ix * 4 + 1] = g;
                    mask[(ny - iy - 1) * nx * 4 + ix * 4 + 2] = b;
                    mask[(ny - iy - 1) * nx * 4 + ix * 4 + 3] = a;
                }
            }
            else
            {
                index = rand() % (validAddress.size() / 2);
                r = int(validAddress[index * 2 + 0]) >> 8;
                g = int(validAddress[index * 2 + 0]) & 0xff;
                b = int(validAddress[index * 2 + 1]) >> 8;
                a = int(validAddress[index * 2 + 1]) & 0xff;

                if (isFirst)
                {
                    mask[(ny - iy - 1) * nx * 4 + ix * 4 + 0] = r;
                    mask[(ny - iy - 1) * nx * 4 + ix * 4 + 1] = g;
                    mask[(ny - iy - 1) * nx * 4 + ix * 4 + 2] = b;
                    mask[(ny - iy - 1) * nx * 4 + ix * 4 + 3] = a;
                }
            }
            
            texture[(ny - iy - 1) * nx * 8 + ix * 8 + 0] = rgba_u.rgba8[0];
            texture[(ny - iy - 1) * nx * 8 + ix * 8 + 1] = rgba_u.rgba8[1];
            texture[(ny - iy - 1) * nx * 8 + ix * 8 + 2] = rgba_u.rgba8[2];
            texture[(ny - iy - 1) * nx * 8 + ix * 8 + 3] = rgba_u.rgba8[3];
            texture[(ny - iy - 1) * nx * 8 + ix * 8 + 4] = rgba_v.rgba8[0];
            texture[(ny - iy - 1) * nx * 8 + ix * 8 + 5] = rgba_v.rgba8[1];
            texture[(ny - iy - 1) * nx * 8 + ix * 8 + 6] = rgba_v.rgba8[2];
            texture[(ny - iy - 1) * nx * 8 + ix * 8 + 7] = rgba_v.rgba8[3];
            
        }
    }

    long ix, iy, tx, ty;
    std::vector<long> filterAddress;
    for (size_t index = 0; index < validAddress.size() / 2; ++index)
    {
        ix = validAddress[index * 2 + 0]; iy = validAddress[index * 2 + 1];
        for (long mx = - 1; mx < 2; ++mx)
        {
            tx = ix + mx;
            if (tx < 0 || tx > nx - 1) continue;
            for (long my = -1; my < 2; ++my)
            {
                ty = iy + my;
                if (ty < 0 || ty > ny - 1) continue;
                if (mx == 0 && my == 0) continue;
                if (m_bmpMask[(ny - ty - 1) * nx * 3 + tx * 3] || m_bmpMask[(ny - ty - 1) * nx * 3 + tx * 3 + 1] == 1) continue;
                else
                {
                    filterAddress.emplace_back(tx);
                    filterAddress.emplace_back(ty);
                    m_bmpMask[(ny - ty - 1) * nx * 3 + tx * 3 + 1] = 1;
                }
            }
        }
    }

    float validCount;
    for (size_t index = 0; index < filterAddress.size() / 2; ++index)
    {
        validCount = 0.0;
        rgba_u.f32 = rgba_v.f32 = 0.0;
        ix = filterAddress[index * 2 + 0];
        iy = filterAddress[index * 2 + 1];
        for (int mx = -1; mx < 2; ++mx)
        {
            tx = ix + mx;
            if (tx < 0 || tx > nx - 1) continue;
            for (int my = -1; my < 2; ++my)
            {
                ty = iy + my;
                if (ty < 0 || ty > ny - 1) continue;

                if (m_bmpMask[(ny - ty - 1) * nx * 3 + tx * 3])
                {
                    validCount += 1.0f / sqrt(float(mx * mx + my * my));
                    rgba_u.f32 += (grd_u.z(tx, ty) - m_flowVelocityRange[0]) / (m_flowVelocityRange[2] - m_flowVelocityRange[0]) / sqrt(float(mx * mx + my * my));
                    rgba_v.f32 += (grd_v.z(tx, ty) - m_flowVelocityRange[1]) / (m_flowVelocityRange[3] - m_flowVelocityRange[1]) / sqrt(float(mx * mx + my * my));
                }
            }
        }
        rgba_u.f32 /= validCount;
        rgba_v.f32 /= validCount;

        texture[(ny - iy - 1) * nx * 8 + ix * 8 + 0] = rgba_u.rgba8[0];
        texture[(ny - iy - 1) * nx * 8 + ix * 8 + 1] = rgba_u.rgba8[1];
        texture[(ny - iy - 1) * nx * 8 + ix * 8 + 2] = rgba_u.rgba8[2];
        texture[(ny - iy - 1) * nx * 8 + ix * 8 + 3] = rgba_u.rgba8[3];
        texture[(ny - iy - 1) * nx * 8 + ix * 8 + 4] = rgba_v.rgba8[0];
        texture[(ny - iy - 1) * nx * 8 + ix * 8 + 5] = rgba_v.rgba8[1];
        texture[(ny - iy - 1) * nx * 8 + ix * 8 + 6] = rgba_v.rgba8[2];
        texture[(ny - iy - 1) * nx * 8 + ix * 8 + 7] = rgba_v.rgba8[3];
    }

    m_result["texture_size"]["flow_field"][0] = nx;  m_result["texture_size"]["flow_field"][1] = ny;
    m_result["texture_size"]["area_mask"][0] = nx;  m_result["texture_size"]["area_mask"][1] = ny;

    int status1 = stbi_write_png(textureFile, nx * 2, ny, 4, texture.data(), 0);
    int status2 = isFirst ? stbi_write_png(maskFile, nx, ny, 4, mask.data(), 0) : true;
    return status1 && status2;
}

void FlowField::MercatorCoordsFromLonLat(OGRCoordinateTransformation* poCT, double& lon, double& lat)
{
    poCT->Transform(1, &lat, &lon);

    lon = (180.0 + lon) / 360.0;
    lat = (180.0 - (180.0 / M_PI * log(tan(M_PI / 4.0 + lat * M_PI / 360.0)))) / 360.0;
}

void FlowField::EPSGFromLonLat(OGRCoordinateTransformation* poCT, double& lon, double& lat)
{
    poCT->Transform(1, &lat, &lon);
}

void FlowField::CesiumWgs84ToCartesian(OGRCoordinateTransformation* poCT, double& lon, double& lat, double& altitude)
{

    poCT->Transform(1, &lat, &lon);

    auto cartesian = new Cartesian3(lon, lat);
    cartesian = Cartesian3::FromDegrees(lon, lat, altitude, cartesian);
    lon = cartesian->x;
    lat = cartesian->y;
    altitude = cartesian->z;
}

OGRGeometry* FlowField::createVectorMask()
{
    GDALAllRegister();

    m_ds = (GDALDataset*) GDALOpenEx((m_vMask).c_str(), GDAL_OF_VECTOR, NULL, NULL, NULL);
    if (m_ds == NULL)
    {
        std::cout << "No shp file was founded, vector mask function will not be activated." << std::endl;
        return nullptr;
    }

    OGRLayer *vLayer;
    vLayer = m_ds->GetLayer(0);
    OGRFeature* vFeature = vLayer->GetFeature(0);
    OGRGeometry* mask = vFeature->GetGeometryRef()->clone();

    char* pszWKT = NULL;
    m_sRef = vLayer->GetSpatialRef();
    m_sRef->exportToWkt(&pszWKT);

    // Update boundary
    OGREnvelope envelope;
    mask->getEnvelope(&envelope);
    m_boundary[0] = envelope.MinX;
    m_boundary[1] = envelope.MinY;
    m_boundary[2] = envelope.MaxX;
    m_boundary[3] = envelope.MaxY;

    // set vector mask options
    m_maskOptions = NULL;

    m_maskOptions = CSLAddString(m_maskOptions, "-i");

    m_maskOptions = CSLAddString(m_maskOptions, "-a");
    m_maskOptions = CSLAddString(m_maskOptions, "Id");

    m_maskOptions = CSLAddString(m_maskOptions, "-l");
    m_maskOptions = CSLAddString(m_maskOptions, vLayer->GetName());

    m_maskOptions = CSLAddString(m_maskOptions, "-init");
    m_maskOptions = CSLAddString(m_maskOptions, "255");

    m_maskOptions = CSLAddString(m_maskOptions, "-of");
    m_maskOptions = CSLAddString(m_maskOptions, "BMP");

    m_maskOptions = CSLAddString(m_maskOptions, "-a_srs");
    m_maskOptions = CSLAddString(m_maskOptions, pszWKT);

    m_maskOptions = CSLAddString(m_maskOptions, "-ot");
    m_maskOptions = CSLAddString(m_maskOptions, "byte");

    return mask;
}

void FlowField::buildBmpMask(int width, int height)
{
    if (m_ds == NULL)
        return;

    // m_maskOptions 是成员变量，由 createVectorMask() 建一次、跨帧复用；而本函数每帧都被调用。
    // 原实现直接往 m_maskOptions 上追加 "-ts w h"，于是从第 2 帧开始参数表里就有两个 -ts，
    // GDAL 报 "Duplicate argument -ts" / "Size and resolution are missing" 并拒绝栅格化，
    // mask.bmp 根本没被写出来 ⇒ readBmpMask() 的 stbi_load 返回 null（未定义行为）
    // ⇒ m_vectorMask 退化成"每个陆域格随机指一个水域格"，粒子会飘上岸。
    // 改法：本帧在副本上追加 -ts，交给 GDALRasterizeOptionsNew 之后就释放，绝不动成员。
    // （GDALRasterizeOptionsNew 只读一遍参数表，返回的 options 不持有 papszArgv。）
    char** argv = CSLDuplicate(m_maskOptions);
    argv = CSLAddString(argv, "-ts");
    argv = CSLAddString(argv, std::to_string(width).c_str());
    argv = CSLAddString(argv, std::to_string(height).c_str());

    auto options = GDALRasterizeOptionsNew(argv, NULL);
    CSLDestroy(argv);

    int error = 0;
    auto bmpPath = m_outUrl + "mask.bmp";
    auto ds = GDALRasterize(bmpPath.c_str(), NULL, m_ds, options, &error);
    if (ds == NULL)
    {
        std::cerr << "[FlowField] 矢量掩膜栅格化失败（mask.bmp 未生成），"
                  << "本轮掩膜语义将退化为随机水域格。GDAL error=" << error << std::endl;
    }
    else
    {
        GDALClose(ds);
    }
    GDALRasterizeOptionsFree(options);
    readBmpMask();

    // 下游（toTextureGRID_uv / toTextureGRID_elevation / Xpand）全部按 (ny-iy-1)*nx*3+ix*3
    // 直接下标访问 m_bmpMask，不做长度检查。栅格化失败时这里必须补一张等尺寸的"全通过"缓冲，
    // 否则就是越界读栈内存 —— 表现为随机的粒子上岸/整图空白，且不报错。
    size_t expected = size_t(width) * size_t(height) * 3;
    if (m_bmpMask.size() != expected)
    {
        std::cerr << "[FlowField] 掩膜尺寸异常（得到 " << m_bmpMask.size() << "，期望 "
                  << expected << "），本帧按「不做陆域屏蔽」处理。" << std::endl;
        m_bmpMask.assign(expected, 1);
    }
}

void FlowField::closeDataSet()
{
    GDALClose(m_ds);
}

void FlowField::readBmpMask()
{
    int width, height, n;
    auto bmpPath = m_outUrl + "mask.bmp";
    auto data = stbi_load(bmpPath.c_str(), &width, &height, &n, 0);
    if (data == NULL)
    {
        // 必须显式挡掉：原来直接拿 nullptr 去构造 vector，越界读栈，报错也不出声。
        std::cerr << "[FlowField] 读不到 " << bmpPath
                  << "（stbi_load 返回 null），矢量掩膜不可用。" << std::endl;
        m_bmpMask.clear();
        return;
    }
    m_bmpMask = std::vector<char>(data, data + width * height * n);
    stbi_image_free(data);
}

FlowField::FlowField(const char* descriptionPath)
    // 下面这几个数组**必须**给初值。原来一个都没初始化，于是：
    //   * updateFlowUV / updateFlowPH 里的 "取更小/更大" 是拿未初始化的栈内存去比，
    //     v_max 曾经拿到 1.8e+184 —— 而 v 的归一化是 (v - v_min)/(v_max - v_min)，
    //     分母一大，整张纹理的 v 分量全被压成 0（实测 v_norm 100% 为零）。
    //   * m_boundary 在没给矢量边界时也是未初始化的，而它直接当过滤框用
    //     （`x < m_boundary[0] || x > m_boundary[2] ...` ⇒ continue），
    //     表现是"所有点都被丢掉、却看不出任何报错"。
    // 给成"反向极值 / 空框"之后：极值会从第一个样本长出来；没有边界时等于不做过滤。
    // 有矢量边界时 m_boundary 会被 createVectorMask() 正常覆盖。
    : m_descriptionPath(descriptionPath),
      m_attributeIndex{-1, -1, -1, -1, -1, -1},
      m_boundary{-DBL_MAX, -DBL_MAX, DBL_MAX, DBL_MAX},
      m_flowVelocityRange{DBL_MAX, DBL_MAX, -DBL_MAX, -DBL_MAX},
      m_flowPRange{DBL_MAX, -DBL_MAX},
      m_flowHRange{DBL_MAX, -DBL_MAX},
      m_terrainRange{DBL_MAX, -DBL_MAX},
      m_ds(nullptr),
      m_sRef(nullptr),
      m_vectorMask(nullptr),
      m_maskOptions(nullptr)
{
    std::ifstream iStream(m_descriptionPath);
    Json descriptiveJson;
    iStream >> descriptiveJson;

    m_inUrl = descriptiveJson["input_url"];
    m_outUrl = descriptiveJson["output_url"];
    m_resourceUrl = descriptiveJson["resource_url"];
    // m_caseID = descriptiveJson["case_id"];
    // m_casePath = descriptiveJson["case_path"];
    m_columnNum = descriptiveJson["column_num"];
    m_xName = descriptiveJson["coordinates"][0];
    m_yName = descriptiveJson["coordinates"][1];
    m_uName = descriptiveJson["velocities"][0];
    m_vName = descriptiveJson["velocities"][1];
    m_pName = descriptiveJson["dem"][0];
    m_hName = descriptiveJson["dem"][1];
    m_outlier = descriptiveJson["outlier"];
    m_headingNum = descriptiveJson["headings"].size();
    m_resolution = descriptiveJson["resolution"];
    m_vMask = descriptiveJson["vector_mask"];
    m_sourceSpace = descriptiveJson["source_space"];
    std::cout << m_vMask << std::endl;

    if (m_vMask.compare("") != 0)
        m_vectorMask = createVectorMask();
    resultPath = m_outUrl + "flow_field_description.json";

    for (const std::string& s : descriptiveJson["target_space"])
    {
        m_targetSpaces.emplace_back(s);
    }

    int count = 0;
    for (const std::string& s : descriptiveJson["headings"])
    {
        if (s.compare(m_xName) == 0) m_attributeIndex[0] = count;
        else if (s.compare(m_yName) == 0) m_attributeIndex[1] = count;
        else if (s.compare(m_uName) == 0) m_attributeIndex[2] = count;
        else if (s.compare(m_vName) == 0) m_attributeIndex[3] = count;
        else if (s.compare(m_pName) == 0) m_attributeIndex[4] = count;
        else if (s.compare(m_hName) == 0) m_attributeIndex[5] = count;
        count++;
    }

    for (const auto& f: descriptiveJson["files"])
    {
        FlowFiledFile file {.in = f["in"], .sn = f["sn"]};
        m_files.emplace_back(file);
    }
}

FlowField::~FlowField()
{
}

FlowField* FlowField::Create(const char* descriptionPath)
{
    return new FlowField{descriptionPath};
}

void FlowField::preprocess()
{

    for (const auto& file : m_files)
    {
        std::cout << "==== Preprocess File " << file.in << " ====" << std::endl;
        auto readFile = m_inUrl + file.in;
        std::ifstream fs(readFile);

        int num;
        fs >> num;
        double x, y, u, v, p, h, temp;
        // ScatData scat_u(num);
        // ScatData scat_v(num);

        // Remove headings
        std::string nn;
        for (size_t i = 0; i < m_headingNum; ++i)
            fs >> nn;

        // Set scatters
        for (size_t i = 0; i < num; ++i)
        {
            // Read x, y, u, v attributes
            for (size_t j = 0; j < m_columnNum; ++j)
            {
                if (j == m_attributeIndex[0]) fs >> x;
                else if (j == m_attributeIndex[1]) fs >> y;
                else if (j == m_attributeIndex[2]) fs >> u;
                else if (j == m_attributeIndex[3]) fs >> v;
                else if (j == m_attributeIndex[4]) fs >> p;
                else if (j == m_attributeIndex[5]) fs >> h;
                else fs >> temp;
            }

            ///////////

            if (x < m_boundary[0] || x > m_boundary[2] || y < m_boundary[1] || y > m_boundary[3]) continue;
            
            if (u != m_outlier && v != m_outlier) updateFlowUV(u, v);
            if (p != m_outlier && h != m_outlier) updateFlowPH(p, h);
        }
    }
}

void encodeFloatToDouble(double value, float* high, float* low)
{
    *high = float(value);
    *low = float(value - *high);
}

void FlowField::buildProjectionTexture(int width, int height, size_t index)
{

    OGRSpatialReference target, source;
    source.importFromEPSG(std::stoi(this->m_sourceSpace));
//    target.importFromEPSG(4979);

    Platform_Types platformType;
    if (m_targetSpaces[index] == "mapbox")
    {
        platformType = Mapbox;
        target.importFromEPSG(4326);
    }
    else if (m_targetSpaces[index] == "cesium")
    {
        platformType = CESIUM;
        target.importFromEPSG(4326);
    }
    else
    {
        platformType = EPSG;
        target.importFromEPSG(std::stoi(m_targetSpaces[index]));
    }

    OGRCoordinateTransformation *poCT;
    poCT = OGRCreateCoordinateTransformation(&source, &target);

    Texel rgba_lon, rgba_lat, rgba_alt;
    Texel rgba_lon_high, rgba_lat_high, rgba_alt_high;
    Texel rgba_lon_low, rgba_lat_low, rgba_alt_low;
    int nx = width;
    int ny = height;
    double dx = (m_boundary[2] - m_boundary[0]) / nx;
    double dy = (m_boundary[3] - m_boundary[1]) / ny;
    std::vector<double> xArray, yArray, coordsArray;
    std::vector<uint8_t> texture;
    std::vector<uint8_t> texture2;
    texture.resize(nx * ny * 4 * 3, 0);
    texture2.resize(nx * ny * 4 * 3, 0);

    for (int i = 0; i < nx; ++i)
    {
        xArray.emplace_back(m_boundary[0] + 0.5 * dx + double(i) * dx);
    }
    for (int j = 0; j < ny; ++j)
    {
        yArray.emplace_back(m_boundary[1] + 0.5 * dy + double(j) * dy);
    }

    // Transform boundary (geographic -> projected: _x -> y, _y -> x)
    double _xMin = m_boundary[0], _xMax = m_boundary[2], _yMin = m_boundary[1], _yMax = m_boundary[3], al = 0.0;
    switch (platformType) {
        case Mapbox:
            MercatorCoordsFromLonLat(poCT, _xMin, _yMin);
            MercatorCoordsFromLonLat(poCT, _xMax, _yMax);
            break;
        case CESIUM:
            al = 0.0;
            CesiumWgs84ToCartesian(poCT, _xMin, _yMin, al);
            al = 0.0;
            CesiumWgs84ToCartesian(poCT, _xMax, _yMax, al);
            break;
        case EPSG:
            EPSGFromLonLat(poCT, _xMin, _yMin);
            EPSGFromLonLat(poCT, _xMax, _yMax);
            break;
    }

    m_result["extent"][0] = _xMin;
    m_result["extent"][1] = _yMin;
    m_result["extent"][2] = _xMax;
    m_result["extent"][3] = _yMax;

    // Transform coords (geographic -> projected: _lon -> lat, _lat -> lon)
    double lon, lat;
    int components = 0, sizePerCell = 0;
    for (int j = 0; j < ny; ++j)
    {
        for (int i = 0; i < nx; ++i)
        {
            lon = xArray[i];
            lat = yArray[j];
            al = 0.0;
            switch (platformType) {
                case Mapbox:
                    MercatorCoordsFromLonLat(poCT, lon, lat);
                    components = 2;
                    break;
                case CESIUM:
                    CesiumWgs84ToCartesian(poCT, lon, lat, al);
                    components = 3;
                    break;
                case EPSG:
                    EPSGFromLonLat(poCT, lon, lat);
                    break;
                case PLATFORM_COUNT:
                    break;
            }

            sizePerCell = components * 4;
            encodeFloatToDouble(lon, &rgba_lon_high.f32, &rgba_lon_low.f32);
            encodeFloatToDouble(lat, &rgba_lat_high.f32, &rgba_lat_low.f32);

            rgba_lon.f32 = lon, rgba_lat.f32 = lat, rgba_alt.f32 = al;
            texture[(height - j - 1) * nx * sizePerCell + i * sizePerCell + 0] = rgba_lon_high.rgba8[0];
            texture[(height - j - 1) * nx * sizePerCell + i * sizePerCell + 1] = rgba_lon_high.rgba8[1];
            texture[(height - j - 1) * nx * sizePerCell + i * sizePerCell + 2] = rgba_lon_high.rgba8[2];
            texture[(height - j - 1) * nx * sizePerCell + i * sizePerCell + 3] = rgba_lon_high.rgba8[3];
            texture[(height - j - 1) * nx * sizePerCell + i * sizePerCell + 4] = rgba_lat_high.rgba8[0];
            texture[(height - j - 1) * nx * sizePerCell + i * sizePerCell + 5] = rgba_lat_high.rgba8[1];
            texture[(height - j - 1) * nx * sizePerCell + i * sizePerCell + 6] = rgba_lat_high.rgba8[2];
            texture[(height - j - 1) * nx * sizePerCell + i * sizePerCell + 7] = rgba_lat_high.rgba8[3];

            texture2[(height - j - 1) * nx * sizePerCell + i * sizePerCell + 0] = rgba_lon_low.rgba8[0];
            texture2[(height - j - 1) * nx * sizePerCell + i * sizePerCell + 1] = rgba_lon_low.rgba8[1];
            texture2[(height - j - 1) * nx * sizePerCell + i * sizePerCell + 2] = rgba_lon_low.rgba8[2];
            texture2[(height - j - 1) * nx * sizePerCell + i * sizePerCell + 3] = rgba_lon_low.rgba8[3];
            texture2[(height - j - 1) * nx * sizePerCell + i * sizePerCell + 4] = rgba_lat_low.rgba8[0];
            texture2[(height - j - 1) * nx * sizePerCell + i * sizePerCell + 5] = rgba_lat_low.rgba8[1];
            texture2[(height - j - 1) * nx * sizePerCell + i * sizePerCell + 6] = rgba_lat_low.rgba8[2];
            texture2[(height - j - 1) * nx * sizePerCell + i * sizePerCell + 7] = rgba_lat_low.rgba8[3];
            if (components == 3)
            {
                texture[(height - j - 1) * nx * sizePerCell + i * sizePerCell + 8] = rgba_alt.rgba8[0];
                texture[(height - j - 1) * nx * sizePerCell + i * sizePerCell + 9] = rgba_alt.rgba8[1];
                texture[(height - j - 1) * nx * sizePerCell + i * sizePerCell + 10] = rgba_alt.rgba8[2];
                texture[(height - j - 1) * nx * sizePerCell + i * sizePerCell + 11] = rgba_alt.rgba8[3];
            }
        }
    }
    auto projectionFile = m_outUrl + "projection_high_" + m_targetSpaces[index] + ".png";
    auto projectionFile2 = m_outUrl + "projection_low_" + m_targetSpaces[index] + ".png";
    m_result["projection"].push_back(m_resourceUrl + "projection_high_" + m_targetSpaces[index] + ".png");
    m_result["projection"].push_back(m_resourceUrl + "projection_low_" + m_targetSpaces[index] + ".png");
    m_result["texture_size"]["projection"][0] = width;  m_result["texture_size"]["projection"][1] = height;

    int status1 = stbi_write_png(projectionFile.c_str(), width * components, height, 4, texture.data(), 0);
    int status2 = stbi_write_png(projectionFile2.c_str(), width * components, height, 4, texture2.data(), 0);
}

void FlowField::buildTextures()
{

    if (m_vectorMask)
    {
        std::cout << "Vector Mask is activated!" << std::endl;
    }
    auto first_file = m_files[0];
    bool isFirst = true;
    for (const auto& file : m_files)
    {
        std::cout << "==== Process File " << file.in << " ====" << std::endl;
        auto readFile = m_inUrl + file.in;
        std::ifstream fs(readFile);

        int num;
        fs >> num;
        double x, y, u, v, p, h, temp;
        ScatData scat_u(num);
        ScatData scat_v(num);
        ScatData scat_p(num);
        ScatData scat_h(num);

        // Remove headings
        std::string nn;
        for (size_t i = 0; i < m_headingNum; ++i)
            fs >> nn;

        // Set scatters
        for (size_t i = 0; i < num; ++i)
        {
            // Read x, y, u, v attributes
            for (size_t j = 0; j < m_columnNum; ++j)
            {
                if (j == m_attributeIndex[0]) fs >> x;
                else if (j == m_attributeIndex[1]) fs >> y;
                else if (j == m_attributeIndex[2]) fs >> u;
                else if (j == m_attributeIndex[3]) fs >> v;
                else if (j == m_attributeIndex[4]) fs >> p;
                else if (j == m_attributeIndex[5]) fs >> h;
                else fs >> temp;
            }

            if (x < m_boundary[0] || x > m_boundary[2] || y < m_boundary[1] || y > m_boundary[3]) continue;
            x -= m_boundary[0];
            y -= m_boundary[1];

            if (u != m_outlier && v != m_outlier) {

                scat_u.SetNext(x, y, u);
                scat_v.SetNext(x, y, v);
            }

            if (p != m_outlier && h != m_outlier) {

                scat_p.SetNext(x, y, p);
                scat_h.SetNext(x, y, h);
            }
        }

        // Set grids
        double xMin = scat_u.xMin();
        double xMax = scat_u.xMax();
        double yMin = scat_u.yMin();
        double yMax = scat_u.yMax();

        double dx = xMax - xMin;
        double dy = yMax - yMin;
        double dl = dx > dy ? dx : dy;
        double ds = dl / m_resolution;

        int nx = dx / ds;
        int ny = dy / ds;

        SurfaceGrid grid_u(nx, ny);
        SurfaceGrid grid_v(nx, ny);
        SurfaceGrid grid_p(nx, ny);
        SurfaceGrid grid_h(nx, ny);
        buildBmpMask(nx, ny);

        dx = (xMax - xMin) / nx;
        dy = (yMax - yMin) / ny;
        double dd = dx > dy ? dx : dy;
        for (size_t ix = 0; ix < nx; ++ix) { grid_u.xset(ix, xMin + double(ix) * dd); grid_v.xset(ix, xMin + double(ix) * dd); grid_p.xset(ix, xMin + double(ix) * dd); grid_h.xset(ix, xMin + double(ix) * dd); }
        for (size_t iy = 0; iy < ny; ++iy) { grid_u.yset(iy, yMin + double(iy) * dd); grid_v.yset(iy, yMin + double(iy) * dd); grid_p.yset(iy, yMin + double(iy) * dd); grid_h.yset(iy, yMin + double(iy) * dd); }

        // Set Xpand
        auto scanRatio = XpandScanRatio(); // 0 - 100 (16)
        auto densityRatio = XpandDensityRatio(350); // 1 - 10000 (150)
        auto edgeFactor = XpandEdgeFactor(); // 1 - 10000 (100)
        Xpand(grid_u, scat_u);
        Xpand(grid_v, scat_v);
        Xpand(grid_p, scat_p);
        Xpand(grid_h, scat_h);

        // Write to PNG texture
        auto textureFile = m_outUrl + "uv_" + file.sn + ".png";
        m_result["flow_fields"].push_back(m_resourceUrl + "uv_" + file.sn + ".png");
        auto maskFile = m_outUrl + "mask_" + first_file.sn + ".png";
        m_result["area_masks"].push_back(m_resourceUrl + "mask_" + first_file.sn + ".png");
        auto demFile = m_outUrl + "height_" + first_file.sn + ".png";
        m_result["height_maps"].push_back(m_resourceUrl + "height_" + first_file.sn + ".png");
        
        
        auto status1 = toTextureGRID_uv(scat_u, scat_v, grid_u, grid_v, textureFile.c_str(), maskFile.c_str(), isFirst);
        auto status2 = toTextureGRID_elevation(scat_p, scat_h, grid_p, grid_h, demFile.c_str(), maskFile.c_str(), isFirst);
        std::string status;
        if (status1 && status2)
            status = "SUCCESSED";
        else status = "FAILED";

        std::cout << "Texture " << file.sn << " Building finished: " << status << std::endl;

        isFirst = false;
    }

    // Fill result JSON
    m_result["flow_boundary"]["u_min"] = m_flowVelocityRange[0];
    m_result["flow_boundary"]["v_min"] = m_flowVelocityRange[1];
    m_result["flow_boundary"]["u_max"] = m_flowVelocityRange[2];
    m_result["flow_boundary"]["v_max"] = m_flowVelocityRange[3];
    m_result["flow_boundary"]["p_min"] = m_flowPRange[0];
    m_result["flow_boundary"]["p_max"] = m_flowPRange[1];
    m_result["flow_boundary"]["h_min"] = m_flowHRange[0];
    m_result["flow_boundary"]["h_max"] = m_flowHRange[1];
    m_result["flow_boundary"]["elevation_min"] = m_terrainRange[0];
    m_result["flow_boundary"]["elevation_max"] = m_terrainRange[1];

    m_result["constraints"]["max_texture_size"] = 4096;
    m_result["constraints"]["max_streamline_num"] = 65536 * 4;
    m_result["constraints"]["max_segment_num"] = 64;
    m_result["constraints"]["max_drop_rate"] = 0.1;
    m_result["constraints"]["max_drop_rate_bump"] = 0.2;
}

void FlowField::finish()
{  

    // Remove temp files
    closeDataSet();
    std::string maskPath = m_outUrl + "mask.bmp";
    std::string maskXMLPath = m_outUrl + "mask.bmp.aux.xml";
    remove(maskPath.c_str());
    remove(maskXMLPath.c_str());

    // Write visualization description json
    std::ofstream outStream(resultPath.c_str());
    outStream << std::setw(4) << m_result << std::endl;

    // Write response json
    // auto responsePath = m_casePath + m_caseID + "/response.json";
    // std::ifstream  iStream(responsePath.c_str());
    // Json responseJson;
    // iStream >> responseJson;

    // responseJson["visualization-description-json"] = "visualization/flow_field_description.json";

    // std::ofstream resposneOutStream(responsePath.c_str());
    // resposneOutStream << std::setw(4) << responseJson << std::endl;
}