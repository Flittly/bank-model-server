cmake_minimum_required(VERSION 3.20)
project(TextureBuilder)

# Set C++ standard and ABI
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Print compiler information
message(STATUS "C Compiler: ${CMAKE_C_COMPILER}")
message(STATUS "CXX Compiler: ${CMAKE_CXX_COMPILER}")

# nlohmann/json 声明的是 cmake_minimum_required(VERSION 3.1)，而 CMake ≥ 4.0 已经
# 移除了对 < 3.5 的兼容策略；这里显式把它抬到 3.5，避免配置阶段直接报错退出。
# （只影响子项目的策略下限，不改变本项目自身的 cmake_minimum_required。）
set(CMAKE_POLICY_VERSION_MINIMUM 3.5 CACHE STRING "" FORCE)

# Include directories and libraries for third-party dependencies
add_subdirectory(${CMAKE_SOURCE_DIR}/thirdparty/json)
include_directories(
    ${CMAKE_SOURCE_DIR}/thirdparty/stb/include
    ${CMAKE_SOURCE_DIR}/thirdparty/quikgrid/include
    ${CMAKE_SOURCE_DIR}/thirdparty/json/include
)

# Add third-party libraries
add_library(stb ${CMAKE_SOURCE_DIR}/thirdparty/stb/stb_image.cpp)
add_library(quikgrid
    ${CMAKE_SOURCE_DIR}/thirdparty/quikgrid/contour.cpp
    ${CMAKE_SOURCE_DIR}/thirdparty/quikgrid/gridxtyp.cpp
    ${CMAKE_SOURCE_DIR}/thirdparty/quikgrid/scatdata.cpp
    ${CMAKE_SOURCE_DIR}/thirdparty/quikgrid/surfgrid.cpp
    ${CMAKE_SOURCE_DIR}/thirdparty/quikgrid/xpand.cpp
)

# Collect libraries
list(APPEND LIBS
    stb
    quikgrid
    nlohmann_json::nlohmann_json
)

# Detect platform and configure GDAL and PROJ accordingly
if(WIN32)
    # Windows specific configuration.
    #
    # 本机没有 vcpkg 的 gdal 端口，但装了 OSGeo4W，它提供的正是 **MSVC 版** 的 GDAL
    # （lib/gdal_i.lib + lib/gdal.lib），可以直接给 Visual Studio 的 cl.exe 用。
    # 只需要 GDAL：源码里用到的 OGRSpatialReference / OGRCreateCoordinateTransformation
    # / GDALRasterize 全是 GDAL 自己的 API，PROJ 由 gdal3xx.dll 在运行时内部链接，
    # 所以这里不显式链接 proj，也不需要 proj.h。
    #
    # 换目录时用 -DOSGEO4W_ROOT=<path> 覆盖。
    set(OSGEO4W_ROOT "C:/OSGeo4W" CACHE PATH "OSGeo4W installation root (provides MSVC-built GDAL)")
    find_path(GDAL_INCLUDE_DIR gdal.h PATHS "${OSGEO4W_ROOT}/include" NO_DEFAULT_PATH)
    find_library(GDAL_LIBRARY NAMES gdal_i gdal312 gdal PATHS "${OSGEO4W_ROOT}/lib" NO_DEFAULT_PATH)
    if(NOT GDAL_INCLUDE_DIR OR NOT GDAL_LIBRARY)
        message(FATAL_ERROR
            "在 ${OSGEO4W_ROOT} 下找不到 GDAL。请安装 OSGeo4W（含 GDAL 开发文件），"
            "或用 -DOSGEO4W_ROOT=<你的 OSGeo4W 路径> 指定。"
            "  include=${GDAL_INCLUDE_DIR}  library=${GDAL_LIBRARY}")
    endif()
    message(STATUS "GDAL include dir: ${GDAL_INCLUDE_DIR}")
    message(STATUS "GDAL library: ${GDAL_LIBRARY}")
    include_directories(${GDAL_INCLUDE_DIR})
    list(APPEND LIBS ${GDAL_LIBRARY})

    # 产物固定落 build/windows/，与 model/hydrodynamic_resource.py 里拼的路径一致。
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_SOURCE_DIR}/build/windows")
    foreach(_cfg IN ITEMS DEBUG RELEASE RELWITHDEBINFO MINSIZEREL)
        set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_${_cfg} "${CMAKE_SOURCE_DIR}/build/windows")
    endforeach()
elseif(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    # macOS specific configuration for PROJ and GDAL
    find_package(PROJ REQUIRED CONFIG)
    find_package(GDAL REQUIRED)

    list(APPEND LIBS PROJ::proj GDAL::GDAL)
else()
    # Linux specific configuration for PROJ and GDAL

    # Using Conda environment for compiler
    if(DEFINED ENV{CONDA_PREFIX})
        set(CMAKE_C_COMPILER $ENV{CONDA_PREFIX}/bin/x86_64-conda-linux-gnu-gcc CACHE PATH "C compiler")
        set(CMAKE_CXX_COMPILER $ENV{CONDA_PREFIX}/bin/x86_64-conda-linux-gnu-g++ CACHE PATH "CXX compiler")
    endif()

    # Function to find GDAL and PROJ paths dynamically
    function(find_gdal_and_proj)
        # Find gdal-config executable
        find_program(GDAL_CONFIG_EXECUTABLE gdal-config)
        if (NOT GDAL_CONFIG_EXECUTABLE)
            message(FATAL_ERROR "gdal-config not found")
        endif()

        # Use gdal-config to find GDAL prefix path
        execute_process(
            COMMAND ${GDAL_CONFIG_EXECUTABLE} --prefix
            OUTPUT_VARIABLE GDAL_PREFIX
            OUTPUT_STRIP_TRAILING_WHITESPACE
        )
        message(STATUS "GDAL_PREFIX: ${GDAL_PREFIX}")

        # Set GDAL include and library paths
        find_path(GDAL_INCLUDE_DIR gdal.h PATHS ${GDAL_PREFIX}/include)
        find_library(GDAL_LIBRARY gdal PATHS ${GDAL_PREFIX}/lib)
        message(STATUS "GDAL include dir: ${GDAL_INCLUDE_DIR}")
        message(STATUS "GDAL library: ${GDAL_LIBRARY}")

        # Find proj executable
        find_program(PROJ_EXECUTABLE proj)
        if (NOT PROJ_EXECUTABLE)
            message(FATAL_ERROR "proj not found")
        endif()

        # Use proj command to find PROJ version
        execute_process(
            COMMAND ${PROJ_EXECUTABLE} --version
            OUTPUT_VARIABLE PROJ_VERSION
            OUTPUT_STRIP_TRAILING_WHITESPACE
        )
        message(STATUS "PROJ_VERSION: ${PROJ_VERSION}")

        # Set PROJ include and library paths
        find_path(PROJ_INCLUDE_DIR proj.h PATHS ${GDAL_PREFIX}/include $ENV{CONDA_PREFIX}/include)
        find_library(PROJ_LIBRARY proj PATHS ${GDAL_PREFIX}/lib $ENV{CONDA_PREFIX}/lib)
        message(STATUS "PROJ include dir: ${PROJ_INCLUDE_DIR}")
        message(STATUS "PROJ library: ${PROJ_LIBRARY}")

        # Set GDAL and PROJ include and library paths
        set(GDAL_INCLUDE_DIRS ${GDAL_INCLUDE_DIR} PARENT_SCOPE)
        set(GDAL_LIBRARIES ${GDAL_LIBRARY} PARENT_SCOPE)
        set(PROJ_INCLUDE_DIRS ${PROJ_INCLUDE_DIR} PARENT_SCOPE)
        set(PROJ_LIBRARIES ${PROJ_LIBRARY} PARENT_SCOPE)
    endfunction()

    # Call the function to find GDAL and PROJ paths
    find_gdal_and_proj()

    # Include GDAL and PROJ directories
    include_directories(${GDAL_INCLUDE_DIRS} ${PROJ_INCLUDE_DIRS})

    # Add GDAL and PROJ libraries to the list
    list(APPEND LIBS ${GDAL_LIBRARIES} ${PROJ_LIBRARIES})
endif()

# Define the executable
aux_source_directory(${CMAKE_SOURCE_DIR}/src SRC_FILES)
add_executable(${PROJECT_NAME} ${SRC_FILES} source/main.cpp source/cesium_cartesian3.cpp source/FlowField.cpp)

# Define the include directories
target_include_directories(${PROJECT_NAME} PRIVATE
    ${CMAKE_SOURCE_DIR}/include
)

# MSVC 的 <math.h> 默认不定义 M_PI（GCC/Clang 会定义）。源码里 FlowField.cpp 与
# cesium_cartesian3.cpp 都用到了它，所以统一打开这个宏，而不是去改源码。
if(MSVC)
    target_compile_definitions(${PROJECT_NAME} PRIVATE _USE_MATH_DEFINES)
    # 源码是 UTF-8（注释与告警串里都有中文）。MSVC 默认按系统 OEM 代码页（本机 936）读源文件，
    # 中文会被误解析；/utf-8 = /source-charset:utf-8 + /execution-charset:utf-8，
    # 保证编译期解析与运行期 std::cerr 输出都对。
    target_compile_options(${PROJECT_NAME} PRIVATE /utf-8)
endif()

# Define the link libraries
target_link_libraries(${PROJECT_NAME} PRIVATE ${LIBS})
if(NOT WIN32)
    # libstdc++ 只在 GCC/Clang 的 Linux 工具链下需要显式链接；MSVC 没有这个库。
    target_link_libraries(${PROJECT_NAME} PRIVATE stdc++)
endif()
