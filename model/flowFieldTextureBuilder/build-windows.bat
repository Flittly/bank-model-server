@echo off
REM ============================================================================
REM  Build TextureBuilder on Windows (offscreen generator for flow-field textures).
REM
REM  NOTE: this file must stay ASCII-only. cmd.exe parses .bat in the OEM code page,
REM  so non-ASCII bytes inside quotes corrupt the parser (a real trap, not theory).
REM
REM  Requirements:
REM    1) Visual Studio 2022 C++ toolchain (cl.exe) - vcvars64.bat is called below
REM    2) OSGeo4W with the MSVC build of GDAL (include/gdal.h + lib/gdal_i.lib)
REM    3) Windows SDK (rc.exe / mt.exe) - paths are injected below, no registry lookup
REM    4) ninja on PATH
REM
REM  Output: build\windows\TextureBuilder.exe
REM  That path is hard-coded in model\hydrodynamic_resource.py - do not change it.
REM ============================================================================
setlocal

set "VCVARS=D:\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvars64.bat"
if not defined OSGEO4W_ROOT set "OSGEO4W_ROOT=C:\OSGeo4W"
if not defined SDK_ROOT   set "SDK_ROOT=D:\Windows Kits\10"
if not defined SDK_VER    set "SDK_VER=10.0.26100.0"

if not exist "%VCVARS%" (
    echo [ERROR] vcvars64.bat not found: %VCVARS%
    echo         Install the Visual Studio 2022 "Desktop development with C++" workload.
    exit /b 1
)
if not exist "%OSGEO4W_ROOT%\include\gdal.h" (
    echo [ERROR] gdal.h not found under %OSGEO4W_ROOT%\include
    echo         Install OSGeo4W with the GDAL development files, or set OSGEO4W_ROOT first.
    exit /b 1
)

call "%VCVARS%"
if errorlevel 1 (
    echo [ERROR] vcvars64.bat initialization failed
    exit /b 1
)

REM vcvars64 locates the Windows SDK through the registry (reg.exe). Where that
REM lookup is unavailable, rc.exe / mt.exe end up off PATH and the link step dies
REM with "RC Pass 1 ... no such file or directory". Inject the SDK paths directly.
if not exist "%SDK_ROOT%\bin\%SDK_VER%\x64\rc.exe" set "SDK_VER=10.0.19041.0"
if not exist "%SDK_ROOT%\bin\%SDK_VER%\x64\rc.exe" (
    echo [ERROR] Windows SDK not found under %SDK_ROOT%
    exit /b 1
)
set "PATH=%SDK_ROOT%\bin\%SDK_VER%\x64;%PATH%"
set "INCLUDE=%SDK_ROOT%\Include\%SDK_VER%\ucrt;%SDK_ROOT%\Include\%SDK_VER%\um;%SDK_ROOT%\Include\%SDK_VER%\shared;%INCLUDE%"
set "LIB=%SDK_ROOT%\Lib\%SDK_VER%\ucrt\x64;%SDK_ROOT%\Lib\%SDK_VER%\um\x64;%LIB%"
where rc.exe >nul 2>&1 || ( echo [ERROR] rc.exe still not on PATH & exit /b 1 )

cd /d "%~dp0"

cmake -S . -B build\_cmake -G Ninja -DCMAKE_POLICY_VERSION_MINIMUM=3.5 -DOSGEO4W_ROOT="%OSGEO4W_ROOT%" -DCMAKE_BUILD_TYPE=Release
if errorlevel 1 exit /b 1

cmake --build build\_cmake
if errorlevel 1 exit /b 1

if not exist "build\windows\TextureBuilder.exe" (
    echo [ERROR] build finished but build\windows\TextureBuilder.exe is missing
    exit /b 1
)

echo.
echo [OK] %~dp0build\windows\TextureBuilder.exe
endlocal
