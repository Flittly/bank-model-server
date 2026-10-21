"""
遥感影像底图（AI 出图的最底层背景图层）

为什么需要
----------
现有出图只有「DEM 灰度底图 + 岸段色带 + 断面」三层。DEM 灰度只能表达高程，
读者看不出河势、岸线、江心洲、城镇与地物，图面信息不完整。在**这三层之下再垫一层
真实遥感影像**，岸段到底落在哪、断面贴着哪一侧河岸，才能一眼看明白。

合规（重要，不可绕过）
----------------------
按地图数据合规要求，只允许使用**国家地理信息公共服务平台「天地图」（NASG）**，
或自建 / 本地瓦片。**禁止**使用 Google、Apple、Bing（海外版）、Mapbox、
OpenStreetMap 直连瓦片等未取得资质的境外源。
腾讯 / 高德 / 百度的栅格瓦片端点是其 JS SDK 的内部实现，不对外提供静态拼接接口，
因此本模块不实现（工作台前端走腾讯地图 GL JS，与此处互不影响）。

天地图需要用户自行申请的 key（`tk`）。**本模块不含任何内置 key**，
缺 key 时按「未启用」处理并打印原因，出图结果与加底图之前完全一致。

三层坐标系
----------
瓦片是 Web Mercator（EPSG:3857，天地图 `w` 矩阵集，256×256，缩放级 0~18），
而 matplotlib 的坐标轴是经纬度。拼接后按行做一次「等距墨卡托纬度 → 线性纬度」重采样，
再交给 ``imshow``，避免纬度方向的非线性带来数像素级错位。
"""
from __future__ import annotations

import io
import math
import os
import re
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, wait
from typing import Optional

TILE_SIZE = 256

#: Web Mercator 的纬度截断（±85.05112878°）
MAX_MERCATOR_LAT = 85.05112878

#: 底图 zorder：必须**严格小于** DEM 底图（risk_map 里 DEM 是 0），
#: 才能保证「在现有三层之下加一层」而不是覆盖它们。
BASEMAP_ZORDER = -10

#: 地球赤道周长 / 256，即 zoom=0 时一像素对应的地面距离（米）
_EQUATOR_METERS_PER_PIXEL_AT_Z0 = 156543.03392804097

#: 天地图各图层的 WMTS 端点前缀（endpoint = {layer}_w，LAYER 参数 = {layer}）
TIANDITU_LAYERS = {
    'img': '影像底图',
    'cia': '影像注记',
    'vec': '矢量底图',
    'cva': '矢量注记',
    'ter': '地形晕渲',
    'cta': '地形注记',
}

#: 天地图为非默认场景（需用户自行申请 key），此处只放占位符，绝不内置真实 key
TIANDITU_KEY_PLACEHOLDER = '请到天地图开放平台（https://console.tianditu.gov.cn/api/key）申请自己的key并替换此占位符'

#: 无 key 判定：空串、纯空白、以及上面那个占位符本身
_PLACEHOLDER_KEYS = {
    TIANDITU_KEY_PLACEHOLDER,
    'your-tianditu-key',
    'TODO',
}

_TILE_MAGIC = ((b'\xff\xd8\xff', '.jpg'), (b'\x89PNG\r\n\x1a\n', '.png'),
               (b'RIFF', '.webp'), (b'II*\x00', '.tif'), (b'MM\x00*', '.tif'))


# ---------------------------------------------------------------------------
# Web Mercator 瓦片数学
# ---------------------------------------------------------------------------

def _clamp_lat(lat: float) -> float:
    return max(-MAX_MERCATOR_LAT, min(MAX_MERCATOR_LAT, float(lat)))


def lat_to_mercator_norm(lat: float) -> float:
    """纬度 → 归一化墨卡托 y（0 = 北边界，1 = 南边界）。"""
    lat = _clamp_lat(lat)
    rad = math.radians(lat)
    return (1.0 - math.log(math.tan(rad) + 1.0 / math.cos(rad)) / math.pi) / 2.0


def mercator_norm_to_lat(norm: float) -> float:
    """归一化墨卡托 y → 纬度。"""
    norm = max(0.0, min(1.0, float(norm)))
    return math.degrees(math.atan(math.sinh(math.pi * (1.0 - 2.0 * norm))))


def lnglat_to_pixel(lng: float, lat: float, zoom: int) -> tuple:
    """经纬度 → 全局像素坐标（左上角为原点，zoom 决定像素密度）。"""
    n = TILE_SIZE * (2 ** zoom)
    x = (float(lng) + 180.0) / 360.0 * n
    y = lat_to_mercator_norm(lat) * n
    return x, y


def pixel_to_lnglat(px: float, py: float, zoom: int) -> tuple:
    """全局像素坐标 → 经纬度。"""
    n = TILE_SIZE * (2 ** zoom)
    lng = px / n * 360.0 - 180.0
    lat = mercator_norm_to_lat(py / n)
    return lng, lat


def tile_range(west: float, south: float, east: float, north: float, zoom: int) -> tuple:
    """覆盖给定经纬度范围所需的瓦片号范围（闭区间，已按该级瓦片总数裁剪）。"""
    n = 2 ** zoom
    x0, y0 = lnglat_to_pixel(west, north, zoom)
    x1, y1 = lnglat_to_pixel(east, south, zoom)
    tx0 = max(0, min(n - 1, int(math.floor(x0 / TILE_SIZE))))
    tx1 = max(0, min(n - 1, int(math.floor(x1 / TILE_SIZE))))
    ty0 = max(0, min(n - 1, int(math.floor(y0 / TILE_SIZE))))
    ty1 = max(0, min(n - 1, int(math.floor(y1 / TILE_SIZE))))
    return tx0, ty0, tx1, ty1


def tile_extent(x: int, y: int, zoom: int) -> tuple:
    """单张瓦片的经纬度范围 (west, east, south, north)。"""
    west, north = pixel_to_lnglat(x * TILE_SIZE, y * TILE_SIZE, zoom)
    east, south = pixel_to_lnglat((x + 1) * TILE_SIZE, (y + 1) * TILE_SIZE, zoom)
    return west, east, south, north


def tile_ground_resolution(zoom: int, lat: float) -> float:
    """该缩放级、该纬度下每像素对应的地面距离（米）。"""
    return _EQUATOR_METERS_PER_PIXEL_AT_Z0 * math.cos(math.radians(_clamp_lat(lat))) / (2 ** zoom)


def tile_count(west: float, south: float, east: float, north: float, zoom: int) -> int:
    x0, y0, x1, y1 = tile_range(west, south, east, north, zoom)
    return (x1 - x0 + 1) * (y1 - y0 + 1)


def choose_zoom(west: float, south: float, east: float, north: float,
                target_px_width: float, min_zoom: int = 3, max_zoom: int = 18,
                max_tiles: int = 256) -> int:
    """挑一个缩放级：底图地面分辨率不粗于目标像素分辨率，且瓦片数不超上限。

    :param target_px_width: 成图上该范围对应的像素宽度（决定「需要多细」）
    :param max_tiles: 瓦片数上限，防止范围一大就拉几百张瓦片（也是内存护栏）
    """
    center_lat = (south + north) / 2.0
    width_m = max(1.0, (east - west) * 111320.0 * math.cos(math.radians(center_lat)))
    target_res = width_m / max(1.0, float(target_px_width))

    best = min_zoom
    for zoom in range(min_zoom, max_zoom + 1):
        best = zoom
        if tile_ground_resolution(zoom, center_lat) <= target_res:
            break

    # 瓦片数超限则逐级回退（宁可粗一点也不要把机器/网络打满）
    while best > min_zoom and tile_count(west, south, east, north, best) > max_tiles:
        best -= 1
    return best


# ---------------------------------------------------------------------------
# 配置解析（缺 key / 未启用 → 返回 None，出图行为与加底图前完全一致）
# ---------------------------------------------------------------------------

def _mask_key(text: str) -> str:
    """日志里屏蔽 tk，避免 key 泄进后端日志。"""
    return re.sub(r'(tk=)[^&\s]+', r'\1***', str(text))


def _is_absent_key(key) -> bool:
    if key is None:
        return True
    text = str(key).strip()
    return (not text) or text in _PLACEHOLDER_KEYS


def _env(*names) -> Optional[str]:
    for name in names:
        value = os.environ.get(name)
        if value and value.strip():
            return value.strip()
    return None


def resolve_config(raw: Optional[dict]) -> Optional[dict]:
    """把载荷 / 调用方给的松散配置规范成可直接使用的配置；不可用时返回 None。

    返回 None 的三种情形，都会打印**明确原因**（不静默）：
      * 未提供配置（``None``）；
      * 显式 ``enabled=False`` / ``false`` / ``0`` / ``off``；
      * provider 需要 key（天地图）但环境里没有可用的 key。
    """
    if raw is None:
        return None
    if not isinstance(raw, dict):
        print('[basemap] basemap 配置不是对象，忽略（底图未启用）', flush=True)
        return None

    enabled = raw.get('enabled', True)
    if isinstance(enabled, str):
        enabled = enabled.strip().lower() not in ('false', '0', 'off', 'no', 'none', 'disabled')
    if not enabled:
        print('[basemap] 已按下发配置关闭底图（enabled=false）', flush=True)
        return None

    provider = str(raw.get('provider') or 'tianditu').strip().lower()
    layer = str(raw.get('layer') or ('img' if provider == 'tianditu' else '')).strip().lower()

    config = {
        'provider': provider,
        'layer': layer,
        'key': raw.get('key'),
        'url_template': raw.get('url_template'),
        'path': raw.get('path'),
        'cache_dir': raw.get('cache_dir'),
        'zoom': raw.get('zoom'),
        'min_zoom': int(raw.get('min_zoom') or 3),
        'max_zoom': int(raw.get('max_zoom') or 18),
        'max_tiles': int(raw.get('max_tiles') or 200),
        'timeout': float(raw.get('timeout') or 6.0),
        'retries': int(raw.get('retries') if raw.get('retries') is not None else 1),
        # 整批取图的总时长预算：Java 侧起 Python 进程只等 60 s，底图不能把关卡卡死
        'time_budget': float(raw.get('time_budget') or 25.0),
        'alpha': float(raw.get('alpha') if raw.get('alpha') is not None else 1.0),
        'attribution': raw.get('attribution'),
    }

    if provider == 'tianditu':
        if layer not in TIANDITU_LAYERS:
            print(f'[basemap] 天地图不支持图层 {layer!r}（可选 {"/".join(TIANDITU_LAYERS)}），'
                  f'回退为 img', flush=True)
            layer = config['layer'] = 'img'
        if _is_absent_key(config['key']):
            config['key'] = _env('TIANDITU_TK', 'TIANDITU_KEY')
        if _is_absent_key(config['key']):
            print('[basemap] 天地图底图未启用：未提供 key。'
                  '请到天地图开放平台申请 tk 后，通过环境变量 TIANDITU_TK 或配置项 '
                  'app.ai.visualization.basemap.key 提供（本模块不内置任何 key）。', flush=True)
            return None

    elif provider == 'custom':
        if not config['url_template']:
            print('[basemap] provider=custom 但未提供 url_template，忽略（底图未启用）', flush=True)
            return None

    elif provider == 'local':
        if not config['path']:
            print('[basemap] provider=local 但未提供 path（本地瓦片根目录），忽略', flush=True)
            return None

    else:
        print(f'[basemap] 未知 provider={provider!r}。合规可用：tianditu（默认）/ local / custom。'
              f'境外源（Google/Bing/OSM/Mapbox 等）不在允许范围内。', flush=True)
        return None

    if config['attribution'] is None and provider == 'tianditu':
        config['attribution'] = f'底图：国家地理信息公共服务平台「天地图」· {TIANDITU_LAYERS[layer]}'
    return config


# ---------------------------------------------------------------------------
# 取瓦片（缓存优先 → 网络；全部失败则整体放弃，不影响其它图层）
# ---------------------------------------------------------------------------

def _default_cache_dir() -> str:
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # bank-model-server/
    return os.path.join(root, 'cache', 'basemap_tiles')


def _ext_from_bytes(data: bytes) -> str:
    for magic, ext in _TILE_MAGIC:
        if data.startswith(magic):
            return ext
    return '.bin'


def build_tile_url(config: dict, z: int, x: int, y: int, subdomain: int = 0) -> str:
    """拼瓦片 URL。天地图走 WMTS；custom 走用户模板；local 不产生 URL。"""
    if config['provider'] == 'tianditu':
        return ('https://t{s}.tianditu.gov.cn/{layer}_w/wmts'
                '?SERVICE=WMTS&REQUEST=GetTile&VERSION=1.0.0'
                '&LAYER={layer}&STYLE=default&TILEMATRIXSET=w&FORMAT=tiles'
                '&TILEMATRIX={z}&TILEROW={y}&TILECOL={x}&tk={key}').format(
            s=subdomain % 8, layer=config['layer'], z=z, x=x, y=y, key=config['key'])
    template = config['url_template']
    return (template.replace('{s}', str(subdomain % 4))
            .replace('{z}', str(z)).replace('{x}', str(x)).replace('{y}', str(y)))


def _tile_path_on_disk(config: dict, z: int, x: int, y: int, ext: str = '') -> str:
    base = config['cache_dir'] or _default_cache_dir()
    directory = os.path.join(base, config['provider'], config['layer'] or 'default',
                             str(z), str(x))
    return os.path.join(directory, f'{y}{ext}')


def _load_from_disk(config: dict, z: int, x: int, y: int):
    for ext in ('.jpg', '.png', '.jpeg', '.webp', '.tif', '.bin'):
        path = _tile_path_on_disk(config, z, x, y, ext)
        if os.path.exists(path) and os.path.getsize(path) > 0:
            try:
                with open(path, 'rb') as handle:
                    return handle.read()
            except OSError:
                continue
    return None


def _save_to_disk(config: dict, z: int, x: int, y: int, data: bytes) -> None:
    path = _tile_path_on_disk(config, z, x, y, _ext_from_bytes(data))
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'wb') as handle:
            handle.write(data)
    except OSError:
        pass          # 缓存写不进去不该让出图失败


def _download(url: str, timeout: float, retries: int, deadline: Optional[float] = None) -> Optional[bytes]:
    """下载单张瓦片。``deadline``（time.monotonic 时间戳）到达后不再发起新请求，
    保证「整批取图」的墙钟时长真正受预算约束，而不是只停住等待的一方。"""
    request = urllib.request.Request(url, headers={
        'User-Agent': 'Mozilla/5.0 (compatible; YangtzeBankWarning/1.0; basemap-tile-fetch)',
        'Accept': 'image/webp,image/jpeg,image/png,*/*',
    })
    for _ in range(max(1, retries + 1)):
        wait_seconds = timeout
        if deadline is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0.3:
                return None
            wait_seconds = min(timeout, remaining)
        try:
            with urllib.request.urlopen(request, timeout=wait_seconds) as resp:
                if resp.status != 200:
                    continue
                data = resp.read()
                return data if data else None
        except (urllib.error.URLError, urllib.error.HTTPError, OSError, ValueError):
            continue
        except Exception:                                  # noqa: BLE001 - 兜底，绝不因底图中断出图
            continue
    return None


def fetch_tile(config: dict, z: int, x: int, y: int,
               deadline: Optional[float] = None) -> Optional[bytes]:
    """取一张瓦片的原始字节：本地缓存 → 本地瓦片目录 → 网络。"""
    if config['provider'] == 'local':
        base = config['path']
        for ext in ('.jpg', '.jpeg', '.png', '.webp', '.tif', '.bin'):
            path = os.path.join(base, str(z), str(x), f'{y}{ext}')
            if os.path.exists(path):
                try:
                    with open(path, 'rb') as handle:
                        return handle.read()
                except OSError:
                    return None
        return None

    cached = _load_from_disk(config, z, x, y)
    if cached:
        return cached
    url = build_tile_url(config, z, x, y, (x + y) % 8)
    data = _download(url, config['timeout'], config['retries'], deadline)
    if data:
        _save_to_disk(config, z, x, y, data)
    return data


def _open_tile(data: bytes):
    from PIL import Image
    try:
        image = Image.open(io.BytesIO(data))
        image.load()
        return image.convert('RGB')
    except Exception:                                       # noqa: BLE001
        return None


def _decode_safe(image) -> Optional['object']:
    """把 PIL 图的像素取成 numpy 数组；损坏瓦片返回 None 而不是抛异常。"""
    import numpy as np
    try:
        return np.asarray(image, dtype=np.uint8)
    except Exception:                                       # noqa: BLE001
        return None


# ---------------------------------------------------------------------------
# 拼接与「墨卡托纬度 → 线性纬度」重采样
# ---------------------------------------------------------------------------

def _warp_rows_to_linear_lat(array, south: float, north: float):
    """把「行等距墨卡托」的影像重采样成「行等距纬度」，消除纬度方向非线性错位。

    拼接出来的图，行号在墨卡托 y 上是均匀的；而 ``imshow`` 是把行号均匀映射到
    **纬度**上的。两者差一个非线性项，实测在 1° 纬度、zoom 12 下约 5 像素，
    足以让影像与岸段/断面出现可见错位。这里按行重采样一次（O(行数)）即可对齐。
    """
    import numpy as np

    rows = array.shape[0]
    if rows < 2:
        return array

    top_norm = lat_to_mercator_norm(north)
    bottom_norm = lat_to_mercator_norm(south)
    norms = top_norm + (bottom_norm - top_norm) * (np.arange(rows) / (rows - 1.0))
    src_lat = np.array([mercator_norm_to_lat(v) for v in norms])      # 自北向南递减
    dst_lat = np.linspace(src_lat[0], src_lat[-1], rows)              # 目标：线性纬度

    indices = np.interp(dst_lat, src_lat[::-1], np.arange(rows)[::-1])
    indices = np.clip(np.round(indices).astype(int), 0, rows - 1)
    return array[indices]


def render_basemap(config: dict, bbox: tuple, target_px_width: float = 1400.0):
    """抓取并拼接覆盖 bbox 的影像。

    :param bbox: (west, south, east, north)，经纬度
    :param target_px_width: 成图上该范围的像素宽度，用于挑选缩放级
    :return: (numpy RGB 数组, (west, east, south, north))；不可用时返回 (None, None)

    取图有**总时长预算**（``time_budget``，默认 25 s）：超时未取到的瓦片一律以浅灰占位，
    不阻塞出图。这是因为 Java 侧起 Python 进程后只等 60 s，底图绝不能把关卡卡死。
    """
    import numpy as np
    from PIL import Image

    west, south, east, north = (float(v) for v in bbox)
    if east <= west or north <= south:
        print(f'[basemap] 范围非法 lng=[{west}, {east}] lat=[{south}, {north}]，跳过底图', flush=True)
        return None, None

    zoom = config.get('zoom')
    if zoom:
        zoom = max(config['min_zoom'], min(config['max_zoom'], int(zoom)))
    else:
        zoom = choose_zoom(west, south, east, north, target_px_width,
                           config['min_zoom'], config['max_zoom'], config['max_tiles'])

    x0, y0, x1, y1 = tile_range(west, south, east, north, zoom)
    cols, rows = x1 - x0 + 1, y1 - y0 + 1
    total = cols * rows
    if total > config['max_tiles']:
        print(f'[basemap] zoom={zoom} 需 {total} 张瓦片（上限 {config["max_tiles"]}），跳过底图', flush=True)
        return None, None

    coordinates = [(x, y) for y in range(y0, y1 + 1) for x in range(x0, x1 + 1)]

    budget = float(config.get('time_budget') or 25.0)
    deadline = time.monotonic() + budget

    def grab(coord):
        # 预算耗尽前不再发起新请求：这样「总时长」是硬约束，
        # 而不会出现「等待超时了、工作线程还在各自重试」的假超时。
        return coord, fetch_tile(config, zoom, coord[0], coord[1], deadline)

    fetched = {}
    with ThreadPoolExecutor(max_workers=min(8, max(1, total))) as pool:
        futures = {pool.submit(grab, c): c for c in coordinates}
        done, pending = wait(futures, timeout=budget + 2.0)
        for future in done:
            try:
                coord, data = future.result()
                fetched[coord] = data
            except Exception:                               # noqa: BLE001
                continue
        if pending:
            print(f'[basemap] 取图超时（>{budget:.0f}s）：{len(pending)}/{total} 张未完成，'
                  f'以浅灰占位', flush=True)
            for future in pending:
                future.cancel()

    mosaic = Image.new('RGB', (cols * TILE_SIZE, rows * TILE_SIZE), (222, 226, 230))
    ok = 0
    for (x, y), data in fetched.items():
        tile = _open_tile(data) if data else None
        if tile is None:
            continue
        if tile.size != (TILE_SIZE, TILE_SIZE):
            tile = tile.resize((TILE_SIZE, TILE_SIZE), Image.LANCZOS)
        mosaic.paste(tile, ((x - x0) * TILE_SIZE, (y - y0) * TILE_SIZE))
        ok += 1

    if ok == 0:
        print(f'[basemap] zoom={zoom} 的 {total} 张瓦片一张都没拿到（key 无效 / 网络不通 / '
              f'本地目录为空），本次出图不加底图', flush=True)
        return None, None

    m_west, _, _, m_north = tile_extent(x0, y0, zoom)
    _, m_east, m_south, _ = tile_extent(x1, y1, zoom)

    array = _decode_safe(mosaic)
    if array is None:
        print('[basemap] 拼接结果解码失败，跳过底图', flush=True)
        return None, None
    array = _warp_rows_to_linear_lat(array, m_south, m_north)

    if ok < total:
        print(f'[basemap] zoom={zoom} 瓦片 {ok}/{total} 张成功，缺失部分以浅灰占位', flush=True)
    print(f'[basemap] provider={config["provider"]} layer={config["layer"] or "-"} zoom={zoom} '
          f'瓦片 {cols}x{rows}({ok}张) 覆盖 lng=[{m_west:.4f}, {m_east:.4f}] '
          f'lat=[{m_south:.4f}, {m_north:.4f}]', flush=True)
    return array, (m_west, m_east, m_south, m_north)


def draw_basemap(ax, raw_config: Optional[dict], bbox: tuple,
                 target_px_width: float = 1400.0) -> dict:
    """在最底层（``zorder=BASEMAP_ZORDER``）画影像底图。返回绘制报告。

    入参是**原始**配置（载荷里那份），内部先做 ``resolve_config``。
    只做「加一层」：配置不可用或取图失败都直接返回不画 —— 不触碰 DEM / 岸段 / 断面。
    """
    report = {'drawn': False}
    config = resolve_config(raw_config)
    if not config:
        return report

    array, extent = render_basemap(config, bbox, target_px_width)
    if array is None:
        return report

    ax.imshow(array, extent=[extent[0], extent[1], extent[2], extent[3]],
              aspect='auto', interpolation='bilinear', zorder=BASEMAP_ZORDER,
              alpha=config.get('alpha', 1.0))
    report.update({'drawn': True, 'extent': extent, 'attribution': config.get('attribution'),
                   'alpha': config.get('alpha', 1.0)})
    # 刻意**不动** xlim/ylim：拼接范围天然覆盖整个 bbox（瓦片边界只会比 bbox 大），
    # 改坐标轴范围会连带改变原本的取景，属于「动了现有图面」，不做。
    return report
