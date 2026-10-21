"""
岸段风险色带：把断面的风险等级沿岸段折线做颜色插值。

设计要点（见 docs/bank-gradient-in-viz-design.md）：

1. 位置参数用「几何投影」求沿程弧长 —— 不用 cross_sections.distance 定位。
   distance 的单位与起点方向都可能与岸段几何不一致（reversed 标志并不可靠），
   一旦不一致整条色带会错位或翻转；投影法的 s_i 与绘制用的折线同源同向，翻转不可能发生。
2. 纯 numpy 实现，不依赖 shapely（本环境未安装 shapely）。
   经纬度先用局部等距圆柱投影转成米制，投影基准取岸段纬度中位数。
   注：shapely 是平面几何库，直接喂经纬度得到的是「度」而不是米，仍要 pyproj 投影才有米制。
3. distance 仅用于一致性自检（告警），不参与定位。
"""
from __future__ import annotations

import logging
import math

import numpy as np

R_EARTH = 6378137.0

#: 「内插/外推」判定阈值的下限（米）。真正的阈值由 adaptive_gap_threshold 按该岸段
#: 断面的实际间距自适应决定——实测真实数据相邻断面的中位间距就是 1000 m，
#: 用固定的 500 m 会把 76.5% 的区段误判成「内插」而整条画成虚线。
DEFAULT_GAP_METERS = 1000.0

#: 自适应阈值 = max(DEFAULT_GAP_METERS, GAP_FACTOR × 该岸段相邻断面间距的中位数)
DEFAULT_GAP_FACTOR = 3.0

#: 闭合折线判定容差（米）：首末点距离不超过该值即视为闭合环
RING_TOLERANCE_METERS = 1.0

#: 无有效断面时岸段的颜色
DEFAULT_COLOR = '#94a3b8'

#: 投影点点到岸段折线的告警阈值（米）：断面中点离岸段太远说明几何可疑
DEFAULT_OFFSET_WARN_METERS = 200.0

log = logging.getLogger(__name__)


# --------------------------------------------------------------------------
# 基础几何：经纬度 <-> 局部米制平面
# --------------------------------------------------------------------------

def lnglat_to_local(point, lat0: float) -> np.ndarray:
    """单个点 (lng, lat) → 局部平面坐标（米）。"""
    lng, lat = float(point[0]), float(point[1])
    return np.array([
        np.radians(lng) * R_EARTH * np.cos(np.radians(lat0)),
        np.radians(lat) * R_EARTH,
    ])


def to_local_xy(coords, lat0: float | None = None) -> np.ndarray:
    """[[lng, lat], ...] → (n, 2) 局部平面坐标（米）。lat0 缺省取纬度中位数。"""
    arr = np.asarray(coords, dtype=float)
    if arr.ndim != 2 or arr.shape[1] < 2:
        raise ValueError('coords 必须是 [[lng, lat], ...] 形状')
    arr = arr[:, :2]
    if lat0 is None:
        lat0 = float(np.median(arr[:, 1]))
    x = np.radians(arr[:, 0]) * R_EARTH * np.cos(np.radians(lat0))
    y = np.radians(arr[:, 1]) * R_EARTH
    return np.column_stack([x, y])


def cumulative_lengths(xy) -> tuple[np.ndarray, np.ndarray, float]:
    """返回 (每段长度[n-1], 顶点累计弧长[n], 总长[米])。"""
    xy = np.asarray(xy, dtype=float)
    if xy.shape[0] < 2:
        return np.zeros(0), np.zeros(xy.shape[0]), 0.0
    seg = np.linalg.norm(np.diff(xy, axis=0), axis=1)
    s = np.concatenate([[0.0], np.cumsum(seg)])
    return seg, s, float(s[-1])


def project_along(xy, s, point) -> tuple[float, float]:
    """把点投影到折线，返回 (沿程弧长[米], 到折线的最近距离[米])。

    逐段求投影参数 t∈[0,1]（含端点 clamp），取距离最小的那段。
    """
    xy = np.asarray(xy, dtype=float)
    p = np.asarray(point, dtype=float)
    a, b = xy[:-1], xy[1:]
    ab = b - a
    denom = np.einsum('ij,ij->i', ab, ab)
    valid = denom > 0
    safe = np.where(valid, denom, 1.0)
    t = np.einsum('ij,ij->i', p - a, ab) / safe
    t = np.clip(np.where(valid, t, 0.0), 0.0, 1.0)
    proj = a + t[:, None] * ab
    dist = np.linalg.norm(proj - p, axis=1)
    idx = int(np.argmin(dist))
    along = float(s[idx] + t[idx] * (s[idx + 1] - s[idx]))
    return along, float(dist[idx])


def approx_distance_meters(p1, p2) -> float:
    """两点近似球面距离（米）。小范围用局部等距近似即可。"""
    lng1, lat1 = float(p1[0]), float(p1[1])
    lng2, lat2 = float(p2[0]), float(p2[1])
    lat_mid = math.radians((lat1 + lat2) / 2.0)
    dx = math.radians(lng2 - lng1) * R_EARTH * math.cos(lat_mid)
    dy = math.radians(lat2 - lat1) * R_EARTH
    return math.hypot(dx, dy)


def is_closed_ring(coords, tolerance_meters: float = RING_TOLERANCE_METERS) -> bool:
    """折线是否闭合（首末点重合）。闭合环要按**环**的语义处理，不能当成开放折线。

    为什么不能「把末点丢掉、把环掰直」：``banks.bank_geometry`` 实测是闭合环
    （首末点距离 0.00 m），掰直会**摘掉那条闭合边**（实测岸段 370 丢 134.7 m、
    岸段 371 丢 975.4 m）。落在这条边上的断面投影不到它，只能被拉到最近端点 ——
    实测 371-S16 的沿程位置从 14 745 m 被拉到 0 m（偏差 476 m），它的风险色
    于是被画到了环的接缝处。而且「换个切点躲开断面」并不可行：实测断面间距约
    950 m、环上顶点间距约 725 m，断面对整条环是**密铺**的，任何切点旁边都有断面。

    所以本模块对闭合环采取三条「环语义」，而不是掰直：
      ① 几何**原样使用**（含那条闭合边），断面投影必然精确（实测 offset 全为 0）；
      ② 颜色**按环插值**（``color_at_cyclic``）—— 环上「最后一个断面」与
         「第一个断面」本就是相邻的两个断面，跨接缝的那一段要正常做渐变，
         而不是当成「超出实测范围」压成端点色；
      ③ 稀疏判定**按环**（``is_gap_segment(..., cyclic=True)``）—— 环上不存在
         「实测范围之外」，只有「相邻断面跨距过大」这一种内插区段。
    """
    if len(coords) < 3:
        return False
    return approx_distance_meters(coords[0], coords[-1]) <= tolerance_meters


# --------------------------------------------------------------------------
# 颜色
# --------------------------------------------------------------------------

def _hex_to_rgb(color: str) -> tuple[int, int, int]:
    value = color.lstrip('#')
    return int(value[0:2], 16), int(value[2:4], 16), int(value[4:6], 16)


def lerp_color(c1: str, c2: str, u: float) -> str:
    """两个 #rrggbb 按比例 u∈[0,1] 线性插值。"""
    u = min(1.0, max(0.0, float(u)))
    r1, g1, b1 = _hex_to_rgb(c1)
    r2, g2, b2 = _hex_to_rgb(c2)
    return '#%02x%02x%02x' % (
        round(r1 + (r2 - r1) * u),
        round(g1 + (g2 - g1) * u),
        round(b1 + (b2 - b1) * u),
    )


def lerp_risk_color(c1: str, c2: str, u: float) -> str:
    """风险色插值：沿**等级序**（1→2→3→4）过渡，而不是在 RGB 空间直连。

    为什么不能 RGB 直连：绿(#10b981)→红(#ef4444) 的 RGB 中点约 #a99987 ——
    一个浑浊的土棕/灰，饱和度极低。跨档断面（如相邻两条断面是 1 级和 4 级）
    之间的整段过渡都会落在这个脏色上，视觉上像是「这一段没着色」。
    而风险等级本就是有序的，1→4 的过渡理应依次经过黄(2)、橙(3)，
    与图例的四档标尺一致。

    取色失败（不是四档标准色，例如无数据灰或自定义色）时退回普通 RGB 插值。
    """
    from .config import RISK_COLORS  # 延迟导入，避免循环依赖

    reverse = {value.lower(): level for level, value in RISK_COLORS.items()}
    l1 = reverse.get(str(c1).lower())
    l2 = reverse.get(str(c2).lower())
    if l1 is None or l2 is None:
        return lerp_color(c1, c2, u)

    u = min(1.0, max(0.0, float(u)))
    level = l1 + (l2 - l1) * u
    low = int(math.floor(level))
    high = min(low + 1, max(RISK_COLORS))
    frac = level - low
    if low not in RISK_COLORS:
        return lerp_color(c1, c2, u)
    if frac <= 0 or low == high or high not in RISK_COLORS:
        return RISK_COLORS[low]
    return lerp_color(RISK_COLORS[low], RISK_COLORS[high], frac)


def color_at(t: float, stops, default: str = DEFAULT_COLOR) -> str:
    """按位置 t∈[0,1] 在 stops=[(t_i, color)] 中取色。

    两端做 clamp（对齐 Mapbox line-gradient / interpolate 语义）：
    t 小于首个 stop 用首色，大于末个 stop 用末色。
    """
    if not stops:
        return default
    if t <= stops[0][0]:
        return stops[0][1]
    if t >= stops[-1][0]:
        return stops[-1][1]
    for i in range(len(stops) - 1):
        t0, c0 = stops[i]
        t1, c1 = stops[i + 1]
        if t0 <= t <= t1:
            span = t1 - t0
            return c1 if span <= 0 else lerp_risk_color(c0, c1, (t - t0) / span)
    return stops[-1][1]


def color_at_cyclic(t: float, stops, default: str = DEFAULT_COLOR) -> str:
    """按位置 t∈[0,1] 在 stops 上取色，并把 endpoints 首尾相接（环语义）。

    与 ``color_at`` 的唯一区别在两端：
      * ``color_at`` 在 t 超出 stops 范围时**压色**（clamp 到端点色）——
        适合开放折线（两端之外确实没有实测控制，颜色不该外推）。
      * ``color_at_cyclic`` 把 stops 看成环上的站点，t 落在两端之外时
        在「最后一个断面 → 第一个断面（绕接缝）」之间插值 —— 适合闭合环。

    若不这样做，闭合环的接缝处会出现一道硬边：接缝前是最后一个断面的颜色、
    接缝后立刻跳到第一个断面的颜色，中间那段真实的渐变丢失（上一版把这段
    当成「外推」画成半透明虚线，视觉上就是一段发灰的缺口）。
    """
    if not stops:
        return default
    if len(stops) == 1:
        return stops[0][1]

    t = float(t) % 1.0
    first_t, first_c = stops[0]
    last_t, last_c = stops[-1]

    if t < first_t:
        # 接缝区间：[last_t - 1, first_t]
        lo_t, lo_c = last_t - 1.0, last_c
        hi_t, hi_c = first_t, first_c
    elif t > last_t:
        # 接缝区间：[last_t, first_t + 1]
        lo_t, lo_c = last_t, last_c
        hi_t, hi_c = first_t + 1.0, first_c
    else:
        return color_at(t, stops, default)

    span = hi_t - lo_t
    return hi_c if span <= 0 else lerp_risk_color(lo_c, hi_c, (t - lo_t) / span)


# --------------------------------------------------------------------------
# 断面 <-> 岸段 的位置换算
# --------------------------------------------------------------------------

def section_midpoint(section):
    """断面中点 (lng, lat)；端点缺失或非法时返回 None。"""
    if not isinstance(section, dict):
        return None
    try:
        slng, slat = section.get('start_lng'), section.get('start_lat')
        if slng is None or slat is None:
            return None
        elng = section.get('end_lng')
        elat = section.get('end_lat')
        elng = slng if elng is None else elng
        elat = slat if elat is None else elat
        return ((float(slng) + float(elng)) / 2.0, (float(slat) + float(elat)) / 2.0)
    except (TypeError, ValueError):
        return None


def build_stops(sections, bank_coords, lat0: float | None = None,
                color_of=None, offset_warn_meters: float = DEFAULT_OFFSET_WARN_METERS,
                cyclic: bool = False):
    """按断面在岸段上的几何位置生成颜色 stop。

    :param sections: 该岸段下的断面列表（需含 start/end 经纬度）
    :param bank_coords: 岸段折线 [[lng, lat], ...]；闭合环请**原样**传入（含那条
        闭合边），不要先掰直 —— 掰直会丢几何、把断面拉到端点（见 is_closed_ring）
    :param color_of: 可调用对象 section -> color；缺省用 config.risk_color
    :param offset_warn_meters: 断面中点离岸段的距离告警阈值
    :param cyclic: bank_coords 是否为闭合环；为 True 时 t 按环归一化
        （along=total 折回 0，因为环上二者是同一点）
    :return: (stops, items, total_length, xy)
        stops 为按 t 升序去重后的 [(t, color)]；
        items 为明细（含 along / t / offset / distance），供日志与自检使用。
    """
    if color_of is None:
        from .config import risk_color as color_of  # 延迟导入，避免循环依赖

    bank_arr = np.asarray(bank_coords, dtype=float)
    if lat0 is None:
        # 投影基准必须先在函数内定下来，否则断面投影会拿到 lat0=None
        lat0 = float(np.median(bank_arr[:, 1]))
    xy = to_local_xy(bank_arr, lat0)
    _, s, total = cumulative_lengths(xy)
    if total <= 0:
        return [], [], 0.0, xy

    items = []
    for section in sections:
        mid = section_midpoint(section)
        if mid is None:
            continue
        point = lnglat_to_local(mid, lat0)
        along, offset = project_along(xy, s, point)
        if offset > offset_warn_meters:
            log.warning(
                '[gradient] 断面 %s 中点距岸段 %.1f m（> %.0f m），投影可能不可靠',
                section.get('section_id'), offset, offset_warn_meters,
            )
        if cyclic:
            # 环上 along=total 与 along=0 是同一个点，归一化到 0 以免出现
            # 「同一位置两个 stop」，接缝处才能正确插值。
            t = (along % total) / total if total > 0 else 0.0
        else:
            t = min(1.0, max(0.0, along / total))
        items.append({
            'section_id': section.get('section_id'),
            'section_name': section.get('section_name'),
            'risk_level': section.get('risk_level'),
            'color': color_of(section),
            'along': along,
            't': t,
            'offset': offset,
            'distance': _as_float(section.get('distance')),
        })

    items.sort(key=lambda item: item['t'])

    # 同一位置（t 相同）只保留一个，避免出现 span=0 的 stop
    stops: list[tuple[float, str]] = []
    merged: list[str] = []
    for item in items:
        if stops and abs(stops[-1][0] - item['t']) < 1e-9:
            merged.append(str(item['section_id']))
            stops[-1] = (item['t'], item['color'])
        else:
            stops.append((item['t'], item['color']))

    if merged:
        # 典型成因：两条断面被布设在同一点（或闭合环上 along=0 与 along=total 处
        # 各有一条，归一化后位置相同）。位置重合就无法区分先后，只能保留一条。
        log.warning('[gradient] %d 个断面因位置与相邻断面重合被合并：%s',
                    len(merged), ', '.join(merged[:8]))

    return stops, items, total, xy


def _as_float(value):
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if np.isfinite(result) else None


# --------------------------------------------------------------------------
# 顶点配色 + 稀疏区段判定
# --------------------------------------------------------------------------

def adaptive_gap_threshold(measured_alongs, override=None,
                           factor: float = DEFAULT_GAP_FACTOR,
                           floor: float = DEFAULT_GAP_METERS,
                           cyclic: bool = False, total: float | None = None):
    """按该岸段断面的实际间距，自适应决定「跨度过大」的阈值（米）。

    为什么不能用固定值：真实数据相邻断面的中位间距**就是 1000 m**（p25~p90 全为 1000），
    用固定的 500 m 会把 76.5% 的区段判成「内插」而整条画成虚线+低透明 —— 视觉上
    等同于「岸段没有着色」。阈值应当相对于该岸段自身的采样密度，只把明显离群的大跨距
    标出来（例如其余都是 1 km、某一处却是 19 km），而不是把所有正常内插都否掉。

    :param measured_alongs: 该岸段各断面的沿程位置
    :param override: 显式指定阈值（来自载荷 options.gap_warn_meters），优先于自适应
    :param factor: 自适应倍数（阈值 = factor × 中位间距）
    :param floor: 阈值下限
    :param cyclic: 是否按闭合环统计间距（把「最后一个断面 → 第一个断面」绕接缝
        的那一段也算作一个间距，否则接缝处会被误当成一个巨大的离群跨距）
    :param total: 环的总长（cyclic=True 时需要）
    """
    if override is not None:
        try:
            value = float(override)
            if value > 0:
                return value
        except (TypeError, ValueError):
            pass
    arr = np.asarray(sorted(measured_alongs), dtype=float)
    if arr.size < 3:
        return float(floor)
    gaps = np.diff(arr)
    if cyclic and total and total > 0:
        # 绕接缝那一段（arr[-1] → arr[0] + total）也是一个真实间距
        gaps = np.append(gaps, float(arr[0] + total - arr[-1]))
    median_gap = float(np.median(gaps))
    if not np.isfinite(median_gap) or median_gap <= 0:
        return float(floor)
    return max(float(floor), factor * median_gap)


def is_gap_segment(along: float, measured, gap_meters: float = DEFAULT_GAP_METERS,
                   cyclic: bool = False, total: float | None = None) -> bool:
    """该位置是否属于「无实测控制」的区段。

    开放折线（cyclic=False）：两端之外外推、或跨度过大的内插，都算无控制区段。
    闭合环（cyclic=True）：环上**不存在「范围之外」**——每个位置都夹在两个相邻
      断面之间（含跨接缝的那一对），所以只保留「跨度 > gap_meters」这一条，
      并且跨度按环计算（``(hi - lo) % total``），跨接缝的那一对才不会被算成
      「一个在 0 m、一个在 15 km」而误报成内插。

    measured 为空时返回 False —— 该岸段没有断面，整条按纯色灰实线绘制
    （属于「无数据」而不是「外推」，由岸段色带与图注区分）。
    """
    if measured.size == 0 or measured.size == 1:
        return False

    if cyclic:
        if not total or total <= 0:
            return False
        n = measured.size
        j = int(np.searchsorted(measured, along, side='left'))
        lo = float(measured[(j - 1) % n])
        hi = float(measured[j % n])
        if j == 0:
            # along 落在「第一个断面之前」，邻居是最后一个断面（绕接缝过来）
            lo = float(measured[-1])
            hi = float(measured[0]) + total
        elif j == n:
            lo = float(measured[-1])
            hi = float(measured[0]) + total
        return (hi - lo) > gap_meters

    if along < measured[0] or along > measured[-1]:
        return True
    j = int(np.searchsorted(measured, along))
    lo = measured[max(0, j - 1)]
    hi = measured[min(measured.size - 1, j)]
    return float(hi - lo) > gap_meters


def build_vertex_colors(xy, s, total: float, stops, measured_alongs,
                        gap_meters: float = DEFAULT_GAP_METERS,
                        cyclic: bool = False):
    """给岸段折线的每一段算一个颜色，并标记是否为稀疏区段。

    :param cyclic: 折线是否为闭合环（决定取色是否跨接缝、稀疏判定是否按环）
    :return: (colors[n-1], is_gap[n-1])
    """
    n = len(xy)
    if n < 2 or total <= 0:
        return [], []
    measured = np.asarray(sorted(measured_alongs), dtype=float)
    colors: list[str] = []
    gaps: list[bool] = []
    for i in range(n - 1):
        mid_along = 0.5 * (s[i] + s[i + 1])
        t = mid_along / total
        colors.append(color_at_cyclic(t, stops) if cyclic else color_at(t, stops))
        gaps.append(is_gap_segment(mid_along, measured, gap_meters,
                                   cyclic=cyclic, total=total))
    return colors, gaps


def nearest_item_index(along_value: float, measured, cyclic: bool = False,
                       total: float | None = None) -> int:
    """折线上某点由哪条断面「管辖」（= 1D 最近邻，即 Voronoi 分段的归属）。

    Voronoi 分段等价于「找最近站点」：两个相邻断面的分界正是它们的中点，
    所以「距离最近的断面」与「落在哪个区间」是同一个判断，用 abs 距离即可，
    不需要显式算中点。闭合环上距离按环取（``min(d, total-d)``），
    否则接缝附近的断面会被判给相隔整条岸段的另一端。

    :param measured: 该岸段全部断面的沿程弧长（未排序也可以，内部排序）
    :return: 在 ``measured`` 排序后数组中的下标；空数组返回 -1
    """
    arr = np.sort(np.asarray(measured, dtype=float))
    if arr.size == 0:
        return -1
    diff = np.abs(arr - float(along_value))
    if cyclic and total and total > 0:
        diff = np.minimum(diff, total - diff)
    return int(np.argmin(diff))


def build_span_colors(xy, s, total: float, items,
                      gap_meters: float = DEFAULT_GAP_METERS,
                      cyclic: bool = False):
    """按「断面管辖区间」给折线逐段着色，**不可渲染的段返回 None**。

    与 ``build_vertex_colors`` 的区别（这是刻意替换，不是增量）：

    * ``build_vertex_colors`` 在相邻断面之间做颜色**渐变**，两端 clamp —— 于是
      「低风险段不画」根本做不到：只要邻居是可见的，它管辖的那段就会被邻居的
      颜色渐变填满（页面上踩过这个坑，见 sectionRenderVisibility.ts 的注释）。
    * 本函数把断面当**抽样点**、把岸段切成首尾相接的 Voronoi 区间，区间内填**纯色**：
      等级只有 4 档离散值，渐变会暗示「半档」；纯色才能让「整段留空」成立，
      也才与页面逐像素同色（页面用的就是同一套硬边区间）。

    只有 ``is_rendered_level(risk_level)`` 为真的断面才给出颜色；其余返回 ``None``，
    调用方应当**不画这一段**（不是画成透明色——透明色仍会占位、仍会参与命中）。

    :param xy: 岸段折线局部平面坐标（仅用于弧长，已由调用方算好 s）
    :param s: 顶点累计弧长
    :param total: 岸段总长（米）
    :param items: ``build_stops`` 返回的断面明细，需含 along / risk_level / color
    :param gap_meters: 「内插/外推」阈值；稀疏区段的样式由调用方决定，这里只做标记
    :param cyclic: 是否闭合环
    :return: (colors[n-1], gaps[n-1])，``colors`` 元素可能为 ``None``
    """
    from .config import is_rendered_level  # 延迟导入，避免循环依赖

    n = len(xy)
    if n < 2 or total <= 0 or not items:
        return [], []

    ordered = sorted(items, key=lambda item: item['along'])
    alongs = [item['along'] for item in ordered]
    measured = np.asarray(sorted(alongs), dtype=float)

    colors: list[str | None] = []
    gaps: list[bool] = []
    for i in range(n - 1):
        mid_along = 0.5 * (s[i] + s[i + 1])
        governor = ordered[nearest_item_index(mid_along, alongs, cyclic=cyclic, total=total)]
        colors.append(governor['color'] if is_rendered_level(governor.get('risk_level')) else None)
        gaps.append(is_gap_segment(mid_along, measured, gap_meters,
                                   cyclic=cyclic, total=total))
    return colors, gaps


def check_distance_consistency(items, total: float, bank_id=None) -> dict:
    """distance 与几何位置的一致性自检 —— 只告警，不自动翻转。

    这是「坑 1 / 坑 2」的观测手段：把存量脏数据暴露到日志，而不是静默修正。
    """
    pairs = [(item['distance'], item['along']) for item in items
             if item.get('distance') is not None]
    report = {'checked': len(pairs), 'corr': None, 'ratio': None}
    if total <= 0 or len(pairs) < 3:
        return report

    dist = np.array([p[0] for p in pairs], dtype=float)
    along = np.array([p[1] for p in pairs], dtype=float)
    if dist.std() == 0 or along.std() == 0:
        return report

    corr = float(np.corrcoef(dist, along)[0, 1])
    ratio = float(dist.max() / total)
    report['corr'] = corr
    report['ratio'] = ratio

    if corr < 0:
        log.warning('[gradient] bank=%s distance 方向与岸段几何相反（corr=%.3f），已按几何绘制',
                    bank_id, corr)
    elif corr < 0.5:
        log.warning('[gradient] bank=%s distance 与几何位置弱相关（corr=%.3f），疑为局部组距离',
                    bank_id, corr)
    if not (0.5 <= ratio <= 1.5):
        log.warning('[gradient] bank=%s max(distance)/岸段长度=%.2f 不在 [0.5, 1.5]，疑单位不一致',
                    bank_id, ratio)
    return report
