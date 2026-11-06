"""regionFlush 向量化的位级等价守门。

背景：全江冲淤的相减与上色原本是 33,355,868 像素的 Python 双重循环
（实测 13.6s + 21.3s），改成 numpy 分块向量化后合计约 1.2s。
风险不在“跑不起来”，而在“看起来对、数值悄悄偏了一档”：

- 相减：提升到 float64 会改变舍入；漏掉 default_value 判断会多出假数据
- 上色：把色表索引夹到 [0,255] 会让越界像素串档（实测曾出现 221 像素不一致）

这里用「照抄原循环」的参考实现做**逐字节**比对，不依赖真实 DEM。
"""

import os
import sys

import numpy as np
import pytest

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from modelResource.riverbedEvolution import regionFlush  # noqa: E402
from modelResource.riverbedEvolution.regionFlush import (  # noqa: E402
    _lut_colorize,
    _subtract_aligned,
    _subtract_fully_aligned,
    color_mapping,
)

NODATA = -99999.0
DEFAULT_VALUE = -9999.0


def _reference_subtract(data1, data2, nodata1, nodata2, default_value=DEFAULT_VALUE):
    """原实现的逐像素版本（整幅对齐、无越界分支时的等价形态）。"""
    height, width = data1.shape
    array = np.full((height, width), fill_value=default_value)
    for row in range(height):
        for col in range(width):
            val1 = data1[row][col]
            val2 = data2[row][col]
            if (
                val1 == nodata1
                or val2 == nodata2
                or val1 == default_value
                or val2 == default_value
            ):
                array[row, col] = default_value
            else:
                array[row, col] = val1 - val2
    return array.astype(np.float32)


def _reference_colorize(image_array, min_value, max_value, nodata):
    height, width = image_array.shape[:2]
    rgba = np.zeros((height, width, 4), dtype=np.uint8)
    for y in range(height):
        for x in range(width):
            r, g, b, a = color_mapping(image_array[y, x], min_value, max_value, nodata)
            rgba[y, x] = [r, g, b, a]
    return rgba


def test_subtract_aligned_bitwise_equal_to_reference(monkeypatch):
    rng = np.random.default_rng(20261106)
    data1 = rng.uniform(-10.0, 13.0, size=(37, 53)).astype(np.float32)
    data2 = rng.uniform(-10.0, 13.0, size=(37, 53)).astype(np.float32)

    data1[::5, ::7] = np.float32(NODATA)
    data2[1::6, 3::11] = np.float32(NODATA)
    data1[2, 2] = np.float32(DEFAULT_VALUE)  # 输入恰好等于默认值
    data2[4, 9] = np.float32(DEFAULT_VALUE)
    data1[0, 0] = np.float32(-0.0)

    # 收窄分块行数，确保覆盖跨块边界
    monkeypatch.setattr(regionFlush, "SUBTRACT_CHUNK_ROWS", 7)

    fast = _subtract_aligned(data1, data2, NODATA, NODATA, DEFAULT_VALUE)
    reference = _reference_subtract(data1, data2, NODATA, NODATA, DEFAULT_VALUE)

    assert fast.dtype == np.float32
    assert fast.tobytes() == reference.tobytes()


def test_subtract_result_independent_of_chunk_size(monkeypatch):
    rng = np.random.default_rng(7)
    data1 = rng.uniform(-10.0, 13.0, size=(101, 39)).astype(np.float32)
    data2 = rng.uniform(-10.0, 13.0, size=(101, 39)).astype(np.float32)
    data1[3::9] = np.float32(NODATA)

    monkeypatch.setattr(regionFlush, "SUBTRACT_CHUNK_ROWS", 1)
    tiny = _subtract_aligned(data1, data2, NODATA, NODATA, DEFAULT_VALUE)

    monkeypatch.setattr(regionFlush, "SUBTRACT_CHUNK_ROWS", 100000)
    whole = _subtract_aligned(data1, data2, NODATA, NODATA, DEFAULT_VALUE)

    assert tiny.tobytes() == whole.tobytes()


def test_lut_colorize_bitwise_equal_to_reference(monkeypatch):
    rng = np.random.default_rng(31)
    # 刻意让部分像素落在 [min, max] 之外，验证越界索引不被夹到 255
    image = rng.uniform(-12.0, 16.0, size=(29, 41)).astype(np.float32)
    image[3::7, 2::9] = np.float32(-9999.0)

    min_value = -8.826091766357422
    max_value = 11.96817684173584
    monkeypatch.setattr(regionFlush, "COLOR_PNG_CHUNK_ROWS", 5)

    result = _lut_colorize(image, min_value, max_value, -9999.0)
    reference = _reference_colorize(image, min_value, max_value, -9999.0)

    assert result is not None
    assert result.shape == (29, 41, 4)
    assert result.tobytes() == reference.tobytes()


def test_lut_colorize_covers_out_of_range_index():
    """色表索引越界（<0 与 >255）时，必须与逐像素原实现逐位相同。

    这不是理论风险：真实 flush.tif 上就有 247 个越界像素（145 个低于 min、
    102 个高于 max），因为 ``get_tif_attributes`` 用的是
    ``ComputeRasterMinMax(True)``——第二个参数 True 表示"允许近似"，走抽样
    估算，不是逐像素求极值。真实数据必然有像素落在 [min, max] 之外。

    第一版把索引 ``clip`` 到 [0, 255]，这 247 个像素里有 221 个颜色错了一档，
    因为 ``jet_color_map`` 并不夹输入，而是让负索引照常参与运算
    （``128 + 4 * (-17) = 60``），夹到 0 之后变成 128。
    """
    min_value = -8.826091766357422
    max_value = 11.96817684173584
    span = max_value - min_value

    # 索引约落在 -11 / -0.6 / 127 / 255.6 / 269，两端明确越界
    image = np.array(
        [
            [
                min_value - 0.9,
                min_value - 0.05,
                (min_value + max_value) / 2.0,
                max_value + 0.05,
                max_value + 1.2,
            ]
        ],
        dtype=np.float32,
    )

    result = _lut_colorize(image, min_value, max_value, DEFAULT_VALUE)
    reference = _reference_colorize(image, min_value, max_value, DEFAULT_VALUE)

    assert result is not None, "越界索引不该触发回退，应被 LUT 覆盖"

    # 先自证样本有效：夹到 [0,255] 的错误实现必须与参考不同，否则这个用例测不到东西
    clipped = np.clip(((image - min_value) / span * 255.0).astype(np.int32), 0, 255)
    wrong = np.array(
        [list(regionFlush.jet_color_map(int(k))) for k in clipped.ravel()],
        dtype=np.uint8,
    ).reshape(image.shape + (4,))
    assert wrong.tobytes() != reference.tobytes(), "构造的样本没落在越界区，用例无效"

    assert result.tobytes() == reference.tobytes()


def test_lut_colorize_marks_nodata_transparent():
    image = np.full((8, 8), 5.0, dtype=np.float32)
    image[2, 3] = np.float32(-9999.0)

    result = _lut_colorize(image, -8.0, 12.0, -9999.0)

    assert result is not None
    assert result[2, 3].tolist() == [0, 0, 0, 0]
    assert result[0, 0, 3] == 255


def test_lut_colorize_falls_back_on_nan():
    image = np.full((8, 8), 1.0, dtype=np.float32)
    image[0, 0] = np.nan

    assert _lut_colorize(image, -8.0, 12.0, -9999.0) is None


def test_lut_colorize_falls_back_without_nodata():
    image = np.full((8, 8), 1.0, dtype=np.float32)

    assert _lut_colorize(image, -8.0, 12.0, None) is None


def test_lut_colorize_falls_back_when_span_too_large():
    image = np.array([[0.0, 1.0], [2.0, 1e-6]], dtype=np.float32)

    # max-min 极小 ⇒ 索引跨度爆炸，应回退标量路径而不是建巨型色表
    assert _lut_colorize(image, 0.0, 1e-4, -9999.0) is None


def test_subtract_fully_aligned_requires_full_overlap():
    data = np.zeros((10, 10), dtype=np.float32)
    limits = dict(
        nodata1=NODATA,
        nodata2=NODATA,
        height=10,
        width=10,
        start_col1=0,
        end_col1=10,
        start_col2=0,
        end_col2=10,
        start_row1=0,
        end_row1=10,
        start_row2=0,
        end_row2=10,
    )

    assert _subtract_fully_aligned(data, data, **limits)

    shifted = dict(limits, start_col2=1, end_col2=11)
    assert not _subtract_fully_aligned(data, data, **shifted)

    wider = dict(limits, width=12)
    assert not _subtract_fully_aligned(data, data, **wider)

    no_nodata = dict(limits, nodata1=None)
    assert not _subtract_fully_aligned(data, data, **no_nodata)

    float64 = data.astype(np.float64)
    assert not _subtract_fully_aligned(float64, data, **limits)
