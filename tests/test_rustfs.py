from util.rustfs import (
    build_tiff_resource_key,
    build_tiff_resource_keys,
    normalize_resource_key,
)


def test_normalize_resource_key_handles_windows_style_paths() -> None:
    assert (
        normalize_resource_key(r"resource\tiff\Mzs\2023\standard\202304\202304.tif")
        == "tiff/Mzs/2023/standard/202304/202304.tif"
    )


def test_build_tiff_resource_key_uses_timepoint_as_object_key() -> None:
    assert (
        build_tiff_resource_key("Mzs", "2023-04-01")
        == "tiff/Mzs/2023/standard/20230401/20230401.tif"
    )


def test_build_tiff_resource_key_keeps_month_granularity() -> None:
    assert (
        build_tiff_resource_key("Mzs", "202304")
        == "tiff/Mzs/2023/standard/202304/202304.tif"
    )


def test_build_tiff_resource_keys_include_month_fallback_for_daily_timepoint() -> None:
    assert build_tiff_resource_keys("Mzs", "2023-04-01") == [
        "tiff/Mzs/2023/standard/20230401/20230401.tif",
        "tiff/Mzs/2023/standard/202304/202304.tif",
    ]
