from app.main import task_service
from util import import_params


def test_normalize_risk_level_from_vector() -> None:
    assert task_service._normalize_risk_level([1, 0, 0, 0]) == 1
    assert task_service._normalize_risk_level([0, 1, 0, 0]) == 2
    assert task_service._normalize_risk_level([0, 0, 1, 0]) == 3
    assert task_service._normalize_risk_level([0, 0, 0, 1]) == 4


def test_build_risk_payload_maps_section_fields() -> None:
    section = {
        "id": 7,
        "section_id": "SEC_001",
        "region_code": "Mzs",
        "segment": "Mzs",
        "set_name": "standard",
        "current_timepoint": "202304",
        "comparison_timepoint": "201904",
        "hs": 0.5,
        "hc": 2.0,
        "protection_level": "systemic",
        "control_level": "strict",
        "section_geometry": {"type": "LineString", "coordinates": [[0, 0], [1, 1]]},
        "bench_id": "tiff/Mzs/2023/standard/202304/202304.tif",
        "ref_id": "tiff/Mzs/2019/standard/201904/201904.tif",
        "water_qs": "10000",
        "tidal_level": "zc",
        "risk_thresholds": {"all": [0.25, 0.5, 0.75]},
        "weights": {"wRE": [0.3, 0.3, 0.4], "wNM": [0.3, 0.3, 0.4]},
    }

    payload = task_service._build_risk_payload(section)

    assert payload["current-timepoint"] == "2023-04-01"
    assert payload["comparison-timepoint"] == "2019-04-01"
    assert payload["bench-id"] == section["bench_id"]
    assert payload["section-geometry"] == section["section_geometry"]
    assert payload["wRE"] == [0.3, 0.3, 0.4]
    assert payload["wGE"] == "NONE"


def test_build_risk_payload_requires_fields() -> None:
    section = {
        "id": 8,
        "section_id": "SEC_002",
        "segment": "Mzs",
    }

    try:
        task_service._build_risk_payload(section)
    except ValueError as exc:
        assert "missing required fields" in str(exc)
    else:
        raise AssertionError("Expected ValueError for missing fields")


def test_import_params_normalize_timepoint() -> None:
    assert import_params.normalize_timepoint("202304") == "2023-04-01"
    assert import_params.normalize_timepoint("20230415") == "2023-04-15"
    assert import_params.normalize_timepoint("2023-04") == "2023-04-01"
    assert import_params.normalize_timepoint("2023-04-01") == "2023-04-01"
