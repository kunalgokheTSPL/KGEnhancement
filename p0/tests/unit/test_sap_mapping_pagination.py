"""getSapMappingContext must slice the parquet DataFrame by limit/offset,
not just report the totals, so the review page's pager actually pages."""

from unittest.mock import patch

import pandas as pd

from p0.api.routers.context import getSapMappingContext


def _make_df(n: int) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "sap_equipment_id": [f"EQ-{i:03d}" for i in range(n)],
            "pid_asset_tag": [f"TAG-{i:03d}" for i in range(n)],
        }
    )


def _call(limit: int = 100, offset: int = 0, n_rows: int = 10):
    with patch("p0.api.plants.validate_plant_code", return_value=None), \
         patch(
             "p0.api.routers.context._read_parquet_from_rustfs",
             return_value=_make_df(n_rows),
         ):
        return getSapMappingContext(plant_code_id="M014", limit=limit, offset=offset)


def test_default_page_returns_all_rows_when_under_the_limit():
    result = _call(limit=100, offset=0, n_rows=10)
    data = result["data"]
    assert data["total_rows"] == 10
    assert len(data["rows"]) == 10
    assert data["rows"][0]["sap_equipment_id"] == "EQ-000"
    assert data["rows"][-1]["sap_equipment_id"] == "EQ-009"


def test_limit_slices_to_the_requested_page_size():
    result = _call(limit=3, offset=0, n_rows=10)
    data = result["data"]
    assert data["total_rows"] == 10
    assert len(data["rows"]) == 3
    assert [r["sap_equipment_id"] for r in data["rows"]] == ["EQ-000", "EQ-001", "EQ-002"]


def test_offset_skips_the_earlier_rows():
    result = _call(limit=3, offset=3, n_rows=10)
    data = result["data"]
    assert data["total_rows"] == 10
    assert [r["sap_equipment_id"] for r in data["rows"]] == ["EQ-003", "EQ-004", "EQ-005"]


def test_offset_plus_limit_beyond_total_returns_the_remaining_tail():
    result = _call(limit=5, offset=8, n_rows=10)
    data = result["data"]
    assert data["total_rows"] == 10
    assert [r["sap_equipment_id"] for r in data["rows"]] == ["EQ-008", "EQ-009"]


def test_offset_past_the_end_returns_no_rows_but_keeps_total_rows():
    result = _call(limit=10, offset=50, n_rows=10)
    data = result["data"]
    assert data["total_rows"] == 10
    assert data["rows"] == []
    assert data["columns"] == []


def test_response_echoes_the_requested_limit_and_offset():
    result = _call(limit=4, offset=2, n_rows=10)
    data = result["data"]
    assert data["limit"] == 4
    assert data["offset"] == 2


def _make_mixed_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "sap_equipment_id": ["EQ-000", "EQ-001", "EQ-002", "EQ-003"],
            "pid_asset_tag": ["TAG-000", "", None, "TAG-003"],
        }
    )


def test_matched_only_filters_out_rows_with_missing_pid_asset_tag():
    with patch("p0.api.plants.validate_plant_code", return_value=None), \
         patch(
             "p0.api.routers.context._read_parquet_from_rustfs",
             return_value=_make_mixed_df(),
         ):
        result = getSapMappingContext(
            plant_code_id="M014", limit=100, offset=0, matched_only=True
        )
    data = result["data"]
    assert data["matched_only"] is True
    assert data["total_rows"] == 2
    assert [r["sap_equipment_id"] for r in data["rows"]] == ["EQ-000", "EQ-003"]


def test_matched_only_false_returns_all_rows_from_mixed_fixture():
    with patch("p0.api.plants.validate_plant_code", return_value=None), \
         patch(
             "p0.api.routers.context._read_parquet_from_rustfs",
             return_value=_make_mixed_df(),
         ):
        result = getSapMappingContext(
            plant_code_id="M014", limit=100, offset=0, matched_only=False
        )
    data = result["data"]
    assert data["matched_only"] is False
    assert data["total_rows"] == 4


def test_matched_only_omitted_defaults_to_false_and_returns_all_rows():
    with patch("p0.api.plants.validate_plant_code", return_value=None), \
         patch(
             "p0.api.routers.context._read_parquet_from_rustfs",
             return_value=_make_mixed_df(),
         ):
        result = getSapMappingContext(plant_code_id="M014", limit=100, offset=0)
    data = result["data"]
    assert data["total_rows"] == 4
    assert data["matched_only"] is False


def test_matched_only_true_without_pid_asset_tag_column_returns_400():
    df = pd.DataFrame({"sap_equipment_id": ["EQ-000", "EQ-001"]})
    with patch("p0.api.plants.validate_plant_code", return_value=None), \
         patch("p0.api.routers.context._read_parquet_from_rustfs", return_value=df):
        result = getSapMappingContext(
            plant_code_id="M014", limit=100, offset=0, matched_only=True
        )
    assert result.status_code == 400
