"""New source columns must be complete, well-formed, and additive only."""

from __future__ import annotations

import os

import yaml

os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

from p0.api.config import SCHEMA_FROZEN_FILE

ALARM_CODES = ["alm", "ans_plus", "ans_minus", "dv_plus", "dv_minus", "hh", "hi",
               "int", "iop", "iop_minus", "ll", "lo", "ltrp", "trip"]

BOUNDARY_CODES = ["sol_l", "sol_h", "ipf_ll", "ipf_hh", "mdl_hhh"]

ORIGINAL = {
    "ts_uid": "BIGSERIAL",
    "plant_code_id": "VARCHAR(50)",
    "site": "VARCHAR(80) NOT NULL DEFAULT '-'",
    "tag_name": "VARCHAR(255)",
    "iotdb_tag_id": "VARCHAR(255)",
    "description": "TEXT",
    "unit": "VARCHAR(50)",
    "data_type": "VARCHAR(40)",
    "equipment_id": "VARCHAR(80)",
    "normalized_asset": "VARCHAR(120)",
    "op_limit_ll": "NUMERIC(18,6)",
    "op_limit_l": "NUMERIC(18,6)",
    "op_limit_h": "NUMERIC(18,6)",
    "op_limit_hh": "NUMERIC(18,6)",
    "safety_limit_ll": "NUMERIC(18,6)",
    "safety_limit_l": "NUMERIC(18,6)",
    "safety_limit_h": "NUMERIC(18,6)",
    "safety_limit_hh": "NUMERIC(18,6)",
    "trip_limit_ll": "NUMERIC(18,6)",
    "trip_limit_l": "NUMERIC(18,6)",
    "trip_limit_h": "NUMERIC(18,6)",
    "trip_limit_hh": "NUMERIC(18,6)",
    "limits_basis": "VARCHAR(80)",
    "limits_source_doc_id": "VARCHAR(120)",
    "source_system": "VARCHAR(80)",
    "source_record_id": "VARCHAR(200)",
    "source_file": "VARCHAR(512)",
    "confidence": "NUMERIC(6,4)",
    "updated_at": "TIMESTAMP",
    "is_active": "BOOLEAN",
}


def _columns():
    """The declared columns of timeseries_metadata."""
    with open(SCHEMA_FROZEN_FILE, encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)
    tables = cfg.get("schema") or cfg.get("tables") or {}
    return (tables.get("timeseries_metadata") or {}).get("columns") or {}


def test_every_original_column_survives_with_its_type_unchanged():
    cols = _columns()
    for name, sql_type in ORIGINAL.items():
        assert name in cols, f"{name} was dropped"
        assert str(cols[name]) == sql_type, f"{name} changed type"


def test_the_change_is_additive_only():
    assert set(ORIGINAL) <= set(_columns())


def test_every_alarm_type_has_its_four_columns():
    cols = _columns()
    for code in ALARM_CODES:
        for suffix in ("count", "percent", "accum_pct", "limit"):
            assert f"alerts_prime_alarm_{code}_{suffix}" in cols


def test_every_boundary_code_has_its_four_columns():
    cols = _columns()
    for code in BOUNDARY_CODES:
        for suffix in ("value", "count", "percent", "accum_pct"):
            assert f"alerts_prime_bnd_{code}_{suffix}" in cols


def test_alarm_counts_are_wide_enough_for_the_observed_range():
    cols = _columns()
    for code in ALARM_CODES:
        assert cols[f"alerts_prime_alarm_{code}_count"] == "BIGINT"


def test_alarm_limits_are_numeric_because_the_source_values_are():
    cols = _columns()
    for code in ALARM_CODES:
        assert cols[f"alerts_prime_alarm_{code}_limit"] == "NUMERIC(18,6)"


def test_the_oem_limits_stay_text_because_they_hold_prose():
    cols = _columns()
    assert cols["oem_lower_limit"] == "TEXT"
    assert cols["oem_upper_limit"] == "TEXT"


def test_the_protean_limits_are_numeric_because_those_ones_are_clean():
    cols = _columns()
    assert cols["protean_lower_limit"] == "NUMERIC(18,6)"
    assert cols["protean_upper_limit"] == "NUMERIC(18,6)"


def test_no_column_name_exceeds_what_postgres_will_keep():
    long = [c for c in _columns() if len(c) > 63]
    assert not long, f"postgres truncates these to 63 chars: {long}"


def test_the_upper_equipment_code_is_not_the_canonical_equipment_id():
    cols = _columns()
    assert "alerts_prime_upper_equipment_id" in cols
    assert cols["equipment_id"] == "VARCHAR(80)"
