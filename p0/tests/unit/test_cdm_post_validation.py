"""Unit tests for the pure checks in cdm_post_validation.py (787 LOC, previously 0%).

Each check takes DataFrames + a config/settings dict and returns a status dict; these
tests pin the PASS/WARN/BLOCK logic with tiny in-memory tables — no CSV, no pipeline.
"""

from __future__ import annotations

import pandas as pd

from p0.utils.cdm_post_validation import (
    BLOCK,
    PASS,
    WARN,
    _check_data_quality_basics,
    _check_equipment_connectivity,
    _check_pk_integrity,
    _check_referential_integrity,
    _check_schema_conformance,
)

_EQUIP_SCHEMA = {
    "equipment": {
        "columns": {"equipment_id": "text", "name": "text", "area": "text"},
        "primary_key": "equipment_id",
        "constraints": [{"type": "unique", "columns": ["equipment_id"]}],
    }
}
_SETTINGS = {
    "schema_coverage_warn_pct": 50,
    "pk_null_block_pct": 10,
    "fk_unresolved_threshold": 0.50,
    "equipment_orphan_threshold": 0.30,
}


def test_schema_conformance_full_coverage_passes():
    df = pd.DataFrame({"name": ["p"], "area": ["a"]})
    res = _check_schema_conformance(df, "equipment", _EQUIP_SCHEMA, _SETTINGS)
    assert res["status"] == PASS
    assert res["coverage_pct"] == 100.0
    assert res["missing_from_csv"] == []


def test_schema_conformance_warns_when_most_columns_missing():
    df = pd.DataFrame({"unrelated": ["x"]})
    res = _check_schema_conformance(df, "equipment", _EQUIP_SCHEMA, _SETTINGS)
    assert res["status"] == WARN
    assert set(res["missing_from_csv"]) == {"name", "area"}


def test_schema_conformance_skips_unknown_table():
    res = _check_schema_conformance(pd.DataFrame(), "nope", _EQUIP_SCHEMA, _SETTINGS)
    assert res["status"] == PASS
    assert "skipped" in res["note"].lower()


def test_pk_integrity_passes_on_unique_non_null():
    df = pd.DataFrame({"equipment_id": ["E1", "E2", "E3"]})
    res = _check_pk_integrity(df, "equipment", _EQUIP_SCHEMA, _SETTINGS)
    assert res["status"] == PASS
    assert res["duplicate_rows"] == 0


def test_pk_integrity_warns_on_duplicates():
    df = pd.DataFrame({"equipment_id": ["E1", "E1", "E2"]})
    res = _check_pk_integrity(df, "equipment", _EQUIP_SCHEMA, _SETTINGS)
    assert res["status"] == WARN
    assert res["duplicate_rows"] == 2


def test_pk_integrity_blocks_when_pk_nulls_exceed_threshold():
    df = pd.DataFrame({"equipment_id": [None, None, "E1"]})
    res = _check_pk_integrity(df, "equipment", _EQUIP_SCHEMA, _SETTINGS)
    assert res["status"] == BLOCK


def test_pk_integrity_no_constraint_is_pass():
    res = _check_pk_integrity(pd.DataFrame({"x": [1]}), "work_order", {}, _SETTINGS)
    assert res["status"] == PASS


def test_equipment_connectivity_all_linked_passes():
    tables = {
        "equipment": pd.DataFrame({"equipment_id": ["E1", "E2"]}),
        "work_order": pd.DataFrame({"equipment_id": ["E1", "E2", "E1"]}),
    }
    cfg = {
        "equipment_id_columns": ["equipment_id"],
        "equipment_linked_tables": {"work_order": {"ref_columns": ["equipment_id"]}},
        "settings": _SETTINGS,
    }
    res = _check_equipment_connectivity(tables, cfg)
    assert res["status"] == PASS
    assert res["total_orphaned"] == 0


def test_equipment_connectivity_flags_orphans_over_threshold():
    tables = {
        "equipment": pd.DataFrame({"equipment_id": ["E1"]}),
        "work_order": pd.DataFrame({"equipment_id": ["E1", "GHOST", "GHOST2"]}),
    }
    cfg = {
        "equipment_id_columns": ["equipment_id"],
        "equipment_linked_tables": {"work_order": {"ref_columns": ["equipment_id"]}},
        "settings": _SETTINGS,
    }
    res = _check_equipment_connectivity(tables, cfg)
    assert res["status"] == WARN
    assert res["total_orphaned"] == 2


def test_equipment_connectivity_blocks_on_empty_equipment():
    res = _check_equipment_connectivity({"equipment": pd.DataFrame()}, {})
    assert res["status"] == BLOCK


def test_referential_integrity_resolved_passes():
    tables = {
        "child": pd.DataFrame({"pid": ["A", "B"]}),
        "parent": pd.DataFrame({"id": ["A", "B", "C"]}),
    }
    fk = [
        {
            "check_name": "child_pid_fk",
            "child_table": "child",
            "child_col": "pid",
            "parent_table": "parent",
            "parent_col": "id",
        }
    ]
    res = _check_referential_integrity(tables, fk, _SETTINGS)
    assert res["child_pid_fk"]["status"] == PASS
    assert res["child_pid_fk"]["unresolved"] == 0


def test_referential_integrity_warns_on_unresolved():
    tables = {
        "child": pd.DataFrame({"pid": ["A", "X", "Y"]}),
        "parent": pd.DataFrame({"id": ["A"]}),
    }
    fk = [
        {
            "check_name": "child_pid_fk",
            "child_table": "child",
            "child_col": "pid",
            "parent_table": "parent",
            "parent_col": "id",
        }
    ]
    res = _check_referential_integrity(tables, fk, _SETTINGS)
    assert res["child_pid_fk"]["status"] == WARN
    assert res["child_pid_fk"]["unresolved"] == 2
    assert set(res["child_pid_fk"]["unresolved_samples"]) == {"X", "Y"}


def test_referential_integrity_skips_missing_table():
    fk = [{"check_name": "x", "child_table": "a", "parent_table": "b"}]
    res = _check_referential_integrity({}, fk, _SETTINGS)
    assert res["x"]["status"] == PASS


def test_data_quality_basics_empty_is_pass():
    res = _check_data_quality_basics(pd.DataFrame(), "equipment", _SETTINGS, {})
    assert res["status"] == PASS
    assert res["rows"] == 0


def test_data_quality_basics_counts_full_duplicate_rows():
    df = pd.DataFrame({"a": ["1", "1", "2"], "b": ["x", "x", "y"]})
    res = _check_data_quality_basics(df, "equipment", _SETTINGS, {})
    assert res["full_duplicate_rows"] == 2  # the two identical rows


def test_data_quality_basics_summarizes_confidence():
    df = pd.DataFrame({"a": ["1", "2"], "confidence": ["0.9", "0.4"]})
    res = _check_data_quality_basics(df, "equipment", _SETTINGS, {})
    assert res["confidence_stats"]["below_0_5_pct"] == 50.0
    assert res["confidence_stats"]["mean"] == 0.65
