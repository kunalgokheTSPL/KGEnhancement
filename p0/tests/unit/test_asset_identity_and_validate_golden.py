"""Golden tests for equipment identity and the pipeline validation gate."""

from __future__ import annotations

import json

import pandas as pd

from p0.utils.asset_identity import (
    assign_equipment_uid,
    fix_connectivity_refs,
    make_equipment_uid,
)
from p0.utils.validate import (
    log_validation_report,
    report_to_json,
    run_validations,
)


def test_make_equipment_uid_is_deterministic_and_distinct():
    uid = make_equipment_uid("plant_1", "PUMP101")
    assert uid == make_equipment_uid("plant_1", "PUMP101")
    assert uid.startswith("eq:")
    assert uid != make_equipment_uid("plant_1", "VALVE202")
    assert uid != make_equipment_uid("plant_2", "PUMP101")


def test_assign_equipment_uid_fills_blanks_from_normalized_asset():
    df = pd.DataFrame(
        {"normalized_asset": ["PUMP101", "VALVE202"], "equipment_uid": ["", ""]}
    )
    out = assign_equipment_uid(df, "plant_1")
    assert out.iloc[0]["equipment_uid"] == make_equipment_uid("plant_1", "PUMP101")
    assert out.iloc[1]["equipment_uid"] == make_equipment_uid("plant_1", "VALVE202")


def test_assign_equipment_uid_leaves_empty_asset_blank():
    df = pd.DataFrame({"normalized_asset": [""], "equipment_uid": [""]})
    assert assign_equipment_uid(df, "plant_1").iloc[0]["equipment_uid"] == ""


def test_assign_equipment_uid_only_fills_blanks_not_existing_ids():
    df = pd.DataFrame({"normalized_asset": ["PUMP101"], "equipment_uid": ["preset"]})
    out = assign_equipment_uid(df, "plant_1", overwrite_blank=True)
    assert out.iloc[0]["equipment_uid"] == "preset"


def test_fix_connectivity_refs_replaces_row_index_with_tag():
    equip = pd.DataFrame({"normalized_asset": ["PUMP101", "VALVE202"]})
    rels = pd.DataFrame({"from_equipment_ref": ["0"], "to_equipment_ref": ["1"]})
    out = fix_connectivity_refs(rels, equip)
    assert out.iloc[0]["from_equipment_ref"] == "PUMP101"
    assert out.iloc[0]["to_equipment_ref"] == "VALVE202"


def test_run_validations_passes_with_required_columns():
    datasets = {"equipment": pd.DataFrame({"equipment_id": ["E1"], "name": ["Pump"]})}
    cfg = {"datasets": {"equipment": {"required_columns": ["equipment_id"]}}}
    rep = run_validations(datasets, cfg)
    assert rep["status"] == "PASS"
    assert rep["datasets"]["equipment"]["status"] == "PASS"


def test_run_validations_fails_on_missing_required_column():
    datasets = {"equipment": pd.DataFrame({"name": ["Pump"]})}
    cfg = {"datasets": {"equipment": {"required_columns": ["equipment_id"]}}}
    rep = run_validations(datasets, cfg)
    assert rep["status"] == "FAIL"
    assert "equipment_id" in rep["datasets"]["equipment"]["missing_required_columns"]


def test_run_validations_fails_on_zero_rows():
    datasets = {"equipment": pd.DataFrame({"equipment_id": [], "name": []})}
    cfg = {"datasets": {"equipment": {"required_columns": ["equipment_id"]}}}
    rep = run_validations(datasets, cfg)
    assert rep["status"] == "FAIL"
    assert "produced 0 rows" in rep["datasets"]["equipment"]["problems"]


def test_run_validations_fails_on_all_null_required_column():
    datasets = {"equipment": pd.DataFrame({"equipment_id": [None, None]})}
    cfg = {"datasets": {"equipment": {"required_columns": ["equipment_id"]}}}
    rep = run_validations(datasets, cfg)
    assert rep["status"] == "FAIL"
    assert rep["datasets"]["equipment"]["required_null_fraction"]["equipment_id"] == 1.0


def test_run_validations_honours_a_tightened_null_limit():
    datasets = {"equipment": pd.DataFrame({"equipment_id": ["E1", None, None, None]})}
    loose = {"datasets": {"equipment": {"required_columns": ["equipment_id"]}}}
    tight = {
        "datasets": {
            "equipment": {"required_columns": ["equipment_id"], "max_null_fraction": 0.5}
        }
    }
    assert run_validations(datasets, loose)["status"] == "PASS"
    assert run_validations(datasets, tight)["status"] == "FAIL"


def test_run_validations_fails_when_no_dataset_reached_it():
    rep = run_validations({}, {"datasets": {"equipment": {"required_columns": ["x"]}}})
    assert rep["status"] == "FAIL"
    assert rep["problems"] == ["no datasets reached validation"]


def test_run_validations_reads_the_shipped_contract_section():
    datasets = {"documents": pd.DataFrame({"title": ["a manual"]})}
    cfg = {"required_after_rename_or_derive": {"documents": ["document_id", "title"]}}
    rep = run_validations(datasets, cfg)
    assert rep["status"] == "FAIL"
    assert "document_id" in rep["datasets"]["documents"]["missing_required_columns"]


def test_run_validations_flags_a_dataset_with_no_contract():
    datasets = {"surprise": pd.DataFrame({"a": [1]})}
    rep = run_validations(datasets, {"required_after_rename_or_derive": {}})
    assert rep["status"] == "FAIL"
    assert "no contract" in rep["datasets"]["surprise"]["problems"][0]


def test_log_validation_report_emits_one_error_per_problem():
    class _Log:
        def __init__(self):
            self.errors = []

        def info(self, *args):
            pass

        def error(self, *args):
            self.errors.append(args)

    datasets = {"equipment": pd.DataFrame({"equipment_id": []})}
    cfg = {"datasets": {"equipment": {"required_columns": ["equipment_id"]}}}
    rep = run_validations(datasets, cfg)
    sink = _Log()
    log_validation_report(rep, sink)
    assert len(sink.errors) == 1 + len(rep["problems"])


def test_report_to_json_roundtrips():
    rep = {"status": "PASS", "datasets": {}}
    assert json.loads(report_to_json(rep)) == rep
