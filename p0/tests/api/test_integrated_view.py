"""The label-wise 6-layer view: template driven, one row per finding, honest about gaps."""

from __future__ import annotations

import pathlib

from p0.api.services import integrated_view as view
from p0.api.services import labels as labels_service

ROUTER = (
    pathlib.Path(__file__).resolve().parents[2] / "api" / "routers" / "integrated.py"
).read_text()


def test_every_label_has_a_template():
    assert set(view.known_labels()) == labels_service.VALID


def test_all_six_layers_are_declared_for_every_label():
    for label in view.known_labels():
        described = view.describe(label)
        assert [layer["layer"] for layer in described["layers"]] == list(view.LAYERS)


def test_every_column_declares_whether_it_is_backed():
    for label in view.known_labels():
        for column in view.describe(label)["columns"]:
            assert column["status"] in (view.BACKED, view.NOT_YET_BACKED)
            assert column["name"]


def test_the_backed_and_unbacked_counts_add_up():
    for label in view.known_labels():
        described = view.describe(label)
        assert described["backed"] + described["not_yet_backed"] == len(described["columns"])


def test_the_findings_tables_actually_back_columns():
    """Damage Mechanism was unbacked until LOPC landed — it must not still be."""
    columns = {c["name"]: c for c in view.describe("integrity")["columns"]}
    assert columns["Damage Mechanism"]["status"] == view.BACKED
    assert columns["Inspection Finding"]["status"] == view.BACKED


def test_a_field_can_declare_one_source_or_many():
    single = view._sources_of(
        {"source_table": "equipment", "source_column": "area", "key_column": "normalized_asset"}
    )
    assert single == [
        {"table": "equipment", "column": "area", "key_column": "normalized_asset", "label": None}
    ]
    many = view._sources_of(
        {
            "key_column": "normalized_asset",
            "sources": [
                {"table": "containment_event_lopc", "column": "damage_mechanism", "label": "LOPC"},
                {"table": "inspection_record_aif", "column": "corrosion_mechanism"},
            ],
        }
    )
    assert [s["table"] for s in many] == ["containment_event_lopc", "inspection_record_aif"]
    assert all(s["key_column"] == "normalized_asset" for s in many)


def test_a_field_with_no_usable_source_yields_nothing_rather_than_a_broken_lookup():
    assert view._sources_of({"status": "not_yet_backed", "note": "no column exists"}) == []
    assert view._sources_of({"source_table": "equipment"}) == []


def test_lookups_are_only_required_for_non_findings_tables():
    spec = {
        "layers": {
            "asset": {
                "fields": [
                    {"name": "A", "status": "backed", "source_table": "equipment",
                     "source_column": "area", "key_column": "normalized_asset"},
                    {"name": "B", "status": "backed", "source_table": "inspection_record_aif",
                     "source_column": "priority"},
                    {"name": "C", "status": "not_yet_backed"},
                ]
            }
        }
    }
    assert view._required_lookups(spec) == {("equipment", "normalized_asset")}


def test_a_row_carries_every_column_even_when_unbacked():
    spec = {
        "layers": {
            layer: {"fields": [{"name": f"{layer}-field", "status": "not_yet_backed",
                                "note": "no column yet"}]}
            for layer in view.LAYERS
        }
    }
    row = view._build_row(
        label="integrity", spec=spec, connector="aif",
        finding={"normalized_asset": "V-101", "notification_no": "1"}, lookups={},
    )
    for layer in view.LAYERS:
        cell = row["layers"][layer][f"{layer}-field"]
        assert cell["value"] is None
        assert cell["status"] == view.NOT_YET_BACKED
        assert cell["note"]


def test_a_row_reads_a_backed_field_straight_off_the_finding():
    spec = {
        "layers": {
            "performance": {
                "fields": [{"name": "Risk Score", "status": "backed",
                            "source_table": "inspection_record_aif", "source_column": "priority"}]
            }
        }
    }
    row = view._build_row(
        label="integrity", spec=spec, connector="aif",
        finding={"normalized_asset": "V-101", "notification_no": "9", "priority": 2}, lookups={},
    )
    assert row["layers"]["performance"]["Risk Score"]["value"] == 2


def test_a_row_falls_through_to_the_next_source_when_the_first_is_empty():
    spec = {
        "layers": {
            "knowledge": {
                "fields": [{
                    "name": "Damage Mechanism", "status": "backed",
                    "sources": [
                        {"table": "containment_event_lopc", "column": "damage_mechanism",
                         "label": "LOPC"},
                        {"table": "inspection_record_aif", "column": "corrosion_mechanism",
                         "label": "AIF"},
                    ],
                }]
            }
        }
    }
    row = view._build_row(
        label="integrity", spec=spec, connector="aif",
        finding={"normalized_asset": "V-101", "corrosion_mechanism": "CUI"}, lookups={},
    )
    cell = row["layers"]["knowledge"]["Damage Mechanism"]
    assert cell["value"] == "CUI"
    assert cell["source_label"] == "AIF"


def test_the_row_identifies_its_finding_and_how_the_asset_was_matched():
    row = view._build_row(
        label="integrity", spec={"layers": {}}, connector="gloc",
        finding={"normalized_asset": "BN-64L", "source_record_id": "k1",
                 "asset_match_method": "equipment_tag", "asset_match_confidence": 0.95},
        lookups={},
    )
    assert row["finding_ref"] == "k1"
    assert row["source_connector"] == "gloc"
    assert row["source_table"] == "containment_event_gloc"
    assert row["asset_match_method"] == "equipment_tag"


def test_each_connector_has_a_finding_reference_column():
    assert set(view._FINDING_REF) == set(view._FINDING_TABLES)


def test_values_are_rendered_as_json_safe_scalars():
    from datetime import datetime

    assert view._cell(datetime(2024, 1, 2)) == "2024-01-02T00:00:00"
    assert view._cell(float("nan")) is None
    assert view._cell(None) is None
    assert view._cell(3) == 3


def test_the_four_endpoints_exist():
    for name in (
        "listIntegratedLabels", "getIntegratedColumns", "getIntegratedView",
        "recomputeIntegratedView",
    ):
        assert f"def {name}(" in ROUTER


def test_the_page_size_default_and_cap_match_the_agreed_contract():
    assert view.DEFAULT_PAGE_SIZE == 50
    assert view.MAX_PAGE_SIZE == 200
    assert "_view.DEFAULT_PAGE_SIZE, ge=1, le=_view.MAX_PAGE_SIZE" in ROUTER


def test_every_success_path_returns_the_standard_envelope():
    """EnvelopeRoute only wraps errors — success bodies must be explicit."""
    assert ROUTER.count('"success": True') == 3
    assert "paginated_envelope(" in ROUTER


def test_an_unknown_label_is_rejected_with_the_allowed_set():
    assert "is not a valid label. Use one of:" in ROUTER
    assert "UNPROCESSABLE_ENTITY" in ROUTER


def test_rows_are_computed_then_materialised_into_one_table():
    assert "def materialise(" in dir(view) or hasattr(view, "materialise")
    assert "INSERT INTO integrated_view" in (
        pathlib.Path(__file__).resolve().parents[2]
        / "api" / "services" / "integrated_view.py"
    ).read_text()


def test_the_view_table_is_declared_in_the_schema():
    from p0.api.config import SCHEMA_FROZEN_FILE
    from p0.api.deps import load_yaml

    schema = load_yaml(str(SCHEMA_FROZEN_FILE))
    tables = schema.get("schema") or schema.get("tables") or {}
    columns = (tables.get("integrated_view") or {}).get("columns") or {}
    for column in ("label", "normalized_asset", "finding_ref", "source_connector", "layers"):
        assert column in columns
