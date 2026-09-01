"""User config layers: standard templates stay untouched, overrides live on top."""

from __future__ import annotations

import pathlib

import yaml

from p0.api.services.user_config_store import deep_merge

SCHEMA = pathlib.Path(__file__).resolve().parents[2] / "config" / "templates" / "_common" / "schema.yaml"
CONFIG_ROUTER = (
    pathlib.Path(__file__).resolve().parents[2] / "api" / "routers" / "config_router.py"
).read_text()
PLANTS = (pathlib.Path(__file__).resolve().parents[2] / "api" / "plants.py").read_text()
STORE = (
    pathlib.Path(__file__).resolve().parents[2]
    / "api"
    / "services"
    / "user_config_store.py"
).read_text()


def _tables():
    doc = yaml.safe_load(SCHEMA.read_text())
    return doc.get("tables") or doc.get("schema") or {}


def test_user_config_table_exists_with_scope():
    columns = _tables()["user_config"]["columns"]
    for expected in ("plant_code_id", "scope", "config", "version", "updated_by", "updated_at"):
        assert expected in columns


def test_scope_is_unique_per_plant():
    indexes = _tables()["user_config"]["indexes"]
    unique = [i for i in indexes if i.get("unique")]
    assert any(i["columns"] == ["plant_code_id", "scope"] for i in unique)


def test_history_table_keeps_before_and_after():
    columns = _tables()["user_config_history"]["columns"]
    for expected in ("action", "patch", "config_before", "config_after", "actor", "ts"):
        assert expected in columns


def test_merge_is_recursive_so_a_patch_appends_rather_than_replaces():
    base = {"document_processing": {"document_types": {"sop": {"source_columns": ["a"]}}}}
    patch = {"document_processing": {"document_types": {"rca": {"source_columns": ["b"]}}}}
    merged = deep_merge(base, patch)
    assert set(merged["document_processing"]["document_types"]) == {"sop", "rca"}


def test_merge_does_not_mutate_its_inputs():
    base = {"a": {"b": 1}}
    patch = {"a": {"c": 2}}
    deep_merge(base, patch)
    assert base == {"a": {"b": 1}}
    assert patch == {"a": {"c": 2}}


def test_a_scalar_override_wins():
    assert deep_merge({"retention": {"value": 90}}, {"retention": {"value": 30}}) == {
        "retention": {"value": 30}
    }


def test_layer_precedence_is_template_then_global_then_plant():
    template = {"industry": "cement", "retention": {"value": 365}}
    global_layer = {"retention": {"value": 90}}
    plant_layer = {"retention": {"value": 30}}
    effective = deep_merge(deep_merge(template, global_layer), plant_layer)
    assert effective["retention"]["value"] == 30
    assert effective["industry"] == "cement"


def test_a_plant_with_no_overrides_gets_the_template_unchanged():
    template = {"industry": "cement"}
    assert deep_merge(deep_merge(template, {}), {}) == template


def test_the_store_never_writes_to_the_template():
    """Standard templates are read-only; overrides go to the user_config table."""
    assert "USER_CONFIG_FILE" not in STORE
    assert "save_yaml" not in STORE


def test_get_reports_whether_the_plant_is_customised():
    block = CONFIG_ROUTER.split("def getUserConfig(")[1]
    assert "is_customised" in block
    assert "using the standard template with no overrides" in block


def test_get_can_return_the_layers_separately():
    block = CONFIG_ROUTER.split("def getUserConfig(")[1]
    assert "include_layers" in block


def test_patch_writes_to_the_plant_layer():
    block = CONFIG_ROUTER.split("def patchUserConfig(")[1]
    assert "_user_config.write_layer(" in block
    assert "_user_config.PLANT_SCOPE" in block


def test_patch_records_the_actor():
    block = CONFIG_ROUTER.split("def patchUserConfig(")[1]
    assert "actor=actor" in block


def test_patch_does_not_store_plant_code_id_inside_the_config():
    block = CONFIG_ROUTER.split("def patchUserConfig(")[1]
    assert 'k != "plant_code_id"' in block


def test_plant_creation_initialises_an_empty_layer():
    assert "initialise_plant" in PLANTS


def test_init_is_idempotent():
    assert "ON CONFLICT (plant_code_id, scope) DO NOTHING" in STORE
