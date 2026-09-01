"""
Docs upload persists a doc-type's extraction fields to user_config.yaml.

What we're protecting:
  1. Document extraction silently producing 0 rows (blank review) because a
     custom doc_type (e.g. 'my_custom_logs') had no source_columns config. The
     upload now persists the type's fields so the pipeline can extract it.
  2. Task-2 guardrail: for a STANDARD type→subtype combo, the backend persists
     the FULL predefined field set (template defaults ∪ incoming), so an edited
     or partial field list from the frontend can't silently drop standard
     columns from extraction.
"""

from __future__ import annotations

import pathlib
import tempfile

import pytest

from p0.api.services import doc_types as conn
from p0.api import config as cfg
from p0.api.deps import load_yaml


@pytest.fixture
def _tmp_user_config():
    tmp = pathlib.Path(tempfile.mktemp(suffix=".yaml"))
    orig_cfg = cfg.USER_CONFIG_FILE
    orig_conn = conn.USER_CONFIG_FILE
    cfg.USER_CONFIG_FILE = tmp
    conn.USER_CONFIG_FILE = tmp
    yield tmp
    cfg.USER_CONFIG_FILE = orig_cfg
    conn.USER_CONFIG_FILE = orig_conn
    if tmp.exists():
        tmp.unlink()


def _types(tmp):
    return load_yaml(tmp).get("document_processing", {}).get("document_types", {})




def test_persists_comma_separated_fields_custom(_tmp_user_config):
    conn._persist_doc_type_config(
        "my_custom_logs", "My Custom Logs", "Field A, Field B, Field C"
    )
    dt = _types(_tmp_user_config)["my_custom_logs"]
    assert dt["document_type"] == "My Custom Logs"
    assert dt["source_columns"] == ["Field A", "Field B", "Field C"]


def test_persists_json_list_fields_custom(_tmp_user_config):
    conn._persist_doc_type_config(
        "custom_jsa", "Custom JSA", '["Step", "Hazard", "Control"]'
    )
    assert _types(_tmp_user_config)["custom_jsa"]["source_columns"] == [
        "Step",
        "Hazard",
        "Control",
    ]


def test_empty_and_unknown_is_noop(_tmp_user_config):
    conn._persist_doc_type_config("totally_unknown_x", "X", "")
    assert "totally_unknown_x" not in _types(_tmp_user_config)
    conn._persist_doc_type_config("totally_unknown_y", "Y", None)
    assert "totally_unknown_y" not in _types(_tmp_user_config)


def test_only_target_type_touched(_tmp_user_config):
    conn._persist_doc_type_config("custom_a", "A", "f1")
    conn._persist_doc_type_config("custom_b", "B", "f2")
    types = _types(_tmp_user_config)
    assert set(types.keys()) == {"custom_a", "custom_b"}
    assert types["custom_a"]["source_columns"] == ["f1"]
    assert types["custom_b"]["source_columns"] == ["f2"]




def test_standard_subtype_with_no_incoming_gets_template_defaults(_tmp_user_config):
    template, label = conn._template_fields_for_subtype("hazop")
    assert template, "hazop must be a known catalog subtype with fields"
    conn._persist_doc_type_config("hazop", None, None)
    dt = _types(_tmp_user_config)["hazop"]
    assert dt["source_columns"] == template
    assert dt["document_type"] == label


def test_standard_subtype_union_appends_user_extras(_tmp_user_config):
    template, _ = conn._template_fields_for_subtype("hazop")
    conn._persist_doc_type_config("hazop", "HAZOP", ["Node", "My Extra Field"])
    cols = _types(_tmp_user_config)["hazop"]["source_columns"]
    for f in template:
        assert f in cols
    assert cols[-1] == "My Extra Field"
    assert sum(1 for c in cols if c.lower() == "node") == 1


def test_standard_subtype_partial_incoming_does_not_drop_fields(_tmp_user_config):
    template, _ = conn._template_fields_for_subtype("equipment_datasheets")
    conn._persist_doc_type_config(
        "equipment_datasheets", "Equipment Datasheets", ["Equipment Tag"]
    )
    cols = _types(_tmp_user_config)["equipment_datasheets"]["source_columns"]
    assert set(template).issubset(set(cols)), "partial upload must not drop fields"
    assert len(cols) == len(template)


def test_template_lookup_unknown_returns_empty():
    fields, label = conn._template_fields_for_subtype("not_a_real_subtype")
    assert fields == []
    assert label is None
