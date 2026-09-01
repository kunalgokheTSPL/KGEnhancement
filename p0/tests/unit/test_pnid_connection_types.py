"""Connection type handling."""

import re

import yaml

SOURCE = "p0/pipelines/run_pnid_from_pdfs.py"
RELS = "p0/config/templates/_common/relationships.yaml"
RENAME = "p0/config/templates/_common/column_rename/pnid_column_rename.yaml"


def _declared_types():
    rels = yaml.safe_load(open(RELS))["relationships"]
    return {str(v.get("type", "")).strip().upper() for v in rels.values() if v.get("type")}


def test_the_known_set_is_read_from_the_relationship_template():
    src = open(SOURCE).read()
    block = src.split("_KNOWN = ")[1][:400]
    assert "relationships_cfg" in block


def test_a_type_declared_in_the_template_is_not_coerced():
    assert "PART_OF_PROCESS_UNIT" in _declared_types()
    assert "LOCATED_AT" in _declared_types()


def test_an_unknown_type_is_reported_before_it_is_coerced():
    src = open(SOURCE).read()
    assert "coerced to " in src
    assert re.search(r"_coerced\s*=", src)


def test_the_connectivity_sheet_renames_through_its_own_source():
    cfg = yaml.safe_load(open(RENAME))
    assert "pid_connectivity" in cfg["sources"]
    assert cfg["sources"]["pid_connectivity"]["mappings"]["Type"] == "connection_type"
    assert cfg["sources"]["pid"]["mappings"]["Type"] == "equipment_type"


def test_the_pipeline_uses_the_connectivity_source_for_the_connectivity_frame():
    src = open(SOURCE).read()
    assert 'source_name=_cn_source' in src
    assert '"pid_connectivity"' in src
