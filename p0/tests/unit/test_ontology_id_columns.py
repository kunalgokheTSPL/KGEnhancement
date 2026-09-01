"""Ontology node ids must be columns the pipeline actually writes."""

import yaml

ONTOLOGY = "p0/config/templates/_common/ontology_template.yaml"
SCHEMA = "p0/config/templates/_common/schema.yaml"

DUAL_KEYED = {"equipment_connectivity": {"conn_uid", "connectivity_uid"}}


def _tables():
    return yaml.safe_load(open(SCHEMA))["schema"]


def _nodes():
    return yaml.safe_load(open(ONTOLOGY))["node_types"]


def test_every_node_id_column_is_one_the_pipeline_writes():
    tables = _tables()
    mismatched = []
    for node in _nodes():
        table = node["table"]
        if table not in tables:
            continue
        allowed = DUAL_KEYED.get(table) or {tables[table].get("primary_key")}
        if node["id_column"] not in allowed:
            mismatched.append((node["label"], node["id_column"], sorted(allowed)))
    assert mismatched == []


def test_the_dual_keyed_table_really_declares_both_names():
    tables = _tables()
    columns = tables["equipment_connectivity"]["columns"]
    assert "conn_uid" in columns
    source = open("p0/pipelines/run_pnid_from_pdfs.py").read()
    assert 'equipment_connection["conn_uid"]' in source
    assert 'equipment_connection["connectivity_uid"]' in source


def test_only_the_sap_equipment_node_lacks_a_schema_table():
    tables = _tables()
    missing = [n["label"] for n in _nodes() if n["table"] not in tables]
    assert missing == ["EquipmentSAP"]
