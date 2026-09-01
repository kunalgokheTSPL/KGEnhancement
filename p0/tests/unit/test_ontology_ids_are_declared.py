"""Every ontology identity column exists in the CDM schema, or it is dropped on write."""

from __future__ import annotations

import pathlib

import yaml

TEMPLATES = (
    pathlib.Path(__file__).resolve().parents[2] / "config" / "templates" / "_common"
)
ONTOLOGY = yaml.safe_load((TEMPLATES / "ontology_template.yaml").read_text())
SCHEMA = yaml.safe_load((TEMPLATES / "schema.yaml").read_text())["schema"]

KNOWN_UNDECLARED_TABLES = {"equipment_sap"}


def _entities():
    def walk(node):
        if isinstance(node, dict):
            if "table" in node and "id_column" in node:
                yield node
            for value in node.values():
                yield from walk(value)
        elif isinstance(node, list):
            for value in node:
                yield from walk(value)

    return list(walk(ONTOLOGY))


def test_the_ontology_actually_declares_entities():
    assert len(_entities()) > 10


def test_every_declared_table_declares_its_ontology_id():
    missing = [
        f"{e['table']}.{e['id_column']}"
        for e in _entities()
        if e["table"] in SCHEMA
        and e["id_column"] not in SCHEMA[e["table"]].get("columns", {})
    ]
    assert not missing, f"ontology id columns absent from schema.yaml: {missing}"


def test_the_set_of_undeclared_ontology_tables_has_not_grown():
    absent = {e["table"] for e in _entities() if e["table"] not in SCHEMA}
    assert absent == KNOWN_UNDECLARED_TABLES


def test_the_connectivity_identity_column_is_declared():
    assert "connectivity_uid" in SCHEMA["equipment_connectivity"]["columns"]
