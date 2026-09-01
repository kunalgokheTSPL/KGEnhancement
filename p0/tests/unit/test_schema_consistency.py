"""Consistency checks for the frozen CDM schema (schema.yaml).

A bad primary key or dangling foreign key here breaks plant provisioning for EVERY
plant (the whole schema is materialised on plant creation), so pin the invariants:
every PK is a real column, and every FK references a table + column that exist. Pure
YAML — no DB, no cluster.
"""

from __future__ import annotations

from pathlib import Path

import yaml

_SCHEMA_FILE = (
    Path(__file__).resolve().parents[3]
    / "p0"
    / "config"
    / "templates"
    / "_common"
    / "schema.yaml"
)
_TABLES: dict = (yaml.safe_load(_SCHEMA_FILE.read_text()) or {}).get("schema", {})


def test_schema_loads_with_tables():
    assert len(_TABLES) > 20, f"only {len(_TABLES)} tables — schema.yaml may have broken"


def test_every_primary_key_is_a_real_column():
    bad = []
    for table, tdef in _TABLES.items():
        pk = tdef.get("primary_key")
        cols = tdef.get("columns", {}) or {}
        if not pk:
            bad.append(f"{table}: no primary_key")
        elif pk not in cols:
            bad.append(f"{table}: primary_key '{pk}' is not a column")
    assert not bad, bad


def test_every_foreign_key_references_an_existing_table_and_column():
    bad = []
    for table, tdef in _TABLES.items():
        cols = tdef.get("columns", {}) or {}
        for fk in tdef.get("foreign_keys", []) or []:
            col, ref_table, ref_col = (
                fk.get("column"),
                fk.get("ref_table"),
                fk.get("ref_column"),
            )
            if col not in cols:
                bad.append(f"{table}: FK column '{col}' not in {table}")
            if ref_table not in _TABLES:
                bad.append(f"{table}: FK -> unknown table '{ref_table}'")
            elif ref_col not in (_TABLES[ref_table].get("columns", {}) or {}):
                bad.append(f"{table}: FK -> {ref_table}.'{ref_col}' is not a column")
    assert not bad, bad
