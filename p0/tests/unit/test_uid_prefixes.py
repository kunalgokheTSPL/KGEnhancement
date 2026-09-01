"""Pins the uid namespace so an entity file and its uid table can be joined."""

from __future__ import annotations

import pathlib

import yaml

import p0
from p0.utils.canonical_entities import PREFIX_MAP

_ENTITIES = (
    pathlib.Path(p0.__file__).parent / "config" / "templates" / "_common" / "entities.yaml"
)


def test_the_pid_uid_namespace_matches_what_the_entity_file_writes():
    """It fell through to entity_name[:3] and wrote equ: while the file wrote pid:."""
    assert PREFIX_MAP["equipment_pid"] == "pid"
    assert PREFIX_MAP["equipment_connection"] == "conn"


def test_the_equipment_family_does_not_share_one_namespace():
    family = ["equipment", "equipment_pid", "equipment_connection", "equipment_sap"]
    prefixes = [PREFIX_MAP[name] for name in family]
    assert len(set(prefixes)) == len(prefixes), prefixes


def test_no_declared_entity_silently_falls_back_to_a_three_letter_stem():
    """A fallback prefix is how two entities end up sharing a uid space."""
    declared = set(yaml.safe_load(_ENTITIES.read_text())["entities"])
    fallbacks = {}
    for name in declared:
        if name not in PREFIX_MAP:
            fallbacks.setdefault(name[:3].lower(), []).append(name)
    collisions = {k: v for k, v in fallbacks.items() if len(v) > 1}
    assert not collisions, f"entities sharing a fallback uid prefix: {collisions}"


def test_the_sap_builder_and_the_uid_table_mint_the_same_namespace():
    import pandas as pd
    import yaml

    from p0.pipelines.canonical_builder_sap import _ensure_primary_key
    from p0.utils.canonical_entities import build_uid_tables_from_canonical_entities

    ent = yaml.safe_load(open("p0/config/templates/_common/entities.yaml"))
    sch = yaml.safe_load(open("p0/config/templates/_common/schema.yaml"))
    df = pd.DataFrame({"plant_code_id": ["M014"], "tag_name": ["TAG-1"]})
    built = _ensure_primary_key(
        df.copy(), "timeseries_metadata", ["plant_code_id", "tag_name"], "ts_uid"
    )
    tables = build_uid_tables_from_canonical_entities(
        {"timeseries_metadata": built.copy()}, ent, sch
    )
    assert set(built["ts_uid"]) == set(tables["timeseries_metadata"]["ts_uid"])


def test_every_declared_entity_has_a_distinct_prefix():
    import yaml

    from p0.utils.canonical_entities import uid_prefix

    ent = yaml.safe_load(open("p0/config/templates/_common/entities.yaml"))
    names = list((ent.get("entities") or ent).keys())
    prefixes = [uid_prefix(n) for n in names]
    assert len(set(prefixes)) == len(names)
