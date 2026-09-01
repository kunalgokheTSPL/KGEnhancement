"""Pins the timeseries to equipment HAS_TAG wiring end to end."""

from __future__ import annotations

import contextlib
import logging
import pathlib

import pandas as pd
import yaml

import p0
from p0.utils.canonical_relationships import build_asset_relationships


class _Sink(logging.Handler):
    """Collects warning text off one named logger."""

    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.text = ""

    def emit(self, record: logging.LogRecord) -> None:
        self.text += record.getMessage() + "\n"


@contextlib.contextmanager
def _warnings_from(name: str):
    """Capture warnings despite the shared test base disabling logging globally."""
    sink = _Sink()
    logger = logging.getLogger(name)
    previous_disable = logging.root.manager.disable
    previous_level = logger.level
    logging.disable(logging.NOTSET)
    logger.addHandler(sink)
    logger.setLevel(logging.WARNING)
    try:
        yield sink
    finally:
        logger.removeHandler(sink)
        logger.setLevel(previous_level)
        logging.disable(previous_disable)

_TEMPLATES = pathlib.Path(p0.__file__).parent / "config" / "templates" / "_common"


def _load(name: str) -> dict:
    return yaml.safe_load((_TEMPLATES / name).read_text())


def test_has_tag_names_entities_that_actually_exist():
    """A relationship naming an undefined entity is skipped, so no edge is ever built."""
    entities = set((_load("entities.yaml").get("entities") or {}).keys())
    rel = (_load("relationships.yaml").get("relationships") or {})["equipment_timeseries"]
    assert rel["from_entity"] in entities
    assert rel["to_entity"] in entities


def test_has_tag_dedupe_keys_cover_both_sides_identity():
    entities = _load("entities.yaml")["entities"]
    rel = _load("relationships.yaml")["relationships"]["equipment_timeseries"]
    required = set(entities[rel["from_entity"]]["identity_keys"]) | set(
        entities[rel["to_entity"]]["identity_keys"]
    )
    assert required.issubset(set(rel["dedupe_keys"]))


def test_a_relationship_whose_source_is_absent_is_logged():
    with _warnings_from("p0.utils.canonical_relationships") as sink:
        build_asset_relationships(
            post_sources={},
            relationships_cfg=_load("relationships.yaml"),
            entities_cfg=_load("entities.yaml"),
            schema_cfg=_load("schema.yaml"),
            entity_uid_tables={},
        )
    assert "not among" in sink.text


def test_has_tag_edges_need_the_equipment_uid_table():
    """The uid table lives in the pnid output, so the ts stage has to read it in."""
    ts = pd.DataFrame(
        [
            {"plant_code_id": "P1", "normalized_asset": "K-2410A", "tag_name": "T1"},
            {"plant_code_id": "P1", "normalized_asset": "K-2410A", "tag_name": "T2"},
        ]
    )
    uids = {
        "timeseries_metadata": pd.DataFrame(
            [
                {"ts_uid": "t1", "plant_code_id": "P1", "tag_name": "T1"},
                {"ts_uid": "t2", "plant_code_id": "P1", "tag_name": "T2"},
            ]
        ),
        "equipment": pd.DataFrame(
            [{"equipment_uid": "e1", "plant_code_id": "P1", "normalized_asset": "K-2410A"}]
        ),
    }
    args = dict(
        post_sources={"timeseries": ts},
        relationships_cfg=_load("relationships.yaml"),
        entities_cfg=_load("entities.yaml"),
        schema_cfg=_load("schema.yaml"),
    )
    with_eq = build_asset_relationships(entity_uid_tables=uids, **args)
    without_eq = build_asset_relationships(
        entity_uid_tables={"timeseries_metadata": uids["timeseries_metadata"]}, **args
    )
    assert len(with_eq) == 2
    assert set(with_eq["relationship_type"]) == {"HAS_TAG"}
    assert len(without_eq) == 0
