"""A catalog type whose records cannot join to anything is worthless downstream."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

CATALOG = (
    Path(__file__).resolve().parents[2]
    / "config"
    / "templates"
    / "_common"
    / "document_types.yaml"
)

JOIN_KEYS = {"Equipment Tag", "Cause Tag", "Effect Tag", "Material Code"}

AREA_KEYS = {
    "Unit",
    "Applicable Unit",
    "Used In Unit",
    "Unit / Equipment",
    "Location",
    "System",
    "Node / System",
}


def _types():
    doc = yaml.safe_load(CATALOG.read_text())
    return [t for cat in doc["categories"] for t in cat["types"]]


ALL_TYPES = _types()
IDS = [t["key"] for t in ALL_TYPES]


@pytest.mark.parametrize("doc_type", ALL_TYPES, ids=IDS)
def test_every_type_can_join_a_record_to_an_identified_subject(doc_type):
    names = {f["name"] for f in doc_type["fields"]}

    assert names & JOIN_KEYS


@pytest.mark.parametrize("doc_type", ALL_TYPES, ids=IDS)
def test_every_type_places_its_records_in_a_plant_area(doc_type):
    names = {f["name"] for f in doc_type["fields"]}

    assert names & AREA_KEYS


@pytest.mark.parametrize("doc_type", ALL_TYPES, ids=IDS)
def test_a_field_is_never_declared_twice_in_one_type(doc_type):
    names = [f["name"] for f in doc_type["fields"]]

    assert len(names) == len(set(names))


@pytest.mark.parametrize("doc_type", ALL_TYPES, ids=IDS)
def test_a_description_says_more_than_the_field_name_already_does(doc_type):
    for field in doc_type["fields"]:
        desc = field.get("desc", "")

        assert len(desc.split()) >= 4, f"{doc_type['key']}.{field['name']}"


def test_no_two_types_share_a_key():
    keys = [t["key"] for t in ALL_TYPES]

    assert len(keys) == len(set(keys))


def test_every_type_is_labelled_for_the_upload_modal():
    assert all(t.get("label") for t in ALL_TYPES)
