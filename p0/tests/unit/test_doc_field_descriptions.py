"""The catalog says what each field means; the model must be told."""

from __future__ import annotations

import os

os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

from p0.source_processing.documents import user_doc_extract as ude
from p0.utils import doc_fields

COLS = ["Equipment Tag", "Manufacturer"]


def test_the_catalog_yields_a_description_for_a_known_type():
    descs = doc_fields.catalog_field_descriptions("equipment_datasheets")

    assert descs["Equipment Tag"]
    assert descs["Manufacturer"]


def test_an_unknown_type_yields_no_descriptions():
    assert doc_fields.catalog_field_descriptions("not_a_real_type") == {}


def test_the_type_key_is_matched_regardless_of_case():
    assert doc_fields.catalog_field_descriptions(
        "EQUIPMENT_DATASHEETS"
    ) == doc_fields.catalog_field_descriptions("equipment_datasheets")


def test_run_config_overrides_the_catalog_for_one_field():
    cfg = {"field_descriptions": {"Equipment Tag": "the plant's own tag scheme"}}
    descs = doc_fields.resolve_field_descriptions("equipment_datasheets", cfg)

    assert descs["Equipment Tag"] == "the plant's own tag scheme"
    assert descs["Manufacturer"] == doc_fields.catalog_field_descriptions(
        "equipment_datasheets"
    )["Manufacturer"]


def test_a_configured_field_the_catalog_never_heard_of_still_lands():
    cfg = {"field_descriptions": {"Skid Number": "which skid it is mounted on"}}
    descs = doc_fields.resolve_field_descriptions("equipment_datasheets", cfg)

    assert descs["Skid Number"] == "which skid it is mounted on"


def test_blank_descriptions_are_not_treated_as_descriptions():
    cfg = {"field_descriptions": {"Manufacturer": "   "}}
    descs = doc_fields.resolve_field_descriptions("equipment_datasheets", cfg)

    assert descs["Manufacturer"].strip()


def test_the_tool_schema_carries_the_description_for_each_field():
    descs = {"Equipment Tag": "unique equipment identifier"}
    props = ude._extract_tool(COLS, descs)["input_schema"]["properties"]["entries"][
        "items"
    ]["properties"]

    assert "unique equipment identifier" in props["Equipment Tag"]["description"]
    assert "Verbatim Manufacturer" in props["Manufacturer"]["description"]


def test_the_tool_schema_still_builds_without_any_descriptions():
    props = ude._extract_tool(COLS)["input_schema"]["properties"]["entries"]["items"][
        "properties"
    ]

    assert set(props) == set(COLS)
    for col in COLS:
        assert f"Verbatim {col}" in props[col]["description"]


def test_a_described_field_keeps_the_verbatim_instruction():
    descs = {"Equipment Tag": "unique equipment identifier"}
    prop = ude._extract_tool(COLS, descs)["input_schema"]["properties"]["entries"][
        "items"
    ]["properties"]["Equipment Tag"]

    assert "Verbatim" in prop["description"]


def test_the_prompt_lists_the_description_beside_the_field_name():
    descs = {"Equipment Tag": "unique equipment identifier"}
    prompt = ude._build_prompt(COLS, "Equipment Datasheets", descs)

    assert "Equipment Tag — unique equipment identifier" in prompt
    assert "  - Manufacturer\n" in prompt


def test_the_prompt_is_unchanged_when_no_descriptions_are_given():
    assert ude._build_prompt(COLS, "Equipment Datasheets") == ude._build_prompt(
        COLS, "Equipment Datasheets", {}
    )


def test_every_catalog_type_describes_all_of_its_fields():
    catalog = doc_fields._catalog()
    assert catalog

    for type_key, descs in catalog.items():
        assert descs, f"{type_key} declares no field descriptions"
