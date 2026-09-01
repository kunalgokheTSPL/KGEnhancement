"""Tests for the canonical IoTDB measurement / tag-id normalization.

These guard the metadata↔values join key. The same rule must map a tag name to
its IoTDB measurement everywhere (ingest, pipeline, worker) or the join silently
breaks. They also cover the collision fix: two distinct tags that normalize to the
same name must NOT silently overwrite each other on a device.

All pure — no DB / IoTDB.
"""

from __future__ import annotations

from p0.utils.measurement import (
    normalize_measurement,
    disambiguate_measurement,
    dedupe_measurements,
)




def test_normalize_replaces_separators_with_underscore():
    assert normalize_measurement("1300FI-685") == "1300FI_685"
    assert normalize_measurement("1300FI.685") == "1300FI_685"
    assert normalize_measurement("FI/685") == "FI_685"
    assert normalize_measurement("FI 685") == "FI_685"


def test_normalize_strips_invalid_chars():
    assert normalize_measurement("Temp(°C)") == "TempC"
    assert normalize_measurement("tag#1") == "tag1"
    assert normalize_measurement("已经") == ""


def test_normalize_is_stable_and_handles_non_str():
    assert normalize_measurement("123") == "123"
    assert normalize_measurement("") == ""
    assert normalize_measurement(None) == "None"
    assert normalize_measurement("FI_685") == "FI_685"




def test_disambiguate_leaves_valid_names_unchanged():
    assert disambiguate_measurement("FI_685") == "FI_685"


def test_disambiguate_suffixes_lossy_names_stably():
    a = disambiguate_measurement("FI-685")
    b = disambiguate_measurement("FI.685")
    assert a.startswith("FI_685_") and b.startswith("FI_685_")
    assert a != b
    assert disambiguate_measurement("FI-685") == a




def test_dedupe_no_collision_returns_unchanged():
    names = ["FI_685", "Temp_C", "Flow_1"]
    out, collisions = dedupe_measurements(names)
    assert out == names
    assert collisions == []


def test_dedupe_collision_keeps_both_and_reports():
    out, collisions = dedupe_measurements(["FI-685", "FI.685", "Pressure"])
    assert out[0] != out[1]
    assert out[2] == "Pressure"
    assert len(collisions) == 1
    assert set(collisions[0]["raw_names"]) == {"FI-685", "FI.685"}
    assert collisions[0]["normalized"] == "FI_685"


def test_dedupe_three_way_collision_all_distinct():
    out, collisions = dedupe_measurements(["A.B", "A-B", "A/B"])
    assert len(set(out)) == 3
    assert len(collisions) == 1


def test_a_name_with_no_legal_characters_never_yields_a_blank_measurement():
    norm, _ = dedupe_measurements(["1300FI-685", "温度"])
    assert norm[0] == "1300FI_685"
    assert norm[1] and norm[1].startswith("m_")


def test_the_repair_does_not_depend_on_how_many_siblings_are_unnamable():
    one, _ = dedupe_measurements(["1300FI-685", "温度"])
    two, _ = dedupe_measurements(["1300FI-685", "温度", "***"])
    assert one[1] == two[1]
    assert "" not in one and "" not in two


def test_every_column_unnamable_still_yields_usable_measurements():
    norm, _ = dedupe_measurements(["温度", "***", "%"])
    assert all(m and m.startswith("m_") for m in norm)
    assert len(set(norm)) == 3


def test_repairing_a_blank_is_not_reported_as_a_collision():
    _, collisions = dedupe_measurements(["1300FI-685", "温度"])
    assert collisions == []


def test_ordinary_collisions_are_untouched_by_the_blank_repair():
    norm, collisions = dedupe_measurements(["FI-685", "FI.685"])
    assert len(collisions) == 1
    assert len(set(norm)) == 2
    assert all(m.startswith("FI_685_") for m in norm)
