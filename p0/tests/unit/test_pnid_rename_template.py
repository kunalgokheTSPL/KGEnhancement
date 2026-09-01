"""P&ID rename template shape."""

import collections

import pandas as pd
import yaml

from p0.utils.renamer import apply_column_rename

CFG = "p0/config/templates/_common/column_rename/pnid_column_rename.yaml"
ENTITIES = "p0/config/templates/_common/entities.yaml"


def _cfg():
    return yaml.safe_load(open(CFG))


def _pid_attribute_sources():
    entities = yaml.safe_load(open(ENTITIES))
    ents = entities.get("entities") or entities
    return {
        ref[4:]
        for d in ents.values()
        for v in (d.get("attributes") or {}).values()
        for ref in (v.get("from") or [])
        if ref.startswith("pid.")
    }


def test_no_mapping_points_at_a_target_nothing_reads():
    targets = set(_cfg()["sources"]["pid"]["mappings"].values())
    assert targets - _pid_attribute_sources() == set()


def test_every_target_with_several_aliases_has_a_deterministic_winner():
    pid = _cfg()["sources"]["pid"]
    counts = collections.Counter(pid["mappings"].values())
    multi = {t for t, n in counts.items() if n > 1}
    assert multi - set(pid["target_priority"]) == set()


def test_priority_beats_the_uploaded_column_order():
    first = pd.DataFrame({"Description": ["generic"], "Equipment Description": ["real"]})
    second = first[["Equipment Description", "Description"]]
    a = apply_column_rename(first, _cfg(), "pid", verbose=False)
    b = apply_column_rename(second, _cfg(), "pid", verbose=False)
    assert a["description"].iloc[0] == "real"
    assert b["description"].iloc[0] == "real"


def test_common_industry_headers_reach_a_canonical_column():
    df = pd.DataFrame(
        {
            "Equipment Type": ["PUMP"],
            "Item Description": ["Feed pump"],
            "Plant Area": ["A-10"],
            "Criticality": ["A"],
            "Make": ["ACME"],
            "Model Number": ["X-1"],
            "Equipment Status": ["IN SERVICE"],
            "Process Fluid": ["CRUDE"],
            "Line Number": ["L-101"],
            "From Equipment": ["P-1"],
            "To Equipment": ["V-1"],
            "Relation Type": ["FLOW"],
        }
    )
    out = apply_column_rename(df, _cfg(), "pid", verbose=False)
    for col in (
        "equipment_type",
        "description",
        "area",
        "criticality",
        "manufacturer",
        "model",
        "status",
        "medium",
        "line",
        "from_equipment_tag",
        "to_equipment_tag",
        "connection_type",
    ):
        assert col in out.columns, col


def test_connectivity_line_number_falls_back_to_the_equipment_line_column():
    entities = yaml.safe_load(open(ENTITIES))
    ents = entities.get("entities") or entities
    refs = ents["equipment_connection"]["attributes"]["line_number"]["from"]
    assert "pid.line" in refs
