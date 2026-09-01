"""Transposition folds finer-than-tag rows without losing or inventing values."""

from __future__ import annotations

import os

import pandas as pd
import pytest
import yaml

os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

from p0.api.config import COMMON_DIR, SCHEMA_FROZEN_FILE
from p0.utils.renamer import apply_column_rename, apply_copies
from p0.utils.transposer import (
    DuplicateTransposeKeyError,
    UnknownTransposeKeyError,
    apply_transpose,
    transpose_columns,
)

RENAME_FILE = COMMON_DIR / "column_rename" / "timeseries_column_rename.yaml"


def _cfg():
    """The timeseries rename/transpose template."""
    with open(RENAME_FILE, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def _schema_columns():
    """Declared columns of timeseries_metadata."""
    with open(SCHEMA_FROZEN_FILE, encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)
    return set((cfg["schema"]["timeseries_metadata"].get("columns") or {}))


def _alarm_frame():
    """Two tags, one with three alarm rows and one with a single row."""
    return pd.DataFrame(
        [
            {"Tag": "T1", "Alarm": "HI", "Count": 10, "Percent": 1.5, "Accum %": 1.5,
             "Limit": 100.0, "Unit": "U1", "Point Description": "d1"},
            {"Tag": "T1", "Alarm": "LO", "Count": 4, "Percent": 0.5, "Accum %": 2.0,
             "Limit": -5.0, "Unit": "U1", "Point Description": "d1"},
            {"Tag": "T1", "Alarm": "ALM", "Count": 7, "Percent": 0.9, "Accum %": 2.9,
             "Limit": None, "Unit": "U1", "Point Description": "d1"},
            {"Tag": "T2", "Alarm": "TRIP", "Count": 2, "Percent": 0.1, "Accum %": 3.0,
             "Limit": None, "Unit": "U2", "Point Description": "d2"},
        ]
    )


def test_three_alarm_rows_become_one_tag_row():
    spec = _cfg()["sources"]["alerts_prime_alarms"]["transpose"]
    out = apply_transpose(_alarm_frame(), spec)
    assert len(out) == 2


def test_each_alarm_lands_in_its_own_column():
    spec = _cfg()["sources"]["alerts_prime_alarms"]["transpose"]
    out = apply_transpose(_alarm_frame(), spec).set_index("Tag")
    assert out.loc["T1", "alerts_prime_alarm_hi_count"] == 10
    assert out.loc["T1", "alerts_prime_alarm_lo_count"] == 4
    assert out.loc["T1", "alerts_prime_alarm_alm_count"] == 7
    assert out.loc["T1", "alerts_prime_alarm_hi_limit"] == 100.0


def test_a_tag_without_an_alarm_type_leaves_that_column_empty():
    spec = _cfg()["sources"]["alerts_prime_alarms"]["transpose"]
    out = apply_transpose(_alarm_frame(), spec).set_index("Tag")
    assert pd.isna(out.loc["T2", "alerts_prime_alarm_hi_count"])
    assert pd.isna(out.loc["T1", "alerts_prime_alarm_trip_count"])


def test_an_alarm_type_that_carries_no_limit_stays_null():
    spec = _cfg()["sources"]["alerts_prime_alarms"]["transpose"]
    out = apply_transpose(_alarm_frame(), spec).set_index("Tag")
    assert pd.isna(out.loc["T1", "alerts_prime_alarm_alm_limit"])


def test_tag_level_columns_are_carried_through():
    spec = _cfg()["sources"]["alerts_prime_alarms"]["transpose"]
    out = apply_transpose(_alarm_frame(), spec).set_index("Tag")
    assert out.loc["T1", "Unit"] == "U1"
    assert out.loc["T2", "Point Description"] == "d2"


def test_an_undeclared_alarm_code_raises_instead_of_vanishing():
    spec = _cfg()["sources"]["alerts_prime_alarms"]["transpose"]
    df = _alarm_frame()
    df.loc[0, "Alarm"] = "NOT_A_REAL_ALARM"
    with pytest.raises(UnknownTransposeKeyError):
        apply_transpose(df, spec)


def test_the_same_code_twice_in_one_group_raises_rather_than_overwriting():
    spec = _cfg()["sources"]["alerts_prime_alarms"]["transpose"]
    df = _alarm_frame()
    df.loc[1, "Alarm"] = "HI"
    with pytest.raises(DuplicateTransposeKeyError):
        apply_transpose(df, spec)


def _excursion_frame():
    return pd.DataFrame(
        [
            {"Tag": "X1", "Boundary": "X1.SOL-L", "Boundary Value": 3.0, "Count": 5,
             "Percent": 1.0, "Accum %": 1.0, "Group": "G"},
            {"Tag": "X1", "Boundary": "X1.IPF-HH", "Boundary Value": 90.0, "Count": 2,
             "Percent": 0.4, "Accum %": 1.4, "Group": "G"},
            {"Tag": "X2", "Boundary": "X2.MDL-HHH DC(out)", "Boundary Value": 265.0,
             "Count": 1, "Percent": 0.1, "Accum %": 1.5, "Group": "G"},
        ]
    )


def test_the_boundary_code_is_read_from_after_the_tag_name():
    spec = _cfg()["sources"]["alerts_prime_excursions"]["transpose"]
    out = apply_transpose(_excursion_frame(), spec).set_index("Tag")
    assert out.loc["X1", "alerts_prime_bnd_sol_l_value"] == 3.0
    assert out.loc["X1", "alerts_prime_bnd_ipf_hh_value"] == 90.0


def test_the_annotated_boundary_variant_folds_onto_its_plain_code():
    spec = _cfg()["sources"]["alerts_prime_excursions"]["transpose"]
    out = apply_transpose(_excursion_frame(), spec).set_index("Tag")
    assert out.loc["X2", "alerts_prime_bnd_mdl_hhh_value"] == 265.0


def test_the_annotated_variant_colliding_with_its_plain_code_raises():
    spec = _cfg()["sources"]["alerts_prime_excursions"]["transpose"]
    df = pd.DataFrame(
        [
            {"Tag": "X3", "Boundary": "X3.MDL-HHH", "Boundary Value": 1.0,
             "Count": 1, "Percent": 0.1, "Accum %": 0.1},
            {"Tag": "X3", "Boundary": "X3.MDL-HHH DC(out)", "Boundary Value": 2.0,
             "Count": 1, "Percent": 0.1, "Accum %": 0.2},
        ]
    )
    with pytest.raises(DuplicateTransposeKeyError):
        apply_transpose(df, spec)


def test_every_column_a_transpose_can_produce_exists_in_the_schema():
    cfg = _cfg()
    declared = _schema_columns()
    for name, src in cfg["sources"].items():
        spec = (src or {}).get("transpose")
        if not spec:
            continue
        for col in transpose_columns(spec):
            assert col in declared, f"{name} produces {col}, which no table column holds"


def test_every_rename_target_of_a_new_source_exists_in_the_schema():
    cfg = _cfg()
    declared = _schema_columns()
    for name in ("osi_pi", "alerts_prime_alarms", "alerts_prime_excursions", "alerts_protean"):
        for target in (cfg["sources"][name].get("mappings") or {}).values():
            assert target in declared, f"{name} renames to {target}, which does not exist"
        for target in (cfg["sources"][name].get("copies") or {}).values():
            assert target in declared, f"{name} copies to {target}, which does not exist"


EXISTING_TIMESERIES_RESOLUTIONS = {
    "Tag Name": "tag_name",
    "PI Tag": "tag_name",
    "Unit": "unit",
    "Data Type": "data_type",
    "Description": "description",
    "Safety LL": "safety_limit_ll",
    "Trip HH": "trip_limit_hh",
}


def test_adding_sources_does_not_change_how_the_existing_source_resolves():
    cfg = _cfg()
    for header, expected in EXISTING_TIMESERIES_RESOLUTIONS.items():
        df = pd.DataFrame([{header: "v"}])
        out = apply_column_rename(df, cfg, source_name="timeseries", verbose=False)
        assert list(out.columns) == [expected], f"{header} now resolves to {list(out.columns)}"


def test_the_new_sources_resolve_their_own_headers():
    cfg = _cfg()
    df = pd.DataFrame([{"Plant_Hierarchy": "h", "Upper_Equipment": "e"}])
    out = apply_column_rename(df, cfg, source_name="alerts_prime_alarms", verbose=False)
    assert set(out.columns) == {"alerts_prime_plant_hierarchy", "alerts_prime_upper_equipment_id"}


def test_the_protean_pipe_prefixed_header_is_matched():
    cfg = _cfg()
    df = pd.DataFrame([{"|OperatingValue": "v"}])
    out = apply_column_rename(df, cfg, source_name="alerts_protean", verbose=False)
    assert list(out.columns) == ["protean_operating_value"]


def test_a_copy_duplicates_the_canonical_column_without_replacing_it():
    df = pd.DataFrame([{"tag_name": "T1"}])
    out = apply_copies(df, {"tag_name": "alerts_prime_tag"})
    assert out["tag_name"].tolist() == ["T1"]
    assert out["alerts_prime_tag"].tolist() == ["T1"]


def test_a_named_source_wins_over_another_sources_differently_cased_key():
    cfg = _cfg()
    out = apply_column_rename(
        pd.DataFrame([{"Description": "x"}]), cfg, source_name="alerts_protean", verbose=False
    )
    assert list(out.columns) == ["protean_description"]


def test_the_excursion_description_also_reaches_its_own_column():
    cfg = _cfg()
    out = apply_column_rename(
        pd.DataFrame([{"Description": "x"}]), cfg,
        source_name="alerts_prime_excursions", verbose=False,
    )
    assert list(out.columns) == ["alerts_prime_description"]


def test_the_generic_source_still_gets_the_plain_description():
    cfg = _cfg()
    out = apply_column_rename(
        pd.DataFrame([{"Description": "x"}]), cfg, source_name="timeseries", verbose=False
    )
    assert list(out.columns) == ["description"]
