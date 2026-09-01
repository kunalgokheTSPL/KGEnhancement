"""Incremental drawing uploads must not destroy each other."""

import re

import pandas as pd

from p0.pipelines.run_pnid_from_pdfs import _write_merged

SOURCE = "p0/pipelines/run_pnid_from_pdfs.py"

CANONICAL_OUTPUTS = [
    '"equipment.parquet"',
    '"equipment_connection.parquet"',
    '"equipment_pid.parquet"',
    'f"{entity_name}_uid.parquet"',
    '"asset_relationship.parquet"',
]


def _merged_call_bodies():
    src = open(SOURCE).read()
    return re.findall(r"_write_merged\((.*?)\n    \)", src, re.S)


def test_every_canonical_output_is_written_through_the_merge():
    bodies = " ".join(_merged_call_bodies())
    for out in CANONICAL_OUTPUTS:
        assert out in bodies, out


def test_no_canonical_output_is_written_with_a_bare_parquet_write():
    src = open(SOURCE).read()
    bare = re.findall(r"fs\.write_parquet\((.*?)\n    \)", src, re.S)
    for body in bare:
        for out in CANONICAL_OUTPUTS:
            assert out not in body, f"{out} bypasses the merge"


def test_a_second_drawing_does_not_delete_the_first(tmp_path):
    path = str(tmp_path / "equipment.parquet")
    first = pd.DataFrame(
        {"equipment_uid": ["eq:a", "eq:b"], "normalized_asset": ["P-1001", "V-2001"]}
    )
    _write_merged(first, path, "equipment_uid")
    second = pd.DataFrame(
        {"equipment_uid": ["eq:c"], "normalized_asset": ["E-3001"]}
    )
    _write_merged(second, path, "equipment_uid")
    on_disk = pd.read_parquet(path)
    assert sorted(on_disk["normalized_asset"]) == ["E-3001", "P-1001", "V-2001"]


def test_re_uploading_a_drawing_replaces_its_own_rows(tmp_path):
    path = str(tmp_path / "equipment.parquet")
    first = pd.DataFrame(
        {"equipment_uid": ["eq:a"], "normalized_asset": ["P-1001"], "kind": ["PUMP"]}
    )
    _write_merged(first, path, "equipment_uid")
    revised = pd.DataFrame(
        {"equipment_uid": ["eq:a"], "normalized_asset": ["P-1001"], "kind": ["VESSEL"]}
    )
    _write_merged(revised, path, "equipment_uid")
    on_disk = pd.read_parquet(path)
    assert len(on_disk) == 1
    assert on_disk.iloc[0]["kind"] == "VESSEL"
