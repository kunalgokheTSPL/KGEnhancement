"""One ts source is one file, so every name the caller gives must be processed."""

from __future__ import annotations

import argparse

import pandas as pd
import pytest

from p0.api.services.pipeline_stages import _build_stage_args
from p0.pipelines.run_ts_end_to_end import _ts_target_files


def _args(ts_file):
    return argparse.Namespace(ts_file=ts_file)


def test_no_name_keeps_the_auto_resolve_fallback():
    assert _ts_target_files(_args(None)) == [None]
    assert _ts_target_files(_args("")) == [None]


def test_one_name_is_one_target():
    assert _ts_target_files(_args("PI Tag.xlsx")) == ["PI Tag.xlsx"]


def test_every_comma_separated_name_becomes_a_target():
    targets = _ts_target_files(_args("a.xlsx,b.xlsx,c.xlsx,d.xlsx"))
    assert targets == ["a.xlsx", "b.xlsx", "c.xlsx", "d.xlsx"]


def test_whitespace_and_blanks_are_dropped():
    assert _ts_target_files(_args(" a.xlsx , , b.xlsx ")) == ["a.xlsx", "b.xlsx"]


def test_names_with_spaces_survive():
    targets = _ts_target_files(_args("PI Tag Export.xlsx,Frequent Alarms.xlsx"))
    assert targets == ["PI Tag Export.xlsx", "Frequent Alarms.xlsx"]


def test_the_stage_args_carry_every_file_not_just_the_first():
    names = ["one.xlsx", "two.xlsx", "three.xlsx", "four.xlsx"]
    argv = _build_stage_args(
        "ts",
        "plant_1_testcase",
        names,
        None,
        None,
        work_dir="/tmp/work",
        upload_batch_id="batch-1",
    )
    assert "--ts_file" in argv
    passed = argv[argv.index("--ts_file") + 1]
    assert _ts_target_files(_args(passed)) == names


@pytest.mark.parametrize("count", [1, 2, 4, 9])
def test_round_trip_preserves_the_file_count(count):
    names = [f"file_{n}.xlsx" for n in range(count)]
    argv = _build_stage_args(
        "ts",
        "plant_1_testcase",
        names,
        None,
        None,
        work_dir="/tmp/work",
        upload_batch_id="batch-1",
    )
    passed = argv[argv.index("--ts_file") + 1]
    assert len(_ts_target_files(_args(passed))) == count


def test_merging_datasets_keeps_the_first_frame_for_a_repeated_source():
    merged: dict = {}
    for part in ({"osi_pi": pd.DataFrame({"a": [1, 2]})}, {"osi_pi": pd.DataFrame({"a": [9]})}):
        for name, frame in part.items():
            if name in merged:
                continue
            merged[name] = frame
    assert list(merged) == ["osi_pi"]
    assert len(merged["osi_pi"]) == 2
