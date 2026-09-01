"""Unit test for the EVENTS_STAGES_REQUIRE_FILE_NAMES guard in pipeline_stages."""

from __future__ import annotations

import pytest

from p0.api.services.pipeline_stages import _build_stage_args


@pytest.mark.parametrize("stage", ["upd_event", "trip_event"])
@pytest.mark.parametrize("file_names", [None, []])
def test_missing_file_names_raises_value_error(stage, file_names):
    with pytest.raises(ValueError):
        _build_stage_args(stage, "plant1", file_names=file_names)


@pytest.mark.parametrize("stage", ["upd_event", "trip_event"])
def test_valid_file_names_does_not_raise(stage):
    _build_stage_args(stage, "plant1", file_names=["some_file.xlsx"])
