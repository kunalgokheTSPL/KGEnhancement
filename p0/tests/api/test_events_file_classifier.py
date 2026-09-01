"""Filename → connector routing for the shared UPD/PD + Trip Event upload."""

from __future__ import annotations

import pytest

from p0.api.routers.connectors import _classify_events_file


@pytest.mark.parametrize(
    "filename, expected",
    [
        ("UPD_PD.xlsx", "upd_event"),
        ("3. UPD PD (2022-2026).xlsx", "upd_event"),
        ("Stripper_UPD_Aug2026.xlsx", "upd_event"),
        ("Trip_Events.xlsx", "trip_event"),
        ("4. Trip Event (2022-2026).xlsx", "trip_event"),
        ("Unknown_file.xlsx", None),
    ],
)
def test_classify_events_file(filename, expected):
    assert _classify_events_file(filename) == expected
