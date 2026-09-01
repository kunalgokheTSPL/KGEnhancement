"""Golden tests for the deterministic P&ID response parsers (deferred half of #2).

The pnid extractor calls Gemini Vision; these parsers turn its text/JSON response into
P&ID components and connections. They are LLM-free — feed a canned response, assert the
parse. No model, no network — so the extraction logic is pinned without the vision call.
"""

from __future__ import annotations

from p0.source_processing.pnid.complete_pnid_extraction_without_ui import (
    _parse_components,
    _parse_components_from_json_fallback,
    _parse_connections,
    _parse_drawing_number,
)


def test_parse_drawing_number_from_the_section():
    assert _parse_drawing_number("--- DRAWING NUMBER ---\nABC-1234\n") == "ABC-1234"


def test_parse_drawing_number_falls_back_to_a_pattern():
    assert _parse_drawing_number("random text PID-4567 more") == "PID-4567"


def test_parse_drawing_number_unknown_when_absent():
    assert _parse_drawing_number("no number here, lowercase only") == "Unknown"


def test_json_fallback_classifies_components_by_tag_prefix():
    text = '[{"label": "MH-101", "box_2d": [1,2,3,4]}, {"label": "FV-202", "box_2d": [5,6,7,8]}]'
    by_tag = {c["tag"]: c for c in _parse_components_from_json_fallback(text)}
    assert by_tag["MH-101"]["type"] == "Vessel/Tank"
    assert by_tag["FV-202"]["type"] == "Valve/Instrument"


def test_parse_components_uses_json_fallback_when_no_table():
    comps = _parse_components('[{"label": "BE-1", "box_2d": [0,0,1,1]}]')
    assert len(comps) == 1
    assert comps[0]["tag"] == "BE-1"
    assert comps[0]["type"] == "Equipment"


def test_parse_connections_from_arrow_notation():
    conns = _parse_connections("Header line\nP-101 → V-202\nT-1 -> P-101")
    assert ["P-101", "V-202"] in conns
    assert ["T-1", "P-101"] in conns
