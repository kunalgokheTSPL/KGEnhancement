"""Resolve an equipment identity for findings sources that name assets inconsistently."""

from __future__ import annotations

import re
from typing import Any

from .asset_identity import normalize_asset_id

SAP_EQUIPMENT = "sap_equipment"
EQUIPMENT_TAG = "equipment_tag"
FLOC_TAIL = "floc_tail"
FLOC_SEGMENT = "floc_segment"
TEXT_EXTRACT = "text_extract"
UNMATCHED = "unmatched"

CONFIDENCE = {
    SAP_EQUIPMENT: 1.0,
    EQUIPMENT_TAG: 0.95,
    FLOC_TAIL: 0.85,
    FLOC_SEGMENT: 0.80,
    TEXT_EXTRACT: 0.60,
    UNMATCHED: 0.0,
}

_FLOAT_ID = re.compile(r"^\d+\.0+$")
_NUMERIC = re.compile(r"^\d+$")
_SYSTEM_CODE = re.compile(r"^[A-Z]{2,4}$")

_TAG_PATTERNS = [
    re.compile(r"\b\d{2,4}-[A-Z]{1,4}-\d{3,5}-\d{3,5}(?:-[A-Z0-9]+)?\b"),
    re.compile(r"\b[A-Z]{1,4}-\d{1,5}[A-Z]?\b"),
    re.compile(r"\b[A-Z]{1,4}\d{2,5}[A-Z]?\b"),
    re.compile(r"\b[A-Z]{2,4}\s\d{1,4}[A-Z]?\b"),
]

_TEXT_STOPWORDS = {
    "PSC", "HC", "CUI", "NDT", "SKA", "EOR", "MALT", "LOPC", "AIF", "PPE",
    "AT", "ON", "IN", "IT", "IS", "AS", "TO", "BY", "OF", "NO", "OR", "AN", "A",
    "AND", "THE", "WAS", "FOR", "ALL", "HRS", "HOUR", "HOURS", "AM", "PM",
    "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
    "TIER", "CAT", "REV", "NO", "REF",
}


def _clean(value: Any) -> str:
    """Trim, drop nulls, and undo Excel's float rendering of numeric ids."""
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in ("nan", "none", "nat", "null", ""):
        return ""
    if _FLOAT_ID.match(text):
        text = text.split(".")[0]
    return normalize_asset_id(text)


def _usable_fragment(value: str) -> bool:
    """A hyphen tail is only an asset if it is not a bare number or a stub."""
    return len(value) >= 2 and not _NUMERIC.match(value)


def from_floc(value: Any) -> tuple[str, str]:
    """Take the equipment out of a functional location, tail first."""
    floc = _clean(value)
    if not floc:
        return "", UNMATCHED
    tail = floc.rsplit(".", 1)[-1].strip()
    if not tail:
        return "", UNMATCHED
    if "-" in tail:
        fragment = tail.rsplit("-", 1)[-1].strip()
        if _usable_fragment(fragment) and not _SYSTEM_CODE.match(fragment):
            return fragment, FLOC_TAIL
    if _SYSTEM_CODE.match(tail) and "." in floc:
        return floc, FLOC_SEGMENT
    return tail, FLOC_SEGMENT


def from_text(*values: Any) -> tuple[str, str]:
    """Pull the first tag-shaped token out of free text, in the order given."""
    for value in values:
        text = _clean(value)
        if not text:
            continue
        body = text.split(":", 1)[-1] if ":" in text[:12] else text
        for pattern in _TAG_PATTERNS:
            for candidate in pattern.findall(body):
                token = candidate.strip(" .,()").replace(" ", "-")
                if token and token.split("-")[0] not in _TEXT_STOPWORDS:
                    return token, TEXT_EXTRACT
    return "", UNMATCHED


def resolve(
    row: dict,
    *,
    sap_equipment_columns: tuple[str, ...] = (),
    equipment_columns: tuple[str, ...] = (),
    floc_columns: tuple[str, ...] = (),
    text_columns: tuple[str, ...] = (),
) -> dict:
    """Walk the ladder and report what matched, never guessing silently."""
    for column in sap_equipment_columns:
        value = _clean(row.get(column))
        if value:
            return _result(value, SAP_EQUIPMENT)
    for column in equipment_columns:
        value = _clean(row.get(column))
        if value:
            return _result(value, EQUIPMENT_TAG)
    for column in floc_columns:
        value, method = from_floc(row.get(column))
        if value:
            return _result(value, method)
    if text_columns:
        value, method = from_text(*(row.get(column) for column in text_columns))
        if value:
            return _result(value, method)
    return _result("", UNMATCHED)


def _result(asset: str, method: str) -> dict:
    return {
        "normalized_asset": asset,
        "asset_match_method": method,
        "asset_match_confidence": CONFIDENCE.get(method, 0.0),
    }


def resolve_frame(df, spec: dict):
    """Apply :func:`resolve` across a DataFrame using one connector's column spec."""
    if df is None or getattr(df, "empty", True):
        return df
    out = df.copy()
    resolved = [
        resolve(
            {column: row.get(column) for column in out.columns},
            sap_equipment_columns=tuple(spec.get("sap_equipment_columns", ())),
            equipment_columns=tuple(spec.get("equipment_columns", ())),
            floc_columns=tuple(spec.get("floc_columns", ())),
            text_columns=tuple(spec.get("text_columns", ())),
        )
        for row in out.to_dict("records")
    ]
    out["normalized_asset"] = [entry["normalized_asset"] for entry in resolved]
    out["asset_match_method"] = [entry["asset_match_method"] for entry in resolved]
    out["asset_match_confidence"] = [entry["asset_match_confidence"] for entry in resolved]
    return out


COLUMN_SPEC = {
    "aif": {
        "floc_columns": ("functional_location",),
        "text_columns": ("description",),
    },
    "gloc": {
        "sap_equipment_columns": ("sap_equipment_id",),
        "equipment_columns": ("equipment_ref",),
        "text_columns": ("findings",),
    },
    "lopc": {
        "text_columns": ("incident_title", "description"),
    },
    "upd_event": {
        "equipment_columns": ("eqp_tag_no",),
        "text_columns": ("problem_statement", "way_forward_description"),
    },
    "trip_event": {
        "equipment_columns": ("eqp_tag_no",),
        "text_columns": ("problem_statement", "way_forward_description"),
    },
}


def spec_for(connector: str) -> dict:
    """The column ladder for one findings connector."""
    return dict(COLUMN_SPEC.get(str(connector or "").lower(), {}))
