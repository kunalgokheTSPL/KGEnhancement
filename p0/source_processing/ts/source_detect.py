"""Decide which timeseries metadata source an uploaded workbook is."""

from __future__ import annotations

import re

import pandas as pd
import yaml

from p0.api.config import COMMON_DIR
from p0.utils import fs

TS_SOURCE_SIGNATURES_FILE = (
    COMMON_DIR / "source_detect" / "timeseries_source_signatures.yaml"
)

HEADER_SCAN_MAX = 15

GENERIC_SOURCE = "timeseries"


class AmbiguousSourceError(ValueError):
    """More than one declared source claims the same workbook."""


def _norm(value: object) -> str:
    """Normalize one header cell so spacing and punctuation stop mattering."""
    return re.sub(r"[^a-z0-9]", "", str(value).lower())


def load_signatures(path: object = None) -> dict:
    """The declared source signatures, straight from the template."""
    with open(str(path or TS_SOURCE_SIGNATURES_FILE), encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh) or {}
    return cfg.get("sources") or {}


def _required_tokens(signature: dict) -> set[str]:
    """The normalized headers a signature demands."""
    return {_norm(h) for h in (signature.get("required_headers") or []) if str(h).strip()}


def matches_in_frame(frame: pd.DataFrame, signatures: dict) -> list[tuple[str, int]]:
    """Every (source, header row) a scanned block of rows satisfies."""
    hits: list[tuple[str, int]] = []
    for row_idx in range(min(len(frame), HEADER_SCAN_MAX)):
        tokens = {
            _norm(v) for v in frame.iloc[row_idx].tolist() if str(v).strip() not in ("", "nan")
        }
        for key, signature in signatures.items():
            required = _required_tokens(signature or {})
            if required and required <= tokens:
                hits.append((key, row_idx))
    return hits


def detect_ts_source(path: str, signatures: dict | None = None) -> dict | None:
    """Which declared source a workbook is, with the sheets that carry it."""
    sigs = load_signatures() if signatures is None else signatures
    if not str(path).lower().endswith((".xlsx", ".xls")):
        return None

    found: dict[str, list[dict]] = {}
    with fs.excel_book(path) as book:
        for sheet in book.sheet_names:
            frame = book.parse(sheet, header=None, nrows=HEADER_SCAN_MAX, dtype=str)
            if frame.empty:
                continue
            seen: set[str] = set()
            for key, row_idx in matches_in_frame(frame, sigs):
                if key in seen:
                    continue
                seen.add(key)
                found.setdefault(key, []).append({"sheet": sheet, "header_row": row_idx})

    if not found:
        return None
    if len(found) > 1:
        raise AmbiguousSourceError(
            "timeseries source detection is ambiguous: "
            f"{sorted(found)} all match this workbook. Every signature must name "
            "at least one header unique to its export; fix "
            "config/templates/_common/source_detect/timeseries_source_signatures.yaml"
        )

    source = next(iter(found))
    return {
        "source": source,
        "label": (sigs.get(source) or {}).get("label") or source,
        "sheets": found[source],
    }
