"""Read a detected timeseries source into one frame, however its sheets are shaped."""

from __future__ import annotations

import pandas as pd

from p0.source_processing.ts.excursion_blocks import read_excursion_blocks
from p0.source_processing.ts.source_parsers import parse_pi_tags, parse_protean
from p0.utils import fs

EXCURSIONS = "alerts_prime_excursions"
PROTEAN = "alerts_protean"
PI = "osi_pi"


def _read_sheet(book, sheet: str, header_row: int) -> pd.DataFrame:
    """One sheet of an open workbook, read from the header row detection found."""
    frame = book.parse(sheet, header=header_row)
    frame.columns = [str(c).strip() for c in frame.columns]
    return frame


def read_detected_source(path: str, detected: dict, spec: dict | None = None):
    """The rows of a detected source, already carrying whatever identity it needs."""
    source = detected["source"]
    sheets = detected.get("sheets") or []
    parse_spec = spec or {}

    if source == EXCURSIONS:
        target = sheets[0]
        frame = read_excursion_blocks(path, sheet_name=target["sheet"])
        return frame, {"rows": int(len(frame))}

    with fs.excel_book(path) as book:
        if source == PROTEAN:
            frames = [_read_sheet(book, s["sheet"], s["header_row"]) for s in sheets]
            return parse_protean(frames, parse_spec)
        frame = _read_sheet(book, sheets[0]["sheet"], sheets[0]["header_row"])

    if source == PI:
        return parse_pi_tags(frame, parse_spec)

    return frame, {"rows": int(len(frame))}
