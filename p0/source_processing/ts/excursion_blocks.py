"""Read the summary rows out of a block-structured excursion export."""

from __future__ import annotations

import pandas as pd

from p0.utils import fs

EQUIPMENT_HEADER = "Equipment"


class InconsistentBlockEquipmentError(ValueError):
    """One block's event rows disagree about which equipment they belong to."""


def _blank(value: object) -> bool:
    """Whether a cell carries nothing worth keeping."""
    if value is None:
        return True
    if isinstance(value, float) and pd.isna(value):
        return True
    return str(value).strip() in ("", "nan", "NaT", "None")


def _row_values(frame: pd.DataFrame, index: int) -> list:
    """One sheet row as a plain list."""
    return frame.iloc[index].tolist()


def _header_width(values: list) -> int:
    """How many leading cells of a header row actually name a column."""
    width = 0
    for cell in values:
        if _blank(cell):
            break
        width += 1
    return width


def _equipment_position(values: list) -> int | None:
    """Where the per-event equipment column sits in a nested sub-header."""
    positions = [i for i, cell in enumerate(values) if str(cell).strip() == EQUIPMENT_HEADER]
    return positions[-1] if positions else None


def _block_equipment(frame: pd.DataFrame, start: int, stop: int) -> object:
    """The single equipment every event row in one block names."""
    position = None
    for index in range(start, stop):
        values = _row_values(frame, index)
        if not _blank(values[0]):
            continue
        found = _equipment_position(values)
        if found is not None:
            position = found
            start = index + 1
            break
    if position is None:
        return None

    seen = []
    for index in range(start, stop):
        values = _row_values(frame, index)
        if position >= len(values):
            continue
        value = values[position]
        if _blank(value):
            continue
        text = str(value).strip()
        if text not in seen:
            seen.append(text)
    if not seen:
        return None
    if len(seen) > 1:
        raise InconsistentBlockEquipmentError(
            f"rows {start}-{stop} name {len(seen)} different equipment values "
            f"({seen[:3]}...); the export is expected to carry exactly one per block"
        )
    return seen[0]


def read_excursion_blocks(
    path: str, sheet_name: str | None = None, equipment_column: str = "alerts_prime_equipment"
) -> pd.DataFrame:
    """The summary rows of an excursion export, each carrying its block's equipment."""
    frame = fs.read_excel(path, sheet_name=sheet_name or 0, header=None, dtype=object)
    if frame.empty:
        return pd.DataFrame()

    marker_rows = [i for i in range(len(frame)) if not _blank(frame.iloc[i, 0])]
    if not marker_rows:
        return pd.DataFrame()

    header_index = marker_rows[0]
    header_values = _row_values(frame, header_index)
    width = _header_width(header_values)
    columns = [str(c).strip() for c in header_values[:width]]

    records = []
    summary_rows = marker_rows[1:]
    for position, row_index in enumerate(summary_rows):
        stop = summary_rows[position + 1] if position + 1 < len(summary_rows) else len(frame)
        values = _row_values(frame, row_index)[:width]
        record = dict(zip(columns, values))
        record[equipment_column] = _block_equipment(frame, row_index + 1, stop)
        records.append(record)

    return pd.DataFrame(records, columns=columns + [equipment_column])
