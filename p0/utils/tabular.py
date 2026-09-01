"""Pick the sheet and header row that actually hold a table's data."""

from __future__ import annotations

import os
from typing import Any, Iterable

import pandas as pd

HEADER_SCAN_ROWS = 15

SNIFF_ROWS = 25


def _norm(value: Any) -> str:
    return str(value).strip().lower().replace("\n", " ").replace("_", " ")


def _score_header(cells: Iterable[Any], wanted: set[str]) -> int:
    if not wanted:
        return 0
    present = {_norm(c) for c in cells if str(c).strip()}
    return len(present & wanted)


def _filled(cells: Iterable[Any]) -> int:
    return sum(1 for c in cells if str(c).strip() and str(c).strip().lower() != "nan")


def _best_header_row(probe: pd.DataFrame, wanted: set[str]) -> tuple[int, int, int]:
    """Return (header_row, matched_columns, filled_cells) for the best row in a probe."""
    best = (0, -1, 0)
    limit = min(HEADER_SCAN_ROWS, len(probe))
    for i in range(limit):
        cells = list(probe.iloc[i])
        score = _score_header(cells, wanted)
        filled = _filled(cells)
        if (score, filled) > (best[1], best[2]):
            best = (i, score, filled)
    return best


def _excel_file(path: Any) -> pd.ExcelFile:
    """Open a workbook, naming the engine for formats pandas will not guess."""
    ext = os.path.splitext(str(path))[1].lower()
    if ext == ".xlsb":
        return pd.ExcelFile(path, engine="pyxlsb")
    return pd.ExcelFile(path)


def _clean(df: pd.DataFrame) -> pd.DataFrame:
    """Drop placeholder columns and rows a sheet only padded out with blanks."""
    df = df.loc[:, [c for c in df.columns if not str(c).startswith("Unnamed:")]]
    if df.empty:
        return df
    keep = df.apply(lambda r: any(str(v).strip() for v in r), axis=1)
    return df.loc[keep]


def read_tables(
    path: Any,
    wanted_columns: Iterable[str] = (),
    dtype: Any = str,
    keep_default_na: bool = True,
    delimiter: str = ",",
) -> list[tuple[pd.DataFrame, str, int]]:
    """Every sheet in a workbook that carries data, each with its own header row."""
    ext = os.path.splitext(str(path))[1].lower()
    if ext == ".csv":
        df = pd.read_csv(
            path, dtype=dtype, keep_default_na=keep_default_na, delimiter=delimiter
        )
        return [(_clean(df.fillna("")), "", 0)]

    wanted = {_norm(c) for c in wanted_columns if str(c).strip()}
    out: list[tuple[pd.DataFrame, str, int]] = []
    xls = _excel_file(path)
    try:
        for sheet in xls.sheet_names:
            try:
                probe = pd.read_excel(
                    xls, sheet_name=sheet, header=None, nrows=SNIFF_ROWS, dtype=str
                )
            except Exception:
                continue
            if probe.empty:
                continue
            header_row, _, _ = _best_header_row(probe, wanted)
            try:
                df = pd.read_excel(
                    xls,
                    sheet_name=sheet,
                    header=header_row,
                    dtype=dtype,
                    keep_default_na=keep_default_na,
                ).fillna("")
            except Exception:
                continue
            df = _clean(df)
            if not df.empty and len(df.columns):
                out.append((df, sheet, header_row))
    finally:
        xls.close()
    return out


def read_table(
    path: Any,
    wanted_columns: Iterable[str] = (),
    dtype: Any = str,
    keep_default_na: bool = True,
    delimiter: str = ",",
) -> tuple[pd.DataFrame, str, int]:
    """Read a csv/excel file from whichever sheet and header row carry the data."""
    ext = os.path.splitext(str(path))[1].lower()
    if ext == ".csv":
        df = pd.read_csv(
            path, dtype=dtype, keep_default_na=keep_default_na, delimiter=delimiter
        )
        return df.fillna(""), "", 0

    wanted = {_norm(c) for c in wanted_columns if str(c).strip()}
    xls = _excel_file(path)
    try:
        best_sheet = xls.sheet_names[0]
        best_header = 0
        best_key = (-1, -1, -1)
        for sheet in xls.sheet_names:
            try:
                probe = pd.read_excel(
                    xls, sheet_name=sheet, header=None, nrows=SNIFF_ROWS, dtype=str
                )
            except Exception:
                continue
            if probe.empty:
                continue
            header_row, matched, filled = _best_header_row(probe, wanted)
            body = max(len(probe) - header_row - 1, 0)
            key = (matched, body, filled)
            if key > best_key:
                best_key = key
                best_sheet = sheet
                best_header = header_row
        df = pd.read_excel(
            xls,
            sheet_name=best_sheet,
            header=best_header,
            dtype=dtype,
            keep_default_na=keep_default_na,
        ).fillna("")
    finally:
        xls.close()

    df = df.loc[:, [c for c in df.columns if not str(c).startswith("Unnamed:")]]
    return df, best_sheet, best_header
