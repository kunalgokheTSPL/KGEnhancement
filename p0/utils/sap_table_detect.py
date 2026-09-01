"""SAP table detection — identify which SAP table a file belongs to.

Shared by the upload endpoint (api/routers/connectors.py, which reports
matched_by to the frontend) and the SAP pipeline (pipelines/run_sap_end_to_end.py,
which gates processing on it). Kept here, rather than in either caller, so both
stay in sync with a single detection implementation.
"""

from __future__ import annotations

import csv
import functools
import io
import re
from pathlib import Path

import yaml

from p0.api.config import COMMON_DIR

# Minimum fraction of a table's expected columns that must be present in the
# uploaded file for a content-based match to be accepted (0.0-1.0).
#
# sap_table_columns.yaml lists only join-key columns (3-6 per table), not all
# columns that table contains. A real SAP export will often only have 1 of those
# 3 join keys (e.g. EQUI exports rarely include ILOAN or TPLNR). 0.25 accepts
# any file where the best-matching table scores at least 1/4 of its join keys —
# in practice this means at least one recognised key column is present and that
# table scores higher than every other table.
SAP_CONTENT_MATCH_THRESHOLD = 0.25
# Tables with fewer expected columns than this are too ambiguous for content matching.
# IFLO has only TPLNR (1 column) and would score 1.0 against any SAP file that
# happens to contain TPLNR — a guaranteed false positive.
CONTENT_MATCH_MIN_EXPECTED = 2

# Row-oriented text formats only need their header line, so cap how much of
# the file detect_sap_table_at_path() pulls into memory for them. Binary
# formats (.xlsx, .parquet) still need a full read — openpyxl/pyarrow can't
# parse their schema from a truncated byte range.
_HEADER_ONLY_EXTS = {".csv", ".json"}
_HEADER_READ_BYTES = 65536


def _load_yaml(path) -> dict:
    p = Path(path)
    if not p.exists():
        return {}
    with open(p, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


@functools.lru_cache(maxsize=1)
def load_sap_table_index() -> dict[str, frozenset[str]]:
    """Build a {TABLE_NAME: frozenset(expected_columns)} index from sap_table_columns.yaml.

    Columns are unioned across all entities a table participates in, then
    normalised to upper-case. The result is cached for the process lifetime.
    """
    yaml_path = COMMON_DIR / "sap_table_columns.yaml"
    try:
        cfg = _load_yaml(yaml_path)
    except Exception:
        return {}

    index: dict[str, set[str]] = {}
    for _entity, entity_def in (cfg.get("entities") or {}).items():
        for table, cols in (entity_def.get("table_columns") or {}).items():
            tbl = str(table).strip().upper()
            index.setdefault(tbl, set()).update(c.strip().upper() for c in (cols or []))

    return {t: frozenset(cols) for t, cols in index.items() if cols}


@functools.lru_cache(maxsize=1)
def load_sap_table_to_datasets() -> dict[str, frozenset[str]]:
    """Build a {TABLE_NAME: frozenset(dataset_keys)} index from sap_mandatory_tables.yaml.

    sap_mandatory_tables.yaml groups tables under pipelines named workorder/floc/
    tasklist/material — the same dataset keys user_config.yaml's join_overrides is
    keyed by (see p0.utils.user_config.get_sap_join_columns). A table can appear
    under more than one pipeline (e.g. EQUI feeds workorder, floc, and tasklist),
    so this maps each table to every dataset that consumes it.
    """
    yaml_path = COMMON_DIR / "sap_mandatory_tables.yaml"
    try:
        cfg = _load_yaml(yaml_path)
    except Exception:
        return {}

    index: dict[str, set[str]] = {}
    for dataset, pipeline_def in (cfg.get("pipelines") or {}).items():
        for table_entry in pipeline_def.get("tables") or []:
            tbl = str(table_entry.get("name", "")).strip().upper()
            if tbl:
                index.setdefault(tbl, set()).add(dataset)

    return {t: frozenset(datasets) for t, datasets in index.items()}


def read_file_columns(detail: bytes, ext: str) -> set[str]:
    """Return the set of upper-case column names found in the first row of *detail*.

    Returns an empty set if the format is unreadable or unsupported.
    """
    try:
        if ext == ".csv":
            # Try a few common delimiters; csv.Sniffer can mis-detect on tiny files.
            text = detail[:8192].decode("utf-8", errors="replace")
            first_line = text.splitlines()[0] if text.splitlines() else ""
            for delim in (",", ";", "\t", "|"):
                cols = list(csv.reader([first_line], delimiter=delim))[0]
                if len(cols) > 1:
                    return {c.strip().upper() for c in cols if c.strip()}
            # Single-column or delimiter unknown — return whatever we have
            return {c.strip().upper() for c in cols if c.strip()}

        if ext in (".xlsx", ".xls"):
            try:
                import openpyxl  # noqa: PLC0415
                wb = openpyxl.load_workbook(io.BytesIO(detail), read_only=True, data_only=True)
                ws = wb.active or next(iter(wb.worksheets), None)
                if ws is None:
                    return set()
                first_row = next(ws.iter_rows(min_row=1, max_row=1, values_only=True), None)
                wb.close()
                if first_row is None:
                    return set()
                return {str(c).strip().upper() for c in first_row if c is not None and str(c).strip()}
            except Exception:
                return set()

        if ext == ".parquet":
            try:
                import pyarrow.parquet as pq  # noqa: PLC0415
                schema = pq.read_schema(io.BytesIO(detail))
                return {n.strip().upper() for n in schema.names}
            except Exception:
                return set()

        if ext == ".json":
            import json  # noqa: PLC0415
            try:
                payload = json.loads(detail[:32768].decode("utf-8", errors="replace"))
                if isinstance(payload, list) and payload:
                    payload = payload[0]
                if isinstance(payload, dict):
                    return {k.strip().upper() for k in payload.keys()}
            except Exception:
                pass
            return set()

    except Exception:
        pass

    return set()


def detect_sap_table(filename: str, detail: bytes, ext: str) -> dict:
    """Identify which SAP table a file belongs to.

    Strategy:
      1. If the filename stem matches a known table name (after progressively
         stripping _ / - separated suffixes) → matched_by='filename'.
         SAP table names never contain _ or -, so splitting on the first
         separator and checking the leading segment is safe and handles
         real-world names like EQUI_01_06_Jan2023 or AUFK_export_2024.
      2. Otherwise read the file's column headers and score them against
         each table's expected columns from sap_table_columns.yaml.
         Tables with fewer than CONTENT_MATCH_MIN_EXPECTED expected columns
         are excluded — a 1-column table like IFLO scores 1.0 against any
         file that happens to contain TPLNR, which is a false positive.
         Best-scoring table above SAP_CONTENT_MATCH_THRESHOLD wins →
         matched_by='column_content'.
      3. If nothing meets the threshold → matched_by='unrecognized'.

    Returns a dict with keys: matched_by, detected_table, match_score (0-1).
    match_score is omitted for filename matches (always 1.0 by definition).
    """
    table_index = load_sap_table_index()

    # --- Step 1: filename-based check ---
    stem = Path(filename).stem.upper()

    # Try the full stem first (plain "EQUI", "AUFK", etc.).
    if stem in table_index:
        return {"matched_by": "filename", "detected_table": stem, "match_score": 1.0}

    # SAP table names never contain _ or -.  Split on the first separator and
    # try each leading segment from longest to shortest, so that a file named
    # EQUI_01_06_Jan2023 → segments ["EQUI","01","06","JAN2023"] → tries
    # "EQUI_01_06_JAN2023", "EQUI_01_06", "EQUI_01", "EQUI" in order.
    # The first hit wins, giving the most-specific match.
    if re.search(r"[_-]", stem):
        raw_parts = re.split(r"[_-]", stem)
        for end in range(len(raw_parts) - 1, 0, -1):
            candidate = "_".join(raw_parts[:end])
            if candidate in table_index:
                return {"matched_by": "filename", "detected_table": candidate, "match_score": 1.0}

    # --- Step 2: content-based fallback ---
    file_cols = read_file_columns(detail, ext)
    if not file_cols or not table_index:
        return {"matched_by": "unrecognized", "detected_table": None, "match_score": 0.0}

    best_table: str | None = None
    best_score = 0.0
    tie = False
    for table, expected in table_index.items():
        # Skip tables whose expected-column set is too small to discriminate.
        # A 1-column table (e.g. IFLO expects only TPLNR) scores 1.0 against
        # any SAP file that has TPLNR — a guaranteed false positive.
        if len(expected) < CONTENT_MATCH_MIN_EXPECTED:
            continue
        score = len(expected & file_cols) / len(expected)
        if score > best_score:
            best_score = score
            best_table = table
            tie = False
        elif score == best_score and score > 0:
            tie = True

    # A tie between two or more tables at the top score is ambiguous — picking
    # one would just reflect sap_table_columns.yaml's dict order, not a real
    # signal, so treat it the same as no match.
    if tie:
        return {
            "matched_by": "unrecognized",
            "detected_table": None,
            "match_score": round(best_score, 3),
        }

    if best_score >= SAP_CONTENT_MATCH_THRESHOLD:
        return {
            "matched_by": "column_content",
            "detected_table": best_table,
            "match_score": round(best_score, 3),
        }

    return {
        "matched_by": "unrecognized",
        "detected_table": None,
        "match_score": round(best_score, 3),
    }


def detect_sap_table_at_path(path: str) -> dict:
    """Run detect_sap_table() against a file already sitting in RustFS/ADLS/local staging.

    Reads the file's bytes from *path* (s3:// or local) and delegates to
    detect_sap_table(). Returns {"matched_by": "unrecognized", ...} if the
    file can't be read at all.
    """
    from p0.utils import fs as _fs

    filename = _fs.basename(path)
    ext = Path(filename).suffix.lower()
    read_size = _HEADER_READ_BYTES if ext in _HEADER_ONLY_EXTS else -1

    try:
        if _fs.is_s3(path):
            with _fs.get_fs().open(_fs.strip_scheme(path), "rb") as fh:
                detail = fh.read(read_size)
        else:
            with open(path, "rb") as fh:
                detail = fh.read(read_size)
    except Exception:
        return {"matched_by": "unrecognized", "detected_table": None, "match_score": 0.0}

    return detect_sap_table(filename, detail, ext)
