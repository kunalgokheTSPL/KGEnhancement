"""Runtime inspection helpers for user-defined Other Files."""

from __future__ import annotations

import io
import re
from pathlib import Path

import pandas as pd

from . import file_metadata as _file_metadata

from p0.source_processing.other_files.other_files_processing import (
    normalize_column_name,
)


# ---------------------------------------------------------
# Supported Other Files
# ---------------------------------------------------------

UNSTRUCTURED_EXTENSIONS = {
    ".pdf",
}

STRUCTURED_EXTENSIONS = {
    ".csv",
    ".tsv",
    ".xlsx",
    ".xls",
    ".parquet",
}

SUPPORTED_EXTENSIONS = (
    UNSTRUCTURED_EXTENSIONS
    | STRUCTURED_EXTENSIONS
)


# Candidate names that commonly represent equipment / asset identity.
EQUIPMENT_COLUMN_CANDIDATES = {
    "equipment_id",
    "equipment",
    "equipment_no",
    "equipment_number",
    "equipment_tag",
    "equipment_tag_no",
    "equipment_tag_number",
    "tag",
    "tag_no",
    "tag_number",
    "asset",
    "asset_id",
    "asset_no",
    "asset_number",
}


def normalize_user_type(value: str) -> str:
    """
    Convert a user-defined data/document type into a safe key.

    Example:
        "Valve Inspection Report" -> "valve_inspection_report"
    """

    value = str(value or "").strip().lower()

    value = re.sub(
        r"[^a-z0-9]+",
        "_",
        value,
    )

    return value.strip("_")


def normalize_columns(
    columns: list[str],
) -> list[str]:
    """
    Normalize source column names while guaranteeing uniqueness.

    Uses the same normalize_column_name() implementation as the runtime
    Other Files processor so inspection and processing agree on the
    normalized base names.

    Examples:
        ["Status", "STATUS"]
            -> ["status", "status_2"]

        ["Status", "Status_2", "STATUS"]
            -> ["status", "status_2", "status_3"]
    """

    used: set[str] = set()
    result: list[str] = []

    for index, column in enumerate(columns):
        base_name = normalize_column_name(
            column
        )

        if not base_name:
            base_name = f"column_{index + 1}"

        candidate = base_name
        suffix = 2

        while candidate in used:
            candidate = f"{base_name}_{suffix}"
            suffix += 1

        used.add(candidate)
        result.append(candidate)

    return result


def detect_data_mode(
    file_name: str,
) -> str:
    """
    Detect which runtime pipeline should process the file.

    PDF:
        unstructured

    CSV / XLS / XLSX / TSV / Parquet:
        structured
    """

    extension = Path(
        file_name or ""
    ).suffix.lower()

    if extension in UNSTRUCTURED_EXTENSIONS:
        return "unstructured"

    if extension in STRUCTURED_EXTENSIONS:
        return "structured"

    raise ValueError(
        "Unsupported Other Files format. "
        "Supported formats are: "
        + ", ".join(
            sorted(SUPPORTED_EXTENSIONS)
        )
    )


def find_equipment_column(
    columns: list[str],
) -> str | None:
    """
    Best-effort detection of an equipment identifier column.

    The returned value is the normalized column name.
    """

    for column in columns:
        if column in EQUIPMENT_COLUMN_CANDIDATES:
            return column

    # Slightly more flexible fallback.
    for column in columns:
        if (
            "equipment" in column
            and (
                "id" in column
                or "no" in column
                or "number" in column
                or "tag" in column
            )
        ):
            return column

    return None


def _parquet_columns(
    payload: bytes,
) -> list[str]:
    """Read only the Parquet schema/column names."""

    try:
        df = pd.read_parquet(
            io.BytesIO(payload)
        )

        return [
            str(column)
            for column in df.columns
        ]

    except Exception as exc:
        raise ValueError(
            f"Unable to read Parquet file: {exc}"
        ) from exc


def _structured_columns(
    file_name: str,
    payload: bytes,
) -> tuple[list[str], dict]:
    """
    Read source columns from a structured file.

    For CSV/TSV/XLS/XLSX we reuse the existing server-side
    file_metadata inspection service.
    """

    extension = Path(
        file_name or ""
    ).suffix.lower()

    # Parquet is not currently handled by file_metadata.inspect().
    if extension == ".parquet":
        raw_columns = _parquet_columns(
            payload
        )

        metadata = {
            "kind": "spreadsheet",
            "extension": "parquet",
            "columns": raw_columns,
        }

        return raw_columns, metadata

    metadata = _file_metadata.inspect(
        file_name,
        payload,
    )

    sheets = metadata.get(
        "sheets"
    ) or []

    if not sheets:
        raise ValueError(
            "Could not detect columns from the uploaded structured file."
        )

    # Expose the first sheet's columns.
    raw_columns = sheets[0].get(
        "columns"
    ) or []

    raw_columns = [
        str(column)
        for column in raw_columns
        if str(column).strip()
    ]

    if not raw_columns:
        raise ValueError(
            "The uploaded structured file does not contain a readable header."
        )

    return raw_columns, metadata


def inspect_other_file(
    *,
    file_name: str,
    payload: bytes,
    user_defined_type: str,
) -> dict:
    """
    Inspect one user-defined Other File and decide its runtime flow.

    This does NOT process or store the file.

    It only answers:
        - structured or unstructured?
        - what type did the user define?
        - which columns exist?
        - do we need extraction fields?
        - can we identify an equipment column?
    """

    if not file_name:
        raise ValueError(
            "file name is required"
        )

    if not payload:
        raise ValueError(
            "uploaded file is empty"
        )

    type_label = str(
        user_defined_type or ""
    ).strip()

    if not type_label:
        raise ValueError(
            "user_defined_type is required"
        )

    type_key = normalize_user_type(
        type_label
    )

    if not type_key:
        raise ValueError(
            "user_defined_type must contain "
            "at least one letter or number"
        )

    extension = Path(
        file_name
    ).suffix.lower()

    data_mode = detect_data_mode(
        file_name
    )

    # -----------------------------------------------------
    # PDF / unstructured
    # -----------------------------------------------------

    if data_mode == "unstructured":
        metadata = _file_metadata.inspect(
            file_name,
            payload,
        )

        return {
            "document_type": type_key,
            "document_type_label": type_label,
            "file_name": file_name,
            "extension": extension,
            "data_mode": "unstructured",

            # UI should display dynamic extraction-field input.
            "requires_source_columns": True,

            # We do NOT know these until the user provides them.
            "source_columns": [],

            "columns": [],
            "normalized_columns": [],

            "preserve_all_columns": False,

            "equipment_column": None,
            "equipment_mapping_required": False,

            "metadata": metadata,
        }

    # -----------------------------------------------------
    # CSV / Excel / Parquet / structured
    # -----------------------------------------------------

    raw_columns, metadata = _structured_columns(
        file_name,
        payload,
    )

    # IMPORTANT:
    # This uses the same base-name normalizer as the runtime pipeline
    # and guarantees globally unique final names.
    normalized_columns = normalize_columns(
        raw_columns
    )

    equipment_column = find_equipment_column(
        normalized_columns
    )

    return {
        "document_type": type_key,
        "document_type_label": type_label,
        "file_name": file_name,
        "extension": extension,
        "data_mode": "structured",

        # Structured flow doesn't ask user what fields to extract.
        "requires_source_columns": False,
        "source_columns": [],

        "columns": raw_columns,
        "normalized_columns": normalized_columns,

        # All user columns must survive into processed output.
        "preserve_all_columns": True,

        "equipment_column": equipment_column,

        # UI can allow manual mapping if automatic detection fails.
        "equipment_mapping_required": (
            equipment_column is None
        ),

        "metadata": metadata,
    }