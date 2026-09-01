"""Runtime processing helpers for user-defined Other Files."""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd


STRUCTURED_EXTENSIONS = {
    ".csv",
    ".tsv",
    ".xlsx",
    ".xls",
    ".parquet",
}

UNSTRUCTURED_EXTENSIONS = {
    ".pdf",
}


EQUIPMENT_COLUMN_CANDIDATES = (
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
    "asset_id",
    "asset",
    "asset_no",
    "asset_number",
)


def normalize_column_name(
    value: object,
) -> str:
    """
    Convert arbitrary source column names to lower snake_case.

    Examples:
        Equipment No.     -> equipment_no
        Inspection Date   -> inspection_date
        Final Status (%)  -> final_status
    """

    value_str = str(
        value or ""
    ).strip().lower()

    value_str = re.sub(
        r"[^a-z0-9]+",
        "_",
        value_str,
    )

    return value_str.strip("_")


def normalize_columns(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Normalize every input column to lower snake_case while guaranteeing
    unique final column names.
    """

    if df is None:
        return df

    out = df.copy()

    used: set[str] = set()
    renamed: list[str] = []

    for index, column in enumerate(out.columns):
        base_name = normalize_column_name(column)

        if not base_name:
            base_name = f"column_{index + 1}"

        candidate = base_name
        suffix = 2

        while candidate in used:
            candidate = f"{base_name}_{suffix}"
            suffix += 1

        used.add(candidate)
        renamed.append(candidate)

    out.columns = renamed

    return out

def make_parquet_safe(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Make a user-defined structured dataframe safe for Parquet.

    Other Files can contain mixed Excel/CSV values in the same column,
    for example:
        report_date -> [20260101, "2026-02-01", None]

    Pandas permits this as dtype=object, but PyArrow requires a
    consistent type. Mixed object columns are therefore persisted
    as strings while numeric, boolean and datetime columns retain
    their native types.
    """

    if df is None:
        return df

    out = df.copy()

    for column in out.columns:
        series = out[column]

        # Only object columns are potentially mixed.
        if series.dtype != "object":
            continue

        non_null = series.dropna()

        if non_null.empty:
            # Empty user-defined columns are safest as nullable strings.
            out[column] = series.astype("string")
            continue

        python_types = {
            type(value)
            for value in non_null
        }

        # Mixed runtime types cannot safely be inferred by Arrow.
        if len(python_types) > 1:
            out[column] = series.map(
                lambda value: (
                    None
                    if pd.isna(value)
                    else str(value).strip()
                )
            ).astype("string")

            continue

        # Even a pure Python string object column should use
        # pandas' explicit nullable string dtype.
        if all(
            isinstance(value, str)
            for value in non_null
        ):
            out[column] = series.astype("string")

    return out

def normalize_equipment_value(
    value: object,
) -> str:
    """
    Normalize an equipment identifier similarly to the structured
    GLOC flow.

    Examples:
        " p-101 "   -> "P-101"
        10883544.0  -> "10883544"
    """

    if value is None:
        return ""

    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass

    value_str = str(value).strip()

    if value_str.lower() in {
        "",
        "nan",
        "none",
        "nat",
    }:
        return ""

    # Excel numeric IDs frequently become strings like 10883544.0.
    if re.fullmatch(
        r"\d+\.0",
        value_str,
    ):
        value_str = value_str[:-2]

    return value_str.upper()


def detect_equipment_column(
    df: pd.DataFrame,
) -> str | None:
    """
    Find the most likely equipment identifier column after normalization.
    """

    if df is None:
        return None

    columns = set(
        str(column)
        for column in df.columns
    )

    for candidate in EQUIPMENT_COLUMN_CANDIDATES:
        if candidate in columns:
            return candidate

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


def add_equipment_id(
    df: pd.DataFrame,
    *,
    equipment_column: str | None = None,
) -> pd.DataFrame:
    """
    Add/normalize equipment_id while retaining all user columns.

    If no equipment-related source field exists, equipment_id remains blank.
    """

    if df is None:
        return df

    out = df.copy()

    selected = (
        normalize_column_name(
            equipment_column
        )
        if equipment_column
        else None
    )

    if (
        selected
        and selected not in out.columns
):
        raise ValueError(
            f"equipment_column '{equipment_column}' does not exist "
            f"after column normalisation. Available columns: "
            f"{', '.join(str(column) for column in out.columns)}"
    )

    if selected is None:
        selected = detect_equipment_column(
            out
        )

    if selected:
        out["equipment_id"] = (
            out[selected]
            .map(normalize_equipment_value)
        )
    elif "equipment_id" not in out.columns:
        out["equipment_id"] = ""

    else:
        out["equipment_id"] = (
            out["equipment_id"]
            .map(normalize_equipment_value)
        )

    return out


def detect_data_mode(
    file_name: str,
) -> str:
    """Determine which Other Files processing branch to use."""

    extension = Path(
        file_name or ""
    ).suffix.lower()

    if extension in STRUCTURED_EXTENSIONS:
        return "structured"

    if extension in UNSTRUCTURED_EXTENSIONS:
        return "unstructured"

    raise ValueError(
        f"Unsupported Other Files extension '{extension}'"
    )


def read_structured_file(
    file_path: str,
) -> pd.DataFrame:
    """
    Read one structured Other File and preserve all source columns.
    """

    extension = Path(
        file_path
    ).suffix.lower()

    if extension == ".csv":
        return pd.read_csv(
            file_path
        )

    if extension == ".tsv":
        return pd.read_csv(
            file_path,
            sep="\t",
        )

    if extension in {
        ".xlsx",
        ".xls",
    }:
        return pd.read_excel(
            file_path
        )

    if extension == ".parquet":
        return pd.read_parquet(
            file_path
        )

    raise ValueError(
        f"Unsupported structured extension: {extension}"
    )


def process_structured_other_file(
    file_path: str,
    *,
    equipment_column: str | None = None,
) -> pd.DataFrame:
    """
    Generic structured Other Files flow.

    1. Read every source column.
    2. Convert headers to lower snake_case.
    3. Add normalized equipment_id.
    4. Preserve all original data fields.
    """

    df = read_structured_file(
        file_path
    )

    df = normalize_columns(
        df
    )

    df = add_equipment_id(
        df,
        equipment_column=equipment_column,
    )
    df = make_parquet_safe(
        df
)

    return df