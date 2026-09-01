"""Processing for ALERTS Excel workbooks.

The ALERTS workbook contains two sheets:

- Open Alerts
- Closed Alerts

Both sheets share the same source structure and are combined into one
canonical ALERTS dataset.

Important identity rules:

1. ``tag_number`` always preserves the original Tag Number supplied
   by the source file.
2. ``equipment_id`` is a separate canonical equipment identity.
3. When P&ID equipment is available, Tag Number is matched against it.
4. When Tag Number is blank, Description is inspected for possible
   equipment references.
5. No equipment_id is fabricated when no P&ID match exists.
"""

from __future__ import annotations

import hashlib
import logging
import re
from pathlib import Path
from typing import Iterable

import pandas as pd
import yaml

_log = logging.getLogger("cdm.alerts")


DEFAULT_COLUMN_MAPPING = {
    "Index": "source_index",
    "Region": "region",
    "Field": "field_name",
    "OEM": "oem",
    "Unit Type": "unit_type",
    "Tag Number": "tag_number",
    "Priority": "priority",
    "Status": "status",
    "Title": "title",
    "Description": "description",
    "Tag": "tag",
    "Current Action": "current_action",
    "Action Taken": "action_taken",
    "Date Open": "date_open",
    "Date closed": "date_closed",
    "Equipment Class": "equipment_class",
    "Subunit": "subunit",
    "Maintainable Item": "maintainable_item",
    "Fault Category": "fault_category",
}


DEFAULT_SHEETS = {
    "Open Alerts": "open",
    "Closed Alerts": "closed",
}


def _clean_text(value) -> str | None:
    """Return a trimmed string or None for an empty value."""

    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    text = str(value).strip()

    if not text:
        return None

    if text.lower() in {
        "nan",
        "none",
        "null",
        "nat",
    }:
        return None

    return text


def _source_record_id(
    *,
    plant_code_id: str,
    source_index: object = None,
    tag_number: object = None,
    date_open: object = None,
) -> str:
    """
    Build a stable logical identity for an ALERTS source record.

    ALERTS business identity is:
        plant_code_id + source_index + tag_number + date_open

    Title, description, sheet name, row number, alert status,
    source file and upload batch are deliberately excluded.
    """

    business_values = [
        _clean_text(source_index),
        _clean_text(tag_number),
        _clean_text(date_open),
    ]

    if not any(business_values):
        raise ValueError(
            "ALERTS record has no usable business identity: "
            "source_index, tag_number, and date_open are all empty"
        )

    values = [
        _clean_text(plant_code_id) or "",
        *(value or "" for value in business_values),
    ]

    seed = "|".join(
        value.strip().lower()
        for value in values
    )

    digest = hashlib.sha256(
        seed.encode("utf-8")
    ).hexdigest()

    return f"alerts:{digest}"


def _normalise_asset_token(value: str | None) -> str | None:
    """Normalize an equipment/tag candidate for tolerant matching.

    This value is only used for matching. It does NOT replace the original
    tag_number stored from the source workbook.
    """

    text = _clean_text(value)

    if not text:
        return None

    text = text.upper()

    # Remove common whitespace and separator variation while preserving
    # letters and digits for equipment identity matching.
    text = re.sub(
        r"[^A-Z0-9]+",
        "",
        text,
    )

    return text or None


def _build_pnid_lookup(
    pnid_equipment_ids: Iterable[str] | None,
) -> dict[str, str]:
    """Build normalized -> canonical P&ID equipment_id lookup."""

    lookup: dict[str, str] = {}

    for value in pnid_equipment_ids or []:
        canonical = _clean_text(value)

        if not canonical:
            continue

        normalized = _normalise_asset_token(
            canonical
        )

        if normalized:
            # Preserve the actual P&ID value as the output.
            lookup.setdefault(
                normalized,
                canonical,
            )

    return lookup


def _match_tag_number(
    tag_number: str | None,
    pnid_lookup: dict[str, str],
) -> tuple[str | None, str | None, float | None]:
    """Try exact normalized matching from Tag Number to P&ID."""

    raw = _clean_text(
        tag_number
    )

    if not raw:
        return None, None, None

    normalized = _normalise_asset_token(
        raw
    )

    if not normalized:
        return None, None, None

    matched = pnid_lookup.get(
        normalized
    )

    if matched:
        return (
            matched,
            "tag_number",
            1.0,
        )

    return (
        None,
        "tag_number_unmatched",
        0.0,
    )


def _description_candidates(
    description: str | None,
) -> list[str]:
    """Extract possible equipment-like tokens from Description.

    This is intentionally conservative. A description candidate is not
    accepted as equipment_id until it matches an actual P&ID equipment ID.
    """

    text = _clean_text(
        description
    )

    if not text:
        return []

    # Examples this can catch:
    #   P-101
    #   SDV-2930
    #   G7510
    #   PVES-060
    #
    # Require at least one alphabetic character and one number so ordinary
    # words are not treated as equipment tags.
    candidates = re.findall(
        r"\b(?=[A-Za-z0-9_-]*[A-Za-z])"
        r"(?=[A-Za-z0-9_-]*\d)"
        r"[A-Za-z0-9]+(?:[-_][A-Za-z0-9]+)*\b",
        text,
    )

    result: list[str] = []
    seen: set[str] = set()

    for candidate in candidates:
        normalized = _normalise_asset_token(
            candidate
        )

        if not normalized:
            continue

        if normalized in seen:
            continue

        seen.add(normalized)
        result.append(
            candidate
        )

    return result


def _match_from_description(
    description: str | None,
    pnid_lookup: dict[str, str],
) -> tuple[str | None, str | None, float | None]:
    """Try candidates from Description against real P&ID equipment."""

    candidates = _description_candidates(
        description
    )

    if not candidates:
        return (
            None,
            "description_no_candidate",
            None,
        )

    for candidate in candidates:
        normalized = _normalise_asset_token(
            candidate
        )

        matched = pnid_lookup.get(
            normalized or ""
        )

        if matched:
            return (
                matched,
                "description",
                0.8,
            )

    return (
        None,
        "description_unmatched",
        0.0,
    )


def resolve_equipment_id(
    *,
    tag_number: str | None,
    description: str | None,
    pnid_equipment_ids: Iterable[str] | None = None,
) -> dict:
    """Resolve the canonical P&ID equipment_id for one ALERTS row.

    Resolution order:

    Tag Number
        -> normalize
        -> compare with P&ID equipment IDs

    If Tag Number is blank:
        Description
        -> extract possible equipment reference
        -> normalize
        -> compare with P&ID equipment IDs

    A source Tag Number is NEVER copied into equipment_id merely because
    it exists. equipment_id only contains a successful canonical match.
    """

    pnid_lookup = _build_pnid_lookup(
        pnid_equipment_ids
    )

    raw_tag = _clean_text(
        tag_number
    )

    if raw_tag:
        equipment_id, method, confidence = (
        _match_tag_number(
            raw_tag,
            pnid_lookup,
        )
    )

        return {
        "equipment_id": equipment_id,
        "normalized_asset": (
            equipment_id
            if equipment_id
            else _normalise_asset_token(raw_tag)
        ),
        "asset_match_method": method,
        "asset_match_confidence": confidence,
    }

    equipment_id, method, confidence = (
        _match_from_description(
            description,
            pnid_lookup,
        )
    )

    return {
        "equipment_id": equipment_id,
        "normalized_asset": (
            equipment_id
            if equipment_id
            else None
        ),
        "asset_match_method": method,
        "asset_match_confidence": confidence,
}


def _load_alerts_config(
    config_file: str | Path | None,
) -> dict:
    """Load the ALERTS template configuration."""

    if config_file is None:
        return {
            "column_mapping": dict(
                DEFAULT_COLUMN_MAPPING
            ),
            "sheets": [
                {
                    "name": name,
                    "alert_status": status,
                }
                for name, status
                in DEFAULT_SHEETS.items()
            ],
        }

    path = Path(
        config_file
    )

    if not path.exists():
        raise FileNotFoundError(
            f"ALERTS config not found: {path}"
        )

    with open(
        path,
        "r",
        encoding="utf-8",
    ) as fh:
        payload = yaml.safe_load(
            fh
        ) or {}

    config = (
        payload.get("alerts")
        or {}
    )

    return config


def _meaningful_columns(
    frame: pd.DataFrame,
) -> pd.DataFrame:
    """Drop Excel formatting-only columns which contain no real values."""

    if frame.empty:
        return frame

    keep: list[str] = []

    for column in frame.columns:
        series = frame[column]

        if series.notna().any():
            keep.append(
                column
            )

    return frame[
        keep
    ].copy()


def _read_alert_sheet(
    workbook_path: str | Path,
    *,
    sheet_name: str,
    alert_status: str,
    column_mapping: dict[str, str],
    source_file: str,
    plant_code_id: str,
    upload_batch_id: str | None,
    pnid_equipment_ids: Iterable[str] | None,
) -> pd.DataFrame:
    """Read and normalize one Open/Closed Alerts sheet."""

    frame = pd.read_excel(
        workbook_path,
        sheet_name=sheet_name,
        dtype=object,
    )

    frame = _meaningful_columns(
        frame
    )

    # Normalize Excel headers before mapping.
    frame.columns = [
        str(column).strip()
        for column in frame.columns
    ]

    # Only map known ALERTS columns.
    rename_map = {
        source: target
        for source, target
        in column_mapping.items()
        if source in frame.columns
    }

    frame = frame.rename(
        columns=rename_map
    )

    expected_columns = list(
        column_mapping.values()
    )

    # Guarantee one common schema for both sheets.
    for column in expected_columns:
        if column not in frame.columns:
            frame[column] = None

    frame = frame[
        expected_columns
    ].copy()

    # ---------------------------------------------------------
    # REMOVE EMPTY / FORMATTING-ONLY EXCEL ROWS
    # ---------------------------------------------------------

    # Convert whitespace-only strings to null so Excel formatting-only
    # rows do not survive as real ALERTS records.
    frame = frame.replace(
        r"^\s*$",
        pd.NA,
        regex=True,
    )

    # Preserve the original Excel row position for diagnostics/lineage.
    frame["_source_row_number"] = frame.index + 2

    # Remove rows where every canonical ALERTS source field is empty.
    frame = frame.dropna(
        how="all",
        subset=expected_columns,
    )

    # Defensive cleanup for values represented as text such as
    # "", "nan", "None", "null", etc.
    meaningful_mask = frame[expected_columns].apply(
        lambda row: any(
            _clean_text(value) is not None
            for value in row
        ),
        axis=1,
    )
    frame = frame.loc[meaningful_mask].copy()

    if frame.empty:
        return frame

    # ---------------------------------------------------------
    # DROP ROWS WITH NO USABLE ALERTS BUSINESS IDENTITY
    # ---------------------------------------------------------
    #
    # ALERTS identity is based on:
    #   source_index + tag_number + date_open
    #
    # Some Excel rows can contain notes/formatting or descriptive values
    # while all three identity fields are blank. Such rows cannot produce
    # a stable source_record_id, so skip them instead of aborting the whole
    # workbook.
    identity_columns = [
        column
        for column in (
            "source_index",
            "tag_number",
            "date_open",
        )
        if column in frame.columns
    ]

    if identity_columns:
        identity_present = frame[identity_columns].apply(
            lambda row: any(
                _clean_text(value) is not None
                for value in row
            ),
            axis=1,
        )

        skipped_identityless = int((~identity_present).sum())

        if skipped_identityless:
            _log.warning(
                "[alerts] skipped %d row(s) with no usable business identity "
                "(source_index, tag_number, date_open all empty)",
                skipped_identityless,
            )

        frame = frame.loc[identity_present].copy()

    if frame.empty:
        return frame

    # Preserve Tag Number exactly as supplied except for trimming surrounding
    # whitespace. Do not replace it with the P&ID equipment identity.
    frame["tag_number"] = frame[
        "tag_number"
    ].map(
        _clean_text
    )

    frame["description"] = frame[
        "description"
    ].map(
        _clean_text
    )

    # Explicit normalized source-section status.
    frame["alert_status"] = (
        alert_status
    )

    # Lineage.
    frame["source_file"] = (
        source_file
    )
    frame["plant_code_id"] = (
        plant_code_id
    )
    frame["upload_batch_id"] = (
        upload_batch_id
    )
    
    # Stable logical source identity.
    #
    # Index alone is not unique in the real ALERTS workbook. Use the
    # business-key composite Index + Tag Number + Date Open instead.
    # Row number, sheet name, alert status, source file, and upload_batch_id
    # are deliberately excluded because they can change across revisions.
    frame["source_record_id"] = [
        _source_record_id(
            plant_code_id=plant_code_id,
            source_index=row.get("source_index"),
            tag_number=row.get("tag_number"),
            date_open=row.get("date_open"),
            
        )
        for row in frame.to_dict(
            orient="records"
        )
    ]

    frame["source_system"] = "alerts"

    # Resolve canonical equipment identity separately.
    resolution = [
        resolve_equipment_id(
            tag_number=row.get(
                "tag_number"
            ),
            description=row.get(
                "description"
            ),
            pnid_equipment_ids=(
                pnid_equipment_ids
            ),
        )
        for row
        in frame.to_dict(
            orient="records"
        )
    ]

    resolution_frame = pd.DataFrame(
        resolution,
        index=frame.index,
    )

    for column in (
        "equipment_id",
        "normalized_asset",
        "asset_match_method",
        "asset_match_confidence",
    ):
        frame[column] = resolution_frame[
            column
        ]

    return frame


def process_alerts_workbook(
    workbook_path: str | Path,
    *,
    plant_code_id: str,
    upload_batch_id: str | None = None,
    source_file: str | None = None,
    config_file: str | Path | None = None,
    pnid_equipment_ids: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Read both ALERTS sheets and return one canonical DataFrame."""

    workbook_path = Path(
        workbook_path
    )

    if not workbook_path.exists():
        raise FileNotFoundError(
            f"ALERTS workbook not found: {workbook_path}"
        )

    if not plant_code_id:
        raise ValueError(
            "plant_code_id is required"
        )

    source_file = (
        source_file
        or workbook_path.name
    )

    config = _load_alerts_config(
        config_file
    )

    column_mapping = (
        config.get(
            "column_mapping"
        )
        or DEFAULT_COLUMN_MAPPING
    )

    configured_sheets = (
        config.get("sheets")
        or [
            {
                "name": name,
                "alert_status": status,
            }
            for name, status
            in DEFAULT_SHEETS.items()
        ]
    )

    workbook = pd.ExcelFile(
        workbook_path
    )

    available_sheets = set(
        workbook.sheet_names
    )

    output_frames: list[pd.DataFrame] = []

    for sheet in configured_sheets:
        sheet_name = str(
            sheet.get("name")
            or ""
        ).strip()

        alert_status = str(
            sheet.get(
                "alert_status"
            )
            or ""
        ).strip().lower()

        if not sheet_name:
            continue

        if sheet_name not in available_sheets:
            _log.warning(
                "[alerts] expected sheet '%s' not found in %s",
                sheet_name,
                source_file,
            )
            continue

        frame = _read_alert_sheet(
            workbook_path,
            sheet_name=sheet_name,
            alert_status=alert_status,
            column_mapping=column_mapping,
            source_file=source_file,
            plant_code_id=plant_code_id,
            upload_batch_id=upload_batch_id,
            pnid_equipment_ids=pnid_equipment_ids,
        )

        if frame.empty:
            continue

        _log.info(
            "[alerts] %s → %d row(s)",
            sheet_name,
            len(frame),
        )

        output_frames.append(
            frame
        )

    if not output_frames:
        raise ValueError(
            "No ALERTS rows were found in the configured sheets"
        )

    result = pd.concat(
        output_frames,
        ignore_index=True,
    )

    # Validate the business identity after all configured sheets have been
    # combined. This catches true composite-key collisions both within a
    # sheet and across Open/Closed Alerts while still allowing repeated or
    # blank Index values when the remaining key fields disambiguate them.
    source_ids = result["source_record_id"]

    duplicate_mask = source_ids.duplicated(keep=False)
    duplicate_source_ids = int(source_ids.duplicated().sum())

    if duplicate_source_ids:
        _log.warning(
        "[alerts] %d duplicate business-key row(s) found; "
        "disambiguating with sheet/row position",
        duplicate_source_ids,
    )

        occurrence = result.groupby(
        "source_record_id",
        dropna=False,
    ).cumcount()

        for idx in result.index[duplicate_mask]:
            if occurrence.loc[idx] == 0:
                continue

            sheet = str(
            result.at[idx, "source_sheet"]
            if "source_sheet" in result.columns
            else ""
            ).strip()

            row_number = str(
            result.at[idx, "source_row_number"]
            if "source_row_number" in result.columns
            else idx
            ).strip()

            result.at[idx, "source_record_id"] = (
            f"{result.at[idx, 'source_record_id']}"
            f":{sheet}:{row_number}"
            )

    # Stable canonical ordering.
    ordered_columns = [
        "source_index",
        "region",
        "field_name",
        "oem",
        "unit_type",
        "tag_number",
        "equipment_id",
        "priority",
        "status",
        "title",
        "description",
        "tag",
        "current_action",
        "action_taken",
        "date_open",
        "date_closed",
        "equipment_class",
        "subunit",
        "maintainable_item",
        "fault_category",
        "alert_status",
        "normalized_asset",
        "asset_match_method",
        "asset_match_confidence",
        "source_system",
        "source_record_id",
        "source_file",
        "plant_code_id",
        "upload_batch_id",
    ]

    for column in ordered_columns:
        if column not in result.columns:
            result[column] = None

    result = result[
        ordered_columns
    ]

    _log.info(
        "[alerts] workbook complete: file=%s rows=%d open=%d closed=%d",
        source_file,
        len(result),
        int(
            (
                result["alert_status"]
                == "open"
            ).sum()
        ),
        int(
            (
                result["alert_status"]
                == "closed"
            ).sum()
        ),
    )

    return result