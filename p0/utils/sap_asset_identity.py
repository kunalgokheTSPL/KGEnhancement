"""SAP ↔ P&ID equipment identity matching.

2-tier cascade for resolving SAP equipment IDs against P&ID asset tags:
  1. exact  — normalised strings are identical                  (confidence 1.00)
  2. floc   — equipment tag extracted from functional location
              (specific half of a hyphenated leaf tried before the
              parent), hyphen-stripped, matches a P&ID tag      (confidence 0.85)
"""

from __future__ import annotations

import logging
import re

import pandas as pd

logger = logging.getLogger(__name__)

_FLOC_CONFIDENCE = 0.85
_FLOC_SEGMENT_RE = re.compile(r"^([A-Za-z]+[0-9][A-Za-z0-9]*)")


def _norm_text(value: object) -> str:
    s = "" if value is None else str(value)
    s = s.strip().upper()
    s = re.sub(r"[^A-Z0-9]+", "", s)
    return s


def _extract_floc_tag(floc: object) -> list[str]:
    """Return candidate equipment tags from a functional location, most specific first.

    For a hyphenated leaf segment like "V1010-101LCV101A", the specific half
    ("101LCV101A") is tried before the parent half ("V1010").
    """
    s = "" if floc is None else str(floc).strip()
    if not s:
        return []
    last_segment = s.split(".")[-1]
    parts = last_segment.split("-")

    candidates: list[str] = []
    if len(parts) > 1:
        m_specific = re.match(r"^([A-Za-z0-9]+)", parts[-1])
        if m_specific:
            candidates.append(m_specific.group(1).upper())

    m_parent = _FLOC_SEGMENT_RE.match(parts[0])
    if m_parent:
        tag = m_parent.group(1).upper()
        if tag not in candidates:
            candidates.append(tag)

    if len(parts) > 2:
        rest = "-".join(parts[1:]).upper()
        if rest not in candidates:
            candidates.append(rest)

    if not candidates:
        bare = _strip_hyphens(last_segment)
        if bare:
            candidates.append(bare)

    return candidates


def _strip_hyphens(value: object) -> str:
    s = "" if value is None else str(value).strip().upper()
    return s.replace("-", "")


def _build_reference_assets(reference_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []
    if reference_df is None or reference_df.empty:
        return pd.DataFrame(columns=["asset_key", "normalized_asset", "equipment_tag", "source"])

    possible_asset_cols = [
        "pid_tag", "normalized_asset", "equipment_tag",
        "equipment_id", "functional_location", "floc", "canonical_code",
    ]
    possible_tag_cols = [
        "pid_tag", "equipment_tag", "normalized_asset",
        "equipment_id", "sap_equipment_id",
    ]

    available_asset_cols = [c for c in possible_asset_cols if c in reference_df.columns]
    if not available_asset_cols:
        return pd.DataFrame(columns=["asset_key", "normalized_asset", "equipment_tag", "source"])

    tag_col = next((c for c in possible_tag_cols if c in reference_df.columns), None)

    for _, row in reference_df.iterrows():
        raw_tag = row.get(tag_col) if tag_col else None
        equipment_tag = str(raw_tag).strip() if tag_col and pd.notna(raw_tag) else ""
        for c in available_asset_cols:
            raw_val = row.get(c)
            asset_val = str(raw_val).strip() if pd.notna(raw_val) else ""
            if not asset_val:
                continue
            rows.append({
                "asset_key": _norm_text(asset_val),
                "normalized_asset": asset_val,
                "equipment_tag": equipment_tag,
                "source": c,
            })

    if not rows:
        return pd.DataFrame(columns=["asset_key", "normalized_asset", "equipment_tag", "source"])

    ref = pd.DataFrame(rows)
    ref = ref[ref["asset_key"] != ""].drop_duplicates(
        subset=["asset_key", "normalized_asset", "equipment_tag"]
    )
    return ref


def _pick_best_reference(
    sap_key: str, ref_df: pd.DataFrame, floc: object = None
) -> tuple[str, str, float, str]:
    """Return (normalized_asset, equipment_tag, confidence, method)."""
    if not sap_key or ref_df is None or ref_df.empty:
        return "", "", 0.0, "none"

    # Tier 1: exact match
    exact = ref_df[ref_df["asset_key"] == sap_key]
    if not exact.empty:
        r = exact.iloc[0]
        return str(r["normalized_asset"]), str(r["equipment_tag"]), 1.0, "exact"

    # Tier 2: floc match — try the specific tag first, then the parent tag
    floc_tags = _extract_floc_tag(floc)
    if floc_tags:
        ref_keys = ref_df["equipment_tag"].apply(_strip_hyphens)
        for floc_tag in floc_tags:
            floc_key = _strip_hyphens(floc_tag)
            mask = ref_keys == floc_key
            if mask.any():
                r = ref_df[mask].iloc[0]
                return str(r["normalized_asset"]), str(r["equipment_tag"]), _FLOC_CONFIDENCE, "floc"

    return "", "", 0.0, "none"


def enrich_sap_asset_identity(
    df: pd.DataFrame,
    *,
    reference_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    out = df.copy()

    logger.debug("enrich_sap_asset_identity: input columns=%s", list(out.columns))

    sap_id_cols = [
        "EQUNR", "equnr", "equipment_id", "sap_equipment_id", "sap_id",
        "Equipment", "EQUIPMENT", "equipment_name",
    ]
    sap_id_col = next((c for c in sap_id_cols if c in out.columns), None)

    sap_floc_cols = [
        "TPLNR", "tplnr", "FLOC", "floc", "functional_location",
        "Functional Loc.", "functional_loc", "FUNCTIONAL_LOC",
    ]
    sap_floc_col = next((c for c in sap_floc_cols if c in out.columns), None)
    logger.debug("enrich_sap_asset_identity: floc column found=%s", sap_floc_col)

    if sap_id_col is None:
        out["asset_match_method"] = "none"
        out["asset_match_confidence"] = 0.0
        out["pid_asset_tag"] = ""
        return out

    ref_assets = _build_reference_assets(reference_df) if reference_df is not None else pd.DataFrame()
    has_reference = not ref_assets.empty

    methods: list[str] = []
    confidences: list[float] = []
    pid_tags: list[str] = []

    for _, row in out.iterrows():
        raw_id = str(row.get(sap_id_col) or "").strip()
        sap_key = _norm_text(raw_id)
        raw_floc = row.get(sap_floc_col) if sap_floc_col else None

        if has_reference and sap_key:
            _, pid_tag, confidence, method = _pick_best_reference(sap_key, ref_assets, raw_floc)
        else:
            pid_tag, confidence, method = "", 0.0, "none"

        methods.append(method)
        confidences.append(round(confidence, 4))
        pid_tags.append(pid_tag)

    out["asset_match_method"] = methods
    out["asset_match_confidence"] = confidences
    out["pid_asset_tag"] = pid_tags

    return out