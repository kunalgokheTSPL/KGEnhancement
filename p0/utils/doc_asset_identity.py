"""
Resolve normalized_asset and equipment_id for document rows using source-intrinsic data
+ layered matching against the P&ID entity reference.

Design rationale:
  - Documents derive normalized_asset from data intrinsic to the document source:
      Strategy 1 — equipment_tag already embedded in the doc (set by PDF enrichment, e.g. 311CD-1)
      Strategy 2 — EQUIPMENT_TAG_MAP curated domain knowledge (equipment name → P&ID code)
      Strategy 3 — fallback to whatever label/tag is available
  - The P&ID entity output (equipment.csv from PNID pipeline) is used to enrich
    equipment_id (P&ID tag code) via layered matching — never to set normalized_asset.
  - SAP is intentionally excluded at this stage; the SAP bridge will be added later.

Adds / updates columns:
  - normalized_asset
  - equipment_id
  - asset_match_method   (inline_tag / name_map / name_fallback / none)
  - asset_match_confidence
"""

from __future__ import annotations

import re
from difflib import SequenceMatcher
from typing import Iterable

import pandas as pd



EQUIPMENT_TAG_MAP: dict[str, str] = {
    "crusher": "CP-CR-0001",
    "raw mill": "CP-RM-0001",
    "vrm": "CP-RM-0001",
    "vertical roller mill": "CP-RM-0001",
    "preheater": "CP-PH-0001",
    "pre-heater": "CP-PH-0001",
    "kiln": "CP-KN-0001",
    "rotary kiln": "CP-KN-0001",
    "coal mill": "CP-CO-0001",
    "cooler": "CP-CC-0001",
    "clinker cooler": "CP-CC-0001",
    "cement mill": "CP-CM-0001",
    "ball mill": "CP-CM-0001",
    "packer": "CP-PP-0001",
}


def _norm_text(value: object) -> str:
    s = "" if value is None else str(value)
    s = s.strip().upper()
    s = re.sub(r"[^A-Z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _load_reference_df(reference_candidates: Iterable[str]) -> pd.DataFrame:
    """Load the first available reference table (CSV or Parquet, local or s3)."""
    from p0.utils import fs as _fs

    for p in reference_candidates:
        if not p:
            continue
        if not _fs.exists(p):
            continue
        try:
            if str(p).endswith(".parquet"):
                return _fs.read_parquet(p)
            return _fs.read_csv(p, low_memory=False)
        except Exception:
            continue
    return pd.DataFrame()


def _build_reference_assets(reference_df: pd.DataFrame) -> pd.DataFrame:
    """Build reference table from P&ID entity output for equipment_id enrichment."""
    rows: list[dict] = []
    if reference_df is None or reference_df.empty:
        return pd.DataFrame(
            columns=["asset_key", "normalized_asset", "equipment_id", "source"]
        )

    possible_asset_cols = [
        "normalized_asset",
        "equipment_tag",
        "equipment_id",
        "functional_location",
        "floc",
        "canonical_code",
    ]
    possible_eq_cols = [
        "equipment_tag",
        "normalized_asset",
        "equipment_id",
        "sap_equipment_id",
    ]

    available = [c for c in possible_asset_cols if c in reference_df.columns]
    if not available:
        return pd.DataFrame(
            columns=["asset_key", "normalized_asset", "equipment_id", "source"]
        )

    eq_col = next((c for c in possible_eq_cols if c in reference_df.columns), None)

    for _, row in reference_df.iterrows():
        equipment_id = str(row.get(eq_col, "")).strip() if eq_col else ""
        for c in available:
            raw_val = row.get(c)
            asset_val = str(raw_val).strip() if pd.notna(raw_val) else ""
            if not asset_val:
                continue
            rows.append(
                {
                    "asset_key": _norm_text(asset_val),
                    "normalized_asset": asset_val,
                    "equipment_id": equipment_id,
                    "source": c,
                }
            )

    if not rows:
        return pd.DataFrame(
            columns=["asset_key", "normalized_asset", "equipment_id", "source"]
        )

    ref = pd.DataFrame(rows)
    ref = ref[ref["asset_key"] != ""].drop_duplicates(
        subset=["asset_key", "normalized_asset", "equipment_id"]
    )
    return ref


def _pick_best_reference(
    search_key: str,
    ref_df: pd.DataFrame,
) -> tuple[str, str, float, str]:
    """
    Returns (normalized_asset, equipment_id, confidence, method).
    """
    if not search_key or ref_df is None or ref_df.empty:
        return "", "", 0.0, "none"

    exact = ref_df[ref_df["asset_key"] == search_key]
    if not exact.empty:
        r = exact.iloc[0]
        return str(r["normalized_asset"]), str(r["equipment_id"]), 1.0, "exact"

    contains = ref_df[
        ref_df["asset_key"].apply(
            lambda x: search_key in str(x) or str(x) in search_key
        )
    ]
    if not contains.empty:
        r = contains.iloc[0]
        return str(r["normalized_asset"]), str(r["equipment_id"]), 0.92, "contains"

    cand_df = ref_df
    if len(search_key) >= 3:
        prefix = search_key[:3]
        narrowed = ref_df[ref_df["asset_key"].str.startswith(prefix)]
        if not narrowed.empty:
            cand_df = narrowed

    if len(cand_df) > 2000:
        cand_df = cand_df.head(2000)

    best_score = 0.0
    best_row = None
    for _, r in cand_df.iterrows():
        score = SequenceMatcher(None, search_key, str(r["asset_key"])).ratio()
        if score > best_score:
            best_score = score
            best_row = r

    if best_row is not None and best_score >= 0.80:
        return (
            str(best_row["normalized_asset"]),
            str(best_row["equipment_id"]),
            float(best_score),
            "fuzzy",
        )

    return "", "", 0.0, "none"


def _name_map_lookup(equipment_name: str) -> tuple[str, float]:
    """Use curated EQUIPMENT_TAG_MAP to resolve equipment name → P&ID code."""
    if not equipment_name:
        return "", 0.0
    low = str(equipment_name).strip().lower()
    for pattern in sorted(EQUIPMENT_TAG_MAP, key=len, reverse=True):
        if pattern in low:
            return EQUIPMENT_TAG_MAP[pattern], 0.85
    return "", 0.0


def _try_enrich_equipment_id(
    out_df: pd.DataFrame,
    idx,
    search_key: str,
    ref_assets: pd.DataFrame,
) -> None:
    """Attempt to fill equipment_id (P&ID tag code) from the P&ID reference when not already set."""
    if ref_assets is None or ref_assets.empty:
        return
    if str(out_df.at[idx, "equipment_id"]).strip():
        return
    normed = _norm_text(search_key)
    _, matched_eq, score, _ = _pick_best_reference(normed, ref_assets)
    if matched_eq and score >= 0.85:
        out_df.at[idx, "equipment_id"] = matched_eq


def enrich_document_asset_identity(
    df: pd.DataFrame,
    *,
    reference_candidates: list[str] | None = None,
) -> pd.DataFrame:
    """Resolve normalized_asset and equipment_id for document rows using source-intrinsic data"""
    if df is None or df.empty:
        return df

    out = df.copy()

    if reference_candidates is None:
        reference_candidates = []
    reference_df = _load_reference_df(reference_candidates)
    ref_assets = _build_reference_assets(reference_df)

    for col in ["normalized_asset", "equipment_id"]:
        if col not in out.columns:
            out[col] = ""

    methods: list[str] = []
    confidences: list[float] = []

    for idx, row in out.iterrows():
        equip_tag = str(row.get("equipment_tag", "")).strip()
        equip_name = str(
            row.get("equipment_label", row.get("equipment_name", ""))
        ).strip()

        if equip_tag and (
            re.match(r"^[0-9]{3}[A-Z]", equip_tag)
            or re.match(r"^CP-[A-Z]{2}-\d{4}", equip_tag)
            or re.match(r"^B\d{2}-[A-Z]", equip_tag)
        ):
            out.at[idx, "normalized_asset"] = equip_tag
            methods.append("inline_tag")
            confidences.append(0.95)
            _try_enrich_equipment_id(out, idx, equip_tag, ref_assets)
            continue

        mapped_tag, map_conf = _name_map_lookup(equip_name or equip_tag)
        if mapped_tag:
            out.at[idx, "normalized_asset"] = mapped_tag
            methods.append("name_map")
            confidences.append(map_conf)
            _try_enrich_equipment_id(out, idx, mapped_tag, ref_assets)
            continue

        fallback = equip_tag or equip_name
        if fallback:
            out.at[idx, "normalized_asset"] = fallback
            methods.append("name_fallback")
            confidences.append(0.50)
            _try_enrich_equipment_id(out, idx, fallback, ref_assets)
        else:
            methods.append("none")
            confidences.append(0.0)

    out["asset_match_method"] = methods
    out["asset_match_confidence"] = confidences

    return out
