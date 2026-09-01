from __future__ import annotations

import re
from typing import Iterable

import pandas as pd


_PID_RE = re.compile(r"(\d{3}[A-Z]{2}\d+(?:-\d+)?)")


def _norm_tag(value: object) -> str:
    s = "" if value is None else str(value)
    s = re.sub(r"[^A-Z0-9\-]+", "", s.upper())
    return s.strip()


def _iter_values(row: pd.Series, cols: Iterable[str]) -> Iterable[str]:
    for c in cols:
        if c in row.index:
            v = row.get(c)
            if pd.notna(v):
                txt = str(v).strip()
                if txt and txt.lower() != "nan":
                    yield txt


def _extract_pid_candidates(row: pd.Series) -> list[str]:
    candidates: list[str] = []

    for v in _iter_values(
        row, ["normalized_asset", "functional_location", "floc", "equipment_tag"]
    ):
        n = _norm_tag(v)
        if n:
            candidates.append(n)

    for v in _iter_values(row, ["functional_location", "floc", "description"]):
        for hit in _PID_RE.findall(v.upper()):
            n = _norm_tag(hit)
            if n:
                candidates.append(n)

    seen = set()
    out = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out


def build_pid_to_sap_bridge(
    sap_assets_df: pd.DataFrame,
    pid_ref_df: pd.DataFrame,
) -> pd.DataFrame:
    """Build a P&ID tag -> SAP EQUNR bridge."""
    cols = [
        "plant_code_id",
        "pid_tag",
        "sap_equnr",
        "sap_floc",
        "match_method",
        "confidence",
    ]

    if (
        sap_assets_df is None
        or sap_assets_df.empty
        or pid_ref_df is None
        or pid_ref_df.empty
    ):
        return pd.DataFrame(columns=cols)

    pid_tags = {
        _norm_tag(v)
        for v in pid_ref_df.get("normalized_tag", pd.Series([], dtype=str)).tolist()
        if _norm_tag(v)
    }
    if not pid_tags:
        return pd.DataFrame(columns=cols)

    rows: list[dict] = []

    for _, row in sap_assets_df.iterrows():
        sap_equnr = ""
        for v in _iter_values(row, ["equipment_id", "sap_equipment_id"]):
            sap_equnr = v
            break
        if not sap_equnr:
            continue

        sap_floc = ""
        for v in _iter_values(row, ["functional_location", "floc"]):
            sap_floc = v
            break

        plant_code_id = ""
        for v in _iter_values(row, ["plant_code_id"]):
            plant_code_id = v
            break

        pid_tag = ""
        method = ""
        confidence = 0.0

        for c in _extract_pid_candidates(row):
            if c in pid_tags:
                pid_tag = c
                method = "floc_or_text_exact"
                confidence = 1.0
                break

        if not pid_tag:
            continue

        rows.append(
            {
                "plant_code_id": plant_code_id,
                "pid_tag": pid_tag,
                "sap_equnr": str(sap_equnr).strip(),
                "sap_floc": str(sap_floc).strip(),
                "match_method": method,
                "confidence": confidence,
            }
        )

    if not rows:
        return pd.DataFrame(columns=cols)

    bridge = pd.DataFrame(rows)
    bridge = bridge[bridge["sap_equnr"].astype(str).str.strip() != ""].copy()
    bridge = bridge.drop_duplicates(
        subset=["plant_code_id", "pid_tag", "sap_equnr"], keep="first"
    )
    bridge = bridge.sort_values(
        ["plant_code_id", "pid_tag", "sap_equnr"], kind="stable"
    ).reset_index(drop=True)
    return bridge[cols]


def apply_pid_to_sap_bridge(
    df: pd.DataFrame,
    bridge_df: pd.DataFrame,
    *,
    pid_key_col: str = "normalized_asset",
    target_col: str = "equipment_id",
) -> pd.DataFrame:
    """Replace/upgrade target_col (usually equipment_id) using bridge mapping:"""
    if df is None or df.empty or bridge_df is None or bridge_df.empty:
        return df
    if pid_key_col not in df.columns:
        return df

    out = df.copy()
    if target_col not in out.columns:
        out[target_col] = ""

    b = bridge_df.copy()
    if "pid_tag" not in b.columns or "sap_equnr" not in b.columns:
        return out

    b["pid_tag_norm"] = b["pid_tag"].map(_norm_tag)
    b = b[b["pid_tag_norm"] != ""]
    b = b.drop_duplicates(subset=["pid_tag_norm"], keep="first")
    pid_to_sap = dict(zip(b["pid_tag_norm"], b["sap_equnr"].astype(str)))

    key_norm = out[pid_key_col].map(_norm_tag)
    mapped = key_norm.map(pid_to_sap)
    has_map = mapped.notna() & (mapped.astype(str).str.strip() != "")
    out.loc[has_map, target_col] = mapped[has_map].astype(str).str.strip()

    return out
