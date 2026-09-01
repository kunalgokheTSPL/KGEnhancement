"""Shared asset identity: equipment UIDs and functional-location parsing."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Any

import pandas as pd




def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _norm(x: Any) -> str:
    """Normalize to clean uppercase string, strip whitespace and nan artifacts."""
    if x is None:
        return ""
    s = str(x).strip()
    if s.lower() in ("nan", "none", "nat", ""):
        return ""
    s = s.upper()
    s = re.sub(r"\s+", " ", s).strip()
    return s




def normalize_asset_id(value: Any) -> str:
    """Return a normalized, uppercase, whitespace-collapsed asset identifier."""
    return _norm(value)


_FLOC_LEVEL = re.compile(r"[./\\]+")

NO_SITE = "-"


def split_site_prefix(value: Any) -> tuple[str, str]:
    """Split a leading site or platform token off a tag, when one is present."""
    s = _norm(value)
    if " " not in s:
        return "", s
    head, tail = s.split(" ", 1)
    tail = tail.strip()
    if tail and any(c.isdigit() for c in tail):
        return head, tail
    return "", s


def normalize_tag(value: Any) -> str:
    """Reduce an equipment tag to its comparable core, without site or separators."""
    _, tag = split_site_prefix(value)
    return re.sub(r"[^A-Z0-9]", "", tag)


@dataclass(frozen=True)
class Floc:
    """One parsed functional location."""

    raw: str
    site: str
    levels: tuple[str, ...]
    parent: str
    item: str


def parse_floc(value: Any) -> Floc | None:
    """Parse a functional location into site, levels and its parent/item pair."""
    raw = _norm(value)
    if not raw:
        return None
    levels = tuple(p for p in _FLOC_LEVEL.split(raw) if p)
    if not levels:
        return None
    leaf = levels[-1]
    parent, _, item = leaf.partition("-")
    if not item:
        parent, item = "", leaf
    site = levels[0].split("-", 1)[0]
    return Floc(
        raw=raw,
        site=site,
        levels=levels,
        parent=re.sub(r"[^A-Z0-9]", "", parent),
        item=re.sub(r"[^A-Z0-9]", "", item),
    )


def floc_site(value: Any) -> str:
    """Return the site code a functional location belongs to."""
    parsed = parse_floc(value)
    return parsed.site if parsed else NO_SITE


def floc_keys(value: Any) -> tuple[str, ...]:
    """Return the tag keys a functional location can be joined on, item first."""
    parsed = parse_floc(value)
    if parsed is None:
        return ()
    return tuple(k for k in (parsed.item, parsed.parent) if k)


def make_equipment_uid(plant_code_id: str, normalized_asset: str) -> str:
    """Generate a deterministic equipment UID from (plant_code_id, normalized_asset)."""
    key = f"{_norm(plant_code_id)}|{_norm(normalized_asset)}"
    return f"eq:{_sha1(key)}"


def assign_equipment_uid(
    df: pd.DataFrame,
    plant_code_id: str,
    uid_col: str = "equipment_uid",
    asset_col: str = "normalized_asset",
    overwrite_blank: bool = True,
) -> pd.DataFrame:
    """Assign deterministic equipment UIDs to a DataFrame in-place."""
    if df is None or df.empty:
        return df

    df = df.copy()

    if uid_col not in df.columns:
        df[uid_col] = ""

    if asset_col not in df.columns:
        return df

    if overwrite_blank:
        mask = df[uid_col].isna() | (df[uid_col].astype(str).str.strip() == "")
    else:
        mask = pd.Series([True] * len(df), index=df.index)

    if not mask.any():
        return df

    plant = _norm(plant_code_id)

    def _make(asset_val: Any) -> str:
        a = _norm(asset_val)
        if not a:
            return ""
        return make_equipment_uid(plant, a)

    df.loc[mask, uid_col] = df.loc[mask, asset_col].map(_make)
    return df


def fix_connectivity_refs(
    df: pd.DataFrame,
    equipment_df: pd.DataFrame,
    from_ref_col: str = "from_equipment_ref",
    to_ref_col: str = "to_equipment_ref",
    asset_col: str = "normalized_asset",
) -> pd.DataFrame:
    """Replace integer row-index values in from/to_equipment_ref columns with"""
    if df is None or df.empty or equipment_df is None or equipment_df.empty:
        return df

    if asset_col not in equipment_df.columns:
        return df

    idx_to_tag: dict[str, str] = {
        str(i): str(row[asset_col]).strip()
        for i, row in equipment_df.iterrows()
        if str(row.get(asset_col, "")).strip()
    }

    df = df.copy()

    def _resolve(val: Any) -> str:
        s = str(val).strip()
        if s.isdigit() and s in idx_to_tag:
            return idx_to_tag[s]
        return s

    for col in (from_ref_col, to_ref_col):
        if col in df.columns:
            df[col] = df[col].map(_resolve)

    return df
