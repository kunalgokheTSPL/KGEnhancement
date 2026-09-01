"""
Template-driven P&ID reference loader.

Reads all Excel files from a P&ID staging directory, applies the column-rename
mapping defined in ``pnid_column_rename.yaml`` (sources.pid.mappings), and
returns a single consolidated DataFrame suitable for use as the equipment
reference in the TS/Docs asset-identity enrichment steps.

Usage (all parameters come from config/CLI — zero hardcoding):
    from src.utils.pnid_loader import load_pnid_reference

    ref_df = load_pnid_reference(
        pnid_dir="./data/staging/pnid",
        pnid_rename_cfg=load_yaml("./config/templates/_common/column_rename/pnid_column_rename.yaml"),
    )
    # ref_df has canonical columns: equipment_tag, normalized_tag, description, area_code, ...
"""

from __future__ import annotations

import os
import re
import glob

import pandas as pd




def _apply_pid_rename(df: pd.DataFrame, mappings: dict[str, str]) -> pd.DataFrame:
    """Rename columns using the pid mappings dict (input_col -> canonical_col)."""
    rename_map = {src: tgt for src, tgt in mappings.items() if src in df.columns}
    return df.rename(columns=rename_map)


def _extract_area_code(tag: str, area_code_len: int = 3) -> str:
    """Extract leading numeric area code from a P&ID tag (e.g. '351HG1' -> '351')."""
    if not isinstance(tag, str):
        return ""
    m = re.match(r"^(\d+)", tag.strip())
    if m:
        return m.group(1)[:area_code_len]
    return ""


def _normalize_tag(raw_tag: str, strip_suffixes: list[str]) -> str:
    """Strip vendor-appended suffixes (HWI, NEW, MOD…) and whitespace from a raw P&ID tag."""
    if not isinstance(raw_tag, str):
        return ""
    tag = raw_tag.strip()
    for sfx in strip_suffixes:
        tag = re.sub(
            r"\s+" + re.escape(sfx) + r"(\s+|$)", " ", tag, flags=re.IGNORECASE
        ).strip()
    return tag.strip()




def load_pnid_reference(
    pnid_dir: str,
    pnid_rename_cfg: dict,
) -> pd.DataFrame:
    """Load all P&ID xlsx files from *pnid_dir*, apply the template rename, and"""
    if not pnid_dir or not os.path.isdir(pnid_dir):
        return pd.DataFrame()

    pid_mappings: dict[str, str] = (
        pnid_rename_cfg.get("sources", {}).get("pid", {}).get("mappings", {})
    )
    strip_suffixes: list[str] = (
        pnid_rename_cfg.get("ts_asset_matching", {})
        .get("layer1", {})
        .get("tag_strip_suffixes", ["HWI"])
    )
    area_code_len: int = int(
        pnid_rename_cfg.get("ts_asset_matching", {})
        .get("layer3", {})
        .get("area_code_len", 3)
    )

    xlsx_files = sorted(glob.glob(os.path.join(pnid_dir, "*.xlsx")))
    if not xlsx_files:
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    for fpath in xlsx_files:
        fname = os.path.basename(fpath)
        try:
            raw = pd.read_excel(fpath, dtype=str)
        except Exception as exc:
            print(f"  [pnid_loader] WARNING: could not read {fname}: {exc}")
            continue

        raw = _apply_pid_rename(raw, pid_mappings)

        tag_col = None
        for candidate in ["equipment_tag", "Tag", "TAG"]:
            if candidate in raw.columns:
                tag_col = candidate
                break
        if tag_col is None:
            print(
                f"  [pnid_loader] WARNING: no equipment_tag column found in {fname} — skipped"
            )
            continue

        if tag_col != "equipment_tag":
            raw = raw.rename(columns={tag_col: "equipment_tag"})

        raw["source_pid_file"] = fname
        frames.append(raw)

    if not frames:
        return pd.DataFrame()

    ref = pd.concat(frames, ignore_index=True)

    for col in ["equipment_tag", "description", "drawing_number", "equipment_type"]:
        if col not in ref.columns:
            ref[col] = ""

    ref["equipment_tag"] = ref["equipment_tag"].fillna("").astype(str).str.strip()

    ref = ref[ref["equipment_tag"] != ""].copy()

    ref["normalized_tag"] = ref["equipment_tag"].apply(
        lambda t: _normalize_tag(t, strip_suffixes)
    )
    ref["area_code"] = ref["normalized_tag"].apply(
        lambda t: _extract_area_code(t, area_code_len)
    )

    ref = ref.drop_duplicates(subset=["normalized_tag"]).reset_index(drop=True)

    print(
        f"  [pnid_loader] loaded {len(ref)} P&ID equipment records "
        f"from {len(frames)} file(s) in '{pnid_dir}'"
    )
    return ref
