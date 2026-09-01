from __future__ import annotations
import pandas as pd
import re


def _norm(s) -> str:
    """Normalise an identity value, dropping only whole null sentinels."""
    s = "" if s is None else str(s)
    s = s.strip().upper()
    s = re.sub(r"\s+", " ", s)
    return "" if s in ("NAN", "NONE", "NAT", "NULL") else s


def apply_identity_rules(
    df: pd.DataFrame, identity_cfg: dict, dataset_name: str
) -> pd.DataFrame:
    """Ensures:"""
    if df is None or df.empty:
        return df

    df = df.copy()

    if "floc" not in df.columns:
        if "functional_location" in df.columns:
            floc_cols = df.loc[:, df.columns == "functional_location"]

            if isinstance(floc_cols, pd.DataFrame):
                df["floc"] = floc_cols.bfill(axis=1).iloc[:, 0]
            else:
                df["floc"] = floc_cols

        elif "TPLNR" in df.columns:
            df["floc"] = df["TPLNR"]

    if "floc" in df.columns:
        df["floc"] = df["floc"].map(_norm)

    if "normalized_asset" not in df.columns:
        prefer = [
            "equipment_id",
            "EQUNR",
            "functional_location",
            "TPLNR",
            "floc",
            "tag_name",
        ]

        def pick(row) -> str:
            for k in prefer:
                if k in row and pd.notna(row[k]) and str(row[k]).strip() != "":
                    return _norm(row[k])
            return ""

        df["normalized_asset"] = df.apply(pick, axis=1)
    else:
        df["normalized_asset"] = df["normalized_asset"].map(_norm)

    if "plant_code_id" in df.columns:
        df["plant_code_id"] = df["plant_code_id"].map(_norm)

    return df
