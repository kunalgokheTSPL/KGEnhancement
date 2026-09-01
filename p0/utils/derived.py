from __future__ import annotations

import pandas as pd
from datetime import datetime, timezone
import hashlib
import os


def _sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def apply_derived_fields(
    df: pd.DataFrame,
    derived_cfg: dict,
    dataset_name: str,
    *,
    source_name: str | None = None,
    plant_code_id: str | None = None,
) -> pd.DataFrame:
    """Adds standard fields required by frozen templates:"""
    if df is None or df.empty:
        return df

    df = df.copy()
    now = datetime.now(timezone.utc).isoformat()

    if plant_code_id is None:
        plant_code_id = os.environ.get("PLANT_CODE")
    if plant_code_id is None:
        for c in ["plant_code_id", "WERKS", "IWERK", "SWERK", "BUKRS"]:
            if c in df.columns and df[c].notna().any():
                plant_code_id = str(df[c].dropna().iloc[0]).strip()
                break
    if plant_code_id is None:
        plant_code_id = "UNKNOWN"

    if "plant_code_id" not in df.columns:
        df["plant_code_id"] = plant_code_id
    else:
        df["plant_code_id"] = (
            df["plant_code_id"].astype(str).str.strip().replace({"nan": "", "None": ""})
        )
        df.loc[df["plant_code_id"] == "", "plant_code_id"] = plant_code_id

    if "ingested_at" not in df.columns:
        df["ingested_at"] = now
    if "updated_at" not in df.columns:
        df["updated_at"] = now

    SOURCE_SYSTEM_MAP = {
        "timeseries": "TS",
        "safety_limits": "TS",
        "pid": "PID",
        "documents": "DOCS",
        "fmea": "FMEA",
        "standards": "STANDARDS",
    }
    source_system = "SAP"
    if source_name:
        if source_name in SOURCE_SYSTEM_MAP:
            source_system = SOURCE_SYSTEM_MAP[source_name]
        elif source_name.startswith("sap_"):
            source_system = "SAP"
    if "source_system" not in df.columns:
        df["source_system"] = source_system

    natural_keys = []
    for k in [
        "plant_code_id",
        "tag_name",
        "wo_number",
        "AUFNR",
        "QMNUM",
        "equipment_id",
        "EQUNR",
        "functional_location",
        "TPLNR",
        "task_list_id",
        "PLNNR",
        "material_code",
        "MATNR",
        "document_id",
    ]:
        if k in df.columns:
            natural_keys.append(k)
    if not natural_keys:
        natural_keys = [df.columns[0]]

    if "source_record_id" not in df.columns:

        def build_id(row) -> str:
            vals = [str(row.get(k, "")).strip() for k in natural_keys]
            return _sha1("|".join(vals))

        df["source_record_id"] = df.apply(build_id, axis=1)

    return df
