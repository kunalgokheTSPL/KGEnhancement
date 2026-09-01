"""SAP ↔ P&ID mapping pipeline.

Runs immediately after upload (before any SAP entity pipelines) to produce
a cross-reference between SAP equipment IDs and P&ID asset tags.

Usage:
    python -m pipelines.run_sap_pid_mapping \
        --plant_code_id CEMENT_PLANT_1 \
        --sap_in s3://bucket/p0/staging-pt/CEMENT_PLANT_1/sap \
        --pid_out s3://bucket/p0/processed_data/CEMENT_PLANT_1/pnid \
        --out_dir s3://bucket/p0/processed_data/CEMENT_PLANT_1/sap_mapping
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

import pandas as pd

from p0.utils import fs as _fs
from p0.utils.sap_asset_identity import enrich_sap_asset_identity

_SAP_ID_COL_CANDIDATES = ["EQUNR", "equnr", "equipment_id", "sap_equipment_id", "Equipment", "EQUIPMENT", "equipment_name"]
_SAP_TYPE_COL_CANDIDATES = ["EQTYP", "eqtyp", "equipment_type", "type"]
_SAP_FLOC_COL_CANDIDATES = ["TPLNR", "tplnr", "functional_location", "floc", "Functional Loc.", "functional_loc"]
_SAP_DESC_COL_CANDIDATES = ["EQKTX", "eqktx", "description", "short_text"]

_PNID_PARQUET = "equipment_pid.parquet"
_OUT_PARQUET = "sap_pid_mapping.parquet"


def _setup_logger(log_dir: str | None) -> logging.Logger:
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger = logging.getLogger("sap_mapping_pipeline")
    logger.setLevel(logging.DEBUG)
    if logger.handlers:
        return logger

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
        fh = logging.FileHandler(
            os.path.join(log_dir, "sap_mapping_pipeline.log"), mode="a", encoding="utf-8"
        )
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    return logger


def _resolve_equi_sheet_name(user_cfg: dict | None, plant_code_id: str | None) -> str | None:
    """Look up the sheet name mapped to the EQUI table for this plant."""
    sheet_mappings = (
        (user_cfg or {}).get("sap_processing", {}).get("sheet_mappings", {}).get(plant_code_id, {})
        if plant_code_id
        else {}
    )
    return next(
        (sheet for sheet, table in sheet_mappings.items() if str(table).strip().upper() == "EQUI"),
        None,
    )


def _read_excel_equi_sheet(source, equi_sheet_name: str | None, log: logging.Logger) -> tuple[pd.DataFrame, object]:
    """Read the EQUI sheet from an xlsx workbook, falling back to 'EQ'.

    Returns (df, sheet_name_used).
    """
    for sheet_name in [equi_sheet_name, "EQ"]:
        if sheet_name is None:
            continue
        try:
            df = pd.read_excel(source, sheet_name=sheet_name)
            log.info("[sap_mapping] read EQUI xlsx sheet=%r", sheet_name)
            return df, sheet_name
        except ValueError as exc:
            log.debug("[sap_mapping] sheet %r not found: %s", sheet_name, exc)
            if hasattr(source, "seek"):
                source.seek(0)
            continue
    raise ValueError(
        "No EQUI sheet found: neither the mapped sheet name nor 'EQ' exists in the workbook"
    )


def _read_equi(
    sap_in: str,
    log: logging.Logger,
    plant_code_id: str | None = None,
    user_cfg: dict | None = None,
) -> pd.DataFrame:
    """Find and read the EQUI file from the SAP staging directory."""
    equi_names = ["EQUI.csv", "equi.csv", "EQUI.parquet", "equi.parquet",
                  "EQUI.xlsx", "equi.xlsx"]
    equi_sheet_name = _resolve_equi_sheet_name(user_cfg, plant_code_id)
    sfs = None
    try:
        from p0.driver import get_object_fs
        sfs = get_object_fs(use_listings_cache=False)
    except Exception:
        pass

    for name in equi_names:
        path = f"{sap_in.rstrip('/')}/{name}"
        try:
            if sfs:
                bare = _fs.strip_scheme(path)
                if not sfs.exists(bare):
                    continue
                with sfs.open(bare, "rb") as fh:
                    if name.endswith(".parquet"):
                        df = pd.read_parquet(fh)
                    elif name.endswith((".xlsx", ".xls")):
                        df, _ = _read_excel_equi_sheet(fh, equi_sheet_name, log)
                    else:
                        df = pd.read_csv(fh, low_memory=False)
            else:
                if not os.path.exists(path):
                    continue
                if name.endswith(".parquet"):
                    df = pd.read_parquet(path)
                elif name.endswith((".xlsx", ".xls")):
                    df, _ = _read_excel_equi_sheet(path, equi_sheet_name, log)
                else:
                    df = pd.read_csv(path, low_memory=False)
            log.info("[sap_mapping] loaded EQUI from %s  rows=%d", path, len(df))
            log.info("[sap_mapping] EQUI columns=%s", list(df.columns))
            return df
        except Exception as exc:
            log.debug("[sap_mapping] could not read %s: %s", path, exc)
            continue

    xlsx_paths: list[str] = []
    try:
        base = sap_in.rstrip("/")
        if sfs:
            bare_base = _fs.strip_scheme(base)
            for entry in sfs.ls(bare_base, detail=False):
                if entry.lower().endswith(".xlsx"):
                    xlsx_paths.append(entry)
        elif os.path.isdir(base):
            for entry in os.listdir(base):
                if entry.lower().endswith(".xlsx"):
                    xlsx_paths.append(os.path.join(base, entry))
    except Exception as exc:
        log.debug("[sap_mapping] could not list xlsx files under %s: %s", sap_in, exc)

    for xlsx_path in xlsx_paths:
        display_name = os.path.basename(xlsx_path)
        try:
            if sfs:
                bare = _fs.strip_scheme(xlsx_path) if _fs.is_s3(xlsx_path) else xlsx_path
                with sfs.open(bare, "rb") as fh:
                    df, sheet_used = _read_excel_equi_sheet(fh, equi_sheet_name, log)
            else:
                df, sheet_used = _read_excel_equi_sheet(xlsx_path, equi_sheet_name, log)
        except Exception as exc:
            log.debug("[sap_mapping] could not read EQUI sheet from %s: %s", display_name, exc)
            continue

        if df is not None and len(df) > 0:
            log.info(
                "[sap_mapping] loaded EQUI from %s sheet=%r  rows=%d",
                display_name, sheet_used, len(df),
            )
            log.info("[sap_mapping] EQUI columns=%s", list(df.columns))
            return df

    log.warning("[sap_mapping] EQUI file not found under %s — returning empty DataFrame", sap_in)
    return pd.DataFrame()


def _read_pnid_equipment(pid_out: str, log: logging.Logger, plant_code_id: str) -> pd.DataFrame:
    """Read the processed P&ID equipment parquet, falling back to Postgres."""
    path = f"{pid_out.rstrip('/')}/{_PNID_PARQUET}"
    try:
        from p0.driver import get_object_fs
        sfs = get_object_fs(use_listings_cache=False)
        bare = _fs.strip_scheme(path)
        with sfs.open(bare, "rb") as fh:
            df = pd.read_parquet(fh)
        if not df.empty:
            log.info("[sap_mapping] loaded P&ID equipment from %s  rows=%d", path, len(df))
            return df
        log.warning("[sap_mapping] P&ID equipment parquet at %s is empty — falling back to Postgres", path)
    except Exception as exc:
        log.warning("[sap_mapping] P&ID equipment not available at %s: %s — falling back to Postgres", path, exc)

    try:
        from p0.driver import PostgresDriver
        from utility.middleware import plant_code_ctx

        token = plant_code_ctx.set(plant_code_id)
        try:
            driver = PostgresDriver()
            conn = driver.connect()
        finally:
            plant_code_ctx.reset(token)
        try:
            df = pd.read_sql(
                "SELECT * FROM equipment_pid WHERE plant_code_id = %(plant_code_id)s",
                conn,
                params={"plant_code_id": plant_code_id},
            )
        finally:
            conn.close()
        log.info(
            "[sap_mapping] loaded P&ID equipment from Postgres  plant=%s  rows=%d",
            plant_code_id, len(df),
        )
        return df
    except Exception as exc:
        log.warning(
            "[sap_mapping] P&ID equipment not available from Postgres for plant=%s: %s",
            plant_code_id, exc,
        )
        return pd.DataFrame()


def _build_output(
    equi_df: pd.DataFrame,
    enriched: pd.DataFrame,
    sap_id_col_candidates: list[str] = _SAP_ID_COL_CANDIDATES,
) -> pd.DataFrame:
    """Assemble the canonical output columns for the mapping parquet."""
    sap_id_col = next((c for c in sap_id_col_candidates if c in enriched.columns), None)
    sap_type_col = next((c for c in _SAP_TYPE_COL_CANDIDATES if c in enriched.columns), None)
    sap_floc_col = next((c for c in _SAP_FLOC_COL_CANDIDATES if c in enriched.columns), None)
    sap_desc_col = next((c for c in _SAP_DESC_COL_CANDIDATES if c in enriched.columns), None)

    rows = []
    for _, row in enriched.iterrows():
        rows.append(
            {
                "sap_asset_id": str(row.get(sap_id_col) or "").strip() if sap_id_col else "",
                "pid_asset_tag": str(row.get("pid_asset_tag") or "").strip(),
                "equipment_type": str(row.get(sap_type_col) or "").strip() if sap_type_col else "",
                "plant_location": str(row.get(sap_floc_col) or "").strip() if sap_floc_col else "",
                "equipment_description": str(row.get(sap_desc_col) or "").strip() if sap_desc_col else "",
                "match_confidence": float(row.get("asset_match_confidence") or 0.0),
                "match_method": str(row.get("asset_match_method") or "none"),
            }
        )
    return pd.DataFrame(rows)


def _write_output(out_df: pd.DataFrame, out_dir: str, log: logging.Logger) -> str:
    """Write the mapping parquet to the output directory."""
    path = f"{out_dir.rstrip('/')}/{_OUT_PARQUET}"
    _fs.write_parquet(out_df, path)
    log.info("[sap_mapping] wrote mapping  rows=%d  path=%s", len(out_df), path)
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description="SAP ↔ P&ID mapping pipeline")
    parser.add_argument("--plant_code_id", required=True)
    parser.add_argument("--sap_in", required=True, help="SAP staging directory (s3:// or local)")
    parser.add_argument("--pid_out", required=True, help="Processed P&ID directory containing equipment_pid.parquet")
    parser.add_argument("--out_dir", required=True, help="Output directory for sap_pid_mapping.parquet")
    parser.add_argument("--log_dir", default=None)
    parser.add_argument("--config_dir", required=False, default=None, help="Path to config/ directory (for user_config overrides)")
    args = parser.parse_args()

    log = _setup_logger(args.log_dir)

    from p0.utils.user_config import load_user_config, get_sap_equipment_id_column

    user_cfg = load_user_config(args.config_dir, plant_code_id=args.plant_code_id) if args.config_dir else {}
    sap_id_col_candidates = get_sap_equipment_id_column(
        user_cfg, "workorder", default=_SAP_ID_COL_CANDIDATES, plant_code_id=args.plant_code_id
    )
    log.info(
        "[sap_mapping] START  plant=%s  sap_in=%s  pid_out=%s  out_dir=%s",
        args.plant_code_id,
        args.sap_in,
        args.pid_out,
        args.out_dir,
    )

    equi_df = _read_equi(args.sap_in, log, plant_code_id=args.plant_code_id, user_cfg=user_cfg)
    pid_df = _read_pnid_equipment(args.pid_out, log, args.plant_code_id)

    if equi_df.empty:
        log.warning("[sap_mapping] No EQUI data — writing empty mapping")
        out_df = pd.DataFrame(
            columns=[
                "sap_asset_id", "pid_asset_tag", "equipment_type",
                "plant_location", "equipment_description",
                "match_confidence", "match_method",
            ]
        )
    else:
        log.info("[sap_mapping] enriching %d SAP equipment rows", len(equi_df))
        enriched = enrich_sap_asset_identity(
            equi_df,
            reference_df=pid_df if not pid_df.empty else None,
        )
        out_df = _build_output(equi_df, enriched, sap_id_col_candidates)

    _write_output(out_df, args.out_dir, log)
    log.info("[sap_mapping] DONE  plant=%s", args.plant_code_id)


if __name__ == "__main__":
    main()
