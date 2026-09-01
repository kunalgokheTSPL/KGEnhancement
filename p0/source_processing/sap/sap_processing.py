from __future__ import annotations
import os
from p0.utils.fs import glob_files, exists
from p0.utils.io import load_table
import pandas as pd
from p0.source_processing.sap.sap_merging_impl import (
    run_sap_merges,
    _merge_safe,
    _ensure_str,
)
from p0.source_processing.sap.sap_merging_xlsx import (
    run_sap_merges_from_xlsx,
)


def _has_individual_sap_tables(sap_in: str) -> bool:
    """Check if the directory contains individual SAP transaction table files"""
    for table in ("AUFK", "EQUI", "MARA", "AFKO"):
        for ext in (".csv", ".parquet", ".xlsx"):
            if exists(f"{sap_in}/{table}{ext}"):
                return True
        if glob_files(f"{sap_in}/{table}_*.csv"):
            return True
        if glob_files(f"{sap_in}/{table}_*.parquet"):
            return True
        if glob_files(f"{sap_in}/{table}_*.xlsx"):
            return True
    return False


def _load_custom_tables(
    sap_in: str,
    user_cfg: dict,
) -> dict[str, pd.DataFrame]:
    """Discover and process custom SAP tables defined in user_config.yaml."""
    custom_cfg = user_cfg.get("sap_processing", {}).get("custom_tables", {})
    if not custom_cfg:
        return {}

    join_overrides = user_cfg.get("sap_processing", {}).get("join_overrides", {})

    results: dict[str, pd.DataFrame] = {}

    for entry_key, entry in custom_cfg.items():
        table_name = entry.get("table", entry_key.upper())
        label = entry.get("label", table_name)

        try:
            df = load_table(sap_in, table_name)
        except FileNotFoundError:
            try:
                df = load_table(sap_in, table_name.upper())
            except FileNotFoundError:
                try:
                    df = load_table(sap_in, table_name.lower())
                except FileNotFoundError:
                    print(
                        f"  [sap/custom] Skipping {table_name}: file not found in {sap_in}"
                    )
                    continue

        print(
            f"  [sap/custom] Loaded {table_name}: {len(df)} rows, {len(df.columns)} cols"
        )

        joins = join_overrides.get(entry_key, {})
        if joins:
            for join_step, join_cfg in joins.items():
                if not isinstance(join_cfg, dict):
                    continue
                right_table = join_cfg.get("table")
                join_cols = join_cfg.get("on", [])
                join_how = join_cfg.get("how", "left")
                if not right_table or not join_cols:
                    continue
                try:
                    right_df = load_table(sap_in, right_table)
                    df = _ensure_str(df, join_cols)
                    right_df = _ensure_str(right_df, join_cols)
                    df = _merge_safe(df, right_df, on=join_cols, how=join_how)
                    print(
                        f"  [sap/custom] Joined {table_name} + {right_table} on {join_cols}: {len(df)} rows"
                    )
                except FileNotFoundError:
                    print(f"  [sap/custom] Join skipped: {right_table} not found")

        results[entry_key] = df

    return results


def run_sap_source_processing(
    sap_in: str, work_dir: str, user_cfg: dict | None = None, plant_code_id: str | None = None
) -> tuple[dict[str, pd.DataFrame], dict[str, set[str]]]:
    os.makedirs(work_dir, exist_ok=True)

    sheet_mappings = (
        (user_cfg or {}).get("sap_processing", {}).get("sheet_mappings", {}).get(plant_code_id, {})
        if plant_code_id
        else {}
    )

    if sheet_mappings:
        print(
            f"  [sap] Plant has sheet_mappings configured ({len(sheet_mappings)} sheet(s)) "
            "→ forcing XLSX processor"
        )
        outputs, primary_columns = run_sap_merges_from_xlsx(
            sap_in=sap_in, user_cfg=user_cfg, plant_code_id=plant_code_id
        )
    elif _has_individual_sap_tables(sap_in):
        print(
            "  [sap] Detected individual SAP table files → using table-merge processor"
        )
        outputs, primary_columns = run_sap_merges(sap_in=sap_in, user_cfg=user_cfg, plant_code_id=plant_code_id)
    else:
        print("  [sap] Detected consolidated XLSX reports → using XLSX processor")
        outputs, primary_columns = run_sap_merges_from_xlsx(
            sap_in=sap_in, user_cfg=user_cfg, plant_code_id=plant_code_id
        )

    custom_tables = _load_custom_tables(sap_in, user_cfg or {})
    for name, df in custom_tables.items():
        outputs[name] = df
        primary_columns[name] = set(df.columns)
        print(f"  [sap/custom] Added custom dataset: {name} ({len(df)} rows)")

    for name, df in outputs.items():
        df.to_csv(os.path.join(work_dir, f"{name}_merged.csv"), index=False)

    return outputs, primary_columns
