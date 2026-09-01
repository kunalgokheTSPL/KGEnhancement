"""SAP source processor for consolidated XLSX report exports."""

from __future__ import annotations

import os
from p0.utils.fs import glob_files, mtime, read_excel, excel_book

import pandas as pd




def _list_xlsx_files(base_path: str) -> list[str]:
    """All xlsx/xls files directly under base_path."""
    return [
        path
        for path in glob_files(f"{base_path}/*")
        if os.path.basename(path).lower().endswith((".xlsx", ".xls"))
    ]


def _load_table_by_sap_code(
    sap_in: str, table_code: str, sheet_mappings: dict[str, str]
) -> pd.DataFrame | None:
    """Scan every xlsx file in sap_in for a sheet holding the given SAP table.

    A sheet matches when its name is mapped to table_code in sheet_mappings
    (e.g. "EQ" -> "EQUI"), or when a sheet/file is already named after the
    SAP table directly. Returns None if no match is found anywhere.
    """
    table_code = table_code.strip().upper()
    mapped_names = {
        name.strip().lower()
        for name, mapped in sheet_mappings.items()
        if str(mapped).strip().upper() == table_code
    }
    files = _list_xlsx_files(sap_in)

    for path in files:
        filename = os.path.basename(path)
        try:
            with excel_book(path) as book:
                sheet_names = list(book.sheet_names)
        except Exception:
            continue

        for sheet_name in sheet_names:
            normalized = sheet_name.strip().lower()
            is_match = normalized in mapped_names or normalized == table_code.lower()
            if is_match:
                try:
                    df = _load_xlsx(path, sheet=sheet_name)
                except Exception:
                    continue
                print(
                    f"  [sap/xlsx] Matched {table_code}: {filename}::{sheet_name} "
                    f"({len(df)} rows, {len(df.columns)} cols)"
                )
                return df

        if table_code.lower() in filename.lower():
            try:
                df = _load_xlsx(path)
            except Exception:
                continue
            print(
                f"  [sap/xlsx] Matched {table_code} by filename: {filename} "
                f"({len(df)} rows, {len(df.columns)} cols)"
            )
            return df

    return None


def _find_files(
    base_path: str, *keywords: str, extensions: tuple[str, ...] = (".xlsx", ".xls")
) -> list[str]:
    """Find files whose name contains ALL given keywords (case-insensitive)."""
    wanted = tuple(e.lower() for e in extensions)
    results = []
    for path in glob_files(f"{base_path}/*"):
        name_lower = os.path.basename(path).lower()
        if not name_lower.endswith(wanted):
            continue
        if all(kw.lower() in name_lower for kw in keywords):
            results.append(path)
    return sorted(results, key=mtime, reverse=True)


def _load_xlsx(path: str, sheet: str | int = 0) -> pd.DataFrame:
    """Load an Excel sheet, strip column names and string values."""
    df = read_excel(path, sheet_name=sheet, dtype=str)
    df.columns = [str(c).strip() for c in df.columns]
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace({"nan": "", "None": "", "NaT": "", "<NA>": ""})
    return df


def _find_col(df: pd.DataFrame, *candidates: str) -> str | None:
    """Return the first column name from candidates that exists in df."""
    for c in candidates:
        if c in df.columns:
            return c
    return None




def run_sap_merges_from_xlsx(
    sap_in: str,
    user_cfg: dict | None = None,
    plant_code_id: str | None = None,
) -> tuple[dict[str, pd.DataFrame], dict[str, set[str]]]:
    """Load consolidated XLSX SAP reports and produce the standard dataset dict."""

    sheet_mappings: dict[str, str] = {}
    if plant_code_id:
        sheet_mappings = (
            (user_cfg or {})
            .get("sap_processing", {})
            .get("sheet_mappings", {})
            .get(plant_code_id, {})
        )
    if sheet_mappings:
        print(f"  [sap/xlsx] Using sheet_mappings for {plant_code_id}: {sheet_mappings}")

    df_workorder = (
        _load_table_by_sap_code(sap_in, "AUFK", sheet_mappings) if sheet_mappings else None
    )
    if df_workorder is not None:
        print(f"  [sap/xlsx] Loaded work orders via sheet_mappings: {len(df_workorder)} rows")
    else:
        wo_frames: list[pd.DataFrame] = []

        startup_files = _find_files(sap_in, "startup")
        if startup_files:
            updated = [f for f in startup_files if "updated" in os.path.basename(f).lower()]
            chosen = updated[0] if updated else startup_files[0]
            df_startup = _load_xlsx(chosen)
            wo_frames.append(df_startup)
            print(
                f"  [sap/xlsx] Loaded work orders: {os.path.basename(chosen)} ({len(df_startup)} rows, {len(df_startup.columns)} cols)"
            )

        years_files = _find_files(sap_in, "history", "year")
        if years_files:
            df_years = _load_xlsx(years_files[0])
            wo_frames.append(df_years)
            print(
                f"  [sap/xlsx] Loaded work orders: {os.path.basename(years_files[0])} ({len(df_years)} rows, {len(df_years.columns)} cols)"
            )

        if wo_frames:
            df_workorder = pd.concat(wo_frames, ignore_index=True, sort=False)
            order_col = _find_col(
                df_workorder, "Order", "Order Number", "Work Order", "AUFNR"
            )
            if order_col:
                before = len(df_workorder)
                df_workorder = df_workorder.drop_duplicates(
                    subset=[order_col], keep="first"
                )
                print(
                    f"  [sap/xlsx] Combined work orders: {before} → {len(df_workorder)} rows (dedup on {order_col})"
                )
        else:
            df_workorder = pd.DataFrame()
            print("  [sap/xlsx] ⚠ No work order XLSX files found")

    df_floc = (
        _load_table_by_sap_code(sap_in, "EQUI", sheet_mappings) if sheet_mappings else None
    )
    if df_floc is not None:
        print(f"  [sap/xlsx] Loaded floc/assets via sheet_mappings: {len(df_floc)} rows")
    elif not df_workorder.empty:
        equip_col = _find_col(
            df_workorder, "Equipment", "Equipment Number", "EQUNR", "Equipment#"
        )
        floc_col = _find_col(
            df_workorder,
            "Functional Loc.",
            "Functional Location",
            "Func. Loc.",
            "TPLNR",
        )
        dedup_cols = [c for c in [equip_col, floc_col] if c is not None]
        if dedup_cols:
            df_floc = df_workorder.drop_duplicates(
                subset=dedup_cols, keep="first"
            ).copy()
        else:
            df_floc = df_workorder.copy()
        print(f"  [sap/xlsx] Derived floc/assets: {len(df_floc)} unique rows")
    else:
        df_floc = pd.DataFrame()

    df_tasklist = (
        _load_table_by_sap_code(sap_in, "MAPL", sheet_mappings) if sheet_mappings else None
    )
    if df_tasklist is not None:
        print(f"  [sap/xlsx] Loaded task lists via sheet_mappings: {len(df_tasklist)} rows")
    else:
        df_tasklist = pd.DataFrame()
        if not df_workorder.empty:
            group_col = _find_col(df_workorder, "Group", "Task List Group", "PLNNR")
            if group_col:
                mask = df_workorder[group_col].astype(str).str.strip().ne("")
                df_tl = df_workorder[mask].copy()
                counter_col = _find_col(df_tl, "Group Counter", "Counter", "PLNAL")
                dedup = [group_col] + ([counter_col] if counter_col else [])
                df_tasklist = df_tl.drop_duplicates(subset=dedup, keep="first")
                print(f"  [sap/xlsx] Derived task lists: {len(df_tasklist)} unique groups")

    df_notification = (
        _load_table_by_sap_code(sap_in, "QMEL", sheet_mappings) if sheet_mappings else None
    )
    if df_notification is not None:
        print(f"  [sap/xlsx] Loaded notifications via sheet_mappings: {len(df_notification)} rows")
    else:
        df_notification = pd.DataFrame()
        if not df_workorder.empty:
            notif_col = _find_col(df_workorder, "Notification", "QMNUM")
            if notif_col:
                mask = df_workorder[notif_col].astype(str).str.strip().ne("")
                df_notification = (
                    df_workorder[mask]
                    .drop_duplicates(subset=[notif_col], keep="first")
                    .copy()
                )
                print(f"  [sap/xlsx] Derived notifications: {len(df_notification)} unique")

    mat_files = _find_files(sap_in, "material")
    if not mat_files:
        mat_files = _find_files(sap_in, "history", "material")
    if mat_files:
        df_material = _load_xlsx(mat_files[0])
        print(
            f"  [sap/xlsx] Loaded materials: {os.path.basename(mat_files[0])} ({len(df_material)} rows)"
        )
    else:
        df_material = pd.DataFrame()
        print("  [sap/xlsx] No material XLSX found (non-critical)")

    df_bom = pd.DataFrame()
    bom_files = _find_files(sap_in, "bom")
    if bom_files:
        df_bom = _load_xlsx(bom_files[0])
        print(
            f"  [sap/xlsx] Loaded BOM: {os.path.basename(bom_files[0])} ({len(df_bom)} rows)"
        )

    outputs: dict[str, pd.DataFrame] = {
        "workorder": df_workorder,
        "floc": df_floc,
        "tasklist": df_tasklist,
        "material": df_material,
    }

    if not df_notification.empty:
        outputs["notification"] = df_notification
    if not df_bom.empty:
        outputs["bom"] = df_bom

    _add_aliases(outputs)

    print(
        f"  [sap/xlsx] Datasets produced: {', '.join(f'{k}({len(v)})' for k, v in outputs.items())}"
    )

    primary_columns = {name: set(df.columns) for name, df in outputs.items()}

    return outputs, primary_columns


def _add_aliases(outputs: dict[str, pd.DataFrame]) -> None:
    """Add synthetic column aliases so the column rename config can populate"""

    tl = outputs.get("tasklist")
    if tl is not None and not tl.empty:
        tl = tl.copy()
        group_col = _find_col(tl, "Group", "Task List Group")
        if group_col and "PLNNR" not in tl.columns:
            tl["PLNNR"] = tl[group_col]
        counter_col = _find_col(tl, "Group Counter", "Counter")
        if counter_col and "PLNAL" not in tl.columns:
            tl["PLNAL"] = tl[counter_col]
        if "VORNR" not in tl.columns and "operation_no" not in tl.columns:
            tl["VORNR"] = "0010"
        outputs["tasklist"] = tl

    bom = outputs.get("bom")
    if bom is not None and not bom.empty:
        bom = bom.copy()
        bom_id_col = _find_col(bom, "9BOM #", "BOM", "STLNR")
        if bom_id_col:
            bom["POSNR"] = (
                bom.groupby(bom_id_col)
                .cumcount()
                .add(1)
                .mul(10)
                .astype(str)
                .str.zfill(4)
            )
        else:
            bom["POSNR"] = (pd.RangeIndex(len(bom)) + 1).astype(str).str.zfill(4)
        outputs["bom"] = bom

    floc = outputs.get("floc")
    if floc is not None and not floc.empty:
        floc = floc.copy()
        equip_col = _find_col(floc, "Equipment", "Equipment Number")
        if equip_col and "EQUNR" not in floc.columns:
            floc["EQUNR"] = floc[equip_col]
        floc_col = _find_col(
            floc, "Functional Loc.", "Functional Location", "Func. Loc."
        )
        if floc_col and "TPLNR" not in floc.columns:
            floc["TPLNR"] = floc[floc_col]
        outputs["floc"] = floc

    wo = outputs.get("workorder")
    if wo is not None and not wo.empty:
        wo = wo.copy()
        order_col = _find_col(wo, "Order", "Order Number", "Work Order")
        if order_col and "AUFNR" not in wo.columns:
            wo["AUFNR"] = wo[order_col]
        equip_col = _find_col(wo, "Equipment", "Equipment Number")
        if equip_col and "EQUNR" not in wo.columns:
            wo["EQUNR"] = wo[equip_col]
        outputs["workorder"] = wo

    notif = outputs.get("notification")
    if notif is not None and not notif.empty:
        notif = notif.copy()
        notif_col = _find_col(notif, "Notification")
        if notif_col and "QMNUM" not in notif.columns:
            notif["QMNUM"] = notif[notif_col]
        outputs["notification"] = notif
