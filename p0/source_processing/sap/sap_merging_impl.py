from __future__ import annotations

import logging

import pandas as pd
from p0.utils.io import load_table
from p0.utils.user_config import get_sap_join_columns

log = logging.getLogger(__name__)


def _load_table_safe(base_path: str, table_name: str) -> pd.DataFrame:
    """Load a SAP table file; return an empty DataFrame if the file is absent."""
    try:
        return load_table(base_path, table_name)
    except FileNotFoundError:
        log.warning(f"  [sap/merge] {table_name}: not found — skipping (empty DataFrame)")
        return pd.DataFrame()


def _ensure_str(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Force selected columns to clean strings to avoid pandas merge dtype issues."""
    if df is None:
        return None
    if df.empty:
        return df.iloc[:0].copy()
    df = df.copy()
    for c in cols:
        if c in df.columns:
            s = df[c].astype(str)
            s = s.str.replace(r"\.0$", "", regex=True)
            s = s.str.strip()
            s = s.replace({"nan": "", "None": "", "NaT": ""})
            df[c] = s
    return df


# Additive SAP fields eligible to be summed when a fan-out join brings in
# several rows per group (RESB reservation lines, COEP cost lines). This is
# an explicit allowlist, not "any numeric column" -- SAP numeric-looking key
# fields such as GJAHR (fiscal year), PERIO (posting period) and BUZEI (line
# item number) must never be summed: summing three 2024 fiscal years gives
# 6072, not 2024.
_ADDITIVE_SAP_COLUMNS = {
    "DMBTR",   # amount in local currency
    "WTGBTR",  # cost line value, transaction currency (COEP)
    "WKGBTR",  # cost line value, object currency (COEP)
    "MENGE",   # quantity
    "MEGBTR",  # quantity (COEP)
    "LABST",   # valuated stock quantity
    "ERFMG",   # quantity in unit of entry
    "BDMNG",   # reservation requirement quantity (RESB)
    "ENMNG",   # reservation withdrawn quantity (RESB)
}


def _to_numeric(series: pd.Series, col: str | None = None) -> pd.Series:
    """Parse a column that may already be numeric or may hold string numbers
    with thousands separators (e.g. "1,100"). Individual cells that still
    won't parse become NaN, which sum() skips -- that alone doesn't raise.
    But if the whole column comes back as NaN, that's almost always an
    unexpected number format rather than scattered bad cells, so this raises
    instead of silently summing to zero."""
    name = col if col is not None else (series.name or "<unnamed>")
    if pd.api.types.is_numeric_dtype(series):
        return series
    cleaned = series.astype(str).str.replace(",", "", regex=False).str.strip()
    result = pd.to_numeric(cleaned, errors="coerce")

    fail_count = int(result.isna().sum())
    if fail_count:
        log.warning(
            "[sap/merge] column %s: %d value(s) failed numeric coercion and became NaN",
            name, fail_count,
        )
    if len(result) > 0 and fail_count == len(result):
        raise ValueError(
            f"Column {name} could not be parsed — all values are NaN after coercion. "
            "Check for unexpected number formats (e.g. trailing-minus negatives like "
            "50.00- or European decimals like 1.234,56)"
        )
    return result


def _sum_numeric_first_other(
    df: pd.DataFrame, group_cols: list[str], numeric_source_cols: set[str]
) -> pd.DataFrame:
    """Collapse a fanned-out dataframe back to one row per group_cols.

    numeric_source_cols are the columns a fan-out join contributed (e.g. RESB
    quantities or COEP cost lines); of those, only the ones in
    _ADDITIVE_SAP_COLUMNS are summed (parsing string numbers like "1,100"
    first), so cost/quantity lines are combined rather than silently
    discarded while key/identifier fields are left alone. Every other column
    keeps its first value (safe when it's identical across the fan-out, e.g.
    master-data columns already unique per group_cols before the fan-out
    join)."""
    if df is None or df.empty or not group_cols or not all(c in df.columns for c in group_cols):
        return df
    df = df.copy()
    agg: dict[str, str] = {}
    for col in df.columns:
        if col in group_cols:
            continue
        if col in numeric_source_cols and col in _ADDITIVE_SAP_COLUMNS:
            df[col] = _to_numeric(df[col], col)
            agg[col] = "sum"
        else:
            agg[col] = "first"
    if not agg:
        return df.drop_duplicates(subset=group_cols, keep="first")
    return df.groupby(group_cols, as_index=False, dropna=False).agg(agg)


def _merge_safe(
    left: pd.DataFrame,
    right: pd.DataFrame,
    on: list[str] | str,
    how: str = "left",
    suffixes: tuple[str, str] = ("", "_r"),
    drop_right_cols: list[str] | None = None,
    validate: str | None = None,
) -> pd.DataFrame:
    """A safe merge wrapper:"""
    if left is None or left.empty:
        return left
    if right is None or right.empty:
        return left

    on_cols = [on] if isinstance(on, str) else list(on)

    left2 = _ensure_str(left, on_cols)
    right2 = _ensure_str(right, on_cols)

    if drop_right_cols:
        right2 = right2.drop(columns=drop_right_cols, errors="ignore")

    overlap = set(left2.columns).intersection(set(right2.columns)) - set(on_cols)
    for col in overlap:
        if f"{col}_r" in right2.columns:
            right2 = right2.drop(columns=[f"{col}_r"])

    out = pd.merge(left2, right2, on=on_cols, how=how, suffixes=suffixes, validate=validate)
    out = out.loc[:, ~out.columns.duplicated()].copy()
    return out


def run_sap_merges(
    sap_in: str, user_cfg: dict | None = None, plant_code_id: str | None = None
) -> tuple[dict[str, pd.DataFrame], dict[str, set[str]]]:
    """SAP source-processing merges:"""
    ucfg = user_cfg or {}

    AUFK = _load_table_safe(sap_in, "AUFK")
    AFKO = _load_table_safe(sap_in, "AFKO")
    AFVC = _load_table_safe(sap_in, "AFVC")
    AFVV = _load_table_safe(sap_in, "AFVV")
    QMEL = _load_table_safe(sap_in, "QMEL")
    QMSM = _load_table_safe(sap_in, "QMSM")
    JEST = _load_table_safe(sap_in, "JEST")
    JSTO = _load_table_safe(sap_in, "JSTO")
    AFIH = _load_table_safe(sap_in, "AFIH")
    RESB = _load_table_safe(sap_in, "RESB")
    COEP = _load_table_safe(sap_in, "COEP")
    EQUI = _load_table_safe(sap_in, "EQUI")
    IFLOT = _load_table_safe(sap_in, "IFLOT")
    STXH = _load_table_safe(sap_in, "STXH")
    STXL = _load_table_safe(sap_in, "STXL")

    AUFK = _ensure_str(AUFK, ["AUFNR", "EQUNR", "TPLNR"])
    AFKO = _ensure_str(AFKO, ["AUFNR", "AUFPL"])
    AFVC = _ensure_str(AFVC, ["AUFNR", "AUFPL", "VORNR", "APLZL"])
    AFVV = _ensure_str(AFVV, ["AUFPL", "APLZL"])
    QMEL = _ensure_str(QMEL, ["AUFNR", "QMNUM"])
    QMSM = _ensure_str(QMSM, ["QMNUM"])
    JEST = _ensure_str(JEST, ["OBJNR"])
    JSTO = _ensure_str(JSTO, ["OBJNR"])
    AFIH = _ensure_str(AFIH, ["AUFNR", "EQUNR"])
    RESB = _ensure_str(RESB, ["AUFNR", "MATNR"])
    COEP = _ensure_str(COEP, ["AUFNR"])
    EQUI = _ensure_str(EQUI, ["EQUNR"])
    IFLOT = _ensure_str(IFLOT, ["TPLNR", "ILOAN"])
    STXH = _ensure_str(STXH, ["TDOBJECT", "TDNAME", "TDID"])
    STXL = _ensure_str(STXL, ["TDNAME"])

    AUFK = AUFK.copy()
    if "AUFNR" in AUFK.columns:
        AUFK["OBJNR"] = "OR" + AUFK["AUFNR"].astype(str)

    df_workorder = _merge_safe(
        AUFK,
        AFKO,
        on=get_sap_join_columns(ucfg, "workorder", "aufk_afko", ["AUFNR"], plant_code_id=plant_code_id),
        how="left",
    )

    df_workorder = _merge_safe(
        df_workorder,
        AFVC,
        on=get_sap_join_columns(ucfg, "workorder", "aufk_afvc", ["AUFNR"], plant_code_id=plant_code_id),
        how="left",
        drop_right_cols=["AUFPL"],
    )

    _afvc_afvv_on = get_sap_join_columns(
        ucfg, "workorder", "afvc_afvv", ["AUFPL", "VORNR"], plant_code_id=plant_code_id
    )
    _afvc_afvv_on = [
        c for c in _afvc_afvv_on if c in AFVC.columns and c in AFVV.columns
    ]
    if _afvc_afvv_on:
        AFVC_AFVV = _merge_safe(AFVC, AFVV, on=_afvc_afvv_on, how="left")
        if {"AUFNR", "VORNR", "DAUNO_MIN"}.issubset(AFVC_AFVV.columns):
            df_workorder = _merge_safe(
                df_workorder,
                AFVC_AFVV[["AUFNR", "VORNR", "DAUNO_MIN"]].copy(),
                on=["AUFNR", "VORNR"],
                how="left",
            )

    wo_qmel_cols = get_sap_join_columns(ucfg, "workorder", "wo_qmel", ["AUFNR"], plant_code_id=plant_code_id)
    if all(c in QMEL.columns for c in wo_qmel_cols):
        QMEL_agg = QMEL.drop_duplicates(subset=wo_qmel_cols, keep="first")
        df_workorder = _merge_safe(
            df_workorder, QMEL_agg, on=wo_qmel_cols, how="left", suffixes=("", "_QMEL")
        )
    wo_qmsm_cols = get_sap_join_columns(ucfg, "workorder", "wo_qmsm", ["QMNUM"], plant_code_id=plant_code_id)
    if all(c in df_workorder.columns for c in wo_qmsm_cols) and all(
        c in QMSM.columns for c in wo_qmsm_cols
    ):
        QMSM_agg = QMSM.drop_duplicates(subset=wo_qmsm_cols, keep="first")
        df_workorder = _merge_safe(
            df_workorder, QMSM_agg, on=wo_qmsm_cols, how="left", suffixes=("", "_QMSM")
        )

    wo_jest_cols = get_sap_join_columns(ucfg, "workorder", "wo_jest", ["OBJNR"], plant_code_id=plant_code_id)
    if all(c in df_workorder.columns for c in wo_jest_cols) and all(
        c in JEST.columns for c in wo_jest_cols
    ):
        df_workorder = _merge_safe(df_workorder, JEST, on=wo_jest_cols, how="left")
    wo_jsto_cols = get_sap_join_columns(ucfg, "workorder", "wo_jsto", ["OBJNR"], plant_code_id=plant_code_id)
    if all(c in df_workorder.columns for c in wo_jsto_cols) and all(
        c in JSTO.columns for c in wo_jsto_cols
    ):
        df_workorder = _merge_safe(
            df_workorder, JSTO, on=wo_jsto_cols, how="left", suffixes=("", "_JSTO")
        )

    wo_primary_cols = set(df_workorder.columns)

    # RESB (reservation lines) and COEP (cost lines) both have several rows per
    # AUFNR. drop_duplicates(keep="first") silently threw the rest away instead
    # of aggregating them — a work order's actual cost was whichever cost line
    # happened to sort first, not its total. Sum every numeric column each
    # table contributes per AUFNR instead; "first" for any non-numeric column.
    wo_resb_cols = get_sap_join_columns(ucfg, "workorder", "wo_resb", ["AUFNR"], plant_code_id=plant_code_id)
    if all(c in RESB.columns for c in wo_resb_cols):
        RESB_agg = _sum_numeric_first_other(
            RESB, wo_resb_cols, set(RESB.columns) - set(wo_resb_cols)
        )
        df_workorder = _merge_safe(
            df_workorder, RESB_agg, on=wo_resb_cols, how="left", suffixes=("", "_RESB")
        )
    wo_coep_cols = get_sap_join_columns(ucfg, "workorder", "wo_coep", ["AUFNR"], plant_code_id=plant_code_id)
    if all(c in COEP.columns for c in wo_coep_cols):
        COEP_agg = _sum_numeric_first_other(
            COEP, wo_coep_cols, set(COEP.columns) - set(wo_coep_cols)
        )
        df_workorder = _merge_safe(
            df_workorder, COEP_agg, on=wo_coep_cols, how="left", suffixes=("", "_COEP")
        )

    wo_equi_cols = get_sap_join_columns(ucfg, "workorder", "wo_equi", ["EQUNR"], plant_code_id=plant_code_id)
    if all(c in df_workorder.columns for c in wo_equi_cols) and all(
        c in EQUI.columns for c in wo_equi_cols
    ):
        df_workorder = _merge_safe(
            df_workorder, EQUI, on=wo_equi_cols, how="left", suffixes=("", "_EQUI"), validate="m:1"
        )
    wo_iflot_cols = get_sap_join_columns(ucfg, "workorder", "wo_iflot", ["TPLNR"], plant_code_id=plant_code_id)
    if all(c in df_workorder.columns for c in wo_iflot_cols) and all(
        c in IFLOT.columns for c in wo_iflot_cols
    ):
        df_workorder = _merge_safe(
            df_workorder, IFLOT, on=wo_iflot_cols, how="left", suffixes=("", "_IFLOT"), validate="m:1"
        )

    STXH_filtered = STXH
    if "TDOBJECT" in STXH.columns:
        STXH_filtered = STXH[STXH["TDOBJECT"] == "AUFK"].copy()

    # Guard against fan-out: STXH/STXL can carry more than one long-text line
    # per TDNAME. Text can't be summed like RESB/COEP's numbers, so dedupe to
    # one row per key BEFORE the join, rather than letting it fan df_workorder
    # out and relying on the later drop_duplicates(wo_dedup_key) to silently
    # (and arbitrarily) clean it up.
    if "TDNAME" in STXH_filtered.columns:
        _stxh_key = [c for c in ["TDNAME", "TDID"] if c in STXH_filtered.columns]
        STXH_filtered = STXH_filtered.drop_duplicates(subset=_stxh_key, keep="first")

    if "TDNAME" in STXL.columns:
        STXL = STXL.drop_duplicates(subset=["TDNAME"], keep="first")

    if "TDNAME" in STXH_filtered.columns and "AUFNR" in df_workorder.columns:
        cols = [c for c in ["TDNAME", "TDID"] if c in STXH_filtered.columns]
        if cols:
            df_workorder = pd.merge(
                df_workorder,
                STXH_filtered[cols],
                left_on="AUFNR",
                right_on="TDNAME",
                how="left",
            )
            df_workorder = df_workorder.loc[
                :, ~df_workorder.columns.duplicated()
            ].copy()

    if "TDNAME" in STXL.columns and "AUFNR" in df_workorder.columns:
        df_workorder = pd.merge(
            df_workorder,
            STXL,
            left_on="AUFNR",
            right_on="TDNAME",
            how="left",
            suffixes=("", "_STXL"),
        )
        df_workorder = df_workorder.loc[:, ~df_workorder.columns.duplicated()].copy()

    wo_dedup_key = [k for k in ["AUFNR", "VORNR"] if k in df_workorder.columns]
    if wo_dedup_key:
        before = len(df_workorder)
        df_workorder = df_workorder.drop_duplicates(subset=wo_dedup_key, keep="first")
        log.warning(
            f"  [sap/workorder] dedup on {wo_dedup_key}: {before} -> {len(df_workorder)} rows"
        )

    df_workorder = df_workorder.loc[:, ~df_workorder.columns.duplicated()].copy()

    ILOA = _load_table_safe(sap_in, "ILOA")
    ILOA = _ensure_str(ILOA, ["ILOAN"])

    df_floc = EQUI.copy()

    equi_iflot_cols = get_sap_join_columns(ucfg, "floc", "equi_iflot", ["TPLNR"], plant_code_id=plant_code_id)
    if all(c in df_floc.columns for c in equi_iflot_cols) and all(
        c in IFLOT.columns for c in equi_iflot_cols
    ):
        df_floc = _merge_safe(
            df_floc, IFLOT, on=equi_iflot_cols, how="left", suffixes=("", "_IFLOT"), validate="m:1"
        )

    equi_iloa_cols = get_sap_join_columns(ucfg, "floc", "equi_iloa", ["ILOAN"], plant_code_id=plant_code_id)
    if all(c in df_floc.columns for c in equi_iloa_cols) and all(
        c in ILOA.columns for c in equi_iloa_cols
    ):
        df_floc = _merge_safe(
            df_floc, ILOA, on=equi_iloa_cols, how="left", suffixes=("", "_ILOA"), validate="m:1"
        )

    floc_primary_cols = set(df_floc.columns)

    df_floc = df_floc.loc[:, ~df_floc.columns.duplicated()].copy()

    PLKO = _load_table_safe(sap_in, "PLKO")
    PLPO = _load_table_safe(sap_in, "PLPO")
    MAPL = _load_table_safe(sap_in, "MAPL")

    PLKO = _ensure_str(PLKO, ["PLNNR", "PLNAL"])
    PLPO = _ensure_str(PLPO, ["PLNNR", "PLNAL"])
    MAPL = _ensure_str(MAPL, ["PLNNR", "PLNAL", "EQUNR"])

    df_tasklist = PLKO.copy()

    plko_mapl_cols = get_sap_join_columns(
        ucfg, "tasklist", "plko_mapl", ["PLNNR", "PLNAL"], plant_code_id=plant_code_id
    )
    if all(c in MAPL.columns for c in plko_mapl_cols) and all(
        c in df_tasklist.columns for c in plko_mapl_cols
    ):
        df_tasklist = _merge_safe(df_tasklist, MAPL, on=plko_mapl_cols, how="left")

    plko_plpo_cols = get_sap_join_columns(ucfg, "tasklist", "plko_plpo", ["PLNNR"], plant_code_id=plant_code_id)
    if all(c in PLPO.columns for c in plko_plpo_cols) and all(
        c in df_tasklist.columns for c in plko_plpo_cols
    ):
        df_tasklist = _merge_safe(
            df_tasklist, PLPO, on=plko_plpo_cols, how="left", suffixes=("", "_PLPO")
        )

    tl_primary_cols = set(df_tasklist.columns)

    tl_equi_cols = get_sap_join_columns(ucfg, "tasklist", "tl_equi", ["EQUNR"], plant_code_id=plant_code_id)
    if all(c in df_tasklist.columns for c in tl_equi_cols) and all(
        c in EQUI.columns for c in tl_equi_cols
    ):
        df_tasklist = _merge_safe(
            df_tasklist, EQUI, on=tl_equi_cols, how="left", suffixes=("", "_EQUI"), validate="m:1"
        )


    df_tasklist = df_tasklist.loc[:, ~df_tasklist.columns.duplicated()].copy()

    dedup_key = [k for k in ["PLNNR", "PLNAL", "VORNR"] if k in df_tasklist.columns]
    if dedup_key:
        df_tasklist = df_tasklist.drop_duplicates(subset=dedup_key)

    MARA = _load_table_safe(sap_in, "MARA")
    MARC = _load_table_safe(sap_in, "MARC")
    MARD = _load_table_safe(sap_in, "MARD")

    MARA = _ensure_str(MARA, ["MATNR"])
    MARC = _ensure_str(MARC, ["MATNR", "WERKS"])
    MARD = _ensure_str(MARD, ["MATNR", "WERKS"])

    # No validate="m:1" here: MARC is keyed MATNR+WERKS (one row per material
    # per plant) but this join is on MATNR alone when the join-column config
    # doesn't include WERKS, so a multi-plant extract legitimately has more
    # than one MARC row per MATNR — "m:1" would raise MergeError on that valid
    # data. EQUI/IFLOT/ILOA below keep validate="m:1" since those joins are on
    # their actual unique key.
    df_material = _merge_safe(
        MARA,
        MARC,
        on=get_sap_join_columns(ucfg, "material", "mara_marc", ["MATNR"], plant_code_id=plant_code_id),
        how="left",
        suffixes=("", "_MARC"),
    )

    marc_mard_cols = get_sap_join_columns(
        ucfg, "material", "marc_mard", ["MATNR", "WERKS"], plant_code_id=plant_code_id
    )
    if all(c in df_material.columns for c in marc_mard_cols) and all(
        c in MARD.columns for c in marc_mard_cols
    ):
        df_material = _merge_safe(
            df_material, MARD, on=marc_mard_cols, how="left", suffixes=("", "_MARD")
        )
    else:
        # Same reasoning: MARD adds LGORT to its key, so falling back to a
        # MATNR-only join (no WERKS in scope) can validly hit multiple MARD
        # rows per MATNR too.
        df_material = _merge_safe(
            df_material,
            MARD,
            on=get_sap_join_columns(ucfg, "material", "mara_marc", ["MATNR"], plant_code_id=plant_code_id),
            how="left",
            suffixes=("", "_MARD"),
        )

    mat_primary_cols = set(df_material.columns)

    df_material = df_material.drop_duplicates()

    # RESB and COEP fan df_material out (many reservations/cost lines per
    # material). Both are joined RAW here, on purpose: RESB brings AUFNR into
    # df_material, which the COEP join right after needs to attach the correct
    # order's cost lines. Dedup happens AFTER both joins below, not before —
    # deduping RESB by MATNR first would pick one arbitrary AUFNR per material
    # and COEP would then only ever see that one order.
    _mat_pre_resb_cols = set(df_material.columns)
    mat_resb_cols = get_sap_join_columns(ucfg, "material", "mat_resb", ["MATNR"], plant_code_id=plant_code_id)
    if all(c in RESB.columns for c in mat_resb_cols) and all(
        c in df_material.columns for c in mat_resb_cols
    ):
        df_material = _merge_safe(
            df_material, RESB, on=mat_resb_cols, how="left", suffixes=("", "_RESB")
        )
    _mat_resb_added_cols = set(df_material.columns) - _mat_pre_resb_cols

    _mat_pre_coep_cols = set(df_material.columns)
    mat_coep_cols = get_sap_join_columns(ucfg, "material", "mat_coep", ["AUFNR"], plant_code_id=plant_code_id)
    if all(c in df_material.columns for c in mat_coep_cols) and all(
        c in COEP.columns for c in mat_coep_cols
    ):
        df_material = _merge_safe(
            df_material, COEP, on=mat_coep_cols, how="left", suffixes=("", "_COEP")
        )
    _mat_coep_added_cols = set(df_material.columns) - _mat_pre_coep_cols

    df_material = df_material.loc[:, ~df_material.columns.duplicated()].copy()

    # Collapse the RESB/COEP fan-out back to one row per material. Numeric
    # columns RESB/COEP contributed (quantities, cost lines) are summed so the
    # totals stay correct instead of being thrown away by drop_duplicates;
    # everything else keeps its first value. Note MARC/MARD no longer carry a
    # validate="m:1" guarantee (a MATNR-only join can legitimately hit more
    # than one MARC/MARD row on a multi-plant extract — see the mara_marc/
    # marc_mard joins above), so "first" there just picks one plant's values
    # arbitrarily rather than summing/losing data outright.
    mat_dedup_key = [k for k in ["MATNR", "WERKS"] if k in df_material.columns]
    if mat_dedup_key:
        before = len(df_material)
        df_material = _sum_numeric_first_other(
            df_material, mat_dedup_key, _mat_resb_added_cols | _mat_coep_added_cols
        )
        log.warning(
            f"  [sap/material] RESB/COEP fan-out collapsed on {mat_dedup_key}: "
            f"{before} -> {len(df_material)} rows (numeric columns summed, not dropped)"
        )

    # Derive a single equipment_ref per material via the reservation chain:
    # RESB.MATNR → RESB.AUFNR → [AFIH preferred, AUFK fallback] → EQUNR
    # AFIH is the standard SAP PM source for AUFNR→EQUNR; AUFK carries EQUNR
    # only in non-standard configurations.
    # Tie-breaking: most-frequent EQUNR per MATNR; ties resolved by first occurrence.
    _mat_equip_ref: dict[str, str] = {}
    if "AUFNR" in RESB.columns and "MATNR" in RESB.columns:
        # Build a single AUFNR→EQUNR lookup: AFIH first, fall back to AUFK
        _order_equip: pd.DataFrame = pd.DataFrame()
        if "AUFNR" in AFIH.columns and "EQUNR" in AFIH.columns:
            _order_equip = AFIH[["AUFNR", "EQUNR"]].copy()
        elif "AUFNR" in AUFK.columns and "EQUNR" in AUFK.columns:
            _order_equip = AUFK[["AUFNR", "EQUNR"]].copy()

        if not _order_equip.empty:
            _resb_wo = RESB[["MATNR", "AUFNR"]].drop_duplicates()
            _resb_equip = _merge_safe(
                _resb_wo,
                _order_equip,
                on=["AUFNR"],
                how="inner",
            )
            _resb_equip = _resb_equip[
                _resb_equip["EQUNR"].astype(str).str.strip().ne("")
            ]
            if not _resb_equip.empty:
                _mat_equip_ref = (
                    _resb_equip.groupby("MATNR")["EQUNR"]
                    .apply(lambda s: s.value_counts().idxmax())
                    .to_dict()
                )
    if "MATNR" in df_material.columns:
        df_material["equipment_ref"] = (
            df_material["MATNR"].map(_mat_equip_ref).fillna("")
        )
    else:
        df_material["equipment_ref"] = ""

    datasets = {
        "workorder": df_workorder,
        "floc": df_floc,
        "tasklist": df_tasklist,
        "material": df_material,
    }

    primary_columns = {
        "workorder": wo_primary_cols,
        "floc": floc_primary_cols,
        "tasklist": tl_primary_cols,
        "material": mat_primary_cols,
    }

    return datasets, primary_columns
