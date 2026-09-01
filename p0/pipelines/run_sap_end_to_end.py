from __future__ import annotations

import argparse
import logging
import os
import re as _re
from pathlib import Path

import pandas as pd

from p0.utils.schema_apply import _resolve_tables
import yaml


def _setup_pipeline_logger(log_dir: str) -> logging.Logger:
    """Write DEBUG+ to sap_pipeline.log and INFO+ to stdout."""
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "sap_pipeline.log")

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    fh = logging.FileHandler(log_file, mode="a", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    log = logging.getLogger("sap_pipeline")
    log.setLevel(logging.DEBUG)
    if not log.handlers:
        log.addHandler(fh)
        log.addHandler(ch)

    log.info("=" * 72)
    log.info("SAP PIPELINE — log file: %s", log_file)
    log.info("=" * 72)
    return log


log = logging.getLogger("sap_pipeline")

from p0.source_processing.sap.sap_processing import (
    run_sap_source_processing,
)
from p0.utils.renamer import apply_column_rename
from p0.utils.derived import apply_derived_fields
from p0.utils.identity import apply_identity_rules
from p0.utils.validate import run_validations, report_to_json, log_validation_report
from p0.utils.schema_apply import apply_schema_keep_extra
from p0.utils.asset_identity import assign_equipment_uid, parse_floc

from p0.pipelines.canonical_builder_sap import (
    build_canonical_entities_from_sap_template,
    build_asset_cross_reference,
)
from p0.utils.canonical_entities import (
    build_uid_tables_from_canonical_entities,
    merge_entity_file_on_disk,
)
from p0.utils.canonical_relationships import (
    build_asset_relationships,
)
from p0.utils.config_resolver import resolve_config_path
from p0.utils.progress import emit as _emit_progress, emit_file as _emit_file

_SAP_STAGED_EXT = {".xlsx", ".xls", ".csv", ".xml", ".parquet"}


class SapColumnMappingRequired(RuntimeError):
    """Raised when a staged SAP file needs a user-defined column mapping before the
    pipeline can run (Stage 8 of the SAP pipeline flow: non-standard columns must be
    mapped via PUT /context/saveSapJoins before processing continues)."""


def _check_sap_column_mapping_gate(
    sap_in: str, user_cfg: dict, plant_code_id: str
) -> None:
    """Gate the pipeline on column detection before any processing begins.

    Re-runs the same detection used at upload time (p0.utils.sap_table_detect)
    against the files actually sitting in staging, rather than trusting stale
    upload-time metadata:
    - matched_by == "filename"        -> proceed, no check needed.
    - matched_by == "column_content"  -> proceed only if join_overrides has
        an entry for a dataset this detected_table actually feeds (per
        sap_mandatory_tables.yaml's pipeline groupings) — not merely if the
        plant has *some* override somewhere. A table with no known dataset
        mapping can't be verified, so it's treated as unmapped.
    - matched_by == "unrecognized"    -> always stop, unconditionally. There is
        no dataset to check overrides against for a table detection couldn't
        even identify, so an override elsewhere on the plant can't vouch for
        this file — the user must rename it to its SAP table name so it can
        be detected by filename instead.
    """
    from p0.utils import fs as _fs
    from p0.utils.sap_table_detect import (
        detect_sap_table_at_path,
        load_sap_table_to_datasets,
    )

    paths = [
        p
        for p in _fs.glob_files(_fs.path_join(sap_in, "*"))
        if not _fs.basename(p).startswith("_")
        and Path(p).suffix.lower() in _SAP_STAGED_EXT
    ]

    plant_join_overrides = (
        (user_cfg.get("sap_processing", {}) or {})
        .get("join_overrides", {})
        .get(plant_code_id, {})
    )
    table_to_datasets = load_sap_table_to_datasets()

    for path in paths:
        filename = _fs.basename(path)
        result = detect_sap_table_at_path(path)
        matched_by = result.get("matched_by")
        if matched_by == "filename":
            continue
        if matched_by == "column_content":
            detected_table = (result.get("detected_table") or "").upper()
            covering_datasets = table_to_datasets.get(detected_table, frozenset())
            if covering_datasets and any(
                plant_join_overrides.get(dataset) for dataset in covering_datasets
            ):
                continue

        # Gap 3 fix: if plant has column rename overrides, skip the gate
        plant_rename_overrides = (
            (user_cfg.get("sap_processing", {}) or {})
            .get("column_rename_overrides", {})
            .get(plant_code_id, {})
        )
        if plant_rename_overrides:
            continue

        raise SapColumnMappingRequired(
            f"File {filename} has unrecognized columns and could not be matched to a "
            "known SAP table. Rename it to match its SAP table name (e.g. AUFK.xlsx, "
            "EQUI.csv, COEP.parquet) and re-upload so it can be detected automatically."
        )


def load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_config(config_dir: str, filename: str) -> dict:
    return load_yaml(resolve_config_path(config_dir, filename))


def _extract_physical_tag_from_floc(floc: str, plant_code_id: str) -> str:
    """Extract a physical equipment tag (B32-K-0001 style) from a SAP functional location."""
    if not floc:
        return ""
    parsed = parse_floc(floc)
    if parsed and len(parsed.levels) > 1:
        return parsed.item or floc
    parts = floc.split("-")
    if len(parts) < 2:
        return floc
    plant = parts[0]
    last = parts[-1]
    if not last:
        return floc
    m = _re.match(r"^([A-Za-z]+)(\d+)([A-Za-z]?)$", last)
    if m:
        letters = m.group(1).upper()
        digits = m.group(2).zfill(4)
        suffix = m.group(3).upper()
        return f"{plant}-{letters}-{digits}{suffix}"
    return floc


def _normalize_sap_equipment_id(
    df: pd.DataFrame, plant_code_id: str, user_cfg: dict | None = None
) -> pd.DataFrame:
    """Normalize the equipment_id column in a SAP dataframe to physical tag format."""
    if "equipment_id" not in df.columns:
        return df

    from p0.utils.user_config import get_sap_equipment_id_column

    floc_candidates = get_sap_equipment_id_column(
        user_cfg or {},
        "floc",
        default=["functional_location", "floc", "TPLNR"],
        plant_code_id=plant_code_id,
    )

    floc_col: str | None = None
    for c in floc_candidates:
        if c in df.columns:
            floc_col = c
            break

    def _resolve(row: pd.Series) -> str:
        eq = str(row.get("equipment_id", "")).strip()
        floc = str(row.get(floc_col, "")).strip() if floc_col else ""

        if not eq or eq.lower() in ("nan", "none", ""):
            return eq

        if eq.replace(" ", "").isdigit():
            resolved = _extract_physical_tag_from_floc(floc, plant_code_id)
            return resolved if resolved else eq

        if eq.count("-") >= 3:
            resolved = _extract_physical_tag_from_floc(eq, plant_code_id)
            return resolved if resolved else eq

        return eq

    df = df.copy()
    original = df["equipment_id"].copy()
    df["equipment_id"] = df.apply(_resolve, axis=1)

    changed = (df["equipment_id"] != original).sum()
    if changed > 0:
        print(
            f"  [sap/equip_id] Normalized {changed} equipment_id values to physical tag format"
        )
        sample_before = original[df["equipment_id"] != original].unique()[:5].tolist()
        sample_after = (
            df.loc[df["equipment_id"] != original, "equipment_id"].unique()[:5].tolist()
        )
        print(f"  [sap/equip_id] e.g. before={sample_before} -> after={sample_after}")

    return df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config_dir", required=True)
    ap.add_argument("--sap_in", required=True)
    ap.add_argument("--work_dir", required=True)
    ap.add_argument("--out_dir", required=True)
    ap.add_argument(
        "--plant_code_id",
        required=False,
        default=os.environ.get("PLANT_CODE", "UNKNOWN"),
    )
    ap.add_argument(
        "--pid_out",
        required=False,
        default="",
        help="P&ID pipeline output dir for cross-reference building (e.g. data/out/pnid)",
    )
    args = ap.parse_args()

    from p0.utils import fs as _fs

    os.makedirs(args.work_dir, exist_ok=True)
    if not _fs.is_s3(args.out_dir):
        os.makedirs(args.out_dir, exist_ok=True)
        os.makedirs(os.path.join(args.out_dir, "entities"), exist_ok=True)
        os.makedirs(os.path.join(args.out_dir, "relationships"), exist_ok=True)
        os.makedirs(os.path.join(args.out_dir, "validation_reports"), exist_ok=True)

    _data_dir = (
        os.environ.get("P0_DATA_DIR")
        or os.environ.get("p0_DATA_DIR")
        or str(Path(__file__).resolve().parents[1] / "data")
    )
    _log_dir = os.environ.get("P0_LOG_DIR") or str(Path(_data_dir) / "logs")
    global log
    log = _setup_pipeline_logger(_log_dir)
    log.info(
        "━━━ SAP PIPELINE START ━━━  plant=%s  sap_in=%s",
        args.plant_code_id,
        args.sap_in,
    )

    rename_cfg = load_config(args.config_dir, "sap_column_rename.yaml")
    derived_cfg = load_config(args.config_dir, "derived_fields.yaml")
    identity_cfg = load_config(args.config_dir, "identity_frozen.yaml")
    relationships_cfg = load_config(args.config_dir, "relationships_frozen.yaml")
    entities_cfg = load_config(args.config_dir, "entities_frozen.yaml")
    schema_cfg = load_config(args.config_dir, "schema_frozen.yaml")
    validation_cfg = load_config(args.config_dir, "validation_contracts.yaml")

    from p0.utils.user_config import load_user_config, get_sap_column_renames

    user_cfg = load_user_config(args.config_dir, plant_code_id=args.plant_code_id)

    log.info("━━━ STEP: COLUMN MAPPING GATE ━━━")
    _check_sap_column_mapping_gate(args.sap_in, user_cfg, args.plant_code_id)

    merged, primary_columns_raw = run_sap_source_processing(
        sap_in=args.sap_in,
        work_dir=args.work_dir,
        user_cfg=user_cfg,
        plant_code_id=args.plant_code_id,
    )

    from p0.utils.io import load_table as _load_table

    _SAP_REVIEW_TABLES = [
        "AUFK",
        "AFKO",
        "AFVC",
        "AFVV",
        "QMEL",
        "QMSM",
        "JEST",
        "JSTO",
        "RESB",
        "COEP",
        "EQUI",
        "IFLOT",
        "ILOA",
        "STXH",
        "STXL",
        "PLKO",
        "PLPO",
        "MAPL",
        "MARA",
        "MARC",
        "MARD",
    ]
    _source_dir = _fs.path_join(args.out_dir, "source")
    if not _fs.is_s3(_source_dir):
        os.makedirs(_source_dir, exist_ok=True)
    _src_written = 0
    _emit_progress(
        "transform",
        label="Transforming to canonical",
        status="running",
        items_done=0,
        items_total=len(_SAP_REVIEW_TABLES),
    )
    for _tbl in _SAP_REVIEW_TABLES:
        try:
            _df = _load_table(args.sap_in, _tbl)
        except FileNotFoundError:
            _emit_file(str(_tbl), "skipped")
            continue
        if _df is None or _df.empty:
            _emit_file(str(_tbl), "skipped")
            continue
        try:
            _fs.write_parquet(_df, _fs.path_join(_source_dir, f"{_tbl}.parquet"))
            _src_written += 1
            _emit_file(str(_tbl), "processed", rows=int(len(_df)))
        except Exception as _exc:
            log.warning("[sap/source] failed to snapshot %s: %s", _tbl, _exc)
            _emit_file(str(_tbl), "failed", error=str(_exc))
    log.info(
        "[sap/source] snapshotted %d raw SAP table(s) for review under %s",
        _src_written,
        _source_dir,
    )

    debug_dir = os.path.join(args.work_dir, "debug")
    os.makedirs(debug_dir, exist_ok=True)
    if "workorder" in merged:
        merged["workorder"].to_csv(
            os.path.join(debug_dir, "sap_before.csv"), index=False
        )
        log.info("[sap/debug] sap_before.csv: %d rows", len(merged["workorder"]))

    DATASET_TO_SOURCE = {
        "workorder": "sap_wo",
        "floc": "sap_assets",
        "tasklist": "sap_task_list",
        "material": "sap_material_master",
        "notification": "sap_notification",
        "bom": "sap_bom",
    }

    custom_table_keys = set(
        user_cfg.get("sap_processing", {}).get("custom_tables", {}).keys()
    )

    post = {}
    post_sources = {}
    primary_columns = {}

    for ds_name, df in merged.items():
        if ds_name in custom_table_keys:
            custom_entry = user_cfg["sap_processing"]["custom_tables"][ds_name]
            source_name = custom_entry.get("source_key", ds_name)

            user_renames = get_sap_column_renames(
                user_cfg, source_name, plant_code_id=args.plant_code_id
            )
            template_renames = (
                rename_cfg.get("sources", {}).get(source_name, {}).get("mappings", {})
            )
            all_renames = {**template_renames, **user_renames}

            if all_renames:
                df = df.rename(columns=all_renames)
                print(
                    f"  [sap/custom] Renamed {len(all_renames)} columns for {ds_name}"
                )

            df["plant_code_id"] = args.plant_code_id
            df["source_system"] = "SAP"

            out_name = custom_entry.get("label", ds_name)
            out_path = _fs.path_join(args.out_dir, "entities", f"{out_name}.parquet")
            _fs.write_parquet(df, out_path)
            print(
                f"  [sap/custom] Output: {out_name}.parquet ({len(df)} rows, {len(df.columns)} cols)"
            )
            continue

        source_name = DATASET_TO_SOURCE.get(ds_name, ds_name)

        raw_primary = primary_columns_raw.get(ds_name, set(df.columns))

        # User overrides reference raw column names — apply BEFORE template so they win.
        # If applied after, the template already renames e.g. EQUNR→equipment_id and the
        # user override {"EQUNR": "custom_field"} becomes a silent no-op.
        _user_renames = get_sap_column_renames(
            user_cfg, source_name, plant_code_id=args.plant_code_id
        )
        if _user_renames:
            df = df.rename(columns=_user_renames)

        df2 = apply_column_rename(df, rename_cfg, source_name=source_name)

        # If user renamed EQUNR to a custom name, the template's EQUNR→equipment_id
        # mapping was skipped (EQUNR already gone). Create equipment_id as an alias so
        # downstream code (identity rules, canonical builder, equipment_sap post-
        # processing) can still find the value under the expected internal name.
        if _user_renames and "EQUNR" in _user_renames:
            _user_equnr_col = _user_renames["EQUNR"]
            if "equipment_id" not in df2.columns and _user_equnr_col in df2.columns:
                df2["equipment_id"] = df2[_user_equnr_col]

        primary_dummy = pd.DataFrame(columns=sorted(raw_primary))
        if _user_renames:
            primary_dummy = primary_dummy.rename(columns=_user_renames)
        primary_renamed = apply_column_rename(
            primary_dummy, rename_cfg, source_name=source_name, verbose=False
        )
        renamed_primary = set(primary_renamed.columns)
        df2 = _normalize_sap_equipment_id(
            df2, plant_code_id=args.plant_code_id, user_cfg=user_cfg
        )
        df2 = apply_derived_fields(
            df2,
            derived_cfg,
            dataset_name=ds_name,
            source_name=source_name,
            plant_code_id=args.plant_code_id,
        )

        if "plant_code_id" in df2.columns:
            df2["plant_code_id"] = args.plant_code_id
        else:
            df2["plant_code_id"] = args.plant_code_id

        df2 = apply_identity_rules(df2, identity_cfg, dataset_name=ds_name)

        pipeline_added = {
            "plant_code_id",
            "ingested_at",
            "updated_at",
            "source_system",
            "source_record_id",
            "normalized_asset",
            "floc",
        }
        primary_columns[source_name] = renamed_primary | pipeline_added

        post[ds_name] = df2
        post_sources[source_name] = df2

        df2.to_csv(os.path.join(args.work_dir, f"{ds_name}_post.csv"), index=False)

    for ds_name, df in post.items():
        _fs.write_parquet(
            df, _fs.path_join(args.out_dir, "entities", f"sap_{ds_name}.parquet")
        )

    canonical_entities = build_canonical_entities_from_sap_template(
        post_sources=post_sources,
        entities_cfg=entities_cfg,
        schema_cfg=schema_cfg,
        plant_code_id=args.plant_code_id,
        primary_columns=primary_columns,
    )

    def _pk(tname: str) -> str | None:
        tdef = _resolve_tables(schema_cfg or {}).get(tname, {})
        pk = tdef.get("primary_key")
        return pk if isinstance(pk, str) and pk else None

    log.info("━━━ STEP: CANONICAL ENTITIES ━━━")
    for table_name, df_ent in canonical_entities.items():
        df_schema = apply_schema_keep_extra(df_ent, schema_cfg, table_name)
        if "site" in df_schema.columns:
            df_schema["site"] = (
                df_schema["site"].replace("", None).fillna(args.plant_code_id)
            )

        if table_name == "equipment_sap":
            df_schema = assign_equipment_uid(
                df_schema,
                plant_code_id=args.plant_code_id,
                uid_col="sap_uid",
                asset_col="sap_equipment_id"
                if "sap_equipment_id" in df_schema.columns
                else "equipment_id",
            )
        elif table_name == "equipment":
            df_schema = assign_equipment_uid(
                df_schema,
                plant_code_id=args.plant_code_id,
                uid_col="equipment_uid",
                asset_col="normalized_asset",
            )

        out_path = _fs.path_join(args.out_dir, "entities", f"{table_name}.parquet")
        df_schema = merge_entity_file_on_disk(df_schema, out_path, _pk(table_name))
        _fs.write_parquet(df_schema, out_path)
        canonical_entities[table_name] = df_schema

    uid_tables = build_uid_tables_from_canonical_entities(
        canonical_entities=canonical_entities,
        entities_cfg=entities_cfg,
        schema_cfg=schema_cfg,
    )
    for entity_name, df_uid in uid_tables.items():
        _fs.write_parquet(
            df_uid,
            _fs.path_join(args.out_dir, "entities", f"{entity_name}_uid.parquet"),
        )

    sap_assets_df = post_sources.get("sap_assets", pd.DataFrame())
    if not sap_assets_df.empty:
        eq_sap = sap_assets_df.copy()
        if (
            "sap_equipment_id" not in eq_sap.columns
            and "equipment_id" in eq_sap.columns
        ):
            eq_sap["sap_equipment_id"] = eq_sap["equipment_id"]
        eq_sap["plant_code_id"] = args.plant_code_id
        eq_sap["source_system"] = "SAP"
        if (
            "normalized_asset" not in eq_sap.columns
            and "sap_equipment_id" in eq_sap.columns
        ):
            eq_sap["normalized_asset"] = eq_sap["sap_equipment_id"]

        if "sap_equipment_id" in eq_sap.columns:
            eq_sap = assign_equipment_uid(
                eq_sap,
                plant_code_id=args.plant_code_id,
                uid_col="sap_uid",
                asset_col="sap_equipment_id",
            )

        eq_sap = apply_schema_keep_extra(eq_sap, schema_cfg, "equipment_sap")

        if "sap_equipment_id" in eq_sap.columns:
            eq_sap = assign_equipment_uid(
                eq_sap,
                plant_code_id=args.plant_code_id,
                uid_col="sap_uid",
                asset_col="sap_equipment_id",
            )
            filled = (eq_sap["sap_uid"].astype(str).str.strip() != "").sum()
            print(f"  [sap] equipment_sap.sap_uid filled: {filled}/{len(eq_sap)} rows")

        if "site" not in eq_sap.columns:
            eq_sap["site"] = ""
        eq_sap["site"] = (
            eq_sap["site"].astype(str).str.strip().replace("", args.plant_code_id)
        )
        eq_sap["site"] = eq_sap["site"].fillna(args.plant_code_id)

        if "normalized_asset" in eq_sap.columns and "equipment_id" in eq_sap.columns:
            blank_asset = eq_sap["normalized_asset"].astype(str).str.strip() == ""
            eq_sap.loc[blank_asset, "normalized_asset"] = eq_sap.loc[
                blank_asset, "equipment_id"
            ]

        eq_sap_path = _fs.path_join(args.out_dir, "entities", "equipment_sap.parquet")
        _fs.write_parquet(eq_sap, eq_sap_path)
        canonical_entities["equipment_sap"] = eq_sap
        log.info("[sap] equipment_sap: %d rows", len(eq_sap))

    pid_out = args.pid_out or os.environ.get("PID_OUT", "")
    pid_equip_base = _fs.path_join(pid_out, "entities", "equipment") if pid_out else ""
    pid_equip_path = ""
    if pid_out and pid_equip_base:
        for ext in (".parquet", ".csv"):
            candidate = pid_equip_base + ext
            if _fs.exists(candidate):
                pid_equip_path = candidate
                break
    if pid_equip_path and pid_equip_path.endswith(".parquet"):
        pid_equip_df = _fs.read_parquet(pid_equip_path).astype(str).fillna("")
    elif pid_equip_path:
        pid_equip_df = _fs.read_csv(pid_equip_path, dtype=str, keep_default_na=False)
    else:
        pid_equip_df = pd.DataFrame()

    if pid_equip_df.empty:
        try:
            from p0.driver import PostgresDriver
            from utility.middleware import plant_code_ctx

            token = plant_code_ctx.set(args.plant_code_id)
            try:
                driver = PostgresDriver()
                conn = driver.connect()
            finally:
                plant_code_ctx.reset(token)
            try:
                xref_df = pd.read_sql(
                    "SELECT * FROM asset_cross_reference WHERE plant_code_id = %(plant_code_id)s AND pid_asset_tag != ''",
                    conn,
                    params={"plant_code_id": args.plant_code_id},
                )
            finally:
                conn.close()
            log.info(
                "[sap] loaded asset_cross_reference from Postgres  plant=%s  rows=%d",
                args.plant_code_id, len(xref_df),
            )
        except Exception as exc:
            log.warning(
                "[sap] asset_cross_reference not available from Postgres for plant=%s: %s",
                args.plant_code_id, exc,
            )
            xref_df = pd.DataFrame()
    else:
        xref_df = build_asset_cross_reference(
            sap_equipment_df=sap_assets_df,
            pid_equipment_df=pid_equip_df,
            plant_code_id=args.plant_code_id,
        )
    xref_df = apply_schema_keep_extra(xref_df, schema_cfg, "asset_cross_reference")
    _fs.write_parquet(
        xref_df,
        _fs.path_join(args.out_dir, "entities", "asset_cross_reference.parquet"),
    )
    print(f"  [sap] asset_cross_reference: {len(xref_df)} P&ID<->SAP matches")

    if (
        not xref_df.empty
        and "sap_equipment_id" in xref_df.columns
        and "pid_tag" in xref_df.columns
    ):
        xref_lookup: dict[str, str] = dict(
            zip(
                xref_df["sap_equipment_id"].astype(str).str.strip().str.upper(),
                xref_df["pid_tag"].astype(str).str.strip(),
            )
        )
        RESOLVE_TABLES = {"work_order", "task_list", "functional_location", "material"}
        resolved_count = 0
        for tname in RESOLVE_TABLES:
            df_ent = canonical_entities.get(tname)
            if df_ent is None or df_ent.empty:
                continue
            for col in ("equipment_ref", "equipment_id"):
                if col not in df_ent.columns:
                    continue
                before = df_ent[col].copy()
                df_ent[col] = df_ent[col].apply(
                    lambda v: xref_lookup.get(str(v).strip().upper(), v)
                )
                resolved_count += (df_ent[col] != before).sum()
            canonical_entities[tname] = df_ent
            out_path = _fs.path_join(args.out_dir, "entities", f"{tname}.parquet")
            if _fs.exists(out_path):
                _fs.write_parquet(df_ent, out_path)
        print(
            f"  [sap] equipment_ref back-resolved: {resolved_count} cells updated to P&ID tags"
        )

        eq_sap_ent = canonical_entities.get("equipment_sap")
        if (
            eq_sap_ent is not None
            and not eq_sap_ent.empty
            and "sap_equipment_id" in eq_sap_ent.columns
            and "equipment_id" in eq_sap_ent.columns
        ):
            eq_sap_ent = eq_sap_ent.copy()
            eq_sap_ent["equipment_id"] = eq_sap_ent["sap_equipment_id"].apply(
                lambda v: xref_lookup.get(str(v).strip().upper(), "")
            )

            if "site" not in eq_sap_ent.columns:
                eq_sap_ent["site"] = ""
            eq_sap_ent["site"] = (
                eq_sap_ent["site"].astype(str).str.strip().replace("", args.plant_code_id)
            )
            eq_sap_ent["site"] = eq_sap_ent["site"].fillna(args.plant_code_id)

            if (
                "normalized_asset" in eq_sap_ent.columns
                and "equipment_id" in eq_sap_ent.columns
            ):
                blank_asset = (
                    eq_sap_ent["normalized_asset"].astype(str).str.strip() == ""
                )
                eq_sap_ent.loc[blank_asset, "normalized_asset"] = eq_sap_ent.loc[
                    blank_asset, "equipment_id"
                ]

            canonical_entities["equipment_sap"] = eq_sap_ent
            _fs.write_parquet(
                eq_sap_ent,
                _fs.path_join(args.out_dir, "entities", "equipment_sap.parquet"),
            )
            print(
                f"  [sap] equipment_sap.equipment_id resolved: "
                f"{(eq_sap_ent['equipment_id'] != '').sum()} matches"
            )
    else:
        print(
            "  [sap] equipment_ref back-resolution: skipped (no cross-reference matches)"
        )

    if "workorder" in post:
        after_df = post["workorder"]
        after_df.to_csv(os.path.join(debug_dir, "sap_after.csv"), index=False)
        print(f"  [sap/debug] sap_after.csv: {len(after_df)} rows")

        canon_wo = canonical_entities.get("work_order", pd.DataFrame())
        try:
            before_df = (
                pd.read_csv(
                    os.path.join(debug_dir, "sap_before.csv"),
                    dtype=str,
                    keep_default_na=False,
                )
                if os.path.exists(os.path.join(debug_dir, "sap_before.csv"))
                else pd.DataFrame()
            )
        except Exception:
            before_df = pd.DataFrame()

        if not before_df.empty and "AUFNR" in before_df.columns:
            wo_nums_before = set(before_df["AUFNR"].astype(str).str.strip())
            wo_nums_after = set()
            if not canon_wo.empty:
                for col in ("wo_number", "source_record_id"):
                    if col in canon_wo.columns:
                        wo_nums_after = set(canon_wo[col].astype(str).str.strip())
                        break
            dropped_ids = wo_nums_before - wo_nums_after
            dropped_df = before_df[
                before_df["AUFNR"].astype(str).str.strip().isin(dropped_ids)
            ]
            dropped_df.to_csv(os.path.join(debug_dir, "dropped_rows.csv"), index=False)
            print(f"  [sap/debug] dropped_rows.csv: {len(dropped_df)} rows")

    asset_rels = build_asset_relationships(
        post_sources=post_sources,
        relationships_cfg=relationships_cfg,
        entities_cfg=entities_cfg,
        schema_cfg=schema_cfg,
        entity_uid_tables=uid_tables,
    )
    rels_path = _fs.path_join(args.out_dir, "relationships", "sap_relationship.parquet")
    _fs.write_parquet(asset_rels, rels_path)

    report = run_validations(post, validation_cfg)
    log_validation_report(report, log)
    report_path = _fs.path_join(
        args.out_dir, "validation_reports", "sap_validation_report.json"
    )
    _report_json = report_to_json(report)
    if _fs.is_s3(report_path):
        with _fs.get_fs().open(report_path, "w") as f:
            f.write(_report_json)
    else:
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(_report_json)

    log.info("━━━ SAP PIPELINE COMPLETE ━━━")
    log.info(
        "[output] plant=%s  status=%s  relationships rows=%d",
        args.plant_code_id,
        report.get("status"),
        len(asset_rels),
    )
    log.info("[output] entities dir: %s", os.path.join(args.out_dir, "entities"))
    for tname, df_ent in canonical_entities.items():
        log.info("[output] %s rows: %d", tname, len(df_ent))
    print("[done] SAP end-to-end completed.")
    print(f"Plant   : {args.plant_code_id}")
    print(f"Input   : {args.sap_in}")
    print(f"Entities: {os.path.join(args.out_dir, 'entities')}")
    print(f"Rels    : {os.path.join(args.out_dir, 'relationships')}")
    _status = report.get("status")
    if _status == "FAIL" and canonical_entities:
        _status = "WARN"
    print(f"Status  : {_status}")
    print(f"Relationships rows: {len(asset_rels)}")


if __name__ == "__main__":
    main()
