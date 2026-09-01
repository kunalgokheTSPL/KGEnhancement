from __future__ import annotations

import argparse
import glob
import logging
import os
import subprocess
import sys
import time
from pathlib import Path
from p0.utils.fs import glob_files, read_csv, read_excel
from p0.utils.progress import emit as _emit_progress, emit_file as _emit_file
from p0.utils import fs
from typing import List

import pandas as pd
import yaml

from p0.utils.renamer import apply_column_rename
from p0.utils.derived import apply_derived_fields
from p0.utils.identity import apply_identity_rules
from p0.utils.schema_apply import apply_schema, apply_schema_keep_extra
from p0.utils.validate import run_validations, report_to_json, log_validation_report
from p0.utils.asset_identity import (
    assign_equipment_uid,
    fix_connectivity_refs,
)

from p0.utils.canonical_entities import (
    build_uid_tables_from_canonical_entities,
    merge_entity_file_on_disk,
)
from p0.utils.canonical_relationships import (
    build_asset_relationships,
)
from p0.utils.config_resolver import resolve_config_path


def _setup_pipeline_logger(log_dir: str) -> logging.Logger:
    """Configure a logger that writes DEBUG+ to a rotating log file and INFO+ to stdout."""
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "pnid_pipeline.log")

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

    log = logging.getLogger("pnid_pipeline")
    log.setLevel(logging.DEBUG)
    if not log.handlers:
        log.addHandler(fh)
        log.addHandler(ch)

    log.info("=" * 72)
    log.info("P&ID PIPELINE — log file: %s", log_file)
    log.info("=" * 72)
    return log


log = logging.getLogger("pnid_pipeline")


def load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_config(config_dir: str, filename: str) -> dict:
    return load_yaml(resolve_config_path(config_dir, filename))


def norm_upper(s: pd.Series) -> pd.Series:
    x = s.astype(str).str.strip().str.upper()
    x = x.str.replace(r"\s+", " ", regex=True)
    x = x.replace({"NAN": "", "NONE": "", "NAT": ""})
    return x


def first_non_empty(df: pd.DataFrame, cols: List[str]) -> pd.Series:
    out = pd.Series([""] * len(df), index=df.index)
    for c in cols:
        if c in df.columns:
            v = df[c].astype(str).str.strip()
            mask = (out == "") & (v != "") & (v.str.lower() != "nan")
            out = out.mask(mask, v)
    return out


def _write_merged(df, path: str, pk_col: str):
    """Write a canonical file, keeping the rows earlier drawings left behind."""
    merged = merge_entity_file_on_disk(df, path, pk_col)
    fs.write_parquet(merged, path)
    return merged


def build_entity_from_entities_yaml(
    post_source_df: pd.DataFrame,
    *,
    entities_cfg: dict,
    entity_name: str,
    schema_cfg: dict,
    source_prefix: str,
) -> pd.DataFrame:
    """Minimal template-driven canonical entity builder using entities_frozen.yaml."""
    ent = (entities_cfg.get("entities", {}) or {}).get(entity_name, {}) or {}
    attrs = ent.get("attributes", {}) or {}

    out = pd.DataFrame(index=post_source_df.index)

    for attr, spec in attrs.items():
        from_list = (spec or {}).get("from", []) or []
        cols = []
        for ref in from_list:
            if isinstance(ref, str) and ref.startswith(source_prefix + "."):
                cols.append(ref.split(".", 1)[1])
        out[attr] = first_non_empty(post_source_df, cols) if cols else ""

    table = ent.get("canonical_table", entity_name) or entity_name
    out = apply_schema(out, schema_cfg, table_name=table)
    return out


def _norm_fname(name: str) -> str:
    """Normalise a filename for tolerant matching."""
    import re as _re

    base = os.path.basename(str(name or "")).strip().lower()
    stem, ext = os.path.splitext(base)
    stem = _re.sub(r"[\s_\-]+", "_", stem).strip("_")
    ext = ext.strip()
    return f"{stem}{ext}"


def load_external_pnid_xlsx(
    staging_dir: str,
    pnid_files_filter: str = "",
    equipment_sheet: str = "equipment_raw",
    connectivity_sheet: str = "connectivity_raw",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Read P&ID extraction results that an EXTERNAL extractor already wrote as"""
    wanted_norm: set[str] = set()
    if pnid_files_filter:
        wanted_norm = {
            _norm_fname(f) for f in pnid_files_filter.split(",") if f.strip()
        }

    xlsx_uris: list[str] = []
    if staging_dir and fs.is_s3(staging_dir):
        try:
            sfs = fs.get_fs()
            prefix = fs.strip_scheme(staging_dir)
            for remote in sorted(sfs.ls(prefix, detail=False)):
                if remote.lower().endswith((".xlsx", ".xls")):
                    xlsx_uris.append(fs.scheme_prefix() + remote)
        except Exception as exc:
            log.warning(
                "[pnid/external] could not list staging xlsx in %s: %s",
                staging_dir,
                exc,
            )
    elif staging_dir and os.path.isdir(staging_dir):
        for fname in sorted(os.listdir(staging_dir)):
            if fname.lower().endswith((".xlsx", ".xls")):
                xlsx_uris.append(os.path.join(staging_dir, fname))

    if not xlsx_uris:
        log.warning(
            "[pnid/external] no .xlsx found in staging dir %s — extraction "
            "is external; nothing to process",
            staging_dir,
        )
        return pd.DataFrame(), pd.DataFrame()

    def _xlsx_stem(uri: str) -> str:
        return _norm_fname(os.path.splitext(uri.split("/")[-1])[0])

    if wanted_norm:
        wanted_stems = {_norm_fname(os.path.splitext(w)[0]) for w in wanted_norm}
        paired_uris = [u for u in xlsx_uris if _xlsx_stem(u) in wanted_stems]
        matched_stems = {_xlsx_stem(u) for u in paired_uris}
        unmatched_stems = wanted_stems - matched_stems
        fallback_uris = (
            [u for u in xlsx_uris if _xlsx_stem(u) not in wanted_stems]
            if unmatched_stems
            else []
        )
        log.info(
            "[pnid/external] %d xlsx total; file-paired %d, fallback-scan %d  "
            "(session files=%d, unmatched-by-name=%d)",
            len(xlsx_uris),
            len(paired_uris),
            len(fallback_uris),
            len(wanted_stems),
            len(unmatched_stems),
        )
        read_uris = paired_uris + fallback_uris
    else:
        read_uris = xlsx_uris
        log.info(
            "[pnid/external] no session filter — reading all %d xlsx", len(xlsx_uris)
        )

    eq_frames: list[pd.DataFrame] = []
    cn_frames: list[pd.DataFrame] = []
    _emit_progress(
        "transform", label="Applying templates", status="running",
        items_done=0, items_total=len(read_uris),
    )
    for uri in read_uris:
        try:
            try:
                eq = read_excel(uri, sheet_name=equipment_sheet)
            except Exception:
                eq = pd.DataFrame()
            try:
                cn = read_excel(uri, sheet_name=connectivity_sheet)
            except Exception:
                cn = pd.DataFrame()
            if not eq.empty:
                eq_frames.append(eq)
            if not cn.empty:
                cn_frames.append(cn)
            _emit_file(os.path.basename(uri), "processed", rows=int(len(eq) + len(cn)))
        except Exception as exc:
            _emit_file(os.path.basename(uri), "failed", error=str(exc))
            log.warning("[pnid/external] failed reading %s: %s", uri, exc)

    eq_raw = pd.concat(eq_frames, ignore_index=True) if eq_frames else pd.DataFrame()
    cn_raw = pd.concat(cn_frames, ignore_index=True) if cn_frames else pd.DataFrame()

    def _filter_session(df: pd.DataFrame, label: str) -> pd.DataFrame:
        if df.empty or not wanted_norm:
            return df
        if "source_file" not in df.columns:
            log.warning(
                "[pnid/external] %s sheet has no source_file column — "
                "cannot session-scope; keeping all rows",
                label,
            )
            return df
        before = len(df)
        keep = df["source_file"].map(lambda v: _norm_fname(v) in wanted_norm)
        out = df[keep].copy()
        log.info(
            "[pnid/external] %s session row filter: kept %d/%d", label, len(out), before
        )
        return out

    eq_raw = _filter_session(eq_raw, "equipment_raw")
    cn_raw = _filter_session(cn_raw, "connectivity_raw")
    return eq_raw, cn_raw


def _classify_sub_equipment(suffix: str) -> str:
    """Classify sub-equipment type from its tag suffix."""
    codes = {
        "-B1": "Burner",
        "-D1": "Drag Chain Conveyor",
        "-F1": "Fan/Blower",
        "-F2": "Fan/Blower",
        "-G1": "Gearbox",
        "-H1": "Hydraulic Unit",
        "-M1": "Motor",
        "-M2": "Motor",
        "-R1": "Refractory",
        "-S1": "Separator",
        "-T1": "Tyre/Roller Support",
        "-T2": "Tyre/Roller Support",
        "-T3": "Tyre/Roller Support",
    }
    return codes.get(suffix, "Sub-component")


def _instrument_to_parent_equipment(
    inst_tag: str,
    pid_equip_map: dict[str, str],
) -> str:
    """Map an instrument tag (e.g., CR-FI-0001) to its parent equipment"""
    import re

    m = re.match(r"([A-Z]{2})-", inst_tag)
    if not m:
        return ""
    area_code = m.group(1)

    candidates = []
    for equip_tag in pid_equip_map:
        equip_area = re.search(r"CP-([A-Z]{2})-\d{4}$", equip_tag)
        if equip_area and equip_area.group(1) == area_code:
            candidates.append(equip_tag)

    if len(candidates) == 1:
        return candidates[0]
    elif len(candidates) > 1:
        return min(candidates, key=len)
    return ""


def _write_processed_to_rustfs(
    equipment_pid: pd.DataFrame,
    equipment_connectivity: pd.DataFrame,
    processed_out: str,
) -> None:
    """Write processed P&ID parquet files to RustFS processed_data path."""
    if not processed_out:
        return
    _wlog = logging.getLogger("pnid_pipeline")
    try:
        if fs.is_s3(processed_out):
            pass
        else:
            os.makedirs(processed_out, exist_ok=True)
        ep_path = fs.path_join(processed_out, "equipment_pid.parquet")
        ec_path = fs.path_join(processed_out, "equipment_connectivity.parquet")
        fs.write_parquet(equipment_pid, ep_path)
        fs.write_parquet(equipment_connectivity, ec_path)
        _wlog.info("[rustfs] equipment_pid (%d rows) → %s", len(equipment_pid), ep_path)
        _wlog.info(
            "[rustfs] equipment_connectivity (%d rows) → %s",
            len(equipment_connectivity),
            ec_path,
        )
        print(f"  [pnid] Processed output written to: {processed_out}")
    except Exception as e:
        _wlog.error("[rustfs] FAILED to write processed_out=%s: %s", processed_out, e)
        print(f"  [pnid] ERROR: Could not write to processed_out {processed_out}: {e}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config_dir", required=True)
    ap.add_argument("--pnid_pdf_dir", required=True)
    ap.add_argument("--pnid_work_dir", required=True)
    ap.add_argument("--pnid_extractor_script", default="")
    ap.add_argument("--out_dir", required=True)
    ap.add_argument("--plant_code_id", required=True)
    ap.add_argument(
        "--pnid_data_dir",
        default="",
        help="Path (local or s3://) to structured P&ID reference data files "
        "(P_IDs_info.xlsx for drawing metadata). Not used as a source — "
        "source processing is always via LLM extraction.",
    )
    ap.add_argument(
        "--pnid_staging_dir",
        default="",
        help="Path (local or s3://) to RustFS staging P&ID PDFs, "
        "synced to local for LLM extraction.",
    )
    ap.add_argument(
        "--processed_out",
        default="",
        help="Path (local or s3://) to write processed parquet files "
        "for the review UI.",
    )
    ap.add_argument(
        "--pnid_files",
        default="",
        help="Comma-separated list of uploaded PDF filenames (e.g. file1.pdf,file2.pdf). "
        "When provided, only data for these files is processed.",
    )
    ap.add_argument(
        "--upload_batch_id",
        default="",
        help="Opaque per-upload batch id. Stamped onto every processed "
        "row (upload_batch_id column) so the review API can scope "
        "the preview to exactly this upload — durable across "
        "restarts and robust to re-uploading the same filename.",
    )
    ap.add_argument("--equipment_sheet", default="equipment_raw")
    ap.add_argument("--connectivity_sheet", default="connectivity_raw")
    args = ap.parse_args()

    import tempfile as _tempfile
    import shutil as _shutil

    _tmp_pdf_dir = _tempfile.mkdtemp(prefix="pnid_pdfs_")
    _tmp_work_dir = _tempfile.mkdtemp(prefix="pnid_work_")

    args.pnid_staging_dir = args.pnid_staging_dir or args.pnid_pdf_dir
    args.pnid_pdf_dir = _tmp_pdf_dir
    args.pnid_work_dir = _tmp_work_dir

    _data_dir = (
        os.environ.get("P0_DATA_DIR")
        or os.environ.get("p0_DATA_DIR")
        or str(Path(__file__).resolve().parents[1] / "data")
    )
    log_dir = os.environ.get("P0_LOG_DIR") or str(Path(_data_dir) / "logs")
    log = _setup_pipeline_logger(log_dir)

    t0_total = time.time()
    log.info("━━━ P&ID PIPELINE START ━━━")
    log.debug("CLI args: %s", vars(args))

    if not fs.is_s3(args.out_dir):
        os.makedirs(os.path.join(args.out_dir, "entities"), exist_ok=True)
        os.makedirs(os.path.join(args.out_dir, "relationships"), exist_ok=True)
        os.makedirs(os.path.join(args.out_dir, "validation_reports"), exist_ok=True)
    os.makedirs(os.path.join(_tmp_work_dir, "work"), exist_ok=True)

    _keys_raw = os.environ.get("GEMINI_API_KEYS", "")
    _key_count = len([k for k in _keys_raw.split(",") if k.strip()]) if _keys_raw else 0
    log.debug("[env] GEMINI_API_KEYS pool size: %d", _key_count)
    log.debug("[env] GEMINI_MODEL: %s", os.environ.get("GEMINI_MODEL", "(not set)"))

    log.debug("[config] Loading templates from: %s", args.config_dir)
    pnid_rename = load_config(args.config_dir, "pnid_column_rename.yaml")
    derived_cfg = load_config(args.config_dir, "derived_fields.yaml")
    identity_cfg = load_config(args.config_dir, "identity_frozen.yaml")
    entities_cfg = load_config(args.config_dir, "entities_frozen.yaml")
    relationships_cfg = load_config(args.config_dir, "relationships_frozen.yaml")
    schema_cfg = load_config(args.config_dir, "schema_frozen.yaml")
    validation_cfg = load_config(args.config_dir, "validation_contracts.yaml")
    log.debug(
        "[config] Templates loaded: entities=%d, schema_tables=%d, relationships=%d",
        len(entities_cfg.get("entities", {})),
        len((schema_cfg.get("schema") or schema_cfg.get("tables") or {})),
        len(relationships_cfg.get("relationships", {})),
    )

    industry = os.environ.get("CDM_INDUSTRY", "")
    if industry:
        from p0.utils.config_resolver import merge_industry_overlay

        log.debug("[overlay] Merging industry overlay: %s", industry)
        entities_cfg = merge_industry_overlay(
            args.config_dir, entities_cfg, industry, "entities"
        )
        schema_cfg = merge_industry_overlay(
            args.config_dir, schema_cfg, industry, "schema"
        )
        relationships_cfg = merge_industry_overlay(
            args.config_dir, relationships_cfg, industry, "relationships"
        )
        validation_cfg = merge_industry_overlay(
            args.config_dir, validation_cfg, industry, "validation"
        )
        log.info(
            "[overlay] Applied industry overlay '%s': entities=%d, schema_tables=%d, relationships=%d",
            industry,
            len(entities_cfg.get("entities", {})),
            len((schema_cfg.get("schema") or schema_cfg.get("tables") or {})),
            len(relationships_cfg.get("relationships", {})),
        )
        print(f"  [pnid] Applied industry overlay: {industry}")
    else:
        log.info("[overlay] No CDM_INDUSTRY set — using base templates only")
        print("  [pnid] No CDM_INDUSTRY set - using base templates only")

    log.info("━━━ STEP 1: SOURCE PROCESSING (external extractor xlsx) ━━━")
    log.debug("[source] pnid_staging_dir: %s", args.pnid_staging_dir)
    log.debug("[source] pnid_pdf_dir: %s", args.pnid_pdf_dir)
    log.debug(
        "[source] pnid_files filter: %s", args.pnid_files or "(none — process all)"
    )
    eq_raw, cn_raw = pd.DataFrame(), pd.DataFrame()

    _staging = args.pnid_staging_dir or args.pnid_pdf_dir
    log.info("[source] Reading external extractor xlsx from: %s", _staging)
    t0_extract = time.time()
    eq_raw, cn_raw = load_external_pnid_xlsx(
        staging_dir=_staging,
        pnid_files_filter=args.pnid_files,
        equipment_sheet=args.equipment_sheet,
        connectivity_sheet=args.connectivity_sheet,
    )
    log.info(
        "[source] External xlsx read in %.1fs — equipment rows: %d, connectivity rows: %d",
        time.time() - t0_extract,
        len(eq_raw),
        len(cn_raw),
    )
    log.debug(
        "[source] equipment_raw columns: %s",
        list(eq_raw.columns) if not eq_raw.empty else "[]",
    )
    log.debug(
        "[source] connectivity_raw columns: %s",
        list(cn_raw.columns) if not cn_raw.empty else "[]",
    )

    if not eq_raw.empty and "source_file" in eq_raw.columns:
        log.debug(
            "[source] source_file values in eq_raw: %s",
            sorted(eq_raw["source_file"].dropna().unique().tolist()),
        )

    _cn_raw_source_files: list[str] = (
        cn_raw["source_file"].tolist()
        if "source_file" in cn_raw.columns and not cn_raw.empty
        else []
    )

    if eq_raw.empty and cn_raw.empty:
        log.error(
            "[source] No equipment or connectivity data found. Leaving the canonical "
            "outputs in %s untouched — an empty extraction is not a reason to delete "
            "the previous run's equipment.",
            args.out_dir,
        )
        print(
            "[pnid] ERROR: No equipment or connectivity data found. "
            "Canonical outputs left untouched."
        )
        _emit_progress("load", label="No equipment or connectivity data", status="failed")
        sys.exit(2)

    log.info("━━━ STEP 3: COLUMN RENAME ━━━")
    log.debug("[rename] eq columns before: %s", list(eq_raw.columns))
    log.debug("[rename] cn columns before: %s", list(cn_raw.columns))
    eq = apply_column_rename(eq_raw, pnid_rename, source_name="pid")
    _cn_source = (
        "pid_connectivity"
        if "pid_connectivity" in (pnid_rename.get("sources") or {})
        else "pid"
    )
    cn = apply_column_rename(cn_raw, pnid_rename, source_name=_cn_source)
    log.debug("[rename] eq columns after: %s", list(eq.columns))
    log.debug("[rename] cn columns after: %s", list(cn.columns))

    log.info("━━━ STEP 4: DERIVED FIELDS ━━━")
    eq = apply_derived_fields(
        eq,
        derived_cfg,
        dataset_name="pid_equipment",
        source_name="pid",
        plant_code_id=args.plant_code_id,
    )
    cn = apply_derived_fields(
        cn,
        derived_cfg,
        dataset_name="pid",
        source_name="pid",
        plant_code_id=args.plant_code_id,
    )

    eq["plant_code_id"] = args.plant_code_id
    cn["plant_code_id"] = args.plant_code_id
    eq["source_system"] = "PID"
    cn["source_system"] = "PID"
    log.debug("[derived] eq rows: %d, cn rows: %d", len(eq), len(cn))

    log.info("━━━ STEP 5: ASSET IDENTITY ━━━")
    if "equipment_tag" in eq.columns:
        eq["equipment_tag"] = norm_upper(eq["equipment_tag"])
        eq["normalized_asset"] = eq["equipment_tag"]
        log.debug(
            "[identity] Set normalized_asset from equipment_tag — sample: %s",
            eq["equipment_tag"].head(5).tolist(),
        )

    if "normalized_asset" not in eq.columns:
        eq["normalized_asset"] = ""
        log.warning(
            "[identity] 'equipment_tag' not found in eq columns — normalized_asset set to empty"
        )

    eq = apply_identity_rules(eq, identity_cfg, dataset_name="pid_equipment")

    eq["normalized_asset"] = norm_upper(eq["normalized_asset"])
    if "equipment_tag" in eq.columns:
        eq.loc[eq["normalized_asset"] == "", "normalized_asset"] = eq["equipment_tag"]
    log.debug(
        "[identity] normalized_asset sample after rules: %s",
        eq["normalized_asset"].head(5).tolist(),
    )

    if "from_equipment_tag" in cn.columns:
        cn["from_equipment_tag"] = norm_upper(cn["from_equipment_tag"])
        cn["from_equipment_ref"] = cn["from_equipment_tag"]
        log.debug(
            "[identity] cn from_equipment_ref sample: %s",
            cn["from_equipment_ref"].head(5).tolist(),
        )
    else:
        cn["from_equipment_ref"] = ""
        log.warning(
            "[identity] 'from_equipment_tag' missing in cn — from_equipment_ref will be empty"
        )

    if "to_equipment_tag" in cn.columns:
        cn["to_equipment_tag"] = norm_upper(cn["to_equipment_tag"])
        cn["to_equipment_ref"] = cn["to_equipment_tag"]
    else:
        cn["to_equipment_ref"] = ""
        log.warning(
            "[identity] 'to_equipment_tag' missing in cn — to_equipment_ref will be empty"
        )

    if "connection_type" not in cn.columns:
        cn["connection_type"] = "FLOW"
    cn["connection_type"] = (
        cn["connection_type"].astype(str).str.strip().replace({"": "FLOW"})
    )
    _KNOWN = {"MONITORS", "HAS_SUBCOMPONENT", "FLOW", "SYSTEM SUB-COMPONENT"} | {
        str(r.get("type", "")).strip().upper()
        for r in (relationships_cfg.get("relationships", {}) or {}).values()
        if isinstance(r, dict) and str(r.get("type", "")).strip()
    }
    _coerced = (
        cn.loc[
            ~cn["connection_type"].astype(str).str.upper().isin(_KNOWN),
            "connection_type",
        ]
        .astype(str)
        .str.upper()
        .value_counts()
        .to_dict()
    )
    if _coerced:
        log.warning(
            "[identity] connection types absent from relationships.yaml, coerced to "
            "FLOW: %s — declare them in the template to keep them",
            _coerced,
        )
        print(f"  [pnid] WARNING: connection types coerced to FLOW: {_coerced}")
    cn["connection_type"] = cn["connection_type"].apply(
        lambda v: v.upper() if v.upper() in _KNOWN else "FLOW"
    )
    cn["connection_type"] = cn["connection_type"].replace(
        {"SYSTEM SUB-COMPONENT": "HAS_SUBCOMPONENT"}
    )
    log.debug(
        "[identity] connection_type distribution: %s",
        cn["connection_type"].value_counts().to_dict() if not cn.empty else {},
    )

    if "description" not in cn.columns:
        cn["description"] = ""

    fs.write_csv(
        eq, os.path.join(_tmp_work_dir, "work", "pid_post_equipment.csv"), index=False
    )
    fs.write_csv(
        cn,
        os.path.join(_tmp_work_dir, "work", "pid_post_connectivity.csv"),
        index=False,
    )
    log.debug("[step5] Saved post-rename snapshots to local work/")

    log.info("━━━ STEP 6: CANONICAL ENTITIES ━━━")
    equipment = build_entity_from_entities_yaml(
        eq,
        entities_cfg=entities_cfg,
        entity_name="equipment",
        schema_cfg=schema_cfg,
        source_prefix="pid",
    )
    equipment_connection = build_entity_from_entities_yaml(
        cn,
        entities_cfg=entities_cfg,
        entity_name="equipment_connection",
        schema_cfg=schema_cfg,
        source_prefix="pid",
    )

    equipment = apply_schema(equipment, schema_cfg, "equipment")
    equipment_connection = apply_schema_keep_extra(
        equipment_connection, schema_cfg, "equipment_connectivity"
    )
    log.info(
        "[canonical] equipment: %d rows × %d cols | equipment_connection: %d rows × %d cols",
        len(equipment),
        len(equipment.columns),
        len(equipment_connection),
        len(equipment_connection.columns),
    )
    log.debug("[canonical] equipment columns: %s", list(equipment.columns))
    log.debug(
        "[canonical] equipment_connection columns: %s",
        list(equipment_connection.columns),
    )

    equipment = assign_equipment_uid(
        equipment, plant_code_id=args.plant_code_id, uid_col="equipment_uid"
    )
    _uid_filled = (equipment["equipment_uid"].astype(str).str.strip() != "").sum()
    print(f"  [pnid] equipment_uid filled: {_uid_filled}/{len(equipment)} rows")
    log.info(
        "[canonical] equipment_uid filled: %d/%d rows", _uid_filled, len(equipment)
    )

    equipment_connection = fix_connectivity_refs(
        equipment_connection,
        equipment_df=equipment,
        from_ref_col="from_equipment_ref",
        to_ref_col="to_equipment_ref",
        asset_col="normalized_asset",
    )

    _write_merged(
        equipment,
        fs.path_join(args.out_dir, "entities", "equipment.parquet"),
        "equipment_uid",
    )

    import hashlib as _hl

    if not equipment_connection.empty:

        def _conn_uid(row):
            key = "|".join(
                [
                    str(row.get("plant_code_id", "")).strip().upper(),
                    str(row.get("from_equipment_ref", "")).strip().upper(),
                    str(row.get("to_equipment_ref", "")).strip().upper(),
                    str(row.get("connection_type", "")).strip().upper(),
                ]
            )
            return "conn:" + _hl.sha1(key.encode()).hexdigest()

        _conn_uids = equipment_connection.apply(_conn_uid, axis=1)
        equipment_connection["connectivity_uid"] = _conn_uids
        equipment_connection["conn_uid"] = _conn_uids

    _write_merged(
        equipment_connection,
        fs.path_join(args.out_dir, "entities", "equipment_connection.parquet"),
        "connectivity_uid",
    )

    equipment_pid = build_entity_from_entities_yaml(
        eq,
        entities_cfg=entities_cfg,
        entity_name="equipment_pid",
        schema_cfg=schema_cfg,
        source_prefix="pid",
    )
    equipment_pid = apply_schema(equipment_pid, schema_cfg, "equipment_pid")
    log.info(
        "[canonical] equipment_pid: %d rows × %d cols",
        len(equipment_pid),
        len(equipment_pid.columns),
    )
    log.debug("[canonical] equipment_pid columns: %s", list(equipment_pid.columns))

    if not equipment_pid.empty:

        def _pid_uid(row):
            key = "|".join(
                [
                    str(row.get("plant_code_id", "")).strip().upper(),
                    str(row.get("pid_tag", "")).strip().upper(),
                ]
            )
            return "pid:" + _hl.sha1(key.encode()).hexdigest()

        equipment_pid["pid_uid"] = equipment_pid.apply(_pid_uid, axis=1)

    _write_merged(
        equipment_pid,
        fs.path_join(args.out_dir, "entities", "equipment_pid.parquet"),
        "pid_uid",
    )

    canonical_entities = {
        "equipment": equipment,
        "equipment_connection": equipment_connection,
        "equipment_pid": equipment_pid,
    }

    log.info("━━━ STEP 7: UID TABLES ━━━")
    uid_tables = build_uid_tables_from_canonical_entities(
        canonical_entities=canonical_entities,
        entities_cfg=entities_cfg,
        schema_cfg=schema_cfg,
    )
    for entity_name, df_uid in uid_tables.items():
        _write_merged(
            df_uid,
            fs.path_join(args.out_dir, "entities", f"{entity_name}_uid.parquet"),
            df_uid.columns[0] if len(df_uid.columns) else None,
        )
        log.debug("[uid] %s_uid: %d rows", entity_name, len(df_uid))

    log.info("━━━ STEP 8: RELATIONSHIPS ━━━")
    asset_rels = build_asset_relationships(
        post_sources={"pid": cn},
        relationships_cfg=relationships_cfg,
        entities_cfg=entities_cfg,
        schema_cfg=schema_cfg,
        entity_uid_tables=uid_tables,
    )

    import hashlib

    eq_uid = uid_tables.get("equipment")
    if eq_uid is not None and not cn.empty:
        uid_map = {}
        for _, r in eq_uid.iterrows():
            k = (
                str(r.get("plant_code_id", "")).strip().upper(),
                str(r.get("normalized_asset", "")).strip().upper(),
            )
            uid_map[k] = str(r.get("equipment_uid", ""))

        now = pd.Timestamp.utcnow().isoformat()
        conn_rows = []
        for _, r in cn.iterrows():
            plant = str(r.get("plant_code_id", "")).strip().upper()
            from_tag = str(r.get("from_equipment_ref", "")).strip().upper()
            to_tag = str(r.get("to_equipment_ref", "")).strip().upper()
            rel_type = str(r.get("connection_type", "FLOW")).strip().upper() or "FLOW"
            if not plant or not from_tag or not to_tag:
                continue
            parent_uid = uid_map.get((plant, from_tag), "")
            child_uid = uid_map.get((plant, to_tag), "")
            if not parent_uid or not child_uid:
                continue
            raw = f"{plant}|{rel_type}|{parent_uid}|{child_uid}"
            rel_uid = "rel:" + hashlib.sha1(raw.encode()).hexdigest()
            srid = (
                "pid_conn:"
                + hashlib.sha1(f"{plant}|{from_tag}|{to_tag}".encode()).hexdigest()
            )
            conn_rows.append(
                {
                    "relationship_uid": rel_uid,
                    "plant_code_id": plant,
                    "parent_ref": parent_uid,
                    "child_ref": child_uid,
                    "relationship_type": rel_type,
                    "source_system": "PID",
                    "source_record_id": srid,
                    "confidence": 0.8,
                    "updated_at": now,
                    "is_active": 1,
                }
            )
        if conn_rows:
            conn_df = pd.DataFrame(conn_rows).drop_duplicates(
                subset=["parent_ref", "child_ref", "relationship_type"]
            )
            asset_rels = pd.concat([asset_rels, conn_df], ignore_index=True)
            print(f"  [pnid] connectivity relationships built: {len(conn_df)}")
            log.info(
                "[relationships] connectivity relationships built: %d", len(conn_df)
            )
            log.debug(
                "[relationships] unresolved refs (uid_map misses): cn rows=%d, resolved=%d",
                len(cn),
                len(conn_rows),
            )

    asset_rels = apply_schema(asset_rels, schema_cfg, "asset_relationship")
    log.info(
        "[relationships] total asset_relationship rows after schema: %d",
        len(asset_rels),
    )
    _write_merged(
        asset_rels,
        fs.path_join(args.out_dir, "relationships", "asset_relationship.parquet"),
        "relationship_uid",
    )

    log.info("━━━ STEP 9: VALIDATION ━━━")
    report = run_validations(
        datasets={
            "pid_post_equipment": eq,
            "pid_post_connectivity": cn,
            "equipment": equipment,
            "equipment_connection": equipment_connection,
            "asset_relationship": asset_rels,
        },
        validation_cfg=validation_cfg,
    )
    log_validation_report(report, log)
    _report_path = fs.path_join(
        args.out_dir, "validation_reports", "pnid_validation_report.json"
    )
    _report_json = report_to_json(report)
    if fs.is_s3(_report_path):
        with fs.get_fs().open(_report_path, "w") as f:
            f.write(_report_json)
    else:
        with open(_report_path, "w", encoding="utf-8") as f:
            f.write(_report_json)
    log.debug(
        "[validation] report written to validation_reports/pnid_validation_report.json"
    )

    t_total = time.time() - t0_total
    log.info("━━━ PIPELINE COMPLETE in %.1fs ━━━", t_total)
    log.info("[output] Source: external extractor xlsx (staging dir)")
    log.info("[output] Entities dir: %s", fs.path_join(args.out_dir, "entities"))
    log.info(
        "[output] Relationships dir: %s", fs.path_join(args.out_dir, "relationships")
    )
    log.info(
        "[output] equipment rows: %d | equipment_pid rows: %d | equipment_connection rows: %d | asset_rels rows: %d",
        len(equipment),
        len(equipment_pid),
        len(equipment_connection),
        len(asset_rels),
    )

    print("[done] PNID PDF -> CDM complete")
    print("Entities:", fs.path_join(args.out_dir, "entities"))
    print("Relationships:", fs.path_join(args.out_dir, "relationships"))
    print("Relationships rows:", len(asset_rels))

    log.info("━━━ STEP 10: WRITE TO RUSTFS FOR REVIEW UI ━━━")
    if args.processed_out:
        log.debug("[rustfs] processed_out target: %s", args.processed_out)
        review_eq = equipment_pid.copy()
        if "source_file" in eq.columns:
            review_eq["source_file"] = (
                eq["source_file"].values[: len(review_eq)]
                if len(eq) == len(review_eq)
                else ""
            )
        if "drawing_no" in eq.columns and "drawing_no" not in review_eq.columns:
            review_eq["drawing_no"] = (
                eq["drawing_no"].values[: len(review_eq)]
                if len(eq) == len(review_eq)
                else ""
            )
        if "pid_title" in eq.columns and "pid_title" not in review_eq.columns:
            review_eq["pid_title"] = (
                eq["pid_title"].values[: len(review_eq)]
                if len(eq) == len(review_eq)
                else ""
            )
        review_eq["plant_code_id"] = args.plant_code_id
        review_eq["source_system"] = "PID"

        review_cn = equipment_connection.copy()

        def _fill_column(values: list, n: int) -> list:
            if len(values) >= n:
                return values[:n]
            return values + [""] * (n - len(values))

        n_cn = len(review_cn)
        if _cn_raw_source_files:
            review_cn["source_file"] = _fill_column(_cn_raw_source_files, n_cn)
        elif "source_file" in cn.columns and not cn.empty:
            review_cn["source_file"] = _fill_column(cn["source_file"].tolist(), n_cn)

        if "drawing_no" in cn.columns and "drawing_no" not in review_cn.columns:
            review_cn["drawing_no"] = _fill_column(cn["drawing_no"].tolist(), n_cn)
        if "pid_title" in cn.columns and "pid_title" not in review_cn.columns:
            review_cn["pid_title"] = _fill_column(cn["pid_title"].tolist(), n_cn)
        review_cn["plant_code_id"] = args.plant_code_id
        review_cn["source_system"] = "PID"

        review_eq["upload_batch_id"] = args.upload_batch_id or ""
        review_cn["upload_batch_id"] = args.upload_batch_id or ""

        for col in ["from_equipment_ref", "to_equipment_ref"]:
            if col not in review_cn.columns:
                alt = "from_equipment_tag" if "from" in col else "to_equipment_tag"
                if alt in review_cn.columns:
                    review_cn[col] = review_cn[alt]
                else:
                    review_cn[col] = ""

        log.debug(
            "[rustfs] review_eq: %d rows × %d cols | review_cn: %d rows × %d cols",
            len(review_eq),
            len(review_eq.columns),
            len(review_cn),
            len(review_cn.columns),
        )
        log.debug("[rustfs] review_eq columns: %s", list(review_eq.columns))
        log.debug("[rustfs] review_cn columns: %s", list(review_cn.columns))
        _write_processed_to_rustfs(
            equipment_pid=review_eq,
            equipment_connectivity=review_cn,
            processed_out=args.processed_out,
        )
        log.info("[rustfs] Parquet files written to: %s", args.processed_out)
    else:
        log.warning(
            "[rustfs] No processed_out configured — skipping RustFS write (review UI will have no data)"
        )

    for _d in [_tmp_pdf_dir, _tmp_work_dir]:
        _shutil.rmtree(_d, ignore_errors=True)
    log.debug("[cleanup] local temp dirs removed")


if __name__ == "__main__":
    main()
