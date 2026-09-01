"""Timeseries end-to-end pipeline."""

from __future__ import annotations

import argparse
import os
from p0.utils import fs
import yaml

from p0.source_processing.ts.ts_processing import (
    run_ts_source_processing,
)
from p0.utils.renamer import apply_column_rename, apply_copies
from p0.utils.transposer import apply_transpose
from p0.utils.derived import apply_derived_fields
from p0.utils.identity import apply_identity_rules
from p0.utils.ts_asset_identity import (
    enrich_timeseries_asset_identity,
)
from p0.utils.validate import run_validations, report_to_json, log_validation_report
from p0.utils.schema_apply import (
    _resolve_tables,
    apply_schema,
    apply_schema_keep_extra,
)

from p0.pipelines.canonical_builder_sap import (
    build_canonical_entities_from_sap_template,
)
from p0.utils.source_attributes import pack_source_attributes
from p0.utils.canonical_entities import (
    build_uid_tables_from_canonical_entities,
    merge_entity_file_on_disk,
)
from p0.utils.canonical_relationships import (
    build_asset_relationships,
)
from p0.utils.config_resolver import resolve_config_path

_TS_REVIEW_KEY = ("plant_code_id", "site", "tag_name", "upload_batch_id")
import logging
from p0.utils.progress import emit as _emit_progress, emit_file as _emit_file


def _setup_pipeline_logger(log_dir: str) -> logging.Logger:
    """Configure a logger that writes DEBUG+ to a file and INFO+ to stdout."""
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "timeseries_pipeline.log")

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

    logger = logging.getLogger("ts_pipeline")
    logger.setLevel(logging.DEBUG)
    if not logger.handlers:
        logger.addHandler(fh)
        logger.addHandler(ch)

    logger.info("=" * 72)
    logger.info("TS PIPELINE — log file: %s", log_file)
    logger.info("=" * 72)
    return logger


log = logging.getLogger("ts_pipeline")


def load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_config(config_dir: str, filename: str) -> dict:
    return load_yaml(resolve_config_path(config_dir, filename))


def _ts_target_files(args) -> list:
    """Every metadata file this run should process, since one source is one file."""
    raw = getattr(args, "ts_file", None)
    if not raw:
        return [None]
    names = [part.strip() for part in str(raw).split(",")]
    names = [name for name in names if name]
    return names or [None]


def main():
    ap = argparse.ArgumentParser(description="Timeseries end-to-end pipeline")
    ap.add_argument("--config_dir", required=True, help="Path to config/ directory")
    ap.add_argument("--ts_in", required=True, help="Path to data/staging/ts/ directory")
    ap.add_argument("--work_dir", required=True, help="Path to data/work/ts/ directory")
    ap.add_argument("--out_dir", required=True, help="Path to data/out/ directory")
    ap.add_argument(
        "--plant_code_id",
        required=False,
        default=os.environ.get("PLANT_CODE", "UNKNOWN"),
        help="Plant code applied to every record (e.g. CEMENT_PLANT_1)",
    )
    ap.add_argument(
        "--pid_out",
        required=False,
        default=None,
        help="Output directory of the P&ID pipeline (provides equipment.csv used to resolve equipment_id)",
    )
    ap.add_argument(
        "--ts_staging_dir",
        required=False,
        default=None,
        help="Optional RustFS staging dir for Timeseries",
    )
    ap.add_argument(
        "--processed_out",
        required=False,
        default=None,
        help="Optional RustFS path to write processed ts parquet for frontend review",
    )
    ap.add_argument(
        "--upload_batch_id",
        required=False,
        default="",
        help="Opaque per-upload batch id stamped onto every processed row "
        "(upload_batch_id column) so the review API can scope the preview "
        "to exactly this upload.",
    )
    ap.add_argument(
        "--ts_file",
        required=False,
        default=None,
        help="Metadata filename (basename) to process from the staging dir, or "
        "several separated by commas. Each file is one source, so every name "
        "given is processed and their datasets merged. When omitted the "
        "pipeline falls back to the newest metadata file in the dir.",
    )
    args = ap.parse_args()

    from pathlib import Path as _Path

    _data_dir = (
        os.environ.get("P0_DATA_DIR")
        or os.environ.get("p0_DATA_DIR")
        or str(_Path(__file__).resolve().parents[1] / "data")
    )
    _log_dir = os.environ.get("P0_LOG_DIR") or str(_Path(_data_dir) / "logs")
    global log
    log = _setup_pipeline_logger(_log_dir)

    if not fs.is_s3(args.work_dir):
        if not fs.is_s3(args.out_dir):
            os.makedirs(args.work_dir, exist_ok=True)
    if not fs.is_s3(args.out_dir):
        if not fs.is_s3(args.out_dir):
            os.makedirs(args.out_dir, exist_ok=True)
    if not fs.is_s3(args.out_dir):
        os.makedirs(fs.path_join(args.out_dir, "entities"), exist_ok=True)
    if not fs.is_s3(args.out_dir):
        os.makedirs(fs.path_join(args.out_dir, "relationships"), exist_ok=True)
    if not fs.is_s3(args.out_dir):
        os.makedirs(fs.path_join(args.out_dir, "validation_reports"), exist_ok=True)
    if getattr(args, "processed_out", None) and not fs.is_s3(args.processed_out):
        os.makedirs(args.processed_out, exist_ok=True)

    rename_cfg = load_config(args.config_dir, "timeseries_column_rename.yaml")
    derived_cfg = load_config(args.config_dir, "derived_fields.yaml")
    identity_cfg = load_config(args.config_dir, "identity_frozen.yaml")
    relationships_cfg = load_config(args.config_dir, "relationships_frozen.yaml")
    entities_cfg = load_config(args.config_dir, "entities_frozen.yaml")
    schema_cfg = load_config(args.config_dir, "schema_frozen.yaml")
    validation_cfg = load_config(args.config_dir, "validation_contracts.yaml")

    ts_input_dir = getattr(args, "ts_staging_dir", None) or args.ts_in
    _emit_progress("read", label="Reading timeseries files", status="running")
    ts_targets = _ts_target_files(args)
    merged = {}
    rows_by_target: dict = {}
    for index, target in enumerate(ts_targets):
        source_work_dir = (
            fs.path_join(args.work_dir, f"source_{index}")
            if len(ts_targets) > 1
            else args.work_dir
        )
        if len(ts_targets) > 1 and not fs.is_s3(source_work_dir):
            os.makedirs(source_work_dir, exist_ok=True)
        part = run_ts_source_processing(
            ts_in=ts_input_dir,
            work_dir=source_work_dir,
            target_file=target,
        )
        contributed = 0
        for dataset_name, frame in part.items():
            if dataset_name in merged:
                log.warning(
                    "  [ts] %s already produced by an earlier file — keeping the first",
                    dataset_name,
                )
                continue
            merged[dataset_name] = frame
            contributed += 0 if frame is None else len(frame)
        rows_by_target[target or "timeseries metadata"] = contributed
    if len(ts_targets) > 1:
        log.info(
            "  [ts] %d file(s) processed, %d dataset(s): %s",
            len(ts_targets),
            len(merged),
            ", ".join(sorted(merged)),
        )

    DATASET_TO_SOURCE = {
        "timeseries": "timeseries",
    }

    post: dict = {}
    post_sources: dict = {}

    for ds_name, df in merged.items():
        source_name = DATASET_TO_SOURCE.get(ds_name, ds_name)
        source_cfg = (rename_cfg.get("sources") or {}).get(source_name) or {}

        df = apply_transpose(df, source_cfg.get("transpose"), verbose=True)
        df2 = apply_column_rename(df, rename_cfg, source_name=source_name)
        df2 = apply_copies(df2, source_cfg.get("copies"))

        pid_base = args.pid_out if args.pid_out else args.out_dir
        pid_reference_paths = [
            fs.resolve_entity_path(fs.path_join(pid_base, "entities", name))
            for name in ("equipment", "equipment_pid")
        ]
        df2 = enrich_timeseries_asset_identity(
            df2,
            reference_candidates=pid_reference_paths,
            identity_cfg=identity_cfg,
            dataset_name=ds_name,
        )

        if ds_name == "timeseries":
            if "tag_name" in df2.columns:
                from p0.utils.measurement import (
                    normalize_measurement,
                )

                df2["iotdb_tag_id"] = df2["tag_name"].map(normalize_measurement)
                if "is_active" not in df2.columns:
                    df2["is_active"] = True

        df2 = apply_derived_fields(
            df2,
            derived_cfg,
            dataset_name=ds_name,
            source_name=source_name,
            plant_code_id=args.plant_code_id,
        )

        df2["plant_code_id"] = args.plant_code_id

        df2 = apply_identity_rules(df2, identity_cfg, dataset_name=ds_name)

        post[ds_name] = df2
        post_sources[source_name] = df2

        fs.write_csv(
            df2, fs.path_join(args.work_dir, f"{ds_name}_post.csv"), index=False
        )

    for ds_name, df in post.items():
        fs.write_parquet(
            df, fs.path_join(args.out_dir, "entities", f"ts_{ds_name}.parquet")
        )

    _emit_progress("process", label="Processing timeseries", status="running",
                   items_done=0, items_total=1)
    canonical_entities = build_canonical_entities_from_sap_template(
        post_sources=post_sources,
        entities_cfg=entities_cfg,
        schema_cfg=schema_cfg,
        plant_code_id=args.plant_code_id,
    )

    def _pk(tname: str) -> str | None:
        tdef = _resolve_tables(schema_cfg or {}).get(tname, {})
        pk = tdef.get("primary_key")
        return pk if isinstance(pk, str) and pk else None

    ts_meta_this_run = None
    _ts_entity_cfg = (entities_cfg or {}).get("entities", {}).get(
        "timeseries_metadata"
    ) or {}
    _ts_prefixes = _ts_entity_cfg.get("source_attribute_prefixes")
    _ts_canonical = canonical_entities.get("timeseries_metadata")
    if _ts_canonical is not None and not _ts_canonical.empty:
        _ts_canonical = _ts_canonical.copy()
        _ts_canonical["source_attributes"] = pack_source_attributes(
            _ts_canonical, _ts_prefixes
        )
        canonical_entities["timeseries_metadata"] = _ts_canonical

    for table_name, df_ent in canonical_entities.items():
        if table_name == "timeseries_metadata":
            df_schema = apply_schema(df_ent, schema_cfg, table_name)
            ts_meta_this_run = df_schema.copy()
        else:
            df_schema = apply_schema_keep_extra(df_ent, schema_cfg, table_name)
        out_path = fs.path_join(args.out_dir, "entities", f"{table_name}.parquet")
        df_schema = merge_entity_file_on_disk(df_schema, out_path, _pk(table_name))
        fs.write_parquet(df_schema, out_path)
        canonical_entities[table_name] = df_schema

    if getattr(args, "processed_out", None):
        ts_meta_df = ts_meta_this_run
        if ts_meta_df is not None and not ts_meta_df.empty:
            ts_meta_df = ts_meta_df.copy()
            ts_meta_df["upload_batch_id"] = getattr(args, "upload_batch_id", "") or ""
            out_path = fs.path_join(
                args.processed_out, "ts_timeseries_metadata.parquet"
            )
            review_key = [
                c for c in _TS_REVIEW_KEY if c in ts_meta_df.columns
            ]
            if not review_key:
                log.warning(
                    "  [ts/rustfs] none of %s present — this run replaces the review "
                    "file instead of merging into it",
                    ", ".join(_TS_REVIEW_KEY),
                )
                merged_df = ts_meta_df
            else:
                merged_df = merge_entity_file_on_disk(
                    ts_meta_df, out_path, review_key
                )
            fs.write_parquet(merged_df, out_path)
            log.info(
                "  [ts/rustfs] wrote %d rows this run, %d total in the review file, "
                "keyed on %s, at %s",
                len(ts_meta_df),
                len(merged_df),
                "+".join(review_key) if review_key else "(none)",
                out_path,
            )

    _emit_progress("load", label="Writing entities", status="running")
    uid_tables = build_uid_tables_from_canonical_entities(
        canonical_entities=canonical_entities,
        entities_cfg=entities_cfg,
        schema_cfg=schema_cfg,
    )
    for entity_name, df_uid in uid_tables.items():
        fs.write_parquet(
            df_uid, fs.path_join(args.out_dir, "entities", f"{entity_name}_uid.parquet")
        )

    rel_uid_tables = dict(uid_tables)
    pid_equipment_uid_base = fs.path_join(
        args.pid_out if args.pid_out else args.out_dir, "entities", "equipment_uid"
    )
    if fs.entity_exists(pid_equipment_uid_base):
        rel_uid_tables["equipment"] = fs.read_entity(pid_equipment_uid_base)
        _eq_df = rel_uid_tables["equipment"]
        _eq_plants = (
            set(_eq_df["plant_code_id"].astype(str).str.strip().str.upper())
            if "plant_code_id" in getattr(_eq_df, "columns", [])
            else set()
        )
        _this_plant = str(args.plant_code_id).strip().upper()
        if _eq_plants and _this_plant not in _eq_plants:
            log.warning(
                "[ts] the equipment uid table at %s holds plant(s) %s but this run "
                "is plant %r — every HAS_TAG lookup will miss and no edges will be "
                "built; check --plant_code_id against the pnid stage",
                pid_equipment_uid_base,
                sorted(_eq_plants),
                args.plant_code_id,
            )
            print(
                f"  [ts] WARNING: pnid equipment is plant {sorted(_eq_plants)} but "
                f"this run is {args.plant_code_id!r} — 0 HAS_TAG edges expected"
            )
    else:
        log.warning(
            "[ts] no equipment uid table at %s — HAS_TAG edges cannot be built; "
            "run the pnid stage for this plant first",
            pid_equipment_uid_base,
        )

    rel_sources = dict(post_sources)
    for ds_name, table_name in (("timeseries", "timeseries_metadata"),):
        canonical_df = canonical_entities.get(table_name)
        if canonical_df is None or canonical_df.empty:
            continue
        existing = rel_sources.get(ds_name)
        if existing is None or existing.empty:
            rel_sources[ds_name] = canonical_df

    ts_rels = build_asset_relationships(
        post_sources=rel_sources,
        relationships_cfg=relationships_cfg,
        entities_cfg=entities_cfg,
        schema_cfg=schema_cfg,
        entity_uid_tables=rel_uid_tables,
    )
    rels_path = fs.path_join(args.out_dir, "relationships", "ts_relationship.parquet")
    fs.write_parquet(ts_rels, rels_path)

    report = run_validations(post, validation_cfg)
    log_validation_report(report, log)
    report_path = fs.path_join(
        args.out_dir, "validation_reports", "ts_validation_report.json"
    )
    _report_json = report_to_json(report)
    if fs.is_s3(report_path):
        with fs.get_fs().open(report_path, "w") as f:
            f.write(_report_json)
    else:
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(_report_json)

    ts_meta_df = canonical_entities.get("timeseries_metadata")
    ts_rows = len(ts_meta_df) if ts_meta_df is not None else 0

    if len(rows_by_target) > 1:
        for target_name, target_rows in rows_by_target.items():
            _emit_file(target_name, "processed", rows=int(target_rows))
    else:
        _emit_file(
            getattr(args, "ts_file", None) or "timeseries metadata",
            "processed",
            rows=int(ts_rows) if isinstance(ts_rows, int) else None,
        )
    log.info("✅ Timeseries end-to-end completed.")
    log.info(f"Plant         : {args.plant_code_id}")
    log.info(f"Input         : {args.ts_in}")
    log.info(f"Work          : {args.work_dir}")
    log.info(f"Entities dir  : {fs.path_join(args.out_dir, 'entities')}")
    log.info(f"Rels dir      : {fs.path_join(args.out_dir, 'relationships')}")
    log.info(f"Report        : {report_path}")
    log.info(f"Tags canonical: {ts_rows}")
    log.info(f"Rel rows      : {len(ts_rels)}")
    log.info(f"Status        : {report.get('status')}")

    if "timeseries" in post and "asset_match_method" in post["timeseries"].columns:
        methods = (
            post["timeseries"]["asset_match_method"]
            .value_counts(dropna=False)
            .to_dict()
        )
        log.info(f"Asset match   : {methods}")


if __name__ == "__main__":
    main()
