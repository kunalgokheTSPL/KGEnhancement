"""Documents end-to-end pipeline."""

from __future__ import annotations

import argparse
import logging
import os
import re
import yaml

import pandas as pd

from p0.utils.schema_apply import _resolve_tables

from p0.source_processing.documents.user_doc_extract import (
    process_user_documents,
)
from p0.utils.user_config import load_user_config
from p0.utils.progress import emit as _emit_progress


def _setup_pipeline_logger(log_dir: str) -> logging.Logger:
    """Configure a logger that writes DEBUG+ to a file and INFO+ to stdout."""
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "docs_pipeline.log")

    from p0.utils.progress_log import setup_pipeline_logger

    log = setup_pipeline_logger("docs_pipeline", log_file)

    log.info("=" * 72)
    log.info("DOCS PIPELINE — log file: %s", log_file)
    log.info("=" * 72)
    return log


log = logging.getLogger("docs_pipeline")
from p0.utils.renamer import apply_column_rename
from p0.utils.derived import apply_derived_fields
from p0.utils.identity import apply_identity_rules
from p0.utils.doc_asset_identity import (
    enrich_document_asset_identity,
)
from p0.utils.validate import run_validations, report_to_json, log_validation_report
from p0.utils.schema_apply import apply_schema

from p0.pipelines.canonical_builder_sap import (
    build_canonical_entities_from_sap_template,
)
from p0.utils.canonical_entities import (
    build_uid_tables_from_canonical_entities,
    merge_entity_file_on_disk,
)
from p0.utils.canonical_relationships import (
    build_asset_relationships,
)
from p0.utils.config_resolver import resolve_config_path


def load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_config(config_dir: str, filename: str) -> dict:
    return load_yaml(resolve_config_path(config_dir, filename))


def _write_processed_docs_to_rustfs(
    docs_df: pd.DataFrame,
    processed_out: str,
    log: logging.Logger,
) -> None:
    """Write processed document rows to RustFS split by document_type."""
    if docs_df.empty:
        log.warning(
            "  [docs/rustfs] DataFrame is empty — nothing to write to %s", processed_out
        )
        return
    try:
        from p0.utils.fs import write_parquet, path_join

        doc_types = (
            docs_df["document_type"].unique()
            if "document_type" in docs_df.columns
            else []
        )

        try:
            from p0.utils.fs import get_fs as _get_fs, strip_scheme

            _sfs = _get_fs()
            prefix = strip_scheme(processed_out)
            for old_path in _sfs.glob(f"{prefix}/*/*.parquet"):
                try:
                    _sfs.rm(old_path)
                    log.info("  [docs/rustfs] deleted stale parquet: %s", old_path)
                except Exception:
                    pass
        except Exception as _exc:
            log.debug("  [docs/rustfs] could not clean up stale parquets: %s", _exc)

        has_key = "document_type_key" in docs_df.columns
        for dtype in doc_types:
            type_df = docs_df[docs_df["document_type"] == dtype].copy()
            if has_key:
                keys = [
                    k for k in type_df["document_type_key"].tolist() if str(k).strip()
                ]
                dir_source = keys[0] if keys else dtype
            else:
                dir_source = dtype
            safe_type = re.sub(r"[^a-z0-9_-]", "_", str(dir_source).lower().strip())
            out_path = path_join(processed_out, safe_type, "docs_documents.parquet")
            write_parquet(type_df, out_path)
            log.info(
                "  [docs/rustfs] wrote %d rows for '%s' → %s",
                len(type_df),
                dtype,
                out_path,
            )
    except Exception as exc:
        log.warning(
            "  [docs/rustfs] Could not write to processed_out=%s: %s",
            processed_out,
            exc,
        )


def main():
    ap = argparse.ArgumentParser(description="Documents end-to-end pipeline")
    ap.add_argument("--config_dir", required=True, help="Path to config/ directory")
    ap.add_argument(
        "--doc_extraction_dir",
        required=True,
        help="Folder with extracted Excel files (sop_sections_*.xlsx, etc.)",
    )
    ap.add_argument(
        "--work_dir", required=True, help="Path to data/work/docs/ directory"
    )
    ap.add_argument("--out_dir", required=True, help="Path to data/out/ directory")
    ap.add_argument(
        "--plant_code_id",
        required=False,
        default=os.environ.get("PLANT_CODE", "UNKNOWN"),
        help="Plant code applied to every record (e.g. CEMENT_PLANT_1)",
    )
    ap.add_argument(
        "--processed_out",
        required=False,
        default=None,
        help="RustFS (or local) path to write processed docs parquet for frontend review "
        "(e.g. s3://staging/processed_data/documents)",
    )
    ap.add_argument(
        "--pid_out",
        required=False,
        default=None,
        help="Output directory of the P&ID pipeline (provides equipment.csv used to resolve equipment_id)",
    )
    ap.add_argument(
        "--doc_types",
        required=False,
        default=None,
        help="Comma-separated list of doc type keys to process (e.g. sop,maintenance_manual). "
        "If omitted, all configured doc types are processed.",
    )
    ap.add_argument(
        "--doc_files",
        required=False,
        default=None,
        help="Comma-separated list of filenames uploaded in this session. When provided, "
        "only these files are processed. Already-processed files (tracked in "
        "$p0_DATA_DIR/docs/docs_done.json) are skipped regardless of this filter.",
    )
    ap.add_argument(
        "--upload_batch_id",
        required=False,
        default="",
        help="Opaque per-upload batch id stamped onto every processed row "
        "(upload_batch_id column) so the review API can scope the preview "
        "to exactly this upload — durable across restarts and robust to "
        "re-uploading the same filename.",
    )
    args = ap.parse_args()

    from pathlib import Path as _Path

    _data_dir = (
        os.environ.get("P0_DATA_DIR")
        or os.environ.get("p0_DATA_DIR")
        or str(_Path(__file__).resolve().parents[1] / "data")
    )
    log_dir = os.environ.get("P0_LOG_DIR") or os.path.join(_data_dir, "logs")
    log = _setup_pipeline_logger(log_dir)

    log.info("plant_code_id        : %s", args.plant_code_id)
    log.info("doc_extraction_dir: %s", args.doc_extraction_dir)
    log.info("work_dir          : %s", args.work_dir)
    log.info("out_dir           : %s", args.out_dir)
    log.info("processed_out     : %s", args.processed_out or "(not provided)")
    log.info("pid_out           : %s", args.pid_out or "(not provided)")

    from p0.utils import fs as _fs

    os.makedirs(args.work_dir, exist_ok=True)
    if not _fs.is_s3(args.out_dir):
        os.makedirs(args.out_dir, exist_ok=True)
        os.makedirs(os.path.join(args.out_dir, "entities"), exist_ok=True)
        os.makedirs(os.path.join(args.out_dir, "relationships"), exist_ok=True)
        os.makedirs(os.path.join(args.out_dir, "validation_reports"), exist_ok=True)

    rename_cfg = load_config(args.config_dir, "docs_column_rename.yaml")
    derived_cfg = load_config(args.config_dir, "derived_fields.yaml")
    identity_cfg = load_config(args.config_dir, "identity_frozen.yaml")
    relationships_cfg = load_config(args.config_dir, "relationships_frozen.yaml")
    entities_cfg = load_config(args.config_dir, "entities_frozen.yaml")
    schema_cfg = load_config(args.config_dir, "schema_frozen.yaml")
    validation_cfg = load_config(args.config_dir, "validation_contracts.yaml")

    log.info("=" * 60)
    log.info("STEP 1: Source processing (user_doc_extract — staging documents)")
    _emit_progress("validate", label="Validating files", status="running")
    log.info("=" * 60)

    user_cfg = load_user_config(args.config_dir)
    doc_types_cfg = user_cfg.get("document_processing", {}).get("document_types", {})

    if args.doc_types:
        allowed = {t.strip() for t in args.doc_types.split(",") if t.strip()}
        doc_types_cfg = {k: v for k, v in doc_types_cfg.items() if k in allowed}
        log.info(
            "  doc_types filter: %s → processing: %s",
            sorted(allowed),
            list(doc_types_cfg.keys()),
        )
        missing = sorted(allowed - set(doc_types_cfg.keys()))
        if missing:
            raise SystemExit(
                "doc_type(s) have no field configuration in user_config.yaml: "
                f"{missing}. Define their extraction fields (source_columns) "
                "before processing — the upload should persist them."
            )

    if not doc_types_cfg:
        raise SystemExit(
            "No document_types resolved for this run — nothing to extract. "
            "Ensure the uploaded doc type(s) have source_columns configured."
        )
    log.info("  doc_types: %s", list(doc_types_cfg.keys()))

    _files_filter: set[str] | None = None
    if args.doc_files:
        _files_filter = {f.strip() for f in args.doc_files.split(",") if f.strip()}
        log.info(
            "  doc_files filter: %d file(s) — %s",
            len(_files_filter),
            sorted(_files_filter)[:10],
        )
    from pathlib import Path as _Path

    _data_dir = (
        os.environ.get("P0_DATA_DIR")
        or os.environ.get("p0_DATA_DIR")
        or str(_Path(__file__).resolve().parents[1] / "data")
    )
    _ledger_dir = os.path.join(_data_dir, "docs")
    os.makedirs(_ledger_dir, exist_ok=True)
    _dedup_ledger = os.path.join(_ledger_dir, "docs_done.json")
    log.info("  dedup ledger: %s", _dedup_ledger)

    df_docs = process_user_documents(
        staging_dir=args.doc_extraction_dir,
        doc_types_cfg=doc_types_cfg,
        log=log,
        files_filter=_files_filter,
        dedup_ledger_path=_dedup_ledger,
        processed_out=args.processed_out,
    )
    log.info("  Extracted %d rows from staging", len(df_docs))

    merged = {"documents": df_docs}
    df_docs.to_csv(os.path.join(args.work_dir, "documents_merged.csv"), index=False)

    DATASET_TO_SOURCE = {
        "documents": "documents",
    }

    post: dict = {}
    post_sources: dict = {}

    for ds_name, df in merged.items():
        source_name = DATASET_TO_SOURCE.get(ds_name, ds_name)
        log.info("")
        log.info("=" * 60)
        log.info("Processing dataset: %s (%d rows)", ds_name, len(df))
        log.info("=" * 60)

        log.info("  STEP 2: Column rename")
        df2 = apply_column_rename(df, rename_cfg, source_name=source_name)
        log.debug("    columns after rename: %s", df2.columns.tolist())

        log.info("  STEP 3: Asset identity (similarity matching)")
        if ds_name == "documents":
            pid_base = args.pid_out or args.out_dir
            reference_candidates = [
                _fs.path_join(pid_base, "entities", "equipment.parquet"),
                _fs.path_join(pid_base, "entities", "equipment.csv"),
            ]
            df2 = enrich_document_asset_identity(
                df2,
                reference_candidates=reference_candidates,
            )

            if "asset_match_method" in df2.columns:
                methods = df2["asset_match_method"].value_counts(dropna=False).to_dict()
                log.info("    asset match: %s", methods)

        log.info("  STEP 4: Derived fields")
        df2 = apply_derived_fields(
            df2,
            derived_cfg,
            dataset_name=ds_name,
            source_name=source_name,
            plant_code_id=args.plant_code_id,
        )
        df2["plant_code_id"] = args.plant_code_id
        df2["upload_batch_id"] = args.upload_batch_id or ""

        log.info("  STEP 5: Identity rules")
        df2 = apply_identity_rules(df2, identity_cfg, dataset_name=ds_name)

        post[ds_name] = df2
        post_sources[source_name] = df2

        df2.to_csv(os.path.join(args.work_dir, f"{ds_name}_post.csv"), index=False)
        log.info("    wrote %s_post.csv (%d rows)", ds_name, len(df2))

    from p0.utils import fs as _fs

    for ds_name, df in post.items():
        out_path = _fs.path_join(args.out_dir, "entities", f"docs_{ds_name}.parquet")
        _fs.write_parquet(df, out_path)
        log.info("  Step 6: wrote %s", out_path)

    if args.processed_out:
        _write_processed_docs_to_rustfs(
            docs_df=post.get("documents", pd.DataFrame()),
            processed_out=args.processed_out,
            log=log,
        )

    log.info("")
    log.info("=" * 60)
    log.info("STEP 7: Canonical entity builder")
    _emit_progress("load", label="Building canonical rows", status="running")
    log.info("=" * 60)
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

    for table_name, df_ent in canonical_entities.items():
        df_schema = apply_schema(df_ent, schema_cfg, table_name)
        out_path = _fs.path_join(args.out_dir, "entities", f"{table_name}.parquet")
        df_schema = merge_entity_file_on_disk(df_schema, out_path, _pk(table_name))
        _fs.write_parquet(df_schema, out_path)
        canonical_entities[table_name] = df_schema
        if not df_schema.empty:
            log.info("  %s: %d rows", table_name, len(df_schema))

    log.info("")
    log.info("=" * 60)
    log.info("STEP 8: UID lookup tables")
    log.info("=" * 60)
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
        if not df_uid.empty:
            log.info("  %s_uid: %d rows", entity_name, len(df_uid))

    log.info("")
    log.info("=" * 60)
    log.info("STEP 9: Relationship builder")
    _emit_progress("finalize", label="Finalising", status="running")
    log.info("=" * 60)
    doc_rels = build_asset_relationships(
        post_sources=post_sources,
        relationships_cfg=relationships_cfg,
        entities_cfg=entities_cfg,
        schema_cfg=schema_cfg,
        entity_uid_tables=uid_tables,
    )
    rels_path = _fs.path_join(
        args.out_dir, "relationships", "docs_relationship.parquet"
    )
    _fs.write_parquet(doc_rels, rels_path)
    log.info("  relationship rows: %d", len(doc_rels))

    log.info("")
    log.info("=" * 60)
    log.info("STEP 10: Validation")
    log.info("=" * 60)
    report = run_validations(post, validation_cfg)
    log_validation_report(report, log)
    report_path = _fs.path_join(
        args.out_dir,
        "validation_reports",
        "docs_validation_report.json",
    )
    _report_json = report_to_json(report)
    if _fs.is_s3(report_path):
        with _fs.get_fs().open(report_path, "w") as f:
            f.write(_report_json)
    else:
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(_report_json)

    doc_meta = canonical_entities.get("document_metadata")
    doc_rows = len(doc_meta) if doc_meta is not None else 0

    log.info("")
    log.info("=" * 60)
    log.info("DOCUMENTS PIPELINE COMPLETE")
    log.info("=" * 60)
    log.info("Plant            : %s", args.plant_code_id)
    log.info("Input            : %s", args.doc_extraction_dir)
    log.info("Work             : %s", args.work_dir)
    log.info("Entities dir     : %s", os.path.join(args.out_dir, "entities"))
    log.info("Rels dir         : %s", os.path.join(args.out_dir, "relationships"))
    log.info("Report           : %s", report_path)
    log.info("Docs canonical   : %d", doc_rows)
    log.info("Rel rows         : %d", len(doc_rels))
    log.info("Validation status: %s", report.get("status"))

    if "documents" in post and "asset_match_method" in post["documents"].columns:
        methods = (
            post["documents"]["asset_match_method"].value_counts(dropna=False).to_dict()
        )
        log.info("Asset match      : %s", methods)


if __name__ == "__main__":
    main()
