"""End-to-end runtime pipeline for user-defined Other Files."""

from __future__ import annotations

import argparse
import logging
import os
import tempfile
from pathlib import Path

import pandas as pd

from p0.source_processing.documents.user_doc_extract import (
    process_user_documents,
)
from p0.source_processing.other_files.other_files_processing import (
    add_equipment_id,
    detect_data_mode,
    make_parquet_safe,
    normalize_columns,
    process_structured_other_file,
)
from p0.utils import fs as _fs
from p0.utils.progress import (
    emit as _emit_progress,
    emit_file as _emit_file,
)


log = logging.getLogger("other_files_pipeline")


def _setup_logger(
    work_dir: str,
) -> logging.Logger:
    """Configure the Other Files pipeline logger."""

    os.makedirs(
        work_dir,
        exist_ok=True,
    )

    logger = logging.getLogger(
        "other_files_pipeline"
    )

    logger.setLevel(
        logging.INFO
    )

    if not logger.handlers:
        handler = logging.StreamHandler()

        formatter = logging.Formatter(
            "%(asctime)s %(levelname)s %(message)s"
        )

        handler.setFormatter(
            formatter
        )

        logger.addHandler(
            handler
        )

    return logger


def _copy_staged_file_to_local(
    *,
    staging_dir: str,
    user_defined_type: str,
    file_name: str,
) -> str:
    """
    Download one staged Other File to a temporary local path.

    Upload layout:
        documents/others/<user_defined_type>/<file>
    """

    remote = _fs.path_join(
        staging_dir,
        "others",
        user_defined_type,
        file_name,
    )

    extension = Path(
        file_name
    ).suffix.lower()

    fd, temp_path = tempfile.mkstemp(
        prefix="other_file_",
        suffix=extension,
    )

    os.close(fd)

    try:
        from p0.utils.fs import get_fs

        fs = get_fs()

        remote_key = _fs.strip_scheme(
            remote
        )

        logger = logging.getLogger(
            "other_files_pipeline"
        )

        logger.info(
            "Reading staged Other File: %s",
            remote,
        )

        with fs.open(
            remote_key,
            "rb",
        ) as source:
            payload = source.read()

        with open(
            temp_path,
            "wb",
        ) as target:
            target.write(
                payload
            )

        logger.info(
            "Copied staged file locally: %s (%d bytes)",
            temp_path,
            len(payload),
        )

    except Exception:
        try:
            os.unlink(
                temp_path
            )
        except OSError:
            pass

        raise

    return temp_path


def _write_processed_output(
    df: pd.DataFrame,
    *,
    processed_out: str,
    out_dir: str,
    user_defined_type: str,
) -> dict[str, str]:
    """
    Persist Other Files output in two locations.

    processed_out:
        durable processed copy used by the document/other-files flow.

    out_dir/entities:
        stage output exposed through /pipeline/outputs and preview APIs.
    """

    processed_path = _fs.path_join(
        processed_out,
        "others",
        user_defined_type,
        "other_files.parquet",
    )

    _fs.write_parquet(
        df,
        processed_path,
    )

    entity_path = _fs.path_join(
        out_dir,
        "entities",
        f"{user_defined_type}.parquet",
    )

    _fs.write_parquet(
        df,
        entity_path,
    )

    return {
        "processed_path": processed_path,
        "entity_path": entity_path,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="User-defined Other Files pipeline"
    )

    parser.add_argument(
        "--extraction_fields",
        default="",
        help=(
            "Comma-separated user-defined fields to extract "
            "for unstructured Other Files."
        ),
    )

    parser.add_argument(
        "--staging_dir",
        required=True,
    )

    parser.add_argument(
        "--processed_out",
        required=True,
    )

    parser.add_argument(
        "--out_dir",
        required=True,
    )

    parser.add_argument(
        "--work_dir",
        required=True,
    )

    parser.add_argument(
        "--plant_code_id",
        required=True,
    )

    parser.add_argument(
        "--other_type",
        required=True,
        help=(
            "Safe user-defined type key, "
            "e.g. valve_inspection"
        ),
    )

    parser.add_argument(
        "--other_file",
        required=True,
    )

    parser.add_argument(
        "--equipment_column",
        default=None,
    )

    parser.add_argument(
        "--upload_batch_id",
        default="",
    )

    args = parser.parse_args()

    logger = _setup_logger(
        args.work_dir
    )

    logger.info("=" * 60)

    logger.info(
        "OTHER FILES PIPELINE"
    )

    logger.info(
        "plant=%s type=%s file=%s",
        args.plant_code_id,
        args.other_type,
        args.other_file,
    )

    # ---------------------------------------------------------
    # VALIDATE
    # ---------------------------------------------------------

    _emit_progress(
        "validate",
        label="Inspecting Other File",
        status="running",
    )

    mode = detect_data_mode(
        args.other_file
    )

    logger.info(
        "Detected runtime mode: %s",
        mode,
    )

    local_path = _copy_staged_file_to_local(
        staging_dir=args.staging_dir,
        user_defined_type=args.other_type,
        file_name=args.other_file,
    )

    _emit_progress(
        "validate",
        label="Inspecting Other File",
        status="completed",
    )

    try:
        # =====================================================
        # STRUCTURED
        # =====================================================

        if mode == "structured":
            # -------------------------------------------------
            # PROCESS
            # -------------------------------------------------

            _emit_progress(
                "process",
                label="Reading structured data",
                status="running",
            )

            df = process_structured_other_file(
                local_path,
                equipment_column=args.equipment_column,
            )

            # Runtime lineage fields.
            df["plant_code_id"] = (
                args.plant_code_id
            )

            df["upload_batch_id"] = (
                args.upload_batch_id
            )

            df["other_file_type"] = (
                args.other_type
            )

            df["source_file"] = (
                args.other_file
            )

            _emit_progress(
                "process",
                label="Reading structured data",
                status="completed",
            )

            # -------------------------------------------------
            # LOAD
            # -------------------------------------------------

            _emit_progress(
                "load",
                label="Writing processed output",
                status="running",
            )

            output_paths = _write_processed_output(
                df,
                processed_out=args.processed_out,
                out_dir=args.out_dir,
                user_defined_type=args.other_type,
            )

            _emit_progress(
                "load",
                label="Writing processed output",
                status="completed",
            )

            logger.info(
                "other_files rows: %d",
                len(df),
            )

            logger.info(
                "Processed output: %s",
                output_paths["processed_path"],
            )

            logger.info(
                "Pipeline entity output: %s",
                output_paths["entity_path"],
            )

            _emit_file(
                args.other_file,
                status="completed",
                rows=len(df),
            )

            # -------------------------------------------------
            # FINALIZE
            # -------------------------------------------------

            _emit_progress(
                "finalize",
                label="Finalising Other Files processing",
                status="running",
            )

            logger.info(
                "Structured Other Files processing completed successfully"
            )

            _emit_progress(
                "finalize",
                label="Finalising Other Files processing",
                status="completed",
            )

            return

        # =====================================================
        # UNSTRUCTURED
        # =====================================================

        if mode == "unstructured":
            extraction_fields = [
                field.strip()
                for field in (
                    args.extraction_fields or ""
                ).split(",")
                if field.strip()
            ]

            if not extraction_fields:
                raise ValueError(
                    "extraction_fields are required "
                    "for unstructured Other Files"
                )

            # -------------------------------------------------
            # PROCESS
            # -------------------------------------------------

            _emit_progress(
                "process",
                label="Extracting fields from PDF",
                status="running",
            )

            logger.info(
                "Unstructured extraction fields: %s",
                extraction_fields,
            )

            # Reuse the existing unified document extractor.
            #
            # Other Files are staged at:
            # documents/others/<other_type>/<file>
            #
            # process_user_documents expects:
            # staging_dir/<doc_type>/<file>
            #
            # Therefore:
            # staging_dir = documents/others
            # doc_type    = <other_type>

            other_staging_root = _fs.path_join(
                args.staging_dir,
                "others",
            )

            doc_types_cfg = {
                args.other_type: {
                    "label": args.other_type,
                    "source_columns": extraction_fields,
                }
            }

            files_filter = {
                args.other_file
            }

            dedup_ledger_path = os.path.join(
                args.work_dir,
                "other_files_docs_done.json",
            )

            df = process_user_documents(
                staging_dir=other_staging_root,
                doc_types_cfg=doc_types_cfg,
                log=logger,
                files_filter=files_filter,
                dedup_ledger_path=dedup_ledger_path,
                processed_out=None,
            )

            if df is None or df.empty:
                raise ValueError(
                    "Document extraction completed but "
                    "returned no rows"
                )

            # Normalize dynamic output field names.
            df = normalize_columns(
                df
            )

            # Normalize/add equipment_id.
            df = add_equipment_id(
                df,
                equipment_column=args.equipment_column,
            )

            # Make dynamic user fields safe for Parquet.
            df = make_parquet_safe(
                df
            )

            # Runtime lineage fields.
            df["plant_code_id"] = (
                args.plant_code_id
            )

            df["upload_batch_id"] = (
                args.upload_batch_id
            )

            df["other_file_type"] = (
                args.other_type
            )

            df["source_file"] = (
                args.other_file
            )

            _emit_progress(
                "process",
                label="Extracting fields from PDF",
                status="completed",
            )

            # -------------------------------------------------
            # LOAD
            # -------------------------------------------------

            _emit_progress(
                "load",
                label="Writing processed output",
                status="running",
            )

            output_paths = _write_processed_output(
                df,
                processed_out=args.processed_out,
                out_dir=args.out_dir,
                user_defined_type=args.other_type,
            )

            _emit_progress(
                "load",
                label="Writing processed output",
                status="completed",
            )

            logger.info(
                "other_files rows: %d",
                len(df),
            )

            logger.info(
                "Processed output: %s",
                output_paths["processed_path"],
            )

            logger.info(
                "Pipeline entity output: %s",
                output_paths["entity_path"],
            )

            _emit_file(
                args.other_file,
                status="completed",
                rows=len(df),
            )

            # -------------------------------------------------
            # FINALIZE
            # -------------------------------------------------

            _emit_progress(
                "finalize",
                label="Finalising Other Files PDF extraction",
                status="running",
            )

            logger.info(
                "Unstructured Other Files processing completed successfully"
            )

            _emit_progress(
                "finalize",
                label="Finalising Other Files PDF extraction",
                status="completed",
            )

            return

        raise ValueError(
            f"Unsupported Other Files data mode: {mode}"
        )

    finally:
        try:
            os.unlink(
                local_path
            )
        except OSError:
            pass


if __name__ == "__main__":
    main()