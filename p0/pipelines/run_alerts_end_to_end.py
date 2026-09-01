"""ALERTS structured end-to-end pipeline.

Flow:
    ALERTS staging
        -> read Open Alerts / Closed Alerts
        -> normalize source columns
        -> preserve Tag Number
        -> resolve equipment_id against P&ID when available
        -> add alert_status
        -> add source_file / plant / batch lineage
        -> write processed ALERTS parquet
        -> write canonical ALERTS output
        -> persist canonical ALERTS rows transactionally
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import tempfile
from pathlib import Path
from utility.secret_manager import load_secrets
from botocore import args
import pandas as pd

from p0.api.database.cdm_writer import _write_to_db
from p0.source_processing.alerts.alerts_processing import (
    process_alerts_workbook,
)
from p0.utils import fs as _fs


log = logging.getLogger("alerts_pipeline")


def _progress(
    *,
    phase: str,
    label: str,
    status: str,
    items_done: int | None = None,
    items_total: int | None = None,
    current_item: str | None = None,
) -> None:
    """Emit a pipeline-progress marker consumed by pipeline.py."""

    payload: dict = {
        "phase": phase,
        "label": label,
        "status": status,
    }

    if items_done is not None:
        payload["items_done"] = items_done

    if items_total is not None:
        payload["items_total"] = items_total

    if current_item is not None:
        payload["current_item"] = current_item

    print(
        "##P0PROGRESS " + json.dumps(payload, default=str),
        flush=True,
    )


def _file_marker(
    file_name: str,
    *,
    status: str,
    rows: int | None = None,
    error: str | None = None,
) -> None:
    """Emit a per-file result marker consumed by pipeline.py."""

    payload: dict = {
        "file": file_name,
        "status": status,
    }

    if rows is not None:
        payload["rows"] = rows

    if error:
        payload["error"] = error

    print(
        "##P0FILE " + json.dumps(payload, default=str),
        flush=True,
    )


def _leaf(value: str) -> str:
    """Return the final filename component."""

    return (
        str(value or "")
        .strip()
        .replace("\\", "/")
        .rsplit("/", 1)[-1]
    )


def _resolve_source_path(
    alerts_in: str,
    alerts_file: str,
) -> str:
    """Build the exact staged ALERTS source path."""

    return _fs.path_join(
        alerts_in,
        alerts_file,
    )


def _copy_to_local(
    source_path: str,
    work_dir: str,
    file_name: str,
) -> str:
    """Copy a staged local/S3 workbook to a local temporary path."""

    os.makedirs(
        work_dir,
        exist_ok=True,
    )

    suffix = Path(file_name).suffix or ".xlsx"

    fd, local_path = tempfile.mkstemp(
        prefix="alerts_",
        suffix=suffix,
        dir=work_dir,
    )

    os.close(fd)

    if _fs.is_s3(source_path):
        fs = _fs.get_fs()

        with fs.open(
            _fs.strip_scheme(source_path),
            "rb",
        ) as src:
            with open(
                local_path,
                "wb",
            ) as dst:
                dst.write(src.read())

    else:
        with open(
            source_path,
            "rb",
        ) as src:
            with open(
                local_path,
                "wb",
            ) as dst:
                dst.write(src.read())

    return local_path


def _load_pnid_equipment_ids(
    pid_out: str | None,
    plant_code_id: str,
) -> list[str]:
    """
    Load canonical P&ID equipment IDs for ALERTS matching.

    Resolution sources:
      1. Plant-scoped PostgreSQL equipment_pid.equipment_id
      2. P&ID parquet output as fallback

    The exact canonical equipment_pid.equipment_id value is preserved so that
    ALERTS can match tolerant source tags such as ``G7510`` against canonical
    P&ID IDs such as ``G-7510`` and store ``G-7510`` in alerts.equipment_id.
    """

    equipment_ids: list[str] = []
    seen: set[str] = set()

    def _add(value) -> None:
        if value is None:
            return

        text = str(value).strip()

        if not text or text.lower() in {
            "nan",
            "none",
            "null",
        }:
            return

        if text not in seen:
            seen.add(text)
            equipment_ids.append(text)

    # ---------------------------------------------------------
    # 1. LOAD CANONICAL IDs FROM PLANT-SCOPED equipment_pid
    # ---------------------------------------------------------

    try:
        from p0.api.database.connection import _get_conn

        conn = _get_conn(plant_code_id)

        try:
            cur = conn.cursor()

            cur.execute(
                """
                SELECT equipment_id
                FROM equipment_pid
                WHERE equipment_id IS NOT NULL
                  AND BTRIM(equipment_id) <> ''
                """
            )

            for row in cur.fetchall():
                _add(row[0])

            if equipment_ids:
                log.info(
                    "[alerts] Loaded %d canonical equipment_pid IDs "
                    "from PostgreSQL for plant=%s",
                    len(equipment_ids),
                    plant_code_id,
                )

                return equipment_ids

        finally:
            conn.close()

    except Exception as exc:
        log.warning(
            "[alerts] Could not load equipment_pid identifiers "
            "from PostgreSQL for plant=%s: %s",
            plant_code_id,
            exc,
        )

    # ---------------------------------------------------------
    # 2. FALL BACK TO P&ID PARQUET
    # ---------------------------------------------------------

    if not pid_out:
        log.warning(
            "[alerts] No PostgreSQL equipment_pid identifiers and "
            "no P&ID output directory supplied."
        )
        return []

    candidates = [
        _fs.path_join(
            pid_out,
            "entities",
            "equipment_pid.parquet",
        ),
        _fs.path_join(
            pid_out,
            "equipment_pid.parquet",
        ),
    ]

    for path in candidates:
        try:
            if not _fs.exists(path):
                continue

            df = _fs.read_parquet(path)

            if df is None or df.empty:
                continue

            if "equipment_id" not in df.columns:
                continue

            for value in df["equipment_id"].tolist():
                _add(value)

            if equipment_ids:
                log.info(
                    "[alerts] Loaded %d canonical equipment_pid IDs "
                    "from P&ID output %s",
                    len(equipment_ids),
                    path,
                )

                return equipment_ids

        except Exception as exc:
            log.warning(
                "[alerts] Could not load equipment_pid from %s: %s",
                path,
                exc,
            )

    log.warning(
        "[alerts] No usable equipment_pid equipment IDs found. "
        "ALERTS equipment_id will remain unresolved."
    )

    return []

def _load_alerts_upload_context(
    plant_code_id: str,
    upload_batch_id: str | None,
    file_name: str,
) -> tuple[str, list[str]]:
    """Resolve site + upload labels from flow_state for this ALERTS file/batch."""

    try:
        from p0.api.services import flow as _flow

        rows = _flow.list_flow(
            plant_code_id,
            "alerts",
            limit=100000,
        )

        wanted_file = _leaf(file_name).lower()

        candidates = [
            row
            for row in rows
            if _leaf(row.get("file_name")).lower() == wanted_file
            and (
                not upload_batch_id
                or row.get("upload_batch_id") == upload_batch_id
            )
        ]

        if not candidates:
            candidates = [
                row
                for row in rows
                if _leaf(row.get("file_name")).lower() == wanted_file
            ]

        if not candidates:
            log.warning(
                "[alerts] no flow_state row found for "
                "plant=%s batch=%s file=%s; "
                "using site='-' and labels=[]",
                plant_code_id,
                upload_batch_id or "(none)",
                file_name,
            )
            return "-", []

        row = candidates[0]

        site = (
            str(row.get("site") or "-").strip()
            or "-"
        )

        raw_labels = row.get("labels") or []

        if isinstance(raw_labels, str):
            try:
                raw_labels = json.loads(raw_labels)
            except Exception:
                raw_labels = [raw_labels]

        if not isinstance(
            raw_labels,
            (list, tuple, set),
        ):
            raw_labels = [raw_labels]

        labels: list[str] = []
        seen: set[str] = set()

        for value in raw_labels:
            label = (
                str(value or "")
                .strip()
                .lower()
            )

            if not label or label in seen:
                continue

            seen.add(label)
            labels.append(label)

        log.info(
            "[alerts] upload context resolved: "
            "plant=%s batch=%s file=%s site=%s labels=%s",
            plant_code_id,
            upload_batch_id or "(none)",
            file_name,
            site,
            labels,
        )

        return site, labels

    except Exception as exc:
        log.warning(
            "[alerts] failed to resolve site/labels "
            "from flow_state: %s; "
            "using site='-' and labels=[]",
            exc,
        )

        return "-", []

def _persist_alerts_rows(
    *,
    plant_code_id: str,
    db_rows: list[dict],
) -> int:
    """
    Transactionally replace logical ALERTS rows.

    source_record_id is the logical identity and does not contain
    upload_batch_id, so re-uploading the same workbook under another
    batch replaces the existing records rather than duplicating them.
    """

    from p0.api.database.connection import _get_conn

    expected_rows = len(db_rows)

    if expected_rows == 0:
        return 0

    source_record_ids = [
        str(
            row.get("source_record_id")
            or ""
        ).strip()
        for row in db_rows
    ]

    if any(
        not source_record_id
        for source_record_id in source_record_ids
    ):
        raise RuntimeError(
            "ALERTS database persistence aborted because "
            "one or more rows have no source_record_id"
        )

    if len(set(source_record_ids)) != expected_rows:
        raise RuntimeError(
            "ALERTS database persistence aborted because "
            "the incoming workbook contains duplicate "
            "source_record_id values"
        )

    conn = _get_conn(
        plant_code_id
    )

    rows_written = 0

    try:
        cur = conn.cursor()

        cur.execute(
            """
            DELETE FROM alerts
            WHERE plant_code_id = %s
              AND source_record_id = ANY(%s)
            """,
            (
                plant_code_id,
                source_record_ids,
            ),
        )

        rows_written = _write_to_db(
            "alerts",
            db_rows,
            plant_code_id=plant_code_id,
            conn=conn,
            strict=True,
        )

        if rows_written != expected_rows:
            raise RuntimeError(
                "ALERTS database write incomplete: "
                f"expected {expected_rows} row(s), "
                f"but wrote {rows_written}"
            )

        conn.commit()

        return rows_written

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


def main() -> None:
    
    parser = argparse.ArgumentParser(
        description="ALERTS structured end-to-end pipeline"
    )

    parser.add_argument(
        "--config_dir",
        required=True,
        help="Path to p0 config directory",
    )

    parser.add_argument(
        "--alerts_in",
        required=True,
        help="Plant-scoped ALERTS staging directory",
    )

    parser.add_argument(
        "--alerts_file",
        required=True,
        help="Specific ALERTS workbook filename to process",
    )

    parser.add_argument(
        "--processed_out",
        required=True,
        help="Plant-scoped processed ALERTS directory",
    )

    parser.add_argument(
        "--work_dir",
        required=True,
        help="Local temporary working directory",
    )

    parser.add_argument(
        "--out_dir",
        required=True,
        help="Plant-scoped ALERTS canonical output directory",
    )

    parser.add_argument(
        "--plant_code_id",
        required=True,
        help="Plant code ID",
    )
    
    parser.add_argument(
    "--persist",
    action="store_true",
    help="Persist validated ALERTS rows to plant PostgreSQL",
)

    parser.add_argument(
        "--upload_batch_id",
        required=False,
        default="",
        help="Upload batch lineage identifier",
    )

    parser.add_argument(
        "--pid_out",
        required=False,
        default=None,
        help=(
            "P&ID output directory used for "
            "equipment_id resolution"
        ),
    )

    parser.add_argument(
        "--alerts_config",
        required=False,
        default=None,
        help="Optional path to alerts_sources.yaml",
    )

    args = parser.parse_args()
    load_secrets()

    file_name = _leaf(args.alerts_file)

    # ---------------------------------------------------------
    # VALIDATE
    # ---------------------------------------------------------

    _progress(
        phase="validate",
        label="Inspecting ALERTS workbook",
        status="running",
    )

    if not file_name:
        raise ValueError(
            "alerts_file is required"
        )

    if Path(file_name).suffix.lower() not in {
        ".xlsx",
        ".xls",
    }:
        raise ValueError(
            "ALERTS supports only .xlsx or .xls files"
        )

    source_path = _resolve_source_path(
        args.alerts_in,
        file_name,
    )

    if not _fs.exists(source_path):
        raise FileNotFoundError(
            f"ALERTS source file not found: {source_path}"
        )

    _progress(
        phase="validate",
        label="Inspecting ALERTS workbook",
        status="succeeded",
    )

    # ---------------------------------------------------------
    # PREPARE LOCAL WORKBOOK
    # ---------------------------------------------------------

    os.makedirs(
        args.work_dir,
        exist_ok=True,
    )

    _progress(
        phase="extract",
        label="Reading ALERTS workbook",
        status="running",
        items_done=0,
        items_total=1,
        current_item=file_name,
    )

    local_file = _copy_to_local(
        source_path,
        args.work_dir,
        file_name,
    )

    log.info(
        "Reading staged ALERTS file: %s",
        source_path,
    )

    log.info(
        "Copied ALERTS workbook locally: %s",
        local_file,
    )

    # ---------------------------------------------------------
    # LOAD OPTIONAL P&ID EQUIPMENT
    # ---------------------------------------------------------

    pnid_equipment_ids = _load_pnid_equipment_ids(
        args.pid_out,
        args.plant_code_id,
    )

    # ---------------------------------------------------------
    # RESOLVE ALERTS CONFIG
    # ---------------------------------------------------------

    alerts_config = args.alerts_config

    if not alerts_config:
        candidate = (
            Path(args.config_dir)
            / "templates"
            / "_common"
            / "alerts_sources.yaml"
        )

        if candidate.exists():
            alerts_config = str(candidate)

    # ---------------------------------------------------------
    # PROCESS BOTH ALERT SHEETS
    # ---------------------------------------------------------

    try:
        df_alerts = process_alerts_workbook(
            local_file,
            plant_code_id=args.plant_code_id,
            upload_batch_id=args.upload_batch_id,
            source_file=file_name,
            config_file=alerts_config,
            pnid_equipment_ids=pnid_equipment_ids,
        )

    except Exception as exc:
        _file_marker(
            file_name,
            status="failed",
            rows=0,
            error=str(exc),
        )

        _progress(
            phase="extract",
            label="Reading ALERTS workbook",
            status="failed",
            items_done=0,
            items_total=1,
            current_item=file_name,
        )

        raise

    if df_alerts.empty:
        _file_marker(
            file_name,
            status="failed",
            rows=0,
            error="ALERTS processing returned no rows",
        )

        raise ValueError(
            "ALERTS processing completed but returned no rows"
        )

    # ---------------------------------------------------------
    # VERIFY SOURCE IDENTITIES
    # ---------------------------------------------------------

    if "source_record_id" not in df_alerts.columns:
        raise RuntimeError(
            "ALERTS processing did not produce source_record_id. "
            "Update alerts_processing.py before running the pipeline."
        )

    source_ids = (
        df_alerts["source_record_id"]
        .fillna("")
        .astype(str)
        .str.strip()
    )

    missing_source_ids = int(
        source_ids.eq("").sum()
    )

    if missing_source_ids:
        raise RuntimeError(
            "ALERTS source identity generation incomplete: "
            f"{missing_source_ids} row(s) have no source_record_id"
        )

    duplicate_source_ids = int(
        source_ids.duplicated().sum()
    )

    if duplicate_source_ids:
        raise RuntimeError(
            "ALERTS processing produced duplicate "
            "source_record_id values inside the incoming workbook: "
            f"{duplicate_source_ids} duplicate row(s)"
        )

    # ---------------------------------------------------------
    # STAMP UPLOAD CONTEXT
    # ---------------------------------------------------------

    site, labels = _load_alerts_upload_context(
        args.plant_code_id,
        args.upload_batch_id or None,
        file_name,
    )

    df_alerts["site"] = site

    df_alerts["labels"] = [
        list(labels)
        for _ in range(len(df_alerts))
    ]

    _file_marker(
        file_name,
        status="processed",
        rows=len(df_alerts),
    )

    _progress(
        phase="extract",
        label="Reading ALERTS workbook",
        status="succeeded",
        items_done=1,
        items_total=1,
        current_item=file_name,
    )

    # ---------------------------------------------------------
    # LOAD / WRITE OUTPUT
    # ---------------------------------------------------------

    _progress(
        phase="load",
        label="Writing processed ALERTS data",
        status="running",
    )

    processed_path = _fs.path_join(
        args.processed_out,
        "alerts.parquet",
    )

    canonical_path = _fs.path_join(
        args.out_dir,
        "entities",
        "alerts.parquet",
    )

    _fs.write_parquet(
        df_alerts,
        processed_path,
    )

    _fs.write_parquet(
        df_alerts,
        canonical_path,
    )

    # ---------------------------------------------------------
    # PERSIST CANONICAL ALERTS TO PLANT-SCOPED POSTGRESQL
    # ---------------------------------------------------------

    db_frame = (
        df_alerts
        .astype(object)
        .where(
            pd.notna(df_alerts),
            None,
        )
    )

    db_rows = db_frame.to_dict(
        orient="records",
    )

    for row in db_rows:
        row["plant_code_id"] = (
            args.plant_code_id
        )

        row["site"] = (
            str(
                row.get("site")
                or "-"
            ).strip()
            or "-"
        )

        row["labels"] = (
            row.get("labels")
            or []
        )

        if args.upload_batch_id:
            row["upload_batch_id"] = (
                args.upload_batch_id
            )

    # ---------------------------------------------------------
    # USE TESTED TRANSACTIONAL PERSISTENCE HELPER
    # ---------------------------------------------------------
    
    rows_written = 0

    if args.persist:
        rows_written = _persist_alerts_rows(
        plant_code_id=args.plant_code_id,
        db_rows=db_rows,
    )

        log.info(
        "[alerts] PostgreSQL load complete: "
        "plant=%s batch=%s rows=%d",
        args.plant_code_id,
        args.upload_batch_id or "(none)",
        rows_written,
    )

        print(
        f"alerts_db rows: {rows_written}",
        flush=True,
    )

    else:
        log.info(
        "[alerts] VALIDATION MODE — PostgreSQL persistence skipped"
    )

    print(
        "alerts_db: SKIPPED (validation mode)",
        flush=True,
    )

    # rows_written = _persist_alerts_rows(
    #     plant_code_id=args.plant_code_id,
    #     db_rows=db_rows,
    # )

    # log.info(
    #     "[alerts] PostgreSQL load complete: "
    #     "plant=%s batch=%s rows=%d",
    #     args.plant_code_id,
    #     args.upload_batch_id or "(none)",
    #     rows_written,
    # )

    # print(
    #     f"alerts_db rows: {rows_written}",
    #     flush=True,
    # )

    log.info(
        "Wrote %d ALERTS rows → %s",
        len(df_alerts),
        processed_path,
    )

    log.info(
        "Wrote canonical ALERTS rows → %s",
        canonical_path,
    )

    _progress(
        phase="load",
        label="Writing processed ALERTS data",
        status="succeeded",
    )

    # ---------------------------------------------------------
    # SUMMARY
    # ---------------------------------------------------------

    open_count = int(
        (
            df_alerts["alert_status"]
            == "open"
        ).sum()
    )

    closed_count = int(
        (
            df_alerts["alert_status"]
            == "closed"
        ).sum()
    )

    matched_count = int(
        df_alerts[
            "equipment_id"
        ]
        .notna()
        .sum()
    )

    print(
        f"alerts rows: {len(df_alerts)}",
        flush=True,
    )

    print(
        f"open_alerts rows: {open_count}",
        flush=True,
    )

    print(
        f"closed_alerts rows: {closed_count}",
        flush=True,
    )

    print(
        f"equipment_matches rows: {matched_count}",
        flush=True,
    )

    _file_marker(
        file_name,
        status="completed",
        rows=len(df_alerts),
    )

    _progress(
        phase="complete",
        label="ALERTS processing complete",
        status="completed",
    )

    log.info(
        "ALERTS PIPELINE COMPLETE "
        "plant=%s file=%s rows=%d "
        "open=%d closed=%d matched=%d",
        args.plant_code_id,
        file_name,
        len(df_alerts),
        open_count,
        closed_count,
        matched_count,
    )

if __name__ == "__main__":
    main()