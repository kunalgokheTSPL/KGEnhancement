"""
Connector endpoints — P&ID and Document file upload + cloud/OneDrive sync.

All connector jobs run in background threads and are tracked via an
in-memory job registry.  Poll ``GET /connectors/jobs/{job_id}`` for status.
"""

from __future__ import annotations

import json as std_json
import io
import logging
import os
import re
import tempfile
import threading
import uuid
from datetime import datetime, timezone
from http import HTTPStatus
from pathlib import Path

from fastapi import (
    APIRouter,
    File,
    Form,
    HTTPException,
    Query,
    Request,
    UploadFile,
)
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, RedirectResponse

from p0.driver import (
    get_object_fs,
    object_store_container,
    storage_backend,
)

from p0.utils import fs as _fs

from ..audit_context import get_actor, spawn_with_context
from ..services import audit as _audit
from ..services import connector_state as _connector_state
from ..services import file_metadata as _file_metadata
from ..services import findings as _findings
from ..services import labels as _labels
from ..services import other_files as _other_files  
from ..services import upload_manifest as _manifest
from ..services import object_metadata as _object_metadata
from ..services import taxonomy as _taxonomy
from ..services import flow as _flow
from ..config import (
    COMMON_DIR,
    DOCS_CONFIG,
    RUSTFS_ENDPOINT,
    SAP_CONFIG,
    TS_CONFIG,
    rustfs_staging_docs,
    rustfs_staging_findings,
    rustfs_staging_pnid,
    rustfs_staging_sap,
    rustfs_staging_ts,
    rustfs_staging_alerts,
)
from ..deps import load_yaml
from ..plants import validate_plant_code as _validate_plant_code
from ..responses import EnvelopeRoute, as_envelope_exc, server_error, human_message, upload_outcome
from ..schemas.connectors import (
    CloudConnectorRequest,
    DocOneDriveRequest,
    OneDriveConnectorRequest,
)
from ..services.doc_types import (
    _get_s3fs,
    _persist_doc_type_config,
)
from ..services.connector_jobs import (
    _ensure_rustfs_bucket,
    _run_connector_bg,
    _upload_jobs,
)
from ..validation import (
    _build_cloud_override,
    _validate_cloud_connector_request,
    _validate_doc_onedrive_request,
    _validate_onedrive_connector_request,
)
from ..database.timeseries import (
    _enforce_ts_retention,
    _ingest_csv_chunked,
    _ingest_one_ts_file,
)










router = APIRouter(route_class=EnvelopeRoute, prefix="/connectors", tags=["Connectors"])

_log = logging.getLogger("cdm.connector")
_pnid_log = logging.getLogger("cdm.connector.pnid")
_docs_log = logging.getLogger("cdm.connector.docs")
_sap_log = logging.getLogger("cdm.connector.sap")
_ts_log = logging.getLogger(
    "cdm.connector.timeseries"
)


ALLOWED_PNID_EXT = {
    ".pdf",
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".bmp",
    ".tif",
    ".tiff",
    ".svg",
    ".webp",
    ".xlsx",
    ".csv",
}
MAX_PNID_SIZE = 50 * 1024 * 1024

ALLOWED_DOC_EXT = {
    ".pdf",
    ".docx",
    ".xlsx",
    ".xlsb",
    ".xls",
    ".csv",
    ".tsv",
    ".parquet",
    ".pptx",
    ".png",
    ".jpg",
    ".jpeg",
    ".tif",
    ".tiff",
    ".svg",
}
MAX_DOC_SIZE = 100 * 1024 * 1024

ALLOWED_SAP_EXT = {".csv", ".xlsx", ".xls", ".json", ".xml", ".parquet"}
MAX_SAP_SIZE = 500 * 1024 * 1024

# SAP table detection (matched_by: filename/column_content/unrecognized) lives in
# p0.utils.sap_table_detect so the run_sap_end_to_end.py pipeline can reuse the same
# logic without importing this (FastAPI) router module.
from p0.utils.sap_table_detect import detect_sap_table as _detect_sap_table

ALLOWED_TS_EXT = {".csv", ".xlsx", ".xls", ".tsv", ".parquet"}
MAX_TS_SIZE = 1000 * 1024 * 1024

ALLOWED_FINDINGS_EXT = {".csv", ".xlsx", ".xls"}
MAX_FINDINGS_SIZE = 200 * 1024 * 1024

ALLOWED_ALERTS_EXT = {".xlsx", ".xls"}
MAX_ALERTS_SIZE = 200 * 1024 * 1024

_FINDINGS_LABEL_HELP = (
    "Use-case labels for the file — repeat the field once per label. These are the four "
    "fixed use-case labels (integrity, reliability, maintenance, production), NOT document "
    "types and NOT free text. At least one is required on every connector except P&ID. "
    "GET /connectors/labels returns the vocabulary and what to pre-tick. A single "
    "comma-separated value is also accepted."
)























































@router.on_event("startup")
def register_validation_handler():
    from ..main import router

    if not hasattr(router, "exception_handler"):
        return

    @router.exception_handler(RequestValidationError)
    def validation_exception_handler(
        request: Request,
        exc: RequestValidationError,
    ):
        error = exc.errors()[0]

        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail={
                "success": False,
                "message": error["msg"].replace("Value error, ", ""),
                "data": None,
            },
        )




@router.get(
    "/pnid/existing",
    summary="List P&ID files already in RustFS staging",
    description="Returns filenames of P&ID PDFs/images already uploaded to "
    "RustFS staging for the given plant_code_id. Used by the frontend "
    "to detect duplicates before a new upload.",
)
def listExistingPnidFiles(
    plant_code_id: str = Query(..., description="Plant code ID"),
):
    """List the P&ID files already staged in RustFS for the given plant."""

    errors = []
    plant_code_id_val = (plant_code_id or "").strip()
    if not plant_code_id_val:
        errors.append({
            "field": "plant_code_id",
            "message": "plant_code_id is required"
        })

    if errors:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": errors
            }
        )

    plant_code_id = plant_code_id_val

    try:
        _validate_plant_code(plant_code_id)
        _ensure_rustfs_bucket("pnid/existing")
        sfs = get_object_fs()
        prefix = _fs.strip_scheme(rustfs_staging_pnid(plant_code_id))
        _pnid_log.debug("[pnid/existing] GET — listing files under s3://%s", prefix)

        files = []
        for path in sfs.glob(f"{prefix}/*"):
            if not sfs.isdir(path):
                files.append(Path(path).name)

        return {
            "success": True,
            "message": "Existing P&ID files listed successfully",
            "data": {"files": files},
        }

    except HTTPException as e:
        raise as_envelope_exc(e)
    except Exception as exc:
        return server_error(exc, action="listing existing pnid files")


@router.post(
    "/pnid/upload",
    summary="Upload P&ID files from device",
    description="Upload P&ID PDF/image files → staging, then push to RustFS via connector. "
    "Returns a `job_id` to poll progress. plant_code_id must be registered "
    "via POST /api/config/plants first.",
)
async def uploadPnidFiles(
    plant_code_id: str = Form(...),
    files: list[UploadFile] = File(...),
):

    errors = []
    plant_code_id_val = (plant_code_id or "").strip()
    if not plant_code_id_val:
        errors.append({
            "field": "plant_code_id",
            "message": "plant_code_id is required"
        })
    if not files:
        errors.append({
            "field": "files",
            "message": "At least one file is required"
        })

    if errors:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": errors
            }
        )

    plant_code_id = plant_code_id_val

    try:
        _validate_plant_code(plant_code_id)
        job_id = str(uuid.uuid4())
        upload_batch_id = str(uuid.uuid4())
        file_statuses: list[dict] = []
        _pnid_log.info(
            "[pnid/upload] POST — %d file(s) received  job_id=%s  plant_code_id=%s",
            len(files),
            job_id,
            plant_code_id,
        )

        _ensure_rustfs_bucket("pnid/upload")
        try:
            sfs = get_object_fs()

            prefix = _fs.strip_scheme(rustfs_staging_pnid(plant_code_id))
        except Exception as exc:
            _pnid_log.error("[pnid/upload] Cannot connect to RustFS: %s", exc)
            raise HTTPException(
                status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
                detail=f"RustFS connection failed: {exc}",
            )

        for f in files:
            ext = Path(f.filename or "").suffix.lower()
            if ext not in ALLOWED_PNID_EXT:
                _pnid_log.debug(
                    "[pnid/upload] REJECTED %s — extension '%s' not allowed",
                    f.filename,
                    ext,
                )
                file_statuses.append(
                    {
                        "name": f.filename,
                        "size": 0,
                        "status": "rejected",
                        "error": f"Type {ext} not allowed",
                    }
                )
                continue
            detail = await f.read()
            if len(detail) > MAX_PNID_SIZE:
                _pnid_log.debug(
                    "[pnid/upload] REJECTED %s — size %d B exceeds 50 MB limit",
                    f.filename,
                    len(detail),
                )
                file_statuses.append(
                    {
                        "name": f.filename,
                        "size": len(detail),
                        "status": "rejected",
                        "error": "File exceeds 50MB",
                    }
                )
                continue
            try:
                dest_path = f"{prefix}/{f.filename}"
                with sfs.open(dest_path, "wb") as fout:
                    fout.write(detail)
                _pnid_log.debug(
                    "[pnid/upload] uploaded %s → RustFS:%s  (%d B)",
                    f.filename,
                    dest_path,
                    len(detail),
                )
                file_status = {
                    "name": f.filename,
                    "size": len(detail),
                    "status": "uploaded",
                }
                file_statuses.append(file_status)
                try:
                    _meta = _file_metadata.inspect(f.filename, detail)
                    _dupe = _flow.find_duplicates(
                        plant_code_id,
                        "pnid",
                        file_name=f.filename,
                        size_bytes=len(detail),
                        content_hash=_flow.hash_bytes(detail, f.filename),
                        category="pnid",
                    )
                    file_status["metadata"] = _meta
                    file_status["duplicate_of"] = _dupe.get("duplicate_of")
                    file_status["is_duplicate"] = _dupe.get("is_duplicate")
                    file_status["duplicate_matched_on"] = _dupe.get("matched_on")
                    flow_row = _flow.upsert_file(
                        plant_code_id,
                        "pnid",
                        _flow.hash_bytes(detail, f.filename),
                        file_name=f.filename,
                        file_type="pnid",
                        size_bytes=len(detail),
                        stage="staged",
                        status="done",
                        upload_job_id=job_id,
                        upload_batch_id=upload_batch_id,
                        rustfs_path=dest_path,
                        error=None,
                        category="pnid",
                        uploaded_by=get_actor(),
                        duplicate_of=_dupe.get("duplicate_of"),
                        **_file_metadata.flow_fields(_meta),
                    )
                    if flow_row is None:
                        raise RuntimeError(
                            "the file could not be recorded in flow_state"
                        )
                    if flow_row:
                        file_status["flow_uid"] = flow_row.get("flow_uid")
                        file_status["stage"] = flow_row.get("stage")
                        file_status["content_hash"] = flow_row.get("content_hash")
                        _object_metadata.record(
                            prefix,
                            plant_code_id=plant_code_id,
                            connector="pnid",
                            category="pnid",
                            file_name=f.filename,
                            object_path=dest_path,
                            size_bytes=len(detail),
                            content_hash=_flow.hash_bytes(detail, f.filename),
                            content_type=_meta.get("content_type"),
                            upload_job_id=job_id,
                            upload_batch_id=upload_batch_id,
                            flow_uid=(flow_row or {}).get("flow_uid"),
                            uploaded_by=get_actor(),
                            metadata=_meta,
                        )
                except Exception as exc:
                    _log.error(
                        "[pnid/upload] flow_state write failed for %s: %s", f.filename, exc
                    )
                    file_status["status"] = "rejected"
                    file_status["error"] = human_message(
                        exc, action="recording the uploaded file", plant_code_id=plant_code_id
                    )
            except Exception as exc:
                _pnid_log.error(
                    "[pnid/upload] failed to write %s to RustFS: %s", f.filename, exc
                )
                file_statuses.append(
                    {
                        "name": f.filename,
                        "size": len(detail),
                        "status": "rejected",
                        "error": human_message(exc, action="storing the uploaded file"),
                    }
                )

        accepted = [s for s in file_statuses if s["status"] == "uploaded"]
        rejected = [s for s in file_statuses if s["status"] == "rejected"]
        _pnid_log.info(
            "[pnid/upload] accepted=%d  rejected=%d  RustFS=s3://%s",
            len(accepted),
            len(rejected),
            prefix,
        )

        try:
            _audit.record_event(
                "upload",
                target_type="file",
                plant_code_id=plant_code_id,
                source="pnid",
                target=", ".join(s["name"] for s in accepted[:5]) or None,
                status=("ok" if accepted else "failed"),
                detail={
                    "accepted": len(accepted),
                    "rejected": len(rejected),
                    "job_id": job_id,
                },
            )
        except Exception as exc:
            _log.warning("[pnid/upload] audit record_event failed: %s", exc)

        _upload_jobs[job_id] = {
            "status": "completed",
            "source": "device",
            "data_type": "pnid",
            "plant_code_id": plant_code_id,
            "files": file_statuses,
            "created": datetime.now(timezone.utc).isoformat(),
        }

        res = {
            "job_id": job_id,
            "upload_batch_id": upload_batch_id,
            "plant_code_id": plant_code_id,
            "files": file_statuses,
        }
        return upload_outcome(res, noun="P&ID files")
    except HTTPException as e:
        raise as_envelope_exc(e)
    except Exception as exc:
        return server_error(exc, action="listing existing pnid files")




async def _validate_alerts_bocpp(
    file: UploadFile,
) -> dict:
    """Validate ALERTS equipment matches against decisionops_bocpp without writes."""

    import pandas as pd
    import psycopg2

    from utility.secret_manager import load_secrets
    from p0.source_processing.alerts.alerts_processing import (
        DEFAULT_COLUMN_MAPPING,
        DEFAULT_SHEETS,
        resolve_equipment_id,
    )

    file_name = (file.filename or "").strip()
    ext = Path(file_name).suffix.lower()

    if ext not in ALLOWED_ALERTS_EXT:
        raise ValueError(
            f"Type {ext} not allowed. ALERTS accepts .xlsx or .xls files."
        )

    payload = await file.read()

    if not payload:
        raise ValueError(f"{file_name}: uploaded file is empty")

    if len(payload) > MAX_ALERTS_SIZE:
        raise ValueError("File exceeds 200MB")

    load_secrets()

    required_env = (
        "POSTGRES_HOST",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
    )
    missing = [name for name in required_env if not os.environ.get(name)]
    if missing:
        raise RuntimeError(
            "Missing PostgreSQL environment variables: " + ", ".join(missing)
        )

    conn = psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname="decisionops_bocpp",
        connect_timeout=10,
    )

    try:
        conn.set_session(readonly=True, autocommit=True)

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT equipment_id
                FROM equipment
                WHERE equipment_id IS NOT NULL
                  AND BTRIM(equipment_id::text) <> ''
                """
            )
            equipment_ids = [
                str(row[0]).strip()
                for row in cur.fetchall()
                if str(row[0] or "").strip()
            ]
    finally:
        conn.close()

    workbook = pd.ExcelFile(io.BytesIO(payload))
    frames: list[pd.DataFrame] = []

    for sheet_name, alert_status in DEFAULT_SHEETS.items():
        if sheet_name not in workbook.sheet_names:
            continue

        frame = pd.read_excel(
            io.BytesIO(payload),
            sheet_name=sheet_name,
            dtype=object,
        )

        frame.columns = [str(column).strip() for column in frame.columns]

        rename_map = {
            source: target
            for source, target in DEFAULT_COLUMN_MAPPING.items()
            if source in frame.columns
        }
        frame = frame.rename(columns=rename_map)

        expected_columns = list(DEFAULT_COLUMN_MAPPING.values())

        for column in expected_columns:
            if column not in frame.columns:
                frame[column] = None

        frame = frame[expected_columns].copy()
        frame = frame.replace(r"^\\s*$", pd.NA, regex=True)
        frame = frame.dropna(how="all", subset=expected_columns)

        if frame.empty:
            continue

        frame["alert_status"] = alert_status
        frame["source_sheet"] = sheet_name
        frames.append(frame)

    if not frames:
        raise ValueError(
            "No ALERTS rows found in Open Alerts or Closed Alerts sheets."
        )

    result = pd.concat(frames, ignore_index=True)

    identity_columns = [
        "source_index",
        "tag_number",
        "date_open",
    ]

    identity_present = result[identity_columns].apply(
        lambda column: (
            column.notna()
            & column.astype("string").str.strip().ne("")
        )
    ).any(axis=1)

    skipped_identityless = int((~identity_present).sum())
    result = result.loc[identity_present].copy()

    resolutions = [
        resolve_equipment_id(
            tag_number=row.get("tag_number"),
            description=row.get("description"),
            pnid_equipment_ids=equipment_ids,
        )
        for row in result.to_dict(orient="records")
    ]

    resolution_frame = pd.DataFrame(
        resolutions,
        index=result.index,
    )

    for column in (
        "equipment_id",
        "normalized_asset",
        "asset_match_method",
        "asset_match_confidence",
    ):
        result[column] = resolution_frame[column]

    matched_mask = (
        result["equipment_id"]
        .astype("string")
        .fillna("")
        .str.strip()
        .ne("")
    )

    matched = result.loc[matched_mask]
    unmatched = result.loc[~matched_mask]
    total = len(result)

    def _safe(value):
        try:
            if pd.isna(value):
                return None
        except Exception:
            pass
        if value is None:
            return None
        return str(value)

    matched_rows = [
        {
            "source_index": _safe(row.get("source_index")),
            "tag_number": _safe(row.get("tag_number")),
            "equipment_id": _safe(row.get("equipment_id")),
            "asset_match_method": row.get("asset_match_method"),
            "asset_match_confidence": row.get("asset_match_confidence"),
            "alert_status": row.get("alert_status"),
            "title": _safe(row.get("title")),
        }
        for row in matched.to_dict(orient="records")
    ]

    unmatched_rows = [
        {
            "source_index": _safe(row.get("source_index")),
            "tag_number": _safe(row.get("tag_number")),
            "alert_status": row.get("alert_status"),
            "title": _safe(row.get("title")),
        }
        for row in unmatched.to_dict(orient="records")
    ]

    matched_count = len(matched)

    return {
        "file_name": file_name,
        "database": "decisionops_bocpp",
        "equipment_ids_loaded": len(equipment_ids),
        "total_alerts": total,
        "matched": matched_count,
        "unmatched": len(unmatched),
        "match_percentage": (
            round(matched_count / total * 100, 2)
            if total
            else 0.0
        ),
        "identityless_rows_skipped": skipped_identityless,
        "database_write": False,
        "matched_rows": matched_rows,
        "unmatched_rows": unmatched_rows,
    }


@router.post(
    "/alerts/upload",
    summary="Upload ALERTS workbook",
    description=(
        "Upload an ALERTS Excel workbook to plant-scoped RustFS staging. "
        "When validate_only=true, validate equipment matching against "
        "decisionops_bocpp and return the result without staging or database writes."
    ),
)
async def uploadAlertsFiles(
    plant_code_id: str = Form(...),
    files: list[UploadFile] = File(...),
    labels: list[str] = Form(default=[], description=_FINDINGS_LABEL_HELP),
    files_meta: list[str] = Form(default=[], description=_manifest.HELP),
    validate_only: bool = Form(
        False,
        description=(
            "When true, validate ALERTS equipment matching against "
            "decisionops_bocpp without staging or writing to the final database."
        ),
    ),
):
    """Upload ALERTS workbooks or run read-only BOCPP equipment validation."""

    errors: list[dict] = []
    plant_code_id_val = (plant_code_id or "").strip()

    if not plant_code_id_val:
        errors.append(
            {
                "field": "plant_code_id",
                "message": "plant_code_id is required",
            }
        )

    if not files:
        errors.append(
            {
                "field": "files",
                "message": "At least one file is required",
            }
        )

    if errors:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": errors,
            },
        )

    plant_code_id = plant_code_id_val

    if validate_only:
        if len(files) != 1:
            return JSONResponse(
                status_code=HTTPStatus.BAD_REQUEST,
                content={
                    "success": False,
                    "message": "Validation failed",
                    "errors": [
                        {
                            "field": "files",
                            "message": (
                                "Exactly one ALERTS workbook is required "
                                "when validate_only=true."
                            ),
                        }
                    ],
                },
            )

        try:
            validation = await _validate_alerts_bocpp(files[0])

            return {
                "success": True,
                "message": (
                    "ALERTS equipment validation completed successfully. "
                    "No database write was performed."
                ),
                "data": {
                    "plant_code_id": plant_code_id,
                    "connector": "alerts",
                    "validation": validation,
                },
            }

        except ValueError as exc:
            return JSONResponse(
                status_code=HTTPStatus.BAD_REQUEST,
                content={
                    "success": False,
                    "message": "Validation failed",
                    "errors": [
                        {
                            "field": "files",
                            "message": str(exc),
                        }
                    ],
                },
            )
        except Exception as exc:
            return server_error(
                exc,
                action=(
                    "validating ALERTS equipment against "
                    "decisionops_bocpp"
                ),
                plant_code_id=plant_code_id,
            )

    _file_meta, label_error = _manifest.resolve(
        files_meta,
        [f.filename for f in (files or [])],
        connector="alerts",
        fallback_labels=labels,
        fallback_document_type=None,
    )

    if label_error:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": [
                    {
                        "field": "labels",
                        "message": label_error,
                    }
                ],
            },
        )

    try:
        _validate_plant_code(plant_code_id)

        job_id = str(uuid.uuid4())
        upload_batch_id = str(uuid.uuid4())
        file_statuses: list[dict] = []

        _ensure_rustfs_bucket("alerts/upload")
        sfs = get_object_fs()
        prefix = _fs.strip_scheme(
            rustfs_staging_alerts(plant_code_id)
        )

        _log.info(
            "[alerts/upload] START plant=%s job_id=%s "
            "upload_batch_id=%s files=%d prefix=s3://%s",
            plant_code_id,
            job_id,
            upload_batch_id,
            len(files),
            prefix,
        )

        for file in files:
            file_name = (file.filename or "").strip()
            ext = Path(file_name).suffix.lower()

            if ext not in ALLOWED_ALERTS_EXT:
                file_statuses.append(
                    {
                        "name": file_name,
                        "size": 0,
                        "status": "rejected",
                        "error": (
                            f"Type {ext} not allowed. "
                            "ALERTS accepts .xlsx or .xls files."
                        ),
                    }
                )
                continue

            payload = await file.read()

            if not payload:
                file_statuses.append(
                    {
                        "name": file_name,
                        "size": 0,
                        "status": "rejected",
                        "error": "Uploaded file is empty",
                    }
                )
                continue

            if len(payload) > MAX_ALERTS_SIZE:
                file_statuses.append(
                    {
                        "name": file_name,
                        "size": len(payload),
                        "status": "rejected",
                        "error": "File exceeds 200MB",
                    }
                )
                continue

            try:
                dest_path = f"{prefix}/{file_name}"

                with sfs.open(dest_path, "wb") as fout:
                    fout.write(payload)

                file_status = {
                    "name": file_name,
                    "size": len(payload),
                    "status": "uploaded",
                    "labels": _file_meta.get(
                        file_name,
                        {},
                    ).get("labels", []),
                }
                file_statuses.append(file_status)

                try:
                    metadata = _file_metadata.inspect(
                        file_name,
                        payload,
                    )
                    content_hash = _flow.hash_bytes(
                        payload,
                        file_name,
                    )

                    duplicate = _flow.find_duplicates(
                        plant_code_id,
                        "alerts",
                        file_name=file_name,
                        size_bytes=len(payload),
                        content_hash=content_hash,
                        category="alerts",
                    )

                    file_status["metadata"] = metadata
                    file_status["duplicate_of"] = duplicate.get(
                        "duplicate_of"
                    )
                    file_status["is_duplicate"] = duplicate.get(
                        "is_duplicate"
                    )
                    file_status["duplicate_matched_on"] = duplicate.get(
                        "matched_on"
                    )

                    flow_row = _flow.upsert_file(
                        plant_code_id,
                        "alerts",
                        content_hash,
                        file_name=file_name,
                        file_type="alerts",
                        size_bytes=len(payload),
                        stage="staged",
                        status="done",
                        upload_job_id=job_id,
                        upload_batch_id=upload_batch_id,
                        rustfs_path=dest_path,
                        error=None,
                        category="alerts",
                        labels=_file_meta.get(
                            file_name,
                            {},
                        ).get("labels", []),
                        uploaded_by=get_actor(),
                        duplicate_of=duplicate.get(
                            "duplicate_of"
                        ),
                        **_file_metadata.flow_fields(metadata),
                    )

                    if flow_row is None:
                        raise RuntimeError(
                            "the ALERTS file could not be recorded in flow_state"
                        )

                    file_status["flow_uid"] = flow_row.get(
                        "flow_uid"
                    )
                    file_status["stage"] = flow_row.get(
                        "stage"
                    )
                    file_status["content_hash"] = flow_row.get(
                        "content_hash"
                    )

                    _object_metadata.record(
                        prefix,
                        plant_code_id=plant_code_id,
                        connector="alerts",
                        category="alerts",
                        file_name=file_name,
                        object_path=dest_path,
                        size_bytes=len(payload),
                        content_hash=content_hash,
                        content_type=metadata.get(
                            "content_type"
                        ),
                        upload_job_id=job_id,
                        upload_batch_id=upload_batch_id,
                        flow_uid=flow_row.get(
                            "flow_uid"
                        ),
                        uploaded_by=get_actor(),
                        metadata=metadata,
                        tags=_file_meta.get(
                            file_name,
                            {},
                        ).get("labels", []),
                    )

                except Exception as exc:
                    _log.exception(
                        "[alerts/upload] metadata/flow failure file=%s",
                        file_name,
                    )
                    file_status["status"] = "rejected"
                    file_status["error"] = human_message(
                        exc,
                        action="recording the ALERTS file",
                        plant_code_id=plant_code_id,
                    )

            except Exception as exc:
                _log.exception(
                    "[alerts/upload] staging failure file=%s",
                    file_name,
                )
                file_statuses.append(
                    {
                        "name": file_name,
                        "size": len(payload),
                        "status": "rejected",
                        "error": human_message(
                            exc,
                            action="storing the ALERTS file",
                            plant_code_id=plant_code_id,
                        ),
                    }
                )

        accepted = [
            item
            for item in file_statuses
            if item.get("status") == "uploaded"
        ]
        rejected = [
            item
            for item in file_statuses
            if item.get("status") == "rejected"
        ]

        _upload_jobs[job_id] = {
            "status": "completed",
            "source": "device",
            "data_type": "alerts",
            "plant_code_id": plant_code_id,
            "files": file_statuses,
            "upload_batch_id": upload_batch_id,
            "created": datetime.now(timezone.utc).isoformat(),
        }

        try:
            _audit.record_event(
                "upload",
                target_type="file",
                plant_code_id=plant_code_id,
                source="alerts",
                target=(
                    ", ".join(
                        item["name"]
                        for item in accepted[:5]
                    )
                    or None
                ),
                status=(
                    "ok"
                    if accepted
                    else "failed"
                ),
                detail={
                    "accepted": len(accepted),
                    "rejected": len(rejected),
                    "job_id": job_id,
                    "upload_batch_id": upload_batch_id,
                },
            )
        except Exception as exc:
            _log.warning(
                "[alerts/upload] audit failed: %s",
                exc,
            )

        result = {
            "job_id": job_id,
            "upload_batch_id": upload_batch_id,
            "plant_code_id": plant_code_id,
            "connector": "alerts",
            "files": file_statuses,
        }

        return upload_outcome(
            result,
            noun="ALERTS files",
        )

    except HTTPException as exc:
        raise as_envelope_exc(exc)
    except Exception as exc:
        return server_error(
            exc,
            action="uploading ALERTS files",
            plant_code_id=plant_code_id,
        )


@router.post(
    "/pnid/connect/cloud",
    summary="Connect P&ID from cloud storage",
    description="Configure and run S3, ADLS, or GCS connector for P&ID files → RustFS.",
)
def connectPnidCloud(
    req: CloudConnectorRequest,
):

    errors = []
    plant_code_id_val = (req.plant_code_id or "").strip()
    if not plant_code_id_val:
        errors.append({"field": "plant_code_id", "message": "plant_code_id is required"})
    
    cloud_type_val = (req.cloud_type or "").strip()
    if not cloud_type_val:
        errors.append({"field": "cloud_type", "message": "cloud_type is required"})
    elif cloud_type_val not in ("s3", "adls", "gcs"):
        errors.append({"field": "cloud_type", "message": f"Invalid cloud_type '{cloud_type_val}'. Must be s3, adls, or gcs"})
    else:
        if cloud_type_val == "s3":
            bucket_val = (req.bucket or "").strip()
            if not bucket_val:
                errors.append({"field": "bucket", "message": "bucket is required for S3"})
            access_key_val = (req.access_key or "").strip()
            if not access_key_val:
                errors.append({"field": "access_key", "message": "access_key is required for S3"})
            secret_key_val = (req.secret_key or "").strip()
            if not secret_key_val:
                errors.append({"field": "secret_key", "message": "secret_key is required for S3"})
        elif cloud_type_val == "gcs":
            bucket_val = (req.bucket or "").strip()
            if not bucket_val:
                errors.append({"field": "bucket", "message": "bucket is required for GCS"})
            project_id_val = (req.project_id or "").strip()
            if not project_id_val:
                errors.append({"field": "project_id", "message": "project_id is required for GCS"})
            service_account_json_val = (req.service_account_json or "").strip()
            if not service_account_json_val:
                errors.append({"field": "service_account_json", "message": "service_account_json is required for GCS"})
        elif cloud_type_val == "adls":
            container_val = (req.container or "").strip()
            if not container_val:
                errors.append({"field": "container", "message": "container is required for ADLS"})
            account_name_val = (req.account_name or "").strip()
            if not account_name_val:
                errors.append({"field": "account_name", "message": "account_name is required for ADLS"})
            
            has_account_key = bool((req.account_key or "").strip())
            has_sas_token = bool((req.sas_token or "").strip())
            has_service_principal = bool(
                (req.tenant_id or "").strip()
                and (req.client_id or "").strip()
                and (req.client_secret or "").strip()
            )
            if not (has_account_key or has_sas_token or has_service_principal):
                errors.append({
                    "field": "credentials",
                    "message": "ADLS requires account_key, sas_token, or service principal credentials (tenant_id, client_id, and client_secret)"
                })

    if errors:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": errors
            }
        )

    # Trim the inputs on the req object
    req.plant_code_id = plant_code_id_val
    req.cloud_type = cloud_type_val
    if req.bucket is not None:
        req.bucket = req.bucket.strip()
    if req.access_key is not None:
        req.access_key = req.access_key.strip()
    if req.secret_key is not None:
        req.secret_key = req.secret_key.strip()
    if req.project_id is not None:
        req.project_id = req.project_id.strip()
    if req.service_account_json is not None:
        req.service_account_json = req.service_account_json.strip()
    if req.container is not None:
        req.container = req.container.strip()
    if req.account_name is not None:
        req.account_name = req.account_name.strip()
    if req.account_key is not None:
        req.account_key = req.account_key.strip()
    if req.sas_token is not None:
        req.sas_token = req.sas_token.strip()
    if req.tenant_id is not None:
        req.tenant_id = req.tenant_id.strip()
    if req.client_id is not None:
        req.client_id = req.client_id.strip()
    if req.client_secret is not None:
        req.client_secret = req.client_secret.strip()
    if req.prefix is not None:
        req.prefix = req.prefix.strip()
    if req.region is not None:
        req.region = req.region.strip()

    plant_code_id = req.plant_code_id

    try:
        _validate_plant_code(plant_code_id)
        job_id = str(uuid.uuid4())
        override = _build_cloud_override(req, "raw/pnid")

        _pnid_log.info(
            "[pnid/cloud] POST  cloud_type=%s  job_id=%s", req.cloud_type, job_id
        )

        _upload_jobs[job_id] = {
            "status": "queued",
            "source": req.cloud_type,
            "data_type": "pnid",
            "plant_code_id": plant_code_id,
            "files": [],
            "created": datetime.now(timezone.utc).isoformat(),
        }

        t = spawn_with_context(
            target=_run_connector_bg,
            args=(job_id, "pnid", req.cloud_type, override),
            daemon=True,
        )
        _pnid_log.info("[pnid/cloud] connector job queued  job_id=%s", job_id)
        res = {"job_id": job_id, "source": req.cloud_type}
        return {
            "success": True,
            "message": "P&ID cloud connector job started successfully",
            "data": res,
        }
    except HTTPException as e:
        raise as_envelope_exc(e)
    except Exception as exc:
        return server_error(exc, action="connecting pnid cloud")


@router.post(
    "/pnid/connect/oneDrive",
    summary="Connect P&ID from OneDrive/SharePoint",
    description="Configure and run OneDrive connector for P&ID files → RustFS.",
)
def connectPnidOnedrive(
    req: OneDriveConnectorRequest,
):

    errors = []
    plant_code_id_val = (req.plant_code_id or "").strip()
    if not plant_code_id_val:
        errors.append({"field": "plant_code_id", "message": "plant_code_id is required"})
    tenant_id_val = (req.tenant_id or "").strip()
    if not tenant_id_val:
        errors.append({"field": "tenant_id", "message": "tenant_id is required"})
    client_id_val = (req.client_id or "").strip()
    if not client_id_val:
        errors.append({"field": "client_id", "message": "client_id is required"})
    client_secret_val = (req.client_secret or "").strip()
    if not client_secret_val:
        errors.append({"field": "client_secret", "message": "client_secret is required"})
    source_path_val = (req.source_path or "").strip()
    if not source_path_val:
        errors.append({"field": "source_path", "message": "source_path is required"})

    if errors:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": errors
            }
        )

    # Trim the inputs on the req object
    req.plant_code_id = plant_code_id_val
    req.tenant_id = tenant_id_val
    req.client_id = client_id_val
    req.client_secret = client_secret_val
    req.source_path = source_path_val
    if req.drive_id is not None:
        req.drive_id = req.drive_id.strip()
    if req.site_id is not None:
        req.site_id = req.site_id.strip()

    plant_code_id = req.plant_code_id

    try:
        _validate_plant_code(plant_code_id)
        job_id = str(uuid.uuid4())
        _pnid_log.info(
            "[pnid/onedrive] POST  source_path=%s  job_id=%s", req.source_path, job_id
        )

        override = {
            "folder_path": req.source_path,
            "drive_id": req.drive_id or "",
            "site_id": req.site_id or "",
            "connection": {
                "tenant_id": req.tenant_id,
                "client_id": req.client_id,
                "client_secret": req.client_secret,
            },
        }

        _upload_jobs[job_id] = {
            "status": "queued",
            "source": "onedrive",
            "data_type": "pnid",
            "plant_code_id": plant_code_id,
            "files": [],
            "created": datetime.now(timezone.utc).isoformat(),
        }

        t = spawn_with_context(
            target=_run_connector_bg,
            args=(job_id, "pnid", "onedrive", override),
            daemon=True,
        )
        _pnid_log.info("[pnid/onedrive] connector job queued  job_id=%s", job_id)
        res = {"job_id": job_id, "source": "onedrive"}
        return {
            "success": True,
            "message": "P&ID OneDrive connector job started successfully",
            "data": res,
        }
    except HTTPException as e:
        raise as_envelope_exc(e)
    except Exception as exc:
        return server_error(exc, action="connecting pnid onedrive")










@router.post(
    "/documents/upload",
    summary="Upload document files from device",
    description=(
        "Upload document files directly to RustFS staging. "
        "For normal document types, `document_type` identifies the configured "
        "document category. For `document_type=others`, `document_type_label` "
        "is the user-defined type. The backend determines structured vs "
        "unstructured mode from the actual uploaded file."
    ),
)
async def uploadDocumentFiles(
    plant_code_id: str = Form(...),
    document_type: str = Form(default=""),
    files: list[UploadFile] = File(...),
    source_columns: str | None = Form(None),
    document_type_label: str | None = Form(None),
    data_mode: str | None = Form(default=None),
    equipment_column: str | None = Form(default=None),
    labels: list[str] = Form(default=[], description=_FINDINGS_LABEL_HELP),
    files_meta: list[str] = Form(default=[], description=_manifest.HELP),
):
    errors: list[dict] = []

    plant_code_id_val = (plant_code_id or "").strip()
    document_type_val = (document_type or "").strip()
    document_type_label_val = (document_type_label or "").strip()

    if not plant_code_id_val:
        errors.append(
            {
                "field": "plant_code_id",
                "message": "plant_code_id is required",
            }
        )

    if not files:
        errors.append(
            {
                "field": "files",
                "message": "At least one file is required",
            }
        )

    _file_meta, label_error = _manifest.resolve(
        files_meta,
        [f.filename for f in (files or [])],
        connector="documents",
        fallback_labels=labels,
        fallback_document_type=document_type,
    )

    chosen_labels = _labels.normalise(
        [
            label
            for meta in _file_meta.values()
            for label in meta.get("labels", [])
        ]
    )

    if label_error:
        errors.append(
            {
                "field": "labels",
                "message": label_error,
            }
        )

    is_other_files = document_type_val.lower() == "others"
    user_defined_type: str | None = None

    if is_other_files:
        user_defined_type = _other_files.normalize_user_type(
            document_type_label_val
        )
        if not user_defined_type:
            errors.append(
                {
                    "field": "document_type_label",
                    "message": (
                        "document_type_label is required for Other Files "
                        "and must contain at least one letter or number"
                    ),
                }
            )
    else:
        _untyped = [
            name
            for name, meta in _file_meta.items()
            if not meta.get("document_type")
        ]
        if _untyped and not document_type_val:
            errors.append(
                {
                    "field": "document_type",
                    "message": (
                        "document_type is required — send it once for the "
                        "whole upload, or give every file its own in "
                        f"{_manifest.FIELD}. Missing: "
                        + ", ".join(_untyped[:5])
                    ),
                }
            )

    client_mode = (data_mode or "").strip().lower()
    if client_mode and client_mode not in {"structured", "unstructured"}:
        errors.append(
            {
                "field": "data_mode",
                "message": (
                    "data_mode must be either 'structured' or 'unstructured'"
                ),
            }
        )

    if errors:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": errors,
            },
        )

    plant_code_id = plant_code_id_val
    document_type = document_type_val

    source_columns = (
        source_columns.strip()
        if source_columns is not None
        else None
    )
    document_type_label = (
        document_type_label.strip()
        if document_type_label is not None
        else None
    )
    equipment_column = (
        equipment_column.strip()
        if equipment_column is not None
        else None
    )

    parsed_source_columns: list[str] = []
    if source_columns:
        try:
            parsed = std_json.loads(source_columns)
            if isinstance(parsed, list):
                parsed_source_columns = [
                    str(value).strip()
                    for value in parsed
                    if str(value).strip()
                ]
            elif isinstance(parsed, str) and parsed.strip():
                parsed_source_columns = [parsed.strip()]
            else:
                raise ValueError(
                    "source_columns must be a JSON list or string"
                )
        except (std_json.JSONDecodeError, ValueError, TypeError):
            parsed_source_columns = [
                value.strip()
                for value in source_columns.split(",")
                if value.strip()
            ]

    try:
        _validate_plant_code(plant_code_id)

        def _safe_type_for(name: str) -> str:
            raw = (
                _file_meta.get(name, {}).get("document_type")
                or document_type
                or ""
            ).strip()
            return re.sub(
                r"[^a-z0-9_-]",
                "_",
                raw.lower(),
            )

        if is_other_files:
            _persist_doc_type_config(
                user_defined_type or "others",
                document_type_label_val,
                (
                    std_json.dumps(parsed_source_columns)
                    if parsed_source_columns
                    else None
                ),
            )
        else:
            for _dt in sorted(
                _manifest.document_types(_file_meta)
                or ({document_type} if document_type else set())
            ):
                _persist_doc_type_config(
                    re.sub(
                        r"[^a-z0-9_-]",
                        "_",
                        _dt.lower().strip(),
                    ),
                    document_type_label or _dt,
                    source_columns,
                )

        job_id = str(uuid.uuid4())
        upload_batch_id = str(uuid.uuid4())
        file_statuses: list[dict] = []

        _ensure_rustfs_bucket("documents/upload")
        sfs = _get_s3fs()
        docs_root = rustfs_staging_docs(
            plant_code_id
        ).replace("s3://", "")

        _docs_log.info(
            "[docs/upload] START  job_id=%s  doc_types=%s  files=%d  rustfs=s3://%s",
            job_id,
            (
                f"others/{user_defined_type}"
                if is_other_files
                else ",".join(
                    sorted(
                        {
                            _safe_type_for(f.filename or "")
                            for f in files
                        }
                    )
                )
            ),
            len(files),
            docs_root,
        )

        for f in files:
            file_name = (f.filename or "").strip()
            ext = Path(file_name).suffix.lower()

            if ext not in ALLOWED_DOC_EXT:
                file_statuses.append(
                    {
                        "name": file_name,
                        "size": 0,
                        "status": "rejected",
                        "error": f"Type {ext} not allowed",
                    }
                )
                continue

            detail = await f.read()

            if len(detail) > MAX_DOC_SIZE:
                file_statuses.append(
                    {
                        "name": file_name,
                        "size": len(detail),
                        "status": "rejected",
                        "error": "File exceeds 100MB",
                    }
                )
                continue

            if not detail:
                file_statuses.append(
                    {
                        "name": file_name,
                        "size": 0,
                        "status": "rejected",
                        "error": "Uploaded file is empty",
                    }
                )
                continue

            other_file_info: dict | None = None
            file_source_columns: list[str] = []

            if is_other_files:
                try:
                    other_file_info = _other_files.inspect_other_file(
                        file_name=file_name,
                        payload=detail,
                        user_defined_type=document_type_label_val,
                    )
                except ValueError as exc:
                    file_statuses.append(
                        {
                            "name": file_name,
                            "size": len(detail),
                            "status": "rejected",
                            "error": str(exc),
                        }
                    )
                    continue

                detected_mode = str(
                    other_file_info.get("data_mode") or ""
                ).strip().lower()

                # Backend detection is authoritative. A supplied client mode
                # may only confirm it, never override it.
                if client_mode and client_mode != detected_mode:
                    file_statuses.append(
                        {
                            "name": file_name,
                            "size": len(detail),
                            "status": "rejected",
                            "error": (
                                f"data_mode '{client_mode}' does not match "
                                f"backend-detected mode '{detected_mode}'"
                            ),
                        }
                    )
                    continue

                if detected_mode == "unstructured":
                    file_source_columns = list(parsed_source_columns)
                    if not file_source_columns:
                        file_statuses.append(
                            {
                                "name": file_name,
                                "size": len(detail),
                                "status": "rejected",
                                "error": (
                                    "source_columns are required for "
                                    "unstructured Other Files"
                                ),
                            }
                        )
                        continue

            try:
                safe_type = _safe_type_for(file_name)

                if is_other_files:
                    dest_path = (
                        f"{docs_root}/others/"
                        f"{user_defined_type}/{file_name}"
                    )
                else:
                    dest_path = (
                        f"{docs_root}/{safe_type}/{file_name}"
                    )

                with sfs.open(dest_path, "wb") as fout:
                    fout.write(detail)

                _docs_log.info(
                    "[docs/upload] UPLOADED %s (%.1f KB) → s3://%s",
                    file_name,
                    len(detail) / 1024,
                    dest_path,
                )

                file_status = {
                    "name": file_name,
                    "size": len(detail),
                    "status": "uploaded",
                    "document_type": (
                        f"others/{user_defined_type}"
                        if is_other_files
                        else safe_type
                    ),
                    "labels": _file_meta.get(
                        file_name,
                        {},
                    ).get("labels", []),
                }

                if is_other_files and other_file_info:
                    detected_equipment_column = (
                        equipment_column
                        or other_file_info.get("equipment_column")
                    )
                    file_status.update(
                        {
                            "user_defined_type": user_defined_type,
                            "document_type_label": document_type_label_val,
                            "data_mode": other_file_info.get("data_mode"),
                            "requires_source_columns": other_file_info.get(
                                "requires_source_columns",
                                False,
                            ),
                            "source_columns": (
                                file_source_columns
                                if other_file_info.get("data_mode")
                                == "unstructured"
                                else []
                            ),
                            "detected_columns": (
                                other_file_info.get(
                                    "normalized_columns",
                                    [],
                                )
                                if other_file_info.get("data_mode")
                                == "structured"
                                else []
                            ),
                            "equipment_column": detected_equipment_column,
                            "equipment_mapping_required": (
                                other_file_info.get(
                                    "equipment_mapping_required",
                                    False,
                                )
                            ),
                            "preserve_all_columns": other_file_info.get(
                                "preserve_all_columns",
                                True,
                            ),
                        }
                    )

                file_statuses.append(file_status)

                try:
                    _meta = _file_metadata.inspect(
                        file_name,
                        detail,
                    )

                    category = (
                        f"others/{user_defined_type}"
                        if is_other_files
                        else safe_type
                    )

                    if is_other_files and other_file_info:
                        _meta.update(
                            {
                                "category": "others",
                                "user_defined_type": user_defined_type,
                                "document_type_label": document_type_label_val,
                                "data_mode": other_file_info.get("data_mode"),
                                "source_columns": (
                                    file_source_columns
                                    if other_file_info.get("data_mode")
                                    == "unstructured"
                                    else []
                                ),
                                "detected_columns": (
                                    other_file_info.get(
                                        "normalized_columns",
                                        [],
                                    )
                                    if other_file_info.get("data_mode")
                                    == "structured"
                                    else []
                                ),
                                "source_column_names": (
                                    other_file_info.get("columns", [])
                                    if other_file_info.get("data_mode")
                                    == "structured"
                                    else []
                                ),
                                "equipment_column": (
                                    equipment_column
                                    or other_file_info.get(
                                        "equipment_column"
                                    )
                                ),
                                "equipment_mapping_required": (
                                    other_file_info.get(
                                        "equipment_mapping_required",
                                        False,
                                    )
                                ),
                                "preserve_all_columns": (
                                    other_file_info.get(
                                        "preserve_all_columns",
                                        True,
                                    )
                                ),
                            }
                        )

                    _hash = _flow.hash_bytes(
                        detail,
                        file_name,
                        salt=category,
                    )

                    _dupe = _flow.find_duplicates(
                        plant_code_id,
                        "docs",
                        file_name=file_name,
                        size_bytes=len(detail),
                        content_hash=_hash,
                        category=category,
                    )

                    file_status["metadata"] = _meta
                    file_status["duplicate_of"] = _dupe.get(
                        "duplicate_of"
                    )
                    file_status["is_duplicate"] = _dupe.get(
                        "is_duplicate"
                    )
                    file_status["duplicate_matched_on"] = _dupe.get(
                        "matched_on"
                    )

                    flow_row = _flow.upsert_file(
                        plant_code_id,
                        "docs",
                        _hash,
                        file_name=file_name,
                        file_type=(
                            user_defined_type
                            if is_other_files
                            else safe_type
                        ),
                        size_bytes=len(detail),
                        stage="staged",
                        status="done",
                        upload_job_id=job_id,
                        upload_batch_id=upload_batch_id,
                        rustfs_path=dest_path,
                        error=None,
                        category=category,
                        labels=_file_meta.get(
                            file_name,
                            {},
                        ).get("labels", []),
                        uploaded_by=get_actor(),
                        duplicate_of=_dupe.get("duplicate_of"),
                        **_file_metadata.flow_fields(_meta),
                    )

                    if flow_row is None:
                        raise RuntimeError(
                            "the file could not be recorded in flow_state"
                        )

                    file_status["flow_uid"] = flow_row.get(
                        "flow_uid"
                    )
                    file_status["stage"] = flow_row.get(
                        "stage"
                    )
                    file_status["content_hash"] = flow_row.get(
                        "content_hash"
                    )

                    _object_metadata.record(
                        (
                            f"{docs_root}/others/{user_defined_type}"
                            if is_other_files
                            else f"{docs_root}/{safe_type}"
                        ),
                        plant_code_id=plant_code_id,
                        connector="documents",
                        category=category,
                        file_name=file_name,
                        object_path=dest_path,
                        size_bytes=len(detail),
                        content_hash=_hash,
                        content_type=_meta.get("content_type"),
                        upload_job_id=job_id,
                        upload_batch_id=upload_batch_id,
                        flow_uid=flow_row.get("flow_uid"),
                        uploaded_by=get_actor(),
                        metadata=_meta,
                        tags=_file_meta.get(
                            file_name,
                            {},
                        ).get("labels", []),
                    )

                except Exception as exc:
                    _log.error(
                        "[docs/upload] flow_state write failed for %s: %s",
                        file_name,
                        exc,
                    )
                    file_status["status"] = "rejected"
                    file_status["error"] = human_message(
                        exc,
                        action="recording the uploaded file",
                        plant_code_id=plant_code_id,
                    )

            except Exception as exc:
                _docs_log.error(
                    "[docs/upload] failed to write %s to RustFS: %s",
                    file_name,
                    exc,
                )
                file_statuses.append(
                    {
                        "name": file_name,
                        "size": len(detail),
                        "status": "rejected",
                        "error": human_message(
                            exc,
                            action="storing the uploaded file",
                        ),
                    }
                )

        accepted = [
            status
            for status in file_statuses
            if status["status"] == "uploaded"
        ]
        rejected = [
            status
            for status in file_statuses
            if status["status"] == "rejected"
        ]

        _docs_log.info(
            "[docs/upload] SUMMARY  job_id=%s  accepted=%d  rejected=%d  rustfs=s3://%s",
            job_id,
            len(accepted),
            len(rejected),
            docs_root,
        )

        try:
            _audit.record_event(
                "upload",
                target_type="file",
                plant_code_id=plant_code_id,
                source="docs",
                target=", ".join(
                    status["name"]
                    for status in accepted[:5]
                ) or None,
                status=("ok" if accepted else "failed"),
                detail={
                    "document_types": (
                        [f"others/{user_defined_type}"]
                        if is_other_files
                        else sorted(
                            {
                                _safe_type_for(x.filename or "")
                                for x in files
                            }
                        )
                    ),
                    "accepted": len(accepted),
                    "rejected": len(rejected),
                    "job_id": job_id,
                },
            )
        except Exception as exc:
            _log.warning(
                "[docs/upload] audit record_event failed: %s",
                exc,
            )

        document_types = (
            [f"others/{user_defined_type}"]
            if is_other_files
            else sorted(
                {
                    _safe_type_for(x.filename or "")
                    for x in files
                }
            )
        )

        _upload_jobs[job_id] = {
            "status": "completed",
            "source": "device",
            "data_type": "documents",
            "plant_code_id": plant_code_id,
            "document_type": (
                "others"
                if is_other_files
                else document_type or None
            ),
            "document_types": document_types,
            "user_defined_type": (
                user_defined_type
                if is_other_files
                else None
            ),
            "files": file_statuses,
            "created": datetime.now(
                timezone.utc
            ).isoformat(),
        }

        res = {
            "job_id": job_id,
            "upload_batch_id": upload_batch_id,
            "plant_code_id": plant_code_id,
            "document_type": (
                "others"
                if is_other_files
                else document_type or None
            ),
            "document_types": document_types,
            "user_defined_type": (
                user_defined_type
                if is_other_files
                else None
            ),
            "document_type_label": (
                document_type_label_val
                if is_other_files
                else document_type_label
            ),
            "labels": chosen_labels,
            "files": file_statuses,
        }

        return upload_outcome(
            res,
            noun="documents",
        )

    except HTTPException as exc:
        raise as_envelope_exc(exc)
    except Exception as exc:
        return server_error(
            exc,
            action="uploading documents",
            plant_code_id=plant_code_id,
        )


@router.post(
    "/documents/connect/cloud",
    summary="Connect documents from cloud storage",
    description="Configure and run cloud connector for document files → RustFS.",
)
def connectDocumentsCloud(
    req: CloudConnectorRequest,
    document_type: str = "",
):

    errors = []
    plant_code_id_val = (req.plant_code_id or "").strip()
    if not plant_code_id_val:
        errors.append({"field": "plant_code_id", "message": "plant_code_id is required"})
    
    cloud_type_val = (req.cloud_type or "").strip()
    if not cloud_type_val:
        errors.append({"field": "cloud_type", "message": "cloud_type is required"})
    elif cloud_type_val not in ("s3", "adls", "gcs"):
        errors.append({"field": "cloud_type", "message": f"Invalid cloud_type '{cloud_type_val}'. Must be s3, adls, or gcs"})
    else:
        if cloud_type_val == "s3":
            bucket_val = (req.bucket or "").strip()
            if not bucket_val:
                errors.append({"field": "bucket", "message": "bucket is required for S3"})
            access_key_val = (req.access_key or "").strip()
            if not access_key_val:
                errors.append({"field": "access_key", "message": "access_key is required for S3"})
            secret_key_val = (req.secret_key or "").strip()
            if not secret_key_val:
                errors.append({"field": "secret_key", "message": "secret_key is required for S3"})
        elif cloud_type_val == "gcs":
            bucket_val = (req.bucket or "").strip()
            if not bucket_val:
                errors.append({"field": "bucket", "message": "bucket is required for GCS"})
            project_id_val = (req.project_id or "").strip()
            if not project_id_val:
                errors.append({"field": "project_id", "message": "project_id is required for GCS"})
            service_account_json_val = (req.service_account_json or "").strip()
            if not service_account_json_val:
                errors.append({"field": "service_account_json", "message": "service_account_json is required for GCS"})
        elif cloud_type_val == "adls":
            container_val = (req.container or "").strip()
            if not container_val:
                errors.append({"field": "container", "message": "container is required for ADLS"})
            account_name_val = (req.account_name or "").strip()
            if not account_name_val:
                errors.append({"field": "account_name", "message": "account_name is required for ADLS"})
            
            has_account_key = bool((req.account_key or "").strip())
            has_sas_token = bool((req.sas_token or "").strip())
            has_service_principal = bool(
                (req.tenant_id or "").strip()
                and (req.client_id or "").strip()
                and (req.client_secret or "").strip()
            )
            if not (has_account_key or has_sas_token or has_service_principal):
                errors.append({
                    "field": "credentials",
                    "message": "ADLS requires account_key, sas_token, or service principal credentials (tenant_id, client_id, and client_secret)"
                })

    if errors:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": errors
            }
        )

    # Trim the inputs on the req object
    req.plant_code_id = plant_code_id_val
    req.cloud_type = cloud_type_val
    if req.bucket is not None:
        req.bucket = req.bucket.strip()
    if req.access_key is not None:
        req.access_key = req.access_key.strip()
    if req.secret_key is not None:
        req.secret_key = req.secret_key.strip()
    if req.project_id is not None:
        req.project_id = req.project_id.strip()
    if req.service_account_json is not None:
        req.service_account_json = req.service_account_json.strip()
    if req.container is not None:
        req.container = req.container.strip()
    if req.account_name is not None:
        req.account_name = req.account_name.strip()
    if req.account_key is not None:
        req.account_key = req.account_key.strip()
    if req.sas_token is not None:
        req.sas_token = req.sas_token.strip()
    if req.tenant_id is not None:
        req.tenant_id = req.tenant_id.strip()
    if req.client_id is not None:
        req.client_id = req.client_id.strip()
    if req.client_secret is not None:
        req.client_secret = req.client_secret.strip()
    if req.prefix is not None:
        req.prefix = req.prefix.strip()
    if req.region is not None:
        req.region = req.region.strip()

    plant_code_id = req.plant_code_id

    try:
        _validate_plant_code(plant_code_id)
        safe_type = re.sub(
            r"[^a-z0-9_-]", "_", (document_type or "general").lower().strip()
        )
        job_id = str(uuid.uuid4())
        override = _build_cloud_override(req, f"raw/documents/{safe_type}")

        _docs_log.info(
            "[docs/cloud] POST  cloud_type=%s  doc_type=%s  job_id=%s",
            req.cloud_type,
            safe_type,
            job_id,
        )

        _upload_jobs[job_id] = {
            "status": "queued",
            "source": req.cloud_type,
            "data_type": "documents",
            "plant_code_id": plant_code_id,
            "document_type": safe_type,
            "files": [],
            "created": datetime.now(timezone.utc).isoformat(),
        }

        t = spawn_with_context(
            target=_run_connector_bg,
            args=(job_id, "documents", req.cloud_type, override, DOCS_CONFIG),
            daemon=True,
        )

        _docs_log.info("[docs/cloud] connector job queued  job_id=%s", job_id)
        res = {"job_id": job_id, "source": req.cloud_type, "document_type": safe_type}
        return {
            "success": True,
            "message": "Documents cloud connector job started successfully",
            "data": res,
        }
    except HTTPException as e:
        raise as_envelope_exc(e)
    except Exception as exc:
        return server_error(exc, action="connecting documents cloud")


@router.post(
    "/documents/connect/oneDrive",
    summary="Connect documents from OneDrive/SharePoint",
    description="Configure and run OneDrive connector for document files → RustFS.",
)
def connectDocumentsOnedrive(
    req: DocOneDriveRequest,
):

    errors = []
    plant_code_id_val = (req.plant_code_id or "").strip()
    if not plant_code_id_val:
        errors.append({"field": "plant_code_id", "message": "plant_code_id is required"})
    tenant_id_val = (req.tenant_id or "").strip()
    if not tenant_id_val:
        errors.append({"field": "tenant_id", "message": "tenant_id is required"})
    client_id_val = (req.client_id or "").strip()
    if not client_id_val:
        errors.append({"field": "client_id", "message": "client_id is required"})
    client_secret_val = (req.client_secret or "").strip()
    if not client_secret_val:
        errors.append({"field": "client_secret", "message": "client_secret is required"})
    source_path_val = (req.source_path or "").strip()
    if not source_path_val:
        errors.append({"field": "source_path", "message": "source_path is required"})
    document_type_val = (req.document_type or "").strip()
    if not document_type_val:
        errors.append({"field": "document_type", "message": "document_type is required"})

    if errors:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": errors
            }
        )

    # Trim the inputs on the req object
    req.plant_code_id = plant_code_id_val
    req.tenant_id = tenant_id_val
    req.client_id = client_id_val
    req.client_secret = client_secret_val
    req.source_path = source_path_val
    req.document_type = document_type_val
    if req.drive_id is not None:
        req.drive_id = req.drive_id.strip()
    if req.site_id is not None:
        req.site_id = req.site_id.strip()

    plant_code_id = req.plant_code_id

    try:
        _validate_plant_code(plant_code_id)
        safe_type = re.sub(r"[^a-z0-9_-]", "_", req.document_type.lower().strip())
        job_id = str(uuid.uuid4())

        _docs_log.info(
            "[docs/onedrive] POST  doc_type=%s  source_path=%s  job_id=%s",
            safe_type,
            req.source_path,
            job_id,
        )

        override = {
            "folder_path": req.source_path,
            "drive_id": req.drive_id or "",
            "site_id": req.site_id or "",
            "connection": {
                "tenant_id": req.tenant_id,
                "client_id": req.client_id,
                "client_secret": req.client_secret,
            },
        }

        _upload_jobs[job_id] = {
            "status": "queued",
            "source": "onedrive",
            "data_type": "documents",
            "plant_code_id": plant_code_id,
            "document_type": safe_type,
            "files": [],
            "created": datetime.now(timezone.utc).isoformat(),
        }

        t = spawn_with_context(
            target=_run_connector_bg,
            args=(job_id, "documents", "onedrive", override, DOCS_CONFIG),
            daemon=True,
        )

        _docs_log.info("[docs/onedrive] connector job queued  job_id=%s", job_id)
        res = {"job_id": job_id, "source": "onedrive", "document_type": safe_type}
        return {
            "success": True,
            "message": "Documents OneDrive connector job started successfully",
            "data": res,
        }
    except HTTPException as e:
        raise as_envelope_exc(e)
    except Exception as exc:
        return server_error(exc, action="connecting documents onedrive")




async def _do_sap_upload(
    plant_code_id: str,
    files: list[UploadFile],
) -> tuple[str, list[dict]]:
    """Upload SAP files to RustFS staging and track in flow_state. Returns (job_id, file_statuses)."""
    job_id = str(uuid.uuid4())
    file_statuses: list[dict] = []

    _ensure_rustfs_bucket("sap/upload")
    sfs = _get_s3fs()
    prefix = _fs.strip_scheme(rustfs_staging_sap(plant_code_id))

    _sap_log.info(
        "[sap/upload] %d file(s) received  job_id=%s  rustfs=s3://%s",
        len(files),
        job_id,
        prefix,
    )

    for f in files:
        ext = Path(f.filename or "").suffix.lower()
        if ext not in ALLOWED_SAP_EXT:
            _sap_log.warning(
                "[sap/upload] REJECTED %s — extension %s not allowed",
                f.filename,
                ext,
            )
            file_statuses.append(
                {
                    "name": f.filename,
                    "size": 0,
                    "status": "rejected",
                    "error": f"Type {ext} not allowed",
                }
            )
            continue
        detail = await f.read()
        if len(detail) > MAX_SAP_SIZE:
            _sap_log.warning(
                "[sap/upload] REJECTED %s — size %d B exceeds 500MB",
                f.filename,
                len(detail),
            )
            file_statuses.append(
                {
                    "name": f.filename,
                    "size": len(detail),
                    "status": "rejected",
                    "error": "File exceeds 500MB",
                }
            )
            continue
        try:
            dest_path = f"{prefix}/{f.filename}"
            with sfs.open(dest_path, "wb") as fout:
                fout.write(detail)
            _sap_log.info(
                "[sap/upload] UPLOADED %s (%.1f KB) → s3://%s",
                f.filename,
                len(detail) / 1024,
                dest_path,
            )
            table_match = _detect_sap_table(f.filename, detail, ext)
            _sap_log.info(
                "[sap/upload] TABLE DETECTION %s → table=%s  by=%s  score=%.2f",
                f.filename,
                table_match["detected_table"],
                table_match["matched_by"],
                table_match["match_score"],
            )
            file_status: dict = {
                "name": f.filename,
                "size": len(detail),
                "status": "uploaded",
                "matched_by": table_match["matched_by"],
                "detected_table": table_match["detected_table"],
            }
            if table_match["matched_by"] != "filename":
                file_status["match_score"] = table_match["match_score"]
            file_statuses.append(file_status)
            try:
                flow_row = _flow.upsert_file(
                    plant_code_id,
                    "sap",
                    _flow.hash_bytes(detail, f.filename),
                    file_name=f.filename,
                    file_type="sap",
                    size_bytes=len(detail),
                    stage="staged",
                    status="done",
                    upload_job_id=job_id,
                    rustfs_path=dest_path,
                    error=None,
                )
                if flow_row:
                    file_status["flow_uid"] = flow_row.get("flow_uid")
                    file_status["stage"] = flow_row.get("stage")
                    file_status["content_hash"] = flow_row.get("content_hash")
            except Exception as exc:
                _log.warning("[sap/upload] flow_state write failed: %s", exc)
        except Exception as exc:
            _sap_log.error(
                "[sap/upload] failed to write %s to RustFS: %s", f.filename, exc
            )
            file_statuses.append(
                {
                    "name": f.filename,
                    "size": len(detail),
                    "status": "rejected",
                    "error": str(exc),
                }
            )

    accepted = [s for s in file_statuses if s["status"] == "uploaded"]
    rejected = [s for s in file_statuses if s["status"] == "rejected"]
    _sap_log.info(
        "[sap/upload] SUMMARY  job_id=%s  accepted=%d  rejected=%d  rustfs=s3://%s",
        job_id,
        len(accepted),
        len(rejected),
        prefix,
    )

    _upload_jobs[job_id] = {
        "status": "completed",
        "source": "device",
        "data_type": "sap",
        "plant_code_id": plant_code_id,
        "files": file_statuses,
        "created": datetime.now(timezone.utc).isoformat(),
    }
    return job_id, file_statuses


@router.post(
    "/sap/upload",
    summary="Upload SAP table files from device",
    description="Upload SAP table files (CSV, XLSX, JSON, XML, Parquet) → staging, "
    "then push to RustFS via connector. One file per SAP table (e.g. AUFK.csv, EQUI.xlsx). "
    "Returns a `job_id` to poll progress.",
)
async def uploadSapFiles(
    plant_code_id: str = Form(...),
    files: list[UploadFile] = File(...),
    labels: list[str] = Form(default=[], description=_FINDINGS_LABEL_HELP),
    files_meta: list[str] = Form(default=[], description=_manifest.HELP),
):

    errors = []
    plant_code_id_val = (plant_code_id or "").strip()
    if not plant_code_id_val:
        errors.append({"field": "plant_code_id", "message": "plant_code_id is required"})
    if not files:
        errors.append({"field": "files", "message": "At least one file is required"})
    _file_meta, label_error = _manifest.resolve(
        files_meta,
        [f.filename for f in (files or [])],
        connector="sap",
        fallback_labels=labels,
        fallback_document_type=None,
    )
    chosen_labels = _labels.normalise(
        [l for m in _file_meta.values() for l in m["labels"]]
    )
    if label_error:
        errors.append({"field": "labels", "message": label_error})

    if errors:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": errors
            }
        )

    plant_code_id = plant_code_id_val

    try:
        _validate_plant_code(plant_code_id)
        job_id = str(uuid.uuid4())
        upload_batch_id = str(uuid.uuid4())
        file_statuses: list[dict] = []

        _ensure_rustfs_bucket("sap/upload")

        sfs = _get_s3fs()
        prefix = _fs.strip_scheme(rustfs_staging_sap(plant_code_id))

        _sap_log.info(
            "[sap/upload] POST — %d file(s) received  job_id=%s  rustfs=s3://%s",
            len(files),
            job_id,
            prefix,
        )

        for f in files:
            ext = Path(f.filename or "").suffix.lower()
            if ext not in ALLOWED_SAP_EXT:
                _sap_log.warning(
                    "[sap/upload] REJECTED %s — extension %s not allowed",
                    f.filename,
                    ext,
                )
                file_statuses.append(
                    {
                        "name": f.filename,
                        "size": 0,
                        "status": "rejected",
                        "error": f"Type {ext} not allowed",
                    }
                )
                continue
            detail = await f.read()
            if len(detail) > MAX_SAP_SIZE:
                _sap_log.warning(
                    "[sap/upload] REJECTED %s — size %d B exceeds 500MB",
                    f.filename,
                    len(detail),
                )
                file_statuses.append(
                    {
                        "name": f.filename,
                        "size": len(detail),
                        "status": "rejected",
                        "error": "File exceeds 500MB",
                    }
                )
                continue
            try:
                dest_path = f"{prefix}/{f.filename}"
                with sfs.open(dest_path, "wb") as fout:
                    fout.write(detail)
                _sap_log.info(
                    "[sap/upload] UPLOADED %s (%.1f KB) → s3://%s",
                    f.filename,
                    len(detail) / 1024,
                    dest_path,
                )
                table_match = _detect_sap_table(f.filename, detail, ext)
                _sap_log.info(
                    "[sap/upload] TABLE DETECTION %s → table=%s  by=%s  score=%.2f",
                    f.filename,
                    table_match["detected_table"],
                    table_match["matched_by"],
                    table_match["match_score"],
                )
                file_status = {
                    "name": f.filename,
                    "size": len(detail),
                    "status": "uploaded",
                    "labels": _file_meta.get(f.filename, {}).get("labels", []),
                    "matched_by": table_match["matched_by"],
                    "detected_table": table_match["detected_table"],
                }
                if table_match["matched_by"] != "filename":
                    file_status["match_score"] = table_match["match_score"]
                file_statuses.append(file_status)
                try:
                    _meta = _file_metadata.inspect(f.filename, detail)
                    _dupe = _flow.find_duplicates(
                        plant_code_id,
                        "sap",
                        file_name=f.filename,
                        size_bytes=len(detail),
                        content_hash=_flow.hash_bytes(detail, f.filename),
                        category="sap",
                    )
                    file_status["metadata"] = _meta
                    file_status["duplicate_of"] = _dupe.get("duplicate_of")
                    file_status["is_duplicate"] = _dupe.get("is_duplicate")
                    file_status["duplicate_matched_on"] = _dupe.get("matched_on")
                    flow_row = _flow.upsert_file(
                        plant_code_id,
                        "sap",
                        _flow.hash_bytes(detail, f.filename),
                        file_name=f.filename,
                        file_type="sap",
                        size_bytes=len(detail),
                        stage="staged",
                        status="done",
                        upload_job_id=job_id,
                        upload_batch_id=upload_batch_id,
                        rustfs_path=dest_path,
                        error=None,
                        category="sap",
                        labels=_file_meta.get(f.filename, {}).get("labels", []),
                        uploaded_by=get_actor(),
                        duplicate_of=_dupe.get("duplicate_of"),
                        **_file_metadata.flow_fields(_meta),
                    )
                    if flow_row is None:
                        raise RuntimeError(
                            "the file could not be recorded in flow_state"
                        )
                    if flow_row:
                        file_status["flow_uid"] = flow_row.get("flow_uid")
                        file_status["stage"] = flow_row.get("stage")
                        file_status["content_hash"] = flow_row.get("content_hash")
                        _object_metadata.record(
                            prefix,
                            plant_code_id=plant_code_id,
                            connector="sap",
                            category="sap",
                            file_name=f.filename,
                            object_path=dest_path,
                            size_bytes=len(detail),
                            content_hash=_flow.hash_bytes(detail, f.filename),
                            content_type=_meta.get("content_type"),
                            upload_job_id=job_id,
                            upload_batch_id=upload_batch_id,
                            flow_uid=(flow_row or {}).get("flow_uid"),
                            uploaded_by=get_actor(),
                            metadata=_meta,
                            tags=_file_meta.get(f.filename, {}).get("labels", []),
                        )
                except Exception as exc:
                    _log.error(
                        "[sap/upload] flow_state write failed for %s: %s", f.filename, exc
                    )
                    file_status["status"] = "rejected"
                    file_status["error"] = human_message(
                        exc, action="recording the uploaded file", plant_code_id=plant_code_id
                    )
            except Exception as exc:
                _sap_log.error(
                    "[sap/upload] failed to write %s to RustFS: %s", f.filename, exc
                )
                file_statuses.append(
                    {
                        "name": f.filename,
                        "size": len(detail),
                        "status": "rejected",
                        "error": human_message(exc, action="storing the uploaded file"),
                    }
                )

        accepted = [s for s in file_statuses if s["status"] == "uploaded"]
        rejected = [s for s in file_statuses if s["status"] == "rejected"]
        _sap_log.info(
            "[sap/upload] SUMMARY  job_id=%s  accepted=%d  rejected=%d  rustfs=s3://%s",
            job_id,
            len(accepted),
            len(rejected),
            prefix,
        )

        _upload_jobs[job_id] = {
            "status": "completed",
            "source": "device",
            "data_type": "sap",
            "plant_code_id": plant_code_id,
            "files": file_statuses,
            "created": datetime.now(timezone.utc).isoformat(),
        }
        res = {
            "job_id": job_id,
            "upload_batch_id": upload_batch_id,
            "plant_code_id": plant_code_id,
            "files": file_statuses,
        }
        return upload_outcome(res, noun="SAP files")
    except HTTPException as e:
        raise as_envelope_exc(e)
    except Exception as exc:
        return server_error(exc, action="connecting documents onedrive")


def _compute_sap_missing_tables(plant_code_id: str) -> dict:
    """Return per-entity missing-tables status dict, reading current flow_state."""
    joins_cfg = load_yaml(COMMON_DIR / "sap_join_graph.yaml")
    mandatory_cfg = load_yaml(COMMON_DIR / "sap_mandatory_tables.yaml")

    entities_cfg = joins_cfg.get("entities", {})
    mandatory_pipelines = mandatory_cfg.get("pipelines", {})

    uploaded_stems: set[str] = set()
    _flow_rows = _flow.list_flow(plant_code_id, "sap")
    _sap_log.warning(
        "[sap/missing-tables] list_flow returned %d row(s): %s",
        len(_flow_rows),
        [r.get("file_name") for r in _flow_rows],
    )
    for row in _flow_rows:
        file_name = str(row.get("file_name") or "").strip()
        if file_name:
            uploaded_stems.add(file_name.rsplit(".", 1)[0].upper())

    entity_to_pipeline = {
        "work_order": "workorder",
        "functional_location": "floc",
        "task_list": "tasklist",
        "material": "material",
    }

    result: dict = {}
    for entity_name, entity_cfg in entities_cfg.items():
        all_sources = [str(s).strip().upper() for s in entity_cfg.get("sources", [])]
        pipeline_key = entity_to_pipeline.get(entity_name, entity_name)
        mandatory_tables = {
            t["name"].upper()
            for t in mandatory_pipelines.get(pipeline_key, {}).get("tables", [])
        }
        optional_tables = [t for t in all_sources if t not in mandatory_tables]

        result[entity_name] = {
            "required_present": sorted(t for t in mandatory_tables if t in uploaded_stems),
            "required_missing": sorted(t for t in mandatory_tables if t not in uploaded_stems),
            "optional_missing": sorted(t for t in optional_tables if t not in uploaded_stems),
            "all_present": all(t in uploaded_stems for t in mandatory_tables),
        }

    return result


@router.post(
    "/sap/missing-tables",
    summary="Check which SAP tables are missing (optionally upload files in the same call)",
    description="Returns per-entity missing-table status for the given plant. "
    "Optionally accepts one or more SAP table files via multipart upload — "
    "if files are provided they are saved to staging first, then the "
    "missing-tables status is recomputed and returned in the same response. "
    "If no files are provided the endpoint behaves exactly as the previous GET.",
    status_code=HTTPStatus.OK,
)
async def getSapMissingTables(
    plant_code_id: str = Form(..., description="Plant code ID"),
    files: list[UploadFile | str] | None = File(None),
):
    plant_code_id_val = (plant_code_id or "").strip()
    if not plant_code_id_val:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": [{"field": "plant_code_id", "message": "plant_code_id is required"}],
            },
        )

    plant_code_id = plant_code_id_val

    try:
        _validate_plant_code(plant_code_id)

        upload_result: dict | None = None
        files = [f for f in (files or []) if isinstance(f, UploadFile) and f.filename] or None
        if files:
            job_id, file_statuses = await _do_sap_upload(plant_code_id, files)
            upload_result = {"job_id": job_id, "files": file_statuses}
            _sap_log.info(
                "[sap/missing-tables] POST — uploaded %d file(s)  job_id=%s  plant=%s",
                len(files),
                job_id,
                plant_code_id,
            )

        entities = _compute_sap_missing_tables(plant_code_id)
        _sap_log.info(
            "[sap/missing-tables] POST — plant=%s  entities=%s",
            plant_code_id,
            list(entities.keys()),
        )

        data: dict = {"plant_code_id": plant_code_id, "entities": entities}
        if upload_result is not None:
            data["upload"] = upload_result

        return {
            "success": True,
            "message": "SAP missing tables check complete",
            "data": data,
        }

    except HTTPException as e:
        raise as_envelope_exc(e)
    except Exception as exc:
        return JSONResponse(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            content={
                "success": False,
                "message": "Internal server error",
                "errors": [{"field": "server", "message": str(exc)}],
            },
        )


@router.post(
    "/sap/connect/cloud",
    summary="Connect SAP data from cloud storage",
    description="Configure and run S3, ADLS, or GCS connector for SAP files → RustFS.",
)
def connectSapCloud(
    req: CloudConnectorRequest,
):

    errors = []
    plant_code_id_val = (req.plant_code_id or "").strip()
    if not plant_code_id_val:
        errors.append({"field": "plant_code_id", "message": "plant_code_id is required"})
    
    cloud_type_val = (req.cloud_type or "").strip()
    if not cloud_type_val:
        errors.append({"field": "cloud_type", "message": "cloud_type is required"})
    elif cloud_type_val not in ("s3", "adls", "gcs"):
        errors.append({"field": "cloud_type", "message": f"Invalid cloud_type '{cloud_type_val}'. Must be s3, adls, or gcs"})
    else:
        if cloud_type_val == "s3":
            bucket_val = (req.bucket or "").strip()
            if not bucket_val:
                errors.append({"field": "bucket", "message": "bucket is required for S3"})
            access_key_val = (req.access_key or "").strip()
            if not access_key_val:
                errors.append({"field": "access_key", "message": "access_key is required for S3"})
            secret_key_val = (req.secret_key or "").strip()
            if not secret_key_val:
                errors.append({"field": "secret_key", "message": "secret_key is required for S3"})
        elif cloud_type_val == "gcs":
            bucket_val = (req.bucket or "").strip()
            if not bucket_val:
                errors.append({"field": "bucket", "message": "bucket is required for GCS"})
            project_id_val = (req.project_id or "").strip()
            if not project_id_val:
                errors.append({"field": "project_id", "message": "project_id is required for GCS"})
            service_account_json_val = (req.service_account_json or "").strip()
            if not service_account_json_val:
                errors.append({"field": "service_account_json", "message": "service_account_json is required for GCS"})
        elif cloud_type_val == "adls":
            container_val = (req.container or "").strip()
            if not container_val:
                errors.append({"field": "container", "message": "container is required for ADLS"})
            account_name_val = (req.account_name or "").strip()
            if not account_name_val:
                errors.append({"field": "account_name", "message": "account_name is required for ADLS"})
            
            has_account_key = bool((req.account_key or "").strip())
            has_sas_token = bool((req.sas_token or "").strip())
            has_service_principal = bool(
                (req.tenant_id or "").strip()
                and (req.client_id or "").strip()
                and (req.client_secret or "").strip()
            )
            if not (has_account_key or has_sas_token or has_service_principal):
                errors.append({
                    "field": "credentials",
                    "message": "ADLS requires account_key, sas_token, or service principal credentials (tenant_id, client_id, and client_secret)"
                })

    if errors:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": errors
            }
        )

    # Trim the inputs on the req object
    req.plant_code_id = plant_code_id_val
    req.cloud_type = cloud_type_val
    if req.bucket is not None:
        req.bucket = req.bucket.strip()
    if req.access_key is not None:
        req.access_key = req.access_key.strip()
    if req.secret_key is not None:
        req.secret_key = req.secret_key.strip()
    if req.project_id is not None:
        req.project_id = req.project_id.strip()
    if req.service_account_json is not None:
        req.service_account_json = req.service_account_json.strip()
    if req.container is not None:
        req.container = req.container.strip()
    if req.account_name is not None:
        req.account_name = req.account_name.strip()
    if req.account_key is not None:
        req.account_key = req.account_key.strip()
    if req.sas_token is not None:
        req.sas_token = req.sas_token.strip()
    if req.tenant_id is not None:
        req.tenant_id = req.tenant_id.strip()
    if req.client_id is not None:
        req.client_id = req.client_id.strip()
    if req.client_secret is not None:
        req.client_secret = req.client_secret.strip()
    if req.prefix is not None:
        req.prefix = req.prefix.strip()
    if req.region is not None:
        req.region = req.region.strip()

    plant_code_id = req.plant_code_id

    try:
        _validate_plant_code(plant_code_id)
        job_id = str(uuid.uuid4())
        override = _build_cloud_override(req, "raw/sap")

        _sap_log.info(
            "[sap/cloud] POST  cloud_type=%s  job_id=%s", req.cloud_type, job_id
        )

        _upload_jobs[job_id] = {
            "status": "queued",
            "source": req.cloud_type,
            "data_type": "sap",
            "plant_code_id": plant_code_id,
            "files": [],
            "created": datetime.now(timezone.utc).isoformat(),
        }

        t = spawn_with_context(
            target=_run_connector_bg,
            args=(job_id, "sap", req.cloud_type, override, SAP_CONFIG),
            daemon=True,
        )
        _sap_log.info("[sap/cloud] connector job queued  job_id=%s", job_id)
        res = {"job_id": job_id, "source": req.cloud_type}
        return {
            "success": True,
            "message": "SAP cloud connector job started successfully",
            "data": res,
        }
    except HTTPException as e:
        raise as_envelope_exc(e)
    except Exception as exc:
        return server_error(exc, action="connecting sap cloud")




@router.post(
    "/timeseries/upload",
    summary="Upload timeseries files from device",
    description="Upload timeseries metadata and/or values files (CSV, XLSX, Parquet) → staging, "
    "then push to RustFS via connector. "
    "Returns a `job_id` to poll progress.",
)
async def uploadTimeseriesFiles(
    plant_code_id: str = Form(...),
    files: list[UploadFile] = File(...),
    file_type: str = Form(default="metadata"),
    labels: list[str] = Form(default=[], description=_FINDINGS_LABEL_HELP),
    files_meta: list[str] = Form(default=[], description=_manifest.HELP),
):

    errors = []
    plant_code_id_val = (plant_code_id or "").strip()
    if not plant_code_id_val:
        errors.append({"field": "plant_code_id", "message": "plant_code_id is required"})
    file_type_val = (file_type or "").strip()
    if not file_type_val:
        errors.append({"field": "file_type", "message": "file_type is required"})
    if not files:
        errors.append({"field": "files", "message": "At least one file is required"})
    _file_meta, label_error = _manifest.resolve(
        files_meta,
        [f.filename for f in (files or [])],
        connector="timeseries",
        fallback_labels=labels,
        fallback_document_type=None,
    )
    chosen_labels = _labels.normalise(
        [l for m in _file_meta.values() for l in m["labels"]]
    )
    if label_error:
        errors.append({"field": "labels", "message": label_error})

    if errors:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": errors
            }
        )

    plant_code_id = plant_code_id_val
    file_type = file_type_val

    try:
        _validate_plant_code(plant_code_id)
        safe_type = re.sub(r"[^a-z0-9_-]", "_", file_type.lower().strip())
        job_id = str(uuid.uuid4())
        upload_batch_id = str(uuid.uuid4())
        file_statuses: list[dict] = []

        _ensure_rustfs_bucket("timeseries/upload")

        sfs = _get_s3fs()
        prefix = f"{rustfs_staging_ts(plant_code_id).replace('s3://', '')}/{safe_type}"

        _ts_log.info(
            "[ts/upload] POST — %d file(s) received  job_id=%s  file_type=%s  plant_code_id=%s  rustfs=s3://%s",
            len(files),
            job_id,
            safe_type,
            plant_code_id,
            prefix,
        )

        pending: list[
            tuple
        ] = []
        _UPLOAD_CHUNK = 8 * 1024 * 1024

        for f in files:
            ext = Path(f.filename or "").suffix.lower()
            if ext not in ALLOWED_TS_EXT:
                _ts_log.warning(
                    "[ts/upload] REJECTED %s — extension %s not allowed",
                    f.filename,
                    ext,
                )
                file_statuses.append(
                    {
                        "name": f.filename,
                        "size": 0,
                        "status": "rejected",
                        "error": f"Type {ext} not allowed",
                    }
                )
                continue

            tmp = tempfile.NamedTemporaryFile(
                prefix="ts_upload_",
                suffix=ext,
                delete=False,
                dir="/tmp",
            )
            size = 0
            too_big = False
            try:
                while True:
                    blob = await f.read(_UPLOAD_CHUNK)
                    if not blob:
                        break
                    size += len(blob)
                    if size > MAX_TS_SIZE:
                        too_big = True
                        break
                    tmp.write(blob)
                tmp.flush()
                tmp.close()
            except Exception as exc:
                tmp.close()
                try:
                    os.unlink(tmp.name)
                except OSError:
                    pass
                _ts_log.error("[ts/upload] failed to spool %s: %s", f.filename, exc)
                file_statuses.append(
                    {
                        "name": f.filename,
                        "size": size,
                        "status": "rejected",
                        "error": human_message(exc, action="storing the uploaded file"),
                    }
                )
                continue

            if too_big:
                try:
                    os.unlink(tmp.name)
                except OSError:
                    pass
                _ts_log.warning("[ts/upload] REJECTED %s — exceeds 1GB", f.filename)
                file_statuses.append(
                    {
                        "name": f.filename,
                        "size": size,
                        "status": "rejected",
                        "error": "File exceeds 1GB",
                    }
                )
                continue
            is_values = (
                safe_type == "values"
                and "metadata" not in Path(f.filename or "").stem.lower()
            )
            file_status = {
                "name": f.filename,
                "size": size,
                "status": "accepted",
                "labels": _file_meta.get(f.filename, {}).get("labels", []),
            }
            file_statuses.append(file_status)
            flow_uid = None
            fhash = None
            try:
                fhash = _flow.hash_file(tmp.name, sampled=is_values)
                _ts_flow = "ts_values" if is_values else "ts"
                _dupe = _flow.find_duplicates(
                    plant_code_id,
                    _ts_flow,
                    file_name=f.filename,
                    size_bytes=size,
                    content_hash=fhash,
                    category=safe_type,
                )
                file_status["duplicate_of"] = _dupe.get("duplicate_of")
                file_status["is_duplicate"] = _dupe.get("is_duplicate")
                file_status["duplicate_matched_on"] = _dupe.get("matched_on")
                _frow = _flow.upsert_file(
                    plant_code_id,
                    _ts_flow,
                    fhash,
                    file_name=f.filename,
                    file_type=safe_type,
                    category=safe_type,
                    labels=_file_meta.get(f.filename, {}).get("labels", []),
                    uploaded_by=get_actor(),
                    duplicate_of=_dupe.get("duplicate_of"),
                    content_type=_file_metadata.content_type_for(f.filename),
                    size_bytes=size,
                    stage="uploaded",
                    status="running",
                    upload_job_id=job_id,
                    upload_batch_id=upload_batch_id,
                    error=None,
                )
                if _frow is None:
                    raise RuntimeError(
                        "the file could not be recorded in flow_state"
                    )
                flow_uid = (_frow or {}).get("flow_uid")
                if _frow:
                    file_status["flow_uid"] = flow_uid
                    file_status["stage"] = _frow.get("stage")
                    file_status["content_hash"] = _frow.get("content_hash")
            except Exception as exc:
                _log.error("[ts/upload] flow_state write failed for %s: %s", f.filename, exc)
                file_status["status"] = "rejected"
                file_status["error"] = human_message(
                    exc, action="recording the uploaded file", plant_code_id=plant_code_id
                )

            pending.append((f.filename, tmp.name, is_values, flow_uid, fhash))

        accepted = [s for s in file_statuses if s["status"] == "accepted"]
        rejected = [s for s in file_statuses if s["status"] == "rejected"]
        _ts_log.info(
            "[ts/upload] SUMMARY  job_id=%s  file_type=%s  accepted=%d  rejected=%d  (RustFS+ingest backgrounded)",
            job_id,
            safe_type,
            len(accepted),
            len(rejected),
        )
        try:
            _audit.record_event(
                "upload",
                target_type="file",
                plant_code_id=plant_code_id,
                source=("ts_values" if safe_type == "values" else "ts"),
                target=", ".join(s["name"] for s in accepted[:5]) or None,
                status=("ok" if accepted else "failed"),
                detail={
                    "file_type": safe_type,
                    "accepted": len(accepted),
                    "rejected": len(rejected),
                    "job_id": job_id,
                },
            )
        except Exception as exc:
            _log.warning("[ts/upload] audit record_event failed: %s", exc)

        _upload_jobs[job_id] = {
            "status": "completed" if not pending else "uploading",
            "source": "device",
            "data_type": "timeseries",
            "plant_code_id": plant_code_id,
            "file_type": safe_type,
            "files": file_statuses,
            "created": datetime.now(timezone.utc).isoformat(),
        }
        if pending:

            _actor = get_actor()

            def _ts_post_bg():
                total = 0
                per_file: list[dict] = []
                ingestable: list[tuple] = []
                for name, tmp_path, is_values, flow_uid, content_hash in pending:
                    try:
                        dest_path = f"{prefix}/{name}"
                        sfs.put(tmp_path, dest_path)
                        _ts_log.info(
                            "[ts/upload] UPLOADED %s → s3://%s", name, dest_path
                        )
                        for s in file_statuses:
                            if s.get("name") == name:
                                s["status"] = "uploaded"
                        if flow_uid is not None:
                            try:
                                _flow.advance(
                                    flow_uid,
                                    plant_code_id=plant_code_id,
                                    stage="staged",
                                    status="done",
                                    rustfs_path=dest_path,
                                )
                            except Exception as exc:
                                _log.warning("[ts/upload] flow_state advance failed: %s", exc)
                        try:
                            _object_metadata.record(
                                prefix,
                                plant_code_id=plant_code_id,
                                connector="timeseries",
                                category=safe_type,
                                file_name=name,
                                object_path=dest_path,
                                size_bytes=os.path.getsize(tmp_path),
                                content_hash=content_hash,
                                content_type=_file_metadata.content_type_for(name),
                                upload_job_id=job_id,
                                upload_batch_id=upload_batch_id,
                                flow_uid=flow_uid,
                                uploaded_by=_actor,
                                tags=_file_meta.get(f.filename, {}).get("labels", []),
                            )
                        except Exception as exc:
                            _log.warning("[ts/upload] object metadata skipped: %s", exc)
                        if is_values:
                            ingestable.append((name, tmp_path, flow_uid, content_hash))
                        else:
                            os.unlink(tmp_path)
                    except Exception as exc:
                        _ts_log.error(
                            "[ts/upload] failed to write %s to RustFS: %s", name, exc
                        )
                        for s in file_statuses:
                            if s.get("name") == name:
                                s["status"] = "rejected"
                                s["error"] = f"RustFS: {exc!s:.150}"
                        if flow_uid is not None:
                            try:
                                _flow.advance(
                                    flow_uid,
                                    plant_code_id=plant_code_id,
                                    status="failed",
                                    error=f"RustFS: {exc!s:.150}",
                                )
                            except Exception as exc:
                                _log.warning("[ts/upload] flow_state advance failed: %s", exc)
                        try:
                            os.unlink(tmp_path)
                        except OSError:
                            pass
                _upload_jobs[job_id]["status"] = (
                    "ingesting" if ingestable else "completed"
                )
                for name, tmp_path, v_flow_uid, content_hash in ingestable:
                    try:
                        p = Path(tmp_path)
                        if p.suffix.lower() == ".csv":
                            rows, stats = _ingest_csv_chunked(
                                p,
                                plant_code_id,
                                device_name=name,
                                content_tag=content_hash,
                            )
                        else:
                            rows, stats = _ingest_one_ts_file(
                                p,
                                plant_code_id,
                                device_name=name,
                                content_tag=content_hash,
                            )
                        stats["file"] = name
                        total += rows
                        per_file.append(stats)
                        if v_flow_uid is not None:
                            try:
                                _rin = int(stats.get("rows_in") or 0)
                                _rrej = int(stats.get("rows_rejected") or 0)
                                _frow = _flow.advance_stage(
                                    v_flow_uid,
                                    "committed",
                                    plant_code_id=plant_code_id,
                                    status="done",
                                    rows_in=_rin,
                                    rows_rejected=_rrej,
                                    rows_ingested=rows,
                                    iotdb_path=stats.get("device"),
                                )
                                _audit.append_event(
                                    plant_code_id,
                                    "ts_values",
                                    "committed",
                                    file_name=name,
                                    flow_uid=v_flow_uid,
                                    rows_in=_rin,
                                    rows_out=rows,
                                    rows_rejected=_rrej,
                                    status="ok",
                                    detail={
                                        "iotdb_path": stats.get("device"),
                                        "rejects": stats.get("rejects") or {},
                                    },
                                )
                            except Exception as exc:
                                _log.warning("[ts/ingest] event record failed: %s", exc)
                    except Exception as exc:
                        _ts_log.exception("IoTDB ingest failed for %s: %s", name, exc)
                        per_file.append(
                            {
                                "file": name,
                                "rows_inserted": 0,
                                "error": f"{type(exc).__name__}: {exc!s:.200}",
                            }
                        )
                        if v_flow_uid is not None:
                            try:
                                _flow.advance_stage(
                                    v_flow_uid,
                                    "failed",
                                    plant_code_id=plant_code_id,
                                    status="failed",
                                    error=f"{type(exc).__name__}: {exc!s:.150}",
                                )
                            except Exception as exc:
                                _log.warning("[ts/ingest] flow_state advance failed: %s", exc)
                    finally:
                        try:
                            os.unlink(tmp_path)
                        except OSError:
                            pass
                retention_prune = _enforce_ts_retention(plant_code_id)
                dropped_total = sum(
                    (p.get("retention") or {}).get("rows_dropped", 0) for p in per_file
                )
                kept_total = sum(
                    (p.get("retention") or {}).get("rows_kept", 0) for p in per_file
                )
                _upload_jobs[job_id]["post_processing"] = {
                    "rows_ingested": total,
                    "rows_dropped_pre_retention": dropped_total,
                    "rows_kept_pre_retention": kept_total,
                    "per_file": per_file,
                    "retention_prune": retention_prune,
                }
                _upload_jobs[job_id]["status"] = "completed"
                _ts_log.info(
                    "[ts/upload] IoTDB ingest done  job_id=%s  rows=%d  dropped_pre_retention=%d",
                    job_id,
                    total,
                    dropped_total,
                )

            spawn_with_context(_ts_post_bg)
            _ts_log.info(
                "[ts/upload] RustFS push + ingest scheduled  job_id=%s  files=%d",
                job_id,
                len(pending),
            )

        res = {
            "job_id": job_id,
            "upload_batch_id": upload_batch_id,
            "plant_code_id": plant_code_id,
            "file_type": safe_type,
            "files": file_statuses,
        }
        return upload_outcome(res, noun="timeseries files")
    except HTTPException as e:
        raise as_envelope_exc(e)
    except Exception as exc:
        return server_error(exc, action="connecting sap cloud")


@router.post(
    "/timeseries/connect/cloud",
    summary="Connect timeseries data from cloud storage",
    description="Configure and run cloud connector for timeseries files → RustFS.",
)
def connectTimeseriesCloud(
    req: CloudConnectorRequest,
):

    errors = []
    plant_code_id_val = (req.plant_code_id or "").strip()
    if not plant_code_id_val:
        errors.append({"field": "plant_code_id", "message": "plant_code_id is required"})
    
    cloud_type_val = (req.cloud_type or "").strip()
    if not cloud_type_val:
        errors.append({"field": "cloud_type", "message": "cloud_type is required"})
    elif cloud_type_val not in ("s3", "adls", "gcs"):
        errors.append({"field": "cloud_type", "message": f"Invalid cloud_type '{cloud_type_val}'. Must be s3, adls, or gcs"})
    else:
        if cloud_type_val == "s3":
            bucket_val = (req.bucket or "").strip()
            if not bucket_val:
                errors.append({"field": "bucket", "message": "bucket is required for S3"})
            access_key_val = (req.access_key or "").strip()
            if not access_key_val:
                errors.append({"field": "access_key", "message": "access_key is required for S3"})
            secret_key_val = (req.secret_key or "").strip()
            if not secret_key_val:
                errors.append({"field": "secret_key", "message": "secret_key is required for S3"})
        elif cloud_type_val == "gcs":
            bucket_val = (req.bucket or "").strip()
            if not bucket_val:
                errors.append({"field": "bucket", "message": "bucket is required for GCS"})
            project_id_val = (req.project_id or "").strip()
            if not project_id_val:
                errors.append({"field": "project_id", "message": "project_id is required for GCS"})
            service_account_json_val = (req.service_account_json or "").strip()
            if not service_account_json_val:
                errors.append({"field": "service_account_json", "message": "service_account_json is required for GCS"})
        elif cloud_type_val == "adls":
            container_val = (req.container or "").strip()
            if not container_val:
                errors.append({"field": "container", "message": "container is required for ADLS"})
            account_name_val = (req.account_name or "").strip()
            if not account_name_val:
                errors.append({"field": "account_name", "message": "account_name is required for ADLS"})
            
            has_account_key = bool((req.account_key or "").strip())
            has_sas_token = bool((req.sas_token or "").strip())
            has_service_principal = bool(
                (req.tenant_id or "").strip()
                and (req.client_id or "").strip()
                and (req.client_secret or "").strip()
            )
            if not (has_account_key or has_sas_token or has_service_principal):
                errors.append({
                    "field": "credentials",
                    "message": "ADLS requires account_key, sas_token, or service principal credentials (tenant_id, client_id, and client_secret)"
                })

    if errors:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": errors
            }
        )

    # Trim the inputs on the req object
    req.plant_code_id = plant_code_id_val
    req.cloud_type = cloud_type_val
    if req.bucket is not None:
        req.bucket = req.bucket.strip()
    if req.access_key is not None:
        req.access_key = req.access_key.strip()
    if req.secret_key is not None:
        req.secret_key = req.secret_key.strip()
    if req.project_id is not None:
        req.project_id = req.project_id.strip()
    if req.service_account_json is not None:
        req.service_account_json = req.service_account_json.strip()
    if req.container is not None:
        req.container = req.container.strip()
    if req.account_name is not None:
        req.account_name = req.account_name.strip()
    if req.account_key is not None:
        req.account_key = req.account_key.strip()
    if req.sas_token is not None:
        req.sas_token = req.sas_token.strip()
    if req.tenant_id is not None:
        req.tenant_id = req.tenant_id.strip()
    if req.client_id is not None:
        req.client_id = req.client_id.strip()
    if req.client_secret is not None:
        req.client_secret = req.client_secret.strip()
    if req.prefix is not None:
        req.prefix = req.prefix.strip()
    if req.region is not None:
        req.region = req.region.strip()

    plant_code_id = req.plant_code_id

    try:
        _validate_plant_code(plant_code_id)
        job_id = str(uuid.uuid4())
        override = _build_cloud_override(req, "raw/timeseries")

        _ts_log.info(
            "[ts/cloud] POST  cloud_type=%s  job_id=%s", req.cloud_type, job_id
        )

        _upload_jobs[job_id] = {
            "status": "queued",
            "source": req.cloud_type,
            "data_type": "timeseries",
            "plant_code_id": plant_code_id,
            "files": [],
            "created": datetime.now(timezone.utc).isoformat(),
        }

        t = spawn_with_context(
            target=_run_connector_bg,
            args=(job_id, "timeseries", req.cloud_type, override, TS_CONFIG),
            daemon=True,
        )
        _ts_log.info("[ts/cloud] connector job queued  job_id=%s", job_id)
        res = {"job_id": job_id, "source": req.cloud_type}
        return {
            "success": True,
            "message": "Timeseries cloud connector job started successfully",
            "data": res,
        }
    except HTTPException as e:
        raise as_envelope_exc(e)
    except Exception as exc:
        return server_error(exc, action="connecting timeseries cloud")




@router.get(
    "/jobs/{jobId}",
    summary="Get connector job status",
    description="Poll the status of any connector job (P&ID or document).",
)
def getJobStatus(
    jobId: str,
    plant_code_id: str = Query(..., description="Plant code ID"),
):

    errors = []
    plant_code_id_val = (plant_code_id or "").strip()
    if not plant_code_id_val:
        errors.append({"field": "plant_code_id", "message": "plant_code_id is required"})
    job_id_val = (jobId or "").strip()
    if not job_id_val:
        errors.append({"field": "jobId", "message": "jobId is required"})

    if errors:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": errors
            }
        )

    plant_code_id = plant_code_id_val
    job_id = job_id_val

    try:
        _validate_plant_code(plant_code_id)
        job = _upload_jobs.get(job_id)
        if not job:
            raise HTTPException(
                status_code=HTTPStatus.NOT_FOUND,
                detail={
                    "success": False,
                    "message": "Job not found",
                    "data": None,
                },
            )
        res = {"job_id": job_id, **job}
        return {
            "success": True,
            "message": "Job status retrieved successfully",
            "data": res,
        }
    except HTTPException as e:
        raise as_envelope_exc(e)
    except Exception as exc:
        return server_error(exc, action="loading job status")


@router.get(
    "/jobs",
    summary="List all connector jobs",
    description="List all recent connector jobs, newest first. "
    "Optionally filter by `data_type` (pnid or documents).",
)
def listJobs(
    plant_code_id: str = Query(..., description="Plant code ID"),
    data_type: str | None = None,
):

    errors = []
    plant_code_id_val = (plant_code_id or "").strip()
    if not plant_code_id_val:
        errors.append({"field": "plant_code_id", "message": "plant_code_id is required"})

    if errors:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": errors
            }
        )

    plant_code_id = plant_code_id_val
    if data_type is not None:
        data_type = data_type.strip()

    try:
        _validate_plant_code(plant_code_id)
        items = _upload_jobs.items()
        if data_type:
            items = [
                (jid, info) for jid, info in items if info.get("data_type") == data_type
            ]
        res = [
            {"job_id": jid, **info}
            for jid, info in sorted(
                items, key=lambda x: x[1].get("created", ""), reverse=True
            )
        ]
        return {
            "success": True,
            "message": "Jobs listed successfully",
            "data": res,
        }
    except HTTPException as e:
        raise as_envelope_exc(e)
    except Exception as exc:
        return server_error(exc, action="listing jobs")


@router.get(
    "/file",
    summary="Resolve a RustFS s3:// URI into a browser-clickable URL",
    description="Used by the docs review table so each row's "
    "``file_reference`` (an ``s3://`` URI) is clickable. "
    "Issues a 302 redirect to a presigned RustFS URL valid for "
    "1 hour. Falls back to the raw HTTP form when the SDK can't "
    "presign — works fine for dev installs without bucket auth.",
)
def resolveFile(
    ref: str,
    plant_code_id: str = Query(..., description="Plant code ID"),
):
    """Translate ``s3://bucket/key`` → a temporary HTTP URL the browser"""

    errors = []
    plant_code_id_val = (plant_code_id or "").strip()
    if not plant_code_id_val:
        errors.append({"field": "plant_code_id", "message": "plant_code_id is required"})
    ref_val = (ref or "").strip()
    if not ref_val:
        errors.append({"field": "ref", "message": "ref is required"})
    elif not ref_val.startswith(("s3://", "abfs://", "az://")):
        errors.append({"field": "ref", "message": "ref must be an s3:// or abfs:// URI"})

    if errors:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": errors
            }
        )

    plant_code_id = plant_code_id_val
    ref = ref_val

    try:
        _validate_plant_code(plant_code_id)
        _schemes = ("s3://", "abfs://", "az://")
        if not ref or not ref.startswith(_schemes):
            raise HTTPException(HTTPStatus.BAD_REQUEST, "ref must be an s3:// or abfs:// URI")
        path = ref.split("://", 1)[1]
        bucket, _, key = path.partition("/")
        if not key:
            raise HTTPException(HTTPStatus.BAD_REQUEST, "s3 URI is missing a key")
        container = object_store_container()
        if bucket != container:
            raise HTTPException(
                HTTPStatus.FORBIDDEN,
                f"only references under {container!r} are resolvable",
            )
        try:
            from p0.driver import presign_object_url

            url = presign_object_url(bucket, key, 3600)
            return RedirectResponse(url=url, status_code=HTTPStatus.FOUND)
        except Exception as exc:
            if storage_backend() == "adls":
                raise
            _log.warning("[file] presign failed (%s); falling back to direct URL", exc)
            endpoint = RUSTFS_ENDPOINT.rstrip("/")
            return RedirectResponse(
                url=f"{endpoint}/{bucket}/{key}", status_code=HTTPStatus.FOUND
            )
    except HTTPException as e:
        raise as_envelope_exc(e)
    except Exception as exc:
        return server_error(exc, action="resolve file")


@router.get(
    "/state",
    summary="Lifecycle state for every connector of a plant",
    description="One rollup per connector — status, the seven standard counts, "
    "unprocessed-file accounting and lifecycle timestamps. Derived from flow_state, "
    "so it can never disagree with the files it summarises.",
)
def listConnectorStates(
    plant_code_id: str = Query(..., description="Plant code ID for the request"),
    include_groups: bool = Query(False, description="Also return the taxonomy rollup"),
):
    errors = []
    plant_code_id_val = (plant_code_id or "").strip()
    if not plant_code_id_val:
        errors.append({"field": "plant_code_id", "message": "plant_code_id is required"})
    if errors:
        return JSONResponse(
            status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
            content={"success": False, "message": "Validation failed", "errors": errors},
        )

    try:
        _validate_plant_code(plant_code_id_val)
        states = _connector_state.all_connector_states(
            plant_code_id_val, include_groups=include_groups
        )
        configured = [s for s in states if s["total_file_count"] > 0]
        return {
            "success": True,
            "message": (
                "Connector states fetched successfully"
                if configured
                else f"No connectors have been configured yet for plant '{plant_code_id_val}'."
            ),
            "data": {
                "plant_code_id": plant_code_id_val,
                "connectors": states,
                "total": len(states),
                "is_empty": not configured,
            },
        }
    except HTTPException as e:
        raise as_envelope_exc(e)
    except Exception as exc:
        return server_error(
            exc, action="loading connector states", plant_code_id=plant_code_id_val
        )


@router.get(
    "/{connector}/state",
    summary="Lifecycle state for one connector",
    description="The single authoritative status surface for a (plant, connector). "
    "Replaces the six browser localStorage keys the UI keeps today.",
)
def getConnectorState(
    connector: str,
    plant_code_id: str = Query(..., description="Plant code ID for the request"),
    include_groups: bool = Query(True, description="Also return the taxonomy rollup"),
):
    errors = []
    plant_code_id_val = (plant_code_id or "").strip()
    if not plant_code_id_val:
        errors.append({"field": "plant_code_id", "message": "plant_code_id is required"})
    connector_val = (connector or "").strip().lower()
    if connector_val not in _connector_state.CONNECTORS:
        errors.append(
            {
                "field": "connector",
                "message": (
                    f"'{connector}' is not a valid connector. "
                    f"Use one of: {', '.join(_connector_state.CONNECTORS)}."
                ),
            }
        )
    if errors:
        return JSONResponse(
            status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
            content={"success": False, "message": "Validation failed", "errors": errors},
        )

    try:
        _validate_plant_code(plant_code_id_val)
        state = _connector_state.connector_state(
            plant_code_id_val, connector_val, include_groups=include_groups
        )
        return {
            "success": True,
            "message": (
                "Connector state fetched successfully"
                if state["total_file_count"]
                else (
                    f"No files have been uploaded yet for the '{connector_val}' "
                    f"connector of plant '{plant_code_id_val}'."
                )
            ),
            "data": state,
        }
    except HTTPException as e:
        raise as_envelope_exc(e)
    except Exception as exc:
        return server_error(
            exc, action="loading connector state", plant_code_id=plant_code_id_val
        )


@router.get(
    "/{connector}/files/{flowUid}/metadata",
    summary="Server-extracted metadata for one uploaded file",
    description="Page counts, sheet names, text-layer flag, stored object path and tags — "
    "read from the sidecar written beside the object at upload time.",
)
def getFileMetadata(
    connector: str,
    flowUid: int,
    plant_code_id: str = Query(..., description="Plant code ID for the request"),
):
    errors = []
    plant_code_id_val = (plant_code_id or "").strip()
    if not plant_code_id_val:
        errors.append({"field": "plant_code_id", "message": "plant_code_id is required"})
    connector_val = (connector or "").strip().lower()
    if connector_val not in _connector_state.CONNECTORS:
        errors.append(
            {
                "field": "connector",
                "message": (
                    f"'{connector}' is not a valid connector. "
                    f"Use one of: {', '.join(_connector_state.CONNECTORS)}."
                ),
            }
        )
    if flowUid <= 0:
        errors.append({"field": "flowUid", "message": "flowUid must be a positive integer"})
    if errors:
        return JSONResponse(
            status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
            content={"success": False, "message": "Validation failed", "errors": errors},
        )

    try:
        _validate_plant_code(plant_code_id_val)
        row = _flow.get(flowUid, plant_code_id_val)
        if row is None:
            return JSONResponse(
                status_code=HTTPStatus.NOT_FOUND,
                content={
                    "success": False,
                    "message": f"No file with id {flowUid} exists for plant '{plant_code_id_val}'.",
                    "errors": [
                        {"field": "flowUid", "message": f"File {flowUid} was not found."}
                    ],
                },
            )

        sidecar = _object_metadata.read_sidecar(row.get("rustfs_path") or "")
        data = {
            "flow_uid": row.get("flow_uid"),
            "plant_code_id": plant_code_id_val,
            "connector": connector_val,
            "file_name": row.get("file_name"),
            "path": row.get("rustfs_path"),
            "content_type": row.get("content_type"),
            "size_bytes": row.get("size_bytes"),
            "content_hash": row.get("content_hash"),
            "category": row.get("category"),
            "page_count": row.get("page_count"),
            "has_text_layer": row.get("has_text_layer"),
            "sheet_names": row.get("sheet_names"),
            "duplicate_of": row.get("duplicate_of"),
            "uploaded_by": row.get("uploaded_by"),
            "uploaded_at": str(row.get("uploaded_at")) if row.get("uploaded_at") else None,
            "taxonomy": _taxonomy.taxonomy_for_file(connector_val, row.get("category")),
            "sidecar": sidecar,
            "has_sidecar": sidecar is not None,
        }
        return {
            "success": True,
            "message": "File metadata fetched successfully",
            "data": data,
        }
    except HTTPException as e:
        raise as_envelope_exc(e)
    except Exception as exc:
        return server_error(
            exc, action="loading file metadata", plant_code_id=plant_code_id_val
        )


def _inspect_findings_columns(connector: str, filename: str, payload: bytes) -> dict:
    """Read the file's schema at upload time so the user sees the mapping immediately."""
    try:
        import io

        import pandas as pd

        suffix = Path(filename or "").suffix.lower()
        if suffix == ".csv":
            frame = pd.read_csv(io.BytesIO(payload), nrows=200)
            sheet = None
        else:
            book = pd.ExcelFile(io.BytesIO(payload))
            sheet = _findings.pick_sheet(connector, list(book.sheet_names))
            frame = book.parse(sheet, nrows=200)
        mapping = _findings.column_map(connector, list(frame.columns))
        return {
            "sheet": sheet,
            "columns_mapped": mapping["mapped"],
            "columns_unmapped": mapping["unmapped"],
            "columns_missing": mapping["missing"],
            "missing_required": _findings.missing_required(connector, mapping),
        }
    except Exception as exc:
        _log.warning("[%s/upload] schema read failed for %s: %s", connector, filename, exc)
        return {"schema_error": human_message(exc, action="reading the file's columns")}
@router.post(
    "/documents/others/inspect",
    summary="Inspect a user-defined Other File",
    description=(
        "Inspect an Other File before upload/processing. "
        "The backend determines whether the file is structured or unstructured. "
        "Structured files return detected source columns; unstructured files "
        "indicate that extraction fields must be supplied by the user."
    ),
)
@router.post(
    "/other-files/inspect",
    include_in_schema=False,
)
async def inspectOtherFile(
    file: UploadFile = File(...),
    plant_code_id: str = Form(default=""),
    user_defined_type: str = Form(default=""),
    data_mode: str | None = Form(default=None),
):
    """Inspect an Other File without persisting it."""

    errors: list[dict] = []

    plant_code_id_val = (plant_code_id or "").strip()
    user_defined_type_val = (user_defined_type or "").strip()

    if file is None:
        errors.append(
            {
                "field": "file",
                "message": "file is required",
            }
        )

    if errors:
        return JSONResponse(
            status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": errors,
            },
        )

    try:
        if plant_code_id_val:
            _validate_plant_code(plant_code_id_val)

        file_name = (file.filename or "").strip()
        if not file_name:
            return JSONResponse(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
                content={
                    "success": False,
                    "message": "Validation failed",
                    "errors": [
                        {
                            "field": "file",
                            "message": "Uploaded file must have a filename",
                        }
                    ],
                },
            )

        payload = await file.read()
        if not payload:
            return JSONResponse(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
                content={
                    "success": False,
                    "message": "Validation failed",
                    "errors": [
                        {
                            "field": "file",
                            "message": "Uploaded file is empty",
                        }
                    ],
                },
            )

        inspection = _other_files.inspect_other_file(
            file_name=file_name,
            payload=payload,
            user_defined_type=user_defined_type_val or "other",
        )

        detected_mode = str(inspection.get("data_mode") or "").strip().lower()
        client_mode = (data_mode or "").strip().lower()

        if client_mode and client_mode not in {"structured", "unstructured"}:
            return JSONResponse(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
                content={
                    "success": False,
                    "message": "Validation failed",
                    "errors": [
                        {
                            "field": "data_mode",
                            "message": (
                                "data_mode must be either 'structured' "
                                "or 'unstructured'."
                            ),
                        }
                    ],
                },
            )

        if client_mode and client_mode != detected_mode:
            return JSONResponse(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
                content={
                    "success": False,
                    "message": "Validation failed",
                    "errors": [
                        {
                            "field": "data_mode",
                            "message": (
                                f"data_mode '{client_mode}' does not match "
                                f"backend-detected mode '{detected_mode}'."
                            ),
                        }
                    ],
                },
            )

        return {
            "success": True,
            "message": "Other file inspected successfully",
            "data": {
                **(
                    {"plant_code_id": plant_code_id_val}
                    if plant_code_id_val
                    else {}
                ),
                **inspection,
            },
        }

    except ValueError as exc:
        return JSONResponse(
            status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": [
                    {
                        "field": "file",
                        "message": str(exc),
                    }
                ],
            },
        )
    except HTTPException as exc:
        raise as_envelope_exc(exc)
    except Exception as exc:
        return server_error(
            exc,
            action="inspecting other file",
            plant_code_id=plant_code_id_val or None,
        )


async def _upload_findings_files(
    connector, plant_code_id, files, labels_raw, meta_raw=None,
    *, job_id=None, upload_batch_id=None,
):
    errors = []
    plant_code_id_val = (plant_code_id or "").strip()
    if not plant_code_id_val:
        errors.append({"field": "plant_code_id", "message": "plant_code_id is required"})
    if not files:
        errors.append({"field": "files", "message": "At least one file is required"})

    _file_meta, label_error = _manifest.resolve(
        meta_raw,
        [f.filename for f in (files or [])],
        connector=connector,
        fallback_labels=labels_raw,
    )
    chosen = _labels.normalise([l for m in _file_meta.values() for l in m["labels"]])
    if label_error:
        errors.append({"field": "labels", "message": label_error})

    if errors:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={"success": False, "message": "Validation failed", "errors": errors},
        )

    plant_code_id = plant_code_id_val
    spec = _findings.spec_for(connector)

    try:
        _validate_plant_code(plant_code_id)
        job_id = job_id or str(uuid.uuid4())
        upload_batch_id = upload_batch_id or str(uuid.uuid4())
        file_statuses: list[dict] = []

        _ensure_rustfs_bucket(f"{connector}/upload")
        sfs = _get_s3fs()
        prefix = _fs.strip_scheme(rustfs_staging_findings(plant_code_id, connector))
        names = [f.filename for f in files]

        _log.info(
            "[%s/upload] POST — %d file(s)  job_id=%s  rustfs=s3://%s",
            connector, len(files), job_id, prefix,
        )

        for f in files:
            ext = Path(f.filename or "").suffix.lower()
            if ext not in ALLOWED_FINDINGS_EXT:
                file_statuses.append({
                    "name": f.filename, "size": 0, "status": "rejected",
                    "error": f"Type {ext} not allowed. Upload a CSV or Excel file.",
                })
                continue
            detail = await f.read()
            if len(detail) > MAX_FINDINGS_SIZE:
                file_statuses.append({
                    "name": f.filename, "size": len(detail), "status": "rejected",
                    "error": "File exceeds 200MB",
                })
                continue
            try:
                dest_path = f"{prefix}/{f.filename}"
                with sfs.open(dest_path, "wb") as fout:
                    fout.write(detail)
                file_labels = _file_meta.get(f.filename, {}).get("labels", [])
                schema = _inspect_findings_columns(connector, f.filename, detail)
                file_status = {
                    "name": f.filename,
                    "size": len(detail),
                    "status": "uploaded",
                    "labels": file_labels,
                    "schema": schema,
                }
                file_statuses.append(file_status)
                try:
                    _meta = _file_metadata.inspect(f.filename, detail)
                    _hash = _flow.hash_bytes(detail, f.filename)
                    _dupe = _flow.find_duplicates(
                        plant_code_id, connector,
                        file_name=f.filename, size_bytes=len(detail),
                        content_hash=_hash, category=connector,
                    )
                    file_status["metadata"] = _meta
                    file_status["duplicate_of"] = _dupe.get("duplicate_of")
                    file_status["is_duplicate"] = _dupe.get("is_duplicate")
                    file_status["duplicate_matched_on"] = _dupe.get("matched_on")
                    flow_row = _flow.upsert_file(
                        plant_code_id, connector, _hash,
                        file_name=f.filename,
                        file_type=connector,
                        size_bytes=len(detail),
                        stage="staged",
                        status="done",
                        upload_job_id=job_id,
                        upload_batch_id=upload_batch_id,
                        rustfs_path=dest_path,
                        error=None,
                        category=connector,
                        labels=file_labels,
                        uploaded_by=get_actor(),
                        duplicate_of=_dupe.get("duplicate_of"),
                        **_file_metadata.flow_fields(_meta),
                    )
                    if flow_row is None:
                        raise RuntimeError(
                            "the file could not be recorded in flow_state"
                        )
                    if flow_row:
                        file_status["flow_uid"] = flow_row.get("flow_uid")
                        file_status["stage"] = flow_row.get("stage")
                        file_status["content_hash"] = flow_row.get("content_hash")
                        _object_metadata.record(
                            prefix,
                            plant_code_id=plant_code_id,
                            connector=connector,
                            category=connector,
                            file_name=f.filename,
                            object_path=dest_path,
                            size_bytes=len(detail),
                            content_hash=_hash,
                            content_type=_meta.get("content_type"),
                            upload_job_id=job_id,
                            upload_batch_id=upload_batch_id,
                            flow_uid=flow_row.get("flow_uid"),
                            uploaded_by=get_actor(),
                            metadata=dict(_meta, **schema),
                            tags=file_labels,
                        )
                except Exception as exc:
                    _log.error(
                        "[%s/upload] flow_state write failed for %s: %s",
                        connector, f.filename, exc,
                    )
                    file_status["status"] = "rejected"
                    file_status["error"] = human_message(
                        exc, action="recording the uploaded file", plant_code_id=plant_code_id
                    )
            except Exception as exc:
                _log.error("[%s/upload] failed to write %s: %s", connector, f.filename, exc)
                file_statuses.append({
                    "name": f.filename, "size": len(detail), "status": "rejected",
                    "error": human_message(exc, action="storing the uploaded file"),
                })

        existing_job = _upload_jobs.get(job_id)
        if existing_job is not None:
            existing_job["files"] = list(existing_job.get("files") or []) + file_statuses
            existing_job["data_type"] = "events"
            existing_job.setdefault("connectors", [existing_job.get("connector")])
            if connector not in existing_job["connectors"]:
                existing_job["connectors"].append(connector)
            existing_job.pop("connector", None)
        else:
            _upload_jobs[job_id] = {
                "status": "completed",
                "source": "device",
                "data_type": connector,
                "connector": connector,
                "plant_code_id": plant_code_id,
                "files": file_statuses,
                "created": datetime.now(timezone.utc).isoformat(),
            }
        res = {
            "job_id": job_id,
            "upload_batch_id": upload_batch_id,
            "plant_code_id": plant_code_id,
            "connector": connector,
            "labels": chosen,
            "target_table": spec.get("target_table"),
            "files": file_statuses,
        }
        return upload_outcome(res, noun=f"{connector.upper()} files")
    except HTTPException as e:
        raise as_envelope_exc(e)
    except Exception as exc:
        return server_error(exc, action=f"uploading {connector} files")


@router.post(
    "/aif/upload",
    summary="Upload Asset Integrity Findings files from device",
    description="Upload AIF register exports (CSV, XLSX, XLS) → staging. Columns are read and "
    "mapped against the AIF template at upload time, so the response already tells you which "
    "columns landed and which are missing. Equipment identity is resolved from the functional "
    "location. Returns a `job_id` and an `upload_batch_id`.",
)
async def uploadAifFiles(
    plant_code_id: str = Form(...),
    files: list[UploadFile] = File(...),
    labels: list[str] = Form(default=[], description=_FINDINGS_LABEL_HELP),
    files_meta: list[str] = Form(default=[], description=_manifest.HELP),
):
    return await _upload_findings_files("aif", plant_code_id, files, labels, files_meta)


@router.post(
    "/gloc/upload",
    summary="Upload Gas Loss of Containment files from device",
    description="Upload GLOC register exports (CSV, XLSX, XLS) → staging. Equipment identity "
    "prefers the SAP equipment column, then the equipment tag column. Returns a `job_id` and "
    "an `upload_batch_id`.",
)
async def uploadGlocFiles(
    plant_code_id: str = Form(...),
    files: list[UploadFile] = File(...),
    labels: list[str] = Form(default=[], description=_FINDINGS_LABEL_HELP),
    files_meta: list[str] = Form(default=[], description=_manifest.HELP),
):
    return await _upload_findings_files("gloc", plant_code_id, files, labels, files_meta)


@router.post(
    "/lopc/upload",
    summary="Upload Loss of Primary Containment files from device",
    description="Upload LOPC register exports (CSV, XLSX, XLS) → staging. These files carry no "
    "equipment column, so identity is extracted from the incident title and description and "
    "returned with a lower confidence for review. Returns a `job_id` and an `upload_batch_id`.",
)
async def uploadLopcFiles(
    plant_code_id: str = Form(...),
    files: list[UploadFile] = File(...),
    labels: list[str] = Form(default=[], description=_FINDINGS_LABEL_HELP),
    files_meta: list[str] = Form(default=[], description=_manifest.HELP),
):
    return await _upload_findings_files("lopc", plant_code_id, files, labels, files_meta)


_EVENTS_FILE_MATCH: list[tuple[str, "re.Pattern"]] = [
    ("trip_event", re.compile(r"(?<![A-Za-z0-9])trip(?![A-Za-z0-9])", re.IGNORECASE)),
    ("upd_event", re.compile(r"(?<![A-Za-z0-9])(?:upd|pd)(?![A-Za-z0-9])", re.IGNORECASE)),
]


def _classify_events_file(filename: str) -> str | None:
    """Which events connector a file belongs to, by filename — None if neither pattern hits."""
    name = filename or ""
    for connector, pattern in _EVENTS_FILE_MATCH:
        if pattern.search(name):
            return connector
    return None


def _events_result_data(outcome) -> tuple[bool, dict]:
    """Normalise one _upload_findings_files call's return (dict or JSONResponse) to (ok, data)."""
    if isinstance(outcome, JSONResponse):
        import json as _json

        body = _json.loads(bytes(outcome.body))
        return bool(body.get("success")), body.get("data") or {}
    return bool(outcome.get("success")), outcome.get("data") or {}


@router.post(
    "/events/upload",
    summary="Upload UPD/PD and Trip Event files from device",
    description="Upload UPD PD and Trip Event register exports (CSV, XLSX, XLS) → a shared "
    "'events' staging folder. Each file is routed to upd_event or trip_event by filename "
    "('trip' → trip_event, 'upd'/'pd' → upd_event) — a file matching neither is rejected "
    "rather than guessed at. Returns one job per matched connector.",
)
async def uploadEventsFiles(
    plant_code_id: str = Form(...),
    files: list[UploadFile] = File(...),
    labels: list[str] = Form(default=[], description=_FINDINGS_LABEL_HELP),
    files_meta: list[str] = Form(default=[], description=_manifest.HELP),
):
    errors = []
    plant_code_id_val = (plant_code_id or "").strip()
    if not plant_code_id_val:
        errors.append({"field": "plant_code_id", "message": "plant_code_id is required"})
    if not files:
        errors.append({"field": "files", "message": "At least one file is required"})

    grouped: dict[str, list[UploadFile]] = {"upd_event": [], "trip_event": []}
    unclassified: list[str] = []
    for f in files or []:
        connector = _classify_events_file(f.filename or "")
        if connector is None:
            unclassified.append(f.filename or "(unnamed)")
        else:
            grouped[connector].append(f)

    if unclassified:
        errors.append({
            "field": "files",
            "message": (
                "Could not tell which table these file(s) belong to from their name — "
                "rename so the filename contains 'UPD'/'PD' or 'Trip': "
                + ", ".join(unclassified[:5])
            ),
        })

    if errors:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={"success": False, "message": "Validation failed", "errors": errors},
        )

    plant_code_id = plant_code_id_val

    try:
        _validate_plant_code(plant_code_id)

        job_id = str(uuid.uuid4())
        upload_batch_id = str(uuid.uuid4())

        target_tables: dict[str, str | None] = {}
        combined_files: list[dict] = []
        for connector, group_files in grouped.items():
            if not group_files:
                continue
            outcome = await _upload_findings_files(
                connector, plant_code_id, group_files, labels, files_meta,
                job_id=job_id, upload_batch_id=upload_batch_id,
            )
            ok, data = _events_result_data(outcome)
            if not ok:
                _log.error(
                    "[events/upload] %s sub-upload failed under job_id=%s", connector, job_id,
                )
                return outcome
            target_tables[connector] = data.get("target_table")
            for entry in data.get("files", []):
                combined_files.append({**entry, "connector": connector})

        res = {
            "job_id": job_id,
            "upload_batch_id": upload_batch_id,
            "plant_code_id": plant_code_id,
            "connector": "events",
            "target_tables": target_tables,
            "files": combined_files,
        }
        return upload_outcome(res, noun="events files")
    except HTTPException as e:
        raise as_envelope_exc(e)
    except Exception as exc:
        return server_error(exc, action="uploading events files")


@router.get(
    "/labels",
    summary="List the use-case labels",
    description="The fixed label vocabulary the upload modal renders, plus which connectors "
    "require a label and what to pre-tick for each.",
)
def listLabels():
    return {
        "success": True,
        "message": "Labels retrieved successfully",
        "data": {
            "labels": _labels.catalogue(),
            "required_for": [
                c for c in _connector_state.CONNECTORS if _labels.requires_labels(c)
            ],
            "not_required_for": sorted(_labels.UNLABELLED_CONNECTORS),
            "suggested": {c: _labels.suggested_for(c) for c in _connector_state.CONNECTORS},
        },
    }


@router.post(
    "/upload",
    summary="Upload files for any connector",
    description=(
        "One endpoint for all supported connectors — pnid, documents, "
        "timeseries, sap, aif, gloc, lopc, events (upd_event + trip_event), "
        "other_files, alerts. `connector` selects the target. Every per-connector "
        "endpoint remains available."
    ),
)
async def uploadConnectorFiles(
    connector: str = Form(
        ...,
        description=(
            "pnid | documents | timeseries | sap | "
            "aif | gloc | lopc | events | other_files | alerts"
        ),
    ),
    plant_code_id: str = Form(...),
    files: list[UploadFile] = File(...),
    document_type: str = Form(default=""),
    document_type_label: str | None = Form(default=None),
    source_columns: str = Form(default=""),
    data_mode: str | None = Form(default=None),
    equipment_column: str | None = Form(default=None),
    file_type: str = Form(default="metadata"),
    labels: list[str] = Form(
        default=[],
        description=_FINDINGS_LABEL_HELP,
    ),
    files_meta: list[str] = Form(
        default=[],
        description=_manifest.HELP,
    ),
    validate_only: bool = Form(
        False,
        description=(
            "For alerts only: validate equipment matching against "
            "decisionops_bocpp without staging or database writes."
        ),
    ),
):
    name = (connector or "").strip().lower()
    if name == "events":
        return await uploadEventsFiles(
            plant_code_id=plant_code_id, files=files, labels=labels, files_meta=files_meta
        )
    if _findings.is_findings_connector(name):
        return await _upload_findings_files(
            name,
            plant_code_id,
            files,
            labels,
            files_meta,
        )

    if name in ("pnid", "p&id", "pid"):
        return await uploadPnidFiles(
            plant_code_id=plant_code_id,
            files=files,
        )

    if name in ("documents", "docs", "document"):
        if (
            not (document_type or "").strip()
            and not _manifest.describes(files_meta)
        ):
            return JSONResponse(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
                content={
                    "success": False,
                    "message": (
                        "document_type is required when uploading documents — "
                        "send it once for the whole upload, or give each file "
                        f"its own in {_manifest.FIELD}."
                    ),
                    "errors": [
                        {
                            "field": "document_type",
                            "message": "document_type is required.",
                        }
                    ],
                },
            )

        return await uploadDocumentFiles(
            plant_code_id=plant_code_id,
            document_type=document_type,
            files=files,
            source_columns=source_columns,
            document_type_label=document_type_label,
            data_mode=data_mode,
            equipment_column=equipment_column,
            labels=labels,
            files_meta=files_meta,
        )

    # Other Files reuse the document upload/storage lifecycle but are
    # explicitly selected through the common API.
    if name in (
        "other_files",
        "other-files",
        "others",
        "other",
    ):
        if not (document_type_label or "").strip():
            return JSONResponse(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
                content={
                    "success": False,
                    "message": "Validation failed",
                    "errors": [
                        {
                            "field": "document_type_label",
                            "message": (
                                "document_type_label is required for "
                                "Other Files and identifies the "
                                "user-defined type."
                            ),
                        }
                    ],
                },
            )

        return await uploadDocumentFiles(
            plant_code_id=plant_code_id,
            document_type="others",
            files=files,
            source_columns=source_columns,
            document_type_label=document_type_label,
            data_mode=data_mode,
            equipment_column=equipment_column,
            labels=labels,
            files_meta=files_meta,
        )

    if name == "sap":
        return await uploadSapFiles(
            plant_code_id=plant_code_id,
            files=files,
            labels=labels,
            files_meta=files_meta,
        )

    if name in (
        "timeseries",
        "ts",
        "time_series",
    ):
        return await uploadTimeseriesFiles(
            plant_code_id=plant_code_id,
            files=files,
            file_type=file_type,
            labels=labels,
            files_meta=files_meta,
        )

    if name == "alerts":
        return await uploadAlertsFiles(
            plant_code_id=plant_code_id,
            files=files,
            labels=labels,
            files_meta=files_meta,
            validate_only=validate_only,
        )

    return JSONResponse(
        status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
        content={
            "success": False,
            "message": (
                f"'{connector}' is not a valid connector. "
                "Use one of: pnid, documents, timeseries, sap, "
                "aif, gloc, lopc, events, other_files, alerts."
            ),
            "errors": [
                {
                    "field": "connector",
                    "message": "Unknown connector.",
                }
            ],
        },
    )