"""Decision-context endpoints: review staged rows, patch them, and commit to the CDM."""

from __future__ import annotations

import io
import logging
import os
import re
import uuid
from datetime import datetime, timezone
from http import HTTPStatus
from pathlib import Path
from typing import Annotated

import pandas as pd
from fastapi import APIRouter, Body, Depends, Query, HTTPException, Header
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse

from p0.api.audit_context import get_actor, spawn_with_context
from p0.api.services import idempotency as _idempotency
from p0.api.services import user_config_store as _user_config
from p0.api.services.audit import upsert_job
from .pipeline import _pipeline_jobs as _pipeline_jobs_registry
from ..responses import (
    as_envelope_exc,
    nothing_to_commit,
    not_processed_yet,
    EnvelopeRoute,
    collection_envelope,
    human_message,
    normalise_commit,
    paginated_envelope,
    server_error,
)
from ..database.rustfs_store import (
    _object_fs,
    _list_docs_from_rustfs,
    _list_rustfs_pnid_images,
    _read_parquet_from_rustfs,
    _write_processed_docs,
    _write_processed_other_files,
    _write_processed_pnid,
    _write_processed_sap,
    _write_processed_sap_mapping,
    _write_processed_ts,
)
from ..database.cdm_writer import (
    _delete_doc_rows,
    _read_ts_from_db,
    _replace_by_natural_key,
    _replace_by_source_file,
    _stale_doc_rows,
    _unique_key_columns,
    _upsert_doc_metadata,
    _upsert_ts_metadata,
    _write_to_db,
    _write_to_doc_metadata_table,
)
from ..services import findings as _findings
from ..services.docs_taxonomy import (
    _group_docs_by_category,
)
from ..services.transforms import (
    _apply_plant_code,
    _batch_id_set,
    _coerce_numeric_limit,
    _filter_pnid_context_by_files,
    _filter_rows_by_batch,
    _filter_rows_by_source_file,
    _norm_fname,
    _parse_commit_body,
    _retransform_docs_rows,
    _retransform_pnid_rows,
    _retransform_ts_rows,
    _stamp_commit_timestamps,
)
from p0.utils import fs as _fs

from ..config import (
    ENTITIES_FROZEN_FILE,
    OUT_DIR,
    SAP_JOIN_COLUMNS_FILE,
    SAP_RENAME_FILE,
    SCHEMA_FROZEN_FILE,
    STAGING_DIR,
    USER_CONFIG_FILE,
)
from ..config import rustfs_out_dir as _out
from ..config import (
    rustfs_processed_findings,
    rustfs_processed_pnid,
    rustfs_processed_sap,
    rustfs_processed_sap_mapping,
    rustfs_processed_ts,
    rustfs_staging_pnid,
)
from ..deps import backup, load_yaml, save_yaml
from ..database.connection import _get_conn

router = APIRouter(route_class=EnvelopeRoute, prefix="/context", tags=["Decision Context"])

_log = logging.getLogger(__name__)

_STAGE_DIR_ALIASES: dict[str, list[str]] = {
    "pnid": ["pnid", "pnid_v2"],
    "docs": ["docs", "docs_v2"],
    "ts": ["ts"],
    "sap": ["sap"],
    "aif": ["aif"],
    "gloc": ["gloc"],
    "lopc": ["lopc"],
    "other_files": ["other_files"],
}


def _resolve_stage_dir(stage: str, plant_code_id: str | None = None) -> str:
    """Find the actual output directory for a stage, checking aliases."""

    if plant_code_id:

        roots = [_out(plant_code_id)]
    else:
        roots = [str(OUT_DIR)]
    for root in roots:
        for alias in _STAGE_DIR_ALIASES.get(stage, [stage]):
            candidate = f"{root.rstrip('/')}/{alias}"
            if _fs.is_s3(candidate):
                if _fs.glob_files(_fs.path_join(candidate, "entities", "*.parquet")):
                    return candidate
            else:
                if Path(candidate).exists():
                    return candidate
    return f"{roots[0].rstrip('/')}/{stage}"






def _read_entity_file(path) -> pd.DataFrame:
    """Read a Parquet or CSV entity file (local or s3://), preferring Parquet."""

    spath = str(path)
    if spath.endswith(".parquet"):
        pq = spath
        csv = spath[: -len(".parquet")] + ".csv"
    elif spath.endswith(".csv"):
        pq = spath[: -len(".csv")] + ".parquet"
        csv = spath
    else:
        pq = spath + ".parquet"
        csv = spath + ".csv"
    if _fs.exists(pq):
        try:
            return _fs.read_parquet(pq).astype(str).fillna("")
        except Exception as exc:
            _log.warning("[context] entity parquet unreadable %s: %s", pq, exc)
            return pd.DataFrame()
    if _fs.exists(csv):
        try:
            return _fs.read_csv(csv, dtype=str).fillna("")
        except pd.errors.EmptyDataError:
            return pd.DataFrame()
    return pd.DataFrame()


def _glob_entity_files(directory) -> list[str]:
    """Glob entity files preferring .parquet, falling back to .csv."""

    sdir = str(directory)
    stems_seen: set[str] = set()
    result: list[str] = []
    for f in sorted(_fs.glob_files(_fs.path_join(sdir, "*.parquet"))):
        stems_seen.add(Path(f).stem)
        result.append(f)
    for f in sorted(_fs.glob_files(_fs.path_join(sdir, "*.csv"))):
        if Path(f).stem not in stems_seen:
            result.append(f)
    return sorted(result, key=lambda p: Path(p).stem)


def _read_stage_csvs(
    stage: str, table_name: str | None = None, plant_code_id: str | None = None
) -> list[dict]:
    """Read entity files from OUT_DIR/{stage}/entities/. If table_name given, read that specific file."""

    stage_dir = _resolve_stage_dir(stage, plant_code_id)
    entity_dir = _fs.path_join(stage_dir, "entities")
    if not _fs.is_s3(entity_dir) and not Path(entity_dir).exists():
        return []

    if table_name:
        df = _read_entity_file(_fs.path_join(entity_dir, f"{table_name}.csv"))
        if df.empty:
            return []
        return df.to_dict(orient="records")

    rows = []
    for entity_file in _glob_entity_files(entity_dir):
        df = _read_entity_file(entity_file)
        for record in df.to_dict(orient="records"):
            record["_source_table"] = Path(entity_file).stem
            rows.append(record)
    return rows












def _record_commit_audit(
    source, plant_code_id, tables, total, current_user, detail=None
) -> int | None:
    """Write one commit_audit row. Derives status from per-table warnings:"""
    try:
        from p0.api.services.audit import record_commit

        actor = get_actor()
        had_warning = any(
            isinstance(t, dict) and t.get("warning") for t in (tables or [])
        )
        status = "partial" if had_warning else "committed"
        return record_commit(
            source=source,
            plant_code_id=plant_code_id,
            status=status,
            rows_written=total,
            tables=tables,
            detail=detail,
            committed_by=actor,
        )
    except Exception as exc:
        _log.warning("[context] commit audit record failed: %s", exc)
        return None


def _advance_flow(
    plant_code_id: str,
    flow: str,
    *,
    only_before_committed: bool = True,
    file_names: list[str] | None = None,
    **fields,
) -> None:
    """Best-effort: advance flow_state rows for *flow*."""
    target_stage = fields.pop("stage", None)
    try:
        from p0.api.services import flow as _flow

        wanted = None
        if file_names:
            wanted = {
                str(n).strip().replace("\\", "/").rsplit("/", 1)[-1]
                for n in file_names
                if n
            }
        for row in _flow.list_flow(plant_code_id, flow):
            fn = (
                str(row.get("file_name") or "")
                .strip()
                .replace("\\", "/")
                .rsplit("/", 1)[-1]
            )
            is_target = (wanted is None) or (fn in wanted)
            if (
                only_before_committed
                and row.get("stage") == "committed"
                and not is_target
            ):
                continue
            if not is_target:
                continue
            if target_stage:
                _flow.advance_stage(row["flow_uid"], target_stage, plant_code_id=plant_code_id, **fields)
            else:
                _flow.advance(row["flow_uid"], plant_code_id=plant_code_id, **fields)
    except Exception as exc:
        _log.warning("[context] flow lookup failed: %s", exc)


def _advance_flow_to_reviewing(plant_code_id: str, flow: str) -> None:
    """Best-effort: move only files currently at 'processed' to 'reviewing'."""
    try:
        from p0.api.services import flow as _flow

        for row in _flow.list_flow(plant_code_id, flow):
            if row.get("stage") == "processed":
                _flow.advance(row["flow_uid"], plant_code_id=plant_code_id, stage="reviewing", status="running")
    except Exception as exc:
        _log.warning("[context] flow advance-to-reviewing failed: %s", exc)


















class ReviewedStateMissing(Exception):
    """The connector has no processed output at all — the caller skipped a step."""


def _read_ts_rows_for_commit(
    plant_code_id: str, upload_batch_id: str | None, source_file: str | None
) -> list[dict]:
    """The reviewed timeseries rows, when the client commits without sending them."""
    path = f"{rustfs_processed_ts(plant_code_id)}/ts_timeseries_metadata.parquet"
    if not _fs.exists(path):
        raise ReviewedStateMissing(path)
    frame = _read_parquet_from_rustfs(path)
    rows = [] if frame is None or frame.empty else frame.to_dict(orient="records")
    if rows and source_file:
        rows = _filter_rows_by_source_file(rows, source_file)
    if rows and upload_batch_id:
        rows = _filter_rows_by_batch(rows, upload_batch_id)
    _log.info(
        "[context/ts/commit] body had no rows — loaded %d reviewed row(s) from %s "
        "(upload_batch_id=%s source_file=%s)",
        len(rows), path, upload_batch_id or "(none)", source_file or "(none)",
    )
    return rows


# Pipeline-internal files written alongside the real canonical entity files in
# entities/ — never meant to be committed as their own CDM tables (Gap 6):
#   sap_<name>   — raw pre-canonicalization snapshot of a built-in SAP merge dataset
#                  (run_sap_end_to_end.py: post[ds_name] -> sap_<ds_name>.parquet)
#   <entity>_uid — identity/dedup lookup table (uid + identity keys only), built for
#                  every entity in entities_frozen.yaml (canonical_entities.py)
# Their canonical, full-schema data is already committed under the entity's plain
# name (e.g. functional_location, equipment, asset_strategy) — these are a
# redundant column subset, not missing data, so committing them too would just
# duplicate rows under a name the CDM schema never defined.
_SAP_RAW_SNAPSHOT_NAMES = {"workorder", "floc", "tasklist", "material", "notification", "bom"}


def _is_internal_sap_artifact(name: str) -> bool:
    lname = name.lower()
    return lname.endswith("_uid") or (
        lname.startswith("sap_") and lname[len("sap_"):] in _SAP_RAW_SNAPSHOT_NAMES
    )


def _read_sap_tables_for_commit(plant_code_id: str) -> dict[str, list[dict]]:
    """The reviewed SAP tables, when the client commits without sending them."""
    entity_dir = _fs.path_join(_resolve_stage_dir("sap", plant_code_id), "entities")
    entity_files = _glob_entity_files(entity_dir)
    if not entity_files:
        raise ReviewedStateMissing(entity_dir)
    tables: dict[str, list[dict]] = {}
    skipped: list[str] = []
    for entity_file in entity_files:
        name = Path(entity_file).stem
        if _is_internal_sap_artifact(name):
            skipped.append(name)
            continue
        safe = re.sub(r"[^a-z0-9_-]", "_", name.lower().strip())
        review = f"{rustfs_processed_sap(plant_code_id)}/{safe}/sap_review.parquet"
        source = review if _fs.exists(review) else entity_file
        frame = _read_entity_file(source)
        if frame is not None and not frame.empty:
            tables[name] = frame.to_dict(orient="records")
    _log.info(
        "[context/sap/commit] body had no tables — loaded %d reviewed table(s): %s"
        "%s",
        len(tables), ", ".join(sorted(tables)) or "(none)",
        f"  (skipped {len(skipped)} internal artifact(s): {', '.join(sorted(skipped))})"
        if skipped else "",
    )
    return tables


def _findings_parquet_path(plant_code_id: str, connector: str) -> str:
    """Where one connector's processed findings live."""
    table = _findings.target_table(connector) or f"{connector}_findings"
    return _fs.path_join(
        rustfs_processed_findings(plant_code_id, connector), f"{table}.parquet"
    )


def _read_findings_rows(plant_code_id: str, connector: str, *, required: bool = False) -> list[dict]:
    """Processed findings for review."""
    path = _findings_parquet_path(plant_code_id, connector)
    if not _fs.exists(path):
        if required:
            raise ReviewedStateMissing(path)
        return []
    frame = _read_entity_file(path)
    if frame is None or frame.empty:
        return []
    return frame.to_dict(orient="records")


def _write_findings_rows(plant_code_id: str, connector: str, rows: list[dict]) -> str:
    """Overwrite the processed findings parquet with the reviewer's rows."""
    path = _findings_parquet_path(plant_code_id, connector)
    frame = pd.DataFrame(rows or [])
    _fs.write_parquet(frame, path)
    return path


def _match_method_counts(rows: list[dict]) -> dict[str, int]:
    """How many rows were identified by each method, for the review header."""
    counts: dict[str, int] = {}
    for row in rows:
        method = str(row.get("asset_match_method") or "unmatched")
        counts[method] = counts.get(method, 0) + 1
    return counts


_FINDINGS_DB_COLUMNS: dict[str, set[str]] = {}


def _findings_db_columns(connector: str) -> set[str]:
    """Columns the canonical table actually has, so review extras never break the insert."""
    table = _findings.target_table(connector)
    if table not in _FINDINGS_DB_COLUMNS:
        schema_cfg = load_yaml(str(SCHEMA_FROZEN_FILE))
        tables = schema_cfg.get("schema") or schema_cfg.get("tables") or {}
        columns = ((tables.get(table) or {}).get("columns") or {}).keys()
        _FINDINGS_DB_COLUMNS[table] = set(columns)
    return _FINDINGS_DB_COLUMNS[table]


def _findings_db_row(connector: str, row: dict) -> dict:
    """Drop pipeline-only columns the canonical table does not declare."""
    allowed = _findings_db_columns(connector)
    if not allowed:
        return dict(row)
    return {k: v for k, v in row.items() if k in allowed}


@router.get(
    "/getPnidContext",
    summary="Get processed P&ID data for review",
    description="Read processed equipment_pid and equipment_connectivity from RustFS "
    "along with uploaded P&ID images. The processed output is kept after commit, "
    "so a committed P&ID stays readable for reference.",
    status_code=HTTPStatus.OK,
)
def getPnidContext(
    upload_batch_id: str | None = None,
    files: str | None = None,
    pipeline_job_id: str | None = None,
    plant_code_id: str = Query(..., description="Plant code ID for the request"),
):

    errors = []
    if not plant_code_id:
        errors.append({"field": "plant_code_id", "message": "Plant Code is required"})
    if errors:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": errors,
            },
        )

    plant_code_id = plant_code_id

    try:
        """Plant-scoped review data for P&ID.

        Scoping precedence (see _filter_rows_by_batch):
          1. ``upload_batch_id`` — canonical. Filters by the per-run ``upload_batch_id``
             stamped on every processed row. Durable, re-upload-safe.
          2. ``files`` — deprecated fallback for parquets written before batch ids
             (tolerant filename match).
          3. neither — return everything for the plant (legacy/unscoped).

        ``pipeline_job_id`` is accepted but ignored for scoping (kept so old callers don't
        400); pass ``upload_batch_id`` instead.
        """
        from ..plants import validate_plant_code as _vp

        _vp(plant_code_id)
        _log.info(
            "[context/pnid] GET /pnid — plant=%s  upload_batch_id=%s",
            plant_code_id,
            upload_batch_id or "(none)",
        )
        equipment_pid: list[dict] = []
        equipment_connectivity: list[dict] = []
        tables_found: list[str] = []

        pnid_processed = rustfs_processed_pnid(plant_code_id)
        ep_path = f"{pnid_processed}/equipment_pid.parquet"
        ec_path = f"{pnid_processed}/equipment_connectivity.parquet"
        _log.debug("[context/pnid] RustFS paths: ep=%s  ec=%s", ep_path, ec_path)

        ep_df = _read_parquet_from_rustfs(ep_path)
        if not ep_df.empty:
            equipment_pid = ep_df.to_dict(orient="records")
            tables_found.append("equipment_pid")
            _log.debug(
                "[context/pnid] equipment_pid from RustFS: %d rows  cols=%s",
                len(equipment_pid),
                list(ep_df.columns),
            )

        ec_df = _read_parquet_from_rustfs(ec_path)
        if not ec_df.empty:
            equipment_connectivity = ec_df.to_dict(orient="records")
            tables_found.append("equipment_connectivity")
            _log.debug(
                "[context/pnid] equipment_connectivity from RustFS: %d rows  cols=%s",
                len(equipment_connectivity),
                list(ec_df.columns),
            )

        if not equipment_pid and not equipment_connectivity:

            _log.debug(
                "[context/pnid] processed_data empty — falling back to canonical out_dir"
            )
            pnid_dir = _resolve_stage_dir("pnid", plant_code_id)
            entity_dir = _fs.path_join(pnid_dir, "entities")
            ep_df = _read_entity_file(_fs.path_join(entity_dir, "equipment_pid.csv"))
            if not ep_df.empty:
                equipment_pid = ep_df.to_dict(orient="records")
                tables_found.append("equipment_pid")
                _log.debug(
                    "[context/pnid] equipment_pid from out_dir: %d rows",
                    len(equipment_pid),
                )
            for name in ["equipment_connectivity", "equipment_connection"]:
                ec_df = _read_entity_file(_fs.path_join(entity_dir, f"{name}.csv"))
                if not ec_df.empty:
                    equipment_connectivity = ec_df.to_dict(orient="records")
                    tables_found.append(name)
                    _log.debug(
                        "[context/pnid] %s from out_dir: %d rows",
                        name,
                        len(equipment_connectivity),
                    )
                    break

        pnid_images = _list_rustfs_pnid_images(plant_code_id)
        _log.debug("[context/pnid] pnid_images listed: %d", len(pnid_images))

        scope = "none"
        requested_files: list[str] = []
        if upload_batch_id:
            ep_before, ec_before = len(equipment_pid), len(equipment_connectivity)
            equipment_pid = _filter_rows_by_batch(equipment_pid, upload_batch_id)
            equipment_connectivity = _filter_rows_by_batch(
                equipment_connectivity, upload_batch_id
            )
            applied = (
                len(equipment_pid) != ep_before
                or len(equipment_connectivity) != ec_before
                or any("upload_batch_id" in r for r in equipment_pid)
                or any("upload_batch_id" in r for r in equipment_connectivity)
            )
            if applied:
                scope = "upload_batch_id"
                kept_files = {
                    _norm_fname(r.get("source_file") or "")
                    for r in equipment_pid + equipment_connectivity
                }
                pnid_images = [
                    im
                    for im in pnid_images
                    if _norm_fname(im.get("filename") or "") in kept_files
                ] or pnid_images
            _log.debug(
                "[context/pnid] upload_batch_id filter → ep=%d ec=%d (applied=%s)",
                len(equipment_pid),
                len(equipment_connectivity),
                applied,
            )

        if scope == "none" and files:
            requested_files = [
                name.strip() for name in files.split(",") if name.strip()
            ]
            if requested_files:
                equipment_pid, equipment_connectivity, pnid_images = (
                    _filter_pnid_context_by_files(
                        equipment_pid,
                        equipment_connectivity,
                        pnid_images,
                        requested_files,
                    )
                )
                scope = "files"
            _log.debug(
                "[context/pnid] files= fallback requested_files=%s", requested_files
            )

        _src_by_norm = {
            _norm_fname(str(r.get("source_file") or "")): str(
                r.get("source_file") or ""
            ).strip()
            for r in (equipment_pid + equipment_connectivity)
            if str(r.get("source_file") or "").strip()
        }
        for im in pnid_images:
            canonical = _src_by_norm.get(_norm_fname(im.get("filename") or ""))
            if canonical and canonical != im.get("filename"):
                im["filename"] = canonical
                im["stem"] = os.path.splitext(canonical)[0]

        connectivity_by_file: dict[str, list[dict]] = {}
        for row in equipment_connectivity:
            src_file = str(row.get("source_file", row.get("_source_table", ""))).strip()
            connectivity_by_file.setdefault(src_file, []).append(row)

        _log.info(
            "[context/pnid] returning: equipment_pid=%d  equipment_connectivity=%d  images=%d  scope=%s",
            len(equipment_pid),
            len(equipment_connectivity),
            len(pnid_images),
            scope,
        )
        result = {
            "source": "pnid",
            "plant_code_id": plant_code_id,
            "upload_batch_id": upload_batch_id,
            "scope": scope,
            "equipment_pid": equipment_pid,
            "equipment_connectivity": equipment_connectivity,
            "connectivity_by_file": connectivity_by_file,
            "rows": equipment_connectivity,
            "pnid_images": pnid_images,
            "tables": tables_found,
            "requested_files": requested_files,
            "total": len(equipment_pid) + len(equipment_connectivity),
        }
        return {
                "success": True,
                "message": "P&ID context retrieved successfully",
                "data": result,
            }

    except HTTPException:
        raise
    except Exception as exc:
        return server_error(exc, action="loading pnid context")


@router.get(
    "/servePnidFile/{filename:path}",
    summary="Serve a P&ID file from RustFS or local staging",
    description="Returns the uploaded P&ID PDF/image for preview in the UI. "
    "If ``plant_code_id`` is provided we look only inside that plant's "
    "staging prefix; otherwise we scan registered plants in order "
    "and return the first match (the historical pre-plant_code_id "
    "staging-pt/pnid/ root is also checked as a final fallback).",
    status_code=HTTPStatus.OK,
)
def servePnidFile(
    filename: str,
    plant_code_id: str = Query(..., description="Plant code ID for the request"),
):

    errors = []
    if not plant_code_id:
        errors.append({"field": "plant_code_id", "message": "Plant Code is required"})
    if errors:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": errors,
            },
        )

    plant_code_id = plant_code_id

    try:

        safe_name = Path(filename).name
        if safe_name != filename or ".." in filename or "/" in filename:
            return JSONResponse(
                status_code=HTTPStatus.BAD_REQUEST,
                content={
                    "success": False,
                    "message": "Failed to serve P&ID file",
                    "errors": [{"field": "filename", "message": "Invalid filename"}],
                },
            )
        if not re.match(r"^[\w\-. ()&,'+#]+$", safe_name):
            return JSONResponse(
                status_code=HTTPStatus.BAD_REQUEST,
                content={
                    "success": False,
                    "message": "Failed to serve P&ID file",
                    "errors": [{"field": "filename", "message": "Invalid filename characters"}],
                },
            )

        def _serve_from_rustfs(rustfs_handle, key: str):
            with rustfs_handle.open(key, "rb") as fh:
                content = fh.read()
            suffix = Path(safe_name).suffix.lower()
            mime = (
                "application/pdf" if suffix == ".pdf" else f"image/{suffix.lstrip('.')}"
            )
            return StreamingResponse(
                io.BytesIO(content),
                media_type=mime,
                headers={"Content-Disposition": f'inline; filename="{safe_name}"'},
            )

        sfs = _object_fs()
        if sfs:
            candidate_prefixes: list[str] = []
            if plant_code_id:
                candidate_prefixes.append(
                    _fs.strip_scheme(rustfs_staging_pnid(plant_code_id))
                )
            else:
                try:
                    from ..plants import list_plants

                    for p in list_plants():
                        code = p.get("plant_code_id")
                        if code:
                            candidate_prefixes.append(
                                _fs.strip_scheme(rustfs_staging_pnid(code))
                            )
                except Exception as exc:
                    _log.warning("[context/pnid] plant scan failed: %s", exc)

            for prefix in candidate_prefixes:
                key = f"{prefix}/{safe_name}"
                try:
                    if sfs.exists(key):
                        return _serve_from_rustfs(sfs, key)
                except Exception:
                    continue

        file_path = STAGING_DIR / "pnid" / safe_name
        if file_path.exists() and file_path.is_file():
            return FileResponse(file_path)

        return JSONResponse(
            status_code=HTTPStatus.NOT_FOUND,
            content={
                "success": False,
                "message": "Failed to serve P&ID file",
                "errors": [{"field": "filename", "message": f"File not found: {safe_name}"}],
            },
        )

    except HTTPException:
        raise
    except Exception as exc:
        return server_error(exc, action="serve pnid file")


















@router.patch(
    "/patchPnidRows",
    summary="Persist edits / deletes on the P&ID review table",
    description="Overwrite the processed_data/<plant>/pnid parquet files "
    "with the supplied rows so the user's review-page edits "
    "survive navigation. Body shape mirrors the commit payload "
    "({plant_code_id, equipment_pid, equipment_connectivity}).",
    status_code=HTTPStatus.OK,
)
def patchPnidRows(
    body: dict = Body(...),

):


    try:
        plant_code_id = body.get("plant_code_id")
        if not plant_code_id:
            return JSONResponse(
                status_code=HTTPStatus.BAD_REQUEST,
                content={
                    "success": False,
                    "message": "Validation failed",
                    "errors": [{"field": "plant_code_id", "message": "plant_code_id is required"}],
                },
            )
        from ..plants import validate_plant_code as _vp

        _vp(plant_code_id)
        ep_rows = body.get("equipment_pid") or []
        ec_rows = body.get("equipment_connectivity") or []
        ep_rows, ec_rows = _retransform_pnid_rows(plant_code_id, ep_rows, ec_rows)
        written = _write_processed_pnid(plant_code_id, ep_rows, ec_rows)
        _log.info(
            "[context/pnid/rows] PATCH — plant=%s  ep=%d  ec=%d",
            plant_code_id,
            len(ep_rows),
            len(ec_rows),
        )
        _reviewed_files = (
            sorted(
                {
                    str(r.get("source_file")).strip()
                    for r in (ep_rows + ec_rows)
                    if r.get("source_file")
                }
            )
            or None
        )
        _advance_flow(
            plant_code_id,
            "pnid",
            file_names=_reviewed_files,
            stage="reviewed",
            status="done",
            review_patches=len(ep_rows) + len(ec_rows),
        )
        result = {
            "status": "ok",
            "written": written,
            "counts": {
                "equipment_pid": len(ep_rows),
                "equipment_connectivity": len(ec_rows),
            },
        }
        return {
                "success": True,
                "message": "P&ID rows patched successfully",
                "data": result,
            }

    except HTTPException:
        raise
    except Exception as exc:
        return server_error(exc, action="updating pnid rows")


@router.patch(
    "/patchDocsRows",
    summary="Persist edits / deletes on the Documents review table",
    description="Overwrite the processed_data/<plant>/documents/<subtype>/"
    "docs_documents.parquet files. Rows are partitioned by "
    "their ``document_type`` field.",
    status_code=HTTPStatus.OK,
)
def patchDocsRows(
    body: dict = Body(...),

):


    try:
        plant_code_id = body.get("plant_code_id")
        if not plant_code_id:
            return JSONResponse(
                status_code=HTTPStatus.BAD_REQUEST,
                content={
                    "success": False,
                    "message": "Validation failed",
                    "errors": [{"field": "plant_code_id", "message": "plant_code_id is required"}],
                },
            )
        from ..plants import validate_plant_code as _vp

        _vp(plant_code_id)
        rows = body.get("rows") or []
        rows = _retransform_docs_rows(plant_code_id, rows)
        written = _write_processed_docs(plant_code_id, rows)
        _log.info(
            "[context/docs/rows] PATCH — plant=%s  rows=%d  subtypes=%d",
            plant_code_id,
            len(rows),
            len(written),
        )
        _reviewed_files = (
            sorted(
                {
                    str(r.get("source_file")).strip()
                    for r in rows
                    if r.get("source_file")
                }
            )
            or None
        )
        _advance_flow(
            plant_code_id,
            "docs",
            file_names=_reviewed_files,
            stage="reviewed",
            status="done",
            review_patches=len(rows),
        )
        result = {"status": "ok", "written": written, "row_count": len(rows)}
        return {
                "success": True,
                "message": "Document rows patched successfully",
                "data": result,
            }

    except HTTPException:
        raise
    except Exception as exc:
        return server_error(exc, action="updating docs rows")


@router.patch(
    "/patchTsRows",
    summary="Persist edits / deletes on the Timeseries review table",
    description="Overwrite the processed_data/<plant>/timeseries/"
    "ts_timeseries_metadata.parquet with the supplied rows.",
    status_code=HTTPStatus.OK,
)
def patchTsRows(
    body: dict = Body(...),

):


    try:
        plant_code_id = body.get("plant_code_id")
        if not plant_code_id:
            return JSONResponse(
                status_code=HTTPStatus.BAD_REQUEST,
                content={
                    "success": False,
                    "message": "Validation failed",
                    "errors": [{"field": "plant_code_id", "message": "plant_code_id is required"}],
                },
            )
        from ..plants import validate_plant_code as _vp

        _vp(plant_code_id)
        rows = body.get("rows") or []
        rows = _retransform_ts_rows(plant_code_id, rows)
        written = _write_processed_ts(plant_code_id, rows)
        _log.info("[context/ts/rows] PATCH — plant=%s  rows=%d", plant_code_id, len(rows))
        _reviewed_files = (
            sorted(
                {
                    str(r.get("source_file")).strip()
                    for r in rows
                    if r.get("source_file")
                }
            )
            or None
        )
        _advance_flow(
            plant_code_id,
            "ts",
            file_names=_reviewed_files,
            stage="reviewed",
            status="done",
            review_patches=len(rows),
        )
        result = {"status": "ok", "written": written, "row_count": len(rows)}
        return {
                "success": True,
                "message": "Timeseries rows patched successfully",
                "data": result,
            }

    except HTTPException:
        raise
    except Exception as exc:
        return server_error(exc, action="updating ts rows")


@router.patch(
    "/patchSapRows",
    summary="Persist edits / deletes on the SAP review table (per table)",
    description="Overwrite the processed_data/<plant>/sap/<table>/sap_review.parquet "
    "with the supplied rows so the user's review-page edits to a SAP "
    "table survive navigation and feed the subsequent commit. Body: "
    "{plant_code_id, table, rows}.",
    status_code=HTTPStatus.OK,
)
def patchSapRows(
    body: dict = Body(...),

):


    try:
        plant_code_id = body.get("plant_code_id")
        if not plant_code_id:
            return JSONResponse(
                status_code=HTTPStatus.BAD_REQUEST,
                content={
                    "success": False,
                    "message": "Validation failed",
                    "errors": [{"field": "plant_code_id", "message": "plant_code_id is required"}],
                },
            )
        from ..plants import validate_plant_code as _vp

        _vp(plant_code_id)
        table = str(body.get("table") or "").strip()
        if not table:
            return JSONResponse(
                status_code=HTTPStatus.BAD_REQUEST,
                content={
                    "success": False,
                    "message": "Validation failed",
                    "errors": [{"field": "table", "message": "table is required"}],
                },
            )
        rows = body.get("rows") or []
        written = _write_processed_sap(plant_code_id, table, rows)
        _log.info(
            "[context/sap/rows] PATCH — plant=%s  table=%s  rows=%d",
            plant_code_id,
            table,
            len(rows),
        )
        result = {"status": "ok", "written": written, "row_count": len(rows)}
        return {
                "success": True,
                "message": "SAP rows patched successfully",
                "data": result,
            }

    except HTTPException:
        raise
    except Exception as exc:
        return server_error(exc, action="updating sap rows")



@router.patch(
    "/patchOtherFilesRows",
    summary="Persist edits on an Other Files review table",
    description=(
        "Overwrite the reviewed user-defined Other Files table. "
        "Body: {plant_code_id, table, rows}."
    ),
    status_code=HTTPStatus.OK,
)
def patchOtherFilesRows(
    body: dict = Body(...),
):
    try:
        plant_code_id = str(
            body.get("plant_code_id") or ""
        ).strip()

        if not plant_code_id:
            return JSONResponse(
                status_code=HTTPStatus.BAD_REQUEST,
                content={
                    "success": False,
                    "message": "Validation failed",
                    "errors": [
                        {
                            "field": "plant_code_id",
                            "message": "plant_code_id is required",
                        }
                    ],
                },
            )

        table = str(
            body.get("table") or ""
        ).strip()

        if not table:
            return JSONResponse(
                status_code=HTTPStatus.BAD_REQUEST,
                content={
                    "success": False,
                    "message": "Validation failed",
                    "errors": [
                        {
                            "field": "table",
                            "message": "table is required",
                        }
                    ],
                },
            )

        rows = body.get("rows")

        if not isinstance(rows, list):
            return JSONResponse(
                status_code=HTTPStatus.BAD_REQUEST,
                content={
                    "success": False,
                    "message": "Validation failed",
                    "errors": [
                        {
                            "field": "rows",
                            "message": "rows must be a list",
                        }
                    ],
                },
            )

        from ..plants import validate_plant_code as _vp

        _vp(plant_code_id)

        safe_table = re.sub(
            r"[^a-z0-9_-]",
            "_",
            table.lower(),
        ).strip("_")

        if not safe_table:
            return JSONResponse(
                status_code=HTTPStatus.BAD_REQUEST,
                content={
                    "success": False,
                    "message": "Validation failed",
                    "errors": [
                        {
                            "field": "table",
                            "message": "Invalid Other Files table name",
                        }
                    ],
                },
            )

        written = _write_processed_other_files(
            plant_code_id,
            safe_table,
            rows,
        )

        _log.info(
            "[context/other_files/rows] PATCH — plant=%s table=%s rows=%d",
            plant_code_id,
            safe_table,
            len(rows),
        )

        reviewed_files = (
            sorted(
                {
                    str(row.get("source_file")).strip()
                    for row in rows
                    if row.get("source_file")
                }
            )
            or None
        )

        # Other Files uploads live in the existing docs flow_state lifecycle.
        _advance_flow(
            plant_code_id,
            "docs",
            file_names=reviewed_files,
            stage="reviewed",
            status="done",
            review_patches=len(rows),
        )

        result = {
            "status": "ok",
            "table": safe_table,
            "written": written,
            "row_count": len(rows),
        }

        return {
            "success": True,
            "message": "Other Files rows patched successfully",
            "data": result,
        }

    except HTTPException:
        raise
    except Exception as exc:
        return server_error(
            exc,
            action="updating Other Files rows",
        )


@router.post(
    "/commitPnidContext",
    summary="Commit P&ID context to database",
    description="Write reviewed P&ID data to equipment_pid and equipment_connectivity "
    "PostgreSQL tables. If no rows are sent, reads latest data from RustFS. "
    "Cleans up RustFS processed_data/pnid/ after commit. "
    "Returns 200 with a commit_id when rows were written, 204 when the reviewed state is empty so nothing was written, and 409 when the pipeline has not run for this connector yet.",
    status_code=HTTPStatus.OK,
)
def commitPnidContext(
    body: dict = Body(
        ...,
        examples=[
            {
                "plant_code_id": "Plant_1",
                "equipment_pid": [{"pid_tag": "B-1106"}],
                "equipment_connectivity": [
                    {"from_equipment_ref": "B-1106", "to_equipment_ref": "SDV-2930"}
                ],
            }
        ],
    ),
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
):


    try:
        total = 0
        tables_committed = []

        plant_code_id = body.get("plant_code_id")
        if not plant_code_id:
            return JSONResponse(
                status_code=HTTPStatus.BAD_REQUEST,
                content={
                    "success": False,
                    "message": "Validation failed",
                    "errors": [{"field": "plant_code_id", "message": "plant_code_id is required"}],
                },
            )
        from ..plants import validate_plant_code as _vp

        _vp(plant_code_id)
        upload_batch_id = (
            body.get("upload_batch_id")
            or body.get("batch_id")
            or body.get("pipeline_job_id")
        )
        _idem_fingerprint = _idempotency.fingerprint(body)
        _replayed, _conflict = _idempotency.lookup(
            plant_code_id, "commit_pnid", idempotency_key, body
        )
        if _conflict == "body_mismatch":
            return JSONResponse(
                status_code=HTTPStatus.CONFLICT,
                content={
                    "success": False,
                    "message": (
                        "This Idempotency-Key was already used with a different request body."
                    ),
                    "errors": [
                        {"field": "Idempotency-Key", "message": "Key reused with a different body."}
                    ],
                },
            )
        if _replayed is not None:
            return _replayed
        pid_rows = body.get("equipment_pid", [])
        conn_rows = body.get("equipment_connectivity", [])
        _log.info(
            "[context/commit] POST /pnid/commit — plant_code_id=%s  body rows: equipment_pid=%d  equipment_connectivity=%d",
            plant_code_id,
            len(pid_rows),
            len(conn_rows),
        )

        pnid_processed = rustfs_processed_pnid(plant_code_id)
        if not pid_rows:
            _log.debug(
                "[context/commit] equipment_pid empty in body — reading from RustFS"
            )
            ep_df = _read_parquet_from_rustfs(f"{pnid_processed}/equipment_pid.parquet")
            if not ep_df.empty:
                pid_rows = ep_df.to_dict(orient="records")
                _log.debug(
                    "[context/commit] equipment_pid loaded from RustFS: %d rows",
                    len(pid_rows),
                )

        if not conn_rows:
            _log.debug(
                "[context/commit] equipment_connectivity empty in body — reading from RustFS"
            )
            ec_df = _read_parquet_from_rustfs(
                f"{pnid_processed}/equipment_connectivity.parquet"
            )
            if not ec_df.empty:
                conn_rows = ec_df.to_dict(orient="records")
                _log.debug(
                    "[context/commit] equipment_connectivity loaded from RustFS: %d rows",
                    len(conn_rows),
                )

        pid_rows = _apply_plant_code(pid_rows, plant_code_id)
        conn_rows = _apply_plant_code(conn_rows, plant_code_id)

        if not pid_rows and not conn_rows:
            return nothing_to_commit("pnid", plant_code_id)

        for table_name, rows_var in [
            ("equipment_pid", pid_rows),
            ("equipment_connectivity", conn_rows),
        ]:
            if not rows_var:
                continue
            _log.debug(
                "[context/commit] writing %s to DB: %d rows", table_name, len(rows_var)
            )
            try:
                conn = _get_conn()
                try:
                    cur = conn.cursor()
                    deleted, replaced_files, original_created = _replace_by_source_file(
                        cur,
                        table_name,
                        rows_var,
                        plant_code_id,
                    )
                    conn.commit()
                finally:
                    conn.close()
                if replaced_files:
                    _log.info(
                        "[context/commit] %s replace-by-source_file: deleted=%d  files=%d  preserved created_at for=%d",
                        table_name,
                        deleted,
                        len(replaced_files),
                        len(original_created),
                    )

                _stamp_commit_timestamps(rows_var, original_created)
                for _r in rows_var:
                    _ia = _r.get("is_active")
                    if _ia is None or _ia == "":
                        _r["is_active"] = True
                    elif isinstance(_ia, str):
                        _r["is_active"] = _ia.strip().lower() in (
                            "true",
                            "t",
                            "1",
                            "yes",
                            "y",
                        )
                count = _write_to_db(table_name, rows_var, plant_code_id=plant_code_id)
                total += count
                dropped = len(rows_var) - count
                if dropped:
                    _log.warning(
                        "[context/commit] %s: %d of %d row(s) did not land",
                        table_name,
                        dropped,
                        len(rows_var),
                    )
                tables_committed.append(
                    {
                        "table": table_name,
                        "rows_written": count,
                        "rows_dropped": dropped,
                        "rows_replaced": deleted,
                        "files_replaced": sorted(replaced_files)[:5],
                    }
                )
                _log.info(
                    "[context/commit] %s replaced+wrote DB: %d row(s) (deleted prior %d)",
                    table_name,
                    count,
                    deleted,
                )
            except Exception as db_exc:
                _log.error(
                    "[context/commit] DB write FAILED for %s — 0 rows written: %s",
                    table_name,
                    db_exc,
                )
                tables_committed.append(
                    {
                        "table": table_name,
                        "rows_written": 0,
                        "rows_dropped": len(rows_var),
                        "warning": human_message(
                            db_exc,
                            action="committing pnid context",
                            plant_code_id=plant_code_id,
                        ),
                    }
                )


        _log.info(
            "[context/commit] COMMIT COMPLETE — total_rows=%d  tables=%s",
            total,
            [t["table"] for t in tables_committed],
        )
        _commit_uid = _record_commit_audit(
            "pnid",
            plant_code_id,
            tables_committed,
            total,
            None,
        )
        _committed_pnid_files = (
            sorted(
                {
                    str(r.get("source_file")).strip()
                    for r in (pid_rows + conn_rows)
                    if r.get("source_file")
                }
            )
            or None
        )
        _advance_flow(
            plant_code_id,
            "pnid",
            file_names=_committed_pnid_files,
            stage="committed",
            status="done",
            commit_uid=_commit_uid,
            rows_committed=total,
        )
        result = {
            "status": "committed",
            "commit_id": _commit_uid,
            "tables": tables_committed,
            "total_rows_written": total,
        }
        result = normalise_commit(
            result,
            connector="pnid",
            commit_id=_commit_uid,
            upload_batch_id=upload_batch_id,
            committed_by=get_actor(),
        )
        _response = {
            "success": True,
            "message": "P&ID context committed successfully",
            "data": result,
        }
        _idempotency.remember(plant_code_id, "commit_pnid", idempotency_key, body, _response,
            request_fingerprint=_idem_fingerprint,
        )
        return _response

    except HTTPException:
        raise
    except Exception as exc:
        return server_error(exc, action="committing pnid context")














@router.get(
    "/getDocsContext",
    summary="Get processed document data for review",
    description="Read processed docs from RustFS (s3://bucket/processed_data/documents/) "
    "for user review before committing to the database. Falls back to local "
    "OUT_DIR/docs/entities/ if RustFS data is not available.",
    status_code=HTTPStatus.OK,
)
def getDocsContext(
    upload_batch_id: str | None = Query(
        None,
        description="Batch id to scope the review to. Accepts a comma-separated list "
        "so a multi-subtype upload can be reviewed in one call.",
    ),
    files: str | None = None,
    doc_types: str | None = None,
    pipeline_job_id: str | None = None,
    plant_code_id: str = Query(..., description="Plant code ID for the request"),
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=1000),
):

    errors = []
    if not plant_code_id:
        errors.append({"field": "plant_code_id", "message": "Plant Code is required"})
    if errors:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": errors,
            },
        )

    plant_code_id = plant_code_id

    try:
        """Plant-scoped review data for documents.

        Scoping precedence:
          1. ``upload_batch_id`` — canonical per-run filter (upload_batch_id column).
             One id, or several comma-separated: the per-subtype upload journey
             mints one batch per subtype, and all of them are one review.
          2. ``files`` — deprecated filename fallback for legacy parquets.
          3. ``doc_types`` — optional subtype narrowing (reads only matching dirs).

        Grouping is done HERE, not in the client: every row is stamped with a
        normalised ``category`` and the response includes ``rows_by_category`` plus
        an ordered ``categories`` tab list, so the review tabs are identical no
        matter which client renders them. ``categories`` counts the whole scoped
        set so the tabs are stable across pages; ``rows_by_category`` holds only
        the rows on this page, so it always agrees with ``rows``. ``pipeline_job_id`` is accepted but ignored for
        scoping (pass ``upload_batch_id``).
        """
        from ..plants import validate_plant_code as _vp

        _vp(plant_code_id)
        requested_types = (
            [t.strip() for t in doc_types.split(",")] if doc_types else None
        )
        requested_types = [t for t in (requested_types or []) if t] or None
        _log.info(
            "[context/docs] GET /docs — plant=%s  upload_batch_id=%s  doc_types=%s",
            plant_code_id,
            upload_batch_id or "(none)",
            requested_types or "(all)",
        )
        rows: list[dict] = []
        tables_found: list[str] = []

        rows = _list_docs_from_rustfs(plant_code_id, requested_types)
        if rows:
            tables_found.append("docs_documents")
            _log.info("[context/docs] %d rows from RustFS", len(rows))
        else:

            _log.debug(
                "[context/docs] processed_data empty — falling back to canonical out_dir"
            )
            docs_dir = _resolve_stage_dir("docs", plant_code_id)
            entity_dir = _fs.path_join(docs_dir, "entities")
            for entity_file in _glob_entity_files(entity_dir):
                df = _read_entity_file(entity_file)
                if not df.empty:
                    rows.extend(df.to_dict(orient="records"))
                    tables_found.append(Path(entity_file).stem)
                    _log.debug(
                        "[context/docs] %s from out_dir: %d rows",
                        Path(entity_file).stem,
                        len(df),
                    )

        scope = "none"
        requested_files: list[str] = []
        if upload_batch_id:
            before = len(rows)
            scoped = _filter_rows_by_batch(rows, upload_batch_id)
            applied = (len(scoped) != before) or any(
                "upload_batch_id" in r for r in rows
            )
            if applied:
                rows = scoped
                scope = "upload_batch_id"
            _log.debug(
                "[context/docs] upload_batch_id filter → %d/%d rows (applied=%s)",
                len(rows),
                before,
                applied,
            )

        if scope == "none" and files:
            requested_files = [
                name.strip() for name in files.split(",") if name.strip()
            ]
            if requested_files:
                wanted = {_norm_fname(name) for name in requested_files}
                before = len(rows)
                rows = [
                    r for r in rows if _norm_fname(r.get("source_file") or "") in wanted
                ]
                scope = "files"
                _log.debug(
                    "[context/docs] files= fallback %s → %d/%d rows",
                    requested_files,
                    len(rows),
                    before,
                )

        rows_by_category, categories = _group_docs_by_category(rows)

        _log.info(
            "[context/docs] returning %d rows  categories=%d  scope=%s",
            len(rows),
            len(categories),
            scope,
        )
        if rows:
            _advance_flow_to_reviewing(plant_code_id, "docs")
        result = {
            "source": "docs",
            "plant_code_id": plant_code_id,
            "upload_batch_id": upload_batch_id,
            "upload_batch_ids": sorted(_batch_id_set(upload_batch_id)),
            "scope": scope,
            "rows": rows,
            "rows_by_category": rows_by_category,
            "categories": categories,
            "total": len(rows),
            "tables": tables_found,
            "requested_files": requested_files,
        }
        envelope = paginated_envelope(
            result,
            items_key="rows",
            page=page,
            page_size=page_size,
            message="Documents context retrieved successfully",
            empty_message=(
                f"No document rows are staged for review for plant '{plant_code_id}'. "
                "Upload documents and run the docs pipeline first."
            ),
        )
        page_grouped, _ = _group_docs_by_category(envelope["data"]["rows"])
        envelope["data"]["rows_by_category"] = page_grouped
        return envelope

    except HTTPException:
        raise
    except Exception as exc:
        return server_error(exc, action="loading docs context")


_DOC_COL_MAP = {
    "id": "document_id",
    "type": "document_type",
    "equip": "equipment_tag",
    "entities": "extracted_entities",
    "relationships": "extracted_relationships",
    "source": "source_file",
}

_DOC_CANONICAL = {
    "plant_code_id",
    "site",
    "document_id",
    "record_id",
    "document_type",
    "title",
    "equipment_tag",
    "equipment_id",
    "extracted_entities",
    "extracted_relationships",
    "source_file",
    "file_reference",
    "chunk_index",
    "page_start",
    "page_end",
    "source_pages",
    "normalized_asset",
    "asset_match_method",
    "asset_match_confidence",
    "source_system",
    "source_record_id",
    "ingested_at",
    "updated_at",
    "is_active",
}

_DOC_TEXT_FIELDS = (
    "document_id",
    "record_id",
    "document_type",
    "title",
    "equipment_tag",
    "equipment_id",
    "extracted_entities",
    "extracted_relationships",
    "source_file",
    "file_reference",
    "source_pages",
    "normalized_asset",
    "asset_match_method",
    "source_system",
    "source_record_id",
)


def _normalise_doc_row(row: dict) -> dict:
    """Reduce one reviewed document row to the columns the CDM table holds."""
    out: dict = {}
    for k, v in row.items():
        canon = _DOC_COL_MAP.get(k, k)
        if canon in _DOC_CANONICAL:
            out[canon] = v

    for _alias, _canon in _DOC_COL_MAP.items():
        if _alias in row and _canon in _DOC_CANONICAL:
            out[_canon] = row[_alias]
    for col in _DOC_CANONICAL:
        out.setdefault(col, None)
    for _f in _DOC_TEXT_FIELDS:
        if isinstance(out.get(_f), str):
            out[_f] = out[_f].strip()

    _ia = out.get("is_active")
    if _ia is None or _ia == "":
        out["is_active"] = True
    elif isinstance(_ia, str):
        out["is_active"] = _ia.strip().lower() in ("true", "t", "1", "yes", "y")

    _site = out.get("site")
    out["site"] = str(_site).strip() if _site not in (None, "") else "-"
    return out


_DOC_COMMIT_MODES = ("replace_scope", "patch")


def _parse_doc_commit_mode(body) -> tuple[str, list[dict], list[dict]]:
    """Read the commit envelope, defaulting to the whole-scope replace clients already send."""
    envelope = body if isinstance(body, dict) else {}
    mode = str(envelope.get("mode") or _DOC_COMMIT_MODES[0]).strip().lower()
    errors = []
    if mode not in _DOC_COMMIT_MODES:
        errors.append(
            {
                "field": "mode",
                "message": f"mode must be one of {', '.join(_DOC_COMMIT_MODES)}",
            }
        )

    deletes = envelope.get("deletes") or []
    if not isinstance(deletes, list) or any(not isinstance(d, dict) for d in deletes):
        errors.append(
            {"field": "deletes", "message": "deletes must be a list of row identities"}
        )
        deletes = []
    if deletes and mode != "patch":
        errors.append(
            {"field": "deletes", "message": "deletes are only accepted when mode is patch"}
        )

    missing = [d for d in deletes if not str(d.get("record_id") or "").strip()]
    if missing:
        errors.append(
            {"field": "deletes", "message": "every delete needs a record_id"}
        )

    identities = []
    for d in deletes:
        site = str(d.get("site") or "").strip() or "-"
        identities.append(
            {
                "site": site,
                "record_id": str(d.get("record_id") or "").strip(),
                "row_version": d.get("row_version"),
            }
        )
    return mode, identities, errors


def _doc_version_conflict(stale: list[dict]) -> JSONResponse:
    """Refuse a batch whose rows moved under the reviewer since they were read."""
    return JSONResponse(
        status_code=HTTPStatus.CONFLICT,
        content={
            "success": False,
            "message": (
                f"{len(stale)} row(s) changed since they were loaded for review. "
                "Reload the rows and re-apply the edits."
            ),
            "errors": [
                {
                    "field": s["record_id"],
                    "message": (
                        f"row_version is now {s['current_row_version']}, "
                        f"the edit was made against {s['expected_row_version']}"
                    ),
                }
                for s in stale
            ],
            "data": {"conflicts": stale},
        },
    )


@router.post(
    "/commitDocsContext",
    summary="Commit documents context to database",
    description="Write reviewed document data into the existing document_metadata PostgreSQL "
    "table. Default mode 'replace_scope' replaces every row of the scope and reads from RustFS when rows is empty. "
    "Mode 'patch' upserts only the rows sent, removes the identities listed in 'deletes', and never reads from RustFS. "
    "Cleans up RustFS processed_data/documents/ after commit. "
    "Returns 200 with a commit_id when rows were written, 204 when the reviewed state is empty so nothing was written, 409 when a patched row moved since it was read or the pipeline has not run for this connector yet, and 422 when the envelope is malformed.",
    status_code=HTTPStatus.OK,
)
def commitDocsContext(
    body=Body(...),
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
):


    try:
        plant_code_id, upload_batch_id, rows = _parse_commit_body(body, "docs")
        mode, deletes, mode_errors = _parse_doc_commit_mode(body)
        if mode_errors:
            return JSONResponse(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
                content={
                    "success": False,
                    "message": "Validation failed",
                    "errors": mode_errors,
                },
            )
        _idem_fingerprint = _idempotency.fingerprint(body)
        _replayed, _conflict = _idempotency.lookup(
            plant_code_id, "commit_docs", idempotency_key, body
        )
        if _conflict == "body_mismatch":
            return JSONResponse(
                status_code=HTTPStatus.CONFLICT,
                content={
                    "success": False,
                    "message": (
                        "This Idempotency-Key was already used with a different request body."
                    ),
                    "errors": [
                        {"field": "Idempotency-Key", "message": "Key reused with a different body."}
                    ],
                },
            )
        if _replayed is not None:
            return _replayed
        _log.info(
            "[context/docs/commit] POST /docs/commit — plant_code_id=%s  upload_batch_id=%s  body rows: %d",
            plant_code_id,
            upload_batch_id or "(none)",
            len(rows),
        )

        if not rows and mode != "patch":
            rows = _list_docs_from_rustfs(plant_code_id)
            if upload_batch_id:
                rows = _filter_rows_by_batch(rows, upload_batch_id)
            _log.info(
                "[context/docs/commit] body had no rows — loaded %d reviewed row(s) "
                "(upload_batch_id=%s)",
                len(rows), upload_batch_id or "(none)",
            )
        if not rows and not deletes:
            return nothing_to_commit("documents", plant_code_id)

        rows = _apply_plant_code(rows, plant_code_id)

        normalised = [_normalise_doc_row(r) for r in rows]

        if mode == "patch":
            for _raw, _norm in zip(rows, normalised):
                _norm["row_version"] = _raw.get("row_version")
            stale = _stale_doc_rows(normalised + deletes, plant_code_id)
            if stale:
                _log.warning(
                    "[context/docs/commit] refused: %d stale row(s)", len(stale)
                )
                return _doc_version_conflict(stale)

        total = 0
        deleted = 0
        tables_committed: list[dict] = []
        try:
            if mode == "patch":
                inserted, updated, unchanged = _upsert_doc_metadata(
                    normalised, plant_code_id=plant_code_id, editor=get_actor()
                )
                deleted, not_found = _delete_doc_rows(deletes, plant_code_id=plant_code_id)
                count = inserted + updated
                total = count
                tables_committed.append(
                    {
                        "table": "document_metadata",
                        "rows_written": count,
                        "rows_inserted": inserted,
                        "rows_updated": updated,
                        "rows_unchanged": unchanged,
                        "rows_deleted": deleted,
                        "deletes_not_found": len(not_found),
                    }
                )
                _log.info(
                    "[context/docs/commit] patch: inserted=%d updated=%d unchanged=%d deleted=%d",
                    inserted, updated, unchanged, deleted,
                )
            else:
                count = _write_to_doc_metadata_table(normalised, plant_code_id=plant_code_id)
                total = count
                tables_committed.append(
                    {"table": "document_metadata", "rows_written": count}
                )
                _log.info("[context/docs/commit] document_metadata: %d rows written", count)
        except Exception as db_exc:
            _log.warning("[context/docs/commit] DB write skipped: %s", db_exc)
            tables_committed.append(
                {
                    "table": "document_metadata",
                    "rows_written": 0,
                    "warning": str(db_exc),
                }
            )


        _log.info("[context/docs/commit] COMMIT COMPLETE — total=%d", total)
        _commit_uid = _record_commit_audit(
            "docs", plant_code_id, tables_committed, total, None
        )
        _had_warning = any(t.get("warning") for t in tables_committed)
        _committed_doc_files = (
            sorted(
                {
                    str(r.get("source_file")).strip()
                    for r in rows
                    if r.get("source_file")
                }
            )
            or None
        )
        _advance_flow(
            plant_code_id,
            "docs",
            file_names=_committed_doc_files,
            stage="committed",
            status="done" if not _had_warning else "failed",
            commit_uid=_commit_uid,
            rows_committed=total,
        )
        result = {
            "status": "committed",
            "commit_id": _commit_uid,
            "mode": mode,
            "tables": tables_committed,
            "total_rows_written": total,
            "total_rows_deleted": deleted,
        }
        result = normalise_commit(
            result,
            connector="documents",
            commit_id=_commit_uid,
            upload_batch_id=upload_batch_id,
            committed_by=get_actor(),
        )
        _response = {
            "success": True,
            "message": "Documents context committed successfully",
            "data": result,
        }
        _idempotency.remember(plant_code_id, "commit_docs", idempotency_key, body, _response,
            request_fingerprint=_idem_fingerprint,
        )
        return _response

    except HTTPException:
        raise
    except Exception as exc:
        return server_error(exc, action="committing docs context")


@router.get(
    "/getTsContext",
    summary="Get processed timeseries data for review",
    description="Read the timeseries pipeline output (tag metadata with limits) "
    "for user review before committing to the database. "
    "Falls back to local OUT_DIR if RustFS data is not yet available.",
    status_code=HTTPStatus.OK,
)
def getTsContext(
    upload_batch_id: str | None = None,
    source_file: str | None = None,
    plant_code_id: str = Query(..., description="Plant code ID for the request"),
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=1000),
):

    errors = []
    if not plant_code_id:
        errors.append({"field": "plant_code_id", "message": "Plant Code is required"})
    if errors:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": errors,
            },
        )

    plant_code_id = plant_code_id

    try:
        """Plant-scoped review data for timeseries metadata.

        ``upload_batch_id`` (canonical) scopes the preview to one pipeline run via the
        ``upload_batch_id`` column. ``source_file`` scopes to a single uploaded
        metadata file via the ``source_file`` column (per-file "Review & patch").
        Omit both for the full plant view (the "Review all" path)."""
        from ..plants import validate_plant_code as _vp

        _vp(plant_code_id)
        _log.info(
            "[context/ts] GET /ts — plant=%s  upload_batch_id=%s  source_file=%s",
            plant_code_id,
            upload_batch_id or "(none)",
            source_file or "(none)",
        )
        rows: list[dict] = []
        scope = "none"


        if not upload_batch_id:
            db_rows = _read_ts_from_db(plant_code_id, source_file=source_file)
            if db_rows:
                rows = db_rows
                scope = "source_file" if source_file else "plant"
                _log.debug(
                    "[context/ts] TS data from DB: %d rows (scope=%s)", len(rows), scope
                )

        if not rows:
            ts_path = (
                f"{rustfs_processed_ts(plant_code_id)}/ts_timeseries_metadata.parquet"
            )
            ts_df = _read_parquet_from_rustfs(ts_path)
            if not ts_df.empty:
                rows = ts_df.to_dict(orient="records")
                _log.debug("[context/ts] TS data from RustFS: %d rows", len(rows))
            if not rows:
                _log.debug("[context/ts] RustFS empty/missing, trying local OUT_DIR")
                rows = _read_stage_csvs("ts", "timeseries_metadata")

            if source_file:
                before = len(rows)
                scoped = _filter_rows_by_source_file(rows, source_file)
                if len(scoped) != before or any("source_file" in r for r in rows):
                    rows = scoped
                    scope = "source_file"
                _log.debug(
                    "[context/ts] source_file filter → %d/%d rows", len(rows), before
                )
            elif upload_batch_id:
                before = len(rows)
                scoped = _filter_rows_by_batch(rows, upload_batch_id)
                if len(scoped) != before or any("upload_batch_id" in r for r in rows):
                    rows = scoped
                    scope = "upload_batch_id"
                _log.debug(
                    "[context/ts] upload_batch_id filter → %d/%d rows", len(rows), before
                )


        if rows:
            _advance_flow_to_reviewing(plant_code_id, "ts")

        result = {
            "source": "ts",
            "plant_code_id": plant_code_id,
            "upload_batch_id": upload_batch_id,
            "scope": scope,
            "rows": rows,
            "total": len(rows),
        }
        return paginated_envelope(
            result,
            items_key="rows",
            page=page,
            page_size=page_size,
            message="Timeseries context retrieved successfully",
            empty_message=(
                f"No timeseries rows are staged for review for plant '{plant_code_id}'. "
                "Upload metadata and run the ts pipeline first."
            ),
        )

    except HTTPException:
        raise
    except Exception as exc:
        return server_error(exc, action="loading ts context")


@router.post(
    "/commitTsContext",
    summary="Commit timeseries context to database",
    description="Write the reviewed timeseries data to PostgreSQL and clean up processed files. "
    "Returns 200 with a commit_id when rows were written, 204 when the reviewed state is empty so nothing was written, and 409 when the pipeline has not run for this connector yet.",
    status_code=HTTPStatus.OK,
)
def commitTsContext(
    body=Body(...),
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
):


    try:
        plant_code_id, upload_batch_id, rows = _parse_commit_body(body, "ts")
        _idem_fingerprint = _idempotency.fingerprint(body)
        _replayed, _conflict = _idempotency.lookup(
            plant_code_id, "commit_ts", idempotency_key, body
        )
        if _conflict == "body_mismatch":
            return JSONResponse(
                status_code=HTTPStatus.CONFLICT,
                content={
                    "success": False,
                    "message": (
                        "This Idempotency-Key was already used with a different request body."
                    ),
                    "errors": [
                        {"field": "Idempotency-Key", "message": "Key reused with a different body."}
                    ],
                },
            )
        if _replayed is not None:
            return _replayed
        if not rows:
            try:
                rows = _read_ts_rows_for_commit(
                    plant_code_id, upload_batch_id, body.get("source_file")
                )
            except ReviewedStateMissing:
                return not_processed_yet("timeseries", plant_code_id)
            if not rows:
                return nothing_to_commit("timeseries", plant_code_id)

        validated_rows = []
        numeric_fields = {
            "op_limit_ll",
            "op_limit_l",
            "op_limit_h",
            "op_limit_hh",
            "safety_limit_ll",
            "safety_limit_l",
            "safety_limit_h",
            "safety_limit_hh",
            "trip_limit_ll",
            "trip_limit_l",
            "trip_limit_h",
            "trip_limit_hh",
        }


        from ..database.timeseries import _norm_measurement as _iotdb_norm
        _text_fields = (
            "tag_name",
            "equipment_id",
            "description",
            "unit",
            "data_type",
            "normalized_asset",
            "limits_basis",
            "limits_source_doc_id",
            "source_system",
            "source_record_id",
            "source_file",
        )

        for idx, row in enumerate(rows):
            for _f in _text_fields:
                v = row.get(_f)
                if isinstance(v, str):
                    row[_f] = v.strip()

            tag = row.get("tag_name")
            if not tag:
                return JSONResponse(
                    status_code=HTTPStatus.BAD_REQUEST,
                    content={
                        "success": False,
                        "message": "Validation failed",
                        "errors": [{"field": "tag_name", "message": f"Row {idx} is missing required field 'tag_name'"}],
                    },
                )

            row["iotdb_tag_id"] = _iotdb_norm(str(tag))
            for field in numeric_fields:
                if field in row and row[field] is not None and row[field] != "":
                    coerced, reason = _coerce_numeric_limit(row[field])
                    if coerced is None and reason and reason != "placeholder":
                        _log.warning(
                            "[context/ts/commit] tag %s: %s=%r not numeric (%s) → NULL",
                            tag,
                            field,
                            row[field],
                            reason,
                        )
                    row[field] = coerced
                elif row.get(field) == "":
                    row[field] = None

            validated_rows.append(row)

        validated_rows = _apply_plant_code(validated_rows, plant_code_id)


        _now = datetime.now(timezone.utc)
        for _r in validated_rows:
            _r["updated_at"] = _now
            _ia = _r.get("is_active")
            if _ia is None or (isinstance(_ia, str) and _ia.strip() == ""):
                _r["is_active"] = True
            elif isinstance(_ia, str):
                _r["is_active"] = _ia.strip().lower() in ("true", "t", "1", "yes", "y")

        inserted = updated = noop = 0
        try:
            inserted, updated, noop = _upsert_ts_metadata(validated_rows, plant_code_id=plant_code_id)
            _log.info(
                "[context/ts/commit] UPSERT done — inserted=%d updated=%d noop=%d (of %d submitted)",
                inserted,
                updated,
                noop,
                len(validated_rows),
            )
            _ts_commit_error = None
        except Exception as db_exc:
            _log.warning("[context/ts/commit] DB write skipped: %s", db_exc)
            _ts_commit_error = human_message(
                db_exc, action="committing ts context", plant_code_id=plant_code_id
            )

        _written = inserted + updated
        _commit_uid = _record_commit_audit(
            "ts",
            plant_code_id,
            [
                {
                    "table": "timeseries_metadata",
                    "rows_written": _written,
                    **({"warning": _ts_commit_error} if _ts_commit_error else {}),
                }
            ],
            _written,
            None,
            detail={"inserted": inserted, "updated": updated, "noop": noop},
        )
        _committed_files = (
            sorted(
                {
                    str(r.get("source_file")).strip()
                    for r in validated_rows
                    if r.get("source_file")
                }
            )
            or None
        )
        _advance_flow(
            plant_code_id,
            "ts",
            file_names=_committed_files,
            stage="committed",
            status="done" if not _ts_commit_error else "failed",
            commit_uid=_commit_uid,
            rows_committed=_written,
            error=_ts_commit_error,
        )
        result = {
            "status": "committed" if not _ts_commit_error else "partial",
            "commit_id": _commit_uid,
            "table": "timeseries_metadata",
            "rows_inserted": inserted,
            "rows_updated": updated,
            "rows_noop": noop,
            "rows_written": _written,
        }
        result = normalise_commit(
            result,
            connector="timeseries",
            commit_id=_commit_uid,
            upload_batch_id=upload_batch_id,
            committed_by=get_actor(),
        )
        if _ts_commit_error:
            _log.error(
                "[context/ts/commit] commit FAILED — 0 rows written to "
                "timeseries_metadata: %s",
                _ts_commit_error,
            )
            return JSONResponse(
                status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
                content={
                    "success": False,
                    "message": "Timeseries context could not be committed",
                    "errors": [
                        {
                            "field": "timeseries_metadata",
                            "message": _ts_commit_error,
                        }
                    ],
                    "data": result,
                },
            )
        _response = {
            "success": True,
            "message": "Timeseries context committed successfully",
            "data": result,
        }
        _idempotency.remember(plant_code_id, "commit_ts", idempotency_key, body, _response,
            request_fingerprint=_idem_fingerprint,
        )
        return _response

    except HTTPException:
        raise
    except Exception as exc:
        return server_error(exc, action="committing ts context")




@router.get(
    "/getSapContext",
    summary="Get processed SAP data for review",
    description="Read the SAP pipeline output for a specific table. "
    "If `table` is not specified, returns all available SAP output tables. "
    "Column display names are applied from user_config.yaml rename overrides.",
    status_code=HTTPStatus.OK,
)
def getSapContext(
    table: str | None = None,
    plant_code_id: str = Query(..., description="Plant code ID for the request"),
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=1000),
):

    errors = []
    if not plant_code_id:
        errors.append({"field": "plant_code_id", "message": "Plant Code is required"})
    if errors:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": errors,
            },
        )

    plant_code_id = plant_code_id

    try:
        if plant_code_id:
            from ..plants import validate_plant_code as _vp

            _vp(plant_code_id)
        if table:

            rows: list[dict] = []


            if plant_code_id:

                _safe = re.sub(r"[^a-z0-9_-]", "_", table.lower().strip())
                _edit_path = (
                    f"{rustfs_processed_sap(plant_code_id)}/{_safe}/sap_review.parquet"
                )
                if _fs.exists(_edit_path):
                    rows = _read_entity_file(_edit_path).to_dict(orient="records")
            if not rows:
                rows = _read_stage_csvs("sap", table, plant_code_id=plant_code_id)
            if not rows:
                sap_dir = _resolve_stage_dir("sap", plant_code_id)
                source_dir = _fs.path_join(sap_dir, "source")
                for variant in (table, table.upper(), table.lower()):
                    candidate = _fs.path_join(source_dir, f"{variant}.parquet")
                    if _fs.exists(candidate):
                        rows = _read_entity_file(candidate).to_dict(orient="records")
                        break


            if plant_code_id and rows and any("plant_code_id" in r for r in rows):
                rows = [
                    r for r in rows if r.get("plant_code_id") in (None, "", plant_code_id)
                ]

            renames = _get_sap_column_renames(table)

            result = {
                "source": "sap",
                "table": table,
                "rows": rows,
                "total": len(rows),
                "columns": list(rows[0].keys()) if rows else [],
                "renames": renames,
            }
            return paginated_envelope(
                result,
                items_key="rows",
                page=page,
                page_size=page_size,
                message="SAP context retrieved successfully",
                empty_message=(
                    f"No rows are staged for SAP table '{table}' in plant '{plant_code_id}'."
                ),
            )


        sap_dir = _resolve_stage_dir("sap", plant_code_id)
        entity_dir = _fs.path_join(sap_dir, "entities")
        tables = []
        for f in _glob_entity_files(entity_dir):
            df = _read_entity_file(f)
            tables.append(
                {
                    "name": Path(f).stem,
                    "columns": list(df.columns),
                    "row_count": len(df),
                }
            )
        result = {"source": "sap", "tables": tables}
        return {
                "success": True,
                "message": "SAP context retrieved successfully",
                "data": result,
            }

    except HTTPException:
        raise
    except Exception as exc:
        return server_error(exc, action="loading sap context")


def _get_sap_column_renames(table_name: str) -> dict[str, str]:
    """Get merged column rename mappings (template + user overrides) for a SAP table."""
    template_data = load_yaml(SAP_RENAME_FILE)
    source_key = table_name.lower()
    template_mappings = (
        template_data.get("sources", {}).get(source_key, {}).get("mappings", {})
    )

    user_cfg = load_yaml(USER_CONFIG_FILE)
    user_mappings = (
        user_cfg.get("sap_processing", {})
        .get("column_rename_overrides", {})
        .get(source_key, {})
        .get("mappings", {})
    )

    merged = {**template_mappings, **user_mappings}
    return merged


def _run_commit_sap_bg(
    pipeline_job_id: str,
    body: dict,
    plant_code_id: str,
    idempotency_key: str | None,
    idem_fingerprint: str,
    upload_batch_id: str | None,
) -> None:
    """Background runner for commitSapContext — identical commit logic to before,
    just executed off the request thread. Reports progress via upsert_job so
    GET /pipeline/jobs/{jobId} can poll it (the same store pipeline/run uses)."""
    _log.info(
        "[context/sap/commit] BG START  pipeline_job_id=%s  plant=%s  "
        "body_tables=%s  upload_batch_id=%s",
        pipeline_job_id, plant_code_id,
        sorted((body.get("tables") or {}).keys()) or "(none — will load reviewed state)",
        upload_batch_id or "(none)",
    )
    job: dict = {
        "pipeline_job_id": pipeline_job_id,
        "stage": "commit_sap",
        "plant_code_id": plant_code_id,
        "status": "running",
        "created_by": get_actor(),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "started_at": datetime.now(timezone.utc).isoformat(),
        "upload_batch_id": upload_batch_id,
    }
    upsert_job(pipeline_job_id, job)
    # Also register in pipeline.py's in-memory registry: reconcile_interrupted_jobs
    # (run once at process boot, main.py) treats a "running" job with no live OS
    # pid as orphaned UNLESS it's found here first. This is a thread, not a
    # subprocess, so it has no pid — without this registration, any reconciliation
    # pass that happens to run while this commit is genuinely still in flight would
    # incorrectly mark it interrupted. Popped in the finally below on every exit path.
    _pipeline_jobs_registry[pipeline_job_id] = job

    try:
        total = 0
        tables_committed = []

        user_cfg = load_yaml(USER_CONFIG_FILE)
        rename_overrides = user_cfg.get("sap_processing", {}).get(
            "column_rename_overrides", {}
        )
        custom_tables = user_cfg.get("sap_processing", {}).get("custom_tables", {})
        custom_labels = {v.get("label", k).lower(): k for k, v in custom_tables.items()}

        schema_cfg = load_yaml(str(SCHEMA_FROZEN_FILE))
        schema_tables: set[str] = set(
            (schema_cfg.get("schema") or schema_cfg.get("tables") or {}).keys()
        )
        entities_cfg = load_yaml(str(ENTITIES_FROZEN_FILE))
        for _ent in (entities_cfg.get("entities") or {}).values():
            ct = _ent.get("canonical_table")
            if ct:
                schema_tables.add(ct)
        # asset_cross_reference is a real Postgres table (written by
        # run_sap_end_to_end.py) but has no entry in schema.yaml/entities.yaml,
        # since it's a consolidation output rather than a canonical entity —
        # without this it would fall through to the cdm_sap_ prefix branch below.
        schema_tables.add("asset_cross_reference")

        _sap_tables = body.get("tables") or {}
        if not any(_sap_tables.values()):
            _log.info(
                "[context/sap/commit] BG pipeline_job_id=%s: body had no rows — "
                "calling _read_sap_tables_for_commit(%s)",
                pipeline_job_id, plant_code_id,
            )
            try:
                _sap_tables = _read_sap_tables_for_commit(plant_code_id)
            except ReviewedStateMissing as _exc:
                _log.warning(
                    "[context/sap/commit] BG pipeline_job_id=%s: "
                    "_read_sap_tables_for_commit raised ReviewedStateMissing(%s)",
                    pipeline_job_id, _exc,
                )
                job["status"] = "failed"
                job["error"] = (
                    f"Nothing has been processed for 'sap' in plant '{plant_code_id}' "
                    "yet, so there is nothing to commit. Upload a file, then run "
                    "POST /pipeline/run with stage 'sap' before committing."
                )
                job["completed_at"] = datetime.now(timezone.utc).isoformat()
                upsert_job(pipeline_job_id, job)
                return
            _log.info(
                "[context/sap/commit] BG pipeline_job_id=%s: "
                "_read_sap_tables_for_commit returned %d table(s), %d total row(s): %s",
                pipeline_job_id,
                len(_sap_tables),
                sum(len(v) for v in _sap_tables.values()),
                {k: len(v) for k, v in _sap_tables.items()},
            )
            if not any(_sap_tables.values()):
                job["status"] = "done"
                job["total_records"] = 0
                job["output_tables"] = []
                job["warning"] = {"reason": "nothing_to_commit"}
                job["completed_at"] = datetime.now(timezone.utc).isoformat()
                upsert_job(pipeline_job_id, job)
                return

        for table_name, rows in _sap_tables.items():
            if not rows:
                continue
            if _is_internal_sap_artifact(table_name):
                _log.info(
                    "[context/sap/commit] %s: pipeline-internal artifact, not a "
                    "CDM table — its canonical data commits under its own entity "
                    "name, skipping without warning",
                    table_name,
                )
                continue

            source_key = table_name.lower()
            renames = rename_overrides.get(source_key, {}).get("mappings", {})
            if renames:
                renamed_rows = []
                for row in rows:
                    renamed_row = {}
                    for col, val in row.items():
                        new_col = renames.get(col, col)
                        renamed_row[new_col] = val
                    renamed_rows.append(renamed_row)
                rows = renamed_rows

            rows = _apply_plant_code(rows, plant_code_id)
            rows = [{**r, "site": r.get("site") or "-"} for r in rows]

            if source_key in schema_tables:
                db_table = table_name
            elif source_key in custom_labels or source_key in custom_tables:
                db_table = table_name
            else:
                db_table = f"cdm_sap_{table_name}"

            replaced = 0
            try:
                _conn = _get_conn()
                try:
                    _cur = _conn.cursor()
                    replaced = _replace_by_natural_key(
                        _cur,
                        db_table,
                        rows,
                        "source_record_id",
                        plant_code_id,
                    )
                    _conn.commit()
                finally:
                    _conn.close()
            except Exception as _exc:
                _log.warning(
                    "[context/sap/commit] dedup pre-delete skipped for %s: %s",
                    db_table,
                    _exc,
                )

            def _bulk_upsert_sap(_db_table: str, _rows: list[dict]) -> int:
                """Batch INSERT ... ON CONFLICT (source_record_id, plant_code_id) DO
                UPDATE, same pattern as _upsert_ts_metadata. Raises on any failure
                (e.g. no matching unique constraint) so the caller falls back to
                the slower per-row _write_to_db."""
                from psycopg2.extras import execute_values
                from ..services.transforms import _coerce_for_pg

                if not _rows:
                    return 0

                _bulk_conn = _get_conn(plant_code_id)
                try:
                    _bulk_cur = _bulk_conn.cursor()

                    columns_in_row = list(_rows[0].keys())
                    for c in columns_in_row:
                        _bulk_cur.execute(
                            f'ALTER TABLE "{_db_table}" ADD COLUMN IF NOT EXISTS "{c}" TEXT'
                        )

                    _bulk_cur.execute(
                        "SELECT column_name, data_type, column_default "
                        "FROM information_schema.columns "
                        "WHERE table_schema = current_schema() AND table_name = %s",
                        (_db_table,),
                    )
                    col_meta = {
                        r[0]: {"type": r[1], "default": r[2] or ""}
                        for r in _bulk_cur.fetchall()
                    }
                    autogen = {
                        c for c, m in col_meta.items() if m["default"].startswith("nextval(")
                    }

                    # Find the table's actual unique constraint(s) instead of
                    # assuming (source_record_id, plant_code_id) — e.g.
                    # asset_strategy's real constraint is uq_asset_strategy_site
                    # on (plant_code_id, site, normalized_asset). The primary key
                    # is excluded: ON CONFLICT only suppresses errors on the one
                    # constraint it targets, so picking the PK while a separate
                    # business-key unique constraint (e.g. equipment's
                    # uq_equip_plant_site_asset) exists still lets that other
                    # constraint raise a plain duplicate-key error.
                    conflict_cols = _unique_key_columns(_bulk_cur, _db_table)
                    if not conflict_cols:
                        raise RuntimeError(
                            f"{_db_table} has no non-primary-key unique constraint "
                            "to conflict on"
                        )
                    if not all(c in col_meta for c in conflict_cols):
                        raise RuntimeError(
                            f"{_db_table} unique constraint columns {conflict_cols} "
                            "not all present in this insert"
                        )

                    insert_cols = [
                        c for c in columns_in_row if c in col_meta and c not in autogen
                    ]
                    update_cols = [c for c in insert_cols if c not in conflict_cols]
                    if not update_cols:
                        raise RuntimeError(f"{_db_table} has no column to update")

                    # ON CONFLICT can't touch the same row twice in one statement —
                    # one row per conflict key, last one in the batch wins.
                    dedup: dict[tuple, list] = {}
                    for row in _rows:
                        values = [
                            _coerce_for_pg(row.get(c), col_meta[c]["type"])
                            for c in insert_cols
                        ]
                        key = tuple(row.get(c) for c in conflict_cols)
                        dedup[key] = values
                    all_values = list(dedup.values())

                    col_names = ", ".join(f'"{c}"' for c in insert_cols)
                    conflict_target = ", ".join(f'"{c}"' for c in conflict_cols)
                    update_setters = ", ".join(
                        f'"{c}" = EXCLUDED."{c}"' for c in update_cols
                    )
                    upsert_sql = (
                        f'INSERT INTO "{_db_table}" ({col_names}) '  # noqa: S608
                        f"VALUES %s "
                        f"ON CONFLICT ({conflict_target}) DO UPDATE "
                        f"SET {update_setters}"
                    )

                    written = 0
                    for start in range(0, len(all_values), 5000):
                        batch = all_values[start : start + 5000]
                        execute_values(
                            _bulk_cur, upsert_sql, batch, page_size=5000
                        )
                        written += len(batch)

                    _bulk_conn.commit()
                    return written
                finally:
                    _bulk_conn.close()

            try:
                try:
                    count = _bulk_upsert_sap(db_table, rows)
                except Exception as _bulk_exc:
                    _log.warning(
                        "[context/sap/commit] %s: bulk upsert failed (%s) — "
                        "falling back to _write_to_db",
                        db_table,
                        _bulk_exc,
                    )
                    count = _write_to_db(db_table, rows, plant_code_id=plant_code_id)
            except Exception as _exc:
                if "does not exist" not in str(_exc).lower():
                    raise
                _log.warning(
                    "[context/sap/commit] %s -> %s: no such table in the plant "
                    "database; skipped (the pipeline emits it, the CDM schema does "
                    "not define it)",
                    table_name,
                    db_table,
                )
                tables_committed.append(
                    {
                        "table": db_table,
                        "rows_written": 0,
                        "warning": f"'{db_table}' is not a CDM table — skipped",
                    }
                )
                continue
            total += count
            tables_committed.append(f"{table_name} → {db_table}")
            if replaced:
                _log.info(
                    "[context/sap/commit] %s: replaced %d prior row(s) by source_record_id",
                    db_table,
                    replaced,
                )

        if total == 0:
            _log.warning(
                "[context/sap/commit] BG pipeline_job_id=%s: commit completed but "
                "wrote 0 rows across %d table(s)",
                pipeline_job_id,
                len(tables_committed),
            )
            job["warning"] = {"reason": "nothing_to_commit"}

        _commit_uid = _record_commit_audit(
            "sap", plant_code_id, tables_committed, total, None
        )
        _advance_flow(
            plant_code_id,
            "sap",
            stage="committed",
            status="done",
            commit_uid=_commit_uid,
            rows_committed=total,
        )
        result = {
            "status": "committed",
            "commit_id": _commit_uid,
            "tables": tables_committed,
            "rows_written": total,
            "total_rows_written": total,
        }
        result = normalise_commit(
            result,
            connector="sap",
            commit_id=_commit_uid,
            upload_batch_id=upload_batch_id,
            committed_by=get_actor(),
        )

        job["status"] = "done"
        job["total_records"] = total
        job["output_tables"] = tables_committed
        job["completed_at"] = datetime.now(timezone.utc).isoformat()
        upsert_job(pipeline_job_id, job)

        # Replay of this Idempotency-Key after completion returns the final commit
        # result (not the 202 placeholder remembered at spawn time).
        _idempotency.remember(
            plant_code_id, "commit_sap", idempotency_key, body,
            {
                "success": True,
                "message": "SAP context committed successfully",
                "data": result,
            },
            request_fingerprint=idem_fingerprint,
        )

    except Exception as exc:
        _log.exception(
            "[context/sap/commit] background commit failed  pipeline_job_id=%s: %s",
            pipeline_job_id, exc,
        )
        job["status"] = "failed"
        job["error"] = human_message(exc, action="committing sap context", plant_code_id=plant_code_id)
        job["completed_at"] = datetime.now(timezone.utc).isoformat()
        upsert_job(pipeline_job_id, job)
    finally:
        _pipeline_jobs_registry.pop(pipeline_job_id, None)


@router.post(
    "/commitSapContext",
    summary="Commit SAP context to database",
    description="Start an asynchronous commit of the reviewed SAP data to PostgreSQL. "
    "If user has edited column display names, renames are applied "
    "from user_config.yaml before writing. "
    "Returns 202 immediately with a pipeline_job_id — poll "
    "GET /pipeline/jobs/{jobId} for status (done/failed) and results.",
    status_code=HTTPStatus.ACCEPTED,
)
def commitSapContext(
    body: dict = Body(
        ...,
        examples=[
            {
                "plant_code_id": "Plant_1",
                "tables": {
                    "sap_equipment": [
                        {"EQUNR": "10001452", "EQKTX": "Crusher Main Drive"}
                    ],
                },
            }
        ],
    ),
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
):

    try:
        plant_code_id = body.get("plant_code_id")
        if not plant_code_id:
            return JSONResponse(
                status_code=HTTPStatus.BAD_REQUEST,
                content={
                    "success": False,
                    "message": "Validation failed",
                    "errors": [{"field": "plant_code_id", "message": "plant_code_id is required"}],
                },
            )
        from ..plants import validate_plant_code as _vp

        _vp(plant_code_id)
        upload_batch_id = (
            body.get("upload_batch_id")
            or body.get("batch_id")
            or body.get("pipeline_job_id")
        )
        _idem_fingerprint = _idempotency.fingerprint(body)
        _replayed, _conflict = _idempotency.lookup(
            plant_code_id, "commit_sap", idempotency_key, body
        )
        if _conflict == "body_mismatch":
            return JSONResponse(
                status_code=HTTPStatus.CONFLICT,
                content={
                    "success": False,
                    "message": (
                        "This Idempotency-Key was already used with a different request body."
                    ),
                    "errors": [
                        {"field": "Idempotency-Key", "message": "Key reused with a different body."}
                    ],
                },
            )
        if _replayed is not None:
            return _replayed

        pipeline_job_id = str(uuid.uuid4())

        # Remember the 202 placeholder now (before spawning) so a duplicate
        # Idempotency-Key received while the commit is still running returns
        # this same job handle instead of racing a second concurrent commit.
        _queued_response = {
            "success": True,
            "message": "SAP context commit started",
            "data": {
                "pipeline_job_id": pipeline_job_id,
                "status": "queued",
                "plant_code_id": plant_code_id,
            },
        }
        _idempotency.remember(
            plant_code_id, "commit_sap", idempotency_key, body, _queued_response,
            request_fingerprint=_idem_fingerprint,
        )

        spawn_with_context(
            target=_run_commit_sap_bg,
            args=(
                pipeline_job_id,
                body,
                plant_code_id,
                idempotency_key,
                _idem_fingerprint,
                upload_batch_id,
            ),
            daemon=True,
        )

        return JSONResponse(
            status_code=HTTPStatus.ACCEPTED,
            content=_queued_response,
        )

    except HTTPException:
        raise
    except Exception as exc:
        return server_error(exc, action="starting sap context commit")


@router.get(
    "/getCommitHistory",
    summary="Commit audit history",
    description="Recent commit outcomes (source, plant, status, rows written, "
    "who, when) from the durable commit_audit table. Answers "
    '"did my reviewed data actually land?" across restarts.',
    status_code=HTTPStatus.OK,
)
def getCommitHistory(
    source: str | None = None,
    limit: int = 50,
    plant_code_id: str = Query(..., description="Plant code ID for the request"),
):

    errors = []
    if not plant_code_id:
        errors.append({"field": "plant_code_id", "message": "Plant Code is required"})
    if errors:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": errors,
            },
        )

    plant_code_id = plant_code_id

    try:
        from p0.api.services.audit import list_commits

        result = {
            "commits": list_commits(plant_code_id=plant_code_id, source=source, limit=limit)
        }
        scope = f" for the '{source}' connector" if source else ""
        return collection_envelope(
            result,
            items_key="commits",
            message="Commit history retrieved successfully",
            empty_message=f"Nothing has been committed yet for plant '{plant_code_id}'{scope}.",
        )

    except HTTPException:
        raise
    except Exception as exc:
        return server_error(exc, action="loading commit history")




@router.get(
    "/getSapJoins",
    summary="Get SAP join configuration",
    description="Return the current SAP join overrides from user_config.yaml, "
    "merged with the default join template used by the merging pipeline.",
    status_code=HTTPStatus.OK,
)
def getSapJoins(
    plant_code_id: str = Query(..., description="Plant code ID for the request"),
):

    errors = []
    if not plant_code_id:
        errors.append({"field": "plant_code_id", "message": "Plant Code is required"})
    if errors:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": errors,
            },
        )

    plant_code_id = plant_code_id

    try:
        template_cfg = load_yaml(SAP_JOIN_COLUMNS_FILE)
        user_cfg = load_yaml(USER_CONFIG_FILE)

        all_overrides = user_cfg.get("sap_processing", {}).get("join_overrides", {})
        overrides = all_overrides.get(plant_code_id, {})
        has_overrides = bool(overrides)

        pipelines_out: dict = {}
        override_count = 0
        total_count = 0

        for pipeline_name, pipeline_data in template_cfg.get("pipelines", {}).items():
            pipeline_overrides = overrides.get(pipeline_name, {})
            merge_steps_out: dict = {}

            for step_name, step_data in pipeline_data.get("merge_steps", {}).items():
                total_count += 1
                step_override = pipeline_overrides.get(step_name)
                if step_override is not None:
                    effective_columns = step_override
                    step_source = "user_config"
                    override_count += 1
                else:
                    effective_columns = step_data.get("join_columns", [])
                    step_source = "template"

                merge_steps_out[step_name] = {
                    "left_table": step_data.get("left_table", ""),
                    "right_table": step_data.get("right_table", ""),
                    "join_columns": effective_columns,
                    "how": step_data.get("how", "left"),
                    "description": step_data.get("description", ""),
                    "source": step_source,
                }

            pipelines_out[pipeline_name] = {"merge_steps": merge_steps_out}

        if override_count == 0:
            overall_source = "template"
        elif override_count == total_count:
            overall_source = "user_config"
        else:
            overall_source = "merged"

        result = {
            "pipelines": pipelines_out,
            "has_overrides": has_overrides,
            "source": overall_source,
        }
        return {
            "success": True,
            "message": "SAP joins retrieved successfully",
            "data": result,
        }

    except HTTPException:
        raise
    except Exception as exc:
        return server_error(exc, action="loading sap joins")


@router.put(
    "/saveSapJoins",
    summary="Save SAP join overrides",
    description="Save per-plant join column overrides to user_config.yaml. "
    "These are used by the SAP merging pipeline (sap_merging_impl.py).",
    status_code=HTTPStatus.OK,
)
def saveSapJoins(
    plant_code_id: str = Query(..., description="Plant code ID for the request"),
    body: dict = Body(...),
):
    errors = []
    if not plant_code_id:
        errors.append({"field": "plant_code_id", "message": "Plant Code is required"})
    if errors:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": errors,
            },
        )

    try:
        join_overrides = body.get("join_overrides", {})
        column_rename_overrides = body.get("column_rename_overrides")
        sheet_mappings = body.get("sheet_mappings")

        sap_processing_patch = {"join_overrides": {plant_code_id: join_overrides}}
        if column_rename_overrides is not None:
            sap_processing_patch["column_rename_overrides"] = {
                plant_code_id: column_rename_overrides
            }
        if sheet_mappings is not None:
            sap_processing_patch["sheet_mappings"] = {plant_code_id: sheet_mappings}

        _user_config.write_layer(
            plant_code_id,
            _user_config.PLANT_SCOPE,
            {"sap_processing": sap_processing_patch},
            actor=get_actor(),
        )

        try:
            user_cfg = load_yaml(USER_CONFIG_FILE)
            sap_proc = user_cfg.setdefault("sap_processing", {})
            plant_overrides = sap_proc.setdefault("join_overrides", {})
            plant_overrides[plant_code_id] = join_overrides
            if column_rename_overrides is not None:
                rename_overrides = sap_proc.setdefault("column_rename_overrides", {})
                rename_overrides[plant_code_id] = column_rename_overrides
            if sheet_mappings is not None:
                plant_sheet_mappings = sap_proc.setdefault("sheet_mappings", {})
                plant_sheet_mappings[plant_code_id] = sheet_mappings
            backup(USER_CONFIG_FILE)
            save_yaml(USER_CONFIG_FILE, user_cfg)
        except Exception as exc:
            _log.warning(
                "[context/sap_joins] user_config.yaml mirror failed for %s: %s",
                plant_code_id, exc,
            )

        result = {
            "status": "saved",
            "plant_code_id": plant_code_id,
            "join_overrides": join_overrides,
            "column_rename_overrides": column_rename_overrides or {},
            "sheet_mappings": sheet_mappings or {},
        }
        return {
            "success": True,
            "message": "SAP joins saved successfully",
            "data": result,
        }

    except HTTPException:
        raise
    except Exception as exc:
        return server_error(exc, action="saving sap joins")


@router.get(
    "/getSapMappingContext",
    summary="Get SAP ↔ P&ID mapping rows for review",
    description="Read the sap_mapping pipeline output (sap_pid_mapping.parquet) "
    "for the given plant. Returns the cross-reference rows produced by the "
    "sap_mapping stage so the user can review and edit them before commit.",
    status_code=HTTPStatus.OK,
)
def getSapMappingContext(
    plant_code_id: str = Query(..., description="Plant code ID for the request"),
    limit: int = Query(100, ge=1, description="Maximum number of rows to return"),
    offset: int = Query(0, ge=0, description="Number of rows to skip before slicing"),
    matched_only: Annotated[
        bool, Query(description="If true, only return rows with a non-empty pid_asset_tag")
    ] = False,
):
    errors = []
    if not plant_code_id:
        errors.append({"field": "plant_code_id", "message": "Plant Code is required"})
    if errors:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={"success": False, "message": "Validation failed", "errors": errors},
        )

    try:
        from ..plants import validate_plant_code as _vp
        _vp(plant_code_id)

        mapping_path = f"{rustfs_processed_sap_mapping(plant_code_id)}/sap_pid_mapping.parquet"
        df = _read_parquet_from_rustfs(mapping_path)
        if matched_only:
            if "pid_asset_tag" not in df.columns:
                return JSONResponse(
                    status_code=HTTPStatus.BAD_REQUEST,
                    content={
                        "success": False,
                        "message": "Validation failed",
                        "errors": [{
                            "field": "matched_only",
                            "message": "pid_asset_tag column is not present in the sap_mapping output",
                        }],
                    },
                )
            df = df[df["pid_asset_tag"].notna() & (df["pid_asset_tag"].astype(str).str.strip() != "")]
        total_rows = len(df)
        df = df.iloc[offset:offset + limit]
        rows = df.to_dict(orient="records") if not df.empty else []

        _log.info(
            "[context/sap_mapping] GET — plant=%s  rows=%d  total_rows=%d  limit=%d  offset=%d",
            plant_code_id, len(rows), total_rows, limit, offset,
        )
        result = {
            "source": "sap_mapping",
            "rows": rows,
            "total": len(rows),
            "total_rows": total_rows,
            "limit": limit,
            "offset": offset,
            "matched_only": matched_only,
            "columns": list(rows[0].keys()) if rows else [],
        }
        return {"success": True, "message": "SAP mapping context retrieved", "data": result}

    except HTTPException:
        raise
    except Exception as exc:
        return JSONResponse(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            content={
                "success": False,
                "message": "Internal server error",
                "errors": [{"field": "server", "message": str(exc)}],
            },
        )


@router.patch(
    "/patchSapMappingRows",
    summary="Persist edits to the SAP ↔ P&ID mapping table",
    description="Overwrite processed_data/<plant>/sap_mapping/sap_pid_mapping.parquet "
    "with the supplied rows so the user's review-page edits survive navigation. "
    "Body: {plant_code_id, rows}.",
    status_code=HTTPStatus.OK,
)
def patchSapMappingRows(
    body: dict = Body(...),
):
    try:
        plant_code_id = body.get("plant_code_id")
        if not plant_code_id:
            return JSONResponse(
                status_code=HTTPStatus.BAD_REQUEST,
                content={
                    "success": False,
                    "message": "Validation failed",
                    "errors": [{"field": "plant_code_id", "message": "plant_code_id is required"}],
                },
            )
        from ..plants import validate_plant_code as _vp
        _vp(plant_code_id)

        rows = body.get("rows") or []
        written = _write_processed_sap_mapping(plant_code_id, rows)
        _log.info(
            "[context/sap_mapping] PATCH — plant=%s  rows=%d", plant_code_id, len(rows)
        )
        if rows:
            _advance_flow_to_reviewing(plant_code_id, "sap")
        result = {"status": "ok", "written": written, "row_count": len(rows)}
        return {"success": True, "message": "SAP mapping rows patched successfully", "data": result}

    except HTTPException:
        raise
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
    "/commitSapMapping",
    summary="Commit reviewed SAP ↔ P&ID mapping to Postgres",
    description=(
        "Reads the current sap_pid_mapping.parquet (as last saved by patchSapMappingRows) "
        "and writes the confirmed cross-reference rows to the asset_cross_reference table "
        "in PostgreSQL. Existing rows for the same plant and equipment IDs are replaced "
        "before the new rows are inserted, so re-committing is safe."
    ),
    status_code=HTTPStatus.OK,
)
def commitSapMapping(
    body: dict = Body(
        ...,
        examples=[{"plant_code_id": "SAP_TEST_5"}],
    ),
):
    try:
        plant_code_id = (body.get("plant_code_id") or "").strip()
        if not plant_code_id:
            return JSONResponse(
                status_code=HTTPStatus.BAD_REQUEST,
                content={
                    "success": False,
                    "message": "Validation failed",
                    "errors": [{"field": "plant_code_id", "message": "plant_code_id is required"}],
                },
            )

        from ..plants import validate_plant_code as _vp
        _vp(plant_code_id)

        # 1. Read the reviewed mapping parquet written by patchSapMappingRows.
        mapping_path = f"{rustfs_processed_sap_mapping(plant_code_id)}/sap_pid_mapping.parquet"
        df = _read_parquet_from_rustfs(mapping_path)
        if df.empty:
            return JSONResponse(
                status_code=HTTPStatus.NOT_FOUND,
                content={
                    "success": False,
                    "message": "No mapping rows found",
                    "errors": [{
                        "field": "sap_pid_mapping",
                        "message": (
                            f"sap_pid_mapping.parquet is empty or missing for plant "
                            f"'{plant_code_id}'. Run the sap_mapping pipeline first."
                        ),
                    }],
                },
            )

        rows = df.to_dict(orient="records")
        rows = _apply_plant_code(rows, plant_code_id)

        _log.info(
            "[context/sap_mapping/commit] plant=%s  rows=%d  → asset_cross_reference",
            plant_code_id,
            len(rows),
        )

        # 2. Dedup: delete existing rows for the same (plant, sap_equipment_id) before
        #    re-inserting, so a re-commit doesn't duplicate the table.
        replaced = 0
        try:
            _conn = _get_conn()
            try:
                _cur = _conn.cursor()
                replaced = _replace_by_natural_key(
                    _cur,
                    "asset_cross_reference",
                    rows,
                    "sap_asset_id",
                    plant_code_id,
                )
                _conn.commit()
            finally:
                _conn.close()
        except Exception as _exc:
            _log.warning(
                "[context/sap_mapping/commit] dedup pre-delete failed (continuing): %s", _exc
            )

        # 3. Write rows to asset_cross_reference.
        count = _write_to_db("asset_cross_reference", rows, plant_code_id=plant_code_id)

        _log.info(
            "[context/sap_mapping/commit] committed %d row(s)  replaced=%d  plant=%s",
            count,
            replaced,
            plant_code_id,
        )

        # 4. Audit trail + flow-state advance.
        _record_commit_audit(
            "sap_mapping",
            plant_code_id,
            ["asset_cross_reference"],
            count,
            None,
        )
        _advance_flow(
            plant_code_id,
            "sap",
            stage="committed",
            status="done",
        )

        return {
            "success": True,
            "message": "SAP mapping committed successfully",
            "data": {
                "table": "asset_cross_reference",
                "rows_committed": count,
                "rows_replaced": replaced,
                "plant_code_id": plant_code_id,
            },
        }

    except HTTPException:
        raise
    except Exception as exc:
        _log.exception("[context/sap_mapping/commit] unexpected error: %s", exc)
        return JSONResponse(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            content={
                "success": False,
                "message": "Internal server error",
                "errors": [{"field": "server", "message": str(exc)}],
            },
        )



@router.get(
    "/getOtherFilesContext",
    summary="Get processed Other Files data for review",
    description=(
        "Read a user-defined Other Files output table for review. "
        "The table name is the user-defined type, for example valve_inspection."
    ),
    status_code=HTTPStatus.OK,
)
def getOtherFilesContext(
    table: str = Query(
        ...,
        description="User-defined Other Files table, e.g. valve_inspection",
    ),
    plant_code_id: str = Query(
        ...,
        description="Plant code ID for the request",
    ),
    upload_batch_id: str | None = Query(
        None,
        description="Scope the review to one upload/pipeline batch",
    ),
    source_file: str | None = Query(
        None,
        description="Scope the review to one source file",
    ),
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=1000),
):
    errors: list[dict] = []

    plant_code_id = (plant_code_id or "").strip()
    table = (table or "").strip()

    if not plant_code_id:
        errors.append(
            {
                "field": "plant_code_id",
                "message": "plant_code_id is required",
            }
        )

    if not table:
        errors.append(
            {
                "field": "table",
                "message": "table is required",
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

    # Prevent path traversal and arbitrary object lookup.
    safe_table = re.sub(
        r"[^a-zA-Z0-9_-]",
        "_",
        table,
    ).strip("_")

    if not safe_table:
        return JSONResponse(
            status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": [
                    {
                        "field": "table",
                        "message": "Invalid Other Files table name",
                    }
                ],
            },
        )

    try:
        from ..plants import validate_plant_code as _vp

        _vp(plant_code_id)

        rows = _read_stage_csvs(
            "other_files",
            table_name=safe_table,
            plant_code_id=plant_code_id,
        )

        if upload_batch_id:
            rows = _filter_rows_by_batch(
                rows,
                upload_batch_id,
            )

        if source_file:
            rows = _filter_rows_by_source_file(
                rows,
                source_file,
            )

        if rows:
            reviewed_files = (
                sorted(
                    {
                        str(row.get("source_file") or "").strip()
                        for row in rows
                        if row.get("source_file")
                    }
                )
                or None
            )
            _advance_flow(
                plant_code_id,
                "docs",
                file_names=reviewed_files,
                stage="reviewing",
                status="running",
            )

        result = {
            "source": "other_files",
            "connector": "other_files",
            "plant_code_id": plant_code_id,
            "table": safe_table,
            "upload_batch_id": upload_batch_id,
            "source_file": source_file,
            "rows": rows,
            "total": len(rows),
            "columns": list(rows[0].keys()) if rows else [],
        }

        return paginated_envelope(
            result,
            items_key="rows",
            page=page,
            page_size=page_size,
            message="Other Files context retrieved successfully",
            empty_message=(
                f"No Other Files rows are staged for table '{safe_table}' "
                f"in plant '{plant_code_id}'. Upload the file and run the "
                "other_files pipeline first."
            ),
        )

    except HTTPException:
        raise
    except Exception as exc:
        return server_error(
            exc,
            action="loading Other Files context",
        )


from p0.api.services.taxonomy import resolve_source as _resolve_source


def _invalid_source(raw: str | None) -> JSONResponse:
    return JSONResponse(
        status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
        content={
            "success": False,
            "message": (
                f"'{raw}' is not a valid connector. "
                "Use one of: pnid, documents, timeseries, sap, "
                "aif, gloc, lopc, other_files."
            ),
            "errors": [{"field": "connector", "message": "Unknown connector."}],
        },
    )


@router.get(
    "/getFindingsContext",
    summary="Get processed AIF / GLOC / LOPC / UPD_EVENT / TRIP_EVENT rows for review",
    description="Read the processed findings output for one connector. Every row carries "
    "`normalized_asset`, `asset_match_method` and `asset_match_confidence` so the reviewer "
    "can see how the equipment was identified and fix the ones that were not matched. "
    "Filter to unresolved rows with `unmatched_only=true`.",
    status_code=HTTPStatus.OK,
)
def getFindingsContext(
    connector: str = Query(..., description="aif | gloc | lopc | upd_event | trip_event"),
    plant_code_id: str = Query(..., description="Plant code ID for the request"),
    upload_batch_id: str | None = Query(None, description="Scope to one upload batch"),
    source_file: str | None = Query(None, description="Scope to one uploaded file"),
    unmatched_only: bool = Query(False, description="Only rows with no equipment identity"),
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=1000),
):
    name = str(connector or "").strip().lower()
    if not _findings.is_findings_connector(name):
        return _invalid_source(connector)
    if not plant_code_id:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": [{"field": "plant_code_id", "message": "Plant Code is required"}],
            },
        )
    try:
        from ..plants import validate_plant_code as _vp

        _vp(plant_code_id)
        rows = _read_findings_rows(plant_code_id, name)
        if upload_batch_id:
            rows = [r for r in rows if r.get("upload_batch_id") in (None, "", upload_batch_id)]
        if source_file:
            rows = [r for r in rows if r.get("source_file") == source_file]
        if unmatched_only:
            rows = [r for r in rows if not str(r.get("normalized_asset") or "").strip()]
        matched = sum(1 for r in rows if str(r.get("normalized_asset") or "").strip())
        result = {
            "source": name,
            "connector": name,
            "table": _findings.target_table(name),
            "rows": rows,
            "total": len(rows),
            "columns": list(rows[0].keys()) if rows else [],
            "identity": {
                "matched": matched,
                "unmatched": len(rows) - matched,
                "methods": _match_method_counts(rows),
            },
        }
        return paginated_envelope(
            result,
            items_key="rows",
            page=page,
            page_size=page_size,
            message=f"{name.upper()} context retrieved successfully",
            empty_message=(
                f"No {name.upper()} rows are staged for plant '{plant_code_id}'. "
                f"Upload a file and run POST /pipeline/run with stage '{name}' first."
            ),
        )
    except HTTPException as e:
        raise as_envelope_exc(e)
    except Exception as exc:
        return server_error(exc, action=f"loading {name} context")


@router.patch(
    "/patchFindingsRows",
    summary="Persist review edits on AIF / GLOC / LOPC / UPD_EVENT / TRIP_EVENT rows",
    description="Overwrite the processed findings parquet with the supplied rows so review "
    "edits survive navigation and feed the subsequent commit. Body: "
    "{connector, plant_code_id, rows}.",
    status_code=HTTPStatus.OK,
)
def patchFindingsRows(body: dict = Body(...)):
    name = str(body.get("connector") or body.get("source") or "").strip().lower()
    if not _findings.is_findings_connector(name):
        return _invalid_source(body.get("connector") or body.get("source"))
    plant_code_id = body.get("plant_code_id")
    if not plant_code_id:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": [{"field": "plant_code_id", "message": "plant_code_id is required"}],
            },
        )
    try:
        from ..plants import validate_plant_code as _vp

        _vp(plant_code_id)
        rows = body.get("rows") or []
        written = _write_findings_rows(plant_code_id, name, rows)
        _advance_flow_to_reviewing(plant_code_id, name)
        _log.info(
            "[context/%s/rows] PATCH — plant=%s rows=%d", name, plant_code_id, len(rows)
        )
        result = {"status": "ok", "written": written, "row_count": len(rows)}
        return {
            "success": True,
            "message": f"{name.upper()} rows patched successfully",
            "data": result,
        }
    except HTTPException as e:
        raise as_envelope_exc(e)
    except Exception as exc:
        return server_error(exc, action=f"updating {name} rows")


@router.post(
    "/commitFindingsContext",
    summary="Commit reviewed AIF / GLOC / LOPC / UPD_EVENT / TRIP_EVENT rows to the database",
    description="Write reviewed findings to their canonical table — inspection_record_aif, "
    "containment_event_gloc, containment_event_lopc, upd_event or trip_event. Rows are replaced by "
    "`source_record_id`, so re-committing the same file updates rather than duplicates. "
    "If no rows are sent, the processed output is read from object storage. "
    "Returns 200 with a commit_id when rows were written, 204 when the reviewed state is empty so nothing was written, and 409 when the pipeline has not run for this connector yet.",
    status_code=HTTPStatus.OK,
)
def commitFindingsContext(
    body: dict = Body(...),
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
):
    name = str(body.get("connector") or body.get("source") or "").strip().lower()
    if not _findings.is_findings_connector(name):
        return _invalid_source(body.get("connector") or body.get("source"))
    plant_code_id = body.get("plant_code_id")
    if not plant_code_id:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": [{"field": "plant_code_id", "message": "plant_code_id is required"}],
            },
        )
    upload_batch_id = (
        body.get("upload_batch_id") or body.get("batch_id") or body.get("pipeline_job_id")
    )
    _idem_fingerprint = _idempotency.fingerprint(body)
    _replayed, _conflict = _idempotency.lookup(
        plant_code_id, f"commit_{name}", idempotency_key, body
    )
    if _conflict == "body_mismatch":
        return JSONResponse(
            status_code=HTTPStatus.CONFLICT,
            content={
                "success": False,
                "message": (
                    "This Idempotency-Key was already used with a different request body."
                ),
                "errors": [
                    {"field": "Idempotency-Key", "message": "Key reused with a different body."}
                ],
            },
        )
    if _replayed is not None:
        return _replayed
    try:
        from ..plants import validate_plant_code as _vp

        _vp(plant_code_id)
        rows = body.get("rows")
        if not rows:
            try:
                rows = _read_findings_rows(plant_code_id, name, required=True)
            except ReviewedStateMissing:
                return not_processed_yet(name, plant_code_id)
            if upload_batch_id:
                rows = [
                    r for r in rows if r.get("upload_batch_id") in (None, "", upload_batch_id)
                ]
        rows = _apply_plant_code(rows or [], plant_code_id)
        if not rows:
            return nothing_to_commit(name, plant_code_id)
        table = _findings.target_table(name)
        written = 0
        replaced = 0
        if rows:
            rows = [_findings_db_row(name, r) for r in rows]
            try:
                _conn = _get_conn(plant_code_id)
                try:
                    _cur = _conn.cursor()
                    replaced = _replace_by_natural_key(
                        _cur, table, rows, "source_record_id", plant_code_id
                    )
                    _conn.commit()
                finally:
                    _conn.close()
            except Exception as _exc:
                _log.warning(
                    "[context/%s/commit] dedup pre-delete skipped for %s: %s", name, table, _exc
                )
            written = _write_to_db(table, rows, plant_code_id)
        _commit_uid = _record_commit_audit(
            name, plant_code_id, [f"{name} → {table}"], written, None
        )
        _advance_flow(
            plant_code_id,
            name,
            stage="committed",
            status="done",
            commit_uid=_commit_uid,
            rows_committed=written,
        )
        result = {
            "status": "committed",
            "commit_id": _commit_uid,
            "tables": [f"{name} → {table}"],
            "rows_written": written,
            "total_rows_written": written,
            "rows_replaced": replaced,
        }
        result = normalise_commit(
            result,
            connector=name,
            commit_id=_commit_uid,
            upload_batch_id=upload_batch_id,
            committed_by=get_actor(),
        )
        _response = {
            "success": True,
            "message": f"{name.upper()} context committed successfully",
            "data": result,
        }
        _idempotency.remember(
            plant_code_id, f"commit_{name}", idempotency_key, body, _response,
            request_fingerprint=_idem_fingerprint,
        )
        return _response
    except HTTPException as e:
        raise as_envelope_exc(e)
    except Exception as exc:
        return server_error(exc, action=f"committing {name} context")


@router.get(
    "/get",
    summary="Get staged review rows for any connector",
    description="One endpoint for all supported connectors, the same way POST /pipeline/run "
    "serves every stage. `connector` selects the flow; the per-connector endpoints "
    "(getPnidContext, getDocsContext, getTsContext, getSapContext) remain available "
    "and are unchanged.",
)
def getContext(
    connector: str = Query(
        ...,
        description=(
            "pnid | documents | timeseries | sap | "
            "aif | gloc | lopc | other_files"
        ),
    ),
    plant_code_id: str = Query(..., description="Plant code ID for the request"),
    upload_batch_id: str | None = Query(None, description="Scope to one upload batch"),
    files: str | None = Query(None, description="P&ID / Documents — comma-separated file names"),
    doc_types: str | None = Query(None, description="Documents only — comma-separated subtypes"),
    source_file: str | None = Query(
        None,
        description="Timeseries / findings / Other Files — one source file",
    ),
    table: str | None = Query(
        None,
        description="SAP table or Other Files user-defined table",
    ),
    unmatched_only: bool = Query(
        False, description="AIF / GLOC / LOPC — only rows with no equipment identity"
    ),
    pipeline_job_id: str | None = Query(
        None, description="Accepted for backward compatibility; ignored for scoping"
    ),
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=1000),
):
    source = _resolve_source(connector)
    if source is None:
        return _invalid_source(connector)
    if _findings.is_findings_connector(source):
        return getFindingsContext(
            connector=source,
            plant_code_id=plant_code_id,
            upload_batch_id=upload_batch_id,
            source_file=source_file,
            unmatched_only=unmatched_only,
            page=page,
            page_size=page_size,
        )
    if source == "pnid":
        return getPnidContext(
            upload_batch_id=upload_batch_id,
            files=files,
            pipeline_job_id=pipeline_job_id,
            plant_code_id=plant_code_id,
        )
    if source == "docs":
        return getDocsContext(
            upload_batch_id=upload_batch_id,
            files=files,
            doc_types=doc_types,
            pipeline_job_id=pipeline_job_id,
            plant_code_id=plant_code_id,
            page=page,
            page_size=page_size,
        )
    if source == "other_files":
        return getOtherFilesContext(
            table=table,
            plant_code_id=plant_code_id,
            upload_batch_id=upload_batch_id,
            source_file=source_file,
            page=page,
            page_size=page_size,
        )
    if source == "ts":
        return getTsContext(
            upload_batch_id=upload_batch_id,
            source_file=source_file,
            plant_code_id=plant_code_id,
            page=page,
            page_size=page_size,
        )
    return getSapContext(
        table=table,
        plant_code_id=plant_code_id,
        page=page,
        page_size=page_size,
    )



@router.post(
    "/commitOtherFilesContext",
    summary="Commit reviewed Other Files data",
    description=(
        "Commit a user-defined Other Files table after review. "
        "The dynamic schema is preserved in object storage and the "
        "existing docs flow lifecycle is marked committed with audit lineage."
    ),
    status_code=HTTPStatus.OK,
)
def commitOtherFilesContext(
    body: dict = Body(...),
    idempotency_key: str | None = Header(
        default=None,
        alias="Idempotency-Key",
    ),
):
    try:
        plant_code_id = str(
            body.get("plant_code_id") or ""
        ).strip()

        if not plant_code_id:
            return JSONResponse(
                status_code=HTTPStatus.BAD_REQUEST,
                content={
                    "success": False,
                    "message": "Validation failed",
                    "errors": [
                        {
                            "field": "plant_code_id",
                            "message": "plant_code_id is required",
                        }
                    ],
                },
            )

        table = str(
            body.get("table") or ""
        ).strip()

        if not table:
            return JSONResponse(
                status_code=HTTPStatus.BAD_REQUEST,
                content={
                    "success": False,
                    "message": "Validation failed",
                    "errors": [
                        {
                            "field": "table",
                            "message": "table is required",
                        }
                    ],
                },
            )

        safe_table = re.sub(
            r"[^a-z0-9_-]",
            "_",
            table.lower(),
        ).strip("_")

        if not safe_table:
            return JSONResponse(
                status_code=HTTPStatus.BAD_REQUEST,
                content={
                    "success": False,
                    "message": "Validation failed",
                    "errors": [
                        {
                            "field": "table",
                            "message": "Invalid table name",
                        }
                    ],
                },
            )

        from ..plants import validate_plant_code as _vp

        _vp(plant_code_id)

        upload_batch_id = (
            body.get("upload_batch_id")
            or body.get("batch_id")
            or body.get("pipeline_job_id")
        )

        idem_fingerprint = _idempotency.fingerprint(
            body
        )

        replayed, conflict = _idempotency.lookup(
            plant_code_id,
            "commit_other_files",
            idempotency_key,
            body,
        )

        if conflict == "body_mismatch":
            return JSONResponse(
                status_code=HTTPStatus.CONFLICT,
                content={
                    "success": False,
                    "message": (
                        "This Idempotency-Key was already used "
                        "with a different request body."
                    ),
                    "errors": [
                        {
                            "field": "Idempotency-Key",
                            "message": "Key reused with a different body.",
                        }
                    ],
                },
            )

        if replayed is not None:
            return replayed

        rows = body.get("rows") or []

        if not rows:
            rows = _read_stage_csvs(
                "other_files",
                table_name=safe_table,
                plant_code_id=plant_code_id,
            )

        if upload_batch_id:
            rows = _filter_rows_by_batch(
                rows,
                upload_batch_id,
            )

        source_file = body.get("source_file")
        if source_file:
            rows = _filter_rows_by_source_file(
                rows,
                source_file,
            )

        if not rows:
            return nothing_to_commit(
                "other_files",
                plant_code_id,
            )

        # Preserve the dynamic schema in the reviewed stage output. Other Files
        # deliberately do not coerce into the fixed document_metadata schema.
        written = _write_processed_other_files(
            plant_code_id,
            safe_table,
            rows,
        )

        total = len(rows)
        tables_committed = [
            {
                "table": safe_table,
                "rows_written": total,
                "storage": "object_store",
                "path": written.get(safe_table),
                "dynamic_schema": True,
            }
        ]

        commit_uid = _record_commit_audit(
            "other_files",
            plant_code_id,
            tables_committed,
            total,
            None,
            detail={
                "table": safe_table,
                "upload_batch_id": upload_batch_id,
                "dynamic_schema": True,
                "storage": "object_store",
            },
        )

        committed_files = (
            sorted(
                {
                    str(row.get("source_file")).strip()
                    for row in rows
                    if row.get("source_file")
                }
            )
            or None
        )

        # Uploads for Other Files use the existing documents/docs lifecycle.
        _advance_flow(
            plant_code_id,
            "docs",
            file_names=committed_files,
            stage="committed",
            status="done",
            commit_uid=commit_uid,
            rows_committed=total,
        )

        result = {
            "status": "committed",
            "commit_id": commit_uid,
            "table": safe_table,
            "tables": tables_committed,
            "rows_written": total,
            "total_rows_written": total,
        }

        result = normalise_commit(
            result,
            connector="other_files",
            commit_id=commit_uid,
            upload_batch_id=upload_batch_id,
            committed_by=get_actor(),
        )

        response = {
            "success": True,
            "message": "Other Files context committed successfully",
            "data": result,
        }

        _idempotency.remember(
            plant_code_id,
            "commit_other_files",
            idempotency_key,
            body,
            response,
            request_fingerprint=idem_fingerprint,
        )

        return response

    except HTTPException:
        raise
    except Exception as exc:
        return server_error(
            exc,
            action="committing Other Files context",
        )


@router.post(
    "/commit",
    summary="Commit reviewed rows for any connector",
    description="One endpoint for all supported connectors. `connector` selects the flow. "
    "The per-connector endpoints (commitPnidContext, commitDocsContext, commitTsContext, "
    "commitSapContext) remain available and are unchanged.",
)
def commitContext(
    body: dict = Body(...),
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
):
    source = _resolve_source(body.get("connector") or body.get("source"))
    if source is None:
        return _invalid_source(body.get("connector") or body.get("source"))
    if source == "pnid":
        return commitPnidContext(body=body, idempotency_key=idempotency_key)
    if source == "docs":
        return commitDocsContext(body=body, idempotency_key=idempotency_key)
    if source == "other_files":
        return commitOtherFilesContext(
            body=body,
            idempotency_key=idempotency_key,
        )
    if source == "ts":
        return commitTsContext(body=body, idempotency_key=idempotency_key)
    if _findings.is_findings_connector(source):
        return commitFindingsContext(body=body, idempotency_key=idempotency_key)
    return commitSapContext(body=body, idempotency_key=idempotency_key)


@router.patch(
    "/rows",
    summary="Persist review edits for any connector",
    description="One endpoint for all supported connectors. `connector` selects the flow. "
    "The per-connector endpoints (patchPnidRows, patchDocsRows, patchTsRows, patchSapRows) "
    "remain available and are unchanged.",
)
def patchContextRows(
    body: dict = Body(...),
):
    source = _resolve_source(body.get("connector") or body.get("source"))
    if source is None:
        return _invalid_source(body.get("connector") or body.get("source"))
    if source == "pnid":
        return patchPnidRows(body=body)
    if source == "docs":
        return patchDocsRows(body=body)
    if source == "other_files":
        return patchOtherFilesRows(body=body)
    if source == "ts":
        return patchTsRows(body=body)
    if _findings.is_findings_connector(source):
        return patchFindingsRows(body=body)
    return patchSapRows(body=body)