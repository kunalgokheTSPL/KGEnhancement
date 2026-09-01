"""Pipeline trigger and status endpoints."""

from __future__ import annotations

import logging
import os
import re
import shutil as _shutil
import subprocess
import sys
import tempfile as _tempfile
import threading
import time
import uuid
from datetime import datetime, timezone
from http import HTTPStatus
from pathlib import Path

from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import JSONResponse

_REPO_ROOT = Path(__file__).resolve().parents[4]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
from ..services import pipeline_progress as _progress
from ..services.audit import get_job
from ..responses import EnvelopeRoute, as_envelope_exc, server_error, human_message
from ..services.pipeline_stages import (
    _append_pipeline_cancel_marker,
    _build_stage_args,
    _diagnose_zero_extraction,
    _kill_by_pid,
    _kill_process_group,
    _pid_is_alive,
    _resolve_industry,
)
from ..audit_context import get_actor, spawn_with_context
from ..plants import validate_plant_code as _vp
from ..services.connector_jobs import _upload_jobs

if __package__:
    from ..services import flow as _flow
    from ..services.audit import upsert_job
    from ..config import (
        BASE_DIR,
        DATA_DIR,
        RUSTFS_ACCESS_KEY,
        RUSTFS_ENDPOINT,
        RUSTFS_SECRET_KEY,
        p0_DIR,
    )
    from ..config import rustfs_out_dir
    from ..config import rustfs_out_dir as _out
    from ..config import (
        rustfs_processed_docs,
        rustfs_processed_ts,
        rustfs_staging_pnid,
        rustfs_staging_gloc,
    )
    from ..schemas.pipeline import RunPipelineRequest
    from p0.utils import fs as _fs
    from p0.driver import get_object_fs
else:
    from p0.api.services import flow as _flow
    from p0.api.services.audit import get_job
    from p0.api.services.audit import upsert_job
    from p0.api.config import (
        BASE_DIR,
        DATA_DIR,
        RUSTFS_ACCESS_KEY,
        RUSTFS_ENDPOINT,
        RUSTFS_SECRET_KEY,
        p0_DIR,
        rustfs_out_dir,
        rustfs_processed_docs,
        rustfs_processed_ts,
        rustfs_staging_pnid,
        rustfs_staging_gloc,
    )
    from p0.api.schemas.pipeline import RunPipelineRequest
    from p0.utils import fs as _fs
    from p0.driver import get_object_fs


router = APIRouter(route_class=EnvelopeRoute, prefix="/pipeline", tags=["Pipeline"])

_log = logging.getLogger("cdm.api.pipeline")

_pipeline_jobs: dict[str, dict] = {}


def _persist_job(pipeline_job_id: str) -> None:
    """Write-through the current in-memory job state to the DB. Best-effort"""
    job = _pipeline_jobs.get(pipeline_job_id)
    if job is None:
        return
    try:
        upsert_job(pipeline_job_id, job)
    except Exception as exc:
        _log.warning("[pipeline] pipeline_jobs upsert failed: %s", exc)


_STAGE_TO_FLOW: dict[str, str] = {
    "ts": "ts",
    "docs": "docs",
    "pnid": "pnid",
    "aif": "aif",
    "gloc": "gloc",
    "lopc": "lopc",
    "upd_event": "upd_event",
    "trip_event": "trip_event",
    # Other Files currently shares the documents flow_state lifecycle.
    "other_files": "docs",
    "alerts": "alerts",
}



def _advance_flow_for_stage(
    pipeline_stage: str,
    plant_code_id: str,
    file_names: list[str] | None = None,
    row_count_by_file: dict[str, int] | None = None,
    file_outcomes: dict[str, dict] | None = None,
    **fields,
) -> None:
    """Best-effort: advance flow_state rows for this plant+flow."""
    flow = _STAGE_TO_FLOW.get(pipeline_stage)
    if not flow:
        return
    target_stage = fields.pop("stage", None)

    def _leaf(name) -> str:
        return str(name or "").strip().replace("\\", "/").rsplit("/", 1)[-1]

    try:
        wanted = None
        if file_names:
            wanted = {_leaf(n) for n in file_names if n}
        rc_by_leaf = {_leaf(k): v for k, v in (row_count_by_file or {}).items()}
        for row in _flow.list_flow(plant_code_id, flow):
            cur_stage = row.get("stage")
            if cur_stage in ("reviewed", "committing", "committed"):
                continue
            fn = _leaf(row.get("file_name"))
            if wanted is not None and fn not in wanted:
                continue
            row_fields = dict(fields)
            if fn in rc_by_leaf:
                row_fields["row_count"] = rc_by_leaf[fn]
            row_stage = target_stage
            outcome = (file_outcomes or {}).get(fn)
            if outcome:
                if outcome.get("status") in _progress.FAILED_FILE_STATUSES:
                    row_fields["status"] = "failed"
                    row_fields["error"] = (outcome.get("error") or "Processing failed")[:500]
                    row_stage = None
                elif outcome.get("status") == "skipped":
                    row_fields["status"] = "skipped"
                    row_stage = None
                else:
                    row_fields["status"] = "done"
                    row_fields["error"] = None
                if isinstance(outcome.get("rows"), int):
                    row_fields["row_count"] = outcome["rows"]
            if row_stage:
                _flow.advance_stage(
                    row["flow_uid"], row_stage, plant_code_id=plant_code_id, **row_fields
                )
            else:
                _flow.advance(row["flow_uid"], plant_code_id=plant_code_id, **row_fields)
            continue
    except Exception as exc:
        _log.warning("[pipeline] flow file-name resolution failed: %s", exc)



_STAGE_FLOWS: dict[str, tuple[str, ...]] = {
    "ts": ("ts", "ts_values"),
    "docs": ("docs",),
    "pnid": ("pnid",),
    "sap": ("sap",),
    "aif": ("aif",),
    "gloc": ("gloc",),
    "lopc": ("lopc",),
    "other_files": ("docs",),
    "alerts": ("alerts",),
    "upd_event": ("upd_event",),
    "trip_event": ("trip_event",),

}


def _pending_file_count(plant_code_id: str, stage: str, upload_batch_id: str | None = None) -> int:
    """How many files this run will touch, so progress has an honest denominator."""
    flows = _STAGE_FLOWS.get(stage)
    if not flows:
        return 0
    total = 0
    try:
        for flow in flows:
            for row in _flow.list_flow(plant_code_id, flow, limit=100000):
                if row.get("stage") in ("committed", "committing"):
                    continue
                if upload_batch_id and row.get("upload_batch_id") not in (None, upload_batch_id):
                    continue
                total += 1
    except Exception as exc:
        _log.warning("[pipeline] file-count for %s/%s failed: %s", plant_code_id, stage, exc)
    return total


_STAGE_SCRIPTS: dict[str, str] = {
    "sap_mapping": "pipelines/run_sap_pid_mapping.py",
    "sap": "pipelines/run_sap_end_to_end.py",
    "pnid": "pipelines/run_pnid_from_pdfs.py",
    "ts": "pipelines/run_ts_end_to_end.py",
    "docs": "pipelines/run_docs_end_to_end.py",
    "aif": "pipelines/run_findings_end_to_end.py",
    "gloc": "pipelines/run_findings_end_to_end.py",
    "lopc": "pipelines/run_findings_end_to_end.py",
    "upd_event": "pipelines/run_findings_end_to_end.py",
    "trip_event": "pipelines/run_findings_end_to_end.py",
    "full": "pipelines/build_final_cdm.py",
    "other_files": "pipelines/run_other_files_end_to_end.py",
    "alerts": "pipelines/run_alerts_end_to_end.py",
}



_STAGE_LOCKS: dict[str, threading.Lock] = {
    "sap_mapping": threading.Lock(),
    "sap": threading.Lock(),
    "pnid": threading.Lock(),
    "ts": threading.Lock(),
    "docs": threading.Lock(),
    "aif": threading.Lock(),
    "gloc": threading.Lock(),
    "lopc": threading.Lock(),
    "upd_event": threading.Lock(),
    "trip_event": threading.Lock(),
    "full": threading.Lock(),
    "other_files": threading.Lock(),
    "alerts": threading.Lock(),
}





def _public_job_view(job: dict) -> dict:
    """Strip internal/non-serialisable fields (e.g. the Popen handle) before"""
    return {k: v for k, v in job.items() if not k.startswith("_")}












def _run_pipeline_bg(
    pipeline_job_id: str,
    stage: str,
    plant_code_id: str,
    industry: str,
    file_names: list[str] | None = None,
    doc_types: list[str] | None = None,
    doc_files: list[str] | None = None,
    upload_batch_id: str | None = None,
    other_type: str | None = None,
    equipment_column: str | None = None,
    extraction_fields: list[str] | None = None,
) -> None:
    """Run a pipeline script in a background thread."""
    _stage_log = logging.getLogger(f"cdm.pipeline.{stage}")
    

    if _pipeline_jobs[pipeline_job_id].get("status") == "cancelled":
        _stage_log.info(
            "[pipeline/%s] JOB SKIPPED (cancelled before start)  pipeline_job_id=%s",
            stage,
            pipeline_job_id,
        )
        return

    _lock = _STAGE_LOCKS.get(stage)
    if _lock is not None:
        if not _lock.acquire(blocking=False):
            _stage_log.info(
                "[pipeline/%s] WAITING for previous run to finish  pipeline_job_id=%s",
                stage,
                pipeline_job_id,
            )
            _lock.acquire()
        if _pipeline_jobs[pipeline_job_id].get("status") == "cancelled":
            _lock.release()
            _stage_log.info(
                "[pipeline/%s] JOB SKIPPED (cancelled while queued)  pipeline_job_id=%s",
                stage,
                pipeline_job_id,
            )
            return

    _lock_held = _lock is not None

    work_dir: str | None = None

    try:
        _pipeline_jobs[pipeline_job_id]["status"] = "running"
        started_at = datetime.now(timezone.utc)
        _pipeline_jobs[pipeline_job_id]["started_at"] = started_at.isoformat()
        _persist_job(pipeline_job_id)
        _advance_flow_for_stage(
            stage,
            plant_code_id,
            file_names=file_names,
            stage="processing",
            status="running",
            process_job_id=pipeline_job_id,
            error=None,
        )

        _stage_log.info(
            "[pipeline/%s] JOB START  pipeline_job_id=%s  plant=%s  industry=%s  files=%s",
            stage,
            pipeline_job_id,
            plant_code_id,
            industry,
            file_names or "(all)",
        )

        work_dir = _tempfile.mkdtemp(prefix=f"cdm_{stage}_")
        _pipeline_jobs[pipeline_job_id]["work_dir"] = work_dir
        _stage_log.debug("[pipeline/%s] ephemeral work_dir: %s", stage, work_dir)

        script = _STAGE_SCRIPTS.get(stage)
        if not script:
            raise ValueError(f"Unknown stage: {stage}")

        script_path = p0_DIR / script
        if not script_path.exists():
            raise FileNotFoundError(f"Pipeline script not found: {script_path}")

        env = {
            **os.environ,
            "PYTHONPATH": str(BASE_DIR),
            "PYTHONIOENCODING": "utf-8",
            "PYTHONUTF8": "1",
            "PLANT_CODE": plant_code_id,
            "CDM_INDUSTRY": industry,
            "P0_DATA_DIR": str(DATA_DIR),
            "RUSTFS_ACCESS_KEY": RUSTFS_ACCESS_KEY,
            "RUSTFS_SECRET_KEY": RUSTFS_SECRET_KEY,
            "RUSTFS_ENDPOINT": RUSTFS_ENDPOINT,
            "RUSTFS_BUCKET": os.environ.get("RUSTFS_BUCKET", ""),
        }

        stage_args = _build_stage_args(
            stage,
            plant_code_id,
            file_names,
            doc_types,
            doc_files,
            work_dir=work_dir,
            upload_batch_id=upload_batch_id or pipeline_job_id,
            other_type=other_type,
            equipment_column=equipment_column,
            extraction_fields=extraction_fields,
        )
        cmd = [sys.executable, str(script_path)] + stage_args
        _stage_log.debug("[pipeline/%s] subprocess cmd: %s", stage, " ".join(cmd))
        _stage_log.debug(
            "[pipeline/%s] cwd=%s  CDM_INDUSTRY=%s  RUSTFS_ENDPOINT=%s",
            stage,
            str(p0_DIR),
            industry,
            RUSTFS_ENDPOINT,
        )

        t0 = time.time()
        _preexec = os.setsid if hasattr(os, "setsid") else None
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            cwd=str(p0_DIR),
            env=env,
            preexec_fn=_preexec,
        )
        _pipeline_jobs[pipeline_job_id]["_process"] = proc
        _pipeline_jobs[pipeline_job_id]["pid"] = proc.pid
        _STAGE_TIMEOUTS = {
            "docs": 14400,
            "pnid": 7200,
            "ts": 1800,
            "sap": 1800,
            "aif": 1800,
            "gloc": 1800,
            "lopc": 1800,
            "other_files": 14400,
            "full": 1800,
            "alerts": 1800,
        }
        _timeout = _STAGE_TIMEOUTS.get(stage, 1800)

        def _on_line(raw: str) -> None:
            job = _pipeline_jobs.get(pipeline_job_id)
            if job is None:
                return
            payload = _progress.parse_marker(raw)
            if payload is not None:
                _progress.apply_marker(job, stage, payload)
                _persist_job(pipeline_job_id)
                return
            file_payload = _progress.parse_file_marker(raw)
            if file_payload is not None:
                _progress.apply_file_marker(job, stage, file_payload)
                _persist_job(pipeline_job_id)

        def _on_timeout(p) -> None:
            _stage_log.error(
                "[pipeline/%s] TIMEOUT after %ds — killing process group", stage, _timeout
            )
            _kill_process_group(p)

        returncode, stdout, stderr = _progress.stream_process(
            proc, timeout=_timeout, on_line=_on_line, on_kill=_on_timeout
        )
        elapsed = time.time() - t0
        _pipeline_jobs[pipeline_job_id].pop("_process", None)

        class _R:
            pass

        result = _R()
        result.returncode = returncode
        result.stdout = stdout
        result.stderr = stderr

        _pipeline_jobs[pipeline_job_id]["completed_at"] = datetime.now(timezone.utc).isoformat()
        _stage_log.debug(
            "[pipeline/%s] returncode=%d  elapsed=%.1fs",
            stage,
            result.returncode,
            elapsed,
        )

        if result.stdout:
            _stage_log.debug(
                "[pipeline/%s] subprocess stdout:\n%s", stage, result.stdout[-2000:]
            )
        if result.stderr:
            _stage_log.debug(
                "[pipeline/%s] subprocess stderr:\n%s", stage, result.stderr[-1000:]
            )

        if _pipeline_jobs[pipeline_job_id].get("status") == "cancelled":
            _stage_log.info(
                "[pipeline/%s] JOB CANCELLED  pipeline_job_id=%s  elapsed=%.1fs",
                stage,
                pipeline_job_id,
                elapsed,
            )
        elif result.returncode == 0:
            _outcomes = _pipeline_jobs[pipeline_job_id].get("file_outcomes") or {}
            _final_status = _progress.outcome_status(result.returncode, _outcomes)
            _pipeline_jobs[pipeline_job_id]["status"] = _final_status
            _progress.finalise_progress(
                _pipeline_jobs[pipeline_job_id], succeeded=_final_status != "failed"
            )
            _pipeline_jobs[pipeline_job_id]["log"] = (
                result.stdout[-3000:] if result.stdout else ""
            )
            _combined = (result.stdout or "") + (result.stderr or "")
            _counts: dict[str, int] = {}
            for _m in re.finditer(r"(\w+)\s+rows[:\s]+(\d+)", _combined):
                _tbl, _cnt = _m.group(1), int(_m.group(2))
                if _cnt > 0:
                    _counts[_tbl] = max(_counts.get(_tbl, 0), _cnt)
            _pipeline_jobs[pipeline_job_id]["record_counts"] = _counts
            _pipeline_jobs[pipeline_job_id]["total_records"] = sum(_counts.values())

            out_stage = f"{rustfs_out_dir(plant_code_id)}/{stage}/entities"
            tables: list[str] = []
            stems: set[str] = set()
            for pat in ("*.parquet", "*.csv"):
                try:
                    stems |= {
                        Path(p).stem for p in _fs.glob_files(f"{out_stage}/{pat}")
                    }
                except Exception as exc:
                    _log.warning("[pipeline] output stem scan failed: %s", exc)
            tables = sorted(stems)
            _pipeline_jobs[pipeline_job_id]["output_tables"] = tables

            warning = (
                _diagnose_zero_extraction(result.stdout, result.stderr)
                if _pipeline_jobs[pipeline_job_id]["total_records"] == 0
                else None
            )
            if warning:
                _pipeline_jobs[pipeline_job_id]["warning"] = warning
                _stage_log.warning(
                    "[pipeline/%s] JOB COMPLETED — NO DATA EXTRACTED  pipeline_job_id=%s  "
                    "elapsed=%.1fs  reason=%s",
                    stage,
                    pipeline_job_id,
                    elapsed,
                    warning.get("reason"),
                )
            else:
                _stage_log.info(
                    "[pipeline/%s] JOB COMPLETED  pipeline_job_id=%s  elapsed=%.1fs  output_tables=%s",
                    stage,
                    pipeline_job_id,
                    elapsed,
                    tables,
                )
            _processed_parquet = None
            _flow_rows = _pipeline_jobs[pipeline_job_id].get("total_records")
            _row_count_by_file: dict[str, int] = {}
            if stage == "ts":
                _processed_parquet = (
                    f"{rustfs_processed_ts(plant_code_id)}/ts_timeseries_metadata.parquet"
                )
                try:
                    _df = _fs.read_parquet(_processed_parquet)
                    if _df is not None and len(_df) > 0:
                        _flow_rows = len(_df)
                except Exception as exc:
                    _log.warning("[pipeline] processed-parquet read failed: %s", exc)
            if (not _pipeline_jobs[pipeline_job_id].get("total_records")) and _flow_rows:
                _pipeline_jobs[pipeline_job_id]["total_records"] = _flow_rows
                if not _pipeline_jobs[pipeline_job_id].get("record_counts"):
                    _tbl = (
                        "timeseries_metadata"
                        if stage == "ts"
                        else "documents"
                        if stage == "docs"
                        else stage
                    )
                    _pipeline_jobs[pipeline_job_id]["record_counts"] = {_tbl: _flow_rows}
                _pipeline_jobs[pipeline_job_id].pop("warning", None)
            elif stage == "docs":
                _processed_parquet = rustfs_processed_docs(plant_code_id)
                try:
                    _total = 0
                    for _p in _fs.glob_files(
                        f"{_processed_parquet}/*/docs_documents.parquet"
                    ):
                        _d = _fs.read_parquet(_p)
                        if _d is None:
                            continue
                        _total += len(_d)
                        if "source_file" in getattr(_d, "columns", []):
                            for _sf, _cnt in _d["source_file"].value_counts().items():
                                _leaf = (
                                    str(_sf)
                                    .strip()
                                    .replace("\\", "/")
                                    .rsplit("/", 1)[-1]
                                )
                                _row_count_by_file[_leaf] = _row_count_by_file.get(
                                    _leaf, 0
                                ) + int(_cnt)
                    if _total > 0:
                        _flow_rows = _total
                except Exception as exc:
                    _log.warning("[pipeline] output row-count failed: %s", exc)
            _advance_flow_for_stage(
                stage,
                plant_code_id,
                file_names=file_names,
                row_count_by_file=_row_count_by_file or None,
                file_outcomes=_outcomes or None,
                stage="processed",
                status="done",
                processed_parquet=_processed_parquet,
                row_count=_flow_rows,
                upload_batch_id=upload_batch_id,
            )
        else:
            err_msg = (result.stderr or result.stdout or "Unknown error")[-3000:]
            _pipeline_jobs[pipeline_job_id]["status"] = "failed"
            _progress.finalise_progress(_pipeline_jobs[pipeline_job_id], succeeded=False)
            _pipeline_jobs[pipeline_job_id]["error"] = err_msg
            _advance_flow_for_stage(
                stage,
                plant_code_id,
                file_names=file_names,
                file_outcomes=_pipeline_jobs[pipeline_job_id].get("file_outcomes") or None,
                status="failed",
                error=err_msg[:500],
            )
            _stage_log.error(
                "[pipeline/%s] JOB FAILED  pipeline_job_id=%s  elapsed=%.1fs\nerror:\n%s",
                stage,
                pipeline_job_id,
                elapsed,
                err_msg,
            )
    except Exception as exc:
        _pipeline_jobs[pipeline_job_id]["status"] = "failed"
        _progress.finalise_progress(_pipeline_jobs[pipeline_job_id], succeeded=False)
        _pipeline_jobs[pipeline_job_id]["error"] = human_message(exc, action="running the pipeline", plant_code_id=plant_code_id)
        _pipeline_jobs[pipeline_job_id]["completed_at"] = datetime.now(timezone.utc).isoformat()
        _advance_flow_for_stage(
            stage,
            plant_code_id,
            file_names=file_names,
            status="failed",
            error=human_message(exc, action="running the pipeline", plant_code_id=plant_code_id)[:500],
        )
        _stage_log.exception(
            "[pipeline/%s] JOB EXCEPTION  pipeline_job_id=%s: %s",
            stage,
            pipeline_job_id,
            exc,
        )
    finally:
        if work_dir:
            try:
                _shutil.rmtree(work_dir, ignore_errors=True)
                _stage_log.debug(
                    "[pipeline/%s] cleaned up work_dir: %s", stage, work_dir
                )
            except Exception:
                pass
        if _lock_held and _lock is not None:
            try:
                _lock.release()
                _stage_log.debug("[pipeline/%s] released stage lock", stage)
            except RuntimeError:
                pass
        _persist_job(pipeline_job_id)




def _latest_staged_pnid_file(plant_code_id: str) -> list[str]:
    """The single most-recently-modified .pdf in the plant's P&ID staging dir."""
    try:
        sfs = get_object_fs(use_listings_cache=False)
        base = _fs.strip_scheme(rustfs_staging_pnid(plant_code_id))
        entries = sfs.ls(base, detail=True)
    except Exception:
        return []
    pdfs = [
        (
            e.get("name", "").split("/")[-1],
            e.get("LastModified") or e.get("last_modified"),
        )
        for e in entries
        if str(e.get("name", "")).lower().endswith(".pdf")
    ]
    pdfs = [(n, m) for n, m in pdfs if n and m is not None]
    if not pdfs:
        return []
    pdfs.sort(key=lambda x: str(x[1]), reverse=True)
    return [pdfs[0][0]]


def _latest_staged_gloc_file(
    plant_code_id: str,
) -> list[str]:
    """
    Return the most recently modified real GLOC source file.

    Important:
    gloc_manifest.parquet is staging metadata and must never be
    passed to the structured GLOC processing pipeline.
    """
    try:
        sfs = get_object_fs(
            use_listings_cache=False
        )

        base = _fs.strip_scheme(
            rustfs_staging_gloc(
                plant_code_id
            )
        )

        entries = sfs.ls(
            base,
            detail=True,
        )

    except Exception as exc:
        _log.warning(
            "[pipeline/gloc] failed to list GLOC staging: %s",
            exc,
        )
        return []

    allowed_extensions = (
        ".xlsx",
        ".xls",
        ".csv",
        ".parquet",
    )

    excluded_files = {
        "gloc_manifest.parquet",
    }

    files = []

    for entry in entries:

        full_name = str(
            entry.get(
                "name",
                "",
            )
        )

        file_name = (
            full_name
            .replace("\\", "/")
            .rsplit("/", 1)[-1]
        )

        if not file_name:
            continue

        # Never process the staging manifest as GLOC data.
        if file_name.lower() in excluded_files:
            continue

        if not file_name.lower().endswith(
            allowed_extensions
        ):
            continue

        modified = (
            entry.get("LastModified")
            or entry.get("last_modified")
        )

        if modified is None:
            continue

        files.append(
            (
                file_name,
                modified,
            )
        )

    if not files:
        _log.warning(
            "[pipeline/gloc] no valid GLOC source files "
            "found in staging: %s",
            base,
        )
        return []

    files.sort(
        key=lambda item: str(item[1]),
        reverse=True,
    )

    selected = files[0][0]

    _log.info(
        "[pipeline/gloc] latest staged GLOC source → %s",
        selected,
    )

    return [
        selected
    ]
@router.post(
    "/run",
    status_code=HTTPStatus.ACCEPTED,
    summary="Trigger a pipeline stage",
    description=(
        "Start a CDM pipeline stage asynchronously. "
        "Returns a `pipeline_job_id` to poll status.\n\n"
        "**Stages:**\n"
        "- `sap` — Process SAP PM/MM tables → canonical entities\n"
        "- `pnid` — Extract equipment from P&ID PDFs → canonical entities\n"
        "- `ts` — Process timeseries tag metadata → canonical entities\n"
        "- `docs` — Process documents (SOP, RCA, O&M) → canonical entities\n"
        "- `aif` — Process Asset Integrity Findings\n"
        "- `gloc` — Process GLOC files using the dedicated GLOC pipeline\n"
        "- `lopc` — Process Loss of Primary Containment findings\n"
        "- `other_files` — Process user-defined structured/unstructured files\n"
        "- `alerts` — Process Open/Closed ALERTS workbook into canonical alerts\n"
        "- `full` — Merge all source outputs into final CDM tables"
    ),
)

def runPipeline(
    req: RunPipelineRequest,
):
    errors = []

    stage = (req.stage or "").strip().lower()
    plant_code_id = (req.plant_code_id or "").strip()

    allowed_stages = ["sap_mapping", "sap", "pnid", "ts", "docs", "aif", "gloc", "lopc", "upd_event", "trip_event", "other_files", "full", "alerts"]

    if not stage:
        errors.append(
            {
                "field": "stage",
                "message": "stage is required",
            }
        )
    elif stage not in allowed_stages:
        errors.append(
            {
                "field": "stage",
                "message": (
                    "Invalid stage. Must be one of: "
                    + ", ".join(allowed_stages)
                ),
            }
        )



    if not plant_code_id:
        errors.append(
            {
                "field": "plant_code_id",
                "message": "plant_code_id is required",
            }
        )

    if req.industry is not None and not req.industry.strip():
        errors.append(
            {
                "field": "industry",
                "message": "industry cannot be empty if provided",
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

    try:

        pipeline_job_id = str(uuid.uuid4())
        actor = get_actor()
        upload_batch_id = req.upload_batch_id or pipeline_job_id
        industry = _resolve_industry(req.industry, req.plant_code_id)
        _log.info(
            "[pipeline] POST /run  stage=%s  plant=%s  industry=%s  "
            "file_names=%s  pipeline_job_id=%s",
            stage,
            plant_code_id,
            industry,
            req.file_names or "(all)",
            pipeline_job_id,
        )

        logging.getLogger(f"cdm.pipeline.{stage}").info(
            "[pipeline/%s] JOB QUEUED  pipeline_job_id=%s  "
            "plant=%s  industry=%s",
            stage,
            pipeline_job_id,
            plant_code_id,
            industry,
        )
        
        _pipeline_jobs[pipeline_job_id] = {
            "stage": stage,
            "plant_code_id": plant_code_id,
            "industry": industry,
            "upload_batch_id": upload_batch_id,
            "file_names": list(req.file_names or []),
            "doc_types": list(req.doc_types or []),
            "doc_files": list(req.doc_files or []),
            "other_type": req.other_type,
            "equipment_column": req.equipment_column,
            "extraction_fields": list(req.extraction_fields or []),
            "status": "queued",
            "created_by": actor,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "started_at": None,
            "completed_at": None,
            "error": None,
            "output_tables": None,
            "stages_planned": _progress.plan_for(req.stage),
            "files_total": _pending_file_count(
                req.plant_code_id, req.stage, req.upload_batch_id
            ),
            "phases": {},
            "progress_percent": 0,
            "is_determinate": _progress.is_determinate(req.stage),
            "current_phase": None,
            "current_label": None,
            "current_item": None,
        }

        _persist_job(pipeline_job_id)

        doc_types = req.doc_types
        doc_files = req.doc_files

        if stage == "docs" and not doc_files:
            derived_types, derived_files = _derive_docs_scope_from_uploads(
                plant_code_id
            )
            if derived_files:
                doc_files = derived_files
                if not doc_types:
                    doc_types = derived_types

                _log.info(
                    "[pipeline] docs scope auto-derived from uploads — "
                    "doc_types=%s doc_files=%d",
                    doc_types,
                    len(doc_files),
                )

        file_names = req.file_names

        if stage == "pnid":
            latest = _latest_staged_pnid_file(plant_code_id)

            if latest:
                file_names = latest

                if (
                    req.file_names
                    and {f.lower() for f in req.file_names}
                    != {f.lower() for f in latest}
                ):
                    _log.info(
                        "[pipeline] pnid file_names from staging (newest) → %s "
                        "(client sent %s, ignored)",
                        file_names,
                        req.file_names,
                    )

        elif stage == "other_files":
            if not req.other_type:
                return JSONResponse(
                    status_code=HTTPStatus.BAD_REQUEST,
                    content={
                        "success": False,
                        "message": "Validation failed",
                        "errors": [
                            {
                                "field": "other_type",
                                "message": "other_type is required for other_files stage",
                            }
                        ],
                    },
                )

            if not req.file_names:
                return JSONResponse(
                    status_code=HTTPStatus.BAD_REQUEST,
                    content={
                        "success": False,
                        "message": "Validation failed",
                        "errors": [
                            {
                                "field": "file_names",
                                "message": "file_names is required for other_files stage",
                            }
                        ],
                    },
                )

            file_names = req.file_names
            
        
        elif stage == "alerts":
            if not req.file_names:
                return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": [
                    {
                        "field": "file_names",
                        "message": (
                            "file_names is required for alerts stage"
                        ),
                    }
                ],
            },
        )

            file_names = req.file_names

        elif stage == "gloc":
            latest = _latest_staged_gloc_file(plant_code_id)

            if latest:
                file_names = latest

                if (
                    req.file_names
                    and {f.lower() for f in req.file_names}
                    != {f.lower() for f in latest}
                ):
                    _log.info(
                        "[pipeline/gloc] using latest staged GLOC file %s "
                        "instead of requested %s",
                        latest,
                        req.file_names,
                    )
            elif req.file_names:
                file_names = req.file_names

        spawn_with_context(
            target=_run_pipeline_bg,
            args=(
                pipeline_job_id,
                stage,
                plant_code_id,
                industry,
                file_names,
                doc_types,
                doc_files,
                upload_batch_id,
                req.other_type,
                req.equipment_column,
                req.extraction_fields,
            ),
            daemon=True,
        )

        return {
            "success": True,
            "message": "Pipeline started successfully",
            "data": {
                "pipeline_job_id": pipeline_job_id,
                "upload_batch_id": upload_batch_id,
                "upload_batch_ids": [req.upload_batch_id] if req.upload_batch_id else [],
                "scoped_to_batch": bool(req.upload_batch_id),
                "stage": stage,
                "plant_code_id": plant_code_id,

                "industry": industry,
                "stages_planned": _progress.plan_for(req.stage),
                "is_determinate": _progress.is_determinate(req.stage),
                "files_total": _pipeline_jobs[pipeline_job_id].get("files_total", 0),
                "progress_percent": 0,
            },
        }

    except HTTPException:
        raise

    except Exception as exc:
        return server_error(exc, action="running pipeline")


@router.get(
    "/jobs/{jobId}",
    summary="Get pipeline job status",
    description="Poll the status of a running or completed pipeline job.",
)
def getPipelineStatus(
    jobId: str,
    plant_code_id: str = Query(..., description="Plant code ID for the request"),
):

    errors = []
    if not plant_code_id or not plant_code_id.strip():
        errors.append(
            {
                "field": "plant_code_id",
                "message": "plant_code_id is required",
            }
        )

    if not jobId or not jobId.strip():
        errors.append(
            {
                "field": "jobId",
                "message": "jobId is required",
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

    try:
        job = _pipeline_jobs.get(jobId)
        if not job:
            persisted = get_job(jobId, plant_code_id)
            if persisted:

                return {
                    "success": True,
                    "message": "Job status fetched successfully",
                    "data": {
                        "pipeline_job_id": jobId,
                        **_public_job_view(persisted),
                    },
                }
            return JSONResponse(
                status_code=HTTPStatus.NOT_FOUND,
                content={
                    "success": False,
                    "message": "Pipeline job not found",
                    "errors": [
                        {
                            "field": "jobId",
                            "message": f"Job '{jobId}' not found",
                        }
                    ],
                },
            )

        return {
            "success": True,
            "message": "Job status fetched successfully",
            "data": {"pipeline_job_id": jobId, **_public_job_view(job)},
        }
    except HTTPException:
        raise
    except Exception as exc:
        return server_error(exc, action="loading pipeline status")



@router.post(
    "/jobs/{jobId}/cancel",
    summary="Cancel a running pipeline job",
    description="Send SIGTERM to the pipeline subprocess (and its whole process group, "
    "so the LLM extractor / Bedrock calls are also stopped). If the process "
    "doesn't exit within 5s, follows up with SIGKILL. "
    "Marks the job status as `cancelled`. "
    "No-op (with 200) if the job is already finished.",
)
def cancelPipelineJob(
    jobId: str,
    plant_code_id: str = Query(..., description="Plant code ID for the request"),
):

    errors = []
    if not plant_code_id or not plant_code_id.strip():
        errors.append(
            {
                "field": "plant_code_id",
                "message": "plant_code_id is required",
            }
        )

    if not jobId or not jobId.strip():
        errors.append(
            {
                "field": "jobId",
                "message": "jobId is required",
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

    try:
        job = _pipeline_jobs.get(jobId)
        if not job:
            return JSONResponse(
                status_code=HTTPStatus.NOT_FOUND,
                content={
                    "success": False,
                    "message": "Pipeline job not found",
                    "errors": [
                        {
                            "field": "jobId",
                            "message": f"Job '{jobId}' not found",
                        }
                    ],
                },
            )
        status = job.get("status")
        if status in ("completed", "failed", "cancelled"):
            return {
                "success": True,
                "message": "Job already finished",
                "data": {"pipeline_job_id": jobId, "status": status},
            }

        proc = job.get("_process")
        if proc is None:
            killed = _kill_by_pid(job.get("pid"))
            job["status"] = "cancelled"
            job["completed_at"] = datetime.now(timezone.utc).isoformat()
            _persist_job(jobId)
            if killed:
                _append_pipeline_cancel_marker(job.get("stage", ""), jobId, job.get("pid"))
                msg = "Termination signal sent to the detached pipeline process."
            elif status == "queued":
                msg = "The job was cancelled before it started."
            else:
                msg = "The pipeline process is no longer running; the job is marked cancelled."
            return {
                "success": True,
                "message": msg,
                "data": {"pipeline_job_id": jobId, "status": "cancelled"},
            }

        _log.info(
            "[pipeline] CANCEL requested  pipeline_job_id=%s  stage=%s  pid=%s",
            jobId,
            job.get("stage"),
            job.get("pid"),
        )
        logging.getLogger(f"cdm.pipeline.{job.get('stage', '?')}").warning(
            "[pipeline/%s] JOB CANCELLED by user  pipeline_job_id=%s  pid=%s",
            job.get("stage", "?"),
            jobId,
            job.get("pid"),
        )
        job["status"] = "cancelled"
        job["completed_at"] = datetime.now(timezone.utc).isoformat()
        _persist_job(jobId)

        _append_pipeline_cancel_marker(job.get("stage", ""), jobId, job.get("pid"))
        _kill_process_group(proc)

        return {
            "success": True,
            "message": "Termination signal sent",
            "data": {"pipeline_job_id": jobId, "status": "cancelled"},
        }
    except HTTPException:
        raise
    except Exception as exc:
        return server_error(exc, action="cancelling pipeline job")


@router.get(
    "/jobs",
    summary="List pipeline jobs",
    description="List all recent pipeline jobs, newest first. "
    "Optionally filter by `stage`.",
)
def listPipelineJobs(
    stage: str | None = None,
    plant_code_id: str = Query(..., description="Plant code ID for the request"),
):

    errors = []
    if not plant_code_id or not plant_code_id.strip():
        errors.append(
            {
                "field": "plant_code_id",
                "message": "plant_code_id is required",
            }
        )

    if stage is not None:
        stage_clean = stage.strip()
        if stage_clean:
            allowed_stages = ["sap_mapping", "sap", "pnid", "ts", "docs", "aif", "gloc", "lopc", "upd_event", "trip_event", "other_files", "full", "alerts"]

            if stage_clean.lower() not in allowed_stages:
                errors.append(
                    {
                        "field": "stage",
                        "message": f"Invalid stage. Must be one of: {', '.join(allowed_stages)}",
                    }
                )

    if plant_code_id is not None:
        plant_code_clean = plant_code_id.strip()
        if not plant_code_clean:
            errors.append(
                {
                    "field": "plant_code_id",
                    "message": "plant_code_id cannot be empty if provided",
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

    try:
        from p0.api.services.audit import list_jobs as _db_list
        from p0.api.services.audit import list_run_events as _list_re

        merged: dict[str, dict] = {}

        for row in _db_list(stage=stage, plant_code_id=plant_code_id, limit=100):
            merged[row["pipeline_job_id"]] = row
        for jid, info in _pipeline_jobs.items():
            if stage and info.get("stage") != stage:
                continue
            if plant_code_id and info.get("plant_code_id") != plant_code_id:
                continue
            merged[jid] = {"pipeline_job_id": jid, **_public_job_view(info)}

        try:
            dq_by_batch: dict[str, dict] = {}
            for e in _list_re(plant_code_id=plant_code_id, limit=500):
                b = e.get("upload_batch_id")
                if not b or e.get("rows_in") is None:
                    continue
                det = e.get("detail") if isinstance(e.get("detail"), dict) else {}
                cur = dq_by_batch.get(b, {"rows_in": 0, "rows_rejected": 0, "rejects": {}})
                cur["rows_in"] = max(cur["rows_in"], int(e.get("rows_in") or 0))
                cur["rows_rejected"] = max(
                    cur["rows_rejected"], int(e.get("rows_rejected") or 0)
                )
                for k, v in (det.get("rejects") or {}).items():
                    cur["rejects"][k] = cur["rejects"].get(k, 0) + int(v or 0)
                dq_by_batch[b] = cur
            for row in merged.values():
                dq = dq_by_batch.get(row.get("upload_batch_id") or row.get("pipeline_job_id"))
                if dq:
                    row["dq"] = dq
        except Exception as exc:
            _log.warning("[pipeline] run_events DQ lookup failed: %s", exc)

        result = sorted(
            merged.values(),
            key=lambda x: x.get("created_at") or "",
            reverse=True,
        )

        return {
            "success": True,
            "message": "Jobs fetched successfully",
            "data": result,
        }
    except HTTPException:
        raise
    except Exception as exc:
        return server_error(exc, action="listing pipeline jobs")


@router.get(
    "/outputs",
    summary="List CDM output tables",
    description="List all CSV files in the CDM output directory, grouped by stage. "
    "``plant_code_id`` scopes to that plant's cdm_out subtree (required "
    "now that output is plant-segregated).",
)
def listOutputs(
    plant_code_id: str = Query(..., description="Plant code ID for the request"),
):

    if not plant_code_id or not plant_code_id.strip():
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

    try:
        _vp(plant_code_id)
        base = _out(plant_code_id)
        result: dict[str, list[str]] = {}
        for stage in [
            "sap",
            "pnid",
            "ts",
            "docs",
            "aif",
            "gloc",
            "lopc",
            "other_files",
            "final",
            "alerts",
        ]:
            entity_dir = f"{base}/{stage}/entities"
            stems: set[str] = set()
            for pat in ("*.parquet", "*.csv"):
                try:
                    stems |= {Path(p).stem for p in _fs.glob_files(f"{entity_dir}/{pat}")}
                except Exception as exc:
                    _log.warning("[pipeline] output stem glob failed: %s", exc)
            result[stage] = sorted(stems)
        rel_dir = f"{base}/final/relationships"
        rel_stems: set[str] = set()
        for pat in ("*.parquet", "*.csv"):
            try:
                rel_stems |= {Path(p).stem for p in _fs.glob_files(f"{rel_dir}/{pat}")}
            except Exception as exc:
                _log.warning("[pipeline] relationship stem glob failed: %s", exc)
        result["final_relationships"] = sorted(rel_stems)

        return {
            "success": True,
            "message": "Outputs fetched successfully",
            "data": result,
        }
    except HTTPException:
        raise
    except Exception as exc:
        return server_error(exc, action="listing outputs")


@router.get(
    "/outputs/{stage}/{table}",
    summary="Preview CDM output table",
    description="Return the first N rows of a CDM output CSV as JSON. "
    "Use `limit` and `offset` for pagination.",
)
def previewOutput(
    stage: str,
    table: str,
    limit: int = 50,
    offset: int = 0,
    plant_code_id: str = Query(..., description="Plant code ID for the request"),
):

    errors = []
    if not plant_code_id or not plant_code_id.strip():
        errors.append(
            {
                "field": "plant_code_id",
                "message": "plant_code_id is required",
            }
        )

    if not stage or not stage.strip():
        errors.append(
            {
                "field": "stage",
                "message": "stage is required",
            }
        )
    else:
        allowed_stages = [
            "sap",
            "pnid",
            "ts",
            "docs",
            "aif",
            "gloc",
            "lopc",
            "other_files",
            "final",
            "alerts",
        ]
        if stage.strip().lower() not in allowed_stages:
            errors.append(
                {
                    "field": "stage",
                    "message": f"Invalid stage. Must be one of: {', '.join(allowed_stages)}",
                }
            )

    if not table or not table.strip():
        errors.append(
            {
                "field": "table",
                "message": "table is required",
            }
        )
    elif not table.replace("_", "").isalnum():
        errors.append(
            {
                "field": "table",
                "message": "Invalid table name",
            }
        )

    if limit < 1 or limit > 1000:
        errors.append(
            {
                "field": "limit",
                "message": "limit must be between 1 and 1000",
            }
        )

    if offset < 0:
        errors.append(
            {
                "field": "offset",
                "message": "offset must be greater than or equal to 0",
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

    try:
        _vp(plant_code_id)
        base = _out(plant_code_id)

        file_path: str | None = None
        suffix = ""
        for subdir in ("entities", "relationships"):
            for ext in (".parquet", ".csv"):
                candidate = f"{base}/{stage}/{subdir}/{table}{ext}"
                if _fs.exists(candidate):
                    file_path, suffix = candidate, ext
                    break
            if file_path:
                break
        if file_path is None:
            return JSONResponse(
                status_code=HTTPStatus.NOT_FOUND,
                content={
                    "success": False,
                    "message": "Table not found",
                    "errors": [
                        {
                            "field": "table",
                            "message": f"Table '{table}' not found in {stage} output",
                        }
                    ],
                },
            )

        try:
            if suffix == ".parquet":
                df = _fs.read_parquet(file_path)
                total_rows = len(df)
                df = df.iloc[offset : offset + limit]
            else:
                df = _fs.read_csv(file_path, nrows=offset + limit)
                total_rows = sum(
                    len(chunk)
                    for chunk in _fs.read_csv_chunked(file_path, chunksize=50_000)
                )
                df = df.iloc[offset : offset + limit]

            return {
                "success": True,
                "message": "Table preview fetched successfully",
                "data": {
                    "stage": stage,
                    "table": table,
                    "total_rows": total_rows,
                    "offset": offset,
                    "limit": limit,
                    "columns": list(df.columns),
                    "rows": df.fillna("").to_dict(orient="records"),
                },
            }
        except Exception as e:
            return JSONResponse(
                status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
                content={
                    "success": False,
                    "message": "Error reading table",
                    "errors": [
                        {
                            "field": "server",
                            "message": f"Error reading table: {e}",
                        }
                    ],
                },
            )
    except HTTPException:
        raise
    except Exception as exc:
        return server_error(exc, action="previewing output")

def _job_doc_types(job: dict) -> list[str]:
    """Document types one docs upload job covers, from the plural or the singular key."""
    raw = job.get("document_types")
    if isinstance(raw, str):
        raw = [raw]
    if not raw:
        raw = [job.get("document_type")]
    return sorted({str(t).strip() for t in (raw or []) if str(t or "").strip()})


def _derive_docs_scope_from_uploads(
    plant_code_id: str,
) -> tuple[list[str], list[str]]:
    """Derive docs run scope from the plant's latest document upload jobs."""

    jobs = sorted(
        (
            j
            for j in _upload_jobs.values()
            if j.get("data_type") == "documents"
            and j.get("plant_code_id") == plant_code_id
        ),
        key=lambda j: j.get("created", ""),
        reverse=True,
    )

    latest_files_by_type: dict[str, list[str]] = {}

    for j in jobs:
        job_types = _job_doc_types(j)

        if not job_types:
            continue

        by_type: dict[str, list[str]] = {
            t: []
            for t in job_types
        }

        for f in j.get("files", []):
            name = str(
                f.get("name", "")
            ).strip()

            if (
                f.get("status") != "uploaded"
                or not name
            ):
                continue

            ft = str(
                f.get("document_type") or ""
            ).strip()

            for t in (
                [ft]
                if ft in by_type
                else job_types
            ):
                by_type[t].append(
                    name
                )

        for dt, names in by_type.items():
            if dt not in latest_files_by_type:
                latest_files_by_type[
                    dt
                ] = names

    doc_types = sorted(
        latest_files_by_type.keys()
    )

    doc_files = sorted(
        {
            f
            for fs in latest_files_by_type.values()
            for f in fs
        }
    )

    return doc_types, doc_files

def reconcile_interrupted_jobs(plant_code_id: str) -> int:
    """Mark jobs whose process died with the API as interrupted, not eternally running."""
    from ..services.audit import list_jobs

    repaired = 0
    try:
        for job in list_jobs(plant_code_id=plant_code_id, limit=200):
            job_id = job.get("pipeline_job_id")
            if job.get("status") not in ("queued", "running"):
                continue
            if job_id in _pipeline_jobs:
                continue
            if _pid_is_alive(job.get("pid")):
                continue
            job["status"] = "interrupted"
            job["error"] = (
                "The pipeline was interrupted when the service restarted. "
                "Retry the run to continue."
            )
            job["completed_at"] = datetime.now(timezone.utc).isoformat()
            try:
                upsert_job(job_id, job)
                repaired += 1
            except Exception as exc:
                _log.warning("[pipeline] could not reconcile job %s: %s", job_id, exc)
    except Exception as exc:
        _log.warning("[pipeline] job reconciliation skipped: %s", exc)
    return repaired


@router.post(
    "/jobs/{jobId}/retry",
    status_code=HTTPStatus.ACCEPTED,
    summary="Retry a finished pipeline job",
    description="Re-run a failed, partial, cancelled or interrupted job with the same "
    "stage and plant. `scope=failed_only` re-runs just the files that did not succeed; "
    "`scope=all` re-runs everything the original job covered.",
)
def retryPipelineJob(
    jobId: str,
    plant_code_id: str = Query(..., description="Plant code ID for the request"),
    scope: str = Query("failed_only", description="failed_only | all"),
):
    errors = []
    plant_code_id_val = (plant_code_id or "").strip()
    if not plant_code_id_val:
        errors.append({"field": "plant_code_id", "message": "plant_code_id is required"})
    job_id_val = (jobId or "").strip()
    if not job_id_val:
        errors.append({"field": "jobId", "message": "jobId is required"})
    scope_val = (scope or "failed_only").strip().lower()
    if scope_val not in ("failed_only", "all"):
        errors.append(
            {"field": "scope", "message": "scope must be either 'failed_only' or 'all'"}
        )
    if errors:
        return JSONResponse(
            status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
            content={"success": False, "message": "Validation failed", "errors": errors},
        )

    try:
        _vp(plant_code_id_val)
        job = _pipeline_jobs.get(job_id_val) or get_job(job_id_val, plant_code_id_val)
        if job is None:
            return JSONResponse(
                status_code=HTTPStatus.NOT_FOUND,
                content={
                    "success": False,
                    "message": f"No pipeline job exists with id '{job_id_val}'.",
                    "errors": [
                        {"field": "jobId", "message": f"Job '{job_id_val}' was not found."}
                    ],
                },
            )

        status = job.get("status")
        if status in ("queued", "running"):
            return JSONResponse(
                status_code=HTTPStatus.CONFLICT,
                content={
                    "success": False,
                    "message": (
                        f"Job '{job_id_val}' is still {status}. "
                        "Cancel it before retrying."
                    ),
                    "errors": [
                        {"field": "jobId", "message": f"The job is currently {status}."}
                    ],
                },
            )

        stage = job.get("stage")
        outcomes = job.get("file_outcomes") or {}
        retry_files = None
        if scope_val == "failed_only" and outcomes:
            retry_files = [
                name
                for name, outcome in outcomes.items()
                if outcome.get("status") in _progress.FAILED_FILE_STATUSES
            ]
            if not retry_files:
                return JSONResponse(
                    status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
                    content={
                        "success": False,
                        "message": "There are no failed files to retry in this job.",
                        "errors": [
                            {
                                "field": "scope",
                                "message": "Use scope=all to re-run the whole job.",
                            }
                        ],
                    },
                )

        new_job_id = str(uuid.uuid4())
        industry = job.get("industry") or _resolve_industry(None, plant_code_id_val)
        original_files = list(job.get("file_names") or [])
        rerun_files = retry_files if retry_files is not None else (original_files or None)
        _pipeline_jobs[new_job_id] = {
            "stage": stage,
            "plant_code_id": plant_code_id_val,
            "industry": industry,
            "upload_batch_id": job.get("upload_batch_id"),
            "file_names": list(rerun_files or []),
            "doc_types": list(job.get("doc_types") or []),
            "doc_files": list(job.get("doc_files") or []),
            "other_type": job.get("other_type"),
            "equipment_column": job.get("equipment_column"),
            "extraction_fields": list(job.get("extraction_fields") or []),
            "status": "queued",
            "created_by": get_actor(),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "started_at": None,
            "completed_at": None,
            "error": None,
            "output_tables": None,
            "retry_of": job_id_val,
            "retry_scope": scope_val,
            "stages_planned": _progress.plan_for(stage),
            "files_total": len(rerun_files or []) or _pending_file_count(
                plant_code_id_val, stage, job.get("upload_batch_id")
            ),
            "phases": {},
            "progress_percent": 0,
            "is_determinate": _progress.is_determinate(stage),
            "current_phase": None,
            "current_label": None,
            "current_item": None,
        }
        _persist_job(new_job_id)

        spawn_with_context(
            target=_run_pipeline_bg,
            args=(
                new_job_id,
                stage,
                plant_code_id_val,
                industry,
                rerun_files,
                job.get("doc_types") or None,
                job.get("doc_files") or None,
                job.get("upload_batch_id"),
                job.get("other_type"),
                job.get("equipment_column"),
                job.get("extraction_fields") or None,
            ),
            daemon=True,
        )

        return {
            "success": True,
            "message": (
                f"Retrying {len(retry_files)} failed file(s)."
                if retry_files
                else "Retrying the full job."
            ),
            "data": {
                "pipeline_job_id": new_job_id,
                "retry_of": job_id_val,
                "scope": scope_val,
                "stage": stage,
                "plant_code_id": plant_code_id_val,
                "files": rerun_files or [],
            },
        }
    except HTTPException as e:
        raise as_envelope_exc(e)
    except Exception as exc:
        return server_error(
            exc, action="retrying the pipeline job", plant_code_id=plant_code_id_val
        )