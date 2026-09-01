"""
Pipeline log file endpoints.

Serves the last N lines of real pipeline log files so the frontend
can display actual extraction progress and errors instead of mock data.
"""

from __future__ import annotations

import logging
import re
import sys
from datetime import datetime, timezone
from http import HTTPStatus
from pathlib import Path

from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import JSONResponse

from ..services import audit as _audit
from ..responses import EnvelopeRoute, server_error

_REPO_ROOT = Path(__file__).resolve().parents[4]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

if __package__:
    from ..config import LOG_DIR
else:
    from p0.api.config import LOG_DIR

router = APIRouter(route_class=EnvelopeRoute, prefix="/logs", tags=["Logs"])

_log = logging.getLogger("cdm.api.logs")

_ALLOWED_LOGS: dict[str, str] = {
    "api": "api.log",
    "pnid": "pnid.log",
    "pnid_pipeline": "pnid_pipeline.log",
    "docs": "docs.log",
    "docs_pipeline": "docs_pipeline.log",
    "sap": "sap.log",
    "sap_pipeline": "sap_pipeline.log",
    "timeseries": "timeseries.log",
    "timeseries_pipeline": "timeseries_pipeline.log",
    "connectors": "connectors.log",
}


@router.get(
    "",
    summary="List available pipeline log files",
    description="Returns all log file names that can be fetched, along with their size and "
    "whether they currently exist on disk.",
)
def listLogs(
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
        result = []
        for key, filename in _ALLOWED_LOGS.items():
            path = LOG_DIR / filename
            result.append(
                {
                    "key": key,
                    "filename": filename,
                    "exists": path.exists(),
                    "size_bytes": path.stat().st_size if path.exists() else 0,
                }
            )

        return {
            "success": True,
            "message": "Logs fetched successfully",
            "data": {"logs": result},
        }
    except HTTPException:
        raise
    except Exception as exc:
        return server_error(exc, action="listing logs")


_LEVELS = ("CRITICAL", "ERROR", "WARNING", "WARN", "INFO", "DEBUG")
_LINE_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?)\s*"
    r"\[?(?P<level>CRITICAL|ERROR|WARNING|WARN|INFO|DEBUG)\]?\s+"
    r"(?P<rest>.*)$"
)


def _parse_log_line(line: str, source: str) -> dict | None:
    m = _LINE_RE.match(line.strip())
    if not m:
        return None
    level = m.group("level").upper()
    if level == "WARN":
        level = "WARNING"
    return {
        "ts": m.group("ts").replace(" ", "T"),
        "level": level,
        "source": source,
        "message": m.group("rest").strip(),
    }


def _iter_parsed_events(sources, level=None, q=None, per_file=2000):
    """Yield parsed events from the given source log files, newest-first."""
    events: list[dict] = []
    want_level = level.upper() if level and level.upper() != "ALL" else None
    ql = q.lower() if q else None
    for key in sources:
        filename = _ALLOWED_LOGS.get(key)
        if not filename:
            continue
        path = LOG_DIR / filename
        if not path.exists():
            continue
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as fh:
                lines = fh.readlines()[-per_file:]
        except Exception:
            continue
        for ln in lines:
            ev = _parse_log_line(ln, key)
            if not ev:
                continue
            if want_level and ev["level"] != want_level:
                continue
            if ql and ql not in ev["message"].lower():
                continue
            events.append(ev)
    events.sort(key=lambda e: e["ts"], reverse=True)
    return events


def _build_histogram(events, buckets=48):
    """Bucket events over their time span, counting by level per bucket — drives"""
    if not events:
        return []
    ts = [e["ts"] for e in events if e.get("ts")]
    if not ts:
        return []
    lo, hi = min(ts), max(ts)

    def _ep(s):
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
        except Exception:
            return None

    lo_e, hi_e = _ep(lo), _ep(hi)
    if lo_e is None or hi_e is None:
        return []
    span = max(hi_e - lo_e, 1.0)
    width = span / buckets
    grid = [
        {"t": lo_e + i * width, "ERROR": 0, "WARNING": 0, "INFO": 0, "OTHER": 0}
        for i in range(buckets)
    ]
    for e in events:
        ep = _ep(e.get("ts", ""))
        if ep is None:
            continue
        idx = min(int((ep - lo_e) / width), buckets - 1)
        lvl = e.get("level")
        key = (
            lvl
            if lvl in ("ERROR", "WARNING", "INFO")
            else ("ERROR" if lvl == "CRITICAL" else "OTHER")
        )
        grid[idx][key] += 1
    return grid


@router.get("/events", summary="Parsed, filterable log events (admin)")
def listEvents(
    source: str | None = Query(
        None, description="Comma-separated log keys; default all"
    ),
    level: str | None = Query(
        None, description="CRITICAL|ERROR|WARNING|INFO|DEBUG|ALL"
    ),
    q: str | None = Query(None, description="Substring match on the message"),
    limit: int = Query(500, ge=1, le=5000),
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

    if level:
        allowed_levels = ["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "ALL"]
        if level.upper() not in allowed_levels:
            errors.append(
                {
                    "field": "level",
                    "message": f"Invalid level. Must be one of: {', '.join(allowed_levels)}",
                }
            )

    if limit < 1 or limit > 5000:
        errors.append(
            {
                "field": "limit",
                "message": "limit must be between 1 and 5000",
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
        """Server-parsed log events so the UI renders structured rows (ts, level,
        source, message) instead of regex-parsing raw text on the client. Also
        returns a time histogram + facet counts over the full filtered set so the
        chart and sidebar reflect everything, not just the returned page."""
        keys = (
            [s.strip() for s in source.split(",") if s.strip()]
            if source
            else list(_ALLOWED_LOGS)
        )
        events = _iter_parsed_events(keys, level=level, q=q)
        by_level = {"CRITICAL": 0, "ERROR": 0, "WARNING": 0, "INFO": 0, "DEBUG": 0}
        by_source: dict[str, int] = {}
        for e in events:
            if e["level"] in by_level:
                by_level[e["level"]] += 1
            by_source[e["source"]] = by_source.get(e["source"], 0) + 1

        return {
            "success": True,
            "message": "Events fetched successfully",
            "data": {
                "events": events[:limit],
                "total": len(events),
                "sources": list(_ALLOWED_LOGS),
                "histogram": _build_histogram(events),
                "facets": {"level": by_level, "source": by_source},
            },
        }
    except HTTPException:
        raise
    except Exception as exc:
        return server_error(exc, action="listing events")


@router.get("/activity", summary="Who-did-what activity feed (admin)")
def listActivity(
    limit: int = Query(100, ge=1, le=500),
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

    if plant_code_id and not plant_code_id.strip():
        errors.append(
            {
                "field": "plant_code_id",
                "message": "plant_code_id cannot be empty if provided",
            }
        )

    if limit < 1 or limit > 500:
        errors.append(
            {
                "field": "limit",
                "message": "limit must be between 1 and 500",
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
        """Unified activity timeline from the durable audit tables: commits
        (commit_audit) + pipeline runs (pipeline_jobs) + generic actions
        (activity_events) + file lifecycle milestones (run_events: upload/reset),
        newest-first. This is the admin 'system awareness / who did what' surface."""
        items: list[dict] = []
        try:
            for c in _audit.list_commits(plant_code_id=plant_code_id, limit=limit):
                items.append(
                    {
                        "ts": c.get("committed_at"),
                        "actor": c.get("committed_by") or "system",
                        "action": "commit",
                        "source": c.get("source"),
                        "plant_code_id": c.get("plant_code_id"),
                        "status": c.get("status"),
                        "rows": c.get("rows_written"),
                        "detail": c.get("detail"),
                        "ref": f"commit:{c.get('commit_uid')}",
                    }
                )
        except Exception as exc:
            _log.warning("[logs/activity] commits read failed: %s", exc)
        try:
            for j in _audit.list_jobs(plant_code_id=plant_code_id, limit=limit):
                items.append(
                    {
                        "ts": j.get("completed_at")
                        or j.get("started_at")
                        or j.get("created_at"),
                        "actor": j.get("created_by") or "system",
                        "action": f"pipeline:{j.get('stage')}",
                        "source": j.get("stage"),
                        "plant_code_id": j.get("plant_code_id"),
                        "status": j.get("status"),
                        "rows": j.get("total_records"),
                        "detail": j.get("warning") or j.get("error"),
                        "ref": f"job:{j.get('pipeline_job_id')}",
                    }
                )
        except Exception as exc:
            _log.warning("[logs/activity] jobs read failed: %s", exc)
        try:
            for e in _audit.list_events(plant_code_id=plant_code_id, limit=limit):
                items.append(
                    {
                        "ts": e.get("ts"),
                        "actor": e.get("actor") or "system",
                        "action": e.get("action"),
                        "source": e.get("source"),
                        "plant_code_id": e.get("plant_code_id"),
                        "status": e.get("status"),
                        "rows": None,
                        "detail": e.get("detail") or e.get("target"),
                        "ref": f"event:{e.get('event_uid')}",
                    }
                )
        except Exception as exc:
            _log.warning("[logs/activity] events read failed: %s", exc)
        try:
            _MILESTONES = {"uploaded", "staged", "reset"}
            for e in _audit.list_run_events(plant_code_id=plant_code_id, limit=limit):
                ev = e.get("event")
                is_values_ingest = ev == "committed" and e.get("flow") == "ts_values"
                if ev not in _MILESTONES and not is_values_ingest:
                    continue
                if is_values_ingest:
                    label = "ingest"
                else:
                    label = "upload" if ev in ("uploaded", "staged") else "reprocess"
                det = e.get("detail") if isinstance(e.get("detail"), dict) else {}
                items.append(
                    {
                        "ts": e.get("ts"),
                        "actor": e.get("actor") or "system",
                        "action": label,
                        "source": e.get("flow"),
                        "plant_code_id": e.get("plant_code_id"),
                        "status": e.get("status"),
                        "rows": e.get("rows_out"),
                        "rows_in": e.get("rows_in"),
                        "rows_rejected": e.get("rows_rejected"),
                        "dq": det.get("rejects")
                        or None,
                        "detail": e.get("file_name"),
                        "ref": f"runevent:{e.get('event_uid')}",
                    }
                )
        except Exception as exc:
            _log.warning("[logs/activity] run_events read failed: %s", exc)
        items.sort(key=lambda x: (x.get("ts") or ""), reverse=True)

        return {
            "success": True,
            "message": "Activity fetched successfully",
            "data": {"activity": items[:limit], "total": len(items)},
        }
    except HTTPException:
        raise
    except Exception as exc:
        return server_error(exc, action="listing activity")


@router.get("/summary", summary="Observability summary counts (admin)")
def getSummary(
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

    if plant_code_id and not plant_code_id.strip():
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
        """Pre-aggregated counts for the admin summary strip: log events by level
        (from today's lines), and recent job/commit outcomes. FE just renders
        numbers — no client-side aggregation."""

        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        by_level = {"CRITICAL": 0, "ERROR": 0, "WARNING": 0, "INFO": 0, "DEBUG": 0}
        for ev in _iter_parsed_events(list(_ALLOWED_LOGS), per_file=2000):
            if ev["ts"][:10] == today and ev["level"] in by_level:
                by_level[ev["level"]] += 1
        jobs = []
        commits = []
        try:
            jobs = _audit.list_jobs(plant_code_id=plant_code_id, limit=200)
        except Exception as exc:
            _log.warning("[logs/summary] jobs read failed: %s", exc)

        try:
            commits = _audit.list_commits(plant_code_id=plant_code_id, limit=200)
        except Exception as exc:
            _log.warning("[logs/summary] commits read failed: %s", exc)
        jobs_failed = sum(1 for j in jobs if j.get("status") == "failed")
        jobs_done = sum(1 for j in jobs if j.get("status") == "completed")
        commits_today = sum(
            1 for c in commits if str(c.get("committed_at") or "")[:10] == today
        )

        return {
            "success": True,
            "message": "Summary fetched successfully",
            "data": {
                "today": today,
                "events_by_level": by_level,
                "jobs": {
                    "completed": jobs_done,
                    "failed": jobs_failed,
                    "total": len(jobs),
                },
                "commits": {"today": commits_today, "total": len(commits)},
            },
        }
    except HTTPException:
        raise
    except Exception as exc:
        return server_error(exc, action="loading summary")


@router.get(
    "/{logKey}",
    summary="Get pipeline log content (raw tail)",
    description="Return the last `lines` lines of a pipeline log file. "
    "Valid keys: api, pnid, docs, sap, timeseries, connectors.",
)
def getLog(
    logKey: str,
    lines: int = Query(default=500, ge=1, le=5000),
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

    if not logKey or not logKey.strip():
        errors.append(
            {
                "field": "logKey",
                "message": "logKey is required",
            }
        )

    if lines < 1 or lines > 5000:
        errors.append(
            {
                "field": "lines",
                "message": "lines must be between 1 and 5000",
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
        filename = _ALLOWED_LOGS.get(logKey)
        if not filename:
            return JSONResponse(
                status_code=HTTPStatus.BAD_REQUEST,
                content={
                    "success": False,
                    "message": "Invalid log key",
                    "errors": [
                        {
                            "field": "logKey",
                            "message": f"Invalid log key '{logKey}'. Valid keys: {list(_ALLOWED_LOGS)}",
                        }
                    ],
                },
            )

        log_path = LOG_DIR / filename
        if not log_path.exists():
            return {
                "success": True,
                "message": "Log file not yet created — run a pipeline first.",
                "data": {
                    "log": logKey,
                    "filename": filename,
                    "lines": [],
                    "total_lines": 0,
                },
            }

        with open(log_path, "r", encoding="utf-8", errors="replace") as fh:
            all_lines = fh.readlines()
        tail = all_lines[-lines:]

        return {
            "success": True,
            "message": "Log fetched successfully",
            "data": {
                "log": logKey,
                "filename": filename,
                "lines": [ln.rstrip("\n") for ln in tail],
                "total_lines": len(all_lines),
                "showing": len(tail),
            },
        }
    except HTTPException:
        raise
    except Exception as exc:
        _log.error("[logs] Failed to read %s: %s", filename, exc)
        return JSONResponse(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            content={
                "success": False,
                "message": "Internal server error",
                "errors": [
                    {
                        "field": "server",
                        "message": f"Could not read log file: {exc}",
                    }
                ],
            },
        )
