"""Best-effort audit ledgers: pipeline jobs, commits, run events and activity."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from ..audit_context import get_actor
from ..database.connection import _get_conn

_log = logging.getLogger("p0.audit")


def _audit_write(do_write, plant_code_id: str | None = None, registry: bool = False) -> bool:
    """Run an audit write on the plant's (or registry's) connection; best-effort."""
    conn = None
    try:
        conn = _get_conn(plant_code_id) if not registry else _get_conn(registry=True)
        cur = conn.cursor()
        do_write(cur)
        conn.commit()
        return True
    except Exception as exc:
        try:
            if conn is not None:
                conn.rollback()
        except Exception:
            pass
        _log.warning("[audit] write failed: %s", exc)
        return False
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass



_JOB_COLS = [
    "pipeline_job_id",
    "stage",
    "plant_code_id",
    "industry",
    "status",
    "pid",
    "total_records",
    "record_counts",
    "output_tables",
    "warning",
    "error",
    "created_by",
    "created_at",
    "started_at",
    "completed_at",
    "upload_batch_id",
    "progress_percent",
    "is_determinate",
    "current_phase",
    "current_label",
    "current_item",
    "items_done",
    "items_total",
    "stages_planned",
    "phases",
]
_JSON_JOB_COLS = {"record_counts", "output_tables", "warning", "stages_planned", "phases"}


def _coerce_ts(v):
    """Pass ISO strings / datetimes through; None stays None."""
    return v


def _iso_utc(dt: datetime) -> str:
    """Serialize a datetime to ISO 8601 with an explicit zone."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()


def upsert_job(pipeline_job_id: str, job: dict) -> None:
    """Mirror an in-memory job dict to pipeline_jobs (insert or update by"""
    try:
        row = {"pipeline_job_id": pipeline_job_id}
        for c in _JOB_COLS:
            if c == "pipeline_job_id":
                continue
            val = job.get(c)
            if c == "created_by" and not val:
                try:
                    val = get_actor()
                except Exception:
                    val = None
            if c in _JSON_JOB_COLS and val is not None and not isinstance(val, str):
                val = json.dumps(val)
            row[c] = val
        cols = [c for c in _JOB_COLS]
        placeholders = ", ".join(["%s"] * len(cols))
        col_names = ", ".join(f'"{c}"' for c in cols)
        update_set = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in cols if c != "pipeline_job_id")
        sql = (
            f"INSERT INTO pipeline_jobs ({col_names}) VALUES ({placeholders}) "
            f"ON CONFLICT (pipeline_job_id) DO UPDATE SET {update_set}"
        )
        values = [row.get(c) for c in cols]
        _audit_write(lambda cur: cur.execute(sql, values), job.get("plant_code_id"))
    except Exception as exc:
        _log.warning("[audit] upsert_job(%s) failed: %s", pipeline_job_id, exc)


def get_job(pipeline_job_id: str, plant_code_id: str | None = None) -> dict | None:
    """Read one job back from the DB (used when the in-memory cache misses,"""
    try:
        conn = _get_conn(plant_code_id)
        try:
            cur = conn.cursor()
            col_names = ", ".join(f'"{c}"' for c in _JOB_COLS)
            cur.execute(
                f"SELECT {col_names} FROM pipeline_jobs WHERE pipeline_job_id = %s", (pipeline_job_id,)
            )
            r = cur.fetchone()
            if not r:
                return None
            out = {}
            for c, v in zip(_JOB_COLS, r):
                if c in _JSON_JOB_COLS and isinstance(v, str):
                    try:
                        v = json.loads(v)
                    except Exception:
                        pass
                if isinstance(v, datetime):
                    v = _iso_utc(v)
                out[c] = v
            return out
        finally:
            conn.close()
    except Exception as exc:
        _log.warning("[audit] get_job(%s) failed: %s", pipeline_job_id, exc)
        return None


def list_jobs(
    stage: str | None = None, plant_code_id: str | None = None, limit: int = 50
) -> list[dict]:
    """List recent jobs from the DB, newest first. Best-effort."""
    try:
        conn = _get_conn(plant_code_id)
        try:
            cur = conn.cursor()
            col_names = ", ".join(f'"{c}"' for c in _JOB_COLS)
            where, params = [], []
            if stage:
                where.append("stage = %s")
                params.append(stage)
            if plant_code_id:
                where.append("plant_code_id = %s")
                params.append(plant_code_id)
            wsql = (" WHERE " + " AND ".join(where)) if where else ""
            params.append(limit)
            cur.execute(
                f"SELECT {col_names} FROM pipeline_jobs{wsql} "
                f"ORDER BY created_at DESC LIMIT %s",
                params,
            )
            rows = []
            for r in cur.fetchall():
                out = {}
                for c, v in zip(_JOB_COLS, r):
                    if c in _JSON_JOB_COLS and isinstance(v, str):
                        try:
                            v = json.loads(v)
                        except Exception:
                            pass
                    if isinstance(v, datetime):
                        v = _iso_utc(v)
                    out[c] = v
                rows.append(out)
            return rows
        finally:
            conn.close()
    except Exception as exc:
        _log.warning("[audit] list_jobs failed: %s", exc)
        return []




def record_commit(
    source: str,
    plant_code_id: str | None,
    status: str,
    rows_written: int,
    tables,
    detail=None,
    committed_by: str | None = None,
) -> int | None:
    """Insert one commit_audit row. Best-effort — never raises, so an audit"""
    captured: dict = {}

    def _do(cur):
        cur.execute(
            "INSERT INTO commit_audit "
            "(source, plant_code_id, status, rows_written, tables, detail, committed_by, committed_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING commit_uid",
            (
                source,
                plant_code_id,
                status,
                int(rows_written or 0),
                json.dumps(tables) if tables is not None else None,
                json.dumps(detail) if detail is not None else None,
                committed_by,
                datetime.now(timezone.utc),
            ),
        )
        r = cur.fetchone()
        captured["uid"] = int(r[0]) if r else None

    if _audit_write(_do, plant_code_id):
        return captured.get("uid")
    return None


def list_commits(
    plant_code_id: str | None = None, source: str | None = None, limit: int = 50
) -> list[dict]:
    """List recent commit-audit rows, newest first. Best-effort."""
    cols = [
        "commit_uid",
        "source",
        "plant_code_id",
        "status",
        "rows_written",
        "tables",
        "detail",
        "committed_by",
        "committed_at",
    ]
    json_cols = {"tables", "detail"}
    try:
        conn = _get_conn(plant_code_id)
        try:
            cur = conn.cursor()
            where, params = [], []
            if plant_code_id:
                where.append("plant_code_id = %s")
                params.append(plant_code_id)
            if source:
                where.append("source = %s")
                params.append(source)
            wsql = (" WHERE " + " AND ".join(where)) if where else ""
            params.append(limit)
            col_names = ", ".join(f'"{c}"' for c in cols)
            cur.execute(
                f"SELECT {col_names} FROM commit_audit{wsql} "
                f"ORDER BY committed_at DESC LIMIT %s",
                params,
            )
            rows = []
            for r in cur.fetchall():
                out = {}
                for c, v in zip(cols, r):
                    if c in json_cols and isinstance(v, str):
                        try:
                            v = json.loads(v)
                        except Exception:
                            pass
                    if isinstance(v, datetime):
                        v = _iso_utc(v)
                    out[c] = v
                rows.append(out)
            return rows
        finally:
            conn.close()
    except Exception as exc:
        _log.warning("[audit] list_commits failed: %s", exc)
        return []




def record_event(
    action: str,
    *,
    actor: str | None = None,
    target_type: str | None = None,
    target: str | None = None,
    plant_code_id: str | None = None,
    source: str | None = None,
    status: str = "ok",
    detail=None,
) -> None:
    """Record one discrete platform action (upload, delete, plant create/purge,"""
    if not actor:
        try:
            actor = get_actor()
        except Exception:
            actor = None
    _audit_write(
        lambda cur: cur.execute(
            "INSERT INTO activity_events "
            "(ts, actor, action, target_type, target, plant_code_id, source, status, detail) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                datetime.now(timezone.utc),
                actor or "system",
                action,
                target_type,
                (str(target)[:255] if target is not None else None),
                plant_code_id,
                source,
                status,
                json.dumps(detail) if detail is not None else None,
            ),
        ),
        plant_code_id,
    )


def list_events(
    plant_code_id: str | None = None, action: str | None = None, limit: int = 100
) -> list[dict]:
    """List recent activity events, newest first. Best-effort."""
    cols = [
        "event_uid",
        "ts",
        "actor",
        "action",
        "target_type",
        "target",
        "plant_code_id",
        "source",
        "status",
        "detail",
    ]
    try:
        conn = _get_conn(plant_code_id)
        try:
            cur = conn.cursor()
            where, params = [], []
            if plant_code_id:
                where.append("plant_code_id = %s")
                params.append(plant_code_id)
            if action:
                where.append("action = %s")
                params.append(action)
            wsql = (" WHERE " + " AND ".join(where)) if where else ""
            params.append(limit)
            col_names = ", ".join(f'"{c}"' for c in cols)
            cur.execute(
                f"SELECT {col_names} FROM activity_events{wsql} "
                f"ORDER BY ts DESC LIMIT %s",
                params,
            )
            rows = []
            for r in cur.fetchall():
                out = {}
                for c, v in zip(cols, r):
                    if c == "detail" and isinstance(v, str):
                        try:
                            v = json.loads(v)
                        except Exception:
                            pass
                    if isinstance(v, datetime):
                        v = _iso_utc(v)
                    out[c] = v
                rows.append(out)
            return rows
        finally:
            conn.close()
    except Exception as exc:
        _log.warning("[audit] list_events failed: %s", exc)
        return []




def append_event(
    plant_code_id: str,
    flow: str,
    event: str,
    *,
    file_name: str | None = None,
    content_hash: str | None = None,
    flow_uid: int | None = None,
    upload_batch_id: str | None = None,
    commit_uid: int | None = None,
    actor: str | None = None,
    rows_in: int | None = None,
    rows_out: int | None = None,
    rows_rejected: int | None = None,
    status: str = "ok",
    detail=None,
) -> None:
    """Append ONE immutable lifecycle event to run_events. This is the permanent"""
    if not actor:
        try:
            actor = get_actor()
        except Exception:
            actor = None
    _audit_write(
        lambda cur: cur.execute(
            "INSERT INTO run_events "
            "(ts, plant_code_id, flow, event, file_name, content_hash, flow_uid, "
            " upload_batch_id, commit_uid, actor, rows_in, rows_out, rows_rejected, "
            " status, detail) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                datetime.now(timezone.utc),
                plant_code_id,
                flow,
                event,
                (str(file_name)[:512] if file_name is not None else None),
                content_hash,
                flow_uid,
                upload_batch_id,
                commit_uid,
                actor or "system",
                rows_in,
                rows_out,
                rows_rejected,
                status,
                json.dumps(detail) if detail is not None else None,
            ),
        ),
        plant_code_id,
    )


def list_run_events(
    plant_code_id: str | None = None,
    flow: str | None = None,
    content_hash: str | None = None,
    file_name: str | None = None,
    limit: int = 200,
) -> list[dict]:
    """List lineage events, newest first. Filter by plant/flow/content_hash (the"""
    cols = [
        "event_uid",
        "ts",
        "plant_code_id",
        "flow",
        "event",
        "file_name",
        "content_hash",
        "flow_uid",
        "upload_batch_id",
        "commit_uid",
        "actor",
        "rows_in",
        "rows_out",
        "rows_rejected",
        "status",
        "detail",
    ]
    try:
        conn = _get_conn(plant_code_id)
        try:
            cur = conn.cursor()
            where, params = [], []
            if plant_code_id:
                where.append("plant_code_id = %s")
                params.append(plant_code_id)
            if flow:
                where.append("flow = %s")
                params.append(flow)
            if content_hash:
                where.append("content_hash = %s")
                params.append(content_hash)
            if file_name:
                where.append("file_name = %s")
                params.append(file_name)
            wsql = (" WHERE " + " AND ".join(where)) if where else ""
            params.append(limit)
            col_names = ", ".join(f'"{c}"' for c in cols)
            cur.execute(
                f"SELECT {col_names} FROM run_events{wsql} "
                f"ORDER BY ts DESC, event_uid DESC LIMIT %s",
                params,
            )
            rows = []
            for r in cur.fetchall():
                out = {}
                for c, v in zip(cols, r):
                    if c == "detail" and isinstance(v, str):
                        try:
                            v = json.loads(v)
                        except Exception:
                            pass
                    if isinstance(v, datetime):
                        v = _iso_utc(v)
                    out[c] = v
                rows.append(out)
            return rows
        finally:
            conn.close()
    except Exception as exc:
        _log.warning("[audit] list_run_events failed: %s", exc)
        return []


def record_plant_event(
    action: str,
    plant_code_id: str,
    *,
    actor: str | None = None,
    status: str = "ok",
    detail=None,
) -> bool:
    """Record a plant lifecycle event (create/delete/purge) in the registry database."""
    if not actor:
        try:
            actor = get_actor()
        except Exception:
            actor = None
    return _audit_write(
        lambda cur: cur.execute(
            "INSERT INTO plant_audit (actor, action, plant_code_id, status, detail) "
            "VALUES (%s, %s, %s, %s, %s)",
            (actor, action, plant_code_id, status, json.dumps(detail) if detail is not None else None),
        ),
        registry=True,
    )


def list_plant_events(plant_code_id: str | None = None, limit: int = 100) -> list[dict]:
    """Read plant lifecycle events from the registry, newest first."""
    conn = _get_conn(registry=True)
    try:
        cur = conn.cursor()
        if plant_code_id:
            cur.execute(
                "SELECT ts, actor, action, plant_code_id, status, detail FROM plant_audit "
                "WHERE plant_code_id = %s ORDER BY ts DESC LIMIT %s",
                (plant_code_id, limit),
            )
        else:
            cur.execute(
                "SELECT ts, actor, action, plant_code_id, status, detail FROM plant_audit "
                "ORDER BY ts DESC LIMIT %s",
                (limit,),
            )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        conn.close()
