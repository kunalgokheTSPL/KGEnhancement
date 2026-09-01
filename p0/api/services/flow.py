"""Durable per-file lifecycle store: one flow_state row per file, per plant, per flow."""

from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from . import audit as _audit
from ..database.connection import _get_conn
from ...utils.asset_identity import NO_SITE

_log = logging.getLogger("p0.flow")


STAGE_ORDER = [
    "uploaded",
    "staged",
    "processing",
    "processed",
    "reviewing",
    "reviewed",
    "committing",
    "committed",
]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()




_SAMPLE_BYTES = 1 * 1024 * 1024


def hash_file(path: str, *, sampled: bool = False) -> str:
    """Return a sha256 identity for the file at *path*."""
    h = hashlib.sha256()
    try:
        size = os.path.getsize(path)
        if not sampled:
            with open(path, "rb") as f:
                for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
                    h.update(chunk)
            return h.hexdigest()
        h.update(os.path.basename(path).encode("utf-8", "ignore"))
        h.update(str(size).encode())
        with open(path, "rb") as f:
            h.update(f.read(_SAMPLE_BYTES))
            if size > _SAMPLE_BYTES:
                f.seek(max(0, size - _SAMPLE_BYTES))
                h.update(f.read(_SAMPLE_BYTES))
        return h.hexdigest()
    except Exception as exc:
        _log.warning("[flow] hash_file(%s) failed: %s", path, exc)
        try:
            return hashlib.sha256(
                f"{os.path.basename(path)}|{os.path.getsize(path)}".encode()
            ).hexdigest()
        except Exception:
            return hashlib.sha256(path.encode()).hexdigest()


def hash_bytes(content: bytes, name: str = "", salt: str = "") -> str:
    """Full sha256 of in-memory bytes — for callers that already hold the file"""
    _salt = (salt or "").encode("utf-8", "ignore")
    try:
        h = hashlib.sha256()
        if _salt:
            h.update(_salt + b"|")
        h.update(content)
        return h.hexdigest()
    except Exception as exc:
        _log.warning("[flow] hash_bytes(%s) failed: %s", name, exc)
        return hashlib.sha256(
            f"{salt}|{name}|{len(content) if content else 0}".encode()
        ).hexdigest()



SCHEMA_VERSION = "2"

UNCATEGORISED = "uncategorised"

_JSON_COLS = {"sheet_names", "labels"}


def _canonical_category(raw) -> str:
    """Never NULL — Postgres treats NULLs as distinct, which would defeat the unique key."""
    from . import taxonomy as _taxonomy

    return _taxonomy.canonical_category(raw) or UNCATEGORISED


def find_duplicates(
    plant_code_id: str,
    flow: str,
    *,
    file_name: str | None = None,
    size_bytes: int | None = None,
    content_hash: str | None = None,
    category: str | None = None,
) -> dict:
    """Server-side duplicate verdict by content, by name, and by name+size."""
    verdict = {
        "is_duplicate": False,
        "duplicate_of": None,
        "matched_on": None,
        "same_name": [],
        "same_content": [],
    }
    try:
        conn = _get_conn(plant_code_id)
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT flow_uid, file_name, size_bytes, content_hash, category, stage "
                "FROM flow_state WHERE plant_code_id = %s AND flow = %s "
                "AND (content_hash = %s OR file_name = %s)",
                (plant_code_id, flow, content_hash or "", file_name or ""),
            )
            wanted = _canonical_category(category)
            for row in cur.fetchall():
                entry = {
                    "flow_uid": row[0],
                    "file_name": row[1],
                    "size_bytes": row[2],
                    "category": row[4],
                    "stage": row[5],
                }
                if content_hash and row[3] == content_hash:
                    verdict["same_content"].append(entry)
                if file_name and row[1] == file_name:
                    verdict["same_name"].append(entry)

            exact = next(
                (
                    e
                    for e in verdict["same_content"]
                    if _canonical_category(e["category"]) == wanted
                ),
                None,
            )
            if exact:
                verdict.update(
                    is_duplicate=True, duplicate_of=exact["flow_uid"], matched_on="content_hash"
                )
            elif verdict["same_content"]:
                first = verdict["same_content"][0]
                verdict.update(
                    is_duplicate=True,
                    duplicate_of=first["flow_uid"],
                    matched_on="content_hash_other_category",
                )
            else:
                by_name_size = next(
                    (
                        e
                        for e in verdict["same_name"]
                        if size_bytes is not None and e["size_bytes"] == size_bytes
                    ),
                    None,
                )
                if by_name_size:
                    verdict.update(
                        is_duplicate=True,
                        duplicate_of=by_name_size["flow_uid"],
                        matched_on="name_and_size",
                    )
                elif verdict["same_name"]:
                    verdict.update(
                        is_duplicate=False,
                        duplicate_of=verdict["same_name"][0]["flow_uid"],
                        matched_on="name_only",
                    )
            return verdict
        finally:
            conn.close()
    except Exception as exc:
        _log.warning("[flow] duplicate check failed: %s", exc)
        return verdict

_COLS = [
    "flow_uid",
    "plant_code_id",
    "site",
    "flow",
    "file_name",
    "file_type",
    "content_hash",
    "size_bytes",
    "stage",
    "status",
    "error",
    "hash_method",
    "schema_version",
    "upload_job_id",
    "rustfs_path",
    "upload_batch_id",
    "process_job_id",
    "processed_parquet",
    "row_count",
    "rows_in",
    "rows_rejected",
    "review_patches",
    "review_deletes",
    "commit_uid",
    "rows_committed",
    "rows_ingested",
    "iotdb_path",
    "ts_from",
    "ts_to",
    "uploaded_at",
    "processing_started_at",
    "processed_at",
    "reviewed_at",
    "committed_at",
    "attempt_count",
    "last_error_at",
    "category",
    "content_type",
    "page_count",
    "has_text_layer",
    "sheet_names",
    "labels",
    "duplicate_of",
    "uploaded_by",
    "created_by",
    "created_at",
    "updated_at",
]
_SETTABLE = {
    "site",
    "category",
    "content_type",
    "page_count",
    "has_text_layer",
    "sheet_names",
    "labels",
    "duplicate_of",
    "uploaded_by",
    "file_name",
    "file_type",
    "size_bytes",
    "stage",
    "status",
    "error",
    "hash_method",
    "schema_version",
    "upload_job_id",
    "rustfs_path",
    "upload_batch_id",
    "process_job_id",
    "processed_parquet",
    "row_count",
    "rows_in",
    "rows_rejected",
    "review_patches",
    "review_deletes",
    "commit_uid",
    "rows_committed",
    "rows_ingested",
    "iotdb_path",
    "ts_from",
    "ts_to",
    "uploaded_at",
    "processing_started_at",
    "processed_at",
    "reviewed_at",
    "committed_at",
    "attempt_count",
    "last_error_at",
    "created_by",
}


def _row_to_dict(row) -> dict:
    out = {}
    for c, v in zip(_COLS, row):
        if isinstance(v, datetime):
            v = v.isoformat()
        out[c] = v
    return out


CONFLICT_KEY = ("plant_code_id", "site", "flow", "category", "content_hash", "file_name")

_IMMUTABLE_ON_CONFLICT = frozenset(CONFLICT_KEY)

UNNAMED = "unnamed"


def _canonical_file_name(raw, content_hash: str) -> str:
    """Never NULL — Postgres treats NULLs as distinct, which would defeat the key."""
    name = str(raw or "").strip()
    return name or f"{UNNAMED}-{(content_hash or '')[:12]}"


def _canonical_site(raw) -> str:
    """Never NULL, for the same reason as the file name."""
    return str(raw or "").strip() or NO_SITE


def upsert_file(plant_code_id: str, flow: str, content_hash: str, **fields) -> dict | None:
    """Insert or advance the flow row identified by (plant_code_id, flow,"""
    try:
        write = {k: v for k, v in fields.items() if k in _SETTABLE}
        write["plant_code_id"] = plant_code_id
        write["flow"] = flow
        write["content_hash"] = content_hash
        write["site"] = _canonical_site(write.get("site"))
        write["category"] = _canonical_category(write.get("category") or fields.get("file_type"))
        write["file_name"] = _canonical_file_name(write.get("file_name"), content_hash)
        for _json_col in _JSON_COLS & set(write):
            if write[_json_col] is not None and not isinstance(write[_json_col], str):
                write[_json_col] = json.dumps(write[_json_col])
        write["updated_at"] = _now()
        if write.get("stage") in ("uploaded", "staged"):
            write.setdefault("uploaded_at", _now())
        write.setdefault("schema_version", SCHEMA_VERSION)

        cols = list(write.keys())
        col_names = ", ".join(f'"{c}"' for c in cols)
        placeholders = ", ".join(["%s"] * len(cols))
        update_set = ", ".join(
            f'"{c}" = COALESCE(EXCLUDED."{c}", flow_state."{c}")'
            for c in cols
            if c not in _IMMUTABLE_ON_CONFLICT
        )
        update_set = update_set.replace(
            '"updated_at" = COALESCE(EXCLUDED."updated_at", flow_state."updated_at")',
            '"updated_at" = EXCLUDED."updated_at"',
        )
        sql = (
            f"INSERT INTO flow_state ({col_names}) VALUES ({placeholders}) "
            f"ON CONFLICT ({', '.join(CONFLICT_KEY)}) DO UPDATE SET {update_set} "
            f"RETURNING {', '.join(_COLS)}"
        )
        conn = _get_conn(plant_code_id)
        try:
            cur = conn.cursor()
            cur.execute(sql, [write[c] for c in cols])
            r = cur.fetchone()
            conn.commit()
            row = _row_to_dict(r) if r else None
            if row and (write.get("stage") in ("uploaded", "staged")):
                _emit_run_event(row, event=write.get("stage"), fields=write)
            return row
        finally:
            conn.close()
    except Exception as exc:
        _log.warning("[flow] upsert_file(%s/%s) failed: %s", plant_code_id, flow, exc)
        return None


_STAGE_TS = {
    "uploaded": "uploaded_at",
    "processing": "processing_started_at",
    "processed": "processed_at",
    "reviewed": "reviewed_at",
    "committed": "committed_at",
}


def _emit_run_event(
    row: dict | None, *, event: str, fields: dict | None = None
) -> None:
    """Append a row to the run_events lineage ledger from a flow_state row dict."""
    if not row:
        return
    try:
        f = fields or {}
        _audit.append_event(
            row.get("plant_code_id"),
            row.get("flow"),
            event,
            file_name=row.get("file_name"),
            content_hash=row.get("content_hash"),
            flow_uid=row.get("flow_uid"),
            upload_batch_id=row.get("upload_batch_id"),
            commit_uid=row.get("commit_uid"),
            rows_in=f.get("rows_in") if "rows_in" in f else row.get("rows_in"),
            rows_out=(
                f.get("rows_committed")
                or f.get("row_count")
                or row.get("rows_committed")
                or row.get("row_count")
            ),
            rows_rejected=row.get("rows_rejected"),
            status="failed" if f.get("status") == "failed" else "ok",
            detail=({"error": f.get("error")} if f.get("error") else None),
        )
    except Exception as exc:
        _log.warning("[flow] run_event emit failed: %s", exc)


def advance_stage(flow_uid: int, stage: str, plant_code_id: str | None = None, **fields) -> dict | None:
    """Advance a row to *stage* and auto-stamp that stage's timestamp column"""
    ts_col = _STAGE_TS.get(stage)
    extra = dict(fields)
    extra["stage"] = stage
    if ts_col and ts_col not in extra:
        extra[ts_col] = _now()
    if (fields.get("status") == "failed") and "last_error_at" not in extra:
        extra["last_error_at"] = _now()
    row = advance(flow_uid, plant_code_id=plant_code_id, **extra)
    _emit_run_event(row, event=stage, fields=fields)
    return row


def find_by_file(plant_code_id: str, flow: str, file_name: str) -> dict | None:
    """Look up one file by its (plant, flow, file_name). Used to scope a"""
    if not file_name:
        return None
    want = str(file_name).strip().replace("\\", "/").rsplit("/", 1)[-1]
    try:
        conn = _get_conn(plant_code_id)
        try:
            cur = conn.cursor()
            cur.execute(
                f"SELECT {', '.join(_COLS)} FROM flow_state "
                "WHERE plant_code_id=%s AND flow=%s "
                "ORDER BY updated_at DESC",
                (plant_code_id, flow),
            )
            rows = [_row_to_dict(r) for r in cur.fetchall()]
        finally:
            conn.close()
        for r in rows:
            fn = (
                str(r.get("file_name") or "")
                .strip()
                .replace("\\", "/")
                .rsplit("/", 1)[-1]
            )
            if fn == want:
                return r
        return None
    except Exception as exc:
        _log.warning("[flow] find_by_file failed: %s", exc)
        return None


def advance(flow_uid: int, plant_code_id: str | None = None, **fields) -> dict | None:
    """Update an existing flow row by flow_uid (e.g. a stage writer that only"""
    try:
        write = {k: v for k, v in fields.items() if k in _SETTABLE and v is not None}
        if not write:
            return get(flow_uid)
        write["updated_at"] = _now()
        set_sql = ", ".join(f'"{c}" = %s' for c in write)
        sql = (
            f"UPDATE flow_state SET {set_sql} WHERE flow_uid = %s "
            f"RETURNING {', '.join(_COLS)}"
        )
        conn = _get_conn(plant_code_id)
        try:
            cur = conn.cursor()
            cur.execute(sql, [*write.values(), flow_uid])
            r = cur.fetchone()
            conn.commit()
            return _row_to_dict(r) if r else None
        finally:
            conn.close()
    except Exception as exc:
        _log.warning("[flow] advance(%s) failed: %s", flow_uid, exc)
        return None


def reset(flow_uid: int, plant_code_id: str | None = None) -> dict | None:
    """Reset a file back to 'uploaded'/'pending' so it can be reprocessed"""
    try:
        sql = (
            "UPDATE flow_state SET stage='uploaded', status='pending', error=NULL, "
            "upload_batch_id=NULL, process_job_id=NULL, processed_parquet=NULL, row_count=NULL, "
            "rows_in=NULL, rows_rejected=NULL, "
            "review_patches=NULL, review_deletes=NULL, commit_uid=NULL, "
            "rows_committed=NULL, rows_ingested=NULL, "
            "processing_started_at=NULL, processed_at=NULL, reviewed_at=NULL, "
            "committed_at=NULL, "
            "attempt_count=COALESCE(attempt_count,0)+1, "
            "updated_at=%s WHERE flow_uid=%s "
            f"RETURNING {', '.join(_COLS)}"
        )
        conn = _get_conn(plant_code_id)
        try:
            cur = conn.cursor()
            cur.execute(sql, [_now(), flow_uid])
            r = cur.fetchone()
            conn.commit()
            row = _row_to_dict(r) if r else None
            _emit_run_event(row, event="reset")
            return row
        finally:
            conn.close()
    except Exception as exc:
        _log.warning("[flow] reset(%s) failed: %s", flow_uid, exc)
        return None


def delete(flow_uid: int, plant_code_id: str | None = None) -> bool:
    """Remove a flow row. Best-effort — returns True on success."""
    try:
        conn = _get_conn(plant_code_id)
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM flow_state WHERE flow_uid = %s", (flow_uid,))
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()
    except Exception as exc:
        _log.warning("[flow] delete(%s) failed: %s", flow_uid, exc)
        return False


def get(flow_uid: int, plant_code_id: str | None = None) -> dict | None:
    try:
        conn = _get_conn(plant_code_id)
        try:
            cur = conn.cursor()
            cur.execute(
                f"SELECT {', '.join(_COLS)} FROM flow_state WHERE flow_uid = %s",
                (flow_uid,),
            )
            r = cur.fetchone()
            return _row_to_dict(r) if r else None
        finally:
            conn.close()
    except Exception as exc:
        _log.warning("[flow] get(%s) failed: %s", flow_uid, exc)
        return None


def find(plant_code_id: str, flow: str, content_hash: str) -> dict | None:
    """Look up one file by identity — used to power the 'already processed'"""
    try:
        conn = _get_conn(plant_code_id)
        try:
            cur = conn.cursor()
            cur.execute(
                f"SELECT {', '.join(_COLS)} FROM flow_state "
                "WHERE plant_code_id=%s AND flow=%s AND content_hash=%s",
                (plant_code_id, flow, content_hash),
            )
            r = cur.fetchone()
            return _row_to_dict(r) if r else None
        finally:
            conn.close()
    except Exception as exc:
        _log.warning("[flow] find failed: %s", exc)
        return None


def list_flow(plant_code_id: str, flow: str | None = None, limit: int = 200) -> list[dict]:
    """List flow rows for a plant (optionally one flow), newest first. Drives"""
    try:
        conn = _get_conn(plant_code_id)
        try:
            cur = conn.cursor()
            where, params = ["plant_code_id = %s"], [plant_code_id]
            if flow:
                where.append("flow = %s")
                params.append(flow)
            params.append(limit)
            cur.execute(
                f"SELECT {', '.join(_COLS)} FROM flow_state "
                f"WHERE {' AND '.join(where)} ORDER BY updated_at DESC LIMIT %s",
                params,
            )
            return [_row_to_dict(r) for r in cur.fetchall()]
        finally:
            conn.close()
    except Exception as exc:
        _log.warning("[flow] list_flow failed: %s", exc)
        return []
