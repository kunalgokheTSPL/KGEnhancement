"""Per-(plant, connector) lifecycle rollup, derived from flow_state so it cannot drift."""

from __future__ import annotations

import logging

from ..database.connection import _get_conn
from . import taxonomy as _taxonomy

_log = logging.getLogger("p0.connector_state")

CONNECTORS = (
    "pnid", "documents", "timeseries", "sap", "aif", "gloc", "lopc",
    "upd_event", "trip_event","alerts",
)

_FLOWS_BY_CONNECTOR = {
    "pnid": ("pnid",),
    "documents": ("docs",),
    "timeseries": ("ts", "ts_values"),
    "sap": ("sap",),
    "aif": ("aif",),
    "gloc": ("gloc",),
    "lopc": ("lopc",),
    "upd_event": ("upd_event",),
    "trip_event": ("trip_event",),
    "alerts": ("alerts",),
}

FLOWS_BY_CONNECTOR = dict(_FLOWS_BY_CONNECTOR)

FLOWS = tuple(sorted({f for flows in _FLOWS_BY_CONNECTOR.values() for f in flows}))

NOT_CONFIGURED = "NOT_CONFIGURED"
UPLOADED = "UPLOADED"
PROCESSING = "PROCESSING"
PROCESSED = "PROCESSED"
IN_REVIEW = "IN_REVIEW"
VALIDATED = "VALIDATED"
COMMITTED = "COMMITTED"
FAILED = "FAILED"

_UNPROCESSED_STAGES = ("uploaded", "staged")
_PROCESSED_STAGES = ("processed", "reviewing", "reviewed", "committing", "committed")

_STAGE_TO_STATE = {
    "uploaded": UPLOADED,
    "staged": UPLOADED,
    "processing": PROCESSING,
    "processed": PROCESSED,
    "reviewing": IN_REVIEW,
    "reviewed": VALIDATED,
    "committing": VALIDATED,
    "committed": COMMITTED,
}

_STATE_RANK = {
    NOT_CONFIGURED: 0,
    UPLOADED: 1,
    PROCESSING: 2,
    PROCESSED: 3,
    IN_REVIEW: 4,
    VALIDATED: 5,
    COMMITTED: 6,
}


def flow_names(connector: str) -> tuple[str, ...]:
    """A connector can span more than one flow — timeseries covers ts and ts_values."""
    return _FLOWS_BY_CONNECTOR.get((connector or "").strip().lower(), ())


def flow_name(connector: str) -> str | None:
    """The primary flow name for a connector, kept for existing callers."""
    flows = flow_names(connector)
    return flows[0] if flows else None


def _empty_counts() -> dict[str, int]:
    return {
        "files_total": 0,
        "pending": 0,
        "processing": 0,
        "processed": 0,
        "failed": 0,
        "skipped": 0,
        "superseded": 0,
    }


def _classify(stage: str | None, status: str | None) -> str:
    """One file's bucket in the seven standard counts."""
    if status == "failed":
        return "failed"
    if status == "skipped":
        return "skipped"
    if stage == "superseded":
        return "superseded"
    if stage == "processing":
        return "processing"
    if stage in _PROCESSED_STAGES:
        return "processed"
    return "pending"


def _rollup(rows: list[dict]) -> dict:
    """Fold the raw file rows into counts, timestamps and a single lifecycle state."""
    counts = _empty_counts()
    state = NOT_CONFIGURED
    any_failed = False
    unprocessed = 0
    configured_at = processed_at = validated_at = committed_at = None
    last_batch = last_job = None

    for row in rows:
        stage = row.get("stage")
        status = row.get("status")
        counts["files_total"] += 1
        counts[_classify(stage, status)] += 1

        if status == "failed":
            any_failed = True
        if stage in _UNPROCESSED_STAGES:
            unprocessed += 1

        row_state = _STAGE_TO_STATE.get(stage)
        if row_state and _STATE_RANK.get(row_state, 0) > _STATE_RANK.get(state, 0):
            state = row_state

        configured_at = _earliest(configured_at, row.get("uploaded_at") or row.get("created_at"))
        processed_at = _latest(processed_at, row.get("processed_at"))
        validated_at = _latest(validated_at, row.get("reviewed_at"))
        committed_at = _latest(committed_at, row.get("committed_at"))
        last_batch = last_batch or row.get("upload_batch_id")
        last_job = last_job or row.get("process_job_id")

    if unprocessed and _STATE_RANK.get(state, 0) > _STATE_RANK[UPLOADED]:
        state = UPLOADED

    return {
        "status": state,
        "counts": counts,
        "has_unprocessed_files": unprocessed > 0,
        "unprocessed_file_count": unprocessed,
        "total_file_count": counts["files_total"],
        "has_failures": any_failed,
        "configured_at": _as_text(configured_at),
        "processed_at": _as_text(processed_at),
        "validated_at": _as_text(validated_at),
        "committed_at": _as_text(committed_at),
        "last_upload_batch_id": last_batch,
        "last_run_id": last_job,
    }


def _earliest(current, candidate):
    if candidate is None:
        return current
    if current is None:
        return candidate
    return min(current, candidate)


def _latest(current, candidate):
    if candidate is None:
        return current
    if current is None:
        return candidate
    return max(current, candidate)


def _as_text(value):
    return None if value is None else str(value)


def _fetch_rows(plant_code_id: str, flows: tuple[str, ...]) -> list[dict]:
    """Read the columns the rollup needs, for every file of one flow."""
    columns = (
        "flow_uid",
        "file_name",
        "file_type",
        "stage",
        "status",
        "uploaded_at",
        "created_at",
        "processed_at",
        "reviewed_at",
        "committed_at",
        "upload_batch_id",
        "process_job_id",
    )
    conn = _get_conn(plant_code_id)
    try:
        cur = conn.cursor()
        cur.execute(
            f"SELECT {', '.join(columns)} FROM flow_state "
            "WHERE plant_code_id = %s AND flow = ANY(%s) ORDER BY updated_at DESC",
            (plant_code_id, list(flows)),
        )
        return [dict(zip(columns, row)) for row in cur.fetchall()]
    finally:
        conn.close()


def connector_state(
    plant_code_id: str, connector: str, *, include_groups: bool = False
) -> dict:
    """The lifecycle summary the source cards render, for one connector."""
    flows = flow_names(connector)
    state = {
        "plant_code_id": plant_code_id,
        "connector": connector,
        "flow": flows[0] if flows else None,
        "flows": list(flows),
        **_rollup([]),
    }
    if include_groups:
        state["groups"] = []
    if not flows:
        return state
    try:
        rows = _fetch_rows(plant_code_id, flows)
    except Exception as exc:
        _log.warning("[connector_state] %s/%s rollup failed: %s", plant_code_id, connector, exc)
        return state
    state.update(_rollup(rows))
    if include_groups:
        state["groups"] = _taxonomy.build_groups(
            connector,
            rows,
            classify=lambda row: _classify(row.get("stage"), row.get("status")),
            key_of=lambda row: row.get("file_type"),
        )
    return state


def all_connector_states(plant_code_id: str, *, include_groups: bool = False) -> list[dict]:
    """Every connector's state for one plant, so the cards render from one call."""
    return [
        connector_state(plant_code_id, name, include_groups=include_groups)
        for name in CONNECTORS
    ]
