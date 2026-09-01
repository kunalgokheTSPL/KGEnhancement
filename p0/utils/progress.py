"""Progress marker emitted by pipeline scripts and parsed by the API runner."""

from __future__ import annotations

import json

MARKER = "##P0PROGRESS"
FILE_MARKER = "##P0FILE"


def emit(
    phase: str,
    *,
    label: str | None = None,
    items_done: int | None = None,
    items_total: int | None = None,
    current_item: str | None = None,
    status: str | None = None,
    message: str | None = None,
) -> None:
    """Print one progress marker on stdout for the parent runner to read."""
    payload: dict[str, object] = {"phase": phase}
    if label is not None:
        payload["label"] = label
    if items_done is not None:
        payload["items_done"] = items_done
    if items_total is not None:
        payload["items_total"] = items_total
    if current_item is not None:
        payload["current_item"] = current_item
    if status is not None:
        payload["status"] = status
    if message is not None:
        payload["message"] = message
    try:
        print(f"{MARKER} {json.dumps(payload)}", flush=True)
    except Exception:
        pass


def emit_file(
    file_name: str,
    status: str,
    *,
    error: str | None = None,
    rows: int | None = None,
) -> None:
    """Report one file's own outcome so a partial run is not reported as total failure."""
    payload: dict[str, object] = {"file": file_name, "status": status}
    if error is not None:
        payload["error"] = str(error)[:500]
    if rows is not None:
        payload["rows"] = rows
    try:
        print(f"{FILE_MARKER} {json.dumps(payload)}", flush=True)
    except Exception:
        pass
