"""Idempotency-Key replay: a repeated mutating request returns the original result."""

from __future__ import annotations

import hashlib
import json
import logging
import threading
import time

_log = logging.getLogger("p0.idempotency")

HEADER = "Idempotency-Key"
TTL_SECONDS = 24 * 60 * 60
_MAX_ENTRIES = 5000

_lock = threading.Lock()
_entries: dict[str, dict] = {}


def fingerprint(payload) -> str:
    """Stable hash of a request body, so the same key with a different body is rejected."""
    try:
        blob = json.dumps(payload, sort_keys=True, default=str)
    except (TypeError, ValueError):
        blob = repr(payload)
    return hashlib.sha256(blob.encode("utf-8", "ignore")).hexdigest()


def _prune(now: float) -> None:
    """Drop expired entries, and the oldest if the store has grown too large."""
    stale = [k for k, v in _entries.items() if now - v["stored_at"] > TTL_SECONDS]
    for key in stale:
        _entries.pop(key, None)
    if len(_entries) > _MAX_ENTRIES:
        for key, _ in sorted(_entries.items(), key=lambda kv: kv[1]["stored_at"])[
            : len(_entries) - _MAX_ENTRIES
        ]:
            _entries.pop(key, None)


def _scoped(plant_code_id: str, operation: str, key: str) -> str:
    return f"{plant_code_id}|{operation}|{key}"


def lookup(plant_code_id: str, operation: str, key: str | None, payload=None):
    """Return (replayed_result, conflict) for a key — both None on first use."""
    if not key:
        return None, None
    scoped = _scoped(plant_code_id, operation, key)
    now = time.time()
    with _lock:
        _prune(now)
        entry = _entries.get(scoped)
        if entry is None:
            return None, None
        if payload is not None and entry.get("fingerprint") != fingerprint(payload):
            return None, "body_mismatch"
        return entry.get("result"), None


def remember(
    plant_code_id: str,
    operation: str,
    key: str | None,
    payload,
    result,
    *,
    request_fingerprint: str | None = None,
) -> None:
    """Store a completed result so a replay of the same key returns it verbatim.

    Handlers mutate the request body in place, so the caller passes the fingerprint
    taken BEFORE the handler ran — otherwise an identical replay looks different.
    """
    if not key:
        return
    scoped = _scoped(plant_code_id, operation, key)
    with _lock:
        _entries[scoped] = {
            "result": result,
            "fingerprint": request_fingerprint or fingerprint(payload),
            "stored_at": time.time(),
        }
        _prune(time.time())


def reset() -> None:
    """Clear the store — tests only."""
    with _lock:
        _entries.clear()
