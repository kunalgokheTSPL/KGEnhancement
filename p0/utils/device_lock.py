"""
Cross-process advisory lock for IoTDB device mutations.

IoTDB has no row/range locks, so a retention DELETE (read t_max, then delete
``time < cutoff``) can race a concurrent INSERT on the same device — the delete
might trim rows another writer just added, or a rolling-window "delete oldest N"
can miscount if the min moved underneath it. The writers live in different
PROCESSES (the API ingest/retention threads + the standalone 15-min simulator),
so an in-process threading.Lock isn't enough.

Postgres advisory locks ARE cross-process and both writers already talk to the
same Postgres, so we key a ``pg_advisory_xact_lock`` by the device path. Lock is
per-device (fine-grained — unrelated devices never block each other) and released
automatically at transaction end. Best-effort: if Postgres is unreachable the
context manager runs the body WITHOUT the lock rather than blocking ingest (the
race is rare and non-fatal — IoTDB stays consistent, only counts can drift).
"""

from __future__ import annotations

import hashlib
import logging
from contextlib import contextmanager

_log = logging.getLogger("p0.device_lock")

_LOCK_NAMESPACE = 0x1071DB


def _device_key(device: str) -> int:
    """Map a device path to a stable signed int4 for pg_advisory_xact_lock."""
    h = int(hashlib.sha1(device.encode("utf-8", "ignore")).hexdigest()[:8], 16)
    return h - 2**31


@contextmanager
def device_lock(device: str, conn_factory):
    """Hold a cross-process advisory lock on *device* for the duration of the"""
    conn = None
    locked = False
    try:
        conn = conn_factory()
        conn.autocommit = False
        cur = conn.cursor()
        cur.execute(
            "SELECT pg_advisory_xact_lock(%s, %s)",
            (_LOCK_NAMESPACE, _device_key(device)),
        )
        locked = True
    except Exception as exc:
        _log.warning("[device_lock] proceeding WITHOUT lock for %s: %s", device, exc)
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
            conn = None

    try:
        yield
        if locked and conn is not None:
            conn.commit()
    except Exception:
        if locked and conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        raise
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
