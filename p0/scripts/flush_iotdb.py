#!/usr/bin/env python3
"""
flush_iotdb.py — safely wipe IoTDB timeseries data via the REST v2 API.

The hydro/decisionops backend ingests sensor "values" into IoTDB under a device
root (default ``root.decisionops.sensors.<plant>.<file>``). During dev you often
need to clear that data so a re-upload starts clean. This script does it
properly: it deletes BOTH the data and the schema (timeseries definitions) so no
stale measurements linger, and it verifies the result.

Connection — all read from the environment, no defaults:
    IOTDB_HOST
    IOTDB_REST_PORT   (hydro local tunnel uses 19080)
    IOTDB_USER
    IOTDB_PASSWORD
    IOTDB_DEVICE_ROOT

Examples:
    # Dry run — show what WOULD be deleted, change nothing:
    python scripts/flush_iotdb.py --dry-run

    # Flush EVERYTHING under the device root (asks to confirm):
    python scripts/flush_iotdb.py

    # Flush one plant only, no prompt (CI/scripted):
    python scripts/flush_iotdb.py --plant HYDRO --yes

    # Against the hydro local tunnel:
    IOTDB_REST_PORT=19080 python scripts/flush_iotdb.py --plant HYDRO --yes

Exit codes: 0 success, 1 user-aborted, 2 connection/IoTDB error.
"""

from __future__ import annotations

import argparse
import os
import sys

try:
    import requests
except ImportError:
    sys.stderr.write(
        "This script needs the 'requests' package (pip install requests).\n"
    )
    sys.exit(2)


HOST = os.environ["IOTDB_HOST"]
PORT = int(os.environ["IOTDB_REST_PORT"])
USER = os.environ["IOTDB_USER"]
PASSWORD = os.environ["IOTDB_PASSWORD"]
DEVICE_ROOT = os.environ["IOTDB_DEVICE_ROOT"]

_BASE = f"http://{HOST}:{PORT}/rest/v2"
_AUTH = (USER, PASSWORD)
_TIMEOUT = 30


def _post(endpoint: str, sql: str) -> dict:
    """POST a SQL statement to /query or /nonQuery. Raises on transport error."""
    try:
        r = requests.post(
            f"{_BASE}/{endpoint}", json={"sql": sql}, auth=_AUTH, timeout=_TIMEOUT
        )
    except requests.RequestException as exc:
        raise SystemExit(_fail(f"cannot reach IoTDB at {_BASE} ({exc})"))
    try:
        body = r.json()
    except ValueError:
        body = {"raw": r.text}
    if r.status_code != 200:
        raise SystemExit(_fail(f"{endpoint} HTTP {r.status_code}: {body}"))
    return body


def query(sql: str) -> dict:
    return _post("query", sql)


def non_query(sql: str) -> dict:
    """Run a write/DDL statement. IoTDB signals SQL-level errors via a non-zero"""
    body = _post("nonQuery", sql)
    code = body.get("code")
    if code not in (None, 200):
        msg = str(body.get("message", "")).lower()
        if "does not exist" in msg or "not exist" in msg or "path not exist" in msg:
            return body
        raise SystemExit(_fail(f"IoTDB rejected: {sql}\n   → {body}"))
    return body


def _info(m):
    print(f"\033[36m→\033[0m {m}")


def _ok(m):
    print(f"\033[32m✓\033[0m {m}")


def _warn(m):
    print(f"\033[33m!\033[0m {m}")


def _fail(m):
    print(f"\033[31m✗\033[0m {m}")
    return 2


def _scalar(sql: str) -> int:
    """Run a COUNT query and return the single integer value (0 if absent)."""
    body = query(sql)
    vals = body.get("values") or []
    try:
        return int(vals[0][0])
    except (IndexError, TypeError, ValueError):
        return 0


def counts(scope: str) -> tuple[int, int]:
    """Return (timeseries_count, device_count) under *scope* path pattern."""
    ts = _scalar(f"COUNT TIMESERIES {scope}")
    try:
        dev = _scalar(f"COUNT DEVICES {scope}")
    except SystemExit:
        body = query(f"SHOW DEVICES {scope}")
        dev = len(body.get("values", [[]])[0]) if body.get("values") else 0
    return ts, dev


def main() -> int:
    ap = argparse.ArgumentParser(description="Safely flush IoTDB timeseries data.")
    ap.add_argument(
        "--plant",
        help="Flush only this plant (root.<root>.<plant>.**). "
        "Omit to flush the entire device root.",
    )
    ap.add_argument(
        "--root", default=DEVICE_ROOT, help=f"Device root path (default {DEVICE_ROOT})"
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deleted; change nothing.",
    )
    ap.add_argument("--yes", action="store_true", help="Skip the confirmation prompt.")
    args = ap.parse_args()

    root = args.root.rstrip(".")
    scope = f"{root}.{args.plant}.**" if args.plant else f"{root}.**"
    label = f"plant '{args.plant}'" if args.plant else "the ENTIRE device root"

    _info(f"IoTDB target: {_BASE}  (user={USER})")
    _info(f"Scope: {scope}  — {label}")

    ts_before, dev_before = counts(scope)
    _info(f"Before: {ts_before} timeseries across {dev_before} device(s)")

    if ts_before == 0 and dev_before == 0:
        _ok("Nothing to flush — already empty.")
        return 0

    if args.dry_run:
        _warn(f"DRY RUN — would delete timeseries: {scope}")
        _warn("No changes made.")
        return 0

    if not args.yes:
        ans = (
            input(
                f"\nDelete {ts_before} timeseries under {label}? "
                f"This is IRREVERSIBLE. Type 'yes' to proceed: "
            )
            .strip()
            .lower()
        )
        if ans != "yes":
            _warn("Aborted — nothing deleted.")
            return 1

    _info("Flushing…")
    non_query(f"DELETE FROM {scope}")
    non_query(f"DELETE TIMESERIES {scope}")
    try:
        non_query("FLUSH")
    except SystemExit:
        _warn("FLUSH not supported on this IoTDB build — deletions still applied.")

    ts_after, dev_after = counts(scope)
    if ts_after == 0 and dev_after == 0:
        _ok(
            f"Flushed: {ts_before} timeseries / {dev_before} device(s) removed. "
            f"Now empty."
        )
        return 0
    _warn(
        f"After flush still see {ts_after} timeseries / {dev_after} device(s) — "
        f"some paths may be outside the scope or recreated by a live ingest."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())