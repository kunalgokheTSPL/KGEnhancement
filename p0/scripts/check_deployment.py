"""Verify every backend the active DEPLOYMENT_MODE needs, in one green/red table.

Run this first on any new or changed environment:

    MASTER_KEY=... PYTHONPATH=$PWD .venv/bin/python p0/scripts/check_deployment.py

Exit code is non-zero if any core service (postgres, object store, iotdb) is red.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(BACKEND))

from utility.secret_manager import load_secrets  # noqa: E402

load_secrets()

from p0.driver import (  # noqa: E402
    deployment_mode,
    get_object_fs,
    object_store_container,
    storage_backend,
)

from p0.api.database.connection import open_db, registry_db_name  # noqa: E402

GREEN, RED, DIM = "\033[32m", "\033[31m", "\033[2m"
END = "\033[0m"


def check_postgres() -> tuple[bool, str]:
    """Connect to the plant registry database."""
    conn = open_db(registry_db_name(), connect_timeout=8)
    try:
        cur = conn.cursor()
        cur.execute("SELECT count(*) FROM plants")
        n = cur.fetchone()[0]
        host = os.environ.get(
            "AZURE_PG_HOST" if deployment_mode() == "azure" else "POSTGRES_HOST"
        )
        return True, f"{host} · {registry_db_name()} · {n} plant(s)"
    finally:
        conn.close()


def check_object_store() -> tuple[bool, str]:
    """List the root of the configured bucket/container."""
    fs = get_object_fs()
    container = object_store_container()
    fs.ls(container)
    return True, f"{storage_backend()} · {container}"


def check_timeseries() -> tuple[bool, str]:
    """Timeseries backend health: IoTDB on-prem, Azure SQL on azure."""
    from p0.driver import timeseries_backend

    if timeseries_backend() == "azuresql":
        return check_azuresql()
    return check_iotdb()


def check_azuresql() -> tuple[bool, str]:
    """Run a trivial query against the Azure SQL timeseries database."""
    from p0.driver import AzureSQLDriver

    driver = AzureSQLDriver()
    try:
        cur = driver.connect().cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        return True, f"{driver.config['server']} · {driver.config['database']}"
    finally:
        driver.close()


def check_iotdb() -> tuple[bool, str]:
    """Ping the IoTDB REST API with a trivial query."""
    import requests

    host, port = os.environ.get("IOTDB_HOST"), os.environ.get("IOTDB_PORT")
    r = requests.post(
        f"http://{host}:{port}/rest/v2/query",
        json={"sql": "SHOW DATABASES"},
        auth=(os.environ.get("IOTDB_USER"), os.environ.get("IOTDB_PASSWORD")),
        timeout=8,
    )
    r.raise_for_status()
    return True, f"{host}:{port} · REST v2"


def check_keycloak() -> tuple[bool, str]:
    """Reach the realm endpoint when real auth is enabled; skipped otherwise."""
    if (os.environ.get("AUTH_ENABLED") or "").upper() != "TRUE":
        return True, "skipped (AUTH_ENABLED != TRUE)"
    import requests

    url = f"{os.environ.get('KEYCLOAK_URL', '').rstrip('/')}/realms/{os.environ.get('KEYCLOAK_REALM')}"
    r = requests.get(url, timeout=8, verify=True)
    r.raise_for_status()
    return True, url


def main() -> int:
    print(f"\n  DEPLOYMENT_MODE = {deployment_mode()}   storage = {storage_backend()}\n")
    core = {"postgres": check_postgres, "object-store": check_object_store, "timeseries": check_timeseries}
    optional = {"keycloak": check_keycloak}
    failed = 0
    for group, req in ((core, True), (optional, False)):
        for name, fn in group.items():
            try:
                ok, detail = fn()
            except Exception as exc:
                ok, detail = False, f"{type(exc).__name__}: {str(exc)[:70]}"
            mark = f"{GREEN}✓{END}" if ok else f"{RED}✗{END}"
            print(f"  {mark} {name:14s} {DIM}{detail}{END}")
            if not ok and req:
                failed += 1
    print()
    if failed:
        print(f"  {RED}{failed} core service(s) unreachable — this environment is not ready.{END}\n")
    else:
        print(f"  {GREEN}environment ready.{END}\n")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
