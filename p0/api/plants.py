"""Plant registry: which plants exist and which database holds each one's data.

See .data/p0-dev-doc/plant-registry.md for the model and rationale.
"""

from __future__ import annotations

import logging
import re
from http import HTTPStatus

import requests
import yaml
from fastapi import HTTPException

from .database.connection import cdm_db_base as _cdm_db_base
from .database.connection import open_db as _open_db
from .database.connection import registry_db_name as _registry_db_name

from p0.utils import fs as _fs

from .config import (
    _PROCESSED_ROOT,
    _STAGING_PT_ROOT,
    CDM_DB_USER,
    IOTDB_DEVICE_ROOT,
    IOTDB_HOST,
    IOTDB_PASSWORD,
    IOTDB_PORT,
    IOTDB_USER,
    RUSTFS_OUT_PREFIX,
    SCHEMA_FROZEN_FILE,
    _sanitise_plant,
)
from .database.connection import _get_conn

_log = logging.getLogger("p0.plants")




def ensure_registry_database() -> None:
    """Create the registry's own database (``<base>_registry``) if missing. Idempotent."""
    db_name = _registry_db_name()
    mconn = _maintenance_conn()
    try:
        cur = mconn.cursor()
        if not _database_exists(cur, db_name):
            try:
                cur.execute(f'CREATE DATABASE "{db_name}" TEMPLATE template0')
                _log.info("[plants] created registry database: %s", db_name)
            except Exception:
                if not _database_exists(cur, db_name):
                    raise
                _log.info("[plants] registry database already created concurrently: %s", db_name)
    finally:
        mconn.close()


def ensure_plants_table() -> None:
    """Materialise the ``plants`` registry table, and backfill ``db_name`` on"""

    create_sql = None
    try:
        with open(SCHEMA_FROZEN_FILE) as f:
            schema_doc = yaml.safe_load(f) or {}
        cols = (schema_doc.get("schema") or {}).get("plants", {}).get("columns") or {}
        if cols:
            col_defs = []
            for name, sql_type in cols.items():
                suffix = " PRIMARY KEY" if name == "plant_code_id" else ""
                if name == "created_at":
                    suffix += " DEFAULT now()"
                col_defs.append(f'"{name}" {sql_type}{suffix}')
            create_sql = f"CREATE TABLE IF NOT EXISTS plants ({', '.join(col_defs)})"
    except Exception as exc:
        _log.warning(
            "[plants] YAML schema read failed (%s); using hard-coded fallback", exc
        )

    if create_sql is None:
        create_sql = (
            "CREATE TABLE IF NOT EXISTS plants ("
            "plant_code_id VARCHAR(50) PRIMARY KEY, "
            "label VARCHAR(200), "
            "industry VARCHAR(80), "
            "db_name VARCHAR(63), "
            "created_at TIMESTAMP DEFAULT now(), "
            "created_by VARCHAR(120))"
        )

    ensure_registry_database()
    conn = _get_conn(registry=True)
    try:
        cur = conn.cursor()
        cur.execute(create_sql)
        cur.execute("ALTER TABLE plants ADD COLUMN IF NOT EXISTS db_name VARCHAR(63)")
        cur.execute(
            "CREATE TABLE IF NOT EXISTS plant_audit ("
            "id BIGSERIAL PRIMARY KEY, ts TIMESTAMPTZ NOT NULL DEFAULT now(), "
            "actor VARCHAR(120), action VARCHAR(60) NOT NULL, "
            "plant_code_id VARCHAR(50) NOT NULL, status VARCHAR(20) NOT NULL, "
            "detail JSONB)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_plant_audit_plant "
            "ON plant_audit (plant_code_id, ts DESC)"
        )
        cur.execute("SELECT plant_code_id FROM plants WHERE db_name IS NULL")
        for (plant_code_id,) in cur.fetchall():
            try:
                db_name = plant_db_name(plant_code_id)
            except HTTPException:
                continue
            cur.execute(
                "UPDATE plants SET db_name = %s WHERE plant_code_id = %s",
                (db_name, plant_code_id),
            )
        conn.commit()
    finally:
        conn.close()




def _connect_plant_db(db_name: str):
    """Open a connection to a specific plant database (via the CDM driver)."""
    return _open_db(db_name, connect_timeout=10)


def _list_plant_databases() -> list[str]:
    """All plant database names on the cluster, excluding the reserved service/infra ones."""
    mconn = _maintenance_conn()
    try:
        cur = mconn.cursor()
        cur.execute(
            r"SELECT datname FROM pg_database WHERE datname LIKE %s ORDER BY datname",
            (_PLANT_DB_BASE + r"\_%",),
        )
        return [r[0] for r in cur.fetchall() if r[0] not in _RESERVED_DBS]
    finally:
        mconn.close()


_registry_db_names: dict[str, str] = {}


def list_plants() -> list[dict]:
    """Every registered plant, read from the central registry."""
    conn = _get_conn(registry=True)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT plant_code_id, label, industry, db_name, created_at, created_by "
            "FROM plants ORDER BY created_at, plant_code_id"
        )
        rows = cur.fetchall()
    finally:
        conn.close()
    return [
        {
            "plant_code_id": r[0],
            "label": r[1],
            "industry": r[2],
            "db_name": r[3],
            "created_at": r[4].isoformat() if r[4] else None,
            "created_by": r[5],
        }
        for r in rows
    ]


def get_plant_codes() -> set[str]:
    """Set of registered plant_code_id values."""
    return {p["plant_code_id"] for p in list_plants()}


def registered_plant_db_name(plant_code_id: str) -> str | None:
    """The database holding this plant's data, or None if unregistered. The routing primitive."""
    plant_code_id = (plant_code_id or "").strip()
    if not plant_code_id:
        return None
    cached = _registry_db_names.get(plant_code_id)
    if cached:
        return cached

    try:
        conn = _get_conn(registry=True)
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT db_name FROM plants WHERE plant_code_id = %s", (plant_code_id,)
            )
            row = cur.fetchone()
        finally:
            conn.close()
    except Exception as exc:
        _log.warning("[plants] registry lookup for '%s' failed: %s", plant_code_id, exc)
        return None

    if row is None:
        return None
    db_name = row[0]
    if not db_name:
        try:
            db_name = plant_db_name(plant_code_id)
        except HTTPException:
            return None
    _registry_db_names[plant_code_id] = db_name
    return db_name


def plant_is_registered(plant_code_id: str) -> bool:
    """Whether the plant has a row in the central registry."""
    return registered_plant_db_name(plant_code_id) is not None


def registered_plant_industry(plant_code_id: str) -> str | None:
    """The industry this plant was registered with, or None if unregistered/unset."""
    plant_code_id = (plant_code_id or "").strip()
    if not plant_code_id:
        return None
    try:
        conn = _get_conn(registry=True)
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT industry FROM plants WHERE plant_code_id = %s", (plant_code_id,)
            )
            row = cur.fetchone()
        finally:
            conn.close()
    except Exception as exc:
        _log.warning("[plants] industry lookup for '%s' failed: %s", plant_code_id, exc)
        return None
    return row[0] if row else None


def validate_plant_code(plant_code_id: str) -> None:
    """Raise 400 if ``plant_code_id`` is not in the registry. Called before any upload persists."""
    if not plant_code_id or plant_code_id.strip() == "":
        raise HTTPException(HTTPStatus.BAD_REQUEST, "plant_code_id is required")
    if not plant_is_registered(plant_code_id):
        raise HTTPException(
            HTTPStatus.BAD_REQUEST,
            f"plant_code_id '{plant_code_id}' is not registered. "
            f"Add it via POST /configRouter/plants first.",
        )


def _register_plant(
    plant_code_id: str,
    label: str | None,
    industry: str | None,
    created_by: str | None,
    db_name: str,
    created_at=None,
):
    """Insert the registry row; returns created_at, or None if already registered."""
    conn = _get_conn(registry=True)
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO plants "
            "(plant_code_id, label, industry, db_name, created_by, created_at) "
            "VALUES (%s, %s, %s, %s, %s, COALESCE(%s::timestamp, now())) "
            "ON CONFLICT (plant_code_id) DO NOTHING RETURNING created_at",
            (
                plant_code_id,
                label or plant_code_id,
                industry,
                db_name,
                created_by,
                created_at,
            ),
        )
        row = cur.fetchone()
        conn.commit()
    finally:
        conn.close()
    if row is None:
        return None
    _registry_db_names[plant_code_id] = db_name
    return row[0]


def _unregister_plant(plant_code_id: str) -> None:
    """Delete the plant's registry row. The plant stops resolving immediately."""
    _registry_db_names.pop(plant_code_id, None)
    conn = _get_conn(registry=True)
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM plants WHERE plant_code_id = %s", (plant_code_id,))
        conn.commit()
    finally:
        conn.close()


def sync_registry_from_plant_databases() -> int:
    """Adopt plant databases that only self-registered locally into the central registry."""
    adopted = 0
    for db_name in _list_plant_databases():
        suffix = db_name[len(_PLANT_DB_BASE) + 1 :]
        try:
            conn = _connect_plant_db(db_name)
            try:
                cur = conn.cursor()
                cur.execute(
                    "SELECT plant_code_id, label, industry, created_by, created_at "
                    "FROM plants LIMIT 1"
                )
                row = cur.fetchone()
            finally:
                conn.close()
        except Exception:
            continue
        if not row:
            continue
        try:
            if _normalize_plant_suffix(row[0]) != suffix:
                continue
        except HTTPException:
            continue
        if _register_plant(row[0], row[1], row[2], row[3], db_name, row[4]) is not None:
            adopted += 1
            _log.info("[plants] adopted pre-registry plant: %s -> %s", row[0], db_name)
    return adopted




def _assert_has_dedicated_database(plant_code_id: str) -> str:
    """Guard the destructive paths: 404 if unregistered, 409 if the plant's data is"""
    db_name = registered_plant_db_name(plant_code_id)
    if db_name is None:
        raise HTTPException(
            HTTPStatus.NOT_FOUND, f"plant_code_id '{plant_code_id}' not found"
        )
    if db_name in _RESERVED_DBS:
        raise HTTPException(
            HTTPStatus.CONFLICT,
            f"plant_code_id '{plant_code_id}' has no dedicated database — its data "
            f"is in the shared '{db_name}', which cannot be dropped for one plant",
        )
    return db_name


def add_plant(
    plant_code_id: str, label: str | None, industry: str | None, created_by: str | None
) -> dict:
    """Write the registry row, then provision the database it names (rollback on failure)."""
    plant_code_id = plant_code_id.strip()
    if not plant_code_id:
        raise HTTPException(HTTPStatus.BAD_REQUEST, "plant_code_id is required")
    if len(plant_code_id) > 50:
        raise HTTPException(HTTPStatus.BAD_REQUEST, "plant_code_id max length is 50")

    db_name = plant_db_name(plant_code_id)
    if db_name in _RESERVED_DBS:
        raise HTTPException(
            HTTPStatus.BAD_REQUEST,
            f"plant_code_id '{plant_code_id}' maps to reserved database name '{db_name}'",
        )
    if plant_db_exists(plant_code_id):
        raise HTTPException(
            HTTPStatus.CONFLICT, f"plant_code_id '{plant_code_id}' already exists"
        )

    ensure_plants_table()

    created_at = _register_plant(plant_code_id, label, industry, created_by, db_name)
    if created_at is None:
        raise HTTPException(
            HTTPStatus.CONFLICT, f"plant_code_id '{plant_code_id}' already exists"
        )

    try:
        _provision_plant_databases(plant_code_id, label, industry, created_by)
    except HTTPException:
        _drop_plant_databases(plant_code_id)
        _unregister_plant(plant_code_id)
        raise
    except Exception as exc:
        _drop_plant_databases(plant_code_id)
        _unregister_plant(plant_code_id)
        raise HTTPException(
            HTTPStatus.INTERNAL_SERVER_ERROR,
            f"Failed to provision database for plant '{plant_code_id}': {exc}",
        )

    try:
        from .services.user_config_store import initialise_plant

        initialise_plant(plant_code_id, created_by)
    except Exception as exc:
        _log.warning("[plants] user config init for %s skipped: %s", plant_code_id, exc)

    _log.info("[plants] added: %s -> %s (by=%s)", plant_code_id, db_name, created_by)
    return {
        "plant_code_id": plant_code_id,
        "label": label or plant_code_id,
        "industry": industry,
        "db_name": db_name,
        "created_at": created_at.isoformat() if created_at else None,
        "created_by": created_by,
    }


def delete_plant(plant_code_id: str) -> None:
    """Drop the plant's dedicated database, then deregister it (that order keeps a"""
    plant_code_id = plant_code_id.strip()
    _assert_has_dedicated_database(plant_code_id)
    res = _drop_plant_databases(plant_code_id)
    if "error" in res:
        raise HTTPException(
            HTTPStatus.INTERNAL_SERVER_ERROR,
            f"Failed to drop database for plant '{plant_code_id}': {res['error']}",
        )
    _unregister_plant(plant_code_id)
    _log.info("[plants] deleted: %s", plant_code_id)




def _plant_code_tables(cur) -> list[str]:
    """Tables in the current schema with a plant_code_id column (excluding the registry)."""
    cur.execute(
        "SELECT table_name FROM information_schema.columns "
        "WHERE table_schema = current_schema() "
        "  AND column_name = 'plant_code_id' "
        "  AND table_name <> 'plants' "
        "ORDER BY table_name"
    )
    return [r[0] for r in cur.fetchall()]


def _azuresql_ts_tables(plant_code_id: str) -> list[str]:
    """This plant's Azure SQL device tables (empty on any error)."""
    from p0.driver import AzureSQLDriver
    from .database import timeseries_sql as _ts_sql
    from .database.timeseries import _norm_measurement

    prefix = f"{IOTDB_DEVICE_ROOT}.{_norm_measurement(plant_code_id)}"
    driver = AzureSQLDriver()
    try:
        return _ts_sql.list_devices(driver.connect(), prefix)
    except Exception as exc:
        _log.warning("[plants] azuresql ts list(%s) failed: %s", plant_code_id, exc)
        return []
    finally:
        driver.close()


def _iotdb_series_count(plant_code_id: str) -> int:
    """COUNT this plant's timeseries series (IoTDB on-prem, Azure SQL tables on azure). 0 on error."""
    from p0.driver import timeseries_backend

    if timeseries_backend() == "azuresql":
        return len(_azuresql_ts_tables(plant_code_id))
    try:
        from .database.timeseries import _norm_measurement

        scope = f"{IOTDB_DEVICE_ROOT}.{_norm_measurement(plant_code_id)}.**"
        r = requests.post(
            f"http://{IOTDB_HOST}:{IOTDB_PORT}/rest/v2/query",
            json={"sql": f"COUNT TIMESERIES {scope}"},
            auth=(IOTDB_USER, IOTDB_PASSWORD),
            timeout=15,
        )
        vals = (r.json() or {}).get("values") or []
        return int(vals[0][0]) if vals and vals[0] else 0
    except Exception as exc:
        _log.warning("[plants] iotdb count(%s) failed: %s", plant_code_id, exc)
        return 0


def _purge_azuresql_ts(plant_code_id: str) -> dict:
    """Drop this plant's Azure SQL device tables. Best-effort."""
    from p0.driver import AzureSQLDriver
    from .database import timeseries_sql as _ts_sql
    from .database.timeseries import _norm_measurement

    prefix = f"{IOTDB_DEVICE_ROOT}.{_norm_measurement(plant_code_id)}"
    driver = AzureSQLDriver()
    try:
        conn = driver.connect()
        tables = _ts_sql.list_devices(conn, prefix)
        for t in tables:
            _ts_sql.drop_device(conn, t)
        return {"series_removed": len(tables)}
    except Exception as exc:
        _log.warning("[plants] azuresql ts purge(%s) failed: %s", plant_code_id, exc)
        return {"error": f"{type(exc).__name__}: {exc!s:.200}"}
    finally:
        driver.close()


def _purge_iotdb(plant_code_id: str) -> dict:
    """Delete this plant's timeseries data + schema (IoTDB on-prem, Azure SQL on azure). Best-effort."""
    from p0.driver import timeseries_backend

    if timeseries_backend() == "azuresql":
        return _purge_azuresql_ts(plant_code_id)
    try:
        from .database.timeseries import _norm_measurement

        scope = f"{IOTDB_DEVICE_ROOT}.{_norm_measurement(plant_code_id)}.**"
        url = f"http://{IOTDB_HOST}:{IOTDB_PORT}/rest/v2/nonQuery"
        auth = (IOTDB_USER, IOTDB_PASSWORD)
        before = _iotdb_series_count(plant_code_id)
        for sql in (f"DELETE FROM {scope}", f"DELETE TIMESERIES {scope}"):
            requests.post(url, json={"sql": sql}, auth=auth, timeout=30)
        return {"series_removed": before}
    except Exception as exc:
        _log.warning("[plants] iotdb purge(%s) failed: %s", plant_code_id, exc)
        return {"error": f"{type(exc).__name__}: {exc!s:.200}"}


def _plant_rustfs_prefixes(plant_code_id: str) -> list[str]:
    """All RustFS prefixes (without the s3:// scheme) that hold this plant's"""

    safe = _sanitise_plant(plant_code_id)
    roots = [_STAGING_PT_ROOT, _PROCESSED_ROOT, RUSTFS_OUT_PREFIX]
    return [_fs.strip_scheme(f"{root}/{safe}") for root in roots]


def _rustfs_object_count(plant_code_id: str) -> int:
    try:
        from .services.doc_types import _get_s3fs

        sfs = _get_s3fs()
        n = 0
        for pref in _plant_rustfs_prefixes(plant_code_id):
            try:
                n += len(sfs.find(pref))
            except Exception as exc:
                _log.warning("[plants] rustfs purge-preview count failed: %s", exc)
        return n
    except Exception as exc:
        _log.warning("[plants] rustfs count(%s) failed: %s", plant_code_id, exc)
        return 0


def _purge_rustfs(plant_code_id: str) -> dict:
    """Recursively delete this plant's RustFS prefixes. Best-effort."""
    try:
        from .services.doc_types import _get_s3fs

        sfs = _get_s3fs()
        removed = 0
        for pref in _plant_rustfs_prefixes(plant_code_id):
            try:
                if sfs.exists(pref):
                    files = sfs.find(pref)
                    removed += len(files)
                    sfs.rm(pref, recursive=True)
            except Exception as exc:
                _log.warning("[plants] rustfs rm %s failed: %s", pref, exc)
        return {"objects_removed": removed}
    except Exception as exc:
        _log.warning("[plants] rustfs purge(%s) failed: %s", plant_code_id, exc)
        return {"error": f"{type(exc).__name__}: {exc!s:.200}"}


def purge_preview(plant_code_id: str) -> dict:
    """Dry-run: count what a purge would delete, per store. Changes nothing."""
    plant_code_id = plant_code_id.strip()
    db_name = registered_plant_db_name(plant_code_id)
    if db_name is None:
        raise HTTPException(
            HTTPStatus.NOT_FOUND, f"plant_code_id '{plant_code_id}' not found"
        )
    postgres: dict[str, int] = {}
    try:
        conn = _connect_plant_db(db_name)
        try:
            cur = conn.cursor()
            for tname in _plant_code_tables(cur):
                cur.execute(
                    f'SELECT count(*) FROM "{tname}" WHERE plant_code_id = %s',
                    (plant_code_id,),
                )
                cnt = cur.fetchone()[0]
                if cnt:
                    postgres[tname] = cnt
        finally:
            conn.close()
    except Exception as exc:
        _log.warning("[plants] purge preview count(%s) failed: %s", plant_code_id, exc)
    return {
        "plant_code_id": plant_code_id,
        "postgres": postgres,
        "postgres_total": sum(postgres.values()),
        "iotdb_series": _iotdb_series_count(plant_code_id),
        "rustfs_objects": _rustfs_object_count(plant_code_id),
    }


def purge_plant(plant_code_id: str) -> dict:
    """Delete the plant and ALL of its data across Postgres, IoTDB and RustFS."""
    plant_code_id = plant_code_id.strip()
    if plant_code_id == "Plant_1":
        raise HTTPException(HTTPStatus.BAD_REQUEST, "refusing to purge the default seed plant 'Plant_1'")
    _assert_has_dedicated_database(plant_code_id)

    database_result = _drop_plant_databases(plant_code_id)
    pg_result: dict
    if "error" in database_result:
        pg_result = {"error": database_result["error"]}
        _log.warning(
            "[plants] PURGE postgres %s failed: %s", plant_code_id, database_result["error"]
        )
    else:
        _unregister_plant(plant_code_id)
        pg_result = {"database_dropped": database_result.get("dropped", [])}

    iotdb_result = _purge_iotdb(plant_code_id)
    rustfs_result = _purge_rustfs(plant_code_id)

    _log.info(
        "[plants] PURGE complete %s: pg=%s iotdb=%s rustfs=%s",
        plant_code_id,
        pg_result,
        iotdb_result,
        rustfs_result,
    )
    return {
        "status": "purged",
        "plant_code_id": plant_code_id,
        "postgres": pg_result,
        "iotdb": iotdb_result,
        "rustfs": rustfs_result,
        "database": database_result,
    }


_PLANT_DB_BASE = _cdm_db_base()

_MAX_PLANT_SUFFIX = 63 - (len(_PLANT_DB_BASE) + 1)

_REGISTRY_DB = _registry_db_name()

_RESERVED_DBS = frozenset(
    {
        "postgres",
        "template0",
        "template1",
        _REGISTRY_DB,
    }
    | {
        f"{_PLANT_DB_BASE}_{svc}"
        for svc in (
            "cdm",
            "registry",
            "cdm_copy",
            "cdm_dev",
            "cdm_pt",
            "cdm_test",
            "app",
            "auth",
            "dashboard",
            "decisionmodel",
            "kg",
            "mlflow",
            "modelfactory",
            "scenario",
            "users",
        )
    }
)


def _normalize_plant_suffix(plant_code_id: str) -> str:
    """Plant code → Postgres-safe suffix ('Plant 1' → 'plant_1'). 400 if empty or too long."""
    suffix = re.sub(r"[^a-z0-9]+", "_", (plant_code_id or "").lower()).strip("_")
    if not suffix:
        raise HTTPException(
            400,
            f"plant_code_id '{plant_code_id}' yields no usable database-name characters",
        )
    if len(suffix) > _MAX_PLANT_SUFFIX:
        raise HTTPException(
            400,
            f"plant_code_id '{plant_code_id}' too long — normalised suffix "
            f"'{suffix}' exceeds {_MAX_PLANT_SUFFIX} characters",
        )
    return suffix


def plant_db_name(plant_code_id: str) -> str:
    """The database name a NEW plant would be provisioned under (use registered_plant_db_name for existing)."""
    return f"{_PLANT_DB_BASE}_{_normalize_plant_suffix(plant_code_id)}"


_existing_plant_dbs: set[str] = set()


def plant_db_exists(plant_code_id: str) -> bool:
    """Whether a database already sits at this plant's name — asks Postgres, the guard against adopting others' data."""
    try:
        name = plant_db_name(plant_code_id)
    except HTTPException:
        return False
    if name in _existing_plant_dbs:
        return True
    try:
        mconn = _maintenance_conn()
        try:
            cur = mconn.cursor()
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,))
            exists = cur.fetchone() is not None
        finally:
            mconn.close()
    except Exception:
        return False
    if exists:
        _existing_plant_dbs.add(name)
    return exists


def _maintenance_conn():
    """Autocommit connection to the 'postgres' db, for CREATE/DROP DATABASE (can't run in a txn)."""
    conn = _open_db("postgres", connect_timeout=10)
    conn.autocommit = True
    return conn


def _database_exists(cur, name: str) -> bool:
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,))
    return cur.fetchone() is not None


def _build_create_table_sql(table: str) -> str | None:
    """CREATE TABLE IF NOT EXISTS SQL from schema.yaml, or None if absent/unreadable."""

    try:
        with open(SCHEMA_FROZEN_FILE) as f:
            doc = yaml.safe_load(f) or {}
        tdef = (doc.get("schema") or {}).get(table, {})
        cols = tdef.get("columns") or {}
        pk = tdef.get("primary_key")
        if not cols:
            return None
        col_defs = []
        for name, sql_type in cols.items():
            suffix = " PRIMARY KEY" if name == pk else ""
            if name == "created_at":
                suffix += " DEFAULT now()"
            col_defs.append(f'"{name}" {sql_type}{suffix}')
        return f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(col_defs)})"
    except Exception as exc:
        _log.warning("[plants] YAML read for %s failed: %s", table, exc)
        return None


def _add_missing_columns(cur, conn, table: str, columns: dict) -> int:
    """Bring an existing table up to schema.yaml — CREATE TABLE IF NOT EXISTS is a no-op."""
    if not columns:
        return 0
    try:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = %s",
            (table,),
        )
        present = {r[0] for r in cur.fetchall()}
    except Exception as exc:
        conn.rollback()
        _log.warning("[plants] column introspection for %s skipped: %s", table, exc)
        return 0

    if not present:
        return 0

    added = 0
    for name, sql_type in columns.items():
        if name in present:
            continue
        declared = str(sql_type).strip()
        if declared.upper().startswith(("BIGSERIAL", "SERIAL")):
            continue
        try:
            cur.execute(f'ALTER TABLE {table} ADD COLUMN "{name}" {declared}')
            conn.commit()
            added += 1
            _log.info("[plants] added column %s.%s", table, name)
        except Exception as exc:
            conn.rollback()
            _log.warning("[plants] add column %s.%s skipped: %s", table, name, exc)
    return added


def _backfill_flow_category(cur, conn, table: str) -> int:
    """Rows predating the category column would otherwise re-insert instead of updating."""
    if table != "flow_state":
        return 0
    try:
        from .services.flow import UNCATEGORISED

        cur.execute("SELECT flow_uid, file_type FROM flow_state WHERE category IS NULL")
        rows = cur.fetchall()
        if not rows:
            return 0
        from .services.taxonomy import canonical_category

        for flow_uid, file_type in rows:
            cur.execute(
                "UPDATE flow_state SET category = %s WHERE flow_uid = %s",
                (canonical_category(file_type) or UNCATEGORISED, flow_uid),
            )
        conn.commit()
        _log.info("[plants] backfilled category on %d flow_state row(s)", len(rows))
        return len(rows)
    except Exception as exc:
        conn.rollback()
        _log.warning("[plants] flow_state category backfill skipped: %s", exc)
        return 0


def _cdm_schema_tables() -> list[str]:
    """Every CDM table name declared in schema.yaml, with ``plants`` first so"""

    try:
        with open(SCHEMA_FROZEN_FILE) as f:
            doc = yaml.safe_load(f) or {}
        tables = list((doc.get("schema") or {}).keys())
        tables.sort(key=lambda t: (t != "plants", t))
        return tables
    except Exception as exc:
        _log.warning("[plants] schema table enumeration failed: %s", exc)
        return ["plants"]


def _schema_doc() -> dict:
    """Parsed schema.yaml (``schema`` mapping), or empty on read failure."""
    try:
        with open(SCHEMA_FROZEN_FILE) as f:
            return (yaml.safe_load(f) or {}).get("schema") or {}
    except Exception as exc:
        _log.warning("[plants] schema.yaml read failed: %s", exc)
        return {}


def _apply_cdm_schema(conn) -> None:
    """Materialise every schema.yaml table, unique, index and plant FK — the single complete initialisation."""
    cur = conn.cursor()
    tables = _cdm_schema_tables()
    schema = _schema_doc()

    for table in tables:
        sql = _build_create_table_sql(table)
        if sql:
            cur.execute(sql)
        else:
            _log.warning("[plants] no YAML definition for %s — skipped", table)
    conn.commit()

    for table in tables:
        _add_missing_columns(cur, conn, table, (schema.get(table) or {}).get("columns") or {})

    for table in tables:
        tdef = schema.get(table) or {}
        for stale in tdef.get("superseded_indexes") or []:
            try:
                cur.execute(f"DROP INDEX IF EXISTS {stale}")
                conn.commit()
                _log.info("[plants] dropped superseded index %s on %s", stale, table)
            except Exception as exc:
                conn.rollback()
                _log.warning("[plants] dropping %s on %s skipped: %s", stale, table, exc)
        uniques = list(tdef.get("unique") or [])
        uniques += [
            c for c in (tdef.get("constraints") or []) if c.get("type") == "unique"
        ]
        for uq in uniques:
            cols = ", ".join(f'"{c}"' for c in uq["columns"])
            try:
                cur.execute(
                    f'CREATE UNIQUE INDEX IF NOT EXISTS {uq["name"]} ON {table} ({cols})'
                )
                conn.commit()
            except Exception as exc:
                conn.rollback()
                _log.warning("[plants] unique %s on %s skipped: %s", uq["name"], table, exc)
        for ix in tdef.get("indexes") or []:
            cols = ", ".join(f'"{c}"' for c in ix["columns"])
            try:
                cur.execute(
                    f'CREATE INDEX IF NOT EXISTS {ix["name"]} ON {table} ({cols})'
                )
                conn.commit()
            except Exception as exc:
                conn.rollback()
                _log.warning("[plants] index %s on %s skipped: %s", ix["name"], table, exc)

    for t in tables:
        if t == "plants":
            continue
        fk = f"fk_{t}_plant"
        try:
            cur.execute(
                "SELECT EXISTS(SELECT FROM information_schema.columns "
                "WHERE table_name=%s AND column_name='plant_code_id')",
                (t,),
            )
            if not cur.fetchone()[0]:
                continue
            cur.execute(
                "SELECT EXISTS(SELECT FROM pg_constraint WHERE conname=%s)", (fk,)
            )
            if cur.fetchone()[0]:
                continue
            cur.execute(
                f'ALTER TABLE "{t}" ADD CONSTRAINT "{fk}" '
                f"FOREIGN KEY (plant_code_id) REFERENCES plants(plant_code_id) ON DELETE RESTRICT"
            )
            conn.commit()
        except Exception as exc:
            conn.rollback()
            _log.warning("[plants] FK on %s in per-plant DB skipped: %s", t, exc)


def _provision_plant_databases(
    plant_code_id: str,
    label: str | None = None,
    industry: str | None = None,
    created_by: str | None = None,
) -> None:
    """Create the plant's dedicated database and materialise the full CDM schema. Idempotent."""

    db_name = plant_db_name(plant_code_id)
    if db_name in _RESERVED_DBS:
        raise HTTPException(
            400,
            f"plant_code_id '{plant_code_id}' maps to reserved database name "
            f"'{db_name}'",
        )

    mconn = _maintenance_conn()
    try:
        cur = mconn.cursor()
        if not _database_exists(cur, db_name):
            cur.execute(
                f'CREATE DATABASE "{db_name}" TEMPLATE template0 OWNER "{CDM_DB_USER}"'
            )
            _log.info("[plants] created database: %s", db_name)
    finally:
        mconn.close()

    new_conn = _connect_plant_db(db_name)
    try:
        _apply_cdm_schema(new_conn)
        cur = new_conn.cursor()
        cur.execute(
            "INSERT INTO plants (plant_code_id, label, industry, db_name, created_by) "
            "VALUES (%s, %s, %s, %s, %s) ON CONFLICT (plant_code_id) DO NOTHING",
            (plant_code_id, label or plant_code_id, industry, db_name, created_by),
        )
        new_conn.commit()
        _log.info("[plants] CDM schema applied + plant row seeded in %s", db_name)
    finally:
        new_conn.close()

    _existing_plant_dbs.add(db_name)


def _drop_plant_databases(plant_code_id: str) -> dict:
    """DROP the plant's database (WITH FORCE). Best-effort; never drops a reserved db."""
    db_name = registered_plant_db_name(plant_code_id)
    if db_name is None:
        try:
            db_name = plant_db_name(plant_code_id)
        except HTTPException:
            return {"skipped": "plant_code_id does not yield a valid database suffix"}

    _existing_plant_dbs.discard(db_name)

    if db_name in _RESERVED_DBS:
        return {"skipped": [db_name]}
    try:
        mconn = _maintenance_conn()
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc!s:.200}"}
    try:
        cur = mconn.cursor()
        cur.execute(f'DROP DATABASE IF EXISTS "{db_name}" WITH (FORCE)')
        _log.info("[plants] dropped database: %s", db_name)
        return {"dropped": [db_name]}
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc!s:.200}"}
    finally:
        mconn.close()


def migrate_plant_schema(plant_code_id: str) -> int:
    """Add columns and rebuild renamed indexes on an existing plant. Deltas only."""
    schema = _schema_doc()
    changes = 0
    try:
        conn = _get_conn(plant_code_id)
        try:
            cur = conn.cursor()
            for table in _cdm_schema_tables():
                tdef = schema.get(table) or {}
                changes += _add_missing_columns(cur, conn, table, tdef.get("columns") or {})
                changes += _backfill_flow_category(cur, conn, table)
                for stale in tdef.get("superseded_indexes") or []:
                    try:
                        cur.execute(f"DROP INDEX IF EXISTS {stale}")
                        conn.commit()
                    except Exception:
                        conn.rollback()
                for uq in tdef.get("unique") or []:
                    cols = ", ".join(f'"{c}"' for c in uq["columns"])
                    try:
                        cur.execute(
                            f'CREATE UNIQUE INDEX IF NOT EXISTS {uq["name"]} ON {table} ({cols})'
                        )
                        conn.commit()
                    except Exception as exc:
                        conn.rollback()
                        _log.warning(
                            "[plants] unique %s on %s skipped: %s", uq["name"], table, exc
                        )
            return changes
        finally:
            conn.close()
    except Exception as exc:
        _log.warning("[plants] schema migration for %s skipped: %s", plant_code_id, exc)
        return changes


_migration_done = False


def migrate_all_plants(force: bool = False) -> int:
    """Run the schema migration across every registered plant, once per process."""
    global _migration_done
    if _migration_done and not force:
        return 0
    _migration_done = True
    changed = 0
    try:
        for plant in list_plants() or []:
            code = plant.get("plant_code_id") if isinstance(plant, dict) else None
            if code:
                changed += migrate_plant_schema(code)
    except Exception as exc:
        _log.warning("[plants] schema migration sweep skipped: %s", exc)
    return changed
