"""Database connection routing for p0: registry-scoped or plant-scoped."""

from __future__ import annotations

from http import HTTPStatus

from fastapi import HTTPException

from p0.driver import PostgresDriver

from ..audit_context import get_plant
from ..config import (
    CDM_DB_HOST,
    CDM_DB_PASS,
    CDM_DB_PORT,
    CDM_DB_SSLMODE,
    CDM_DB_USER,
)
from ..responses import as_envelope_exc

_bootstrapping_registry = False


def cdm_db_base() -> str:
    """The prefix every per-plant database shares — plants are ``decisionops_<plant>``."""
    return "decisionops"


def cdm_db_name() -> str:
    """The legacy shared CDM database name, ``decisionops_cdm``."""
    return f"{cdm_db_base()}_cdm"


def registry_db_name() -> str:
    """The database holding the plant registry — its own db, separate from any plant's."""
    return "plant_registry"


def open_db(dbname: str, connect_timeout: int = 10):
    """Open a connection to a specific database using p0's deployment-aware CDM host/creds."""
    driver = PostgresDriver()
    driver.config.update(
        host=CDM_DB_HOST,
        user=CDM_DB_USER,
        password=CDM_DB_PASS,
        port=CDM_DB_PORT,
        database=dbname,
    )
    if CDM_DB_SSLMODE:
        driver.config["sslmode"] = CDM_DB_SSLMODE
    return driver.connect(connect_timeout=connect_timeout)


def _get_conn(plant_code_id: str | None = None, registry: bool = False):
    """Get a PostgreSQL connection routed to the right database."""
    if CDM_DB_PASS is None:
        raise HTTPException(
            HTTPStatus.SERVICE_UNAVAILABLE,
            "PostgreSQL not configured. Set CDM_DB_PASS.",
        )
    try:
        if registry:
            try:
                return open_db(registry_db_name())
            except Exception as exc:
                global _bootstrapping_registry
                if _bootstrapping_registry or "does not exist" not in str(exc).lower():
                    raise
                _bootstrapping_registry = True
                try:
                    from ..plants import ensure_plants_table

                    ensure_plants_table()
                    return open_db(registry_db_name())
                finally:
                    _bootstrapping_registry = False

        pc = plant_code_id
        if pc is None:
            pc = get_plant()
        if not pc:
            raise HTTPException(
                HTTPStatus.BAD_REQUEST, "plant_code_id is required for this operation"
            )
        from ..plants import registered_plant_db_name

        dbname = registered_plant_db_name(pc)
        if not dbname:
            raise HTTPException(
                HTTPStatus.BAD_REQUEST, f"plant_code_id '{pc}' is not registered"
            )
        return open_db(dbname)
    except HTTPException as e:
        raise as_envelope_exc(e)
    except ImportError:
        raise HTTPException(HTTPStatus.INTERNAL_SERVER_ERROR, "psycopg2 not installed")
    except Exception as e:
        raise HTTPException(HTTPStatus.BAD_GATEWAY, f"Database connection error: {e}")
