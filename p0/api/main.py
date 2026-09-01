"""p0 CDM Admin API — router assembly and startup checks."""

from __future__ import annotations

import functools
import logging
import os

import fastapi.applications
import fastapi.openapi.utils
from fastapi import APIRouter

from .routers import (
    config_router,
    connectors,
    context,
    data,
    flow,
    health,
    integrated,
    logs,
)

from .config import LOG_DIR
from .logging_config import setup_logging

setup_logging(LOG_DIR)

_log = logging.getLogger("cdm.api.main")

from .routers import pipeline

_log.info("CDM API starting — log directory: %s", LOG_DIR)

try:
    from p0.utils.fs import ensure_bucket
    from p0.driver import object_store_container

    _container = object_store_container()
    if ensure_bucket(_container):
        _log.info("Object-store container ready: %s", _container)
    else:
        _log.warning(
            "Object-store container '%s' could not be auto-created — pipeline "
            "writes may fail. Check endpoint reachability / credentials, or "
            "create the container manually.",
            _container,
        )
except Exception as _exc:
    _log.warning("Container ensure step skipped: %s", _exc)

try:
    from .plants import ensure_plants_table, sync_registry_from_plant_databases

    ensure_plants_table()
    sync_registry_from_plant_databases()
    _log.info("plants registry ready")
except Exception as _exc:
    _log.warning("plants registry init skipped: %s", _exc)

def _skip_startup_migration() -> bool:
    """Whether the startup schema migration is disabled by environment."""
    return (os.getenv("P0_SKIP_STARTUP_MIGRATION") or "").strip().lower() in (
        "1",
        "true",
        "yes",
    )


try:
    if _skip_startup_migration():
        _log.warning(
            "startup schema migration SKIPPED by P0_SKIP_STARTUP_MIGRATION; "
            "existing plant databases keep their current schema"
        )
    else:
        from .plants import migrate_all_plants

        _migrated = migrate_all_plants()
        if _migrated:
            _log.info("schema migration applied to %d plant database(s)", _migrated)
except Exception as _exc:
    _log.warning("plant schema migration skipped: %s", _exc)

try:
    from .plants import list_plants as _list_plants
    from .routers.pipeline import reconcile_interrupted_jobs

    _repaired = 0
    for _plant in _list_plants() or []:
        _code = _plant.get("plant_code_id") if isinstance(_plant, dict) else None
        if _code:
            _repaired += reconcile_interrupted_jobs(_code)
    if _repaired:
        _log.info("marked %d interrupted pipeline job(s) after restart", _repaired)
except Exception as _exc:
    _log.warning("pipeline job reconciliation skipped: %s", _exc)



from fastapi import Depends
from .audit_context import request_context_dep

# RBAC moved to unified middleware in app.py
# from .deps_auth import require_admin

router = APIRouter(
    prefix="/p0/cdm",
    dependencies=[Depends(request_context_dep)],
)

# _admin_only = [Depends(require_admin)]

router.include_router(
    health.router,
)
# router.include_router(config_router.router, dependencies=_admin_only)
# router.include_router(pipeline.router, dependencies=_admin_only)
# router.include_router(connectors.router, dependencies=_admin_only)
# router.include_router(data.router, dependencies=_admin_only)
# router.include_router(context.router, dependencies=_admin_only)
# router.include_router(flow.router, dependencies=_admin_only)
# router.include_router(logs.router, dependencies=_admin_only)
router.include_router(config_router.router)
router.include_router(pipeline.router)
router.include_router(connectors.router)
router.include_router(data.router)
router.include_router(context.router)
router.include_router(integrated.router)
router.include_router(flow.router)
router.include_router(logs.router)


from fastapi import Request
from fastapi.responses import RedirectResponse


@router.api_route(
    "/api/pnid/{path:path}", methods=["GET", "POST"], include_in_schema=False
)
async def pnid_compat(path: str, request: Request):
    """Redirect legacy /api/pnid/* → /api/connectors/pnid/*"""
    new_url = f"/api/connectors/pnid/{path}"
    if request.query_params:
        new_url += f"?{request.query_params}"
    return RedirectResponse(url=new_url, status_code=307)


@router.api_route(
    "/api/documents/{path:path}", methods=["GET", "POST"], include_in_schema=False
)
async def documents_compat(path: str, request: Request):
    """Redirect legacy /api/documents/* → /api/connectors/documents/*"""
    new_url = f"/api/connectors/documents/{path}"
    if request.query_params:
        new_url += f"?{request.query_params}"
    return RedirectResponse(url=new_url, status_code=307)


def _rewrite_binary_file_fields(node) -> None:
    if isinstance(node, dict):
        if (
            node.get("type") == "string"
            and node.get("contentMediaType") == "application/octet-stream"
            and "format" not in node
        ):
            node.pop("contentMediaType", None)
            node["format"] = "binary"
        for value in node.values():
            _rewrite_binary_file_fields(value)
    elif isinstance(node, list):
        for item in node:
            _rewrite_binary_file_fields(item)


def _install_binary_file_schema_fix() -> None:
    """Wrap get_openapi once (both binding sites) so every schema renders file pickers. Idempotent."""
    if getattr(fastapi.openapi.utils.get_openapi, "_binary_file_fix", False):
        return

    _original = fastapi.openapi.utils.get_openapi

    @functools.wraps(_original)
    def _get_openapi_with_binary_files(*args, **kwargs):
        schema = _original(*args, **kwargs)
        _rewrite_binary_file_fields(schema)
        return schema

    _get_openapi_with_binary_files._binary_file_fix = True
    fastapi.openapi.utils.get_openapi = _get_openapi_with_binary_files
    fastapi.applications.get_openapi = _get_openapi_with_binary_files


_install_binary_file_schema_fix()
