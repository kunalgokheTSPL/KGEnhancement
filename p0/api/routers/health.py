"""Health and readiness probe endpoints."""

from __future__ import annotations

from http import HTTPStatus

from fastapi import APIRouter

from ..config import (
    CDM_CONFIG_FILE,
    CDM_DB_PASS,
    CONFIG_DIR,
    OUT_DIR,
    SAP_EXTRACTION_FILE,
    SAP_RENAME_FILE,
)
from ..database.connection import _get_conn
from ..responses import EnvelopeRoute

router = APIRouter(route_class=EnvelopeRoute, tags=["Health"])


@router.get(
    "/health",
    summary="Health check",
    description="Liveness probe — returns config directory status and database connectivity.",
    status_code=HTTPStatus.OK,
)
def health():
    result = {
        "status": "ok",
        "config_dir": str(CONFIG_DIR),
        "configs_present": {
            "sap_extraction": SAP_EXTRACTION_FILE.exists(),
            "sap_column_rename": SAP_RENAME_FILE.exists(),
            "cdm_config": CDM_CONFIG_FILE.exists(),
        },
        "output_dir": str(OUT_DIR),
    }

    if CDM_DB_PASS:
        try:
            conn = _get_conn(registry=True)
            conn.close()
            result["database"] = "connected"
        except Exception as e:
            result["database"] = f"error: {e}"
    else:
        result["database"] = "not configured"

    return {
        "success": True,
        "message": "Health check passed",
        "data": result,
    }
