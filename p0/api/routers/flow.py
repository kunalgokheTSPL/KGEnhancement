"""Per-file flow state endpoints — the backend replacement for browser localStorage."""

from __future__ import annotations

import logging

from http import HTTPStatus

from fastapi import APIRouter, Body, HTTPException, Query
from fastapi.responses import JSONResponse

from ..audit_context import get_actor
from ..services import flow as flow_store
from ..services.connector_state import (
    _PROCESSED_STAGES,
    FLOWS,
    FLOWS_BY_CONNECTOR,
)
from ..plants import validate_plant_code as _validate_plant_code
from ..responses import (
    EnvelopeRoute,
    as_envelope_exc,
    paginated_envelope,
    server_error,
)

router = APIRouter(route_class=EnvelopeRoute, prefix="/flow", tags=["Flow"])

_log = logging.getLogger("cdm.api.flow")


_LIFECYCLE = ("pending", "processed", "failed", "all")


def _matches_status(row: dict, wanted: str) -> bool:
    """Server-side processed/unprocessed split, so the UI never derives it."""
    stage = row.get("stage")
    status = row.get("status")
    if wanted == "failed":
        return status == "failed"
    if wanted == "processed":
        return status != "failed" and stage in _PROCESSED_STAGES
    if wanted == "pending":
        return status != "failed" and stage not in _PROCESSED_STAGES
    return True


def _resolve_flow(raw: str) -> tuple[str | None, dict | None]:
    """Canonical flow_state name for a spelling, or the error that names the alternatives."""
    key = raw.strip().lower()
    if not key:
        return None, {"field": "flow", "message": "flow cannot be empty if provided"}
    if key in FLOWS:
        return key, None
    if key in FLOWS_BY_CONNECTOR:
        named = " or ".join(f"flow={f}" for f in FLOWS_BY_CONNECTOR[key])
        return None, {
            "field": "flow",
            "message": (
                f"'{key}' is a connector name, not a flow. Use {named}, "
                f"or call /connectors/{key}/state instead."
            ),
        }
    return None, {
        "field": "flow",
        "message": f"flow must be one of {', '.join(FLOWS)}",
    }


def _resolve_lifecycle(
    lifecycle: str | None, status: str | None
) -> tuple[str, str, dict | None]:
    """The wanted lifecycle, the field it came from, and any conflict between the two spellings."""
    new = (lifecycle or "").strip().lower()
    old = (status or "").strip().lower()
    if new and old and new != old:
        return new, "lifecycle", {
            "field": "lifecycle",
            "message": (
                f"lifecycle='{new}' and the deprecated status='{old}' disagree. "
                "Send only lifecycle."
            ),
        }
    if old and not new:
        _log.info("[flow] deprecated 'status' query param used — prefer 'lifecycle'")
        return old, "status", None
    return new or "all", "lifecycle", None



@router.get("/state", summary="List flow state for a plant")
def listState(
    plant_code_id: str = Query(..., description="Plant code ID"),
    flow: str | None = Query(
        None, description=f"Filter by flow: {' | '.join(FLOWS)}"
    ),
    lifecycle: str | None = Query(
        None, description=f"Filter by lifecycle: {' | '.join(_LIFECYCLE)}"
    ),
    status: str | None = Query(
        None,
        deprecated=True,
        description="Deprecated alias for lifecycle. Not the same vocabulary as the status field on a row.",
    ),
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=1000),
):
    """One row per file. Drives the UI button-gating and the reprocess modal."""

    errors = []
    plant_code_id_val = (plant_code_id or "").strip()
    if not plant_code_id_val:
        errors.append({"field": "plant_code_id", "message": "plant_code_id is required"})
    if flow is not None:
        flow, flow_error = _resolve_flow(flow)
        if flow_error:
            errors.append(flow_error)

    wanted, field, alias_error = _resolve_lifecycle(lifecycle, status)
    if alias_error:
        errors.append(alias_error)
    elif wanted not in _LIFECYCLE:
        errors.append(
            {
                "field": field,
                "message": f"{field} must be one of {', '.join(_LIFECYCLE)}",
            }
        )

    if errors:
        return JSONResponse(
            status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": errors
            }
        )

    plant_code_id = plant_code_id_val

    try:
        _validate_plant_code(plant_code_id)
        rows = flow_store.list_flow(plant_code_id, flow, limit=100000)
        if wanted != "all":
            rows = [r for r in rows if _matches_status(r, wanted)]
        res = {
            "plant_code_id": plant_code_id,
            "flow": flow,
            "lifecycle": wanted,
            "status": wanted,
            "files": rows,
        }
        scope = f" in the '{flow}' flow" if flow else ""
        qualifier = "" if wanted == "all" else f" at lifecycle '{wanted}'"
        return paginated_envelope(
            res,
            items_key="files",
            page=page,
            page_size=page_size,
            message="Flow state list fetched successfully",
            empty_message=(
                f"No files{qualifier} for plant '{plant_code_id}'{scope}."
            ),
        )
    except HTTPException as e:
        raise as_envelope_exc(e)
    except Exception as exc:
        return server_error(exc, action="listing state")


@router.get("/state/{flowUid}", summary="Get one file's flow lifecycle")
def getState(
    flowUid: int,
    plant_code_id: str = Query(..., description="Plant code ID"),
):

    errors = []
    plant_code_id_val = (plant_code_id or "").strip()
    if not plant_code_id_val:
        errors.append({"field": "plant_code_id", "message": "plant_code_id is required"})
    if flowUid <= 0:
        errors.append({"field": "flowUid", "message": "flowUid must be a positive integer"})

    if errors:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": errors
            }
        )

    plant_code_id = plant_code_id_val
    flow_uid = flowUid

    try:
        _validate_plant_code(plant_code_id)
        if flow_uid <= 0:
            raise HTTPException(
                status_code=HTTPStatus.BAD_REQUEST,
                detail={
                    "success": False,
                    "message": "flowUid must be a positive integer",
                    "data": None,
                },
            )
        row = flow_store.get(flow_uid, plant_code_id)
        if row is None:
            raise HTTPException(
                status_code=HTTPStatus.NOT_FOUND,
                detail={
                    "success": False,
                    "message": "flow_uid not found",
                    "data": None,
                },
            )
        return {
            "success": True,
            "message": "Flow state fetched successfully",
            "data": row,
        }
    except HTTPException as e:
        raise as_envelope_exc(e)
    except Exception as exc:
        return server_error(exc, action="loading state")


@router.post(
    "/upsert",
    summary="Create or advance a file's flow row (admin / recovery only)",
    description="Admin/recovery only — the backend maintains flow_state itself; clients never need this.",
)
def upsertState(
    input_data: dict = Body(...),
):
    """Identity is (plant_code_id, flow, content_hash). Non-null fields advance"""

    errors = []
    plant_code_id = input_data.get("plant_code_id")
    plant_code_id_val = (plant_code_id or "").strip()
    if not plant_code_id_val:
        errors.append({"field": "plant_code_id", "message": "plant_code_id is required"})

    flow = input_data.get("flow")
    flow_val = (flow or "").strip() if flow else ""
    if not flow_val:
        errors.append({"field": "flow", "message": "flow cannot be empty"})

    content_hash = input_data.get("content_hash")
    content_hash_val = (content_hash or "").strip() if content_hash else ""
    if not content_hash_val:
        errors.append({"field": "content_hash", "message": "content_hash cannot be empty"})

    if errors:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": errors
            }
        )

    plant_code_id = plant_code_id_val
    flow = flow_val
    content_hash = content_hash_val

    try:
        _validate_plant_code(plant_code_id)
        fields = {
            k: v
            for k, v in input_data.items()
            if k not in ("plant_code_id", "flow", "content_hash")
        }
        trimmed_fields = {}
        for k, v in fields.items():
            if isinstance(v, str):
                trimmed_fields[k] = v.strip()
            else:
                trimmed_fields[k] = v

        trimmed_fields.setdefault("created_by", get_actor())
        row = flow_store.upsert_file(plant_code_id, flow, content_hash, **trimmed_fields)
        if row is None:
            raise HTTPException(
                status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
                detail={
                    "success": False,
                    "message": "flow upsert failed",
                    "data": None,
                },
            )
        return {
            "success": True,
            "message": "Flow state upserted successfully",
            "data": row,
        }
    except HTTPException as e:
        raise as_envelope_exc(e)
    except Exception as exc:
        return server_error(exc, action="saving state")


@router.post("/{flowUid}/reset", summary="Reset a file to 'uploaded' for reprocess")
def resetState(
    flowUid: int,
    input_data: dict = Body(...),
):

    errors = []
    plant_code_id = input_data.get("plant_code_id")
    plant_code_id_val = (plant_code_id or "").strip()
    if not plant_code_id_val:
        errors.append({"field": "plant_code_id", "message": "plant_code_id is required"})
    if flowUid <= 0:
        errors.append({"field": "flowUid", "message": "flowUid must be a positive integer"})

    if errors:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": errors
            }
        )

    plant_code_id = plant_code_id_val
    flow_uid = flowUid

    try:
        _validate_plant_code(plant_code_id)
        if flow_uid <= 0:
            raise HTTPException(
                status_code=HTTPStatus.BAD_REQUEST,
                detail={
                    "success": False,
                    "message": "flowUid must be a positive integer",
                    "data": None,
                },
            )
        row = flow_store.reset(flow_uid, plant_code_id)
        if row is None:
            raise HTTPException(
                status_code=HTTPStatus.NOT_FOUND,
                detail={
                    "success": False,
                    "message": "flow_uid not found",
                    "data": None,
                },
            )
        return {
            "success": True,
            "message": "Flow state reset successfully",
            "data": row,
        }
    except HTTPException as e:
        raise as_envelope_exc(e)
    except Exception as exc:
        return server_error(exc, action="resetting state")


@router.delete("/{flowUid}", summary="Remove a file from the flow")
def deleteState(
    flowUid: int,
    plant_code_id: str = Query(..., description="Plant code ID"),
):

    errors = []
    plant_code_id_val = (plant_code_id or "").strip()
    if not plant_code_id_val:
        errors.append({"field": "plant_code_id", "message": "plant_code_id is required"})
    if flowUid <= 0:
        errors.append({"field": "flowUid", "message": "flowUid must be a positive integer"})

    if errors:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": errors
            }
        )

    plant_code_id = plant_code_id_val
    flow_uid = flowUid

    try:
        _validate_plant_code(plant_code_id)
        if flow_uid <= 0:
            raise HTTPException(
                status_code=HTTPStatus.BAD_REQUEST,
                detail={
                    "success": False,
                    "message": "flowUid must be a positive integer",
                    "data": None,
                },
            )
        ok = flow_store.delete(flow_uid, plant_code_id)
        if not ok:
            raise HTTPException(
                status_code=HTTPStatus.NOT_FOUND,
                detail={
                    "success": False,
                    "message": "flow_uid not found",
                    "data": None,
                },
            )
        return {
            "success": True,
            "message": "Flow state deleted successfully",
            "data": {"deleted": flow_uid},
        }
    except HTTPException as e:
        raise as_envelope_exc(e)
    except Exception as exc:
        return server_error(exc, action="deleting state")
