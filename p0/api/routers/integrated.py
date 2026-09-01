"""Label-wise integrated view of everything the user has processed."""

from __future__ import annotations

import logging
from http import HTTPStatus

from fastapi import APIRouter, Body, HTTPException, Query
from fastapi.responses import JSONResponse

from ..plants import validate_plant_code as _validate_plant_code
from ..responses import (
    EnvelopeRoute,
    as_envelope_exc,
    no_content,
    paginated_envelope,
    server_error,
)
from ..services import integrated_view as _view
from ..services import labels as _labels

router = APIRouter(route_class=EnvelopeRoute, prefix="/integrated", tags=["Integrated View"])

_log = logging.getLogger("cdm.api.integrated")


def _unknown_label(label: str):
    known = ", ".join(_view.known_labels())
    return JSONResponse(
        status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
        content={
            "success": False,
            "message": f"'{label}' is not a valid label. Use one of: {known}.",
            "errors": [{"field": "label", "message": "Unknown label."}],
        },
    )


@router.get(
    "/labels",
    summary="Label tabs with their row counts",
    description="The four use-case labels, each with how many rows the integrated view "
    "currently holds for this plant — enough to render the tab strip and its badges "
    "without fetching any rows.",
)
def listIntegratedLabels(plant_code_id: str = Query(..., description="Plant code ID")):
    try:
        _validate_plant_code(plant_code_id)
        totals = _view.counts(plant_code_id)
        return {
            "success": True,
            "message": "Integrated view labels retrieved successfully",
            "data": {
                "plant_code_id": plant_code_id,
                "labels": [
                    dict(entry, row_count=totals.get(entry["key"], 0))
                    for entry in _labels.catalogue()
                    if entry["key"] in _view.known_labels()
                ],
                "total": sum(totals.values()),
            },
        }
    except HTTPException as e:
        raise as_envelope_exc(e)
    except Exception as exc:
        return server_error(exc, action="counting integrated view rows")


@router.get(
    "/columns",
    summary="Column plan for one label",
    description="The 6-layer column list for a label — every field with whether the CDM can "
    "back it yet. Fields marked `not_yet_backed` always come back as null in /view, so the "
    "table shape is stable; grey them out rather than treating them as missing data.",
)
def getIntegratedColumns(label: str = Query(..., description="integrity | reliability | maintenance | production")):
    key = str(label or "").strip().lower()
    if key not in _view.known_labels():
        return _unknown_label(label)
    return {
        "success": True,
        "message": f"Integrated {key} columns retrieved successfully",
        "data": _view.describe(key),
    }


@router.get(
    "/view",
    summary="Integrated view rows for one label",
    description="One row per finding, built on the fly from the committed findings and the "
    "label template. `row_status` is `committed` or `processed` — processed rows are staged "
    "but not yet committed. Filter with `normalized_asset`, `connector` or `row_status`.",
)
def getIntegratedView(
    label: str = Query(..., description="integrity | reliability | maintenance | production"),
    plant_code_id: str = Query(..., description="Plant code ID"),
    normalized_asset: str | None = Query(None, description="Scope to one equipment"),
    connector: str | None = Query(None, description="aif | gloc | lopc"),
    row_status: str | None = Query(None, description="committed | processed"),
    include_processed: bool = Query(True, description="Include rows that are not committed yet"),
    page: int = Query(1, ge=1),
    page_size: int = Query(
        _view.DEFAULT_PAGE_SIZE, ge=1, le=_view.MAX_PAGE_SIZE, description="Default 50, max 200"
    ),
):
    key = str(label or "").strip().lower()
    if key not in _view.known_labels():
        return _unknown_label(label)
    try:
        _validate_plant_code(plant_code_id)
        rows = _view.compute(plant_code_id, key, include_processed=include_processed)
        if normalized_asset:
            wanted = normalized_asset.strip().upper()
            rows = [r for r in rows if str(r["normalized_asset"]).upper() == wanted]
        if connector:
            rows = [r for r in rows if r["source_connector"] == connector.strip().lower()]
        if row_status:
            rows = [r for r in rows if r["row_status"] == row_status.strip().lower()]
        plan = _view.describe(key)
        result = {
            "label": key,
            "title": plan["title"],
            "plant_code_id": plant_code_id,
            "columns": plan["columns"],
            "rows": rows,
            "total": len(rows),
            "committed": sum(1 for r in rows if r["row_status"] == _view.COMMITTED),
            "processed": sum(1 for r in rows if r["row_status"] == _view.PROCESSED),
        }
        return paginated_envelope(
            result,
            items_key="rows",
            page=page,
            page_size=page_size,
            message=f"Integrated {key} view retrieved successfully",
            empty_message=(
                f"No {key} rows yet for plant '{plant_code_id}'. Upload a file labelled "
                f"'{key}', run its pipeline, then review and commit it."
            ),
        )
    except HTTPException as e:
        raise as_envelope_exc(e)
    except Exception as exc:
        return server_error(exc, action="building the integrated view")


@router.post(
    "/recompute",
    summary="Recompute and store the integrated view",
    description="Rebuild the view from the current CDM and persist it into the single "
    "`integrated_view` table, which the four label views are filters over. Body: "
    "{plant_code_id, labels?}. Omit `labels` to rebuild all four.",
)
def recomputeIntegratedView(body: dict = Body(...)):
    plant_code_id = body.get("plant_code_id")
    if not plant_code_id:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": [{"field": "plant_code_id", "message": "plant_code_id is required"}],
            },
        )
    wanted = body.get("labels") or _view.known_labels()
    unknown = [w for w in wanted if str(w).lower() not in _view.known_labels()]
    if unknown:
        return _unknown_label(", ".join(str(u) for u in unknown))
    try:
        _validate_plant_code(plant_code_id)
        written = {}
        for label in wanted:
            key = str(label).lower()
            rows = _view.compute(plant_code_id, key)
            written[key] = _view.materialise(plant_code_id, key, rows)
        if not sum(written.values()):
            _log.info(
                "[integrated/recompute] %s — nothing to materialise for %s",
                plant_code_id, ", ".join(str(w) for w in wanted),
            )
            return no_content()
        return {
            "success": True,
            "message": "Integrated view recomputed successfully",
            "data": {
                "plant_code_id": plant_code_id,
                "rows_written": written,
                "total_rows_written": sum(written.values()),
            },
        }
    except HTTPException as e:
        raise as_envelope_exc(e)
    except Exception as exc:
        return server_error(exc, action="recomputing the integrated view")
