"""
Configuration management endpoints.

Manages SAP extraction tables, column rename mappings, user config,
and read-only access to frozen CDM schemas.
"""

from __future__ import annotations

import logging
import sys
from http import HTTPStatus
from pathlib import Path

from fastapi import APIRouter, Query, HTTPException, Body
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator

from ..audit_context import get_actor
from ..services import audit as _audit
from ..services import user_config_store as _user_config
from ..responses import EnvelopeRoute, collection_envelope, server_error

_REPO_ROOT = Path(__file__).resolve().parents[4]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

if __package__:
    from ..config import (
        CDM_CONFIG_FILE,
        ENTITIES_FROZEN_FILE,
        RELATIONSHIPS_FROZEN_FILE,
        SAP_EXTRACTION_FILE,
        SAP_RENAME_FILE,
        SCHEMA_FROZEN_FILE,
        TEMPLATES_DIR,
        USER_CONFIG_FILE,
        VALIDATION_CONTRACTS_FILE,
    )
    from ..deps import backup, deep_merge, load_yaml, save_yaml, validate_id
    from ..plants import add_plant as _add_plant
    from ..plants import delete_plant as _delete_plant
    from ..plants import list_plants as _list_plants
    from ..plants import purge_plant as _purge_plant
    from ..plants import purge_preview as _purge_preview
    from ..plants import validate_plant_code as _validate_plant_code
    from ..schemas.config import AddSapTableRequest, UpdateColumnRenamesRequest
else:
    from p0.api.config import (
        CDM_CONFIG_FILE,
        ENTITIES_FROZEN_FILE,
        RELATIONSHIPS_FROZEN_FILE,
        SAP_EXTRACTION_FILE,
        SAP_RENAME_FILE,
        SCHEMA_FROZEN_FILE,
        TEMPLATES_DIR,
        USER_CONFIG_FILE,
        VALIDATION_CONTRACTS_FILE,
    )
    from p0.api.deps import (
        backup,
        deep_merge,
        load_yaml,
        save_yaml,
        validate_id,
    )
    from p0.api.plants import add_plant as _add_plant
    from p0.api.plants import delete_plant as _delete_plant
    from p0.api.plants import list_plants as _list_plants
    from p0.api.plants import purge_plant as _purge_plant
    from p0.api.plants import purge_preview as _purge_preview
    from p0.api.plants import validate_plant_code as _validate_plant_code
    from p0.api.schemas.config import (
        AddSapTableRequest,
        UpdateColumnRenamesRequest,
    )


router = APIRouter(route_class=EnvelopeRoute, prefix="/configRouter", tags=["Configuration"])

_log = logging.getLogger("cdm.api.config")


def _read_only_config(
    plant_code_id: str | None,
    config_file,
    message: str,
):
    """Validate the plant and return one frozen YAML in the standard envelope."""
    if not plant_code_id or not plant_code_id.strip():
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": [
                    {
                        "field": "plant_code_id",
                        "message": "plant_code_id is required",
                    }
                ],
            },
        )

    try:
        return {
            "success": True,
            "message": message,
            "data": load_yaml(config_file),
        }
    except HTTPException:
        raise
    except Exception as exc:
        return server_error(exc, action="loading the configuration file", plant_code_id=plant_code_id)



class AddPlantRequest(BaseModel):
    plant_code_id: str = Field(
        ...,
        min_length=1,
        max_length=50,
        description="Stable identifier, e.g. 'Plant_2'",
    )
    label: str | None = Field(
        default=None, max_length=200, description="Friendly name shown in the UI"
    )
    industry: str | None = Field(default=None, description="cement / oil_gas / generic")

    @field_validator("plant_code_id")
    @classmethod
    def validate_plant_code_id(cls, value):
        if not value or not value.strip():
            raise ValueError("plant_code_id cannot be empty")
        return value.strip()


@router.get(
    "/plantAudit",
    summary="List plant lifecycle audit events",
    description="Read plant create/delete/purge events from the central registry, newest first.",
)
def listPlantAudit(
    plant_code_id: str = Query(None, description="Filter to one plant"),
    limit: int = Query(100, ge=1, le=1000),
):
    try:
        events = _audit.list_plant_events(plant_code_id, limit)
        for e in events:
            if e.get("ts") is not None:
                e["ts"] = str(e["ts"])
        return {
            "success": True,
            "message": "Plant audit events fetched successfully",
            "data": {"events": events},
        }
    except HTTPException:
        raise
    except Exception as exc:
        return server_error(exc, action="listing plant audit")


@router.get(
    "/plants",
    summary="List registered plants",
    description="Returns every row from the ``plants`` registry.",
)
def listPlants(
):

    try:
        plants = _list_plants()
        return collection_envelope(
            {"plants": plants},
            items_key="plants",
            message="Plants fetched successfully",
            empty_message="No plants have been created yet.",
        )
    except HTTPException:
        raise
    except Exception as exc:
        return server_error(exc, action="listing plants")


@router.post(
    "/plants",
    status_code=HTTPStatus.CREATED,
    summary="Register a new plant (admin only)",
    description="Add a plant_code_id so uploads can be tagged with it. "
    "The plant_code_id must be unique and ≤50 characters. "
    "Requires the 'admin' role.",
)
def postPlant(
    req: AddPlantRequest,
):

    errors = []

    plant_code_id = req.plant_code_id.strip() if req.plant_code_id else ""
    if not plant_code_id:
        errors.append(
            {
                "field": "plant_code_id",
                "message": "plantCode is required",
            }
        )
    else:
        if len(plant_code_id) > 50:
            errors.append(
                {
                    "field": "plant_code_id",
                    "message": "plantCode must be at most 50 characters",
                }
            )
        if len(plant_code_id) < 1:
            errors.append(
                {
                    "field": "plant_code_id",
                    "message": "plantCode must be at least 1 character",
                }
            )

    if req.label is not None:
        label = req.label.strip()
        if len(label) > 200:
            errors.append(
                {
                    "field": "label",
                    "message": "label must be at most 200 characters",
                }
            )

    if req.industry is not None:
        industry = req.industry.strip()
        allowed_industries = ["cement", "oil_gas", "generic"]
        if industry and industry not in allowed_industries:
            errors.append(
                {
                    "field": "industry",
                    "message": f"industry must be one of: {', '.join(allowed_industries)}",
                }
            )

    if errors:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": errors,
            },
        )

    try:
        actor = get_actor() or "system"
        result = _add_plant(
            plant_code_id=req.plant_code_id,
            label=req.label,
            industry=req.industry,
            created_by=actor,
        )
        try:
            _audit.record_plant_event(
                "plant.create",
                req.plant_code_id,
                actor=actor,
                detail={"label": req.label, "industry": req.industry},
            )
        except Exception as exc:
            _log.warning("[config/plants] audit record_event failed: %s", exc)

        return {
            "success": True,
            "message": "Plant created successfully",
            "data": result,
        }
    except HTTPException:
        raise
    except Exception as exc:
        return server_error(exc, action="creating plant")


@router.get(
    "/plants/{plantCode}/purgePreview",
    summary="Preview what a cascade purge would delete (admin only)",
    description="Dry-run for the destructive cascade delete: returns the row "
    "counts per Postgres table, IoTDB series count, and RustFS "
    "object count that a purge would remove. Changes nothing.",
)
def previewPurgePlant(
    plantCode: str,
):

    if not plantCode or not plantCode.strip():
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": [
                    {
                        "field": "plantCode",
                        "message": "plantCode is required",
                    }
                ],
            },
        )

    try:
        result = _purge_preview(plantCode)
        return {
            "success": True,
            "message": "Purge preview fetched successfully",
            "data": result,
        }
    except HTTPException:
        raise
    except Exception as exc:
        return server_error(exc, action="previewing purge plant")


@router.delete(
    "/plants/{plantCode}",
    summary="Delete a plant (admin only)",
    description="Default (safe): removes a plant_code_id from the registry, but "
    "refuses if any CDM data still references it (409). Pass "
    "?cascade=true&confirm=<plant_code_id> to instead PURGE the plant "
    "and ALL of its data across Postgres, IoTDB and RustFS — "
    "irreversible. Refuses to delete the default seed (Plant_1).",
)
def deletePlant(
    plantCode: str,
    cascade: bool = Query(False, description="If true, purge all of the plant's data."),
    confirm: str | None = Query(
        None, description="Must equal plantCode to run a cascade purge."
    ),
):

    errors = []
    if not plantCode or not plantCode.strip():
        errors.append(
            {
                "field": "plantCode",
                "message": "plantCode is required",
            }
        )

    if cascade:
        if not confirm or confirm.strip() == "":
            errors.append(
                {
                    "field": "confirm",
                    "message": "confirm parameter is required for cascade purge",
                }
            )
        elif confirm != plantCode:
            errors.append(
                {
                    "field": "confirm",
                    "message": "cascade purge requires confirm=<plant_code_id> to exactly match the plant being deleted",
                }
            )

    if errors:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": errors,
            },
        )

    try:
        actor = get_actor() or "system"

        def _audit_event(action, status, detail=None):
            try:
                _audit.record_plant_event(
                    action, plantCode, actor=actor, status=status, detail=detail
                )
            except Exception as exc:
                _log.warning("[config/plants] audit record_plant_event failed: %s", exc)

        if cascade:
            result = _purge_plant(plantCode)
            _audit_event(
                "plant.purge", "ok", detail=result if isinstance(result, dict) else None
            )
            return {
                "success": True,
                "message": "Plant purged successfully",
                "data": result,
            }

        _delete_plant(plantCode)
        _audit_event("plant.delete", "ok")

        return {
            "success": True,
            "message": "Plant deleted successfully",
            "data": {"status": "deleted", "plantCode": plantCode},
        }
    except HTTPException:
        raise
    except Exception as exc:
        return server_error(exc, action="deleting plant")


_SAP_TABLE_TO_SOURCE_KEY: dict[str, str] = {
    "equi": "sap_assets",
    "eqkt": "sap_assets",
    "aufk": "sap_wo",
    "afko": "sap_wo",
    "afvc": "sap_wo",
    "afvv": "sap_wo",
    "qmel": "sap_notification",
    "qmsm": "sap_notification",
    "jest": "sap_wo",
    "jsto": "sap_wo",
    "resb": "sap_bom",
    "coep": "sap_wo",
    "iflot": "sap_assets",
    "iloa": "sap_assets",
    "stxh": "sap_wo",
    "stxl": "sap_wo",
    "plko": "sap_task_list",
    "plpo": "sap_task_list",
    "mapl": "sap_task_list",
    "mara": "sap_material_master",
    "marc": "sap_material_master",
    "mard": "sap_material_master",
}


@router.get(
    "/sapTables",
    summary="List SAP extraction tables",
    description="Return all SAP tables currently in `sap_extraction.yaml`, "
    "grouped by logical category (technical_objects, notifications, etc.).",
    response_description="List of SAP table entries with their fields and key columns.",
)
def listSapTables(
    plant_code_id: str = Query(..., description="Plant code ID for the request"),
):

    if not plant_code_id or not plant_code_id.strip():
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": [
                    {
                        "field": "plant_code_id",
                        "message": "plant_code_id is required",
                    }
                ],
            },
        )

    try:
        data = load_yaml(SAP_EXTRACTION_FILE)
        result = []
        for group_name, group_val in data.get("sap_pm_mm", {}).items():
            if not isinstance(group_val, dict):
                continue
            for entry_name, entry_val in group_val.items():
                if isinstance(entry_val, dict) and "table" in entry_val:
                    result.append(
                        {
                            "group": group_name,
                            "entry": entry_name,
                            "table": entry_val["table"],
                            "key": entry_val.get("key", []),
                            "fields": entry_val.get("fields", []),
                        }
                    )
        user_cfg = load_yaml(USER_CONFIG_FILE)
        custom = user_cfg.get("sap_processing", {}).get("custom_tables", {})
        for entry_name, entry_val in custom.items():
            result.append(
                {
                    "group": entry_val.get("group", "custom"),
                    "entry": entry_name,
                    "table": entry_val.get("table", entry_name.upper()),
                    "key": entry_val.get("key", []),
                    "fields": entry_val.get("fields", []),
                    "custom": True,
                }
            )

        return collection_envelope(
            {"tables": result},
            items_key="tables",
            message="SAP tables fetched successfully",
            empty_message=f"No SAP tables are configured for plant '{plant_code_id}'.",
        )
    except HTTPException:
        raise
    except Exception as exc:
        return server_error(exc, action="listing sap tables")


@router.post(
    "/sapTables",
    status_code=HTTPStatus.CREATED,
    summary="Add a SAP table",
    description="Register a new SAP table.  Extraction metadata and column renames "
    "are stored in `user_config.yaml` so standard templates stay untouched.",
)
def addSapTable(
    req: AddSapTableRequest,
):

    errors = []

    if not req.table_name or not req.table_name.strip():
        errors.append(
            {
                "field": "table_name",
                "message": "table_name is required",
            }
        )

    if not req.group or not req.group.strip():
        errors.append(
            {
                "field": "group",
                "message": "group is required",
            }
        )

    if not req.source_key or not req.source_key.strip():
        errors.append(
            {
                "field": "source_key",
                "message": "source_key is required",
            }
        )

    if not req.key_fields or len(req.key_fields) == 0:
        errors.append(
            {
                "field": "key_fields",
                "message": "key_fields must contain at least one field",
            }
        )

    if not req.columns or len(req.columns) == 0:
        errors.append(
            {
                "field": "columns",
                "message": "At least one column is required",
            }
        )
    else:
        for idx, col in enumerate(req.columns):
            if not col.sap_field or not col.sap_field.strip():
                errors.append(
                    {
                        "field": f"columns[{idx}].sap_field",
                        "message": f"Column {idx}: sap_field is required",
                    }
                )
            if not col.canonical_name or not col.canonical_name.strip():
                errors.append(
                    {
                        "field": f"columns[{idx}].canonical_name",
                        "message": f"Column {idx}: canonical_name is required",
                    }
                )

    if errors:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": errors,
            },
        )

    try:
        user_cfg = load_yaml(USER_CONFIG_FILE)
        sap_ext = user_cfg.setdefault("sap_processing", {}).setdefault("custom_tables", {})
        entry_key = req.table_name.lower()
        if entry_key in sap_ext:
            return JSONResponse(
                status_code=HTTPStatus.CONFLICT,
                content={
                    "success": False,
                    "message": "Duplicate table entry",
                    "errors": [
                        {
                            "field": "table_name",
                            "message": f"Table entry '{entry_key}' already exists in user config",
                        }
                    ],
                },
            )

        sap_fields = [c.sap_field for c in req.columns]
        sap_ext[entry_key] = {
            "table": req.table_name,
            "group": req.group,
            "label": req.label if hasattr(req, "label") else req.table_name,
            "key": req.key_fields,
            "fields": sap_fields,
            "source_key": req.source_key,
        }

        rename_overrides = user_cfg.setdefault("sap_processing", {}).setdefault(
            "column_rename_overrides", {}
        )
        rename_overrides[req.source_key] = {
            "mappings": {
                c.sap_field: c.canonical_name.replace(" ", "_").lower() for c in req.columns
            }
        }

        backup(USER_CONFIG_FILE)
        save_yaml(USER_CONFIG_FILE, user_cfg)

        return {
            "success": True,
            "message": "SAP table created successfully",
            "data": {
                "status": "created",
                "table": req.table_name,
                "source_key": req.source_key,
            },
        }
    except HTTPException:
        raise
    except Exception as exc:
        return server_error(exc, action="creating sap table")


@router.delete(
    "/sapTables/{group}/{entry}",
    summary="Remove a SAP table",
    description="Delete a SAP table entry from `sap_extraction.yaml`.",
)
def deleteSapTable(
    group: str,
    entry: str,
    plant_code_id: str = Query(..., description="Plant code ID for the request"),
):

    errors = []
    if not plant_code_id or not plant_code_id.strip():
        errors.append(
            {
                "field": "plant_code_id",
                "message": "plant_code_id is required",
            }
        )

    if not group or not group.strip():
        errors.append(
            {
                "field": "group",
                "message": "group is required",
            }
        )

    if not entry or not entry.strip():
        errors.append(
            {
                "field": "entry",
                "message": "entry is required",
            }
        )

    if errors:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": errors,
            },
        )

    try:
        validate_id(group, "group")
        validate_id(entry, "entry")

        user_cfg = load_yaml(USER_CONFIG_FILE)
        custom = user_cfg.get("sap_processing", {}).get("custom_tables", {})
        if entry in custom:
            backup(USER_CONFIG_FILE)
            del custom[entry]
            overrides = user_cfg.get("sap_processing", {}).get(
                "column_rename_overrides", {}
            )
            src_key = None
            for k, v in list(overrides.items()):
                if k.startswith(f"sap_custom_{entry}"):
                    src_key = k
                    break
            if src_key:
                del overrides[src_key]
            save_yaml(USER_CONFIG_FILE, user_cfg)

            return {
                "success": True,
                "message": "SAP table deleted successfully",
                "data": {"status": "deleted"},
            }

        ext_data = load_yaml(SAP_EXTRACTION_FILE)
        pm = ext_data.get("sap_pm_mm", {})
        grp = pm.get(group, {})
        if entry not in grp:
            return JSONResponse(
                status_code=HTTPStatus.NOT_FOUND,
                content={
                    "success": False,
                    "message": "Table entry not found",
                    "errors": [
                        {
                            "field": "entry",
                            "message": f"Entry '{entry}' not found in group '{group}'",
                        }
                    ],
                },
            )

        return JSONResponse(
            status_code=HTTPStatus.FORBIDDEN,
            content={
                "success": False,
                "message": "Cannot delete standard template tables",
                "errors": [
                    {
                        "field": "entry",
                        "message": "Cannot delete standard template tables; only custom tables can be removed",
                    }
                ],
            },
        )
    except HTTPException:
        raise
    except Exception as exc:
        return server_error(exc, action="deleting sap table")


@router.get(
    "/columnRenames",
    summary="List all column rename sources",
    description="Return all source→rename mappings from `sap_column_rename.yaml`.",
)
def listColumnRenames(
    plant_code_id: str = Query(..., description="Plant code ID for the request"),
):

    if not plant_code_id or not plant_code_id.strip():
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": [
                    {
                        "field": "plant_code_id",
                        "message": "plant_code_id is required",
                    }
                ],
            },
        )

    try:
        data = load_yaml(SAP_RENAME_FILE)
        sources = dict(data.get("sources", {}))
        user_cfg = load_yaml(USER_CONFIG_FILE)
        all_overrides = user_cfg.get("sap_processing", {}).get("column_rename_overrides", {})
        overrides = all_overrides.get(plant_code_id, {})
        for name, val in overrides.items():
            if name in sources:
                merged = dict(sources[name].get("mappings", {}))
                merged.update(val.get("mappings", {}))
                sources[name] = {"mappings": merged}
            else:
                sources[name] = val

        return {
            "success": True,
            "message": "Column renames fetched successfully",
            "data": {
                "sources": {
                    name: {
                        "mappings": val.get("mappings", {}),
                        "column_count": len(val.get("mappings", {})),
                    }
                    for name, val in sources.items()
                }
            },
        }
    except HTTPException:
        raise
    except Exception as exc:
        return server_error(exc, action="listing column renames")


@router.get(
    "/columnRenames/{sourceKey}",
    summary="Get renames for one source",
    description="Return the column rename mappings for a specific source key.",
)
def getColumnRenames(
    sourceKey: str,
    plant_code_id: str = Query(..., description="Plant code ID for the request"),
):

    errors = []
    if not plant_code_id or not plant_code_id.strip():
        errors.append(
            {
                "field": "plant_code_id",
                "message": "plant_code_id is required",
            }
        )

    if not sourceKey or not sourceKey.strip():
        errors.append(
            {
                "field": "sourceKey",
                "message": "sourceKey is required",
            }
        )

    if errors:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": errors,
            },
        )

    try:
        validate_id(sourceKey, "source_key")
        resolved_key = _SAP_TABLE_TO_SOURCE_KEY.get(sourceKey, sourceKey)
        data = load_yaml(SAP_RENAME_FILE)
        src = data.get("sources", {}).get(resolved_key)
        user_cfg = load_yaml(USER_CONFIG_FILE)
        user_src = (
            user_cfg.get("sap_processing", {})
            .get("column_rename_overrides", {})
            .get(plant_code_id, {})
            .get(resolved_key)
        )
        if not src and not user_src:
            return JSONResponse(
                status_code=HTTPStatus.NOT_FOUND,
                content={
                    "success": False,
                    "message": "Source key not found",
                    "errors": [
                        {
                            "field": "sourceKey",
                            "message": f"Source key '{sourceKey}' not found",
                        }
                    ],
                },
            )
        mappings = dict(src.get("mappings", {})) if src else {}
        if user_src:
            mappings.update(user_src.get("mappings", {}))

        return {
            "success": True,
            "message": "Column renames fetched successfully",
            "data": {"source_key": resolved_key, "mappings": mappings},
        }
    except HTTPException:
        raise
    except Exception as exc:
        return server_error(exc, action="loading column renames")


@router.put(
    "/columnRenames/{sourceKey}",
    summary="Update renames for one source",
    description="Replace the per-plant rename mappings for a source key.",
)
def updateColumnRenames(
    sourceKey: str,
    plant_code_id: str = Query(..., description="Plant code ID for the request"),
    *,
    body: UpdateColumnRenamesRequest,
):

    errors = []
    if not plant_code_id or not plant_code_id.strip():
        errors.append({"field": "plant_code_id", "message": "plant_code_id is required"})
    if not sourceKey or not sourceKey.strip():
        errors.append(
            {
                "field": "sourceKey",
                "message": "sourceKey is required",
            }
        )

    if not body.mappings or len(body.mappings) == 0:
        errors.append(
            {
                "field": "mappings",
                "message": "mappings is required and must contain at least one mapping",
            }
        )

    if errors:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": errors,
            },
        )

    try:
        validate_id(sourceKey, "source_key")
        resolved_key = _SAP_TABLE_TO_SOURCE_KEY.get(sourceKey, sourceKey)
        data = load_yaml(SAP_RENAME_FILE)
        user_cfg = load_yaml(USER_CONFIG_FILE)
        std_exists = resolved_key in data.get("sources", {})
        usr_exists = resolved_key in (
            user_cfg.get("sap_processing", {})
            .get("column_rename_overrides", {})
            .get(plant_code_id, {})
        )
        if not std_exists and not usr_exists:
            return JSONResponse(
                status_code=HTTPStatus.NOT_FOUND,
                content={
                    "success": False,
                    "message": "Source key not found",
                    "errors": [
                        {
                            "field": "sourceKey",
                            "message": f"Source key '{sourceKey}' not found",
                        }
                    ],
                },
            )

        sap_proc = user_cfg.setdefault("sap_processing", {})
        plant_overrides = sap_proc.setdefault("column_rename_overrides", {}).setdefault(
            plant_code_id, {}
        )
        plant_overrides.setdefault(resolved_key, {})["mappings"] = body.mappings
        backup(USER_CONFIG_FILE)
        save_yaml(USER_CONFIG_FILE, user_cfg)

        return {
            "success": True,
            "message": "Column renames updated successfully",
            "data": {
                "status": "updated",
                "plant_code_id": plant_code_id,
                "source_key": sourceKey,
            },
        }
    except HTTPException:
        raise
    except Exception as exc:
        return server_error(exc, action="updating column renames")


@router.get(
    "/userConfig",
    summary="Get user config",
    description="Return the contents of `user_config.yaml` (user-editable overrides).",
)
def getUserConfig(
    plant_code_id: str = Query(..., description="Plant code ID for the request"),
    include_layers: bool = Query(
        False, description="Also return each layer separately (template / global / plant)"
    ),
):
    """Resolved config: standard template first, then global overrides, then this plant."""
    errors = []
    plant_code_id_val = (plant_code_id or "").strip()
    if not plant_code_id_val:
        errors.append({"field": "plant_code_id", "message": "plant_code_id is required"})
    if errors:
        return JSONResponse(
            status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
            content={"success": False, "message": "Validation failed", "errors": errors},
        )

    try:
        _validate_plant_code(plant_code_id_val)
        template = load_yaml(USER_CONFIG_FILE) or {}
        described = _user_config.describe(plant_code_id_val, template)
        data = {
            "plant_code_id": plant_code_id_val,
            "config": described["config"],
            "versions": described["versions"],
            "is_customised": described["is_customised"],
        }
        if include_layers:
            data["layers"] = described["layers"]
        return {
            "success": True,
            "message": (
                "User config fetched successfully"
                if described["is_customised"]
                else f"Plant '{plant_code_id_val}' is using the standard template with no overrides."
            ),
            "data": data,
        }
    except HTTPException:
        raise
    except Exception as exc:
        return server_error(
            exc, action="loading the user config", plant_code_id=plant_code_id_val
        )


@router.put(
    "/userConfig",
    summary="Replace user config",
    description="Overwrite `user_config.yaml` entirely with the provided body.",
)
def replaceUserConfig(
    body: dict,
):

    errors = []
    plant_code_id = body.get("plant_code_id")
    if not plant_code_id or not plant_code_id.strip():
        errors.append(
            {
                "field": "plant_code_id",
                "message": "plant_code_id is required",
            }
        )

    if not body:
        errors.append(
            {
                "field": "body",
                "message": "Request body is required",
            }
        )

    if errors:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": errors,
            },
        )

    try:
        if USER_CONFIG_FILE.exists():
            backup(USER_CONFIG_FILE)
        save_yaml(USER_CONFIG_FILE, body)

        return {
            "success": True,
            "message": "User config replaced successfully",
            "data": {"status": "ok"},
        }
    except HTTPException:
        raise
    except Exception as exc:
        return server_error(exc, action="replacing user config")


@router.patch(
    "/userConfig",
    summary="Patch user config",
    description="Deep-merge the provided fields into existing `user_config.yaml`.",
)
def patchUserConfig(
    body: dict,
):

    errors = []
    plant_code_id = body.get("plant_code_id")
    if not plant_code_id or not plant_code_id.strip():
        errors.append(
            {
                "field": "plant_code_id",
                "message": "plant_code_id is required",
            }
        )

    if not body:
        errors.append(
            {
                "field": "body",
                "message": "Request body is required",
            }
        )

    if errors:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": errors,
            },
        )

    plant_code_id_val = plant_code_id.strip()

    try:
        _validate_plant_code(plant_code_id_val)
        patch = {k: v for k, v in body.items() if k != "plant_code_id"}
        actor = get_actor()

        stored = _user_config.write_layer(
            plant_code_id_val, _user_config.PLANT_SCOPE, patch, actor=actor
        )

        template = load_yaml(USER_CONFIG_FILE) or {}
        merged = _user_config.resolve(plant_code_id_val, template)
        try:
            if USER_CONFIG_FILE.exists():
                backup(USER_CONFIG_FILE)
            save_yaml(USER_CONFIG_FILE, deep_merge(template, patch))
        except Exception as exc:
            _log.warning("[config] user_config.yaml mirror failed: %s", exc)

        return {
            "success": True,
            "message": "User config patched successfully",
            "data": {
                "status": "ok",
                "plant_code_id": plant_code_id_val,
                "config": merged,
                "plant_overrides": stored,
                "version": _user_config.layer_version(
                    plant_code_id_val, _user_config.PLANT_SCOPE
                ),
            },
        }
    except HTTPException:
        raise
    except Exception as exc:
        return server_error(
            exc, action="updating the user config", plant_code_id=plant_code_id_val
        )


@router.get(
    "/cdm",
    summary="Get CDM master config",
    description="Return the `cdm_config_frozen.yaml` (read-only merge/consolidation rules).",
)
def getCdmConfig(
    plant_code_id: str = Query(..., description="Plant code ID for the request"),
):
    return _read_only_config(plant_code_id, CDM_CONFIG_FILE, "CDM config fetched successfully")


@router.get(
    "/entities",
    summary="Get entity definitions",
    description="Return the `entities_frozen.yaml` — all CDM entity types, "
    "identity keys, attributes, and source mappings.",
)
def listEntities(
    plant_code_id: str = Query(..., description="Plant code ID for the request"),
):
    return _read_only_config(plant_code_id, ENTITIES_FROZEN_FILE, "Entities fetched successfully")


@router.get(
    "/schema",
    summary="Get SQL schema definitions",
    description="Return the `schema_frozen.yaml` — SQL table definitions "
    "(columns, types, constraints, indexes).",
)
def getSchema(
    plant_code_id: str = Query(..., description="Plant code ID for the request"),
):
    return _read_only_config(plant_code_id, SCHEMA_FROZEN_FILE, "Schema fetched successfully")


@router.get(
    "/relationships",
    summary="Get relationship definitions",
    description="Return the `relationships_frozen.yaml` — relationship types, "
    "dedupe keys, and entity-to-entity mappings.",
)
def listRelationships(
    plant_code_id: str = Query(..., description="Plant code ID for the request"),
):
    return _read_only_config(plant_code_id, RELATIONSHIPS_FROZEN_FILE, "Relationships fetched successfully")


@router.get(
    "/validation",
    summary="Get validation contracts",
    description="Return the `validation_contracts.yaml` — required columns and quality gates.",
)
def getValidationContracts(
    plant_code_id: str = Query(..., description="Plant code ID for the request"),
):
    return _read_only_config(plant_code_id, VALIDATION_CONTRACTS_FILE, "Validation contracts fetched successfully")


@router.get(
    "/industries",
    summary="List available industries",
    description="Return the list of industries defined in `_manifest.yaml`.",
)
def listIndustries(
    plant_code_id: str = Query(..., description="Plant code ID for the request"),
):

    if not plant_code_id or not plant_code_id.strip():
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": [
                    {
                        "field": "plant_code_id",
                        "message": "plant_code_id is required",
                    }
                ],
            },
        )

    try:
        manifest = load_yaml(TEMPLATES_DIR / "_manifest.yaml")
        industries = manifest.get("industries", {})

        return {
            "success": True,
            "message": "Industries fetched successfully",
            "data": {
                "industries": [
                    {"id": k, "name": v.get("name", k)} for k, v in industries.items()
                ]
            },
        }
    except HTTPException:
        raise
    except Exception as exc:
        return server_error(exc, action="listing industries")


@router.get(
    "/industries/{industryId}",
    summary="Get industry config",
    description="Return the `industry_config.yaml` for the selected industry.",
)
def getIndustryConfig(
    industryId: str,
    plant_code_id: str = Query(..., description="Plant code ID for the request"),
):

    errors = []
    if not plant_code_id or not plant_code_id.strip():
        errors.append(
            {
                "field": "plant_code_id",
                "message": "plant_code_id is required",
            }
        )

    if not industryId or not industryId.strip():
        errors.append(
            {
                "field": "industryId",
                "message": "industryId is required",
            }
        )

    if errors:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": errors,
            },
        )

    try:
        validate_id(industryId, "industry_id")
        manifest = load_yaml(TEMPLATES_DIR / "_manifest.yaml")
        ind = manifest.get("industries", {}).get(industryId)
        if not ind:
            return JSONResponse(
                status_code=HTTPStatus.NOT_FOUND,
                content={
                    "success": False,
                    "message": "Industry not found",
                    "errors": [
                        {
                            "field": "industryId",
                            "message": f"Industry '{industryId}' not found",
                        }
                    ],
                },
            )
        config_path = (
            TEMPLATES_DIR / ind["path"] / ind.get("config_file", "industry_config.yaml")
        )
        if not config_path.exists():
            return JSONResponse(
                status_code=HTTPStatus.NOT_FOUND,
                content={
                    "success": False,
                    "message": "Config file not found",
                    "errors": [
                        {
                            "field": "industryId",
                            "message": f"Config file not found for industry '{industryId}'",
                        }
                    ],
                },
            )
        config = load_yaml(config_path)

        return {
            "success": True,
            "message": "Industry config fetched successfully",
            "data": config,
        }
    except HTTPException:
        raise
    except Exception as exc:
        return server_error(exc, action="loading industry config")


@router.get(
    "/documentTypes",
    summary="Standard document-type catalog with default extraction fields",
    description="Returns the categories / types / default fields the Documents "
    "upload modal pre-populates from. Sourced from "
    "templates/_common/document_types.yaml. Users can still add / "
    "edit / remove fields per upload; those overrides are stored in "
    "user_config.yaml so the standard catalog stays untouched.",
)
def listDocumentTypes(
    plant_code_id: str = Query(..., description="Plant code ID for the request"),
):

    if not plant_code_id or not plant_code_id.strip():
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": [
                    {
                        "field": "plant_code_id",
                        "message": "plant_code_id is required",
                    }
                ],
            },
        )

    try:
        catalog = load_yaml(TEMPLATES_DIR / "_common" / "document_types.yaml")
        if not catalog or not isinstance(catalog, dict):
            return {
                "success": True,
                "message": "Document types fetched successfully",
                "data": {"version": None, "categories": []},
            }

        return {
            "success": True,
            "message": "Document types fetched successfully",
            "data": {
                "version": catalog.get("version"),
                "categories": catalog.get("categories", []),
            },
        }
    except HTTPException:
        raise
    except Exception as exc:
        return server_error(exc, action="listing document types")
