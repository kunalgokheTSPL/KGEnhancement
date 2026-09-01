"""
CDM data query endpoints.

Read CDM output tables (equipment, work orders, relationships, etc.)
from either CSV files or PostgreSQL, depending on deployment mode.
"""

from __future__ import annotations

import logging
from http import HTTPStatus
from pathlib import Path
from typing import TYPE_CHECKING

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    psycopg2 = None

if TYPE_CHECKING:
    import pandas as pd

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from p0.utils import fs as _fs

from ..config import CDM_DB_PASS, OUT_DIR
from ..plants import validate_plant_code as _validate_plant_code
from ..responses import EnvelopeRoute, as_envelope_exc, server_error
from ..database.connection import _get_conn

router = APIRouter(route_class=EnvelopeRoute, prefix="/data", tags=["CDM Data"])

_log = logging.getLogger("cdm.api.data")


def _resolve_table_path(table_name: str) -> str:
    """Resolve the s3:// path for a CDM output table, preferring Parquet over CSV."""
    for subdir in ("entities", "relationships"):
        for ext in (".parquet", ".csv"):
            p = _fs.path_join(str(OUT_DIR), "final", subdir, f"{table_name}{ext}")
            if _fs.exists(p):
                return p
    raise HTTPException(
        HTTPStatus.NOT_FOUND, f"Table '{table_name}' not found in CDM output"
    )


def _read_table_file(path) -> "pd.DataFrame":
    """Read a Parquet or CSV file (local or s3://)."""
    sp = str(path)
    if sp.endswith(".parquet"):
        return _fs.read_parquet(sp)
    return _fs.read_csv(sp)


def _read_csv_table(
    table_name: str, limit: int, offset: int, search: str | None
) -> dict:
    """Read a CDM output table from final/ directory."""

    table_path = _resolve_table_path(table_name)
    df = _read_table_file(table_path)

    if search:
        search_lower = search.lower()
        text_cols = df.select_dtypes(include=["object"]).columns
        mask = (
            df[text_cols]
            .apply(
                lambda col: col.astype(str)
                .str.lower()
                .str.contains(search_lower, na=False)
            )
            .any(axis=1)
        )
        df = df[mask]

    total = len(df)
    df = df.iloc[offset : offset + limit]

    return {
        "table": table_name,
        "total": total,
        "offset": offset,
        "limit": limit,
        "columns": list(df.columns),
        "rows": df.fillna("").to_dict(orient="records"),
    }


def _read_pg_table(
    table_name: str,
    limit: int,
    offset: int,
    search: str | None,
    plant_code_id: str | None,
    equipment_uid: str | None = None,
) -> dict:
    """Read a CDM table from PostgreSQL, optionally filtered to one equipment_uid."""
    if psycopg2 is None:
        raise HTTPException(
            HTTPStatus.INTERNAL_SERVER_ERROR,
            "psycopg2 not installed — cannot query PostgreSQL",
        )

    conn = _get_conn(plant_code_id)
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cur.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename = %s",
            (table_name,),
        )
        if not cur.fetchone():
            raise HTTPException(
                HTTPStatus.NOT_FOUND, f"Table '{table_name}' not found in database"
            )

        conditions = []
        params: list = []

        if plant_code_id:
            conditions.append("plant_code_id = %s")
            params.append(plant_code_id)

        if equipment_uid:
            conditions.append("equipment_uid::text = %s")
            params.append(str(equipment_uid))

        if search:
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = %s AND data_type IN ('character varying', 'text')",
                (table_name,),
            )
            text_cols = [r["column_name"] for r in cur.fetchall()]
            if text_cols:
                search_clauses = [
                    f"{col} ILIKE %s" for col in text_cols[:5]
                ]
                conditions.append(f"({' OR '.join(search_clauses)})")
                params.extend([f"%{search}%"] * len(search_clauses))

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        cur.execute(f"SELECT COUNT(*) FROM {table_name} {where}", params)  # noqa: S608
        total = cur.fetchone()["count"]

        cur.execute(
            f"SELECT * FROM {table_name} {where} LIMIT %s OFFSET %s",  # noqa: S608
            params + [limit, offset],
        )
        rows = cur.fetchall()

        columns = [desc[0] for desc in cur.description] if cur.description else []

        return {
            "table": table_name,
            "source": "postgresql",
            "total": total,
            "offset": offset,
            "limit": limit,
            "columns": columns,
            "rows": [
                {k: (str(v) if v is not None else "") for k, v in row.items()}
                for row in rows
            ],
        }
    finally:
        conn.close()


def _use_postgres() -> bool:
    """Check if PostgreSQL is configured and reachable."""
    return bool(CDM_DB_PASS)


def _validate_query_params(limit: int, offset: int, search: str | None) -> HTTPException | None:
    if limit < 1 or limit > 10000:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail={
                "success": False,
                "message": "Limit must be between 1 and 10000",
                "data": None,
            },
        )
    if offset < 0:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail={
                "success": False,
                "message": "Offset must be non-negative",
                "data": None,
            },
        )
    if search is not None:
        search_val = search.strip()
        if not search_val:
            raise HTTPException(
                status_code=HTTPStatus.BAD_REQUEST,
                detail={
                    "success": False,
                    "message": "search query cannot be empty if provided",
                    "data": None,
                },
            )
        if len(search_val) > 200:
            raise HTTPException(
                status_code=HTTPStatus.BAD_REQUEST,
                detail={
                    "success": False,
                    "message": "search query cannot exceed 200 characters",
                    "data": None,
                },
            )
    return None


def _validate_limit_offset(limit: int, offset: int) -> HTTPException | None:
    if limit < 1 or limit > 10000:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail={
                "success": False,
                "message": "Limit must be between 1 and 10000",
                "data": None,
            },
        )
    if offset < 0:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail={
                "success": False,
                "message": "Offset must be non-negative",
                "data": None,
            },
        )
    return None




@router.get(
    "/tables",
    summary="List available CDM tables",
    description="List all CDM tables available for querying (from CSV output or PostgreSQL).",
    status_code=HTTPStatus.OK,
)
def listTables(
    plant_code_id: str = Query(..., description="Plant code ID"),
):

    errors = []
    plant_code_id_val = (plant_code_id or "").strip()
    if not plant_code_id_val:
        errors.append({
            "field": "plant_code_id",
            "message": "plant_code_id is required"
        })

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
        tables: list[dict] = []

        for subdir in ["entities", "relationships"]:
            d = _fs.path_join(str(OUT_DIR), "final", subdir)
            stems_seen: set[str] = set()
            for f in sorted(_fs.glob_files(_fs.path_join(d, "*.parquet"))):
                stems_seen.add(Path(f).stem)
                tables.append(
                    {"name": Path(f).stem, "type": subdir, "source": "parquet"}
                )
            for f in sorted(_fs.glob_files(_fs.path_join(d, "*.csv"))):
                if Path(f).stem not in stems_seen:
                    tables.append(
                        {"name": Path(f).stem, "type": subdir, "source": "csv"}
                    )

        if _use_postgres():
            try:
                conn = _get_conn(plant_code_id)
                cur = conn.cursor()
                cur.execute(
                    "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename"
                )
                for row in cur.fetchall():
                    if not any(t["name"] == row[0] for t in tables):
                        tables.append(
                            {"name": row[0], "type": "table", "source": "postgresql"}
                        )
                conn.close()
            except Exception as exc:
                _log.warning("[data] table row-count query failed: %s", exc)

        return {
            "success": True,
            "message": "Tables listed successfully",
            "data": {"tables": tables},
        }
    except HTTPException as e:
        raise as_envelope_exc(e)
    except Exception as exc:
        return server_error(exc, action="listing tables")


@router.get(
    "/equipment",
    summary="List equipment records",
    description="Query the canonical `equipment` table from CDM output. "
    "Supports search, pagination, and optional plant_code_id filter.",
    status_code=HTTPStatus.OK,
)
def listEquipment(
    plant_code_id: str = Query(..., description="Plant code ID"),
    limit: int = Query(default=100, ge=1, le=10000),
    offset: int = Query(default=0, ge=0),
    search: str | None = Query(default=None, max_length=200),
):

    errors = []
    plant_code_id_val = (plant_code_id or "").strip()
    if not plant_code_id_val:
        errors.append({"field": "plant_code_id", "message": "plant_code_id is required"})
    if limit < 1 or limit > 10000:
        errors.append({"field": "limit", "message": "Limit must be between 1 and 10000"})
    if offset < 0:
        errors.append({"field": "offset", "message": "Offset must be non-negative"})
    if search is not None:
        search_val = search.strip()
        if not search_val:
            errors.append({"field": "search", "message": "search query cannot be empty if provided"})
        elif len(search_val) > 200:
            errors.append({"field": "search", "message": "search query cannot exceed 200 characters"})
        search = search_val

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
        if _use_postgres():
            res = _read_pg_table("equipment", limit, offset, search, plant_code_id)
        else:
            res = _read_csv_table("equipment", limit, offset, search)
        return {
            "success": True,
            "message": "Equipment records listed successfully",
            "data": res,
        }
    except HTTPException as e:
        raise as_envelope_exc(e)
    except Exception as exc:
        return server_error(exc, action="listing equipment")


@router.get(
    "/equipment/{equipmentUid}",
    summary="Get one equipment record",
    description="Get a single equipment record by UID, including all linked relationships.",
    status_code=HTTPStatus.OK,
)
def getEquipment(
    equipmentUid: str,
    plant_code_id: str = Query(..., description="Plant code ID"),
):

    errors = []
    plant_code_id_val = (plant_code_id or "").strip()
    if not plant_code_id_val:
        errors.append({"field": "plant_code_id", "message": "plant_code_id is required"})
    equipment_uid_val = (equipmentUid or "").strip()
    if not equipment_uid_val:
        errors.append({"field": "equipmentUid", "message": "equipmentUid is required"})

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
    equipmentUid = equipment_uid_val
    equipment_uid = equipmentUid

    try:
        _validate_plant_code(plant_code_id)

        record = None
        relationships = []

        if _use_postgres():
            res = _read_pg_table("equipment", 1, 0, None, plant_code_id, equipment_uid)
            rows = res.get("rows") or []
            record = rows[0] if rows else None
            if record is not None:
                try:
                    rel = _read_pg_table(
                        "asset_relationship", 500, 0, None, plant_code_id, None
                    )
                    relationships = [
                        r
                        for r in (rel.get("rows") or [])
                        if str(r.get("parent_ref")) == equipment_uid
                        or str(r.get("child_ref")) == equipment_uid
                    ]
                except Exception as exc:
                    _log.warning("[data] asset_relationship read failed: %s", exc)
        else:
            try:
                equip_path = _resolve_table_path("equipment")
            except HTTPException as e:
                raise as_envelope_exc(e)
            df = _read_table_file(equip_path)
            match = df[df["equipment_uid"].astype(str) == equipment_uid]
            if not match.empty:
                record = match.iloc[0].fillna("").to_dict()
                try:
                    rel_path = _resolve_table_path("asset_relationship")
                    rel_df = _read_table_file(rel_path)
                    linked = rel_df[
                        (rel_df["parent_ref"].astype(str) == equipment_uid)
                        | (rel_df["child_ref"].astype(str) == equipment_uid)
                    ]
                    relationships = linked.fillna("").to_dict(orient="records")
                except Exception as exc:
                    _log.warning("[data] asset_relationship read failed: %s", exc)

        if record is None:
            raise HTTPException(
                status_code=HTTPStatus.NOT_FOUND,
                detail={
                    "success": False,
                    "message": "Equipment not found",
                    "data": None,
                },
            )

        return {
            "success": True,
            "message": "Equipment record retrieved successfully",
            "data": {"equipment": record, "relationships": relationships},
        }
    except HTTPException as e:
        raise as_envelope_exc(e)
    except Exception as exc:
        return server_error(exc, action="loading equipment")


@router.get(
    "/workOrders",
    summary="List work orders",
    description="Query the canonical `work_order` table from CDM output.",
    status_code=HTTPStatus.OK,
)
def listWorkOrders(
    plant_code_id: str = Query(..., description="Plant code ID"),
    limit: int = Query(default=100, ge=1, le=10000),
    offset: int = Query(default=0, ge=0),
    search: str | None = Query(default=None, max_length=200),
):

    errors = []
    plant_code_id_val = (plant_code_id or "").strip()
    if not plant_code_id_val:
        errors.append({"field": "plant_code_id", "message": "plant_code_id is required"})
    if limit < 1 or limit > 10000:
        errors.append({"field": "limit", "message": "Limit must be between 1 and 10000"})
    if offset < 0:
        errors.append({"field": "offset", "message": "Offset must be non-negative"})
    if search is not None:
        search_val = search.strip()
        if not search_val:
            errors.append({"field": "search", "message": "search query cannot be empty if provided"})
        elif len(search_val) > 200:
            errors.append({"field": "search", "message": "search query cannot exceed 200 characters"})
        search = search_val

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
        if _use_postgres():
            res = _read_pg_table("work_order", limit, offset, search, plant_code_id)
        else:
            res = _read_csv_table("work_order", limit, offset, search)
        return {
            "success": True,
            "message": "Work orders listed successfully",
            "data": res,
        }
    except HTTPException as e:
        raise as_envelope_exc(e)
    except Exception as exc:
        return server_error(exc, action="listing work orders")


@router.get(
    "/relationships",
    summary="List CDM relationships",
    description="Query the `asset_relationship` table. "
    "Optionally filter by `relationship_type` (e.g. EXECUTED_ON, HAS_TAG, FLOW).",
    status_code=HTTPStatus.OK,
)
def listRelationships(
    plant_code_id: str = Query(..., description="Plant code ID"),
    relationship_type: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=10000),
    offset: int = Query(default=0, ge=0),
):

    errors = []
    plant_code_id_val = (plant_code_id or "").strip()
    if not plant_code_id_val:
        errors.append({"field": "plant_code_id", "message": "plant_code_id is required"})
    if limit < 1 or limit > 10000:
        errors.append({"field": "limit", "message": "Limit must be between 1 and 10000"})
    if offset < 0:
        errors.append({"field": "offset", "message": "Offset must be non-negative"})

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
    if relationship_type is not None:
        relationship_type = relationship_type.strip()

    try:
        _validate_plant_code(plant_code_id)

        try:
            rel_path = _resolve_table_path("asset_relationship")
        except HTTPException:
            if _use_postgres():
                res = _read_pg_table(
                    "asset_relationship", limit, offset, None, plant_code_id
                )
                return {
                    "success": True,
                    "message": "Relationships listed successfully",
                    "data": res,
                }
            raise HTTPException(
                status_code=HTTPStatus.NOT_FOUND,
                detail={
                    "success": False,
                    "message": "Relationship table not found",
                    "data": None,
                },
            )

        df = _read_table_file(rel_path)
        if relationship_type:
            df = df[df["relationship_type"] == relationship_type]
        if plant_code_id:
            df = df[df["plant_code_id"] == plant_code_id]

        total = len(df)
        df = df.iloc[offset : offset + limit]

        res = {
            "table": "asset_relationship",
            "total": total,
            "offset": offset,
            "limit": limit,
            "columns": list(df.columns),
            "rows": df.fillna("").to_dict(orient="records"),
        }
        return {
            "success": True,
            "message": "Relationships listed successfully",
            "data": res,
        }
    except HTTPException as e:
        raise as_envelope_exc(e)
    except Exception as exc:
        return server_error(exc, action="listing relationships")


@router.get(
    "/query/{tableName}",
    summary="Query any CDM table",
    description="Generic query endpoint for any CDM table in the final output. "
    "Reads from PostgreSQL if configured, otherwise from CSV files.",
    status_code=HTTPStatus.OK,
)
def queryTable(
    tableName: str,
    plant_code_id: str = Query(..., description="Plant code ID"),
    limit: int = Query(default=100, ge=1, le=10000),
    offset: int = Query(default=0, ge=0),
    search: str | None = Query(default=None, max_length=200),
):

    errors = []
    plant_code_id_val = (plant_code_id or "").strip()
    if not plant_code_id_val:
        errors.append({"field": "plant_code_id", "message": "plant_code_id is required"})
    table_name_val = (tableName or "").strip()
    if not table_name_val:
        errors.append({"field": "tableName", "message": "tableName is required"})
    elif not table_name_val.replace("_", "").isalnum():
        errors.append({"field": "tableName", "message": "Invalid table name"})

    if limit < 1 or limit > 10000:
        errors.append({"field": "limit", "message": "Limit must be between 1 and 10000"})
    if offset < 0:
        errors.append({"field": "offset", "message": "Offset must be non-negative"})
    if search is not None:
        search_val = search.strip()
        if not search_val:
            errors.append({"field": "search", "message": "search query cannot be empty if provided"})
        elif len(search_val) > 200:
            errors.append({"field": "search", "message": "search query cannot exceed 200 characters"})
        search = search_val

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
    tableName = table_name_val
    table_name = tableName

    try:
        _validate_plant_code(plant_code_id)

        if _use_postgres():
            res = _read_pg_table(table_name, limit, offset, search, plant_code_id)
        else:
            res = _read_csv_table(table_name, limit, offset, search)
        return {
            "success": True,
            "message": f"Table '{table_name}' queried successfully",
            "data": res,
        }
    except HTTPException as e:
        raise as_envelope_exc(e)
    except Exception as exc:
        return server_error(exc, action="query table")
