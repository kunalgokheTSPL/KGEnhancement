from fastapi import Body
import io
import json
import uuid
from datetime import datetime
from http import HTTPStatus
import pandas as pd
from fastapi import APIRouter, Cookie, File, Header, UploadFile, Form
from fastapi.responses import JSONResponse
from psycopg2.extras import Json, execute_values
from p2.utility.database_driver import PostgresDriver
from utility.middleware import plant_code_ctx

NODE_SHEET = "node"
REL_SHEET = "relationship"
BLANK_LINEAGE = (
    None  # -> SQL NULL. Use Json({}) or Json([]) if you'd rather store empty json.
)

router = APIRouter(prefix="/graph", tags=["Graph"])


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def is_blank(value) -> bool:
    try:
        return value is None or pd.isna(value)
    except (TypeError, ValueError):
        return False


def clean_str(value):
    """Trim a cell to a clean string, or None if blank."""
    if is_blank(value):
        return None
    text = str(value).strip()
    return text or None


def parse_properties(value):
    """Turn an Excel cell into a jsonb-friendly value (always returns Json(...))."""
    if is_blank(value):
        return Json({})
    if isinstance(value, dict):
        return Json(value)
    text = str(value).strip()
    if not text:
        return Json({})
    try:
        return Json(json.loads(text))
    except (json.JSONDecodeError, TypeError, ValueError):
        # cell wasn't valid JSON -> keep the raw text so nothing is lost
        return Json({"value": text})


# --------------------------------------------------------------------------- #
# endpoint
# --------------------------------------------------------------------------- #
@router.post("/kgUpload", status_code=HTTPStatus.OK)
async def upload_excel(
    file: UploadFile = File(...),
    plant_code_id: str = Form(...),
    access_token: str = Cookie(None),
):
    if not access_token:
        return JSONResponse(
            status_code=HTTPStatus.UNAUTHORIZED,
            content={
                "success": False,
                "message": "Authentication required",
                "errors": [
                    {"field": "access_token", "message": "Access token is required"}
                ],
            },
        )

    if not plant_code_id:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": [
                    {"field": "plant_code_id", "message": "Plant code ID is required"}
                ],
            },
        )
    token = plant_code_ctx.set(plant_code_id)

    # ---- validate file extension ------------------------------------------ #
    if not file.filename or not file.filename.lower().endswith((".xlsx", ".xls")):
        return JSONResponse(
            status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": [{"field": "file", "message": "Please upload an .xlsx or .xls file."}],
            },
        )

    content = await file.read()
    # ---- parse Excel -------------------------------------------------------- #
    try:
        sheets = pd.read_excel(io.BytesIO(content), sheet_name=None, dtype=object)
    except Exception as exc:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Failed to read Excel file",
                "errors": [{"field": "file", "message": f"Could not read Excel file: {exc}"}],
            },
        )

    if NODE_SHEET not in sheets:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Failed to read Excel file",
                "errors": [{"field": "file", "message": f'Missing sheet "{NODE_SHEET}".'}],
            },
        )
    if REL_SHEET not in sheets:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={
                "success": False,
                "message": "Failed to read Excel file",
                "errors": [{"field": "file", "message": f'Missing sheet "{REL_SHEET}".'}],
            },
        )

    node_df = sheets[NODE_SHEET]
    rel_df = sheets[REL_SHEET]

    now = datetime.now()  # same value used for created_at and updated_at
    errors: list[str] = []

    # ---- 1. build node rows ------------------------------------------------ #
    node_rows = []  # tuples ready for insert
    name_to_id: dict[str, str] = {}  # name -> node_id (from this upload)

    for i, row in node_df.iterrows():
        label = clean_str(row.get("label"))
        name = clean_str(row.get("name"))
        if not label or not name:
            errors.append(f'node row {i + 2}: "label" and "name" are required.')
            continue
        node_id = str(uuid.uuid4())
        node_rows.append(
            (
                node_id,
                label,
                name,
                parse_properties(row.get("properties")),
                BLANK_LINEAGE,
                now,
                now,
            )
        )
        if name in name_to_id:
            errors.append(
                f'node row {i + 2}: duplicate name "{name}" in sheet; '
                f"relationships using this name may link to the wrong node."
            )
        name_to_id[name] = node_id  # last one wins on duplicate names

    # ---- 2. parse relationship rows --------------------------------------- #
    rel_parsed = []  # (excel_row, rel_type, from_name, to_name, properties_cell)
    ref_names: set[str] = set()
    for i, row in rel_df.iterrows():
        rel_type = clean_str(row.get("rel_type"))
        from_name = clean_str(row.get("from_name"))
        to_name = clean_str(row.get("to_name"))
        if not rel_type or not from_name or not to_name:
            errors.append(
                f"relationship row {i + 2}: "
                f'"rel_type", "from_name" and "to_name" are required.'
            )
            continue
        ref_names.update((from_name, to_name))
        rel_parsed.append((i, rel_type, from_name, to_name, row.get("properties")))

    # --------------------------------------------------------------------- #
    # 3. write everything in a single transaction
    # --------------------------------------------------------------------- #
    db = PostgresDriver()
    db.connect()
    conn = db.conn
    rel_rows = []
    try:
        cur = conn.cursor()

        # Create kg_nodes table if it doesn't exist
        cur.execute("""
        CREATE TABLE IF NOT EXISTS public.kg_nodes (
            node_id      TEXT PRIMARY KEY,
            label        TEXT NOT NULL,
            name         TEXT NOT NULL,
            properties   JSONB DEFAULT '{}',
            lineage      JSONB DEFAULT '{}',
            created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        # Create kg_relationships table if it doesn't exist
        cur.execute("""
        CREATE TABLE IF NOT EXISTS public.kg_relationships (
            relationship_id TEXT PRIMARY KEY,
            from_node_id    TEXT NOT NULL REFERENCES public.kg_nodes(node_id) ON DELETE CASCADE,
            to_node_id      TEXT NOT NULL REFERENCES public.kg_nodes(node_id) ON DELETE CASCADE,
            rel_type        TEXT NOT NULL,
            properties      JSONB DEFAULT '{}',
            lineage         JSONB DEFAULT '{}',
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        # Create indexes 
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_nodes_label
        ON public.kg_nodes(label);
        """)

        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_rel_from
        ON public.kg_relationships(from_node_id);
        """)

        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_rel_to
        ON public.kg_relationships(to_node_id);
        """)

        if node_rows:
            execute_values(
                cur,
                """INSERT INTO kg_nodes
                       (node_id, label, name, properties, lineage, created_at, updated_at)
                   VALUES %s""",
                node_rows,
            )

        # resolve any referenced names that weren't created in this upload
        unknown = [n for n in ref_names if n not in name_to_id]
        if unknown:
            cur.execute(
                "SELECT name, node_id FROM kg_nodes WHERE name = ANY(%s)",
                (unknown,),
            )
            for name, node_id in cur.fetchall():
                name_to_id.setdefault(name, node_id)  # don't overwrite fresh inserts

        # build relationship rows now that we can resolve ids
        for i, rel_type, from_name, to_name, props in rel_parsed:
            from_id = name_to_id.get(from_name)
            to_id = name_to_id.get(to_name)
            if not from_id:
                errors.append(
                    f'relationship row {i + 2}: no node found named "{from_name}".'
                )
                continue
            if not to_id:
                errors.append(
                    f'relationship row {i + 2}: no node found named "{to_name}".'
                )
                continue
            rel_rows.append(
                (
                    str(uuid.uuid4()),
                    from_id,
                    to_id,
                    rel_type,
                    parse_properties(props),
                    BLANK_LINEAGE,
                    now,
                    now,
                )
            )

        if rel_rows:
            execute_values(
                cur,
                """INSERT INTO kg_relationships
                       (relationship_id, from_node_id, to_node_id, rel_type,
                        properties, lineage, created_at, updated_at)
                   VALUES %s""",
                rel_rows,
            )

        conn.commit()
    except Exception as exc:
        conn.rollback()
        print(f"KG upload failed, transaction rolled back: {exc}")
        return JSONResponse(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            content={
                "success": False,
                "message": "Internal server error",
                "errors": [{"field": "server", "message": f"Import failed, transaction rolled back: {exc}"}],
            },
        )
    finally:
        db.close()
        plant_code_ctx.reset(token)
 

    return {
        "success": True,
        "message": "File uploaded successfully.",
        "data": {
            "nodes_inserted": len(node_rows),
            "relationships_inserted": len(rel_rows),
            "errors": errors,
        },
    }
