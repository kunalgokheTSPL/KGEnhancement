"""Shared response helpers for the p0 CDM API (guideline 5 envelope)."""

import logging
import re
from http import HTTPStatus

from fastapi import HTTPException
from fastapi.responses import JSONResponse, Response
from fastapi.routing import APIRoute

_log = logging.getLogger("p0.api.responses")


def _error_field(message: str, status_code: int) -> str:
    """Best-effort field name for a generic error, matching the envelope guideline."""
    if "plant_code_id" in message:
        return "plant_code_id"
    if status_code in (401, 403) or "token" in message.lower():
        return "access_token"
    return "server" if status_code >= 500 else "request"


def error_envelope(status_code: int, message: str, field: str | None = None) -> dict:
    """The standard error body: {success, message, errors:[{field, message}]}."""
    return {
        "success": False,
        "message": message,
        "errors": [{"field": field or _error_field(message, status_code), "message": message}],
    }


_SECRET_PATTERN = re.compile(
    r"((?:password|passwd|pwd|token|secret|api[_-]?key|access[_-]?key)\s*[=:]\s*)[^\s;,'\"]+",
    re.IGNORECASE,
)
_URI_CREDENTIAL_PATTERN = re.compile(r"://[^\s/@]+:[^\s/@]+@")


def redact(text: str) -> str:
    """Strip credentials from text before it can reach a client or a log line."""
    if not text:
        return ""
    cleaned = _SECRET_PATTERN.sub(r"\1***", text)
    return _URI_CREDENTIAL_PATTERN.sub("://***:***@", cleaned)


_MISSING_COLUMN_PATTERN = re.compile(r"column\s+[\"']?([\w.]+)[\"']?\s+does not exist", re.I)
_MISSING_TABLE_PATTERN = re.compile(r"relation\s+[\"']?([\w.]+)[\"']?\s+does not exist", re.I)


def _database_message(text: str, plant_code_id: str | None) -> str | None:
    """Human sentence for the database failures p0 actually hits."""
    lowered = text.lower()
    scope = f" for plant '{plant_code_id}'" if plant_code_id else ""
    table_match = _MISSING_TABLE_PATTERN.search(text)
    if table_match:
        return (
            f"The table '{table_match.group(1)}' does not exist{scope}. "
            "The plant database may not be fully provisioned."
        )
    column_match = _MISSING_COLUMN_PATTERN.search(text)
    if column_match:
        return (
            f"The column '{column_match.group(1)}' does not exist{scope}. "
            "The plant database schema is out of date."
        )
    if "could not connect" in lowered or "connection refused" in lowered:
        return f"Could not reach the database{scope}. The database may be down or unreachable."
    if "does not exist" in lowered and "database" in lowered:
        return f"The database{scope} does not exist. The plant may not be provisioned yet."
    if "duplicate key" in lowered or "unique constraint" in lowered:
        return "That record already exists."
    if "deadlock" in lowered or "lock timeout" in lowered:
        return "The database is busy with another operation. Please retry in a moment."
    return None


def _storage_message(text: str, plant_code_id: str | None) -> str | None:
    """Human sentence for object-storage failures."""
    lowered = text.lower()
    if "nosuchbucket" in lowered or "bucket does not exist" in lowered:
        return "The object storage container does not exist. Storage may not be provisioned."
    if "nosuchkey" in lowered or "404" in lowered and "s3" in lowered:
        return "The requested file was not found in object storage."
    if "accessdenied" in lowered or "forbidden" in lowered:
        return "Access to object storage was denied. Check the storage credentials."
    if "endpointconnectionerror" in lowered or "failed to establish a new connection" in lowered:
        return "Could not reach object storage. The storage service may be down."
    return None


def human_message(
    exc: Exception,
    *,
    action: str | None = None,
    plant_code_id: str | None = None,
) -> str:
    """Turn an exception into a sentence a user can act on, never a raw traceback."""
    if isinstance(exc, HTTPException):
        detail = exc.detail
        if isinstance(detail, dict):
            return str(detail.get("message") or detail.get("detail") or detail)
        return redact(str(detail))

    raw = redact(str(exc) or exc.__class__.__name__)
    name = exc.__class__.__name__
    doing = f" while {action}" if action else ""

    database = _database_message(raw, plant_code_id)
    if database:
        return database
    storage = _storage_message(raw, plant_code_id)
    if storage:
        return storage

    if isinstance(exc, FileNotFoundError):
        target = getattr(exc, "filename", None)
        return f"The file '{target}' was not found." if target else f"A required file was not found{doing}."
    if isinstance(exc, PermissionError):
        return f"Permission was denied{doing}."
    if isinstance(exc, (TimeoutError,)) or "timeout" in raw.lower():
        return f"The operation timed out{doing}. Please retry."
    if isinstance(exc, KeyError):
        return f"A required field or column ({raw.strip(chr(39))}) is missing from the input data."
    if isinstance(exc, (ValueError, TypeError)) and raw:
        return f"The provided data could not be processed{doing}: {raw}"
    if isinstance(exc, MemoryError):
        return f"The server ran out of memory{doing}. The input may be too large."

    return (
        f"An unexpected error occurred{doing} ({name}). "
        "The details have been logged for support."
    )


def server_error(
    exc: Exception,
    *,
    action: str | None = None,
    plant_code_id: str | None = None,
    field: str = "server",
    status_code: int = HTTPStatus.INTERNAL_SERVER_ERROR,
) -> JSONResponse:
    """Standard 500 envelope carrying a human message; the raw error goes to the log."""
    _log.exception(
        "[p0] unhandled error%s%s: %s",
        f" while {action}" if action else "",
        f" (plant={plant_code_id})" if plant_code_id else "",
        redact(str(exc)),
    )
    message = human_message(exc, action=action, plant_code_id=plant_code_id)
    return JSONResponse(
        status_code=status_code,
        content={
            "success": False,
            "message": message,
            "errors": [{"field": field, "message": message}],
        },
    )


MAX_PAGE_SIZE = 1000
DEFAULT_PAGE_SIZE = 100


def paginate(items: list, page: int = 1, page_size: int = DEFAULT_PAGE_SIZE) -> tuple[list, dict]:
    """Slice a collection and return it with the standard Pagination object."""
    total_items = len(items or [])
    size = max(1, min(int(page_size or DEFAULT_PAGE_SIZE), MAX_PAGE_SIZE))
    total_pages = (total_items + size - 1) // size if total_items else 0
    current = max(1, int(page or 1))
    start = (current - 1) * size
    window = list(items or [])[start : start + size]
    return window, {
        "page": current,
        "page_size": size,
        "total_items": total_items,
        "total_pages": total_pages,
        "has_more": start + size < total_items,
        "next_cursor": None,
    }


def paginated_envelope(
    data: dict,
    *,
    items_key: str,
    page: int,
    page_size: int,
    message: str,
    empty_message: str,
    pagination_key: str = "pagination",
) -> dict:
    """Success envelope for a paginated list, with an honest message when it is empty."""
    window, pagination = paginate(data.get(items_key) or [], page, page_size)
    data[items_key] = window
    data[pagination_key] = pagination
    data["total"] = pagination["total_items"]
    data["is_empty"] = pagination["total_items"] == 0
    return {
        "success": True,
        "message": empty_message if data["is_empty"] else message,
        "data": data,
    }


def normalise_commit(
    result: dict,
    *,
    connector: str,
    commit_id=None,
    upload_batch_id: str | None = None,
    committed_by: str | None = None,
) -> dict:
    """One commit shape for all four connectors, added alongside the existing keys."""
    tables = result.get("tables")
    normalised: list[dict] = []

    if isinstance(tables, list):
        for entry in tables:
            if isinstance(entry, dict):
                row = dict(entry)
                row.setdefault("table", row.get("name"))
                row.setdefault("rows_written", row.get("rows") or 0)
                normalised.append(row)
            elif entry:
                normalised.append({"table": str(entry), "rows_written": 0})
    elif result.get("table"):
        normalised.append(
            {
                "table": result["table"],
                "rows_written": result.get("rows_written") or 0,
                "rows_updated": result.get("rows_updated") or 0,
                "rows_skipped": result.get("rows_noop") or 0,
                "rows_rejected": result.get("rows_rejected") or 0,
            }
        )

    for row in normalised:
        for key in ("rows_written", "rows_updated", "rows_skipped", "rows_rejected"):
            row.setdefault(key, 0)

    total = result.get("total_rows_written")
    if total is None:
        total = result.get("rows_written")
    if total is None:
        total = sum(row.get("rows_written") or 0 for row in normalised)

    result["connector"] = connector
    result["tables"] = normalised
    result["total_rows_written"] = total
    result["rows_written"] = result.get("rows_written", total)
    result.setdefault("status", "committed")
    if commit_id is not None:
        result["commit_id"] = commit_id
    if upload_batch_id is not None:
        result.setdefault("upload_batch_id", upload_batch_id)
    if committed_by is not None:
        result.setdefault("committed_by", committed_by)
    result.setdefault("rejected_rows", [])
    return result


def no_content() -> Response:
    """204 for operations that genuinely have no representation to return."""
    return Response(status_code=HTTPStatus.NO_CONTENT)


def nothing_to_commit(connector: str, plant_code_id: str) -> Response:
    """204 when the reviewed state exists but holds no rows — a genuine no-op."""
    _log.info(
        "[commit] %s/%s — reviewed state is empty, nothing to commit",
        plant_code_id, connector,
    )
    return no_content()


def not_processed_yet(connector: str, plant_code_id: str) -> JSONResponse:
    """409 when there is no reviewed state at all — the caller skipped a step."""
    return JSONResponse(
        status_code=HTTPStatus.CONFLICT,
        content={
            "success": False,
            "message": (
                f"Nothing has been processed for '{connector}' in plant "
                f"'{plant_code_id}' yet, so there is nothing to commit. Upload a file, "
                f"then run POST /pipeline/run with stage '{connector}' before committing."
            ),
            "errors": [
                {
                    "field": "connector",
                    "message": f"No processed output exists for '{connector}'.",
                }
            ],
        },
    )


def annotate_collection(data: dict, items_key: str) -> dict:
    """Stamp total/is_empty on a collection payload so the client never counts."""
    if not isinstance(data, dict):
        return data
    items = data.get(items_key)
    if items is None:
        items = []
    total = len(items) if hasattr(items, "__len__") else 0
    data.setdefault("total", total)
    data["is_empty"] = total == 0
    return data


def collection_envelope(
    data: dict,
    *,
    items_key: str,
    message: str,
    empty_message: str,
) -> dict:
    """Success envelope for a list payload, with an honest message when it is empty."""
    annotated = annotate_collection(data, items_key)
    is_empty = annotated.get("is_empty", False) if isinstance(annotated, dict) else False
    return {
        "success": True,
        "message": empty_message if is_empty else message,
        "data": annotated,
    }


def upload_outcome(
    data: dict,
    *,
    files_key: str = "files",
    noun: str = "files",
) -> dict | JSONResponse:
    """Honest upload envelope: all-rejected is a 422, not a success."""
    entries = data.get(files_key) or []
    accepted = [f for f in entries if f.get("status") not in ("rejected", "failed")]
    rejected = [f for f in entries if f.get("status") in ("rejected", "failed")]
    data["accepted_count"] = len(accepted)
    data["rejected_count"] = len(rejected)
    data["is_partial"] = bool(accepted) and bool(rejected)

    if entries and not accepted:
        reasons = [
            {"field": f.get("name") or files_key, "message": f.get("error") or "File was rejected."}
            for f in rejected
        ]
        return JSONResponse(
            status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
            content={
                "success": False,
                "message": f"None of the {len(rejected)} {noun} could be uploaded.",
                "errors": reasons,
                "data": data,
            },
        )

    if rejected:
        message = (
            f"{len(accepted)} of {len(entries)} {noun} uploaded successfully; "
            f"{len(rejected)} rejected."
        )
    elif accepted:
        message = f"{len(accepted)} {noun} uploaded successfully."
    else:
        message = f"No {noun} were provided."

    return {"success": True, "message": message, "data": data}


def as_envelope_exc(exc: HTTPException) -> HTTPException:
    """Return exc with its detail normalised to the standard error envelope."""
    detail = exc.detail
    if isinstance(detail, dict) and "success" in detail:
        return exc
    message = detail if isinstance(detail, str) else str(detail)
    return HTTPException(
        status_code=exc.status_code,
        detail=error_envelope(exc.status_code, message),
    )


class EnvelopeRoute(APIRoute):
    """Route class that renders raised HTTPExceptions in the standard error envelope."""

    def get_route_handler(self):
        original = super().get_route_handler()

        async def handler(request):
            try:
                return await original(request)
            except HTTPException as exc:
                detail = exc.detail
                if isinstance(detail, dict) and "errors" in detail:
                    body = detail
                elif isinstance(detail, dict) and "success" in detail:
                    body = error_envelope(exc.status_code, str(detail.get("message", "")))
                else:
                    message = detail if isinstance(detail, str) else str(detail)
                    body = error_envelope(exc.status_code, message)
                return JSONResponse(status_code=exc.status_code, content=body)

        return handler
