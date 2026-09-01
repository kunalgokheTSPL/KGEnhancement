from datetime import datetime, timedelta, timezone
from typing import Optional
from fastapi import APIRouter, Query, HTTPException
import logging
import os

from utility.logging.config import LoggingSettings
# CHANGED: import get_log_storage instead of RustFSStorageBackend directly.
# get_log_storage reads DEPLOYMENT_MODE and returns the correct backend:
#   on_prem → RustFSStorageBackend (reads from RustFS)
#   azure   → ADLSStorageBackend   (reads from Azure Blob container)
from utility.logging.storage import get_log_storage
from utility.logging.protobuf.apilog_pb2 import ApiBatch

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/logs", tags=["Logs Overview"])

settings = LoggingSettings()

# uses get_log_storage(DEPLOYMENT_MODE) so on Azure deployment it reads
# from Azure Blob container, on on_prem it reads from RustFS. Same env var
# that manager.py uses for writing — reads and writes always go to same backend.
storage = get_log_storage(
    os.getenv("DEPLOYMENT_MODE", "on_prem"),
    bucket=settings.rustfs_bucket,
    endpoint=settings.rustfs_endpoint,
    access_key=settings.rustfs_access_key,
    secret_key=settings.rustfs_secret_key,
)


def parse_time_to_ms(time_str: str) -> int:
    """Parses a time string (UNIX timestamp, ISO 8601, or standard YYYY-MM-DD HH:MM:SS) to milliseconds."""
    time_str = time_str.strip()
    if time_str.isdigit():
        val = int(time_str)
        if val < 10000000000:
            return val * 1000
        return val
    normalized_str = time_str
    if normalized_str.endswith("Z"):
        normalized_str = normalized_str[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(normalized_str)
        return int(dt.timestamp() * 1000)
    except ValueError:
        pass
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%d",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(time_str, fmt)
            return int(dt.timestamp() * 1000)
        except ValueError:
            continue
    raise ValueError(f"Unsupported format. Use YYYY-MM-DD HH:MM:SS, ISO-8601, or UNIX timestamp.")


def parse_relative_time_to_ms(relative_str: str) -> int:
    """Parses relative time strings like '15m', '10 minutes', '1h', 'last 1 hour' and returns duration in ms."""
    import re
    relative_str = relative_str.strip().lower()
    if relative_str.startswith("last "):
        relative_str = relative_str[5:].strip()
    pattern = re.compile(r"^(\d+)\s*(ms|millisecond|s|second|m|minute|h|hour|d|day)s?$")
    match = pattern.match(relative_str)
    if not match:
        raise ValueError(
            f"Unsupported relative time format: '{relative_str}'. "
            f"Use format like '15m', '10 minutes', '1h', or 'last 2 hours'."
        )
    value = int(match.group(1))
    unit = match.group(2)
    if unit in ("ms", "millisecond"):
        return value
    elif unit in ("s", "second"):
        return value * 1000
    elif unit in ("m", "minute"):
        return value * 60 * 1000
    elif unit in ("h", "hour"):
        return value * 60 * 60 * 1000
    elif unit in ("d", "day"):
        return value * 24 * 60 * 60 * 1000
    raise ValueError(f"Unknown time unit: '{unit}'")


# SHARED HELPERS (extracted for reuse by both endpoints)

def _ensure_connected():
    """Ensures storage backend is connected. Works for both RustFS and ADLS.
    CHANGED: added None check before connecting — reuses existing connection
    instead of reconnecting on every request.
    Self-healing: if connection drops, client becomes None and reconnects automatically.
    """
    # Only connect if not already connected — avoids unnecessary reconnection overhead
    if storage.driver.client is None:
        try:
            storage.connect()
        except Exception as conn_err:
            raise HTTPException(
                status_code=503,
                detail=f"Failed to connect to storage backend: {conn_err}"
            )


def _read_pb_file(key: str) -> list:
    """
    Downloads and parses one .pb file from storage backend.
    CHANGED: uses storage.read(key) instead of driver.client.get_object().
    storage.read() works on both RustFS (S3 get_object) and ADLS (blob download).
    Returns list of log dicts. Returns [] on any error — never raises.
    """
    try:
        pb_bytes = storage.read(key)
        batch = ApiBatch()
        batch.ParseFromString(pb_bytes)
        records = []
        for log in batch.logs:
            records.append({
                "request_timestamp": log.request_timestamp,
                "request_time_utc": datetime.fromtimestamp(
                    log.request_timestamp / 1000.0, tz=timezone.utc
                ).isoformat(),
                "api_endpoint": log.api_endpoint,
                "http_method": log.http_method,
                "status_code": log.status_code,
                "success": log.success,
                "error_message": log.error_message,
                "latency_ms": log.latency_ms,
                "context": dict(log.context),
            })
        return records
    except Exception as e:
        logger.error(f"Failed to read or parse pb object {key}: {e}")
        return []


def _list_pb_keys_for_range(start_ts_ms: int, end_ts_ms: int) -> list:
    """
    Lists all .pb keys that could contain logs within the given time range.
    CHANGED: uses storage.list_files(prefix) instead of driver.client.list_objects_v2().
    storage.list_files() works on both RustFs and ADLS (Azure Blob API).
    Both return list of dicts with 'Key' field — identical shape.
    """
    start_date = datetime.fromtimestamp(start_ts_ms / 1000.0, tz=timezone.utc).date()
    end_date   = datetime.fromtimestamp(end_ts_ms   / 1000.0, tz=timezone.utc).date()
    keys = []
    curr = start_date
    while curr <= end_date:
        prefix = f"{curr.year:04d}/{curr.month:02d}/{curr.day:02d}/"
        try:
            contents = storage.list_files(prefix)
            for obj in contents:
                key = obj["Key"]
                filename = key.split("/")[-1]
                if not filename.endswith(".pb"):
                    continue
                try:
                    ts_str = filename.replace("_final.pb", "").replace(".pb", "")
                    file_ts_ms = int(ts_str) * 1000
                except ValueError:
                    continue
                batch_duration_ms = 60000
                if (file_ts_ms + batch_duration_ms >= start_ts_ms) and (file_ts_ms <= end_ts_ms):
                    keys.append(key)
        except Exception as e:
            logger.error(f"Failed to list objects for prefix {prefix}: {e}")
        curr += timedelta(days=1)
    return keys


# EXISTING ENDPOINT — pagination added
@router.get("/apilogs")
def get_logs(
    start_time: Optional[str] = Query(None, description="Start time (e.g. '2026-06-30 15:00:00', ISO-8601, or UNIX timestamp)"),
    end_time: Optional[str] = Query(None, description="End time (e.g. '2026-06-30 15:30:00', ISO-8601, or UNIX timestamp). Defaults to now."),
    relative_time: Optional[str] = Query(None, description="Relative window from end_time, e.g. '15m', '10 minutes', '1h', 'last 1 hour'"),
    # Pagination — aligned with existing
    page_no: int = Query(1, ge=1, description="Page number, starts from 1."),
    page_size: int = Query(20, ge=1, le=500, description="Records per page. Default 20, max 500."),
):
    """Fetches, decodes, and filters API log records from RustFS S3 .pb files within a time range."""
    try:
        if end_time:
            end_ts_ms = parse_time_to_ms(end_time)
        else:
            end_ts_ms = int(datetime.now().timestamp() * 1000)
        if relative_time:
            delta_ms = parse_relative_time_to_ms(relative_time)
            start_ts_ms = end_ts_ms - delta_ms
        elif start_time:
            start_ts_ms = parse_time_to_ms(start_time)
        else:
            start_ts_ms = end_ts_ms - (15 * 60 * 1000)
    except Exception as parse_err:
        raise HTTPException(status_code=400, detail=f"Invalid time format: {parse_err}")

    if start_ts_ms > end_ts_ms:
        raise HTTPException(status_code=400, detail="start_time must be before or equal to end_time")

    max_range_ms = 30 * 24 * 60 * 60 * 1000
    if (end_ts_ms - start_ts_ms) > max_range_ms:
        raise HTTPException(
            status_code=400,
            detail="The requested query time range is too large. Please limit the query window to a maximum of 30 days."
        )

    _ensure_connected()

    matching_logs = []
    for key in _list_pb_keys_for_range(start_ts_ms, end_ts_ms):
        for record in _read_pb_file(key):
            if start_ts_ms <= record["request_timestamp"] <= end_ts_ms:
                matching_logs.append(record)

    matching_logs.sort(key=lambda x: x["request_timestamp"]) 

    # total_records reflects filtered count — frontend uses this to calculate pages
    # page_no/page_size validated by FastAPI (ge=1, le=500) — no manual check needed
    total_records = len(matching_logs)
    total_pages   = max(1, -(-total_records // page_size))
    start_idx     = (page_no - 1) * page_size
    end_idx       = start_idx + page_size
    paginated_logs = matching_logs[start_idx:end_idx]

    return {
        "success": True,
        "start_time_ms": start_ts_ms,
        "end_time_ms":   end_ts_ms,
        "logs": paginated_logs,           # kept as "logs" — frontend already integrates with this key
        "pagination": {                   
            "page_no":       page_no,
            "page_size":     page_size,
            "total_records": total_records,
            "total_pages":   total_pages,
            "has_next":      page_no < total_pages,
        }
    }


#NEW ENDPOINT — detail view for one failed request
@router.get("/apilogs/{trace_id}")
def get_log_detail(
    trace_id: str,
    request_timestamp: int = Query(
        ...,
        description=(
            "request_timestamp in milliseconds — from the list response. "
            "Used to locate the exact .pb file without scanning all of RustFS."
        )
    ),
):
    """
    Returns the complete log record for one specific request by trace_id
    """
    if not trace_id or len(trace_id.strip()) < 8:
        raise HTTPException(status_code=400, detail="Invalid trace_id.")

    _ensure_connected()

    # Search ±60s around timestamp — covers any batch boundary edge case
    # This means at most 2-3 .pb files are read — never a full RustFS scan
    search_start_ms = request_timestamp - 60_000
    search_end_ms   = request_timestamp + 60_000

    for key in _list_pb_keys_for_range(search_start_ms, search_end_ms):
        for record in _read_pb_file(key):
            if record.get("context", {}).get("trace_id") == trace_id:
                return record

    raise HTTPException(
        status_code=404,
        detail=f"No log found for trace_id '{trace_id}' near timestamp {request_timestamp}."
    )