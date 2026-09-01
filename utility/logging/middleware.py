import json
import logging
import time
import uuid

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.types import ASGIApp

from utility.logging.config import ApiLogRecord
from utility.logging.error_utils import extract_error_location
from utility.logging.breadcrumb_utils import (
    _init_breadcrumbs,
    _collect_breadcrumbs,
    add_breadcrumb,
)
from utility.logging.context import (
    trace_id_var,
    parent_trace_id_var,
    session_id_var,
    user_id_var,
)
from utility.logging.session_breadcrumbs import (
    record_api_call,
    get_session_breadcrumbs,
)

# Direct Redis access — same driver and key pattern used by SessionManager
from utility.database_driver import redis_driver as _get_redis_driver
_redis_driver = _get_redis_driver()
_redis = _redis_driver.redis  # redis.asyncio.Redis client

logger = logging.getLogger(__name__)


class ApiLoggingMiddleware(BaseHTTPMiddleware):
    """
    Identity is read from request.state populated by EntraAuthMiddleware (Redis).
    """

    def __init__(self, app: ASGIApp, manager) -> None:
        super().__init__(app)
        self.manager = manager

    async def dispatch(
        self,
        request: Request,
        call_next: RequestResponseEndpoint,
    ) -> Response:

        # SETUP
        start_time = time.perf_counter()
        request_timestamp = int(time.time() * 1000)

        if not hasattr(request.state, "logging_context"):
            request.state.logging_context = {}

        _init_breadcrumbs()
        add_breadcrumb("Request Received")

        # TRACE ID
        # Reuse X-Trace-Id if an upstream service passed one.
        # incoming_trace_id = request.headers.get("X-Trace-Id")
        # parent_trace_id = request.headers.get("X-Parent-Trace-Id")
        trace_id = str(uuid.uuid4())

        request.state.logging_context["trace_id"] = trace_id
        # if parent_trace_id:
        #     request.state.logging_context["parent_trace_id"] = parent_trace_id

        t_tok = trace_id_var.set(trace_id)
        p_tok = parent_trace_id_var.set(None)

        #SESSION ID + USER IDENTITY
        # direct redis fetching session data for logging context 

        session_id = "unauthenticated"
        user_id = "guest"

        try:
            cookie_session_id = request.cookies.get("session_id", "").strip()

            if cookie_session_id:
                # Direct Redis call - matching session_key pattern from session_manager
                session_key = f"session:{cookie_session_id}"
                data = await _redis.hgetall(session_key)

                if data:
                    # Session found and valid — extract identity fields
                    session_id = cookie_session_id
                    user_id = (
                        data.get("email") or
                        data.get("username") or
                        "guest"
                    )
                # else: empty dict = session expired or not found
                # session_id and user_id stay at safe defaults

        except Exception as redis_err:
            # Redis down, timeout, or unexpected error
            logger.warning(
                "Redis session fetch failed for logging — "
                "falling back to guest | error=%s", redis_err
            )

        request.state.logging_context["session_id"] = session_id
        request.state.logging_context["user_id"] = user_id
        u_tok = user_id_var.set(user_id)
        s_tok = session_id_var.set(session_id)

        add_breadcrumb("Authenticated" if user_id != "guest" else "Unauthenticated")

        #REQUEST EXECUTION
        success = True
        error_message = ""
        status_code = 500
        response = None

        try:
            add_breadcrumb("Handler Started")
            response = await call_next(request)
            status_code = response.status_code

            if status_code >= 400:
                success = False
                error_message = f"HTTP Error {status_code}"
                add_breadcrumb(f"Response: {status_code}", status="FAILED")
            else:
                add_breadcrumb(f"Response: {status_code}")

            return response

        except Exception as exc:
            # Fires only for exceptions that escape ALL sub-app handlers
            success = False
            error_message = f"{type(exc).__name__}: {str(exc)}"
            status_code = 500

            add_breadcrumb(
                f"Exception: {type(exc).__name__}",
                status="FAILED",
                details=str(exc)[:200],
            )

            try:
                error_location = extract_error_location(exc)
                request.state.logging_context.update(error_location)
                logger.error(
                    "API failure | trace_id=%s | endpoint=%s | %s",
                    trace_id,
                    request.url.path,
                    error_location,
                )
            except Exception as capture_exc:
                logger.error("Failed to capture error location: %s", capture_exc)

            raise exc

        finally:
            # RESET CONTEXTVARS
            # Proper reset via tokens — never leaks between concurrent requests
            trace_id_var.reset(t_tok)
            parent_trace_id_var.reset(p_tok)
            user_id_var.reset(u_tok)
            session_id_var.reset(s_tok)

            # BUILD AND ENQUEUE LOG RECORD 
            path = request.url.path
            is_ignored = (
                path == "/" or
                path.startswith("/logging") or
                path.startswith("/.well-known") or
                path.startswith("/p0/login") or
                path.startswith("/swagger") or
                path.endswith("/docs") or
                path.endswith("/openapi.json") or
                path.endswith("/redoc") or
                path.endswith("/favicon.ico")
            )

            if not is_ignored:
                latency_ms = (time.perf_counter() - start_time) * 1000

                # Record into session breadcrumb history
                record_api_call(session_id, path, request.method, status_code)

                # Collect within-request timeline
                timeline_json = _collect_breadcrumbs()
                #only stored on failure
                if not success and timeline_json != "[]":
                    request.state.logging_context["timeline"] = timeline_json

                # Attach session breadcrumbs on failure only
                if not success:
                    sb = get_session_breadcrumbs(session_id)
                    if sb != "[]":
                        request.state.logging_context["session_breadcrumbs"] = sb

                # Build flat string-keyed context dict
                context: dict = {}
                if hasattr(request.state, "logging_context") and isinstance(
                    request.state.logging_context, dict
                ):
                    context = {
                        str(k): str(v)
                        for k, v in request.state.logging_context.items()
                        if not k.startswith("_")
                    }

                log_record = ApiLogRecord(
                    request_timestamp=request_timestamp,
                    api_endpoint=path,
                    http_method=request.method,
                    status_code=status_code,
                    success=success,
                    error_message=error_message,
                    latency_ms=latency_ms,
                    context=context,
                )

                # Queue failures must never affect the HTTP response
                try:
                    self.manager.enqueue_log(log_record)
                except Exception as e:
                    logger.error(f"Failed to enqueue API log record: {e}")