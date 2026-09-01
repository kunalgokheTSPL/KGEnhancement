from typing import Dict
from utility.logging.context import get_current_trace_context

def get_tracing_headers() -> Dict[str, str]:
    try:
        ctx = get_current_trace_context()
        headers: Dict[str, str] = {}

        if ctx.get("trace_id"):
            headers["X-Parent-Trace-Id"] = ctx["trace_id"]

        if ctx.get("user_id") and ctx["user_id"] != "guest":
            headers["X-User-Id"] = ctx["user_id"]

        return headers
    except Exception:
        return {}