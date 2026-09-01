import contextvars
from typing import Optional

# Request correlation
trace_id_var: contextvars.ContextVar[Optional[str]] = (
    contextvars.ContextVar("trace_id", default=None)
)

parent_trace_id_var: contextvars.ContextVar[Optional[str]] = (
    contextvars.ContextVar("parent_trace_id", default=None)
)

# Identity
session_id_var: contextvars.ContextVar[Optional[str]] = (
    contextvars.ContextVar("session_id", default=None)
)

user_id_var: contextvars.ContextVar[Optional[str]] = (
    contextvars.ContextVar("user_id", default=None)
)


def get_current_trace_context() -> dict:
    return {
        "trace_id": trace_id_var.get(),
        "parent_trace_id": parent_trace_id_var.get(),
        "session_id": session_id_var.get(),
        "user_id": user_id_var.get(),
    }