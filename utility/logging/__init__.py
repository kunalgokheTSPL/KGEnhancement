# Reusable API Logging Framework
# Core — same three exports as before, no change to existing import paths
from utility.logging.manager import LoggingManager
from utility.logging.middleware import ApiLoggingMiddleware
from utility.logging.config import LoggingSettings

# New utilities — optional, import from here or directly from the submodule
from utility.logging.error_utils import attach_error_location
from utility.logging.breadcrumb_utils import add_breadcrumb
from utility.logging.context import get_current_trace_context
from utility.logging.outbound_utils import get_tracing_headers
from utility.logging.subapp_utils import register_observability
from utility.logging.async_utils import ContextAwareThreadPoolExecutor

__all__ = [
    # Core (pre-existing)
    "LoggingManager",
    "ApiLoggingMiddleware",
    "LoggingSettings",
    # New utilities
    "attach_error_location",
    "add_breadcrumb",
    "get_current_trace_context",
    "get_tracing_headers",
    "register_observability",
    "ContextAwareThreadPoolExecutor",
]