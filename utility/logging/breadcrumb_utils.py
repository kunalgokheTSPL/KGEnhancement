import contextvars
import json
from typing import Dict, List, Optional

_breadcrumbs_var: contextvars.ContextVar[Optional[List[Dict]]] = (
    contextvars.ContextVar("request_breadcrumbs", default=None)
)

_MAX_BREADCRUMBS = 50
_MAX_DETAIL_LENGTH = 200


def add_breadcrumb(
    stage: str,
    status: str = "SUCCESS",
    details: Optional[str] = None,
) -> None:
    try:
        crumbs = _breadcrumbs_var.get()
        if crumbs is None:
            return
        if len(crumbs) >= _MAX_BREADCRUMBS:
            return

        crumb: Dict = {
            "step": len(crumbs) + 1,
            "stage": str(stage),
            "status": str(status),
        }
        if details is not None:
            crumb["details"] = str(details)[:_MAX_DETAIL_LENGTH]

        crumbs.append(crumb)
    except Exception:
        pass  # logging must NEVER crash the app


def _init_breadcrumbs() -> None:
    """Initialise the breadcrumb list for this request. Called by middleware."""
    _breadcrumbs_var.set([])


def _collect_breadcrumbs() -> str:
    crumbs = _breadcrumbs_var.get() or []
    _breadcrumbs_var.set(None)
    return json.dumps(crumbs)