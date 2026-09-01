import json
import threading
from typing import Dict, List

# Max API calls tracked per session
MAX_BREADCRUMBS_PER_SESSION = 50

# In-memory store: session_id → list of API calls
_session_history: Dict[str, List[dict]] = {}

# NEW: separate counter per session — tracks TOTAL calls ever made
_session_counters: Dict[str, int] = {}

# Thread lock for safe concurrent access under high traffic
_lock = threading.Lock()


def record_api_call(
    session_id: str,
    endpoint: str,
    method: str,
    status_code: int,
) -> None:
    #not creating session breadcrumbs for sessions where we dont have the valid session_id
    if not session_id or session_id == "unauthenticated":
        return

    try:
        #shared sesion_history
        with _lock:
            # Initialize session if first call
            if session_id not in _session_history:
                _session_history[session_id] = []
                _session_counters[session_id] = 0  # NEW: init counter

            # NEW: increment total call counter - reflects true total calls
            _session_counters[session_id] += 1
            seq = _session_counters[session_id]  # true sequential number

            history = _session_history[session_id]

            # FIFO cap — drop oldest when buffer full (unchanged)
            if len(history) >= MAX_BREADCRUMBS_PER_SESSION:
                history.pop(0)

            # Append with correct seq from counter (not len-based)
            history.append({
                "seq":      seq,
                "endpoint": endpoint,
                "method":   method,
                "status":   status_code,
            })
    except Exception:
        pass  # must never crash the request


def get_session_breadcrumbs(session_id: str) -> str:
    """
    Returns the full API call history for this session as JSON string.
    Called by middleware only on failure — never on success.
    """
    if not session_id or session_id == "unauthenticated":
        return "[]"
    try:
        with _lock:
            history = _session_history.get(session_id, [])
            return json.dumps(history)
    except Exception:
        return "[]"


def clear_session(session_id: str) -> None:
    """
    Remove session history and counter on logout, removal from in-memory store.
    """
    if not session_id:
        return
    try:
        with _lock:
            _session_history.pop(session_id, None)
            _session_counters.pop(session_id, None) 
    except Exception:
        pass