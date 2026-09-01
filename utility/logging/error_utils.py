import json
import os
from fastapi import Request

_NOISE_PATHS = (
    ".venv",
    "site-packages",
    "contextlib.py",
    "anyio",
    "starlette",
    "fastapi",
    os.path.join("Lib", "asyncio"),
    "Lib/asyncio",
    os.path.join("utility", "logging", "middleware.py"), 
    "utility/logging/middleware.py",
    "app.py",       
)

def _is_app_frame(filepath: str) -> bool:
    return not any(noise in filepath for noise in _NOISE_PATHS)


def _shorten_path(filepath: str) -> str:
    normalized = filepath.replace("\\", "/")
    for marker in ("utility/", "p0/", "p1/", "p2/", "deployment/", "jenkins/"):
        idx = normalized.find(marker)
        if idx != -1:
            return filepath[idx:]
    return filepath


def extract_error_location(exc: BaseException) -> dict:
    traceback = exc.__traceback__

    if traceback is None:
        return {
            "error_type": type(exc).__name__,
            "error_message": str(exc),
            "error_file": "unknown",
            "error_function": "unknown",
            "error_line": "-1",
            "error_stack": "[]",
        }

    frames = []
    current = traceback
    while current is not None:
        frame = current.tb_frame
        frames.append({
            "file": _shorten_path(frame.f_code.co_filename),
            "function": frame.f_code.co_name,
            "line": current.tb_lineno,
        })
        current = current.tb_next

    deepest = frames[-1] if frames else {
        "file": "unknown",
        "function": "unknown",
        "line": -1,
    }
    #Filtering frames to only include application frames
    app_frames = [f for f in frames if _is_app_frame(f["file"])]
    final_frames = app_frames if app_frames else frames
    deepest = final_frames[-1] if final_frames else deepest

    return {
        "error_type": type(exc).__name__,
        "error_message": str(exc),
        "error_file": deepest["file"],
        "error_function": deepest["function"],
        "error_line": str(deepest["line"]),
        "error_stack": json.dumps(final_frames),
    }


def attach_error_location(request: Request, exc: BaseException) -> None:
    try:
        if not hasattr(request.state, "logging_context"):
            request.state.logging_context = {}
        location = extract_error_location(exc)
        request.state.logging_context.update(location)
        try:
            from utility.logging.breadcrumb_utils import add_breadcrumb
            add_breadcrumb(
                f"Exception: {type(exc).__name__}",
                status="FAILED",
                details=str(exc)[:200],
            )
        except Exception:
            pass
    except Exception:
        pass