"""Server-computed pipeline progress: stage plans, marker parsing, and output streaming."""

from __future__ import annotations

import json
import logging
import subprocess
import threading
from typing import Callable

from p0.utils.progress import FILE_MARKER, MARKER, emit, emit_file  # noqa: F401

_log = logging.getLogger("p0.pipeline.progress")


# `file_wise` marks the phase whose progress is driven by files done / files total.
# Every stage has one, because the API knows the file set from flow_state before it spawns.
STAGE_PLANS: dict[str, list[dict]] = {
    "docs": [
        {"phase": "validate", "label": "Validating files", "weight": 5, "determinate": True},
        {"phase": "extract", "label": "Extracting documents", "weight": 70,
         "determinate": True, "file_wise": True},
        {"phase": "load", "label": "Building canonical rows", "weight": 20, "determinate": True},
        {"phase": "finalize", "label": "Finalising", "weight": 5, "determinate": True},
    ],
    
        "alerts": [
        {
            "phase": "validate",
            "label": "Inspecting ALERTS workbook",
            "weight": 10,
            "determinate": True,
        },
        {
            "phase": "extract",
            "label": "Reading ALERTS workbook",
            "weight": 65,
            "determinate": True,
            "file_wise": True,
        },
        {
            "phase": "load",
            "label": "Writing processed ALERTS data",
            "weight": 20,
            "determinate": True,
        },
        {
            "phase": "complete",
            "label": "ALERTS processing complete",
            "weight": 5,
            "determinate": True,
        },
    ],
    
    "other_files": [
    {
        "phase": "validate",
        "label": "Validating Other Files",
        "weight": 10,
        "determinate": True,
    },
    {
        "phase": "process",
        "label": "Processing Other Files",
        "weight": 65,
        "determinate": True,
        "file_wise": True,
    },
    {
        "phase": "load",
        "label": "Writing processed output",
        "weight": 20,
        "determinate": True,
    },
    {
        "phase": "finalize",
        "label": "Finalising",
        "weight": 5,
        "determinate": True,
    },
],
    
    "full": [
        {"phase": "consolidate", "label": "Consolidating CDM tables", "weight": 100,
         "determinate": True, "file_wise": True},
    ],
    "sap": [
        {"phase": "read", "label": "Reading SAP extracts", "weight": 20, "determinate": True},
        {"phase": "transform", "label": "Transforming to canonical", "weight": 60,
         "determinate": True, "file_wise": True},
        {"phase": "load", "label": "Writing entities", "weight": 20, "determinate": True},
    ],
    "ts": [
        {"phase": "read", "label": "Reading timeseries files", "weight": 20, "determinate": True},
        {"phase": "process", "label": "Processing timeseries", "weight": 60,
         "determinate": True, "file_wise": True},
        {"phase": "load", "label": "Writing entities", "weight": 20, "determinate": True},
    ],
    "pnid": [
        {"phase": "read", "label": "Reading extraction output", "weight": 20, "determinate": True},
        {"phase": "transform", "label": "Applying templates", "weight": 60,
         "determinate": True, "file_wise": True},
        {"phase": "load", "label": "Writing entities", "weight": 20, "determinate": True},
    ],
}

_FINDINGS_PLAN = [
    {"phase": "validate", "label": "Reading source files", "weight": 10, "determinate": True},
    {"phase": "extract", "label": "Extracting findings", "weight": 60,
     "determinate": True, "file_wise": True},
    {"phase": "load", "label": "Writing canonical output", "weight": 25, "determinate": True},
    {"phase": "finalize", "label": "Finalising", "weight": 5, "determinate": True},
]

for _findings_stage in ("aif", "gloc", "lopc", "upd_event", "trip_event"):
    STAGE_PLANS[_findings_stage] = [dict(phase) for phase in _FINDINGS_PLAN]


def plan_for(stage: str) -> list[dict]:
    """The declared phase list for a stage, so the UI can render a stepper up front."""
    return [dict(p) for p in STAGE_PLANS.get(stage, STAGE_PLANS["ts"])]


def is_determinate(stage: str) -> bool:
    """True when at least one phase of the stage reports countable work."""
    return any(p.get("determinate") for p in STAGE_PLANS.get(stage, []))


def parse_marker(line: str) -> dict | None:
    """Return the payload of a ##P0PROGRESS line, or None for ordinary output."""
    if MARKER not in line:
        return None
    _, _, payload = line.partition(MARKER)
    payload = payload.strip()
    if not payload:
        return None
    try:
        data = json.loads(payload)
    except (ValueError, TypeError):
        _log.debug("[progress] unparseable marker: %s", payload[:200])
        return None
    return data if isinstance(data, dict) else None


FAILED_FILE_STATUSES = {"failed", "error", "rejected"}


def parse_file_marker(line: str) -> dict | None:
    """Return the payload of a ##P0FILE line, or None for ordinary output."""
    if FILE_MARKER not in line:
        return None
    _, _, payload = line.partition(FILE_MARKER)
    payload = payload.strip()
    if not payload:
        return None
    try:
        data = json.loads(payload)
    except (ValueError, TypeError):
        _log.debug("[progress] unparseable file marker: %s", payload[:200])
        return None
    if not isinstance(data, dict) or not data.get("file"):
        return None
    return data


def apply_file_marker(job: dict, stage: str | None = None, payload: dict | None = None) -> None:
    """Record one file's outcome, keep counters in step, and advance file-wise progress."""
    if payload is None and isinstance(stage, dict):
        stage, payload = None, stage
    outcomes = job.setdefault("file_outcomes", {})
    name = _leaf_name(payload["file"])
    entry = outcomes.setdefault(name, {"file": name})
    entry["status"] = payload.get("status") or "processed"
    if payload.get("error"):
        entry["error"] = payload["error"]
    if isinstance(payload.get("rows"), int):
        entry["rows"] = payload["rows"]

    counters = summarise_files(outcomes)
    declared = job.get("files_total") or 0
    counters["files_total"] = max(counters["files_total"], declared)
    job["counters"] = counters
    job["current_item"] = name

    settled = counters["files_done"] + counters["files_failed"] + counters["files_skipped"]
    total = counters["files_total"]
    if total > 0:
        job["items_done"] = settled
        job["items_total"] = total
        percent = file_percent(stage, settled, total)
        job["progress_percent"] = max(job.get("progress_percent") or 0, percent)
        job["is_determinate"] = True


def _leaf_name(name) -> str:
    """Compare files by leaf name; the pipeline and flow_state disagree on prefixes."""
    return str(name or "").strip().replace("\\", "/").rsplit("/", 1)[-1]


def summarise_files(outcomes: dict[str, dict]) -> dict[str, int]:
    """Counts the UI renders without touching the file list itself."""
    failed = sum(1 for o in outcomes.values() if o.get("status") in FAILED_FILE_STATUSES)
    skipped = sum(1 for o in outcomes.values() if o.get("status") == "skipped")
    done = len(outcomes) - failed - skipped
    return {
        "files_total": len(outcomes),
        "files_done": done,
        "files_failed": failed,
        "files_skipped": skipped,
    }


def file_percent(stage: str | None, settled: int, total: int) -> int:
    """Per-file progress, scaled into the weight of the stage's file-processing phase."""
    if total <= 0:
        return 0
    fraction = max(0.0, min(1.0, settled / total))
    plan = STAGE_PLANS.get(stage or "", [])
    weights = sum(p["weight"] for p in plan) or 100
    before = 0
    span = weights
    for entry in plan:
        if entry.get("file_wise"):
            span = entry["weight"]
            break
        before += entry["weight"]
    else:
        before, span = 0, weights
    return int(round(100 * (before + span * fraction) / weights))


def outcome_status(returncode: int, outcomes: dict[str, dict] | None) -> str:
    """A run where some files worked and some did not is 'partial', not 'failed'."""
    if not outcomes:
        return "completed" if returncode == 0 else "failed"
    counts = summarise_files(outcomes)
    if counts["files_failed"] == 0:
        return "completed" if returncode == 0 else "partial"
    if counts["files_done"] > 0 or counts["files_skipped"] > 0:
        return "partial"
    return "failed"


def compute_percent(stage: str, phases_state: dict[str, dict]) -> int:
    """Weighted, monotonic run percentage across a stage's declared phases."""
    plan = STAGE_PLANS.get(stage)
    if not plan:
        return 0
    total_weight = sum(p["weight"] for p in plan) or 1
    earned = 0.0
    for entry in plan:
        state = phases_state.get(entry["phase"]) or {}
        status = state.get("status")
        if status in ("succeeded", "skipped"):
            fraction = 1.0
        elif status == "running":
            done = state.get("items_done")
            total = state.get("items_total")
            if isinstance(done, int) and isinstance(total, int) and total > 0:
                fraction = max(0.0, min(1.0, done / total))
            else:
                fraction = 0.0
        else:
            fraction = 0.0
        earned += entry["weight"] * fraction
    return int(round(100 * earned / total_weight))


def apply_marker(job: dict, stage: str, payload: dict) -> None:
    """Fold one progress marker into the job record, never letting percent go backwards."""
    phases = job.setdefault("phases", {})
    phase_key = str(payload.get("phase") or "").strip()
    if not phase_key:
        return

    entry = phases.setdefault(phase_key, {})
    entry["phase"] = phase_key
    if payload.get("label"):
        entry["label"] = payload["label"]
    if payload.get("status"):
        entry["status"] = payload["status"]
    else:
        entry.setdefault("status", "running")
    for key in ("items_done", "items_total"):
        value = payload.get(key)
        if isinstance(value, int) and value >= 0:
            entry[key] = value
    if "current_item" in payload:
        entry["current_item"] = payload.get("current_item")
    if payload.get("message"):
        entry["message"] = payload["message"]

    for other_key, other in phases.items():
        if other_key != phase_key and other.get("status") == "running":
            other["status"] = "succeeded"

    job["current_phase"] = phase_key
    job["current_label"] = entry.get("label") or phase_key
    job["current_item"] = entry.get("current_item")
    job["items_done"] = entry.get("items_done")
    job["items_total"] = entry.get("items_total")

    percent = compute_percent(stage, phases)
    previous = job.get("progress_percent") or 0
    job["progress_percent"] = max(previous, percent)
    job["is_determinate"] = is_determinate(stage)


def finalise_progress(job: dict, *, succeeded: bool) -> None:
    """Only a terminal run reports 100; a failed run keeps the percent it reached."""
    phases = job.get("phases") or {}
    for entry in phases.values():
        if entry.get("status") == "running":
            entry["status"] = "succeeded" if succeeded else "failed"
    if succeeded:
        job["progress_percent"] = 100
    job["current_item"] = None


def stream_process(
    proc: subprocess.Popen,
    *,
    timeout: int,
    on_line: Callable[[str], None] | None = None,
    on_kill: Callable[[subprocess.Popen], None] | None = None,
) -> tuple[int, str, str]:
    """Read stdout/stderr as they arrive so progress is live, and still honour the timeout."""
    out_chunks: list[str] = []
    err_chunks: list[str] = []

    def _drain(pipe, sink, notify):
        try:
            for raw in iter(pipe.readline, ""):
                sink.append(raw)
                if notify is not None:
                    try:
                        notify(raw)
                    except Exception as exc:
                        _log.warning("[progress] line handler failed: %s", exc)
        except Exception as exc:
            _log.warning("[progress] reader stopped: %s", exc)
        finally:
            try:
                pipe.close()
            except Exception:
                pass

    readers = [
        threading.Thread(target=_drain, args=(proc.stdout, out_chunks, on_line), daemon=True),
        threading.Thread(target=_drain, args=(proc.stderr, err_chunks, None), daemon=True),
    ]
    for reader in readers:
        reader.start()

    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        if on_kill is not None:
            on_kill(proc)
        else:
            proc.kill()
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            _log.error("[progress] process did not exit after kill")

    for reader in readers:
        reader.join(timeout=30)

    return proc.returncode, "".join(out_chunks), "".join(err_chunks)


