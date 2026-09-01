"""Stage argument building, scope derivation and process control for pipeline runs."""

from __future__ import annotations

import logging
import os
import signal
import time
import subprocess
from datetime import datetime, timezone

from ..config import (
    CONFIG_DIR,
    LOG_DIR,
    RUSTFS_DATA_PNID,
    STAGING_DIR,
    USER_CONFIG_FILE,
    WORK_DIR,
    p0_DIR,
    rustfs_out_dir,
    rustfs_processed_docs,
    rustfs_processed_findings,
    rustfs_processed_pnid,
    rustfs_processed_sap_mapping,
    rustfs_processed_ts,
    rustfs_staging_docs,
    rustfs_staging_findings,
    rustfs_staging_pnid,
    rustfs_staging_sap,
    rustfs_staging_ts,
    rustfs_processed_alerts,
    rustfs_staging_alerts,
)
from ..deps import load_yaml

_log = logging.getLogger("cdm.api.pipeline")


_INDUSTRY_TO_TEMPLATE: dict[str, str] = {
    "refinery": "oil_gas",
    "lng": "oil_gas",
    "cement": "cement",
    "oil_gas": "oil_gas",
}

_STAGE_LOG_FILE: dict[str, str] = {
    "sap_mapping": "sap_mapping_pipeline.log",
    "pnid": "pnid_pipeline.log",
    "docs": "docs_pipeline.log",
    "sap": "sap_pipeline.log",
    "ts": "timeseries_pipeline.log",
    "aif": "findings_pipeline.log",
    "gloc": "findings_pipeline.log",
    "lopc": "findings_pipeline.log",
    "other_files": "other_files_pipeline.log",
    "upd_event": "findings_pipeline.log",
    "trip_event": "findings_pipeline.log",
    "alerts": "alerts_pipeline.log",
}

FINDINGS_STAGES = ("aif", "gloc", "lopc", "upd_event", "trip_event")

EVENTS_STAGES_REQUIRE_FILE_NAMES = ("upd_event", "trip_event")


def _resolve_industry(explicit: str | None, plant_code_id: str | None = None) -> str:
    """Return explicit industry, else the plant's registered industry, else user_config.yaml."""
    raw = explicit
    if not raw and plant_code_id:
        from ..plants import registered_plant_industry

        raw = registered_plant_industry(plant_code_id)
    if not raw:
        raw = load_yaml(USER_CONFIG_FILE).get("industry", "cement")
    return _INDUSTRY_TO_TEMPLATE.get(raw, raw)


def _build_stage_args(
    stage: str,
    plant_code_id: str,
    file_names: list[str] | None = None,
    doc_types: list[str] | None = None,
    doc_files: list[str] | None = None,
    work_dir: str | None = None,
    upload_batch_id: str | None = None,
    other_type: str | None = None,
    equipment_column: str | None = None,
    extraction_fields: list[str] | None = None,
) -> list[str]:
    """Build command-line arguments for each pipeline script."""
    config_dir = str(CONFIG_DIR)
    work_dir = work_dir or str(WORK_DIR)
    out_dir = rustfs_out_dir(plant_code_id)
    staging = str(STAGING_DIR)
    pnid_extractor = str(
        p0_DIR
        / "source_processing"
        / "pnid"
        / "complete_pnid_extraction_without_ui.py"
    )

    pnid_staging = rustfs_staging_pnid(plant_code_id)
    pnid_out = rustfs_processed_pnid(plant_code_id)
    docs_staging = rustfs_staging_docs(plant_code_id)
    docs_out = rustfs_processed_docs(plant_code_id)
    ts_staging = rustfs_staging_ts(plant_code_id)
    ts_out = rustfs_processed_ts(plant_code_id)
    sap_staging = rustfs_staging_sap(plant_code_id)
    sap_mapping_out = rustfs_processed_sap_mapping(plant_code_id)

    if stage == "sap_mapping":
        return [
            "--config_dir",
            config_dir,
            "--plant_code_id",
            plant_code_id,
            "--sap_in",
            sap_staging,
            "--pid_out",
            rustfs_processed_pnid(plant_code_id),
            "--out_dir",
            sap_mapping_out,
        ]
    elif stage == "sap":
        return [
            "--config_dir",
            config_dir,
            "--sap_in",
            sap_staging,
            "--work_dir",
            f"{work_dir}/sap",
            "--out_dir",
            f"{out_dir}/sap",
            "--plant_code_id",
            plant_code_id,
            "--pid_out",
            f"{out_dir}/pnid",
        ]
    elif stage == "pnid":
        return (
            [
                "--config_dir",
                config_dir,
                "--pnid_pdf_dir",
                f"{staging}/pnid",
                "--pnid_work_dir",
                f"{work_dir}/pnid",
                "--pnid_extractor_script",
                pnid_extractor,
                "--out_dir",
                f"{out_dir}/pnid",
                "--plant_code_id",
                plant_code_id,
                "--pnid_data_dir",
                RUSTFS_DATA_PNID,
                "--pnid_staging_dir",
                pnid_staging,
                "--processed_out",
                pnid_out,
            ]
            + (["--pnid_files", ",".join(file_names)] if file_names else [])
            + (["--upload_batch_id", upload_batch_id] if upload_batch_id else [])
        )
    elif stage in FINDINGS_STAGES:
        if stage in EVENTS_STAGES_REQUIRE_FILE_NAMES and not file_names:
            raise ValueError(
                f"stage '{stage}' shares its staging folder with the other events "
                "connector — file_names must name exactly the file(s) for this "
                "connector, or the run would also ingest the other connector's file"
            )
        return (
            [
                "--config_dir",
                config_dir,
                "--connector",
                stage,
                "--findings_in",
                rustfs_staging_findings(plant_code_id, stage),
                "--work_dir",
                f"{work_dir}/{stage}",
                "--out_dir",
                f"{out_dir}/{stage}",
                "--processed_out",
                rustfs_processed_findings(plant_code_id, stage),
                "--plant_code_id",
                plant_code_id,
                "--pid_out",
                f"{out_dir}/pnid",
            ]
            + (["--findings_files", ",".join(file_names)] if file_names else [])
            + (["--upload_batch_id", upload_batch_id] if upload_batch_id else [])
        )
        
    elif stage == "alerts":
        if not file_names:
            raise ValueError(
            "file_names is required for the alerts stage"
            )

        args = [
            "--config_dir",
            config_dir,

            "--alerts_in",
            rustfs_staging_alerts(plant_code_id),

            "--alerts_file",
            file_names[0],

            "--processed_out",
            rustfs_processed_alerts(plant_code_id),

            "--work_dir",
            f"{work_dir}/alerts",

            "--out_dir",
            f"{out_dir}/alerts",

            "--plant_code_id",
            plant_code_id,

            "--pid_out",
            f"{out_dir}/pnid",

            "--persist",
        ]

        if upload_batch_id:
            args += [
                "--upload_batch_id",
                upload_batch_id,
            ]
        return args
    elif stage == "ts":
        return (
            [
                "--config_dir",
                config_dir,
                "--ts_in",
                f"{staging}/ts",
                "--work_dir",
                f"{work_dir}/ts",
                "--out_dir",
                f"{out_dir}/ts",
                "--plant_code_id",
                plant_code_id,
                "--pid_out",
                f"{out_dir}/pnid",
                "--ts_staging_dir",
                f"{ts_staging}/metadata",
                "--processed_out",
                ts_out,
            ]
            + (["--ts_file", ",".join(file_names)] if file_names else [])
            + (["--upload_batch_id", upload_batch_id] if upload_batch_id else [])
        )
    elif stage == "docs":
        args = [
            "--config_dir",
            config_dir,
            "--doc_extraction_dir",
            docs_staging,
            "--work_dir",
            f"{work_dir}/docs",
            "--out_dir",
            f"{out_dir}/docs",
            "--plant_code_id",
            plant_code_id,
            "--pid_out",
            f"{out_dir}/pnid",
            "--processed_out",
            docs_out,
        ]
        if doc_types:
            args += ["--doc_types", ",".join(doc_types)]
        elif file_names:
            args += ["--doc_types", ",".join(file_names)]
        if doc_files:
            args += ["--doc_files", ",".join(doc_files)]
        if upload_batch_id:
            args += ["--upload_batch_id", upload_batch_id]
        return args
    
    elif stage == "other_files":
        if not (other_type or "").strip():
            raise ValueError(
            "other_type is required for the other_files stage"
        )

        if not file_names:
            raise ValueError(
            "file_names is required for the other_files stage"
        )

        args = [
        "--staging_dir",
        docs_staging,

        "--processed_out",
        docs_out,

        "--work_dir",
        f"{work_dir}/other_files",

        "--out_dir",
        f"{out_dir}/other_files",

        "--plant_code_id",
        plant_code_id,

        "--other_type",
        other_type.strip(),

        "--other_file",
        file_names[0],
    ]

        if upload_batch_id:
            args += [
            "--upload_batch_id",
            upload_batch_id,
        ]

        if equipment_column:
            args += [
            "--equipment_column",
            equipment_column,
        ]

        if extraction_fields:
            args += [
            "--extraction_fields",
            ",".join(extraction_fields),
        ]

        return args
    
    elif stage == "full":
        return [
            "--pnid_out",
            f"{out_dir}/pnid",
            "--ts_out",
            f"{out_dir}/ts",
            "--docs_out",
            f"{out_dir}/docs",
            "--sap_out",
            f"{out_dir}/sap",
            "--fmea_out",
            f"{out_dir}/fmea",
            "--out_dir",
            f"{out_dir}/final",
            "--config_dir",
            config_dir,
        ]
    return []


def _diagnose_zero_extraction(stdout: str | None, stderr: str | None) -> dict | None:
    """Identify the *cause* of a pipeline run that finished with returncode=0"""
    blob = (stdout or "") + "\n" + (stderr or "")
    if not blob.strip():
        return {
            "reason": "Pipeline produced no output.",
            "category": "empty_output",
            "evidence": "",
        }

    patterns: list[tuple[str, str, str]] = [
        (
            "All API keys exhausted",
            "gemini_quota_exhausted",
            "Gemini API quota exhausted — top up credits at https://ai.studio/projects "
            "or add more keys to GEMINI_API_KEYS, then retry.",
        ),
        (
            "credits depleted",
            "gemini_quota_exhausted",
            "Gemini API credits depleted — top up at https://ai.studio/projects, then retry.",
        ),
        (
            "TooManyRequests",
            "gemini_rate_limited",
            "Gemini rate-limited the request (429). Wait a minute and retry.",
        ),
        (
            "API_KEY_INVALID",
            "gemini_bad_key",
            "Gemini reported an invalid API key. Check GEMINI_API_KEYS in .env.enc.",
        ),
        (
            "No equipment or connectivity data found",
            "no_content_extracted",
            "Source file was processed but no structured content was extracted. "
            "Likely a low-quality scan or an unsupported page layout.",
        ),
        (
            "No source data found",
            "no_source_data",
            "Pipeline didn't find any source files to process. "
            "Re-upload through the connector page and retry.",
        ),
    ]
    for needle, category, reason in patterns:
        if needle.lower() in blob.lower():
            line = next(
                (l for l in blob.splitlines() if needle.lower() in l.lower()),
                "",
            ).strip()[:240]
            return {"reason": reason, "category": category, "evidence": line}

    tail = next((l for l in reversed(blob.splitlines()) if l.strip()), "")[-240:]
    return {
        "reason": "Pipeline completed but extracted 0 rows.",
        "category": "zero_rows_unknown_cause",
        "evidence": tail,
    }


def _kill_process_group(proc: "subprocess.Popen") -> None:
    """Send SIGTERM (then SIGKILL after 5s) to the entire process group of *proc*."""
    if proc.poll() is not None:
        return
    try:
        if hasattr(os, "killpg"):
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        else:
            proc.terminate()
        try:
            proc.wait(timeout=5)
            return
        except subprocess.TimeoutExpired:
            pass
        if hasattr(os, "killpg"):
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        else:
            proc.kill()
    except (ProcessLookupError, PermissionError):
        pass


def _append_pipeline_cancel_marker(stage: str, pipeline_job_id: str, pid: int | None) -> None:
    """Append a clearly-visible abort marker to the per-stage pipeline log."""
    fname = _STAGE_LOG_FILE.get(stage)
    if not fname:
        return
    path = LOG_DIR / fname
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")
        marker = (
            "\n"
            f"{ts} [WARNING] {stage}_pipeline — "
            f"━━━ JOB ABORTED BY USER ━━━  pipeline_job_id={pipeline_job_id}  pid={pid}\n"
            f"{ts} [WARNING] {stage}_pipeline — Subprocess was sent SIGTERM. "
            "Any in-flight LLM/Bedrock call was interrupted; partial work above "
            "this line is incomplete. Dedup ledger (if any) preserves files "
            "that were fully processed.\n"
        )
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(marker)
    except Exception as exc:
        _log.warning("[pipeline] could not write cancel marker to %s: %s", path, exc)




def _pid_is_alive(pid) -> bool:
    """True when a pid still exists, used to spot jobs orphaned by an API restart."""
    try:
        pid_int = int(pid)
    except (TypeError, ValueError):
        return False
    if pid_int <= 0:
        return False
    try:
        os.kill(pid_int, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _kill_by_pid(pid) -> bool:
    """Terminate a detached pipeline process group by pid after the API restarted."""
    if not _pid_is_alive(pid):
        return False
    pid_int = int(pid)
    try:
        if hasattr(os, "killpg"):
            try:
                os.killpg(os.getpgid(pid_int), signal.SIGTERM)
            except ProcessLookupError:
                return False
        else:
            os.kill(pid_int, signal.SIGTERM)
        for _ in range(10):
            time.sleep(0.5)
            if not _pid_is_alive(pid_int):
                return True
        if hasattr(os, "killpg"):
            os.killpg(os.getpgid(pid_int), signal.SIGKILL)
        else:
            os.kill(pid_int, signal.SIGKILL)
        return True
    except (ProcessLookupError, PermissionError, OSError):
        return False
