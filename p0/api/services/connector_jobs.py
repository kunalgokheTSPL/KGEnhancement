"""In-memory upload-job state and the connector subprocess runner."""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import yaml
from fastapi import HTTPException
from http import HTTPStatus

from p0.utils.fs import ensure_bucket as _ensure_bucket
from p0.driver import object_store_container, storage_backend

from ..config import (
    BASE_DIR,
    CONNECTORS_DIR,
    DEST_CONFIG,
    PNID_CONFIG,
    RUSTFS_ACCESS_KEY,
    RUSTFS_BASE_PREFIX,
    RUSTFS_BUCKET,
    RUSTFS_ENDPOINT,
    RUSTFS_SECRET_KEY,
)
from ..deps import load_yaml

_log = logging.getLogger("cdm.api.connectors")


_upload_jobs: dict[str, dict] = {}


def _ensure_rustfs_bucket(label: str) -> None:
    """Idempotently create the active object store's bucket/container before the first write."""
    container = object_store_container()
    if not _ensure_bucket(container):
        raise HTTPException(
            status_code=HTTPStatus.SERVICE_UNAVAILABLE,
            detail=(
                f"Object-store container '{container}' not available and could "
                f"not be created (endpoint: {label}). Check credentials and "
                f"whether the deploy grants create-container permission."
            ),
        )


def _run_connector_bg(
    job_id: str,
    data_type: str,
    source_type: str,
    config_override: dict | None = None,
    config_path: Path | None = None,
    post_fn: callable | None = None,
) -> None:
    """Run a connector subprocess in a background thread, updating _upload_jobs."""
    _conn_log = logging.getLogger(f"cdm.connector.{data_type}")

    _upload_jobs[job_id]["status"] = "running"
    _conn_log.info(
        "[connector/%s] BG THREAD START  job_id=%s  source_type=%s",
        data_type,
        job_id,
        source_type,
    )

    try:
        cfg = load_yaml(config_path or PNID_CONFIG)
        if config_override:
            src_section = cfg.setdefault("sources", {}).setdefault(source_type, {})
            for k, v in config_override.items():
                if isinstance(v, dict) and isinstance(src_section.get(k), dict):
                    src_section[k].update(v)
                else:
                    src_section[k] = v

        tmp = tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".yaml",
            delete=False,
            dir="/tmp",
        )
        yaml.safe_dump(cfg, tmp)
        tmp.close()

        dest_cfg = load_yaml(DEST_CONFIG)
        if storage_backend() == "adls":
            dest_type = "adls"
            sec = dest_cfg.setdefault("destinations", {}).setdefault("adls", {})
            sec["type"] = "adls"
            sec["container"] = object_store_container()
            sec["base_prefix"] = RUSTFS_BASE_PREFIX
            conn = sec.setdefault("connection", {})
            conn["account_name"] = os.environ.get("ADLS_ACCOUNT_NAME")
            conn["account_key"] = os.environ.get("ADLS_ACCOUNT_KEY")
        else:
            dest_type = "rustfs"
            sec = dest_cfg.setdefault("destinations", {}).setdefault("rustfs", {})
            sec["bucket"] = RUSTFS_BUCKET
            sec["base_prefix"] = RUSTFS_BASE_PREFIX
            conn = sec.setdefault("connection", {})
            conn["endpoint_url"] = RUSTFS_ENDPOINT
            conn["access_key"] = RUSTFS_ACCESS_KEY
            conn["secret_key"] = RUSTFS_SECRET_KEY
        dest_tmp = tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".yaml",
            delete=False,
            dir="/tmp",
        )
        yaml.safe_dump(dest_cfg, dest_tmp)
        dest_tmp.close()

        cmd = [
            sys.executable,
            str(CONNECTORS_DIR / "run_connector.py"),
            "--data-type",
            data_type,
            "--source-type",
            source_type,
            "--config",
            tmp.name,
            "--destination",
            dest_tmp.name,
            "--destination-type",
            dest_type,
            "--log-level",
            "INFO",
        ]
        _conn_log.debug(
            "[connector/%s] subprocess cmd: %s",
            data_type,
            " ".join(cmd),
        )
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=600,
            cwd=str(BASE_DIR),
            env={
                **os.environ,
                "PYTHONPATH": os.pathsep.join([str(BASE_DIR), str(BASE_DIR / "p0")]),
            },
        )
        os.unlink(tmp.name)
        os.unlink(dest_tmp.name)

        if result.returncode == 0:
            _upload_jobs[job_id]["status"] = "completed"
            _upload_jobs[job_id]["log"] = result.stdout[-2000:] if result.stdout else ""
            _conn_log.info(
                "[connector/%s] COMPLETED → RustFS  job_id=%s  source=%s",
                data_type,
                job_id,
                source_type,
            )
            if result.stdout:
                _conn_log.debug(
                    "[connector/%s] subprocess stdout:\n%s",
                    data_type,
                    result.stdout[-2000:],
                )
            if post_fn:
                _conn_log.info(
                    "[connector/%s] running post-processing  job_id=%s",
                    data_type,
                    job_id,
                )
                try:
                    post_result = post_fn()
                    _upload_jobs[job_id]["post_processing"] = post_result
                    _conn_log.info(
                        "[connector/%s] post-processing done  job_id=%s  result=%s",
                        data_type,
                        job_id,
                        post_result,
                    )
                except Exception as exc:
                    _upload_jobs[job_id]["post_processing_error"] = str(exc)
                    _conn_log.error(
                        "[connector/%s] post-processing FAILED  job_id=%s: %s",
                        data_type,
                        job_id,
                        exc,
                    )
        else:
            err = (result.stderr or result.stdout or "Unknown error")[-2000:]
            _upload_jobs[job_id]["status"] = "failed"
            _upload_jobs[job_id]["error"] = err
            _conn_log.error(
                "[connector/%s] FAILED  job_id=%s  returncode=%d\nerror:\n%s",
                data_type,
                job_id,
                result.returncode,
                err,
            )
    except Exception as exc:
        _upload_jobs[job_id]["status"] = "failed"
        _upload_jobs[job_id]["error"] = str(exc)
        _conn_log.exception(
            "[connector/%s] EXCEPTION  job_id=%s: %s",
            data_type,
            job_id,
            exc,
        )
