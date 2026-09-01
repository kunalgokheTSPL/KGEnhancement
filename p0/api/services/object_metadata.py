"""Metadata written beside every uploaded object, so a file can be previewed or described later."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from p0.utils import fs as _fs

_log = logging.getLogger("p0.object_metadata")

SIDECAR_SUFFIX = ".meta.json"
MANIFEST_NAME = "_manifest.json"


def sidecar_path(object_path: str) -> str:
    """Where one object's metadata document lives."""
    return f"{object_path}{SIDECAR_SUFFIX}"


def manifest_path(prefix: str) -> str:
    """Where a folder's rolled-up manifest lives."""
    return f"{prefix.rstrip('/')}/{MANIFEST_NAME}"


def build_document(
    *,
    plant_code_id: str,
    connector: str,
    category: str | None,
    file_name: str,
    object_path: str,
    size_bytes: int,
    content_hash: str | None = None,
    content_type: str | None = None,
    upload_job_id: str | None = None,
    upload_batch_id: str | None = None,
    flow_uid=None,
    uploaded_by: str | None = None,
    metadata: dict | None = None,
    tags: list[str] | None = None,
) -> dict:
    """The metadata document stored next to the object."""
    return {
        "schema_version": 1,
        "plant_code_id": plant_code_id,
        "connector": connector,
        "category": category,
        "file_name": file_name,
        "path": object_path,
        "size_bytes": size_bytes,
        "content_hash": content_hash,
        "content_type": content_type,
        "upload_job_id": upload_job_id,
        "upload_batch_id": upload_batch_id,
        "flow_uid": flow_uid,
        "uploaded_by": uploaded_by,
        "uploaded_at": datetime.now(timezone.utc).isoformat(),
        "tags": tags or [],
        "metadata": metadata or {},
    }


def write_sidecar(object_path: str, document: dict) -> bool:
    """Write one object's metadata beside it. Best-effort — never fails an upload."""
    try:
        sfs = _fs.get_fs()
        target = sidecar_path(object_path)
        with sfs.open(target, "wb") as handle:
            handle.write(json.dumps(document, indent=2, default=str).encode("utf-8"))
        return True
    except Exception as exc:
        _log.warning("[object_metadata] sidecar write for %s failed: %s", object_path, exc)
        return False


def read_sidecar(object_path: str) -> dict | None:
    """Read one object's metadata document, or None when it has none."""
    try:
        sfs = _fs.get_fs()
        target = sidecar_path(object_path)
        if not sfs.exists(target):
            return None
        with sfs.open(target, "rb") as handle:
            return json.loads(handle.read().decode("utf-8"))
    except Exception as exc:
        _log.warning("[object_metadata] sidecar read for %s failed: %s", object_path, exc)
        return None


def update_manifest(prefix: str, document: dict) -> bool:
    """Keep a per-folder index so a whole source type can be described in one read."""
    target = manifest_path(prefix)
    try:
        sfs = _fs.get_fs()
        existing = {"schema_version": 1, "prefix": prefix, "files": []}
        try:
            if sfs.exists(target):
                with sfs.open(target, "rb") as handle:
                    loaded = json.loads(handle.read().decode("utf-8"))
                if isinstance(loaded, dict) and isinstance(loaded.get("files"), list):
                    existing = loaded
        except Exception as exc:
            _log.warning("[object_metadata] manifest read for %s failed: %s", prefix, exc)

        entry = {
            "file_name": document.get("file_name"),
            "path": document.get("path"),
            "category": document.get("category"),
            "size_bytes": document.get("size_bytes"),
            "content_type": document.get("content_type"),
            "content_hash": document.get("content_hash"),
            "flow_uid": document.get("flow_uid"),
            "uploaded_at": document.get("uploaded_at"),
            "uploaded_by": document.get("uploaded_by"),
        }
        files = [f for f in existing["files"] if f.get("path") != entry["path"]]
        files.append(entry)
        existing["files"] = files
        existing["file_count"] = len(files)
        existing["updated_at"] = datetime.now(timezone.utc).isoformat()

        with sfs.open(target, "wb") as handle:
            handle.write(json.dumps(existing, indent=2, default=str).encode("utf-8"))
        return True
    except Exception as exc:
        _log.warning("[object_metadata] manifest write for %s failed: %s", prefix, exc)
        return False


def read_manifest(prefix: str) -> dict | None:
    """The folder manifest, or None when nothing has been uploaded there."""
    try:
        sfs = _fs.get_fs()
        target = manifest_path(prefix)
        if not sfs.exists(target):
            return None
        with sfs.open(target, "rb") as handle:
            return json.loads(handle.read().decode("utf-8"))
    except Exception as exc:
        _log.warning("[object_metadata] manifest read for %s failed: %s", prefix, exc)
        return None


def record(prefix: str, **kwargs) -> dict:
    """Write both the sidecar and the folder manifest for one uploaded object."""
    document = build_document(**kwargs)
    document["sidecar_written"] = write_sidecar(document["path"], document)
    document["manifest_updated"] = update_manifest(prefix, document)
    return document
