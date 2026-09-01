"""Document-type catalog config: source columns, template fields, persistence."""

from __future__ import annotations

import json as _json
import logging
from http import HTTPStatus

from fastapi import HTTPException

from p0.driver import get_object_fs

from ..config import TEMPLATES_DIR, USER_CONFIG_FILE
from ..deps import backup, load_yaml, save_yaml

_docs_log = logging.getLogger("cdm.connector.documents")


def _get_s3fs():
    """Return the active object store's filesystem, or raise HTTPException."""
    try:
        return get_object_fs()
    except Exception as exc:
        raise HTTPException(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            detail=f"RustFS connection failed: {exc}",
        )


def _parse_source_columns(source_columns) -> list[str]:
    """Normalize an incoming ``source_columns`` value (comma-string, JSON list,"""
    if not source_columns:
        return []
    cols: list[str] = []
    raw = source_columns
    if isinstance(raw, str):
        raw = raw.strip()
        if raw.startswith("["):
            try:
                cols = [str(c).strip() for c in _json.loads(raw) if str(c).strip()]
            except Exception:
                cols = []
        if not cols:
            cols = [c.strip() for c in raw.split(",") if c.strip()]
    elif isinstance(raw, (list, tuple)):
        cols = [str(c).strip() for c in raw if str(c).strip()]
    seen: set[str] = set()
    out: list[str] = []
    for c in cols:
        k = c.lower()
        if k not in seen:
            seen.add(k)
            out.append(c)
    return out


def _template_fields_for_subtype(safe_type: str) -> tuple[list[str], str | None]:
    """Return ``(field_names, label)`` predefined for this subtype in the"""
    try:
        catalog = load_yaml(TEMPLATES_DIR / "_common" / "document_types.yaml") or {}
    except Exception as exc:
        _docs_log.warning(
            "[docs/upload] could not read document_types catalog: %s", exc
        )
        return [], None
    for cat in catalog.get("categories", []) or []:
        for t in cat.get("types", []) or []:
            if str(t.get("key", "")).strip().lower() == safe_type.lower():
                names = [
                    str(f.get("name", "")).strip()
                    for f in (t.get("fields", []) or [])
                    if str(f.get("name", "")).strip()
                ]
                return names, t.get("label")
    return [], None


def _persist_doc_type_config(safe_type: str, label: str, source_columns) -> None:
    """Write a document type's extraction fields into user_config.yaml so the"""
    incoming = _parse_source_columns(source_columns)
    template_fields, template_label = _template_fields_for_subtype(safe_type)
    seen: set[str] = set()
    cols: list[str] = []
    for c in [*template_fields, *incoming]:
        k = c.lower()
        if k not in seen:
            seen.add(k)
            cols.append(c)
    if not cols:
        return

    final_label = label or template_label or safe_type
    try:
        cfg = load_yaml(USER_CONFIG_FILE) or {}
        dp = cfg.setdefault("document_processing", {})
        dts = dp.setdefault("document_types", {})
        dts[safe_type] = {"document_type": final_label, "source_columns": cols}
        if USER_CONFIG_FILE.exists():
            backup(USER_CONFIG_FILE)
        save_yaml(USER_CONFIG_FILE, cfg)
        _docs_log.info(
            "[docs/upload] persisted doc-type config: %s → %d field(s) "
            "(template=%d, incoming=%d, merged union)",
            safe_type,
            len(cols),
            len(template_fields),
            len(incoming),
        )
    except Exception as exc:
        _docs_log.warning(
            "[docs/upload] could not persist doc-type config for %s: %s", safe_type, exc
        )
