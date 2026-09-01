"""What each document-type field means, as written in the type catalog."""

from __future__ import annotations

import logging
import os
from functools import lru_cache
from typing import Any

import yaml

logger = logging.getLogger(__name__)

CATALOG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "config",
    "templates",
    "_common",
    "document_types.yaml",
)


@lru_cache(maxsize=1)
def _catalog() -> dict[str, dict[str, str]]:
    """Read the type catalog once into {type_key: {field name: description}}."""
    try:
        with open(CATALOG_PATH, encoding="utf-8") as fh:
            cfg = yaml.safe_load(fh) or {}
    except Exception as exc:
        logger.warning("Could not read the document type catalog: %s", exc)
        return {}

    out: dict[str, dict[str, str]] = {}
    for category in cfg.get("categories") or []:
        for entry in category.get("types") or []:
            key = str(entry.get("key", "")).strip().lower()
            if not key:
                continue
            descs = {}
            for field in entry.get("fields") or []:
                name = str(field.get("name", "")).strip()
                desc = str(field.get("desc", "")).strip()
                if name and desc:
                    descs[name] = desc
            out[key] = descs
    return out


def catalog_field_descriptions(doc_type_key: str) -> dict[str, str]:
    """Descriptions the catalog defines for one document type, empty when unknown."""
    return dict(_catalog().get(str(doc_type_key or "").strip().lower(), {}))


def resolve_field_descriptions(
    doc_type_key: str, type_cfg: dict[str, Any] | None = None
) -> dict[str, str]:
    """Field descriptions for a type: the run's own config first, catalog behind it."""
    descs = catalog_field_descriptions(doc_type_key)
    configured = (type_cfg or {}).get("field_descriptions") or {}
    if isinstance(configured, dict):
        for name, desc in configured.items():
            name = str(name).strip()
            desc = str(desc).strip()
            if name and desc:
                descs[name] = desc
    return descs
