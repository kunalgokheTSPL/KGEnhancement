"""Document category and label taxonomy for the docs context."""

from __future__ import annotations

import logging
import re
import re as _re

from ..config import TEMPLATES_DIR
from ..deps import load_yaml


_DOC_CAT_MAP: dict[str, str] = {
    "operating_manual": "operating-manuals",
    "operating_manuals": "operating-manuals",
    "operating-manual": "operating-manuals",
    "o_and_m": "operating-manuals",
    "sop": "sop",
    "procedure": "sop",
    "datasheet": "datasheets",
    "datasheets": "datasheets",
    "equipment_datasheet": "datasheets",
    "equipment_datasheets": "datasheets",
    "spec": "datasheets",
    "maintenance_manual": "maintenance-manuals",
    "maintenance_manuals": "maintenance-manuals",
    "maintenance-manual": "maintenance-manuals",
    "maintenance": "maintenance-manuals",
}


def _doc_category(raw: str) -> str:
    """Normalise a document_type to a stable category id (review-tab key)."""
    norm = re.sub(r"\s+", "_", str(raw or "").lower())
    if norm in _DOC_CAT_MAP:
        return _DOC_CAT_MAP[norm]
    for k, v in _DOC_CAT_MAP.items():
        if norm.startswith(k):
            return v
    slug = re.sub(r"[^a-z0-9]+", "-", str(raw or "").lower()).strip("-")
    return slug or "general"


def _category_label(category_id: str) -> str:
    """Human label for a category id: 'maintenance-manuals' → 'Maintenance Manuals'."""
    return re.sub(r"[-_]+", " ", category_id).title()


def _docs_type_label_map() -> dict[str, str]:
    """Return ``{type_key: label}`` from the document-types catalog template."""
    try:

        cat = load_yaml(TEMPLATES_DIR / "_common" / "document_types.yaml") or {}
        out: dict[str, str] = {}
        for c in cat.get("categories", []) or []:
            for t in c.get("types", []) or []:
                key = str(t.get("key") or "").strip()
                label = str(t.get("label") or "").strip()
                if key:
                    out[key] = label or key
        return out
    except Exception as exc:
        logging.getLogger("cdm.api.context").warning(
            "[docs] document_types catalog unreadable: %s", exc
        )
        return {}


def _group_docs_by_category(
    rows: list[dict],
) -> tuple[dict[str, list[dict]], list[dict]]:
    """Stamp each row with ``category`` and group into {category_id: [rows]}."""
    grouped: dict[str, list[dict]] = {}
    for r in rows:
        cat = _doc_category(r.get("document_type") or r.get("type") or "general")
        r["category"] = cat
        grouped.setdefault(cat, []).append(r)
    categories = [
        {"id": cid, "label": _category_label(cid), "count": len(grp)}
        for cid, grp in grouped.items()
    ]
    return grouped, categories


def _docs_wanted_dirs(doc_types: list[str]) -> set[str]:
    """All possible subtype-dir names for the requested doc types."""
    label_map = _docs_type_label_map()
    wanted: set[str] = set()
    for d in doc_types:
        d = str(d).strip()
        if not d:
            continue
        wanted.add(_docs_subtype_dirname(d))
        label = label_map.get(d)
        if label:
            wanted.add(_docs_subtype_dirname(label))
    return wanted


def _docs_subtype_dirname(doc_type: str) -> str:
    """Reproduce the subtype directory name the docs pipeline writes under."""

    return _re.sub(r"[^a-z0-9_-]", "_", str(doc_type).lower().strip())
