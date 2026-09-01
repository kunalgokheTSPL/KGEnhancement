"""Per-file metadata for one upload, sent as a single array of objects."""

from __future__ import annotations

import json
import logging

from . import labels as _labels

_log = logging.getLogger("p0.upload_manifest")

FIELD = "files_meta"

HELP = (
    "Per-file metadata as a JSON array, one object per uploaded file: "
    '[{"file_name": "a.xlsx", "labels": ["integrity","reliability"], "document_type": "sop"}]. '
    "`file_name` must match the uploaded file exactly. Use this when the user tagged each file "
    "differently in the modal. Omit it and the flat `labels` / `document_type` fields apply to "
    "every file instead."
)


def _decode(raw) -> list[dict] | None:
    """Every object the caller sent, whether bare, as one array, or one field per file."""
    candidates = raw if isinstance(raw, (list, tuple)) else [raw]
    collected: list[dict] = []
    for entry in candidates:
        if isinstance(entry, dict):
            collected.append(entry)
            continue
        if not isinstance(entry, str):
            continue
        text = entry.strip()
        if not text.startswith(("[", "{")):
            continue
        try:
            decoded = json.loads(text)
        except ValueError:
            continue
        if isinstance(decoded, dict):
            collected.append(decoded)
        elif isinstance(decoded, list) and all(isinstance(i, dict) for i in decoded):
            collected.extend(decoded)
    return collected or None


def _name_of(entry: dict) -> str:
    for key in ("file_name", "filename", "name", "file"):
        value = entry.get(key)
        if value:
            return str(value).strip()
    return ""


def describes(raw) -> bool:
    """True when the caller sent a usable files_meta payload."""
    return _decode(raw) is not None


def resolve(
    raw,
    file_names: list[str],
    *,
    connector: str | None = None,
    fallback_labels=None,
    fallback_document_type: str | None = None,
) -> tuple[dict[str, dict], str | None]:
    """Per-file metadata for the whole batch, and the first problem found."""
    entries = _decode(raw)
    resolved: dict[str, dict] = {}

    if entries is None:
        mapping, error = _labels.validate_per_file(
            fallback_labels, file_names, connector=connector
        )
        for name in file_names:
            resolved[name] = {
                "labels": mapping.get(name, []),
                "document_type": (fallback_document_type or "").strip() or None,
            }
        return resolved, error

    by_name = {_name_of(e): e for e in entries if _name_of(e)}
    unmatched = [name for name in file_names if name not in by_name]
    if unmatched:
        return resolved, (
            f"{FIELD} does not describe every uploaded file. Missing: "
            + ", ".join(unmatched[:5])
            + ". Each entry needs a file_name matching the uploaded file exactly."
        )
    stray = [name for name in by_name if name not in file_names]
    if stray:
        return resolved, (
            f"{FIELD} names files that were not uploaded: " + ", ".join(sorted(stray)[:5]) + "."
        )

    for name in file_names:
        entry = by_name[name]
        chosen = _labels.normalise(_labels.parse(entry.get("labels")))
        document_type = str(
            entry.get("document_type") or entry.get("doc_type") or fallback_document_type or ""
        ).strip()
        resolved[name] = {"labels": chosen, "document_type": document_type or None}

    if not _labels.requires_labels(connector):
        for meta in resolved.values():
            meta["labels"] = []
        return resolved, None

    bad = _labels.unknown([l for m in resolved.values() for l in m["labels"]])
    if bad:
        allowed = ", ".join(sorted(_labels.VALID))
        return resolved, (
            f"Unknown label(s): {', '.join(sorted(set(bad)))}. Choose from {allowed}."
        )
    missing = [name for name, meta in resolved.items() if not meta["labels"]]
    if missing:
        return resolved, (
            "Select at least one label for every file. Missing: " + ", ".join(missing[:5])
        )
    return resolved, None


def document_types(resolved: dict[str, dict]) -> set[str]:
    """Every distinct document type in the batch."""
    return {m["document_type"] for m in resolved.values() if m.get("document_type")}
