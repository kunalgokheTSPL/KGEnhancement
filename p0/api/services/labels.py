"""The four use-case labels a user attaches to every uploaded file."""

from __future__ import annotations

import json

INTEGRITY = "integrity"
RELIABILITY = "reliability"
MAINTENANCE = "maintenance"
PRODUCTION = "production"

LABELS = [
    {"key": INTEGRITY, "label": "Integrity", "colour": "green", "sort_order": 1},
    {"key": RELIABILITY, "label": "Reliability", "colour": "blue", "sort_order": 2},
    {"key": PRODUCTION, "label": "Production", "colour": "amber", "sort_order": 3},
    {"key": MAINTENANCE, "label": "Maintenance", "colour": "purple", "sort_order": 4},
]

VALID = {entry["key"] for entry in LABELS}

UNLABELLED_CONNECTORS = {"pnid"}

SUGGESTED = {
    "aif": [INTEGRITY],
    "gloc": [INTEGRITY],
    "lopc": [INTEGRITY],
    "upd_event": [INTEGRITY],
    "trip_event": [INTEGRITY],
    "sap": [MAINTENANCE],
    "timeseries": [RELIABILITY],
    "documents": [],
}

_ALIASES = {
    "asset_integrity": INTEGRITY,
    "integrity_": INTEGRITY,
    "reliablity": RELIABILITY,
    "maintainance": MAINTENANCE,
    "maintenence": MAINTENANCE,
    "ops": PRODUCTION,
    "operations": PRODUCTION,
}


def catalogue() -> list[dict]:
    """The label vocabulary, for the upload modal."""
    return [dict(entry) for entry in LABELS]


def requires_labels(connector: str | None) -> bool:
    """P&ID is the one connector that carries no labels."""
    return str(connector or "").strip().lower() not in UNLABELLED_CONNECTORS


def suggested_for(connector: str | None) -> list[str]:
    """Labels the modal pre-ticks for this connector."""
    return list(SUGGESTED.get(str(connector or "").strip().lower(), []))


def parse(raw) -> list[str]:
    """Accept a JSON array, a comma-separated string, or a list; return canonical keys."""
    if raw is None:
        return []
    values: list = []
    if isinstance(raw, (list, tuple, set)):
        for entry in raw:
            if isinstance(entry, str) and entry.strip().startswith(("[", "{")):
                values.extend(parse(entry.strip()))
            elif isinstance(entry, str) and "," in entry:
                values.extend(entry.split(","))
            else:
                values.append(entry)
    else:
        text = str(raw).strip()
        if not text:
            return []
        if text.startswith("["):
            try:
                decoded = json.loads(text)
                values = decoded if isinstance(decoded, list) else [decoded]
            except ValueError:
                values = text.split(",")
        else:
            values = text.split(",")
    seen: list[str] = []
    for value in values:
        key = str(value).strip().lower().replace("-", "_").replace(" ", "_")
        key = _ALIASES.get(key, key)
        if key and key not in seen:
            seen.append(key)
    return seen


def unknown(values: list[str]) -> list[str]:
    """Whichever supplied labels are not in the vocabulary."""
    return [value for value in values if value not in VALID]


def validate(raw, *, connector: str | None = None) -> tuple[list[str], str | None]:
    """Return canonical labels and, when the selection is unusable, why."""
    values = parse(raw)
    if not requires_labels(connector):
        return [], None
    bad = unknown(values)
    if bad:
        allowed = ", ".join(sorted(VALID))
        return values, f"Unknown label(s): {', '.join(bad)}. Choose from {allowed}."
    if not values:
        return [], "Select at least one label for this file."
    return values, None


def sort_key(value: str) -> int:
    """Stable display order regardless of the order the user ticked them."""
    for entry in LABELS:
        if entry["key"] == value:
            return int(entry["sort_order"])
    return 99


def normalise(values: list[str]) -> list[str]:
    """Canonical order, so the same selection always stores identically."""
    return sorted(set(values), key=sort_key)


def _as_mapping(raw) -> dict | None:
    """The per-file object, whether it arrived bare or inside a repeated field."""
    candidates = raw if isinstance(raw, (list, tuple)) else [raw]
    for entry in candidates:
        if not isinstance(entry, str):
            continue
        text = entry.strip()
        if not text.startswith("{"):
            continue
        try:
            decoded = json.loads(text)
        except ValueError:
            continue
        if isinstance(decoded, dict):
            return decoded
    return None


def per_file(raw, file_names: list[str], *, connector: str | None = None) -> dict[str, list[str]]:
    """Map each uploaded file to its labels."""
    if not requires_labels(connector):
        return {name: [] for name in file_names}
    mapping = _as_mapping(raw)
    if mapping is not None:
        return {name: normalise(parse(mapping.get(name))) for name in file_names}
    shared = normalise(parse(raw))
    return {name: list(shared) for name in file_names}


def validate_per_file(
    raw, file_names: list[str], *, connector: str | None = None
) -> tuple[dict[str, list[str]], str | None]:
    """Resolve labels for every file in the batch and report the first problem."""
    mapping = per_file(raw, file_names, connector=connector)
    if not requires_labels(connector):
        return mapping, None
    bad = unknown([label for labels in mapping.values() for label in labels])
    if bad:
        allowed = ", ".join(sorted(VALID))
        return mapping, f"Unknown label(s): {', '.join(sorted(set(bad)))}. Choose from {allowed}."
    missing = [name for name in file_names if not mapping.get(name)]
    if missing:
        if len(missing) == len(file_names):
            return mapping, "Select at least one label for this file."
        return mapping, (
            "Select at least one label for every file. Missing: " + ", ".join(missing[:5])
        )
    return mapping, None
