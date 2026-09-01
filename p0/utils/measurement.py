"""
Canonical IoTDB measurement / tag-id normalization.

SINGLE SOURCE OF TRUTH. This rule maps a raw sensor tag name to the exact string
IoTDB stores as a measurement (and that the metadata table stores as
``iotdb_tag_id``). It MUST be identical everywhere — the metadata↔values join
(``timeseries_metadata.iotdb_tag_id`` == the IoTDB measurement under
``root.<root>.<plant>.<file>.<measurement>``) silently breaks if two call sites
normalize differently. Previously this regex was copy-pasted in the ingest
router, the TS pipeline, and the worker entrypoint; they must never drift, so
they all import this one function now.

Rule (unchanged from the original): replace ``.  -  /`` and whitespace with
``_``, then drop every remaining non-[A-Za-z0-9_] character.
"""

from __future__ import annotations

import hashlib
import re

_SUB_TO_UNDERSCORE = re.compile(r"[\.\s\-/]")
_STRIP_INVALID = re.compile(r"[^a-zA-Z0-9_]")


def normalize_measurement(name: object) -> str:
    """Normalize a raw tag/column name to a valid IoTDB measurement name."""
    s = _SUB_TO_UNDERSCORE.sub("_", str(name))
    return _STRIP_INVALID.sub("", s)


def disambiguate_measurement(name: object) -> str:
    """Normalized measurement, made collision-resistant by appending a short"""
    raw = str(name)
    norm = normalize_measurement(raw)
    if norm == raw:
        return norm
    suffix = hashlib.sha1(raw.encode("utf-8", "ignore")).hexdigest()[:4]
    return f"{norm}_{suffix}" if norm else f"m_{suffix}"


def dedupe_measurements(names: list) -> tuple[list[str], list[dict]]:
    """Map a list of raw column/tag names to UNIQUE IoTDB measurements, preserving"""
    plain = [
        normalize_measurement(n) or disambiguate_measurement(n) for n in names
    ]
    seen: dict[str, list[int]] = {}
    for i, m in enumerate(plain):
        seen.setdefault(m, []).append(i)
    dup_keys = {m for m, idxs in seen.items() if len(idxs) > 1}
    if not dup_keys:
        return plain, []

    out = list(plain)
    collisions: list[dict] = []
    for m in dup_keys:
        idxs = seen[m]
        for i in idxs:
            out[i] = disambiguate_measurement(names[i])
        collisions.append(
            {
                "normalized": m,
                "raw_names": [str(names[i]) for i in idxs],
                "resolved": [out[i] for i in idxs],
            }
        )
    return out, collisions
