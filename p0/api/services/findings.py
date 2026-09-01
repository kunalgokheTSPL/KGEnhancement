"""Schema reading for the findings connectors — AIF, GLOC, LOPC."""

from __future__ import annotations

import functools
import hashlib
import logging
import re
from datetime import datetime

import yaml

from p0.utils import findings_identity as _identity
from .. import config as _config

_log = logging.getLogger("p0.findings")

CONNECTORS = ("aif", "gloc", "lopc", "upd_event", "trip_event")

FLOW_BY_CONNECTOR = {
    "aif": "aif",
    "gloc": "gloc",
    "lopc": "lopc",
    "upd_event": "upd_event",
    "trip_event": "trip_event",
}

_PUNCT = re.compile(r"[^a-z0-9]+")
_TRUE = {"yes", "y", "true", "t", "1"}
_FALSE = {"no", "n", "false", "f", "0"}
_NUMBER = re.compile(r"-?\d+(?:\.\d+)?")


def _fold(value) -> str:
    """Collapse a header to its comparable form."""
    return _PUNCT.sub("", str(value or "").strip().lower())


@functools.lru_cache(maxsize=1)
def template() -> dict:
    """The findings source template, loaded once."""
    path = _config.COMMON_DIR / "findings_sources.yaml"
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return yaml.safe_load(handle) or {}
    except Exception as exc:
        _log.error("[findings] cannot read %s: %s", path, exc)
        return {}


def spec_for(connector: str) -> dict:
    """One connector's template block."""
    return dict((template().get("sources") or {}).get(str(connector or "").lower()) or {})


def is_findings_connector(connector: str) -> bool:
    return str(connector or "").strip().lower() in CONNECTORS


def target_table(connector: str) -> str | None:
    return spec_for(connector).get("target_table")


def catalogue() -> list[dict]:
    """What the upload modal needs to render the three new tiles."""
    entries = []
    for key in CONNECTORS:
        spec = spec_for(key)
        entries.append(
            {
                "key": key,
                "label": spec.get("label") or key.upper(),
                "extensions": (template().get("defaults") or {}).get("extensions", []),
                "target_table": spec.get("target_table"),
                "required_columns": spec.get("required", []),
                "recommended_columns": spec.get("recommended", []),
            }
        )
    return entries


@functools.lru_cache(maxsize=8)
def _alias_index(connector: str) -> dict[str, str]:
    """Every accepted spelling folded to the canonical column it feeds."""
    index: dict[str, str] = {}
    for canonical, meta in (spec_for(connector).get("columns") or {}).items():
        index.setdefault(_fold(canonical), canonical)
        for alias in (meta or {}).get("aliases", []) or []:
            index.setdefault(_fold(alias), canonical)
    return index


def column_map(connector: str, headers: list) -> dict:
    """Map the file's headers onto canonical columns, reporting what did not land."""
    index = _alias_index(connector)
    mapped: dict[str, str] = {}
    unmapped: list[str] = []
    for header in headers:
        canonical = index.get(_fold(header))
        if canonical and canonical not in mapped.values():
            mapped[header] = canonical
        else:
            unmapped.append(str(header))
    known = set(spec_for(connector).get("columns") or {})
    return {
        "mapped": mapped,
        "unmapped": unmapped,
        "missing": sorted(known - set(mapped.values())),
    }


def pick_sheet(connector: str, sheet_names: list) -> str | None:
    """Choose the data sheet by the template's hints, else the first one."""
    if not sheet_names:
        return None
    hints = [str(h).lower() for h in spec_for(connector).get("sheet_hints", [])]
    for hint in hints:
        for name in sheet_names:
            if hint in str(name).lower():
                return name
    return sheet_names[0]


def _vocabulary(field: str) -> dict[str, str]:
    folded: dict[str, str] = {}
    for canonical, spellings in ((template().get("vocabularies") or {}).get(field) or {}).items():
        for spelling in spellings:
            folded[_fold(spelling)] = canonical
    return folded


def canonical_value(field: str, value):
    """Fold a controlled-vocabulary value to its canonical spelling."""
    if value is None or str(value).strip() == "":
        return value
    return _vocabulary(field).get(_fold(value), value)


def coerce_date(value):
    """Best-effort date, never raising on the junk real exports contain."""
    if value is None or str(value).strip() == "":
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%b-%Y", "%d %B %Y"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    try:
        import pandas as pd

        parsed = pd.to_datetime(text, errors="coerce", dayfirst=True)
        return None if parsed is None or pd.isna(parsed) else parsed.to_pydatetime()
    except Exception:
        return None


def coerce_number(value):
    """Pull a number out of a cell that may hold anything."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    match = _NUMBER.search(text.replace(",", ""))
    if not match:
        return None
    try:
        return float(match.group(0))
    except ValueError:
        return None


def coerce_integer(value):
    number = coerce_number(value)
    return None if number is None else int(number)


def coerce_boolean(value):
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in _TRUE:
        return True
    if text in _FALSE:
        return False
    return None


_COERCERS = {
    "date": coerce_date,
    "numeric": coerce_number,
    "integer": coerce_integer,
    "boolean": coerce_boolean,
}


def normalise_row(connector: str, row: dict) -> dict:
    """Apply the template's types, vocabularies and raw-value preservation."""
    columns = spec_for(connector).get("columns") or {}
    out = dict(row)
    for canonical, meta in columns.items():
        if canonical not in out:
            continue
        meta = meta or {}
        raw = out[canonical]
        keep_raw = meta.get("keep_raw")
        if keep_raw:
            out[keep_raw] = None if raw is None else str(raw).strip() or None
        coercer = _COERCERS.get(meta.get("type"))
        if coercer:
            out[canonical] = coercer(raw)
        elif raw is not None:
            out[canonical] = canonical_value(canonical, raw)
    return out


def identity_spec(connector: str) -> dict:
    """The identity ladder, template first so a site can override it."""
    declared = spec_for(connector).get("identity")
    return dict(declared) if declared else _identity.spec_for(connector)


def resolve_identity(row: dict, connector: str) -> dict:
    """normalized_asset plus how it was matched and how far to trust it."""
    spec = identity_spec(connector)
    return _identity.resolve(
        row,
        sap_equipment_columns=tuple(spec.get("sap_equipment_columns", ())),
        equipment_columns=tuple(spec.get("equipment_columns", ())),
        floc_columns=tuple(spec.get("floc_columns", ())),
        text_columns=tuple(spec.get("text_columns", ())),
    )


def read_frame(connector: str, df):
    """Rename, coerce and resolve identity for a whole DataFrame."""
    if df is None or getattr(df, "empty", True):
        return df, {"mapped": {}, "unmapped": [], "missing": []}
    mapping = column_map(connector, list(df.columns))
    out = df.rename(columns=mapping["mapped"])
    keep = [c for c in out.columns if c in (spec_for(connector).get("columns") or {})]
    out = out[keep] if keep else out
    rows = [normalise_row(connector, row) for row in out.to_dict("records")]
    import pandas as pd

    out = pd.DataFrame(rows) if rows else out
    out = _identity.resolve_frame(out, identity_spec(connector))
    return out, mapping


def missing_required(connector: str, mapping: dict) -> list[str]:
    """Required columns the uploaded file does not provide."""
    landed = set(mapping.get("mapped", {}).values())
    return [c for c in spec_for(connector).get("required", []) if c not in landed]


_FLOAT_ID = re.compile(r"^\d+\.0+$")


def key_part(value) -> str:
    """One natural-key component, free of nulls, Excel floats and clock noise."""
    if value is None or value != value:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    text = str(value).strip()
    if text.lower() in ("nan", "nat", "none", "null"):
        return ""
    if _FLOAT_ID.match(text):
        return text.split(".")[0]
    if len(text) == 19 and text[4] == "-" and text.endswith("00:00:00"):
        return text[:10]
    return text


MAX_RECORD_ID = 180


def _bounded(key: str) -> str:
    """Keep the key inside source_record_id's column width without losing identity."""
    if len(key) <= MAX_RECORD_ID:
        return key
    digest = hashlib.sha1(key.encode("utf-8", "ignore")).hexdigest()[:12]
    return f"{key[: MAX_RECORD_ID - 13]}~{digest}"


def source_record_id(connector: str, row: dict) -> str:
    """A stable natural key for one finding, so re-uploads update rather than duplicate."""
    parts = [key_part(row.get(column)) for column in spec_for(connector).get("natural_key", [])]
    return _bounded("|".join(parts).strip("|"))


def assign_record_ids(connector: str, rows: list[dict]) -> list[str]:
    """Natural keys for a whole file, made unique without losing a row."""
    seen: dict[str, int] = {}
    ids: list[str] = []
    for position, row in enumerate(rows, start=1):
        key = source_record_id(connector, row) or f"row-{position}"
        count = seen.get(key, 0) + 1
        seen[key] = count
        ids.append(key if count == 1 else _bounded(f"{key}#{count}"))
    return ids
