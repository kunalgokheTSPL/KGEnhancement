"""Build the label-wise 6-layer view, one row per finding."""

from __future__ import annotations

import functools
import json
import logging
from datetime import datetime, timezone

import yaml

from ..database.connection import _get_conn
from . import findings as _findings
from . import labels as _labels
from .. import config as _config

_log = logging.getLogger("p0.integrated_view")

LAYERS = ("asset", "process", "performance", "knowledge", "action", "business")

BACKED = "backed"
NOT_YET_BACKED = "not_yet_backed"

COMMITTED = "committed"
PROCESSED = "processed"

DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 200

_FINDING_TABLES = {
    "aif": "inspection_record_aif",
    "gloc": "containment_event_gloc",
    "lopc": "containment_event_lopc",
}

_FINDING_REF = {
    "aif": "notification_no",
    "gloc": "source_record_id",
    "lopc": "source_record_id",
}


@functools.lru_cache(maxsize=8)
def template(label: str) -> dict:
    """One label's 6-layer template."""
    key = str(label or "").strip().lower()
    path = _config.COMMON_DIR / "integrated_view" / f"{key}.yaml"
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return yaml.safe_load(handle) or {}
    except Exception as exc:
        _log.error("[integrated_view] cannot read %s: %s", path, exc)
        return {}


def known_labels() -> list[str]:
    """Labels that have a template behind them."""
    return [entry["key"] for entry in _labels.catalogue() if template(entry["key"])]


def describe(label: str) -> dict:
    """The column plan for one label, so the UI can render headers before any data."""
    spec = template(label)
    columns: list[dict] = []
    for layer in LAYERS:
        for field in ((spec.get("layers") or {}).get(layer) or {}).get("fields", []) or []:
            columns.append(
                {
                    "layer": layer,
                    "name": field.get("name"),
                    "status": field.get("status", NOT_YET_BACKED),
                    "source_label": field.get("source_label"),
                    "note": field.get("note"),
                }
            )
    return {
        "label": str(label or "").lower(),
        "title": spec.get("label"),
        "description": (spec.get("description") or "").strip(),
        "layers": [
            {
                "layer": layer,
                "standard": ((spec.get("layers") or {}).get(layer) or {}).get("standard"),
                "fields": [c for c in columns if c["layer"] == layer],
            }
            for layer in LAYERS
        ],
        "columns": columns,
        "backed": sum(1 for c in columns if c["status"] == BACKED),
        "not_yet_backed": sum(1 for c in columns if c["status"] != BACKED),
    }


def _query(plant_code_id: str, sql: str, params: tuple) -> list[dict]:
    """Run one read, returning dicts, and never raise on a table that is not there yet."""
    try:
        conn = _get_conn(plant_code_id)
        try:
            cur = conn.cursor()
            cur.execute(sql, params)
            columns = [d[0] for d in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
        finally:
            conn.close()
    except Exception as exc:
        _log.debug("[integrated_view] query skipped: %s", exc)
        return []


def _labelled_files(plant_code_id: str, label: str) -> dict[str, set[str]]:
    """Which source files carry this label, per connector."""
    rows = _query(
        plant_code_id,
        "SELECT flow, file_name, labels, stage FROM flow_state WHERE plant_code_id = %s",
        (plant_code_id,),
    )
    out: dict[str, set[str]] = {}
    for row in rows:
        raw = row.get("labels")
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except ValueError:
                raw = []
        if label not in (raw or []):
            continue
        out.setdefault(str(row.get("flow")), set()).add(str(row.get("file_name")))
    return out


def _lookup_by_asset(plant_code_id: str, table: str, key_column: str) -> dict[str, dict]:
    """One canonical table indexed by the asset key the template joins on."""
    rows = _query(plant_code_id, f"SELECT * FROM {table} WHERE plant_code_id = %s", (plant_code_id,))
    index: dict[str, dict] = {}
    for row in rows:
        key = str(row.get(key_column) or "").strip().upper()
        if key and key not in index:
            index[key] = row
    return index


def _cell(value) -> object:
    """Render one value as something JSON can carry."""
    if value is None or value != value:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _build_row(
    *,
    label: str,
    spec: dict,
    connector: str,
    finding: dict,
    lookups: dict[tuple[str, str], dict[str, dict]],
) -> dict:
    """One integrated row: every template field resolved or honestly reported as absent."""
    asset = str(finding.get("normalized_asset") or "").strip()
    asset_key = asset.upper()
    finding_table = _FINDING_TABLES[connector]
    layers: dict[str, dict] = {}
    for layer in LAYERS:
        fields: dict[str, dict] = {}
        for field in ((spec.get("layers") or {}).get(layer) or {}).get("fields", []) or []:
            name = field.get("name")
            if field.get("status") != BACKED:
                fields[name] = {
                    "value": None,
                    "status": NOT_YET_BACKED,
                    "note": field.get("note"),
                }
                continue
            value, source_label = None, field.get("source_label")
            for source in _sources_of(field):
                if source["table"] == finding_table:
                    value = finding.get(source["column"])
                else:
                    index = lookups.get((source["table"], source["key_column"])) or {}
                    value = (index.get(asset_key) or {}).get(source["column"])
                if value is not None and value == value and str(value).strip() != "":
                    source_label = source.get("label") or source_label
                    break
                value = None
            fields[name] = {
                "value": _cell(value),
                "status": BACKED,
                "source_label": source_label,
            }
        layers[layer] = fields
    return {
        "label": label,
        "normalized_asset": asset,
        "finding_ref": str(finding.get(_FINDING_REF[connector]) or finding.get("source_record_id") or ""),
        "source_connector": connector,
        "source_table": finding_table,
        "asset_match_method": finding.get("asset_match_method"),
        "asset_match_confidence": _cell(finding.get("asset_match_confidence")),
        "source_file": finding.get("source_file"),
        "upload_batch_id": finding.get("upload_batch_id"),
        "layers": layers,
    }


def _sources_of(field: dict) -> list[dict]:
    """A field's backing columns, in preference order."""
    default_key = field.get("key_column") or "normalized_asset"
    declared = field.get("sources")
    if declared:
        return [
            {
                "table": entry.get("table"),
                "column": entry.get("column"),
                "key_column": entry.get("key_column") or default_key,
                "label": entry.get("label"),
            }
            for entry in declared
            if entry.get("table") and entry.get("column")
        ]
    if field.get("source_table") and field.get("source_column"):
        return [
            {
                "table": field["source_table"],
                "column": field["source_column"],
                "key_column": default_key,
                "label": field.get("source_label"),
            }
        ]
    return []


def _required_lookups(spec: dict) -> set[tuple[str, str]]:
    """Which (table, key_column) pairs this template needs beyond the findings table."""
    wanted: set[tuple[str, str]] = set()
    for layer in LAYERS:
        for field in ((spec.get("layers") or {}).get(layer) or {}).get("fields", []) or []:
            if field.get("status") != BACKED:
                continue
            for source in _sources_of(field):
                if source["table"] not in _FINDING_TABLES.values():
                    wanted.add((source["table"], source["key_column"]))
    return wanted


def compute(plant_code_id: str, label: str, *, include_processed: bool = True) -> list[dict]:
    """Build every row for one label, on the fly, from committed findings."""
    key = str(label or "").strip().lower()
    spec = template(key)
    if not spec:
        return []
    files_by_flow = _labelled_files(plant_code_id, key)
    if not files_by_flow:
        return []
    lookups = {
        pair: _lookup_by_asset(plant_code_id, pair[0], pair[1]) for pair in _required_lookups(spec)
    }
    rows: list[dict] = []
    for connector, table in _FINDING_TABLES.items():
        names = files_by_flow.get(connector)
        if not names:
            continue
        for finding in _query(
            plant_code_id, f"SELECT * FROM {table} WHERE plant_code_id = %s", (plant_code_id,)
        ):
            if finding.get("source_file") and finding["source_file"] not in names:
                continue
            row = _build_row(
                label=key, spec=spec, connector=connector, finding=finding, lookups=lookups
            )
            row["row_status"] = COMMITTED
            rows.append(row)
    if include_processed:
        rows.extend(_processed_rows(plant_code_id, key, spec, files_by_flow, lookups))
    return rows


def _processed_rows(plant_code_id, label, spec, files_by_flow, lookups) -> list[dict]:
    """Rows that are processed but not committed yet, read from object storage."""
    from p0.utils import fs as _fs

    rows: list[dict] = []
    for connector in _FINDING_TABLES:
        names = files_by_flow.get(connector)
        if not names:
            continue
        table = _findings.target_table(connector)
        path = _fs.path_join(
            _config.rustfs_processed_findings(plant_code_id, connector), f"{table}.parquet"
        )
        try:
            if not _fs.exists(path):
                continue
            frame = _fs.read_parquet(path)
        except Exception as exc:
            _log.debug("[integrated_view] processed read skipped for %s: %s", connector, exc)
            continue
        committed = {
            str(r.get("source_record_id"))
            for r in _query(
                plant_code_id,
                f"SELECT source_record_id FROM {_FINDING_TABLES[connector]} WHERE plant_code_id = %s",
                (plant_code_id,),
            )
        }
        for finding in frame.to_dict("records"):
            if str(finding.get("source_record_id")) in committed:
                continue
            if finding.get("source_file") and finding["source_file"] not in names:
                continue
            row = _build_row(
                label=label, spec=spec, connector=connector, finding=finding, lookups=lookups
            )
            row["row_status"] = PROCESSED
            rows.append(row)
    return rows


def materialise(plant_code_id: str, label: str, rows: list[dict]) -> int:
    """Persist one label's rows into the single integrated_view table."""
    if not rows:
        return 0
    now = datetime.now(timezone.utc)
    written = 0
    try:
        conn = _get_conn(plant_code_id)
        try:
            cur = conn.cursor()
            cur.execute(
                "DELETE FROM integrated_view WHERE plant_code_id = %s AND label = %s",
                (plant_code_id, label),
            )
            for row in rows:
                cur.execute(
                    "INSERT INTO integrated_view (plant_code_id, label, normalized_asset, "
                    "finding_ref, source_connector, source_table, row_status, layers, "
                    "upload_batch_id, computed_at, updated_at) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                    "ON CONFLICT (plant_code_id, label, source_connector, finding_ref) "
                    "DO UPDATE SET layers = EXCLUDED.layers, row_status = EXCLUDED.row_status, "
                    "normalized_asset = EXCLUDED.normalized_asset, updated_at = EXCLUDED.updated_at",
                    (
                        plant_code_id, label, row["normalized_asset"], row["finding_ref"],
                        row["source_connector"], row["source_table"], row["row_status"],
                        json.dumps(row["layers"]), row.get("upload_batch_id"), now, now,
                    ),
                )
                written += 1
            conn.commit()
        finally:
            conn.close()
    except Exception as exc:
        _log.warning("[integrated_view] materialise failed for %s: %s", label, exc)
        return 0
    return written


def counts(plant_code_id: str) -> dict[str, int]:
    """How many rows each label would render, for the tab badges."""
    return {label: len(compute(plant_code_id, label)) for label in known_labels()}
