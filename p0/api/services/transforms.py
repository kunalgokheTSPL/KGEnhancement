"""Row-level shaping used by the context endpoints: coercion, filtering, retransform."""

from __future__ import annotations

import json as _json
import logging
import os
import re
from datetime import datetime, timezone
from http import HTTPStatus

import pandas as _pd
import yaml as _yaml
from fastapi import HTTPException

from p0.utils import fs as _fs
from p0.utils.config_resolver import resolve_config_path
from p0.utils.derived import apply_derived_fields
from p0.utils.doc_asset_identity import (
    enrich_document_asset_identity,
)
from p0.utils.identity import apply_identity_rules
from p0.utils.measurement import normalize_measurement

from ..config import COMMON_DIR, rustfs_out_dir

_log = logging.getLogger("cdm.api.context")

_SI_SUFFIX = {"k": 1e3, "m": 1e6, "g": 1e9, "b": 1e9, "t": 1e12}


def _coerce_numeric_limit(value):
    """Best-effort coerce a messy numeric-limit cell to a float."""
    if value is None:
        return None, None
    if isinstance(value, (int, float)):
        return float(value), None
    s = str(value).strip()
    if s == "" or s.lower() in {
        "n/a",
        "na",
        "none",
        "null",
        "-",
        "--",
        "faulty tag",
        "faulty",
    }:
        return None, ("placeholder" if s else None)
    try:
        return float(s), None
    except ValueError:
        pass
    cleaned = s.replace(",", "")
    m = re.match(r"^[+-]?\d*\.?\d+\s*([kKmMgGbBtT])?", cleaned)
    if not m:
        return None, f"non-numeric:{s[:40]}"
    num = m.group(0)
    suffix = (m.group(1) or "").lower()
    try:
        base = float(num[:-1] if suffix else num)
    except ValueError:
        return None, f"non-numeric:{s[:40]}"
    return base * _SI_SUFFIX.get(suffix, 1.0), None


PG_NULL_TOKENS = ("nat", "nan", "none", "null", "")


def _coerce_for_pg(value, pg_type: str):
    """Coerce one Python value so it binds cleanly to a Postgres column."""
    if value is None:
        return None
    try:
        if _pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, str) and value.strip().lower() in PG_NULL_TOKENS:
        return None
    if pg_type in ("bigint", "integer", "smallint"):
        try:
            return int(value)
        except (TypeError, ValueError):
            pass
        try:
            as_float = float(value)
        except (TypeError, ValueError):
            return None
        return int(as_float) if as_float.is_integer() else None
    if pg_type in ("numeric", "double precision", "real"):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
    if pg_type == "boolean":
        if isinstance(value, bool):
            return value
        s = str(value).strip().lower()
        if s in ("true", "t", "1", "yes"):
            return True
        if s in ("false", "f", "0", "no"):
            return False
        return None
    if pg_type in ("json", "jsonb"):

        if isinstance(value, (dict, list)):
            return _json.dumps(value, default=str)
        if isinstance(value, str):
            try:
                _json.loads(value)
                return value
            except (ValueError, TypeError):
                return _json.dumps(value)
        return _json.dumps(value, default=str)
    return value


def _norm_fname(name: str) -> str:
    """Normalise a filename for tolerant matching: basename, lower-cased,"""
    base = os.path.basename(str(name or "")).strip().lower()
    stem, ext = os.path.splitext(base)
    stem = re.sub(r"[\s_\-]+", "_", stem).strip("_")
    ext = ext.strip()
    return f"{stem}{ext}"


def _apply_plant_code(rows: list[dict], plant_code_id: str | None) -> list[dict]:
    """If ``plant_code_id`` is supplied, OVERWRITE every row's plant_code_id field"""
    if not plant_code_id:
        return list(rows)
    from ..plants import validate_plant_code

    validate_plant_code(plant_code_id)
    return [{**r, "plant_code_id": plant_code_id, "site": r.get("site") or "-"} for r in rows]


def _stamp_commit_timestamps(
    rows: list[dict],
    original_created_by_file: dict,
) -> list[dict]:
    """In-place stamp every row with ``created_at`` / ``updated_at``."""

    now = datetime.now(timezone.utc)
    for r in rows:
        sf = str(r.get("source_file", "") or "").strip()
        r["updated_at"] = now
        if not r.get("created_at"):
            r["created_at"] = original_created_by_file.get(sf, now)
    return rows


def _filter_rows_by_source_file(rows: list[dict], source_file: str) -> list[dict]:
    """Keep only rows whose ``source_file`` matches (per-file review scoping)."""
    if not source_file:
        return rows
    has_col = any("source_file" in r for r in rows)
    if not has_col:
        return rows
    want = str(source_file).strip().replace("\\", "/").rsplit("/", 1)[-1]

    def _base(v):
        return str(v or "").strip().replace("\\", "/").rsplit("/", 1)[-1]

    return [r for r in rows if _base(r.get("source_file")) == want]


def _batch_id_set(upload_batch_id) -> set[str]:
    """Normalise one id, a comma-separated string, or a sequence into a set of ids."""
    if not upload_batch_id:
        return set()
    raw = (
        list(upload_batch_id)
        if isinstance(upload_batch_id, (list, tuple, set))
        else str(upload_batch_id).split(",")
    )
    return {str(b).strip() for b in raw if str(b or "").strip()}


def _filter_rows_by_batch(rows: list[dict], upload_batch_id) -> list[dict]:
    """Keep only rows whose ``upload_batch_id`` matches any of the requested ids."""
    wanted = _batch_id_set(upload_batch_id)
    if not wanted:
        return rows
    has_col = any("upload_batch_id" in r for r in rows)
    if not has_col:
        return rows
    return [r for r in rows if str(r.get("upload_batch_id") or "").strip() in wanted]


def _filter_pnid_context_by_files(
    equipment_pid: list[dict],
    equipment_connectivity: list[dict],
    pnid_images: list[dict],
    file_names: list[str],
) -> tuple[list[dict], list[dict], list[dict]]:
    """Restrict PNID review data to a specific upload job/file set."""
    wanted = {_norm_fname(name) for name in file_names if str(name).strip()}
    if not wanted:
        return equipment_pid, equipment_connectivity, pnid_images

    def _row_file(row: dict) -> str:
        return _norm_fname(row.get("source_file") or row.get("_source_table") or "")

    pid_filtered = [row for row in equipment_pid if _row_file(row) in wanted]
    conn_filtered = [row for row in equipment_connectivity if _row_file(row) in wanted]
    image_filtered = [
        img for img in pnid_images if _norm_fname(img.get("filename") or "") in wanted
    ]

    return pid_filtered, conn_filtered, image_filtered


def _parse_commit_body(body, source: str) -> tuple[str, str | None, list[dict]]:
    """Parse a commit request body into (plant_code_id, upload_batch_id, rows)."""
    if isinstance(body, list):
        _log.warning(
            "[context/%s/commit] DEPRECATED bare-list body — send "
            "{plant_code_id, rows[, upload_batch_id]} instead.",
            source,
        )
        rows: list[dict] = body
        plant_code_id = (
            next(
                (str(r.get("plant_code_id") or "") for r in rows if r.get("plant_code_id")),
                None,
            )
            or None
        )
        upload_batch_id = None
    else:
        rows = body.get("rows", []) or []
        plant_code_id = body.get("plant_code_id")
        upload_batch_id = (
            body.get("upload_batch_id")
            or body.get("batch_id")
            or body.get("pipeline_job_id")
        )
    if not plant_code_id:
        raise HTTPException(HTTPStatus.BAD_REQUEST, "plant_code_id is required")
    from ..plants import validate_plant_code as _vp

    _vp(plant_code_id)
    return plant_code_id, upload_batch_id, rows


def _retransform_pnid_rows(
    plant_code_id: str, equipment_pid: list[dict], equipment_connectivity: list[dict]
) -> tuple[list[dict], list[dict]]:
    """Re-run the post-extraction transform steps over edited P&ID review rows so"""
    try:

        def _cfg(fname):
            try:
                with open(resolve_config_path(str(COMMON_DIR.parent), fname)) as f:
                    return _yaml.safe_load(f) or {}
            except Exception:
                return {}

        def _norm_upper(s):
            x = (
                s.astype(str)
                .str.strip()
                .str.upper()
                .str.replace(r"\s+", " ", regex=True)
            )
            return x.replace({"NAN": "", "NONE": "", "NAT": ""})

        derived = _cfg("derived_fields.yaml")
        identity = _cfg("identity_frozen.yaml")

        ep = equipment_pid
        if ep:
            df = _pd.DataFrame(ep)
            df = apply_derived_fields(
                df,
                derived,
                dataset_name="pid_equipment",
                source_name="pid",
                plant_code_id=plant_code_id,
            )
            df["plant_code_id"] = plant_code_id
            df["source_system"] = "PID"
            if "equipment_tag" in df.columns:
                df["equipment_tag"] = _norm_upper(df["equipment_tag"])
                df["normalized_asset"] = df["equipment_tag"]
            df = apply_identity_rules(df, identity, dataset_name="pid_equipment")
            if "normalized_asset" in df.columns:
                df["normalized_asset"] = _norm_upper(df["normalized_asset"])
                if "equipment_tag" in df.columns:
                    df.loc[df["normalized_asset"] == "", "normalized_asset"] = df[
                        "equipment_tag"
                    ]
            ep = df.where(_pd.notna(df), None).to_dict(orient="records")

        ec = equipment_connectivity
        if ec:
            dc = _pd.DataFrame(ec)
            dc = apply_derived_fields(
                dc,
                derived,
                dataset_name="pid",
                source_name="pid",
                plant_code_id=plant_code_id,
            )
            dc["plant_code_id"] = plant_code_id
            dc["source_system"] = "PID"
            ec = dc.where(_pd.notna(dc), None).to_dict(orient="records")

        _log.info(
            "[context/pnid] re-transformed %d equipment + %d connectivity edited row(s)",
            len(ep or []),
            len(ec or []),
        )
        return ep, ec
    except Exception as exc:
        _log.warning("[context/pnid] re-transform failed (%s); saving rows as-is", exc)
        return equipment_pid, equipment_connectivity


def _retransform_docs_rows(plant_code_id: str, rows: list[dict]) -> list[dict]:
    """Re-run the in-pipeline TRANSFORM steps over user-edited DOCS review rows so"""
    if not rows:
        return rows
    try:

        def _cfg(fname):
            try:
                with open(resolve_config_path(str(COMMON_DIR.parent), fname)) as f:
                    return _yaml.safe_load(f) or {}
            except Exception:
                return {}

        df = _pd.DataFrame(rows)
        try:
            pid_base = rustfs_out_dir(plant_code_id)
            ref = [
                _fs.path_join(pid_base, "entities", "equipment.parquet"),
                _fs.path_join(pid_base, "entities", "equipment.csv"),
            ]
            df = enrich_document_asset_identity(df, reference_candidates=ref)
        except Exception as _e:
            _log.warning("[context/docs] asset-identity re-enrich skipped: %s", _e)
        df = apply_derived_fields(
            df,
            _cfg("derived_fields.yaml"),
            dataset_name="documents",
            source_name="documents",
            plant_code_id=plant_code_id,
        )
        df["plant_code_id"] = plant_code_id
        df = apply_identity_rules(
            df, _cfg("identity_frozen.yaml"), dataset_name="documents"
        )
        out = df.where(_pd.notna(df), None).to_dict(orient="records")
        _log.info(
            "[context/docs] re-transformed %d edited row(s) through pipeline steps",
            len(out),
        )
        return out
    except Exception as exc:
        _log.warning("[context/docs] re-transform failed (%s); saving rows as-is", exc)
        return rows


def _retransform_ts_rows(plant_code_id: str, rows: list[dict]) -> list[dict]:
    """Re-run the in-pipeline TRANSFORM steps over user-edited review rows so a"""
    if not rows:
        return rows
    try:

        def _cfg(fname):
            try:
                with open(resolve_config_path(str(COMMON_DIR.parent), fname)) as f:
                    return _yaml.safe_load(f) or {}
            except Exception:
                return {}

        df = _pd.DataFrame(rows)
        if "tag_name" in df.columns:
            df["iotdb_tag_id"] = df["tag_name"].map(normalize_measurement)
        df = apply_derived_fields(
            df,
            _cfg("derived_fields.yaml"),
            dataset_name="timeseries",
            source_name="timeseries",
            plant_code_id=plant_code_id,
        )
        df["plant_code_id"] = plant_code_id
        df = apply_identity_rules(
            df, _cfg("identity_frozen.yaml"), dataset_name="timeseries"
        )
        out = df.where(_pd.notna(df), None).to_dict(orient="records")
        _log.info(
            "[context/ts] re-transformed %d edited row(s) through pipeline steps",
            len(out),
        )
        return out
    except Exception as exc:
        _log.warning("[context/ts] re-transform failed (%s); saving rows as-is", exc)
        return rows


def as_list(raw) -> list[str]:
    """Normalise any shape a multi-value field arrives in."""
    if raw is None:
        return []
    values: list = raw if isinstance(raw, (list, tuple, set)) else [raw]
    out: list[str] = []
    seen: set[str] = set()
    for entry in values:
        if entry is None:
            continue
        text = str(entry).strip()
        if not text:
            continue
        parts: list[str]
        if text.startswith("["):
            try:
                import json as _json

                decoded = _json.loads(text)
                parts = [str(p) for p in decoded] if isinstance(decoded, list) else [text]
            except ValueError:
                parts = text.split(",")
        elif "," in text:
            parts = text.split(",")
        else:
            parts = [text]
        for part in parts:
            cleaned = part.strip()
            if cleaned and cleaned not in seen:
                seen.add(cleaned)
                out.append(cleaned)
    return out
