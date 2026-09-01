"""IoTDB timeseries ingest: parsing, batching, retention and device queries."""

from __future__ import annotations

import logging
import math as _math
import re
import time as _time
from http import HTTPStatus
from pathlib import Path

import pandas as pd
import requests as _requests

from p0.utils.device_lock import device_lock
from p0.utils.measurement import (
    dedupe_measurements,
    normalize_measurement,
)
from p0.driver import AzureSQLDriver, IoTDBDriver, timeseries_backend

from ..config import IOTDB_DEVICE_ROOT, USER_CONFIG_FILE
from .connection import _get_conn as _pg_conn
from ..deps import load_yaml

_ts_log = logging.getLogger("cdm.connector.timeseries")


_IOTDB_BATCH = 1000

_IOTDB_ROOT = IOTDB_DEVICE_ROOT


_IOTDB_MAX_RETRIES = 5

_IOTDB_RETRY_BACKOFF = 0.5

_IOTDB_INTER_BATCH_SLEEP = 0.2

_TS_CSV_CHUNK_ROWS = 50_000


def _iotdb_driver():
    return IoTDBDriver()


def _iotdb_insert_url() -> str:
    return _iotdb_driver().insert_tablet_url()


def _iotdb_query_url() -> str:
    return _iotdb_driver().query_url()


def _iotdb_nonquery_url() -> str:
    return _iotdb_driver().non_query_url()


def _iotdb_auth() -> tuple[str, str]:
    return _iotdb_driver().auth


def _norm_measurement(name: str) -> str:
    """Normalize a sensor tag to a valid IoTDB measurement name. Thin alias over"""
    return normalize_measurement(name)


def _device_stem(
    device_name: str | None, file_path: "Path", content_tag: str | None = None
) -> str:
    """Build the IoTDB device stem (the segment between <plant> and the tag)."""
    raw_stem = Path(device_name).stem if device_name else file_path.stem
    stem = normalize_measurement(raw_stem)
    if not stem or not stem.strip("_"):
        fallback = normalize_measurement(str(content_tag))[:12] if content_tag else ""
        stem = f"file_{fallback}" if fallback else "file"
        _ts_log.warning(
            "[ts/ingest] degenerate device stem from %r — using %r",
            device_name or (file_path.name if file_path else None),
            stem,
        )
        return stem
    if content_tag:
        tag = normalize_measurement(str(content_tag))[:12]
        if tag:
            return f"{stem}__{tag}"
    return stem


def _parse_ts_series(series):
    """Parse a timestamp column to datetime, robust to ISO vs day-first formats."""
    s = series.astype(str).str.strip()
    sample = s[s.str.len() > 0]
    is_iso = bool(len(sample)) and bool(
        re.match(r"^\d{4}[-/]\d{1,2}[-/]\d{1,2}", sample.iloc[0])
    )
    if is_iso:
        return pd.to_datetime(series, yearfirst=True, dayfirst=False, errors="coerce")
    return pd.to_datetime(series, dayfirst=True, errors="coerce")


def _datetime_to_epoch_ms(series) -> "pd.Series":
    """Convert a parsed datetime Series to int64 epoch milliseconds."""
    if getattr(series.dt, "tz", None) is not None:
        series = series.dt.tz_convert("UTC").dt.tz_localize(None)
    return series.astype("datetime64[ns]").astype("int64") // 1_000_000


def _retention_ms_from_config() -> tuple[int, int, str]:
    """Return (ttl_ms, value, unit) parsed from user_config.yaml."""
    cfg = load_yaml(USER_CONFIG_FILE)
    retention = cfg.get("timeseries", {}).get("retention", {}) or {}
    if not retention:
        return 0, 0, ""
    try:
        value = int(retention.get("value", 0))
    except (ValueError, TypeError):
        value = 0
    unit = str(retention.get("unit", "days"))
    if value <= 0:
        return 0, value, unit
    if unit == "months":
        ms = value * 30 * 86_400_000
    elif unit == "years":
        ms = value * 365 * 86_400_000
    else:
        ms = value * 86_400_000
    return ms, value, unit


def _insert_frame_to_iotdb(
    df, time_col, sensor_cols, norm_cols, device, session=None
) -> int:
    """POST one (already time-parsed, retention-filtered) dataframe to IoTDB in"""
    auth = _iotdb_auth()
    headers = {"Content-Type": "application/json"}
    if session is None:
        session = _requests.Session()

    def _clean(v):
        try:
            if v is None or _math.isnan(v) or _math.isinf(v):
                return None
        except (TypeError, ValueError):
            pass
        return v

    total = 0
    try:
        for start in range(0, len(df), _IOTDB_BATCH):
            chunk = df.iloc[start : start + _IOTDB_BATCH]
            ts_batch = _datetime_to_epoch_ms(chunk[time_col]).tolist()
            vals_batch = [
                [_clean(v) for v in chunk[col].tolist()] for col in sensor_cols
            ]
            payload = {
                "device": device,
                "is_aligned": False,
                "measurements": norm_cols,
                "data_types": ["FLOAT"] * len(norm_cols),
                "timestamps": ts_batch,
                "values": vals_batch,
            }
            inserted = False
            for attempt in range(1, _IOTDB_MAX_RETRIES + 1):
                try:
                    resp = session.post(
                        _iotdb_insert_url(),
                        json=payload,
                        auth=auth,
                        headers=headers,
                        timeout=60,
                    )
                    body_code = None
                    if resp.status_code in (HTTPStatus.OK, HTTPStatus.CREATED):
                        try:
                            body_code = resp.json().get("code")
                        except Exception:
                            body_code = HTTPStatus.OK
                    if resp.status_code in (
                        HTTPStatus.OK,
                        HTTPStatus.CREATED,
                    ) and body_code in (HTTPStatus.OK, HTTPStatus.CREATED, None):
                        total += len(ts_batch)
                        inserted = True
                        break
                    _ts_log.warning(
                        "IoTDB insert failed  device=%s  tablet=%d  http=%d  iotdb_code=%s  body=%s",
                        device,
                        len(ts_batch),
                        resp.status_code,
                        body_code,
                        resp.text[:300],
                    )
                    break
                except Exception as exc:
                    try:
                        session.close()
                    except Exception:
                        pass
                    session = _requests.Session()
                    if attempt < _IOTDB_MAX_RETRIES:
                        _time.sleep(_IOTDB_RETRY_BACKOFF * (2 ** (attempt - 1)))
                        continue
                    _ts_log.warning(
                        "IoTDB insert error for %s after %d attempts: %s",
                        device,
                        attempt,
                        exc,
                    )
            if _IOTDB_INTER_BATCH_SLEEP:
                _time.sleep(_IOTDB_INTER_BATCH_SLEEP)
    finally:
        try:
            session.close()
        except Exception:
            pass
    return total


def _insert_frame(df, time_col, sensor_cols, norm_cols, device) -> int:
    """Insert one prepared frame into the active timeseries backend (IoTDB or Azure SQL)."""
    if timeseries_backend() != "azuresql":
        return _insert_frame_to_iotdb(df, time_col, sensor_cols, norm_cols, device)
    from . import timeseries_sql as _ts_sql

    driver = AzureSQLDriver()
    try:
        return _ts_sql.insert_frame(
            driver.connect(), df, time_col, sensor_cols, norm_cols, device
        )
    finally:
        driver.close()


def _apply_retention_window(df, time_col: str, source_label: str) -> tuple:
    """Trim a parsed TS dataframe to the configured retention window."""
    rows_total = int(len(df))
    window_ms, value, unit = _retention_ms_from_config()
    stats = {
        "rows_total": rows_total,
        "rows_kept": rows_total,
        "rows_dropped": 0,
        "window_ms": window_ms,
        "window_value": value,
        "window_unit": unit,
        "t_max_file_ms": None,
        "t_min_kept_ms": None,
        "cutoff_ms": None,
        "retention_applied": False,
    }
    if window_ms <= 0 or rows_total == 0:
        return df, stats
    t_max_ts = df[time_col].max()
    cutoff_ts = t_max_ts - pd.Timedelta(milliseconds=window_ms)
    mask = df[time_col] >= cutoff_ts
    kept_n = int(mask.sum())
    dropped_n = rows_total - kept_n

    def _to_ms(ts) -> int:
        return int(ts.timestamp() * 1000)

    stats.update(
        {
            "rows_kept": kept_n,
            "rows_dropped": dropped_n,
            "t_max_file_ms": _to_ms(t_max_ts),
            "cutoff_ms": _to_ms(cutoff_ts),
            "retention_applied": True,
        }
    )

    if dropped_n == 0:
        _ts_log.info(
            "[ts/retention] %s: file span <= window (%d %s) — keeping all %d rows",
            source_label,
            value,
            unit,
            rows_total,
        )
        return df, stats

    filtered = df[mask]
    if len(filtered):
        stats["t_min_kept_ms"] = _to_ms(filtered[time_col].min())
    _ts_log.info(
        "[ts/retention] %s: kept %d / %d rows  dropped=%d  t_max=%d  cutoff=%d  window=%d %s",
        source_label,
        kept_n,
        rows_total,
        dropped_n,
        stats["t_max_file_ms"],
        stats["cutoff_ms"],
        value,
        unit,
    )
    return filtered, stats


def _ingest_csv_chunked(
    file_path: "Path",
    plant_code_id: str,
    device_name: str | None = None,
    content_tag: str | None = None,
) -> tuple[int, dict]:
    """Memory-bounded CSV → IoTDB ingest for LARGE values files."""
    stem = _device_stem(device_name, file_path, content_tag)
    plant = _norm_measurement(plant_code_id)
    file_stats: dict = {"file": device_name or file_path.name, "rows_inserted": 0}
    device = f"{_IOTDB_ROOT}.{plant}.{stem}"
    _ts_log.info("IoTDB device: %s  file(chunked): %s", device, file_path.name)

    try:
        header = pd.read_csv(file_path, nrows=0)
    except Exception as exc:
        file_stats["error"] = f"read_failed: {exc!s:.200}"
        return 0, file_stats
    time_col = next(
        (
            c
            for c in ("Timestamp", "timestamp", "Date", "date", "datetime", "time")
            if c in header.columns
        ),
        None,
    )
    if time_col is None:
        file_stats["error"] = "no_timestamp_column"
        return 0, file_stats

    window_ms, value, unit = _retention_ms_from_config()
    cutoff_ts = None
    t_max_ts = None
    try:
        ts_only = pd.read_csv(file_path, usecols=[time_col])
        ts_parsed = _parse_ts_series(ts_only[time_col])
        if ts_parsed.notna().any():
            t_max_ts = ts_parsed.max()
            if window_ms > 0:
                cutoff_ts = t_max_ts - pd.Timedelta(milliseconds=window_ms)
        del ts_only, ts_parsed
    except Exception as exc:
        _ts_log.warning(
            "[ts/ingest] %s: t_max pass failed (%s) — ingesting without retention",
            file_path.name,
            exc,
        )

    sensor_cols: list[str] | None = None
    norm_cols: list[str] | None = None
    total = 0
    rows_seen = 0
    rows_kept = 0
    try:
        reader = pd.read_csv(file_path, chunksize=_TS_CSV_CHUNK_ROWS)
    except Exception as exc:
        file_stats["error"] = f"read_failed: {exc!s:.200}"
        return 0, file_stats
    session = _requests.Session()
    for raw in reader:
        rows_seen += len(raw)
        if sensor_cols is None:
            cand = [c for c in raw.columns if c != time_col]
            for c in cand:
                raw[c] = pd.to_numeric(raw[c], errors="coerce")
            sensor_cols = [c for c in cand if raw[c].notna().any()]
            if not sensor_cols:
                file_stats["error"] = "no_numeric_columns"
                return 0, file_stats
            norm_cols, _collisions = dedupe_measurements(sensor_cols)
            if _collisions:
                file_stats["collisions"] = _collisions
                _ts_log.warning(
                    "[ts/ingest] %s: %d measurement-name collision(s) disambiguated",
                    file_path.name,
                    len(_collisions),
                )
        else:
            for c in sensor_cols:
                raw[c] = pd.to_numeric(raw[c], errors="coerce")

        raw[time_col] = _parse_ts_series(raw[time_col])
        raw = raw[raw[time_col].notna()]
        _b = len(raw)
        raw = raw.drop_duplicates(subset=[time_col], keep="last")
        if len(raw) < _b:
            file_stats.setdefault("rejects", {})
            file_stats["rejects"]["duplicate_timestamp"] = file_stats["rejects"].get(
                "duplicate_timestamp", 0
            ) + (_b - len(raw))
        if cutoff_ts is not None:
            raw = raw[raw[time_col] >= cutoff_ts]
        if len(raw) == 0:
            continue
        rows_kept += len(raw)
        total += _insert_frame_to_iotdb(
            raw, time_col, sensor_cols, norm_cols, device, session=session
        )
        del raw
    session.close()

    def _to_ms(ts):
        return int(ts.timestamp() * 1000) if ts is not None else None

    file_stats["rows_inserted"] = total
    file_stats["device"] = device
    file_stats["retention"] = {
        "rows_total": rows_seen,
        "rows_kept": rows_kept,
        "rows_dropped": rows_seen - rows_kept,
        "window_ms": window_ms,
        "window_value": value,
        "window_unit": unit,
        "t_max_file_ms": _to_ms(t_max_ts),
        "cutoff_ms": _to_ms(cutoff_ts),
        "retention_applied": cutoff_ts is not None,
    }
    _ts_log.info(
        "IoTDB %-42s %6d rows (chunked; seen=%d kept=%d)",
        file_path.stem,
        total,
        rows_seen,
        rows_kept,
    )
    return total, file_stats


def _ingest_frame(df, plant_code_id: str, device: str, label: str) -> tuple[int, dict]:
    """Ingest one wide-format dataframe (timestamp + N value columns) into the"""
    stats: dict = {
        "device": device,
        "rows_inserted": 0,
        "rows_in": int(len(df)),
        "rows_rejected": 0,
        "rejects": {},
    }

    time_col = next(
        (
            c
            for c in ("Timestamp", "timestamp", "Date", "date", "datetime", "time")
            if c in df.columns
        ),
        None,
    )
    if time_col is None:
        _ts_log.warning("No timestamp column found in %s", label)
        stats["error"] = "no_timestamp_column"
        stats["rows_rejected"] = int(len(df))
        stats["rejects"] = {"no_timestamp_column": int(len(df))}
        return 0, stats

    sensor_cols = [
        c for c in df.columns if c != time_col and pd.api.types.is_numeric_dtype(df[c])
    ]
    if not sensor_cols:
        for c in [c for c in df.columns if c != time_col]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        sensor_cols = [c for c in df.columns if c != time_col and df[c].notna().any()]
    if not sensor_cols:
        _ts_log.warning("No numeric sensor columns found in %s", label)
        stats["error"] = "no_numeric_columns"
        return 0, stats
    norm_cols, _collisions = dedupe_measurements(sensor_cols)
    if _collisions:
        stats["collisions"] = _collisions
        _ts_log.warning(
            "[ts/ingest] %s: %d measurement-name collision(s) disambiguated: %s",
            label,
            len(_collisions),
            _collisions,
        )

    df[time_col] = _parse_ts_series(df[time_col])
    _bad_ts = int(df[time_col].isna().sum())
    if _bad_ts:
        stats["rejects"]["bad_timestamp"] = _bad_ts
        df = df[df[time_col].notna()]
    _before_dups = len(df)
    if _before_dups:
        df = df.drop_duplicates(subset=[time_col], keep="last")
        _dup_ts = _before_dups - len(df)
        if _dup_ts > 0:
            stats["rejects"]["duplicate_timestamp"] = int(_dup_ts)

    _before_ret = len(df)
    df, retention_stats = _apply_retention_window(df, time_col, label)
    stats["retention"] = retention_stats
    _out_of_window = _before_ret - len(df)
    if _out_of_window > 0:
        stats["rejects"]["out_of_retention_window"] = int(_out_of_window)
    if len(df) == 0:
        _ts_log.info(
            "[ts/ingest] %s: empty after retention filter — skipping insert", label
        )
        stats["rows_rejected"] = stats["rows_in"]
        return 0, stats

    total = _insert_frame(df, time_col, sensor_cols, norm_cols, device)
    _ts_log.info("ts %-44s %6d rows  (device=%s)", label, total, device)
    stats["rows_inserted"] = total
    stats["rows_rejected"] = max(0, stats["rows_in"] - total)
    _accounted = sum(stats["rejects"].values())
    if stats["rows_rejected"] > _accounted:
        stats["rejects"]["insert_dropped"] = stats["rows_rejected"] - _accounted
    return total, stats


def _ingest_one_ts_file(
    file_path: Path,
    plant_code_id: str,
    device_name: str | None = None,
    content_tag: str | None = None,
) -> tuple[int, dict]:
    """Ingest one timeseries file (parquet / Excel / csv) into IoTDB."""
    stem = _device_stem(device_name, file_path, content_tag)
    plant = _norm_measurement(plant_code_id)
    file_stats: dict = {"file": device_name or file_path.name, "rows_inserted": 0}
    ext = file_path.suffix.lower()

    try:
        if ext in (".xlsx", ".xls"):
            xl = pd.ExcelFile(file_path)
            sheets = xl.sheet_names
            multi = len(sheets) > 1
            total = 0
            per_sheet: list[dict] = []
            for sh in sheets:
                df = pd.read_excel(xl, sheet_name=sh)
                dev_stem = f"{stem}__{_norm_measurement(str(sh))}" if multi else stem
                device = f"{_IOTDB_ROOT}.{plant}.{dev_stem}"
                rows, sst = _ingest_frame(
                    df, plant_code_id, device, f"{file_path.name}[{sh}]"
                )
                sst["sheet"] = sh
                total += rows
                per_sheet.append(sst)
            file_stats["rows_inserted"] = total
            file_stats["sheets"] = per_sheet
            file_stats["device"] = (
                per_sheet[0]["device"]
                if len(per_sheet) == 1
                else [s["device"] for s in per_sheet]
            )
            if per_sheet and per_sheet[0].get("retention"):
                file_stats["retention"] = per_sheet[0]["retention"]
            _ts_log.info(
                "IoTDB %-30s %d sheet(s)  %6d rows total",
                file_path.name,
                len(sheets),
                total,
            )
            return total, file_stats

        if ext == ".parquet":
            df = pd.read_parquet(file_path)
        else:
            df = pd.read_csv(file_path)
    except Exception as exc:
        _ts_log.warning("Failed to read %s: %s", file_path, exc)
        file_stats["error"] = f"read_failed: {exc!s:.200}"
        return 0, file_stats

    device = f"{_IOTDB_ROOT}.{plant}.{stem}"
    total, stats = _ingest_frame(df, plant_code_id, device, file_path.name)
    file_stats.update({k: v for k, v in stats.items() if k != "rows_inserted"})
    file_stats["rows_inserted"] = total
    return total, file_stats



def _iotdb_list_devices(plant_code_id: str) -> list[str]:
    """Return device paths under ``_IOTDB_ROOT.<plant>`` via SHOW DEVICES."""
    pattern = f"{_IOTDB_ROOT}.{_norm_measurement(plant_code_id)}.**"
    try:
        resp = _requests.post(
            _iotdb_query_url(),
            json={"sql": f"SHOW DEVICES {pattern}"},
            auth=_iotdb_auth(),
            headers={"Content-Type": "application/json"},
            timeout=15,
        )
    except Exception as exc:
        _ts_log.warning("[retention] SHOW DEVICES failed: %s", exc)
        return []
    if resp.status_code not in (HTTPStatus.OK, HTTPStatus.CREATED):
        _ts_log.warning(
            "[retention] SHOW DEVICES status=%d body=%s",
            resp.status_code,
            resp.text[:200],
        )
        return []
    try:
        body = resp.json()
    except Exception as exc:
        _ts_log.warning("[retention] device listing unparseable: %s", exc)
        return []
    devices: list[str] = []
    for col in body.get("values") or []:
        for v in col or []:
            if isinstance(v, str) and v.startswith(_IOTDB_ROOT):
                devices.append(v)
    return list(dict.fromkeys(devices))


def _iotdb_device_t_max(device: str) -> int | None:
    """Return latest timestamp (ms) for one device, or None if empty/error."""
    try:
        resp = _requests.post(
            _iotdb_query_url(),
            json={"sql": f"SELECT MAX_TIME(*) FROM {device}"},
            auth=_iotdb_auth(),
            headers={"Content-Type": "application/json"},
            timeout=15,
        )
    except Exception as exc:
        _ts_log.warning("[retention] MAX_TIME(%s) error: %s", device, exc)
        return None
    if resp.status_code not in (HTTPStatus.OK, HTTPStatus.CREATED):
        _ts_log.warning("[retention] MAX_TIME(%s) -> HTTP %s", device, resp.status_code)
        return None
    try:
        body = resp.json()
    except Exception as exc:
        _ts_log.warning("[retention] MAX_TIME(%s) unparseable: %s", device, exc)
        return None
    best: int | None = None
    for col in body.get("values") or []:
        for v in col or []:
            if isinstance(v, (int, float)) and v == v:
                iv = int(v)
                if best is None or iv > best:
                    best = iv
    return best


def _enforce_ts_retention_sql(plant_code_id, window_ms, value, unit) -> dict:
    """Retention prune for the Azure SQL timeseries backend (per-table DELETE)."""
    from . import timeseries_sql as _ts_sql

    prefix = f"{_IOTDB_ROOT}.{_norm_measurement(plant_code_id)}"
    driver = AzureSQLDriver()
    results: list[dict] = []
    try:
        conn = driver.connect()
        tables = _ts_sql.list_devices(conn, prefix)
        if not tables:
            return {
                "window_ms": window_ms, "value": value, "unit": unit,
                "plant_code_id": plant_code_id, "status": "no_devices", "devices": [],
            }
        for table in tables:
            with device_lock(table, _pg_conn):
                t_max = _ts_sql.device_t_max(conn, table)
                if t_max is None:
                    results.append({"device": table, "status": "empty_or_unreadable"})
                    continue
                cutoff = t_max - window_ms
                removed = _ts_sql.delete_before(conn, table, cutoff)
                results.append({
                    "device": table, "t_max_ms": t_max, "cutoff_ms": cutoff,
                    "window_ms": window_ms, "deleted": removed, "status": "applied",
                })
    except Exception as exc:
        _ts_log.warning("[retention/sql] prune failed for %s: %s", plant_code_id, exc)
        return {
            "window_ms": window_ms, "value": value, "unit": unit,
            "plant_code_id": plant_code_id, "status": "error",
            "error": str(exc)[:200], "devices": results,
        }
    finally:
        driver.close()
    applied = sum(1 for r in results if r.get("status") == "applied")
    return {
        "window_ms": window_ms, "value": value, "unit": unit,
        "plant_code_id": plant_code_id,
        "status": "applied" if applied == len(results) else "partial",
        "devices": results,
    }


def _enforce_ts_retention(plant_code_id: str) -> dict:
    """Trim each device's history so its newest sample sets the right edge."""
    if not plant_code_id:
        raise ValueError("_enforce_ts_retention requires an explicit plant_code_id")

    window_ms, value, unit = _retention_ms_from_config()
    if window_ms <= 0:
        _ts_log.info("[retention] no retention configured — skipping prune")
        return {
            "window_ms": 0,
            "status": "no_retention_configured",
            "value": value,
            "unit": unit,
            "devices": [],
        }

    if timeseries_backend() == "azuresql":
        return _enforce_ts_retention_sql(plant_code_id, window_ms, value, unit)

    devices = _iotdb_list_devices(plant_code_id)
    if not devices:
        _ts_log.info(
            "[retention] no devices under %s.%s — nothing to prune",
            _IOTDB_ROOT,
            plant_code_id,
        )
        return {
            "window_ms": window_ms,
            "value": value,
            "unit": unit,
            "status": "no_devices",
            "devices": [],
        }
    results: list[dict] = []
    for device in devices:
        with device_lock(device, _pg_conn):
            t_max = _iotdb_device_t_max(device)
            if t_max is None:
                results.append({"device": device, "status": "empty_or_unreadable"})
                continue
            cutoff = t_max - window_ms
            sql = f"DELETE FROM {device}.* WHERE time < {cutoff}"
            try:
                resp = _requests.post(
                    _iotdb_nonquery_url(),
                    json={"sql": sql},
                    auth=_iotdb_auth(),
                    headers={"Content-Type": "application/json"},
                    timeout=30,
                )
            except Exception as exc:
                _ts_log.warning("[retention] DELETE error  device=%s  %s", device, exc)
                results.append(
                    {"device": device, "status": "error", "error": str(exc)[:200]}
                )
                continue
            if resp.status_code in (HTTPStatus.OK, HTTPStatus.CREATED):
                results.append(
                    {
                        "device": device,
                        "t_max_ms": t_max,
                        "cutoff_ms": cutoff,
                        "window_ms": window_ms,
                        "status": "applied",
                    }
                )
                _ts_log.info(
                    "[retention] pruned device=%s  t_max=%d  cutoff=%d  window=%d ms (%d %s)",
                    device,
                    t_max,
                    cutoff,
                    window_ms,
                    value,
                    unit,
                )
            else:
                _ts_log.warning(
                    "[retention] DELETE failed  device=%s  status=%d  body=%s",
                    device,
                    resp.status_code,
                    resp.text[:200],
                )
                results.append(
                    {
                        "device": device,
                        "status": "failed",
                        "http_status": resp.status_code,
                        "error": resp.text[:200],
                    }
                )
    applied = sum(1 for r in results if r.get("status") == "applied")
    _ts_log.info(
        "[retention] done  plant=%s  devices=%d  applied=%d  window=%d %s",
        plant_code_id,
        len(devices),
        applied,
        value,
        unit,
    )
    return {
        "window_ms": window_ms,
        "value": value,
        "unit": unit,
        "plant_code_id": plant_code_id,
        "status": "applied" if applied == len(devices) else "partial",
        "devices": results,
    }
