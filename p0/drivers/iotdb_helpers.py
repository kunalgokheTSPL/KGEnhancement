"""IoTDB data helpers p0 owns, because the utility drivers no longer expose them."""

from __future__ import annotations

import datetime as dt
import logging
import re
from functools import reduce

import pandas as pd
import requests

_log = logging.getLogger(__name__)

_TAG_LOOKUP_DB = "decisionops_cdm_copy"


def get_iotdb_connection(host=None, port=None, user=None, password=None) -> dict:
    """Build a connection dict (base_url + auth) for the active timeseries backend."""
    from p0.driver import IoTDBDriver

    return IoTDBDriver(host=host, port=port, user=user, password=password).as_dict()


def _query(sql, conn, row_limit=1_000_000):
    """Run a read query against the IoTDB REST v2 endpoint."""
    response = requests.post(
        f"{conn['base_url']}/query",
        json={"sql": sql, "row_limit": row_limit},
        auth=conn["auth"],
        headers={"Content-Type": "application/json"},
        timeout=45,
    )
    response.raise_for_status()
    data = response.json()
    if "code" in data and "message" in data:
        raise RuntimeError(f"IoTDB Error {data['code']}: {data['message']}")
    return data


def _parse_iotdb_result(result):
    """Turn an IoTDB REST result into a DataFrame with a timestamp column."""
    columns = result.get("column_names") or result.get("expressions") or []
    values = result.get("values") or []
    timestamps = result.get("timestamps") or []

    if not values or not values[0]:
        return pd.DataFrame(columns=["timestamp"])

    n_rows = len(values[0])
    rows = []
    for i in range(n_rows):
        row = {"timestamp": timestamps[i] if i < len(timestamps) else None}
        for j, col in enumerate(columns):
            row[col] = values[j][i] if j < len(values) else None
        rows.append(row)
    return pd.DataFrame(rows)


def list_timeseries_paths(conn, pattern="root.decisionops.**"):
    """List timeseries paths matching a pattern."""
    result = _query(f"SHOW TIMESERIES {pattern}", conn)
    df = _parse_iotdb_result(result)
    if df.empty:
        return []
    cols = {c.lower(): c for c in df.columns}
    if "timeseries" not in cols:
        return []
    return df[cols["timeseries"]].dropna().astype(str).tolist()


def resolve_path(tag, conn):
    """Resolve a tag name to its full IoTDB timeseries path."""
    if tag.startswith("root."):
        return tag
    pattern = f"root.**.{tag}"
    matches = list_timeseries_paths(conn, pattern)
    exact = [p for p in matches if p.split(".")[-1] == tag]
    if exact:
        matches = exact
    if not matches:
        resolved_tag = tag
        try:
            from p0.driver import PostgresDriver

            driver = PostgresDriver()
            driver.config["database"] = _TAG_LOOKUP_DB
            db_conn = driver.connect()
            with db_conn.cursor() as cur:
                cur.execute(
                    "SELECT iotdb_tag_id FROM public.timeseries_metadata WHERE tag_name = %s LIMIT 1",
                    (tag,),
                )
                row = cur.fetchone()
                if row and row[0]:
                    resolved_tag = row[0]
        except Exception as exc:
            _log.warning("Failed to lookup iotdb_tag_id in Postgres for %s: %s", tag, exc)

        if resolved_tag == tag:
            s = re.sub(r"[\.\s\-/]", "_", tag)
            resolved_tag = re.sub(r"[^a-zA-Z0-9_]", "", s)

        pattern = f"root.**.{resolved_tag}"
        matches = list_timeseries_paths(conn, pattern)
        exact = [p for p in matches if p.split(".")[-1] == resolved_tag]
        if exact:
            matches = exact

    if not matches:
        raise ValueError(f"No timeseries found for tag '{tag}'")
    return matches[0]


def fetch_iot_data(conn, tags, start, end):
    """Fetch one DataFrame of the given tags between start and end."""

    def _to_epoch_ms(value):
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, dt.datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=dt.timezone.utc)
            return int(value.timestamp() * 1000)
        return int(pd.to_datetime(value, utc=True).timestamp() * 1000)

    start_ms = _to_epoch_ms(start)
    end_ms = _to_epoch_ms(end)
    start_sec = start_ms // 1000
    end_sec = end_ms // 1000
    dfs = []

    for tag in tags:
        try:
            full_path = resolve_path(tag, conn)
        except Exception as exc:
            _log.warning("Skipping tag '%s': %s", tag, exc)
            continue

        device, measurement = full_path.rsplit(".", 1)
        sql = (
            f"SELECT {measurement} FROM {device} "
            f"WHERE (time >= {start_ms} AND time <= {end_ms}) "
            f"OR (time >= {start_sec} AND time <= {end_sec})"
        )
        result = _query(sql, conn)
        df_raw = _parse_iotdb_result(result)
        if df_raw.empty:
            continue

        value_col = [c for c in df_raw.columns if c != "timestamp"][0]
        df_raw["timestamp"] = df_raw["timestamp"].apply(
            lambda x: x * 1000 if pd.notna(x) and x < 10_000_000_000 else x
        )
        df = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(df_raw["timestamp"], unit="ms"),
                tag: df_raw[value_col],
            }
        )
        dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    df_final = reduce(
        lambda left, right: pd.merge(left, right, on="timestamp", how="outer"),
        dfs,
    )
    return df_final.sort_values("timestamp").reset_index(drop=True)
