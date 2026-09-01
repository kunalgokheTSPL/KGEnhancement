# p0/Data_Quality/utils.py

from __future__ import annotations

import sys
from functools import reduce
from pathlib import Path

import pandas as pd

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from p0.driver import _parse_iotdb_result, _query


def get_numeric_tags(conn, pattern: str = "root.plant.**") -> list:
    """
    Return only DOUBLE tags from IoTDB — skip TEXT, INT, BOOLEAN etc.
    Also returns full paths so same-named tags from different devices don't clash.

    Returns list of (full_path, tag_name) tuples.
    """
    result = _query(f"SHOW TIMESERIES {pattern}", conn)

    if not result.get("values"):
        return []

    # column_names: Timeseries, Alias, Database, DataType, ...
    col_names = result.get("column_names", [])
    values = result.get("values", [])

    # Find index of Timeseries and DataType columns
    try:
        ts_idx = [c.lower() for c in col_names].index("timeseries")
        type_idx = [c.lower() for c in col_names].index("datatype")
    except ValueError:
        # Fallback: first column is timeseries, fourth is datatype
        ts_idx, type_idx = 0, 3

    paths = values[ts_idx]
    data_types = values[type_idx]

    numeric_tags = []
    for path, dtype in zip(paths, data_types):
        if path and dtype and dtype.upper() in ("DOUBLE", "FLOAT", "INT32", "INT64"):
            # Use last part as column name — but make unique if duplicate
            tag_name = path.split(".")[-1]
            numeric_tags.append((path, tag_name))

    # Deduplicate tag names — if same name exists in multiple devices, use full path as name
    seen = {}
    for full_path, tag_name in numeric_tags:
        seen.setdefault(tag_name, []).append(full_path)

    result_tags = []
    for full_path, tag_name in numeric_tags:
        if len(seen[tag_name]) > 1:
            # Use device.measurement as name to avoid collision
            parts = full_path.split(".")
            col_name = f"{parts[-2]}.{parts[-1]}"
        else:
            col_name = tag_name
        result_tags.append((full_path, col_name))

    return result_tags


def fetch_all_data(conn, pattern: str = "root.plant.**") -> pd.DataFrame:
    """
    Fetch ALL available data for all DOUBLE tags from IoTDB.
    No start/end needed — queries everything.

    Parameters
    ----------
    conn    : dict   Connection from get_iotdb_connection()
    pattern : str    IoTDB pattern (default root.plant.**)

    Returns
    -------
    pd.DataFrame  Wide-format: timestamp + one column per numeric tag
    """
    # Get only numeric tags with deduplicated column names
    tag_pairs = get_numeric_tags(conn, pattern)

    if not tag_pairs:
        return pd.DataFrame()

    dfs = []

    for full_path, col_name in tag_pairs:
        try:
            device, measurement = full_path.rsplit(".", 1)
        except Exception:
            continue

        # No WHERE clause — fetch everything
        sql = f"SELECT {measurement} FROM {device}"
        result = _query(sql, conn)
        df_raw = _parse_iotdb_result(result)

        if df_raw.empty:
            continue

        value_col = [c for c in df_raw.columns if c != "timestamp"][0]

        # Normalise timestamps: seconds → milliseconds
        df_raw["timestamp"] = df_raw["timestamp"].apply(
            lambda x: x * 1000 if pd.notna(x) and x < 10_000_000_000 else x
        )

        df = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(df_raw["timestamp"], unit="ms"),
                col_name: df_raw[value_col],
            }
        )

        dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    df_final = reduce(
        lambda l, r: pd.merge(l, r, on="timestamp", how="outer"),
        dfs,
    )

    return df_final.sort_values("timestamp").reset_index(drop=True)
