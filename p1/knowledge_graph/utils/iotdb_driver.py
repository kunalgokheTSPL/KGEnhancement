import requests
import pandas as pd
import datetime as dt
from functools import reduce


# --------------------------------------------------
# CONNECTION
# --------------------------------------------------
def get_iotdb_connection(host="localhost", port=18080, user="root", password="root"):
    """
    Build a connection config dict for IoTDB REST API v2.
    Pass the returned dict into all other functions.
    """
    return {"base_url": f"http://{host}:{port}/rest/v2", "auth": (user, password)}


def connect(conn):
    """
    Validate the IoTDB connection by pinging the server.
    Raises an exception if the server is unreachable or returns non-200.
    """
    try:
        response = requests.get(
            f"{conn['base_url']}/ping", auth=conn["auth"], timeout=60
        )
        if response.status_code != 200:
            raise Exception("IoTDB connection failed")
    except Exception as e:
        raise Exception(f"IoTDB connection error: {e}")


# --------------------------------------------------
# TIME CONVERSION
# --------------------------------------------------
def _to_epoch_ms(value):
    """
    Convert various time formats to epoch milliseconds (int).
    Accepts: int/float (already ms), datetime object, or datetime string.
    """
    if isinstance(value, (int, float)):
        return int(value)

    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=dt.timezone.utc)
        return int(value.timestamp() * 1000)

    return int(pd.to_datetime(value, utc=True).timestamp() * 1000)


# --------------------------------------------------
# QUERY EXECUTION
# --------------------------------------------------
def _query(sql, conn, row_limit=1_000_000):
    """
    Execute a raw SQL query against IoTDB REST API.
    Raises RuntimeError on IoTDB-level errors.
    """
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


# --------------------------------------------------
# PARSE RESULT
# --------------------------------------------------
def _parse_iotdb_result(result):
    """
    Parse IoTDB REST API response into a pandas DataFrame.
    Returns an empty DataFrame with a 'timestamp' column if no data found.
    """
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


# --------------------------------------------------
# LIST TIMESERIES
# --------------------------------------------------
def list_timeseries_paths(conn, pattern="root.decisionops.**"):
    """
    List all timeseries paths matching a given pattern.
    Returns a list of path strings.
    """
    result = _query(f"SHOW TIMESERIES {pattern}", conn)
    df = _parse_iotdb_result(result)

    if df.empty:
        return []

    cols = {c.lower(): c for c in df.columns}
    if "timeseries" not in cols:
        return []

    return df[cols["timeseries"]].dropna().astype(str).tolist()


# --------------------------------------------------
# RESOLVE TAG → FULL PATH
# --------------------------------------------------
def resolve_path(tag, conn):
    """
    Resolve a short tag name to its full IoTDB path.
    If the tag already starts with 'root.' it is returned as-is.
    Raises ValueError if no match is found.
    """
    if tag.startswith("root."):
        return tag

    pattern = f"root.decisionops.**.{tag}"
    matches = list_timeseries_paths(conn, pattern)

    # Prefer exact leaf matches
    exact = [p for p in matches if p.split(".")[-1] == tag]
    if exact:
        matches = exact

    if not matches:
        raise ValueError(f"No timeseries found for tag '{tag}'")

    if len(matches) > 1:
        print(f"⚠️  Multiple matches for '{tag}', using first: {matches[0]}")

    return matches[0]


# --------------------------------------------------
# MAIN FETCH FUNCTION
# --------------------------------------------------
def fetch_iot_data(conn, tags, start, end):
    """
    Fetch time-series data for a list of tags over a time range.

    Parameters
    ----------
    conn  : dict         Connection config from get_iotdb_connection()
    tags  : list[str]    Tag names or full IoTDB paths
    start : str/datetime Start of time range
    end   : str/datetime End of time range

    Returns
    -------
    pd.DataFrame  Wide-format DataFrame with columns: timestamp, <tag1>, <tag2>, ...
                  Returns an empty DataFrame if no data is found.
    """
    start_ms = _to_epoch_ms(start)
    end_ms = _to_epoch_ms(end)

    # IoTDB may store timestamps in seconds or milliseconds — query both
    start_sec = start_ms // 1000
    end_sec = end_ms // 1000

    dfs = []

    for tag in tags:
        try:
            full_path = resolve_path(tag, conn)
        except Exception as e:
            print(f"⚠️  Skipping tag '{tag}': {e}")
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

        # Normalise timestamps: seconds → milliseconds
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

    df_final = reduce(lambda l, r: pd.merge(l, r, on="timestamp", how="outer"), dfs)

    return df_final.sort_values("timestamp").reset_index(drop=True)


def upload_anomaly_to_iotdb(
    conn, experiment_id, timestamps, anomaly_score, anomaly_mask
):
    import requests
    import numpy as np

    ts = [_to_epoch_ms(t) for t in timestamps]

    anomaly_score = np.asarray(anomaly_score).astype(float).tolist()
    anomaly_mask = np.asarray(anomaly_mask).astype(int).tolist()

    device = f"root.experimentID{experiment_id}"

    payload = {
        "device": device,
        "measurements": ["anomaly_score", "anomaly_mask"],
        "data_types": ["DOUBLE", "INT32"],
        "timestamps": ts,
        "values": [anomaly_score, anomaly_mask],
        "is_aligned": True,
    }

    # 🔥 IMPORTANT CHANGE HERE
    response = requests.post(
        f"{conn['base_url']}/insertTablet",  # ✅ FIXED
        json=payload,
        auth=conn["auth"],
        headers={"Content-Type": "application/json"},
        timeout=30,
    )

    response.raise_for_status()

    data = response.json()

    if "code" in data and data["code"] != 200:
        raise RuntimeError(f"IoTDB write failed: {data}")

    print(f"✅ Uploaded anomaly data → {device}")


def get_latest_timestamp(conn, experiment_id):
    """
    Get latest timestamp for given experiment from IoTDB
    """

    device = f"root.experimentID{experiment_id}"

    sql = f"SELECT LAST anomaly_score FROM {device}"

    result = _query(sql, conn)
    df = _parse_iotdb_result(result)

    if df.empty:
        return None

    # timestamp already returned
    ts = df["timestamp"].iloc[0]

    return pd.to_datetime(ts, unit="ms")
