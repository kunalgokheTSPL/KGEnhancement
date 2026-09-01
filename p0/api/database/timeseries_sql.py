"""Azure SQL timeseries backend: table-per-device store mirroring the IoTDB layer.

One SQL table per device (columns = measurements, ts = BIGINT epoch-ms primary key),
in a single Azure SQL database with the plant encoded in the table name. Pure SQL/name
helpers are import-safe; the ops lazy-import pyodbc via a passed-in connection.
"""

from __future__ import annotations

import hashlib
import math
import re

_TS_BATCH = 1000


def _sanitize(s: str) -> str:
    """Lowercase and reduce to a safe SQL identifier fragment ([a-z0-9_])."""
    return re.sub(r"[^a-z0-9]+", "_", str(s).lower()).strip("_")


def table_name(device: str) -> str:
    """Map an IoTDB-style device path to its Azure SQL table name (<=128 chars)."""
    name = "ts_" + _sanitize(device)
    if len(name) > 128:
        h = hashlib.sha1(device.encode()).hexdigest()[:8]
        name = name[:119] + "_" + h
    return name


def table_prefix(device_prefix: str) -> str:
    """Table-name prefix shared by every device under a plant (for LIKE listing)."""
    return "ts_" + _sanitize(device_prefix)


def _like_escape(s: str) -> str:
    """Escape LIKE metacharacters so '_' and '%' match literally (ESCAPE '\\')."""
    return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def create_table_sql(table: str) -> str:
    """DDL that creates the device table (ts primary key) only if it is absent."""
    return (
        f"IF OBJECT_ID(N'[{table}]', N'U') IS NULL "
        f"EXEC('CREATE TABLE [{table}] (ts BIGINT NOT NULL PRIMARY KEY)')"
    )


def add_column_sql(table: str, col: str) -> str:
    """DDL that adds one measurement column only if it is not already present."""
    return (
        f"IF COL_LENGTH(N'[{table}]', N'{col}') IS NULL "
        f"EXEC('ALTER TABLE [{table}] ADD [{col}] FLOAT NULL')"
    )


def insert_sql(table: str, cols: list[str]) -> str:
    """Parameterized INSERT for the given column order."""
    collist = ",".join(f"[{c}]" for c in cols)
    placeholders = ",".join("?" for _ in cols)
    return f"INSERT INTO [{table}] ({collist}) VALUES ({placeholders})"


def delete_ts_sql(table: str, n: int) -> str:
    """DELETE the given number of timestamps (upsert step before re-insert)."""
    marks = ",".join("?" for _ in range(n))
    return f"DELETE FROM [{table}] WHERE ts IN ({marks})"


def max_ts_sql(table: str) -> str:
    """Latest timestamp in a device table."""
    return f"SELECT MAX(ts) FROM [{table}]"


def delete_before_sql(table: str) -> str:
    """Retention prune: drop rows older than a cutoff (parameter)."""
    return f"DELETE FROM [{table}] WHERE ts < ?"


def drop_table_sql(table: str) -> str:
    """DROP one device table if it exists (plant purge)."""
    return f"IF OBJECT_ID(N'[{table}]', N'U') IS NOT NULL DROP TABLE [{table}]"


def epoch_ms(series) -> list[int]:
    """Convert a parsed datetime Series to a list of int64 epoch milliseconds."""
    s = series
    if getattr(getattr(s, "dt", None), "tz", None) is not None:
        s = s.dt.tz_convert("UTC").dt.tz_localize(None)
    ms = s.astype("datetime64[ns]").astype("int64") // 1_000_000
    return [int(x) for x in ms.tolist()]


def _clean(v):
    """Coerce a value to float, mapping NaN/inf/None/non-numeric to SQL NULL."""
    try:
        if v is None or math.isnan(v) or math.isinf(v):
            return None
    except (TypeError, ValueError):
        pass
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def ensure_table(cur, table: str, cols: list[str]) -> None:
    """Create the device table and add any missing measurement columns."""
    cur.execute(create_table_sql(table))
    for c in cols:
        cur.execute(add_column_sql(table, c))


def insert_frame(conn, df, time_col, sensor_cols, norm_cols, device, batch: int = _TS_BATCH) -> int:
    """Upsert one time-parsed frame into the device's table; returns rows written."""
    if len(df) == 0:
        return 0
    table = table_name(device)
    cur = conn.cursor()
    ensure_table(cur, table, list(norm_cols))
    try:
        cur.fast_executemany = True
    except Exception:
        pass
    cols = ["ts"] + list(norm_cols)
    ins = insert_sql(table, cols)
    ts_all = epoch_ms(df[time_col])
    col_values = [[_clean(v) for v in df[c].tolist()] for c in sensor_cols]
    n = len(ts_all)
    total = 0
    for start in range(0, n, batch):
        end = min(start + batch, n)
        ts_chunk = ts_all[start:end]
        rows = [
            [ts_chunk[i - start]] + [col_values[j][i] for j in range(len(sensor_cols))]
            for i in range(start, end)
        ]
        cur.execute(delete_ts_sql(table, len(ts_chunk)), ts_chunk)
        cur.executemany(ins, rows)
        total += len(rows)
    conn.commit()
    cur.close()
    return total


def list_devices(conn, device_prefix: str) -> list[str]:
    """Return the device tables belonging to one plant (table names)."""
    like = _like_escape(table_prefix(device_prefix)) + "%"
    cur = conn.cursor()
    cur.execute("SELECT name FROM sys.tables WHERE name LIKE ? ESCAPE '\\'", like)
    names = [r[0] for r in cur.fetchall()]
    cur.close()
    return names


def device_t_max(conn, table: str) -> int | None:
    """Latest timestamp (ms) for one device table, or None if empty."""
    cur = conn.cursor()
    cur.execute(max_ts_sql(table))
    row = cur.fetchone()
    cur.close()
    if not row or row[0] is None:
        return None
    return int(row[0])


def delete_before(conn, table: str, cutoff: int) -> int:
    """Prune rows older than cutoff from one device table; returns rows removed."""
    cur = conn.cursor()
    cur.execute(delete_before_sql(table), int(cutoff))
    n = cur.rowcount
    conn.commit()
    cur.close()
    return n if (n is not None and n >= 0) else 0


def drop_device(conn, table: str) -> None:
    """Drop one device table (plant purge)."""
    cur = conn.cursor()
    cur.execute(drop_table_sql(table))
    conn.commit()
    cur.close()
