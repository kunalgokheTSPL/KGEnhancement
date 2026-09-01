"""Azure SQL timeseries backend: table naming, SQL generation, and op flow (no DB)."""

from __future__ import annotations

import pandas as pd

import importlib.util

import pytest

from p0.api.database import timeseries_sql as ts

def _pyodbc_usable() -> bool:
    """True only when pyodbc imports, not merely when it is installed."""
    try:
        importlib.import_module("pyodbc")
    except Exception:
        return False
    return True


_needs_azure_driver = pytest.mark.skipif(
    not _pyodbc_usable(),
    reason="azure driver stack (utility.drivers.azure_*) requires a working pyodbc",
)


# ---- backend selection (each driver is a mode constant; the facade picks) ----

def test_onprem_driver_timeseries_backend_is_iotdb():
    from p0.drivers.database_driver import timeseries_backend
    assert timeseries_backend() == "iotdb"


@_needs_azure_driver
def test_azure_driver_timeseries_backend_is_azuresql():
    from p0.drivers.azure_driver import timeseries_backend
    assert timeseries_backend() == "azuresql"


# ---- table naming ---------------------------------------------------------

def test_table_name_is_valid_and_deterministic():
    dev = "root.decisionops.sensors.plant_1.pump_a"
    t = ts.table_name(dev)
    assert t == ts.table_name(dev)
    assert t.startswith("ts_")
    assert all(c.isalnum() or c == "_" for c in t)


def test_table_prefix_is_a_prefix_of_device_tables():
    prefix = "root.decisionops.sensors.plant_1"
    dev = prefix + ".pump_a"
    assert ts.table_name(dev).startswith(ts.table_prefix(prefix))


def test_table_name_capped_at_128_chars():
    dev = "root.x." + "y" * 300
    t = ts.table_name(dev)
    assert len(t) <= 128


def test_plants_do_not_collide_by_prefix():
    p1 = ts.table_prefix("root.r.plant_1")
    p10 = ts.table_prefix("root.r.plant_10")
    # plant_1's LIKE pattern must not swallow plant_10's tables
    assert not ts.table_name("root.r.plant_10.d").startswith(p1 + "_")


# ---- SQL generation -------------------------------------------------------

def test_create_table_sql_is_idempotent_ddl():
    s = ts.create_table_sql("ts_x")
    assert "IF OBJECT_ID" in s and "ts BIGINT NOT NULL PRIMARY KEY" in s


def test_add_column_sql_guards_existing_column():
    s = ts.add_column_sql("ts_x", "flow")
    assert "COL_LENGTH" in s and "ADD [flow] FLOAT NULL" in s


def test_insert_sql_matches_column_count():
    s = ts.insert_sql("ts_x", ["ts", "a", "b"])
    assert s.count("?") == 3 and "[ts],[a],[b]" in s


def test_like_escape_neutralizes_wildcards():
    assert ts._like_escape("a_b%c") == "a\\_b\\%c"


def test_epoch_ms_matches_known_timestamp():
    s = pd.to_datetime(pd.Series(["1970-01-01T00:00:01Z"]))
    assert ts.epoch_ms(s) == [1000]


def test_clean_maps_bad_values_to_none():
    assert ts._clean(float("nan")) is None
    assert ts._clean(None) is None
    assert ts._clean("x") is None
    assert ts._clean(3) == 3.0


# ---- op flow against a fake connection ------------------------------------

class _FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.fast_executemany = False
        self.rowcount = conn.rowcount
        self._one = None
        self._all = []

    def execute(self, sql, *params):
        self.conn.calls.append(("execute", sql, params))
        if sql.startswith("SELECT MAX(ts)"):
            self._one = (self.conn.max_ts,)
        elif sql.startswith("SELECT name FROM sys.tables"):
            self._all = [(n,) for n in self.conn.table_names]

    def executemany(self, sql, rows):
        self.conn.calls.append(("executemany", sql, list(rows)))

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._all

    def close(self):
        pass


class _FakeConn:
    def __init__(self, max_ts=None, table_names=(), rowcount=0):
        self.calls = []
        self.max_ts = max_ts
        self.table_names = list(table_names)
        self.rowcount = rowcount
        self.commits = 0

    def cursor(self):
        return _FakeCursor(self)

    def commit(self):
        self.commits += 1


def _frame():
    return pd.DataFrame(
        {
            "Timestamp": pd.to_datetime(["1970-01-01T00:00:01Z", "1970-01-01T00:00:02Z"]),
            "flow": [1.0, 2.0],
            "temp": [10.0, 20.0],
        }
    )


def test_insert_frame_creates_table_upserts_and_commits():
    conn = _FakeConn()
    df = _frame()
    n = ts.insert_frame(conn, df, "Timestamp", ["flow", "temp"], ["flow", "temp"],
                        "root.r.plant_1.dev")
    assert n == 2
    assert conn.commits == 1
    kinds = [c[0] for c in conn.calls]
    sqls = [c[1] for c in conn.calls]
    # table + column DDL ran, a delete preceded the insert (upsert), one executemany
    assert any("CREATE TABLE" in s for s in sqls)
    assert sum("ADD [" in s for s in sqls) == 2
    assert any(s.startswith("DELETE FROM") and "WHERE ts IN" in s for s in sqls)
    assert kinds.count("executemany") == 1


def test_insert_frame_empty_is_noop():
    conn = _FakeConn()
    empty = _frame().iloc[0:0]
    assert ts.insert_frame(conn, empty, "Timestamp", ["flow"], ["flow"], "root.r.p.d") == 0
    assert conn.calls == []


def test_list_devices_uses_escaped_like():
    conn = _FakeConn(table_names=["ts_root_r_plant_1_a", "ts_root_r_plant_1_b"])
    out = ts.list_devices(conn, "root.r.plant_1")
    assert out == ["ts_root_r_plant_1_a", "ts_root_r_plant_1_b"]
    like_call = [c for c in conn.calls if "sys.tables" in c[1]][0]
    assert like_call[2][0].endswith("%") and "\\_" in like_call[2][0]


def test_device_t_max_reads_max_or_none():
    assert ts.device_t_max(_FakeConn(max_ts=1700), "ts_x") == 1700
    assert ts.device_t_max(_FakeConn(max_ts=None), "ts_x") is None


def test_delete_before_returns_rowcount_and_commits():
    conn = _FakeConn(rowcount=5)
    assert ts.delete_before(conn, "ts_x", 1000) == 5
    assert conn.commits == 1


def test_drop_table_sql_guards_existence():
    s = ts.drop_table_sql("ts_x")
    assert "IF OBJECT_ID" in s and "DROP TABLE [ts_x]" in s


def test_drop_device_executes_and_commits():
    conn = _FakeConn()
    ts.drop_device(conn, "ts_x")
    assert conn.commits == 1
    assert any(c[1].startswith("IF OBJECT_ID") and "DROP TABLE" in c[1] for c in conn.calls)


