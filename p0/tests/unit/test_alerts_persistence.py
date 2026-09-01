from __future__ import annotations

import pytest

from p0.pipelines import run_alerts_end_to_end as alerts_pipeline


class FakeCursor:
    def __init__(self):
        self.executed = []
        self.rowcount = 0

    def execute(
        self,
        sql,
        params=None,
    ):
        self.executed.append(
            (
                " ".join(sql.split()),
                params,
            )
        )

        if sql.strip().upper().startswith(
            "DELETE"
        ):
            self.rowcount = 0


class FakeConnection:
    def __init__(self):
        self.cursor_instance = FakeCursor()
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def cursor(self):
        return self.cursor_instance

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


def _rows(
    batch_id: str,
) -> list[dict]:
    """
    Same two logical alerts under a caller-selected upload batch.

    source_record_id stays unchanged between batches.
    """

    return [
        {
            "plant_code_id": "PLANT_A",
            "source_record_id": "alerts:record-001",
            "upload_batch_id": batch_id,
            "alert_status": "open",
            "title": "Alert one",
        },
        {
            "plant_code_id": "PLANT_A",
            "source_record_id": "alerts:record-002",
            "upload_batch_id": batch_id,
            "alert_status": "closed",
            "title": "Alert two",
        },
    ]


def test_alerts_delete_uses_source_record_id_not_batch(
    monkeypatch,
):
    """
    The replacement DELETE must target logical record identity,
    not upload_batch_id.
    """

    conn = FakeConnection()

    monkeypatch.setattr(
        "p0.api.database.connection._get_conn",
        lambda plant_code_id: conn,
    )

    monkeypatch.setattr(
        alerts_pipeline,
        "_write_to_db",
        lambda *args, **kwargs: 2,
    )

    rows = _rows(
        "batch-b"
    )

    written = alerts_pipeline._persist_alerts_rows(
        plant_code_id="PLANT_A",
        db_rows=rows,
    )

    assert written == 2

    assert len(
        conn.cursor_instance.executed
    ) == 1

    sql, params = (
        conn.cursor_instance.executed[0]
    )

    assert "DELETE FROM alerts" in sql
    assert "source_record_id = ANY" in sql

    assert "upload_batch_id" not in sql

    assert params[0] == "PLANT_A"

    assert params[1] == [
        "alerts:record-001",
        "alerts:record-002",
    ]

    assert conn.committed is True
    assert conn.rolled_back is False
    assert conn.closed is True


def test_same_logical_rows_in_new_batch_keep_same_identity():
    """
    This is the core cross-batch regression.

    The batch changes, but the logical source IDs do not.
    """

    first = _rows(
        "batch-a"
    )

    second = _rows(
        "batch-b"
    )

    assert (
        first[0]["upload_batch_id"]
        != second[0]["upload_batch_id"]
    )

    assert [
        row["source_record_id"]
        for row in first
    ] == [
        row["source_record_id"]
        for row in second
    ]


def test_successful_write_commits(
    monkeypatch,
):
    conn = FakeConnection()

    monkeypatch.setattr(
        "p0.api.database.connection._get_conn",
        lambda plant_code_id: conn,
    )

    monkeypatch.setattr(
        alerts_pipeline,
        "_write_to_db",
        lambda *args, **kwargs: 2,
    )

    result = (
        alerts_pipeline._persist_alerts_rows(
            plant_code_id="PLANT_A",
            db_rows=_rows("batch-a"),
        )
    )

    assert result == 2
    assert conn.committed is True
    assert conn.rolled_back is False
    assert conn.closed is True


def test_partial_write_rolls_back(
    monkeypatch,
):
    """
    If fewer rows land than were supplied, nothing is committed.
    """

    conn = FakeConnection()

    monkeypatch.setattr(
        "p0.api.database.connection._get_conn",
        lambda plant_code_id: conn,
    )

    monkeypatch.setattr(
        alerts_pipeline,
        "_write_to_db",
        lambda *args, **kwargs: 1,
    )

    with pytest.raises(
        RuntimeError,
        match="database write incomplete",
    ):
        alerts_pipeline._persist_alerts_rows(
            plant_code_id="PLANT_A",
            db_rows=_rows("batch-a"),
        )

    assert conn.committed is False
    assert conn.rolled_back is True
    assert conn.closed is True


def test_database_write_exception_rolls_back(
    monkeypatch,
):
    conn = FakeConnection()

    monkeypatch.setattr(
        "p0.api.database.connection._get_conn",
        lambda plant_code_id: conn,
    )

    def fail_write(
        *args,
        **kwargs,
    ):
        raise RuntimeError(
            "simulated insert failure"
        )

    monkeypatch.setattr(
        alerts_pipeline,
        "_write_to_db",
        fail_write,
    )

    with pytest.raises(
        RuntimeError,
        match="simulated insert failure",
    ):
        alerts_pipeline._persist_alerts_rows(
            plant_code_id="PLANT_A",
            db_rows=_rows("batch-a"),
        )

    assert conn.committed is False
    assert conn.rolled_back is True
    assert conn.closed is True


def test_missing_source_record_id_is_rejected(
    monkeypatch,
):
    """
    No DB connection should even be opened when logical identity
    is incomplete.
    """

    called = False

    def fake_get_conn(
        plant_code_id,
    ):
        nonlocal called
        called = True
        return FakeConnection()

    monkeypatch.setattr(
        "p0.api.database.connection._get_conn",
        fake_get_conn,
    )

    rows = _rows(
        "batch-a"
    )

    rows[0][
        "source_record_id"
    ] = None

    with pytest.raises(
        RuntimeError,
        match="no source_record_id",
    ):
        alerts_pipeline._persist_alerts_rows(
            plant_code_id="PLANT_A",
            db_rows=rows,
        )

    assert called is False


def test_duplicate_source_record_ids_inside_upload_are_rejected(
    monkeypatch,
):
    called = False

    def fake_get_conn(
        plant_code_id,
    ):
        nonlocal called
        called = True
        return FakeConnection()

    monkeypatch.setattr(
        "p0.api.database.connection._get_conn",
        fake_get_conn,
    )

    rows = _rows(
        "batch-a"
    )

    rows[1][
        "source_record_id"
    ] = rows[0][
        "source_record_id"
    ]

    with pytest.raises(
        RuntimeError,
        match="duplicate source_record_id",
    ):
        alerts_pipeline._persist_alerts_rows(
            plant_code_id="PLANT_A",
            db_rows=rows,
        )

    assert called is False


def test_empty_alert_set_is_safe():
    result = (
        alerts_pipeline._persist_alerts_rows(
            plant_code_id="PLANT_A",
            db_rows=[],
        )
    )

    assert result == 0