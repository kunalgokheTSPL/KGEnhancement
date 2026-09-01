"""Schema changes must reach plants that already exist, not just new ones."""

from __future__ import annotations

import pathlib

import yaml

import p0.api.plants as plants
from p0.api.services.flow import UNCATEGORISED, _canonical_category

SCHEMA = (
    pathlib.Path(__file__).resolve().parents[2]
    / "config"
    / "templates"
    / "_common"
    / "schema.yaml"
)
PLANTS_SRC = (pathlib.Path(__file__).resolve().parents[2] / "api" / "plants.py").read_text()
MAIN = (pathlib.Path(__file__).resolve().parents[2] / "api" / "main.py").read_text()


def _tables():
    doc = yaml.safe_load(SCHEMA.read_text())
    return doc.get("tables") or doc.get("schema") or {}


class _FakeCursor:
    def __init__(self, present):
        self.present = set(present)
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append(sql)
        if "information_schema.columns" in sql:
            self._rows = [(c,) for c in self.present]
        else:
            self._rows = []

    def fetchall(self):
        return self._rows


class _FakeConn:
    def __init__(self):
        self.commits = 0
        self.rollbacks = 0

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def test_only_missing_columns_are_added():
    cur = _FakeCursor(present=["a", "b"])
    conn = _FakeConn()
    added = plants._add_missing_columns(
        cur, conn, "flow_state", {"a": "TEXT", "b": "TEXT", "c": "INTEGER"}
    )
    assert added == 1
    alters = [s for s in cur.executed if s.startswith("ALTER TABLE")]
    assert len(alters) == 1
    assert '"c" INTEGER' in alters[0]


def test_nothing_is_altered_when_the_table_matches():
    cur = _FakeCursor(present=["a", "b"])
    conn = _FakeConn()
    assert plants._add_missing_columns(cur, conn, "t", {"a": "TEXT", "b": "TEXT"}) == 0
    assert not [s for s in cur.executed if s.startswith("ALTER TABLE")]


def test_serial_columns_are_never_altered_in():
    cur = _FakeCursor(present=["plant_code_id"])
    conn = _FakeConn()
    plants._add_missing_columns(
        cur, conn, "t", {"flow_uid": "BIGSERIAL", "plant_code_id": "TEXT"}
    )
    assert not [s for s in cur.executed if "BIGSERIAL" in s]


def test_a_missing_table_is_skipped_not_created_column_by_column():
    cur = _FakeCursor(present=[])
    conn = _FakeConn()
    assert plants._add_missing_columns(cur, conn, "absent", {"a": "TEXT"}) == 0


def test_flow_state_declares_the_new_taxonomy_and_metadata_columns():
    columns = _tables()["flow_state"]["columns"]
    for expected in (
        "category",
        "content_type",
        "page_count",
        "has_text_layer",
        "sheet_names",
        "duplicate_of",
        "uploaded_by",
    ):
        assert expected in columns


def test_the_renamed_unique_index_supersedes_every_older_one():
    """CREATE UNIQUE INDEX IF NOT EXISTS matches by NAME — a reused name never rebuilds."""
    flow_state = _tables()["flow_state"]
    assert flow_state["superseded_indexes"] == [
        "uq_flow_file", "uq_flow_file_cat", "uq_flow_file_named"
    ]
    unique = flow_state["unique"][0]
    assert unique["name"] == "uq_flow_file_site"
    assert unique["columns"] == [
        "plant_code_id", "site", "flow", "category", "content_hash", "file_name"
    ]


def test_each_generation_of_the_key_is_weaker_than_the_last():
    """A strictly weaker key cannot collide on data the old key already allowed."""
    flow_state = _tables()["flow_state"]
    current = flow_state["unique"][0]["columns"]
    previous = ["plant_code_id", "flow", "category", "content_hash", "file_name"]
    assert set(previous) < set(current), "the migration could fail on existing rows"


def test_the_upsert_conflict_target_matches_the_declared_index():
    """A mismatch here is an immediate runtime error on every upload."""
    from p0.api.services import flow as flow_service

    declared = _tables()["flow_state"]["unique"][0]["columns"]
    assert sorted(flow_service.CONFLICT_KEY) == sorted(declared)


def test_pipeline_jobs_progress_columns_are_declared():
    columns = _tables()["pipeline_jobs"]["columns"]
    for expected in ("progress_percent", "is_determinate", "phases", "upload_batch_id"):
        assert expected in columns


def test_migration_drops_superseded_indexes_before_recreating():
    assert "superseded_indexes" in PLANTS_SRC
    assert "DROP INDEX IF EXISTS" in PLANTS_SRC


def test_migration_runs_at_startup():
    assert "migrate_all_plants" in MAIN


def test_migration_runs_once_per_process():
    assert "_migration_done" in PLANTS_SRC
    plants._migration_done = True
    assert plants.migrate_all_plants() == 0


def test_category_is_never_null_so_the_unique_key_holds():
    """Postgres treats NULLs as distinct — a null category would defeat dedupe."""
    assert _canonical_category(None) == UNCATEGORISED
    assert _canonical_category("") == UNCATEGORISED
    assert _canonical_category("   ") == UNCATEGORISED


def test_category_is_normalised_on_write():
    assert _canonical_category("linelist_files") == "line_list"
    assert _canonical_category("AUFK") == "AUFK"
