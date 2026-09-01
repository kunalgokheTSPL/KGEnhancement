"""
Per-plant database provisioning and the central plant registry — the real thing.

Creating a plant writes its row in the central ``plants`` registry — which lives
in its own ``<base>_registry`` database and records where that plant's data
lives — and materialises the plant's own database with the full CDM schema (all
schema.yaml tables + the plant FKs + the flow_state index + the local plants row
the FKs point at). p1/p2 add their own tables to the same database. There is no
shared database for a registered plant's data.

These tests hit the real cluster (create + drop actual databases, write registry
rows), so they carry ``@pytest.mark.real_provisioning`` to opt out of the
in-memory fake, and always tear down what they create.
"""

from __future__ import annotations

import psycopg2
import pytest

from p0.tests._p0_test_base import app, assert_test_plant

from p0.api import plants as pl
from p0.api.database.connection import cdm_db_name, open_db

_LEGACY_CDM_DB = cdm_db_name()

pytestmark = pytest.mark.real_provisioning

_TEST_PLANT = "provisioning_testcase"
_ROUTING_PLANT = "routing_testcase"


def _db_exists(db_name: str) -> bool:
    m = pl._maintenance_conn()
    try:
        cur = m.cursor()
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
        return cur.fetchone() is not None
    finally:
        m.close()


def _raise_provisioning_failure(*_args, **_kwargs):
    raise RuntimeError("provisioning blew up")


def _forget_plant(plant: str) -> None:
    """Drop the plant's database and its registry row, in that order."""
    assert_test_plant(plant)
    pl._drop_plant_databases(plant)
    pl._unregister_plant(plant)


@pytest.fixture
def _clean_plant():
    """Guarantee the test plant's database and registry row are gone before and
    after."""
    _forget_plant(_TEST_PLANT)
    yield _TEST_PLANT
    _forget_plant(_TEST_PLANT)




def test_normalises_plant_code_to_db_suffix():
    assert pl._normalize_plant_suffix("Plant 1") == "plant_1"
    assert pl._normalize_plant_suffix("ACME-2 Cement!") == "acme_2_cement"


def test_rejects_empty_and_overlong_suffix():
    with pytest.raises(Exception):
        pl._normalize_plant_suffix("!!!")
    with pytest.raises(Exception):
        pl._normalize_plant_suffix("x" * (pl._MAX_PLANT_SUFFIX + 1))


def test_db_name_is_single_decisionops_database():
    assert pl.plant_db_name("Plant 1") == "decisionops_plant_1"


def test_registry_bootstraps_on_first_use_when_blank(monkeypatch):
    """Brand-new server: the registry DB does not exist yet. The first registry
    access must create it (DB + plants table) so nothing downstream is blocked."""
    from p0.api.database import connection as dbmod

    test_db = "plant_registry_testcase"
    assert "test" in test_db  # guard: only ever drop a throwaway registry
    monkeypatch.setattr(pl, "_registry_db_name", lambda: test_db)
    monkeypatch.setattr(dbmod, "registry_db_name", lambda: test_db)

    def _drop_registry():
        m = pl._maintenance_conn()
        try:
            m.cursor().execute(f'DROP DATABASE IF EXISTS "{test_db}" WITH (FORCE)')
        finally:
            m.close()

    _drop_registry()
    try:
        assert not _db_exists(test_db), "precondition: registry DB absent"
        conn = dbmod._get_conn(registry=True)  # first use → self-bootstrap
        try:
            cur = conn.cursor()
            cur.execute("SELECT current_database()")
            assert cur.fetchone()[0] == test_db
            cur.execute("SELECT count(*) FROM plants")  # table created too
            assert cur.fetchone()[0] == 0
        finally:
            conn.close()
        assert _db_exists(test_db), "registry DB must exist after first use"
    finally:
        _drop_registry()




def test_provision_creates_single_db_with_full_cdm_schema(_clean_plant):
    plant = _clean_plant
    db_name = pl.plant_db_name(plant)
    assert not _db_exists(db_name), "precondition: no database yet"

    pl._provision_plant_databases(plant)

    assert _db_exists(db_name), "the plant's dedicated database must exist"

    conn = open_db(db_name, connect_timeout=10)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema='public'"
        )
        assert cur.fetchone()[0] == len(pl._cdm_schema_tables())
        cur.execute("SELECT count(*) FROM pg_indexes WHERE indexname='uq_flow_file'")
        assert cur.fetchone()[0] == 1
        cur.execute(
            "SELECT count(*) FROM pg_constraint WHERE contype='f' AND conname LIKE 'fk_%_plant'"
        )
        assert cur.fetchone()[0] > 0, "plant FKs must be applied"
        # every schema.yaml index + unique/constraint is materialised at provisioning
        cur.execute("SELECT count(*) FROM pg_indexes WHERE indexname LIKE 'idx_%'")
        assert cur.fetchone()[0] >= 70, "all schema.yaml indexes must be created"
        for uq in ("uq_equip_plant_asset", "uq_wo_plant_number", "uq_ts_plant_tag"):
            cur.execute("SELECT count(*) FROM pg_indexes WHERE indexname = %s", (uq,))
            assert cur.fetchone()[0] == 1, f"{uq} must be provisioned"
        cur.execute("SELECT count(*) FROM plants WHERE plant_code_id = %s", (plant,))
        assert cur.fetchone()[0] == 1, "per-plant plants table must hold the plant row"
        cur.execute(
            "INSERT INTO equipment (plant_code_id, equipment_id) VALUES (%s, %s)",
            (plant, "PROV-FK-1"),
        )
        conn.commit()
    finally:
        conn.close()


def test_provision_is_idempotent(_clean_plant):
    plant = _clean_plant
    pl._provision_plant_databases(plant)
    pl._provision_plant_databases(plant)
    assert _db_exists(pl.plant_db_name(plant))


def test_drop_removes_the_database(_clean_plant):
    plant = _clean_plant
    pl._provision_plant_databases(plant)
    res = pl._drop_plant_databases(plant)
    assert res["dropped"] == [pl.plant_db_name(plant)]
    assert not _db_exists(pl.plant_db_name(plant))


def test_never_targets_protected_databases():
    with pytest.raises(Exception):
        pl._provision_plant_databases("cdm")




def test_registry_lives_in_its_own_database():
    """The registry has its own database, separate from the shared CDM one, so the
    CDM database can be retired without taking the plant registry with it."""
    from p0.api.database.connection import registry_db_name

    assert registry_db_name() != _LEGACY_CDM_DB
    assert _db_exists(registry_db_name()), "registry database must be provisioned"

    conn = pl._get_conn(registry=True)
    try:
        cur = conn.cursor()
        cur.execute("SELECT current_database()")
        assert cur.fetchone()[0] == registry_db_name()
        cur.execute("SELECT to_regclass('public.plants')")
        assert cur.fetchone()[0] is not None, "plants table must be in the registry DB"
    finally:
        conn.close()


def test_add_plant_registers_the_plant_with_its_database(_clean_plant):
    plant = _clean_plant
    assert not pl.plant_is_registered(plant), "precondition: not registered"

    created = pl.add_plant(plant, "Prov Test", "cement", "tester")

    assert created["db_name"] == pl.plant_db_name(plant)
    assert pl.registered_plant_db_name(plant) == pl.plant_db_name(plant)

    row = next(p for p in pl.list_plants() if p["plant_code_id"] == plant)
    assert row["db_name"] == pl.plant_db_name(plant)
    assert row["label"] == "Prov Test"
    assert row["industry"] == "cement"
    assert plant in pl.get_plant_codes()


def test_add_plant_rejects_an_already_registered_plant(_clean_plant):
    plant = _clean_plant
    pl.add_plant(plant, None, None, "tester")
    with pytest.raises(Exception):
        pl.add_plant(plant, None, None, "tester")


def test_delete_plant_deregisters_and_drops_the_database(_clean_plant):
    plant = _clean_plant
    pl.add_plant(plant, None, None, "tester")

    pl.delete_plant(plant)

    assert not pl.plant_is_registered(plant)
    assert pl.registered_plant_db_name(plant) is None
    assert not _db_exists(pl.plant_db_name(plant))
    assert plant not in pl.get_plant_codes()


def test_failed_provisioning_rolls_the_registry_row_back(_clean_plant, monkeypatch):
    plant = _clean_plant
    monkeypatch.setattr(
        pl, "_provision_plant_databases", _raise_provisioning_failure
    )

    with pytest.raises(Exception):
        pl.add_plant(plant, None, None, "tester")

    assert not pl.plant_is_registered(plant), "a plant must never outlive its database"


def test_refuses_to_destroy_a_plant_pointing_at_a_reserved_database(_clean_plant):
    """A legacy row may still point at the shared CDM database. Dropping that to
    delete one plant would take everything with it, so delete/purge must refuse."""
    plant = _clean_plant
    pl._register_plant(plant, None, None, "tester", _LEGACY_CDM_DB)

    with pytest.raises(Exception):
        pl.delete_plant(plant)
    with pytest.raises(Exception):
        pl.purge_plant(plant)

    assert pl.plant_is_registered(plant), "the plant must survive the refusal"
    assert _db_exists(_LEGACY_CDM_DB), "the shared database must survive it too"


def test_registry_adopts_a_pre_registry_plant_database(_clean_plant):
    """A database provisioned before the registry existed only self-registers
    locally. The boot sync must adopt it, or it stops resolving."""
    plant = _clean_plant
    pl._provision_plant_databases(plant, "Legacy", "cement", "old")
    assert not pl.plant_is_registered(plant), "precondition: no registry row"

    pl.sync_registry_from_plant_databases()

    assert pl.registered_plant_db_name(plant) == pl.plant_db_name(plant)




def test_data_write_routes_to_the_per_plant_database(app):
    """A plant-scoped write made through the API must land in the plant's own
    database, not the shared/main one — the whole point of per-plant isolation.
    Uses the real app so the plant-context dependency + connection routing run
    for real."""
    import base64

    from fastapi.testclient import TestClient
    from p0.Login.auth.auth_deps import get_current_user, TokenData

    plant = _ROUTING_PLANT
    content_hash = "r" * 64

    def _count_flow(dbname: str) -> int:
        conn = open_db(dbname, connect_timeout=10)
        try:
            cur = conn.cursor()
            try:
                cur.execute(
                    "SELECT count(*) FROM flow_state WHERE content_hash = %s",
                    (content_hash,),
                )
                return cur.fetchone()[0]
            except psycopg2.errors.UndefinedTable:
                return 0
        finally:
            conn.close()

    app.dependency_overrides[get_current_user] = lambda: TokenData(
        sub="a", email="e", username="t", roles=["admin"]
    )
    tok = base64.urlsafe_b64encode(b"{}").decode().rstrip("=")

    _forget_plant(plant)
    pl.add_plant(plant, "route-e2e", None, "tester")

    try:
        client = TestClient(app, cookies={"access_token": f"{tok}.{tok}.s"})
        r = client.post(
            "/p0/cdm/flow/upsert",
            params={"plant_code_id": plant},
            json={
                "plant_code_id": plant,
                "flow": "ts",
                "content_hash": content_hash,
                "file_name": "x.csv",
                "stage": "staged",
            },
        )
        assert r.status_code == 200, r.text
        assert _count_flow(pl.plant_db_name(plant)) == 1, "row must be in per-plant DB"
        assert _count_flow(_LEGACY_CDM_DB) == 0, "row must NOT be in the legacy CDM DB"
    finally:
        app.dependency_overrides.pop(get_current_user, None)
        pl.purge_plant(plant)


def test_two_plants_do_not_see_each_others_data(app):
    """Cross-tenant isolation: a row written under plant A must be invisible to plant
    B's database and vice versa — the guarantee per-plant databases exist to provide.
    Guards Phase 5: if config/data ever leaks across plants, this fails."""
    import base64

    from fastapi.testclient import TestClient
    from p0.Login.auth.auth_deps import get_current_user, TokenData

    plant_a, plant_b = "isolation_a_testcase", "isolation_b_testcase"
    hash_a, hash_b = "a" * 64, "b" * 64

    def _count_flow(dbname: str, content_hash: str) -> int:
        conn = open_db(dbname, connect_timeout=10)
        try:
            cur = conn.cursor()
            try:
                cur.execute(
                    "SELECT count(*) FROM flow_state WHERE content_hash = %s",
                    (content_hash,),
                )
                return cur.fetchone()[0]
            except psycopg2.errors.UndefinedTable:
                return 0
        finally:
            conn.close()

    app.dependency_overrides[get_current_user] = lambda: TokenData(
        sub="a", email="e", username="t", roles=["admin"]
    )
    tok = base64.urlsafe_b64encode(b"{}").decode().rstrip("=")

    for p in (plant_a, plant_b):
        _forget_plant(p)
        pl.add_plant(p, "isolation", None, "tester")

    try:
        client = TestClient(app, cookies={"access_token": f"{tok}.{tok}.s"})
        for plant, chash in ((plant_a, hash_a), (plant_b, hash_b)):
            r = client.post(
                "/p0/cdm/flow/upsert",
                params={"plant_code_id": plant},
                json={
                    "plant_code_id": plant,
                    "flow": "ts",
                    "content_hash": chash,
                    "file_name": "x.csv",
                    "stage": "staged",
                },
            )
            assert r.status_code == 200, r.text

        db_a, db_b = pl.plant_db_name(plant_a), pl.plant_db_name(plant_b)
        assert _count_flow(db_a, hash_a) == 1
        assert _count_flow(db_b, hash_a) == 0, "plant A's row leaked into plant B's DB"
        assert _count_flow(db_b, hash_b) == 1
        assert _count_flow(db_a, hash_b) == 0, "plant B's row leaked into plant A's DB"
    finally:
        app.dependency_overrides.pop(get_current_user, None)
        for p in (plant_a, plant_b):
            pl.purge_plant(p)
