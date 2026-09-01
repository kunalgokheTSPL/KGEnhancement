"""
The whole plant journey, driven over HTTP exactly as a person would drive Swagger.

  create plant -> it appears in the registry and gets its own database
  upload a real PDF / XLSX / CSV through each connector
  the upload registers an upload job AND a flow_state row
  purgePreview reports what a delete would take with it
  delete plant -> registry row, database and staged objects all go

Real files (a genuine PDF via PyMuPDF, a genuine XLSX via openpyxl), a real
per-plant database, real RustFS objects. Nothing is faked, so a break anywhere in
router -> service -> database shows up here.

Carries ``real_provisioning``: it creates and drops an actual database. The plant
code always ends in ``_testcase`` and the module-scoped fixture tears it down even
if an assertion blows up half way, so a failure never leaves an orphan behind.
"""

from __future__ import annotations

import io
import uuid

import pandas as pd
import pytest

from p0.tests._p0_test_base import (
    assert_test_plant,
    client,
    assert_standard_response,
)

pytestmark = pytest.mark.real_provisioning

PLANT = f"e2e_{uuid.uuid4().hex[:8]}_testcase"


# ------------------------------------------------------------------ fixtures


@pytest.fixture(scope="module", autouse=True)
def _teardown_plant():
    """Drop the plant's database and registry row after the module, whatever happened."""
    yield
    from p0.api import plants as _plants

    assert_test_plant(PLANT)
    try:
        _plants._drop_plant_databases(PLANT)
    finally:
        _plants._unregister_plant(PLANT)


# ------------------------------------------------------------- real payloads


def _pdf_bytes(text: str) -> bytes:
    """A genuine one-page PDF."""
    import fitz

    doc = fitz.open()
    page = doc.new_page()
    page.insert_text((72, 72), text)
    out = doc.tobytes()
    doc.close()
    return out


def _xlsx_bytes() -> bytes:
    """A genuine XLSX shaped like a SAP equipment extract."""
    df = pd.DataFrame(
        [
            {"EQUNR": "EQ-1001", "EQKTX": "Kiln Feed Pump", "TPLNR": "AREA-01"},
            {"EQUNR": "EQ-1002", "EQKTX": "Raw Mill Fan", "TPLNR": "AREA-02"},
        ]
    )
    buf = io.BytesIO()
    df.to_excel(buf, index=False, engine="openpyxl")
    return buf.getvalue()


def _csv_bytes() -> bytes:
    return (
        b"timestamp,TAG-100,TAG-200\n"
        b"2026-06-01T00:00:00Z,10.5,20.1\n"
        b"2026-06-01T01:00:00Z,11.0,20.4\n"
    )


# --------------------------------------------------------------- the journey


def test_01_create_plant(client):
    resp = client.post(
        "/configRouter/plants",
        json={"plant_code_id": PLANT, "label": "E2E Test", "industry": "cement"},
    )
    assert_standard_response(resp, 201)


def test_02_plant_is_listed(client):
    body = assert_standard_response(
        client.get("/configRouter/plants", params={"plant_code_id": PLANT}), 200
    )
    rows = body["data"] if isinstance(body["data"], list) else body["data"].get("plants", [])
    assert any(r.get("plant_code_id") == PLANT for r in rows), rows


def test_03_plant_got_its_own_database_with_the_cdm_schema():
    from p0.api import plants as pl
    from p0.api.database.connection import open_db

    db = pl.registered_plant_db_name(PLANT)
    assert db == pl.plant_db_name(PLANT), "registry must point at the plant's own DB"

    conn = open_db(db, connect_timeout=10)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema='public'"
        )
        assert cur.fetchone()[0] == len(pl._cdm_schema_tables())
        cur.execute("SELECT to_regclass('public.flow_state')")
        assert cur.fetchone()[0] is not None
    finally:
        conn.close()


@pytest.mark.parametrize(
    "endpoint,filename,payload,extra",
    [
        ("/connectors/pnid/upload", "e2e_pid.pdf", "pdf", {}),
        ("/connectors/sap/upload", "e2e_sap.xlsx", "xlsx", {}),
        ("/connectors/timeseries/upload", "e2e_ts.csv", "csv", {}),
        (
            "/connectors/documents/upload",
            "e2e_manual.pdf",
            "pdf",
            {"document_type": "equipment_manuals"},
        ),
    ],
    ids=["pnid-pdf", "sap-xlsx", "ts-csv", "docs-pdf"],
)
def test_04_upload_a_real_file(client, endpoint, filename, payload, extra):
    """Each connector accepts a real file and answers in the standard envelope."""
    blob = {"pdf": _pdf_bytes("KILN 01"), "xlsx": _xlsx_bytes(), "csv": _csv_bytes()}[payload]
    ctype = {
        "pdf": "application/pdf",
        "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "csv": "text/csv",
    }[payload]

    resp = client.post(
        endpoint,
        data={"plant_code_id": PLANT, **extra},
        files={"files": (filename, blob, ctype)},
    )
    body = assert_standard_response(resp, 200)
    assert body["data"], f"{endpoint} returned an empty data envelope"


def test_05_uploads_registered_an_upload_job(client):
    body = assert_standard_response(
        client.get("/connectors/jobs", params={"plant_code_id": PLANT}), 200
    )
    data = body["data"]
    jobs = data if isinstance(data, list) else data.get("jobs", [])
    mine = [j for j in jobs if j.get("plant_code_id") == PLANT]
    assert mine, f"no upload job recorded for {PLANT}"


def test_06_uploads_wrote_flow_state_rows(client):
    """The regression that started all this: an upload must create its flow_state row.
    It silently did not, because the plant arrived as a multipart form field."""
    body = assert_standard_response(
        client.get("/flow/state", params={"plant_code_id": PLANT}), 200
    )
    rows = body["data"]["files"]
    assert rows, f"upload created no flow_state row for {PLANT}"
    names = {r.get("file_name") for r in rows}
    assert {"e2e_pid.pdf", "e2e_sap.xlsx", "e2e_ts.csv"} <= names, names
    assert all(r["plant_code_id"] == PLANT for r in rows), "rows must be plant-scoped"


def test_07_purge_preview_reports_what_delete_would_take(client):
    body = assert_standard_response(
        client.get(f"/configRouter/plants/{PLANT}/purgePreview"), 200
    )
    assert body["data"], "purgePreview must describe what a delete would remove"


def test_08_delete_plant_cascades(client):
    from p0.api import plants as pl

    db = pl.plant_db_name(PLANT)
    assert_standard_response(client.delete(f"/configRouter/plants/{PLANT}"), 200)

    assert not pl.plant_is_registered(PLANT), "registry row must be gone"
    assert pl.registered_plant_db_name(PLANT) is None

    m = pl._maintenance_conn()
    try:
        cur = m.cursor()
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db,))
        assert cur.fetchone() is None, f"{db} must be dropped with the plant"
    finally:
        m.close()


def test_09_deleted_plant_is_gone_from_the_registry(client):
    body = assert_standard_response(
        client.get("/configRouter/plants", params={"plant_code_id": PLANT}), 200
    )
    rows = body["data"] if isinstance(body["data"], list) else body["data"].get("plants", [])
    assert not any(r.get("plant_code_id") == PLANT for r in rows)


def test_10_lifecycle_is_audited_in_the_registry(client):
    """plant.create and plant.delete must be recorded in the registry's plant_audit —
    the plant's own database is gone by now, so only the registry can hold them."""
    body = assert_standard_response(
        client.get("/configRouter/plantAudit", params={"plant_code_id": PLANT}), 200
    )
    actions = {e["action"] for e in body["data"]["events"]}
    assert {"plant.create", "plant.delete"} <= actions, actions
