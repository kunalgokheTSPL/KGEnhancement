"""request_context_dep captures plant_code_id from query, JSON body, and multipart
uploads — without consuming the upload before the endpoint reads it."""

from __future__ import annotations

from fastapi import Depends, FastAPI, File, Form, UploadFile
from fastapi.testclient import TestClient

from p0.api.audit_context import get_plant, request_context_dep


def _app() -> FastAPI:
    app = FastAPI()

    @app.post("/upload", dependencies=[Depends(request_context_dep)])
    async def upload(
        plant_code_id: str = Form(...), files: list[UploadFile] = File(...)
    ):
        payload = await files[0].read()
        return {
            "ctx_plant": get_plant(),
            "form_plant": plant_code_id,
            "bytes": len(payload),
        }

    @app.post("/json", dependencies=[Depends(request_context_dep)])
    async def json_ep(payload: dict):
        return {"ctx_plant": get_plant()}

    @app.get("/query", dependencies=[Depends(request_context_dep)])
    async def query_ep():
        return {"ctx_plant": get_plant()}

    return app


client = TestClient(_app())


def test_plant_captured_from_multipart_without_eating_the_upload():
    r = client.post(
        "/upload",
        data={"plant_code_id": "Plant_1"},
        files={"files": ("d.csv", b"a,b\n1,2\n", "text/csv")},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ctx_plant"] == "Plant_1"
    assert body["form_plant"] == "Plant_1"  # the endpoint still received the form
    assert body["bytes"] == 8  # ...and the upload bytes are intact


def test_plant_captured_from_json_body():
    r = client.post("/json", json={"plant_code_id": "Plant_2", "x": 1})
    assert r.json()["ctx_plant"] == "Plant_2"


def test_plant_captured_from_query_string():
    r = client.get("/query", params={"plant_code_id": "Plant_3"})
    assert r.json()["ctx_plant"] == "Plant_3"


def test_set_plant_feeds_the_shared_ctx_the_driver_routes_on():
    """p0's plant is the shared plant_code_ctx the CDM driver routes on."""
    from p0.drivers.database_driver import PostgresDriver
    from utility.middleware import plant_code_ctx

    from p0.api.audit_context import get_plant, set_plant

    set_plant("Plant_1")
    try:
        assert plant_code_ctx.get() == "Plant_1"
        assert get_plant() == "Plant_1"
        assert PostgresDriver().config["database"] == "decisionops_plant_1"
    finally:
        set_plant(None)


def test_driver_routes_to_the_database_p0_actually_provisions():
    """The adapter's db name must match plants.py's, whatever the upstream driver derives."""
    from p0.api.plants import plant_db_name

    from p0.drivers._plant_db import plant_database_name

    for plant_code_id in ("Plant_1", "plant_1", "Plant 2", "PLANT-3"):
        assert plant_database_name(plant_code_id) == plant_db_name(plant_code_id)
