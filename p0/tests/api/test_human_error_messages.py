"""Error envelopes must carry human-readable messages, never raw exception text."""

from __future__ import annotations

import pathlib
import re

import pytest

from p0.api.responses import human_message, redact, server_error

ROUTERS = pathlib.Path(__file__).resolve().parents[2] / "api" / "routers"


def test_no_router_leaks_raw_exception_text():
    """No router may put str(exc) into a response body."""
    offenders = []
    for path in sorted(ROUTERS.glob("*.py")):
        for lineno, line in enumerate(path.read_text().splitlines(), start=1):
            if "str(exc)" in line and "human_message" not in line:
                offenders.append(f"{path.name}:{lineno}: {line.strip()}")
    assert not offenders, "raw exception text reaches the client:\n" + "\n".join(offenders)


def test_redact_strips_credentials():
    assert "hunter2" not in redact("password=hunter2 host=db")
    assert "***" in redact("password=hunter2 host=db")
    assert redact("postgres://user:pw@host/db") == "postgres://***:***@host/db"
    assert redact("") == ""


@pytest.mark.parametrize(
    "raw, expected_fragment",
    [
        ('relation "cdm_equipment" does not exist', "does not exist"),
        ("could not connect to server", "Could not reach the database"),
        ("duplicate key value violates unique constraint", "already exists"),
    ],
)
def test_database_errors_become_sentences(raw, expected_fragment):
    message = human_message(Exception(raw), plant_code_id="PLANT_X")
    assert expected_fragment in message
    assert "Traceback" not in message


def test_missing_table_names_the_plant():
    message = human_message(
        Exception('relation "cdm_work_order" does not exist'), plant_code_id="PLANT_X"
    )
    assert "cdm_work_order" in message
    assert "PLANT_X" in message


def test_file_not_found_names_the_file():
    exc = FileNotFoundError(2, "No such file", "/tmp/missing.xlsx")
    assert "/tmp/missing.xlsx" in human_message(exc)


def test_key_error_reads_as_missing_column():
    assert "missing" in human_message(KeyError("equipment_tag")).lower()


def test_unknown_error_is_generic_but_not_raw():
    message = human_message(RuntimeError("boom at 0xdeadbeef"), action="loading plants")
    assert "0xdeadbeef" not in message
    assert "loading plants" in message


def test_server_error_envelope_shape():
    response = server_error(RuntimeError("internal detail"), action="listing plants")
    assert response.status_code == 500
    body = response.body.decode()
    assert '"success":false' in body.replace(" ", "")
    assert "internal detail" not in body
    assert re.search(r'"field"\s*:\s*"server"', body)


def test_server_error_message_matches_errors_entry():
    import json

    body = json.loads(server_error(ValueError("bad input"), action="saving config").body)
    assert body["message"] == body["errors"][0]["message"]
