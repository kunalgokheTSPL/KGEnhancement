"""Every findings source must key on business columns, never row position."""

from __future__ import annotations

import pytest

from p0.api.services import findings

SOURCES = ("aif", "gloc", "lopc", "upd_event", "trip_event")


@pytest.mark.parametrize("connector", SOURCES)
def test_every_findings_source_declares_a_natural_key(connector):
    spec = findings.spec_for(connector)
    assert spec.get("natural_key"), (
        f"{connector} has an empty natural_key, so source_record_id falls back to "
        "row position and a later upload deletes the earlier one"
    )


@pytest.mark.parametrize("connector", SOURCES)
def test_the_declared_key_columns_exist_on_the_source(connector):
    spec = findings.spec_for(connector)
    columns = set(spec.get("columns") or {})
    missing = [c for c in spec.get("natural_key", []) if c not in columns]
    assert not missing, f"{connector} keys on undeclared column(s): {missing}"


@pytest.mark.parametrize("connector", ("upd_event", "trip_event"))
def test_two_uploads_of_different_rows_do_not_collide(connector):
    first = [
        {"code": "C1", "start_datetime": "2026-08-01", "end_datetime": "2026-08-02",
         "eqp_tag_no": "T1", "field": "F"},
    ]
    second = [
        {"code": "C9", "start_datetime": "2026-09-01", "end_datetime": "2026-09-02",
         "eqp_tag_no": "T9", "field": "F"},
    ]
    assert not set(findings.assign_record_ids(connector, first)) & set(
        findings.assign_record_ids(connector, second)
    )
