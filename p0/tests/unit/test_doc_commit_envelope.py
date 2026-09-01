"""A patch commit says exactly what changed; the old whole-scope commit still works."""

from __future__ import annotations

import os

os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

from p0.api.routers.context import _doc_version_conflict, _parse_doc_commit_mode

DELETE = {"record_id": "REC-1", "site": "S1"}


def _parse(**body):
    return _parse_doc_commit_mode(body)


def test_a_client_that_sends_no_mode_gets_todays_behaviour():
    mode, deletes, errors = _parse(rows=[])

    assert (mode, deletes, errors) == ("replace_scope", [], [])


def test_a_bare_list_body_is_still_a_whole_scope_replace():
    mode, deletes, errors = _parse_doc_commit_mode([{"record_id": "REC-1"}])

    assert (mode, deletes, errors) == ("replace_scope", [], [])


def test_a_patch_commit_is_recognised():
    mode, _, errors = _parse(mode="patch", rows=[])

    assert mode == "patch"
    assert errors == []


def test_the_mode_is_read_case_and_space_insensitively():
    assert _parse(mode="  Patch ")[0] == "patch"


def test_a_mode_nobody_implements_is_refused_rather_than_guessed():
    _, _, errors = _parse(mode="merge")

    assert [e["field"] for e in errors] == ["mode"]


def test_a_reviewer_delete_carries_through_the_envelope():
    _, deletes, errors = _parse(mode="patch", deletes=[DELETE])

    assert errors == []
    assert deletes[0]["record_id"] == "REC-1"
    assert deletes[0]["site"] == "S1"


def test_a_delete_that_names_no_site_takes_the_column_default():
    _, deletes, _ = _parse(mode="patch", deletes=[{"record_id": "REC-1"}])

    assert deletes[0]["site"] == "-"


def test_a_delete_without_a_record_id_is_refused():
    _, _, errors = _parse(mode="patch", deletes=[{"site": "S1"}])

    assert any(e["field"] == "deletes" for e in errors)


def test_deleting_during_a_whole_scope_replace_is_refused_as_ambiguous():
    _, _, errors = _parse(deletes=[DELETE])

    assert any("mode is patch" in e["message"] for e in errors)


def test_a_deletes_field_that_is_not_a_list_of_rows_is_refused():
    assert _parse(mode="patch", deletes="REC-1")[2]
    assert _parse(mode="patch", deletes=["REC-1"])[2]


def test_a_delete_may_state_the_version_it_was_read_at():
    _, deletes, errors = _parse(mode="patch", deletes=[dict(DELETE, row_version=4)])

    assert errors == []
    assert deletes[0]["row_version"] == 4


def test_a_stale_batch_is_refused_with_both_versions_named():
    stale = [
        {
            "site": "S1",
            "record_id": "REC-1",
            "expected_row_version": 2,
            "current_row_version": 5,
        }
    ]

    response = _doc_version_conflict(stale)

    assert response.status_code == 409
    body = response.body.decode()
    assert "REC-1" in body
    assert "now 5" in body
    assert "against 2" in body
