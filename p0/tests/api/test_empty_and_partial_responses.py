"""Empty results carry an honest signal; all-rejected uploads are not a success."""

from __future__ import annotations

import json

from p0.api.responses import (
    annotate_collection,
    collection_envelope,
    no_content,
    upload_outcome,
)


def test_annotate_marks_empty_collection():
    data = annotate_collection({"files": []}, "files")
    assert data["is_empty"] is True
    assert data["total"] == 0


def test_annotate_marks_populated_collection():
    data = annotate_collection({"files": [{"a": 1}, {"a": 2}]}, "files")
    assert data["is_empty"] is False
    assert data["total"] == 2


def test_annotate_tolerates_missing_key():
    data = annotate_collection({}, "files")
    assert data["is_empty"] is True
    assert data["total"] == 0


def test_annotate_keeps_existing_total():
    data = annotate_collection({"rows": [{"a": 1}], "total": 500}, "rows")
    assert data["total"] == 500
    assert data["is_empty"] is False


def test_empty_collection_uses_the_empty_message():
    envelope = collection_envelope(
        {"files": []},
        items_key="files",
        message="Flow state list fetched successfully",
        empty_message="No files have been uploaded yet for plant 'PLANT_X'.",
    )
    assert envelope["success"] is True
    assert "No files have been uploaded yet" in envelope["message"]
    assert "PLANT_X" in envelope["message"]
    assert envelope["data"]["is_empty"] is True


def test_populated_collection_uses_the_normal_message():
    envelope = collection_envelope(
        {"files": [{"n": 1}]},
        items_key="files",
        message="Flow state list fetched successfully",
        empty_message="nothing here",
    )
    assert envelope["message"] == "Flow state list fetched successfully"
    assert envelope["data"]["is_empty"] is False


def test_envelope_keeps_the_data_key_so_clients_do_not_break():
    """An empty result must still be 200 with a data object, not a bare 204."""
    envelope = collection_envelope(
        {"files": []}, items_key="files", message="ok", empty_message="empty"
    )
    assert set(envelope) == {"success", "message", "data"}
    assert isinstance(envelope["data"], dict)
    assert envelope["data"]["files"] == []


def test_no_content_is_204():
    assert no_content().status_code == 204


def test_all_files_accepted_is_success():
    result = upload_outcome(
        {"files": [{"name": "a.pdf", "status": "uploaded"}]}, noun="P&ID files"
    )
    assert result["success"] is True
    assert result["data"]["accepted_count"] == 1
    assert result["data"]["rejected_count"] == 0
    assert result["data"]["is_partial"] is False


def test_partial_upload_reports_both_counts():
    result = upload_outcome(
        {
            "files": [
                {"name": "a.pdf", "status": "uploaded"},
                {"name": "b.exe", "status": "rejected", "error": "Type .exe not allowed"},
            ]
        },
        noun="P&ID files",
    )
    assert result["success"] is True
    assert result["data"]["is_partial"] is True
    assert "1 of 2" in result["message"]
    assert "1 rejected" in result["message"]


def test_all_files_rejected_is_422_not_success():
    result = upload_outcome(
        {
            "files": [
                {"name": "a.exe", "status": "rejected", "error": "Type .exe not allowed"},
                {"name": "b.exe", "status": "rejected", "error": "File exceeds 50MB"},
            ]
        },
        noun="P&ID files",
    )
    assert result.status_code == 422
    body = json.loads(result.body)
    assert body["success"] is False
    assert "None of the 2" in body["message"]
    reasons = {e["message"] for e in body["errors"]}
    assert "Type .exe not allowed" in reasons
    assert "File exceeds 50MB" in reasons


def test_rejected_entries_name_the_offending_file():
    result = upload_outcome(
        {"files": [{"name": "broken.xlsx", "status": "rejected", "error": "corrupt"}]},
        noun="documents",
    )
    body = json.loads(result.body)
    assert body["errors"][0]["field"] == "broken.xlsx"


def test_no_files_provided_is_not_an_error():
    result = upload_outcome({"files": []}, noun="documents")
    assert result["success"] is True
    assert "No documents were provided" in result["message"]
