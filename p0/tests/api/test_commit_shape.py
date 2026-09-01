"""All four connectors must return the same commit shape."""

from __future__ import annotations

import pathlib

import pytest

from p0.api.responses import normalise_commit

CONTEXT = (
    pathlib.Path(__file__).resolve().parents[2] / "api" / "routers" / "context.py"
).read_text()

REQUIRED = {
    "status",
    "connector",
    "commit_id",
    "tables",
    "total_rows_written",
    "rows_written",
    "rejected_rows",
}


def test_documents_shape_is_preserved_and_completed():
    result = normalise_commit(
        {"status": "committed", "tables": [{"table": "document_metadata", "rows_written": 12}],
         "total_rows_written": 12},
        connector="documents",
        commit_id=7,
    )
    assert REQUIRED <= set(result)
    assert result["total_rows_written"] == 12
    assert result["tables"][0]["rows_written"] == 12


def test_timeseries_single_table_becomes_a_tables_array():
    """ts returned `table` + `rows_written` and no tables[] at all."""
    result = normalise_commit(
        {
            "status": "committed",
            "table": "timeseries_metadata",
            "rows_inserted": 55,
            "rows_updated": 0,
            "rows_noop": 0,
            "rows_written": 55,
        },
        connector="timeseries",
        commit_id=9,
    )
    assert result["tables"] == [
        {
            "table": "timeseries_metadata",
            "rows_written": 55,
            "rows_updated": 0,
            "rows_skipped": 0,
            "rows_rejected": 0,
        }
    ]
    assert result["total_rows_written"] == 55


def test_sap_mixed_string_and_dict_entries_are_normalised():
    """sap returned a mixed array of bare strings and dicts."""
    result = normalise_commit(
        {"status": "committed", "tables": ["work_order", {"table": "equipment", "rows_written": 4}],
         "rows_written": 4},
        connector="sap",
        commit_id=11,
    )
    assert all(isinstance(t, dict) for t in result["tables"])
    assert result["tables"][0] == {
        "table": "work_order",
        "rows_written": 0,
        "rows_updated": 0,
        "rows_skipped": 0,
        "rows_rejected": 0,
    }


def test_pnid_extra_keys_survive():
    """pnid carries rows_replaced/files_replaced — additive, must not be dropped."""
    result = normalise_commit(
        {
            "status": "committed",
            "tables": [{"table": "equipment_pid", "rows_written": 3, "rows_replaced": 2}],
            "total_rows_written": 3,
            "files_replaced": ["a.pdf"],
        },
        connector="pnid",
        commit_id=3,
    )
    assert result["files_replaced"] == ["a.pdf"]
    assert result["tables"][0]["rows_replaced"] == 2


def test_total_is_derived_when_absent():
    result = normalise_commit(
        {"tables": [{"table": "a", "rows_written": 2}, {"table": "b", "rows_written": 3}]},
        connector="sap",
    )
    assert result["total_rows_written"] == 5


def test_status_defaults_to_committed():
    assert normalise_commit({}, connector="sap")["status"] == "committed"


def test_partial_status_is_not_overwritten():
    result = normalise_commit({"status": "partial"}, connector="timeseries")
    assert result["status"] == "partial"


def test_every_table_row_carries_the_four_counters():
    result = normalise_commit(
        {"tables": [{"table": "equipment", "rows_written": 1}]}, connector="sap"
    )
    row = result["tables"][0]
    for key in ("rows_written", "rows_updated", "rows_skipped", "rows_rejected"):
        assert key in row


def test_batch_and_actor_are_carried_when_known():
    result = normalise_commit(
        {}, connector="docs", commit_id=1, upload_batch_id="bat_1", committed_by="u@x.com"
    )
    assert result["upload_batch_id"] == "bat_1"
    assert result["committed_by"] == "u@x.com"


@pytest.mark.parametrize("connector", ["pnid", "documents", "timeseries", "sap"])
def test_every_commit_endpoint_normalises(connector):
    assert f'connector="{connector}"' in CONTEXT


def test_normalisation_runs_before_the_response_is_built():
    assert CONTEXT.count("result = normalise_commit(") == 5


def test_no_connector_reports_success_while_committing_nothing():
    """Timeseries used to return 'committed successfully' with a null commit_id."""
    assert '"status": "skipped", "message": "No rows provided"' not in CONTEXT


def test_every_connector_falls_back_to_the_reviewed_rows_on_an_empty_body():
    """The backend owns the reviewed state; the client should not have to resend it."""
    fallbacks = {
        "commitDocsContext": "_list_docs_from_rustfs(",
        "commitTsContext": "_read_ts_rows_for_commit(",
        "commitFindingsContext": "_read_findings_rows(",
    }
    for fn, call in fallbacks.items():
        block = CONTEXT.split(f"def {fn}(")[1].split("\n@router")[0]
        assert call in block, f"{fn} does not read the reviewed rows via {call}"

    # SAP's commit runs off-thread — the reviewed-rows read lives in
    # _run_commit_sap_bg, not commitSapContext itself.
    bg = CONTEXT.split("def _run_commit_sap_bg(")[1].split("\n@router")[0]
    assert "_read_sap_tables_for_commit(" in bg, (
        "_run_commit_sap_bg does not read the reviewed rows via _read_sap_tables_for_commit("
    )


def test_the_timeseries_read_has_one_source_and_no_second_guess():
    """One canonical location. A cascade would hide a broken pipeline."""
    block = CONTEXT.split("def _read_ts_rows_for_commit(")[1].split("\ndef ")[0]
    assert "ts_timeseries_metadata.parquet" in block
    assert "_read_stage_csvs(" not in block
    assert "_read_ts_from_db(" not in block
    assert "raise ReviewedStateMissing" in block


def test_the_timeseries_fallback_honours_the_same_scoping_as_the_review_page():
    block = CONTEXT.split("def _read_ts_rows_for_commit(")[1].split("\ndef ")[0]
    assert "_filter_rows_by_source_file(" in block
    assert "_filter_rows_by_batch(" in block
