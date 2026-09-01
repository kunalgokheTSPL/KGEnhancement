"""ConnectorState is derived from flow_state, so it can never drift from the files."""

from __future__ import annotations

import pathlib

import pytest

from p0.api.services import connector_state as cs
from p0.api.services import findings

CONNECTORS_ROUTER = (
    pathlib.Path(__file__).resolve().parents[2] / "api" / "routers" / "connectors.py"
).read_text()


def _rows(*specs):
    """specs are (stage, status) pairs."""
    return [{"stage": stage, "status": status} for stage, status in specs]


def test_connector_names_map_to_flow_names():
    assert cs.flow_name("documents") == "docs"
    assert cs.flow_name("timeseries") == "ts"
    assert cs.flow_name("pnid") == "pnid"
    assert cs.flow_name("sap") == "sap"
    assert cs.flow_name("nonsense") is None


def test_connector_name_is_case_insensitive():
    assert cs.flow_name("  Documents ") == "docs"


def test_no_files_is_not_configured():
    state = cs._rollup([])
    assert state["status"] == cs.NOT_CONFIGURED
    assert state["total_file_count"] == 0
    assert state["has_unprocessed_files"] is False


def test_uploaded_files_are_pending_and_unprocessed():
    state = cs._rollup(_rows(("uploaded", "done"), ("staged", "done")))
    assert state["status"] == cs.UPLOADED
    assert state["counts"]["pending"] == 2
    assert state["unprocessed_file_count"] == 2
    assert state["has_unprocessed_files"] is True


def test_state_is_the_furthest_any_file_reached_once_nothing_is_pending():
    state = cs._rollup(_rows(("processed", "done"), ("committed", "done")))
    assert state["status"] == cs.COMMITTED


def test_committed_connector_with_new_files_reopens_without_losing_the_commit():
    """The 'add files to an already-processed source' case, decided server-side."""
    state = cs._rollup(_rows(("committed", "done"), ("uploaded", "done")))
    assert state["status"] == cs.UPLOADED
    assert state["has_unprocessed_files"] is True
    assert state["unprocessed_file_count"] == 1
    assert state["counts"]["processed"] == 1


def test_the_seven_standard_counts_are_always_present():
    state = cs._rollup(_rows(("uploaded", "done")))
    assert set(state["counts"]) == {
        "files_total",
        "pending",
        "processing",
        "processed",
        "failed",
        "skipped",
        "superseded",
    }


def test_counts_partition_the_files_exactly_once():
    rows = _rows(
        ("uploaded", "done"),
        ("processing", "running"),
        ("processed", "done"),
        ("processed", "failed"),
        ("processed", "skipped"),
    )
    state = cs._rollup(rows)
    counts = state["counts"]
    buckets = counts["pending"] + counts["processing"] + counts["processed"]
    buckets += counts["failed"] + counts["skipped"] + counts["superseded"]
    assert buckets == counts["files_total"] == 5


def test_a_failed_file_is_counted_as_failed_whatever_its_stage():
    state = cs._rollup(_rows(("processed", "failed")))
    assert state["counts"]["failed"] == 1
    assert state["counts"]["processed"] == 0
    assert state["has_failures"] is True


def test_partial_run_shows_both_processed_and_failed():
    rows = _rows(*([("processed", "done")] * 8), *([("processed", "failed")] * 2))
    state = cs._rollup(rows)
    assert state["counts"]["processed"] == 8
    assert state["counts"]["failed"] == 2
    assert state["has_failures"] is True


def test_review_and_validated_states_are_distinguished():
    assert cs._rollup(_rows(("reviewing", "done")))["status"] == cs.IN_REVIEW
    assert cs._rollup(_rows(("reviewed", "done")))["status"] == cs.VALIDATED


def test_timestamps_are_rolled_up():
    rows = [
        {
            "stage": "committed",
            "status": "done",
            "created_at": "2026-01-01T00:00:00",
            "processed_at": "2026-01-02T00:00:00",
            "reviewed_at": "2026-01-03T00:00:00",
            "committed_at": "2026-01-04T00:00:00",
        },
        {
            "stage": "committed",
            "status": "done",
            "created_at": "2025-12-31T00:00:00",
            "committed_at": "2026-01-05T00:00:00",
        },
    ]
    state = cs._rollup(rows)
    assert state["configured_at"] == "2025-12-31T00:00:00"
    assert state["committed_at"] == "2026-01-05T00:00:00"


def test_last_batch_and_run_ids_are_surfaced():
    rows = [
        {
            "stage": "processed",
            "status": "done",
            "upload_batch_id": "bat_1",
            "process_job_id": "job_1",
        }
    ]
    state = cs._rollup(rows)
    assert state["last_upload_batch_id"] == "bat_1"
    assert state["last_run_id"] == "job_1"


def test_unknown_connector_returns_an_empty_state_not_an_error():
    state = cs.connector_state("PLANT_X", "nonsense")
    assert state["status"] == cs.NOT_CONFIGURED
    assert state["flow"] is None


def test_every_connector_is_reported():
    assert set(cs.CONNECTORS) == set(cs._FLOWS_BY_CONNECTOR)
    assert len(cs.CONNECTORS) == len(set(cs.CONNECTORS))
    assert set(findings.CONNECTORS) <= set(cs.CONNECTORS)


def test_endpoints_exist_and_validate_the_connector_name():
    assert '"/{connector}/state"' in CONNECTORS_ROUTER
    assert '"/state"' in CONNECTORS_ROUTER
    assert "is not a valid connector" in CONNECTORS_ROUTER


def test_endpoints_follow_the_validation_guideline():
    """Validation failures return 422 per the API guidelines."""
    block = CONNECTORS_ROUTER.split("def getConnectorState(")[1]
    assert "HTTPStatus.UNPROCESSABLE_ENTITY" in block
    assert 'plant_code_id: str = Query(' in block


def test_a_connector_with_a_new_file_is_not_reported_committed():
    """Upload after commit and the tile must re-open, not stay COMMITTED."""
    rolled = cs._rollup([
        {"stage": "committed", "status": "done"},
        {"stage": "staged", "status": "done"},
    ])
    assert rolled["status"] == cs.UPLOADED
    assert rolled["has_unprocessed_files"] is True
    assert rolled["unprocessed_file_count"] == 1
    assert rolled["total_file_count"] == 2


def test_a_fully_committed_connector_still_reports_committed():
    rolled = cs._rollup([
        {"stage": "committed", "status": "done"},
        {"stage": "committed", "status": "done"},
    ])
    assert rolled["status"] == cs.COMMITTED
    assert rolled["has_unprocessed_files"] is False


def test_processing_is_not_demoted_by_a_queued_file():
    """Only unprocessed files re-open the tile; a run in flight is not one."""
    rolled = cs._rollup([
        {"stage": "processing", "status": "running"},
        {"stage": "processed", "status": "done"},
    ])
    assert rolled["status"] == cs.PROCESSED
