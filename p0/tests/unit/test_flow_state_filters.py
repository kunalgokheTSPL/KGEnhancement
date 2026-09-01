"""A filter the server cannot honour is refused, never silently ignored."""

from __future__ import annotations

import os

import pytest

os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

from p0.api.routers.flow import (
    _LIFECYCLE,
    _matches_status,
    _resolve_flow,
    _resolve_lifecycle,
)
from p0.api.services.connector_state import CONNECTORS, FLOWS, FLOWS_BY_CONNECTOR


@pytest.mark.parametrize("name", FLOWS)
def test_every_flow_the_store_can_hold_is_accepted(name):
    assert _resolve_flow(name) == (name, None)


def test_a_flow_is_matched_however_the_caller_capitalised_it():
    assert _resolve_flow("DOCS")[0] == "docs"
    assert _resolve_flow("  Ts_Values  ")[0] == "ts_values"


def test_a_flow_nobody_writes_is_refused_instead_of_returning_nothing():
    resolved, error = _resolve_flow("nonsense")

    assert resolved is None
    assert error["field"] == "flow"
    assert "docs" in error["message"]


def test_an_empty_flow_is_still_refused():
    assert _resolve_flow("   ")[1]["field"] == "flow"


@pytest.mark.parametrize("connector", CONNECTORS)
def test_a_connector_name_is_refused_by_naming_the_flow_it_maps_to(connector):
    resolved, error = _resolve_flow(connector)

    if connector in FLOWS:
        assert (resolved, error) == (connector, None)
        return

    assert resolved is None
    for flow in FLOWS_BY_CONNECTOR[connector]:
        assert f"flow={flow}" in error["message"]


def test_the_connector_endpoint_is_named_as_the_other_way_round():
    _, error = _resolve_flow("documents")

    assert "/connectors/documents/state" in error["message"]


def test_a_connector_with_two_flows_names_both_rather_than_picking_one():
    _, error = _resolve_flow("timeseries")

    assert "flow=ts" in error["message"]
    assert "flow=ts_values" in error["message"]


def test_the_lifecycle_filter_offers_exactly_what_it_documents():
    assert _LIFECYCLE == ("pending", "processed", "failed", "all")


def test_a_failed_row_is_failed_whatever_stage_it_reached():
    row = {"stage": "committed", "status": "failed"}

    assert _matches_status(row, "failed")
    assert not _matches_status(row, "processed")
    assert not _matches_status(row, "pending")


def test_a_row_past_processing_counts_as_processed():
    row = {"stage": "reviewing", "status": "done"}

    assert _matches_status(row, "processed")
    assert not _matches_status(row, "pending")


def test_a_freshly_uploaded_row_counts_as_pending():
    row = {"stage": "uploaded", "status": "pending"}

    assert _matches_status(row, "pending")
    assert not _matches_status(row, "processed")


def test_a_caller_who_asks_for_nothing_gets_every_row():
    assert _resolve_lifecycle(None, None) == ("all", "lifecycle", None)
    assert _resolve_lifecycle("", "") == ("all", "lifecycle", None)


def test_the_new_name_is_read():
    wanted, field, error = _resolve_lifecycle("processed", None)

    assert (wanted, field, error) == ("processed", "lifecycle", None)


def test_a_client_still_sending_the_old_name_keeps_working():
    wanted, field, error = _resolve_lifecycle(None, "processed")

    assert (wanted, field, error) == ("processed", "status", None)


def test_the_error_names_whichever_field_the_caller_actually_sent():
    assert _resolve_lifecycle(None, "bogus")[1] == "status"
    assert _resolve_lifecycle("bogus", None)[1] == "lifecycle"


def test_the_two_names_agreeing_is_not_a_conflict():
    assert _resolve_lifecycle("failed", "failed")[2] is None


def test_the_two_names_disagreeing_is_refused_rather_than_ranked():
    _, _, error = _resolve_lifecycle("processed", "pending")

    assert error["field"] == "lifecycle"
    assert "disagree" in error["message"]


def test_a_lifecycle_value_is_read_however_it_was_capitalised():
    assert _resolve_lifecycle("  Processed ", None)[0] == "processed"
