"""File lists and context rows must page, and the processed split is a server decision."""

from __future__ import annotations

import pathlib

import pytest

from p0.api.responses import MAX_PAGE_SIZE, paginate, paginated_envelope
from p0.api.routers.flow import _matches_status

CONTEXT = (
    pathlib.Path(__file__).resolve().parents[2] / "api" / "routers" / "context.py"
).read_text()
FLOW = (pathlib.Path(__file__).resolve().parents[2] / "api" / "routers" / "flow.py").read_text()


def test_first_page_of_many():
    window, meta = paginate(list(range(250)), page=1, page_size=100)
    assert window == list(range(100))
    assert meta["total_items"] == 250
    assert meta["total_pages"] == 3
    assert meta["has_more"] is True


def test_last_page_is_partial_and_has_no_more():
    window, meta = paginate(list(range(250)), page=3, page_size=100)
    assert window == list(range(200, 250))
    assert meta["has_more"] is False


def test_page_beyond_the_end_is_empty_not_an_error():
    window, meta = paginate(list(range(10)), page=99, page_size=100)
    assert window == []
    assert meta["total_items"] == 10
    assert meta["has_more"] is False


def test_empty_collection_reports_zero_pages():
    window, meta = paginate([], page=1, page_size=100)
    assert window == []
    assert meta["total_pages"] == 0
    assert meta["has_more"] is False


def test_page_size_is_capped():
    _, meta = paginate(list(range(5000)), page=1, page_size=999999)
    assert meta["page_size"] == MAX_PAGE_SIZE


def test_degenerate_inputs_are_coerced():
    _, meta = paginate(list(range(10)), page=0, page_size=0)
    assert meta["page"] == 1
    assert meta["page_size"] >= 1


def test_pagination_object_has_the_agreed_shape():
    _, meta = paginate([1, 2, 3], page=1, page_size=2)
    assert set(meta) == {
        "page",
        "page_size",
        "total_items",
        "total_pages",
        "has_more",
        "next_cursor",
    }


def test_envelope_reports_the_full_total_not_the_page_length():
    envelope = paginated_envelope(
        {"rows": list(range(250))},
        items_key="rows",
        page=1,
        page_size=100,
        message="ok",
        empty_message="empty",
    )
    assert len(envelope["data"]["rows"]) == 100
    assert envelope["data"]["total"] == 250
    assert envelope["data"]["is_empty"] is False
    assert envelope["data"]["pagination"]["total_pages"] == 3


def test_envelope_uses_the_empty_message_when_there_is_nothing():
    envelope = paginated_envelope(
        {"rows": []},
        items_key="rows",
        page=1,
        page_size=100,
        message="ok",
        empty_message="No rows staged yet.",
    )
    assert envelope["message"] == "No rows staged yet."
    assert envelope["data"]["is_empty"] is True


@pytest.mark.parametrize(
    "stage, status, wanted, expected",
    [
        ("uploaded", "done", "pending", True),
        ("staged", "done", "pending", True),
        ("processed", "done", "pending", False),
        ("processed", "done", "processed", True),
        ("committed", "done", "processed", True),
        ("uploaded", "done", "processed", False),
        ("processed", "failed", "failed", True),
        ("processed", "failed", "processed", False),
        ("processed", "failed", "pending", False),
        ("uploaded", "done", "all", True),
    ],
)
def test_status_filter_splits_processed_from_unprocessed(stage, status, wanted, expected):
    assert _matches_status({"stage": stage, "status": status}, wanted) is expected


def test_flow_state_accepts_page_and_status():
    block = FLOW.split("def listState(")[1]
    assert "page: int = Query(1, ge=1)" in block
    assert "page_size: int = Query(100" in block
    assert "status: str | None = Query(" in block


def test_every_paginating_context_endpoint_paginates():
    """docs, ts, sap and findings paginate, plus the combined /context/get that fronts them."""
    assert CONTEXT.count("page_size: int = Query(100, ge=1, le=1000)") == 5
    assert CONTEXT.count("paginated_envelope(") == 4
