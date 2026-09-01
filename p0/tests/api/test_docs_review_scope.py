"""Docs review scoping: multi-batch preview and page-consistent grouping."""

from __future__ import annotations

import pytest

from p0.tests._p0_test_base import client, assert_standard_response

from p0.api.services.transforms import _batch_id_set, _filter_rows_by_batch
from p0.api.services.docs_taxonomy import _group_docs_by_category
import p0.api.routers.context as ctx

PLANT = "plant_1_testcase"


def _rows(*pairs):
    return [
        {"document_id": f"d{i}", "document_type": t, "upload_batch_id": b}
        for i, (t, b) in enumerate(pairs)
    ]


def test_batch_id_set_accepts_one_id():
    assert _batch_id_set("abc") == {"abc"}


def test_batch_id_set_accepts_comma_separated():
    assert _batch_id_set("a, b ,c") == {"a", "b", "c"}


def test_batch_id_set_accepts_a_sequence():
    assert _batch_id_set(["a", "b"]) == {"a", "b"}


def test_batch_id_set_empty_is_no_filter():
    assert _batch_id_set(None) == set()
    assert _batch_id_set("") == set()
    assert _batch_id_set(" , ") == set()


def test_single_batch_still_filters():
    rows = _rows(("sop", "A"), ("rca_reports", "B"))
    kept = _filter_rows_by_batch(rows, "A")
    assert [r["document_id"] for r in kept] == ["d0"]


def test_multi_batch_keeps_every_named_batch():
    """The per-subtype journey mints one batch per subtype — all are one review."""
    rows = _rows(("sop", "A"), ("rca_reports", "B"), ("hazop", "C"))
    kept = _filter_rows_by_batch(rows, "A,B")
    assert [r["document_id"] for r in kept] == ["d0", "d1"]


def test_multi_batch_excludes_unnamed_batch():
    rows = _rows(("sop", "A"), ("rca_reports", "B"), ("hazop", "C"))
    kept = _filter_rows_by_batch(rows, "A,B")
    assert all(r["upload_batch_id"] != "C" for r in kept)


def test_rows_without_the_column_are_never_dropped():
    rows = [{"document_id": "d0", "document_type": "sop"}]
    assert _filter_rows_by_batch(rows, "A") == rows


def test_grouping_of_a_page_agrees_with_that_page():
    """rows_by_category is grouped from the returned page, so the two agree."""
    rows = _rows(*[("sop", "A")] * 3, *[("rca_reports", "A")] * 2)
    page = rows[:2]
    grouped, _ = _group_docs_by_category(page)
    assert sum(len(v) for v in grouped.values()) == len(page)


@pytest.fixture
def _staged_docs(monkeypatch):
    """Feed getDocsContext a known five-row, two-subtype, two-batch review set."""
    rows = _rows(
        ("sop", "A"), ("sop", "A"), ("sop", "A"),
        ("rca_reports", "B"), ("rca_reports", "B"),
    )
    monkeypatch.setattr(ctx, "_list_docs_from_rustfs", lambda *a, **k: [dict(r) for r in rows])
    monkeypatch.setattr(ctx, "_advance_flow_to_reviewing", lambda *a, **k: None)
    return rows


def test_docs_context_page_grouping_matches_rows(client, _staged_docs):
    """End to end: rows_by_category never claims more rows than the page returns."""
    resp = client.get(
        "/context/getDocsContext",
        params={"plant_code_id": PLANT, "page": 1, "page_size": 2},
    )
    data = assert_standard_response(resp, 200)["data"]
    assert len(data["rows"]) == 2
    assert data["total"] == 5
    grouped_total = sum(len(v) for v in data["rows_by_category"].values())
    assert grouped_total == 2, (
        f"rows_by_category holds {grouped_total} rows but the page returned "
        f"{len(data['rows'])}"
    )


def test_docs_context_categories_count_the_whole_scope(client, _staged_docs):
    """Tabs stay stable across pages: categories counts the scope, not the page."""
    resp = client.get(
        "/context/getDocsContext",
        params={"plant_code_id": PLANT, "page": 1, "page_size": 2},
    )
    data = assert_standard_response(resp, 200)["data"]
    assert sum(c["count"] for c in data["categories"]) == 5


def test_docs_context_scopes_to_several_batches(client, _staged_docs):
    """A multi-subtype upload is reviewable in one call."""
    resp = client.get(
        "/context/getDocsContext",
        params={"plant_code_id": PLANT, "upload_batch_id": "A,B", "page_size": 100},
    )
    data = assert_standard_response(resp, 200)["data"]
    assert data["total"] == 5
    assert data["scope"] == "upload_batch_id"
    assert data["upload_batch_ids"] == ["A", "B"]


def test_docs_context_single_batch_is_unchanged(client, _staged_docs):
    """The published single-id contract keeps its exact behaviour."""
    resp = client.get(
        "/context/getDocsContext",
        params={"plant_code_id": PLANT, "upload_batch_id": "A", "page_size": 100},
    )
    data = assert_standard_response(resp, 200)["data"]
    assert data["total"] == 3
    assert {r["upload_batch_id"] for r in data["rows"]} == {"A"}
