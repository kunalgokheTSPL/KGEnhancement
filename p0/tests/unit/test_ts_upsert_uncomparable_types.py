"""A json column has no equality operator, so the change test must cast it."""

from __future__ import annotations

import pytest

from p0.api.database.cdm_writer import _ts_change_test

LIVE = {
    "source_attributes": "json",
    "notes_xml": "xml",
    "area": "character varying",
    "asset_link_confidence": "numeric",
    "updated_at": "timestamp with time zone",
}


@pytest.mark.parametrize("column", ["source_attributes", "notes_xml"])
def test_a_type_without_an_equality_operator_is_compared_as_text(column):
    sql = _ts_change_test(column, LIVE)
    assert sql.count("::text") == 2
    assert "IS DISTINCT FROM" in sql


@pytest.mark.parametrize("column", ["area", "asset_link_confidence", "updated_at"])
def test_a_comparable_type_is_left_alone(column):
    sql = _ts_change_test(column, LIVE)
    assert "::text" not in sql
    assert "IS DISTINCT FROM" in sql


def test_an_unknown_column_is_treated_as_comparable():
    assert "::text" not in _ts_change_test("brand_new", LIVE)


def test_the_cast_wraps_the_incoming_expression_not_just_the_column():
    sql = _ts_change_test("source_attributes", LIVE)
    assert sql.startswith('timeseries_metadata."source_attributes"::text')
    assert sql.rstrip().endswith(")::text")
