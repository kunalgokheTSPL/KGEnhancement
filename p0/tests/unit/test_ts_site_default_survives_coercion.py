"""site is a NOT NULL upsert key, so its default must survive value coercion."""

from __future__ import annotations

import pytest

from p0.api.database.cdm_writer import _TS_SITE_DEFAULT, _ts_site
from p0.api.services.transforms import PG_NULL_TOKENS, _coerce_for_pg

BLANKS = [None, "", "   ", "nan", "NaN", "none", "NULL", "NaT"]


@pytest.mark.parametrize("value", BLANKS)
def test_every_blank_spelling_becomes_the_default(value):
    assert _ts_site({"site": value}) == _TS_SITE_DEFAULT


@pytest.mark.parametrize("value", BLANKS)
def test_the_default_is_not_nulled_by_coercion(value):
    coerced = _coerce_for_pg(_ts_site({"site": value}), "character varying")
    assert coerced is not None


def test_a_real_site_is_untouched():
    assert _ts_site({"site": "SITE-1"}) == "SITE-1"
    assert _coerce_for_pg("SITE-1", "character varying") == "SITE-1"


def test_the_two_layers_share_one_definition_of_blank():
    assert _TS_SITE_DEFAULT.lower() not in PG_NULL_TOKENS
    for token in PG_NULL_TOKENS:
        assert _ts_site({"site": token}) == _TS_SITE_DEFAULT
