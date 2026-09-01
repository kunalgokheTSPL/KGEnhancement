"""Conformance tests for the connector registry (connectors/base/registry.py).

Every (data_type, source_type) must map to a real, concrete BaseConnector subclass —
including the dark-launched historian / SAP / Maximo connectors that are otherwise
never exercised. Catches a mapping pointing at a missing, non-connector, or still-
abstract class, and guards that the registry imports without the enterprise SDKs
(pyrfc / hdbcli / kerberos / cloud) installed.
"""

from __future__ import annotations

import pytest

from connectors.base import BaseConnector
from connectors.base.registry import REGISTRY, get_connector_class

_ENTRIES = sorted(REGISTRY.items())
_IDS = [f"{dt}-{st}" for (dt, st), _ in _ENTRIES]


@pytest.mark.parametrize("key,cls", _ENTRIES, ids=_IDS)
def test_registered_connector_is_a_concrete_baseconnector(key, cls):
    assert isinstance(cls, type), f"{key} maps to a non-class: {cls!r}"
    assert issubclass(cls, BaseConnector), f"{key} -> {cls.__name__} is not a BaseConnector"
    missing = getattr(cls, "__abstractmethods__", frozenset())
    assert not missing, f"{key} -> {cls.__name__} is abstract; unimplemented: {sorted(missing)}"


def test_get_connector_class_returns_the_mapped_class():
    for key, cls in REGISTRY.items():
        assert get_connector_class(*key) is cls


def test_get_connector_class_rejects_an_unknown_pair():
    with pytest.raises(ValueError):
        get_connector_class("nope", "nope")


def test_registry_covers_all_data_types_and_keeps_the_byo_sources():
    assert {dt for dt, _ in REGISTRY} == {"documents", "pnid", "sap", "timeseries"}
    # the dark-launched BYO connectors must stay registered — dropping one is a silent
    # loss of a differentiating capability
    byo = {
        ("timeseries", "osisoft_pi"),
        ("timeseries", "wonderware"),
        ("timeseries", "honeywell_phd"),
        ("timeseries", "aveva_historian"),
        ("sap", "sap_hana"),
        ("sap", "maximo"),
    }
    assert byo <= set(REGISTRY)
    assert len(REGISTRY) >= 32
