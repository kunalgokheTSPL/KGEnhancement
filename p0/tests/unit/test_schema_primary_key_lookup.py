"""Primary key lookup used by the incremental entity merge."""

import yaml

from p0.utils.config_resolver import resolve_config_path
from p0.utils.schema_apply import _resolve_tables

MERGED_TABLES = [
    "timeseries_metadata",
    "equipment",
    "equipment_pid",
    "document_metadata",
    "work_order",
]


def _schema():
    return yaml.safe_load(open(resolve_config_path("p0/config", "schema_frozen.yaml")))


def test_the_live_schema_does_not_use_the_tables_top_level_key():
    assert _schema().get("tables") is None


def test_every_merged_table_resolves_a_primary_key():
    tables = _resolve_tables(_schema())
    for name in MERGED_TABLES:
        assert name in tables, name
        pk = tables[name].get("primary_key")
        assert isinstance(pk, str) and pk, name


def test_a_raw_tables_lookup_would_find_nothing():
    schema = _schema()
    assert schema.get("tables", {}).get("timeseries_metadata", {}).get(
        "primary_key"
    ) is None
    assert _resolve_tables(schema)["timeseries_metadata"]["primary_key"] == "ts_uid"
