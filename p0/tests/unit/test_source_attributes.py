"""Per-source attribute packing and the downstream ts contract."""

import json

import pandas as pd
import yaml

from p0.utils.source_attributes import pack_source_attributes, source_columns

SCHEMA = "p0/config/templates/_common/schema.yaml"
ENTITIES = "p0/config/templates/_common/entities.yaml"

CONTRACT_COLUMNS = [
    "ts_uid",
    "plant_code_id",
    "site",
    "tag_name",
    "iotdb_tag_id",
    "description",
    "unit",
    "data_type",
    "equipment_id",
    "normalized_asset",
    "op_limit_ll",
    "op_limit_l",
    "op_limit_h",
    "op_limit_hh",
    "safety_limit_ll",
    "safety_limit_l",
    "source_system",
    "source_record_id",
    "confidence",
    "is_active",
]


def _ts_table():
    return yaml.safe_load(open(SCHEMA))["schema"]["timeseries_metadata"]["columns"]


def test_the_downstream_contract_columns_all_still_exist():
    cols = _ts_table()
    missing = [c for c in CONTRACT_COLUMNS if c not in cols]
    assert missing == []


def test_source_attributes_is_declared_as_json():
    assert _ts_table()["source_attributes"] == "JSON"


def test_the_prefix_list_is_template_declared_not_hardcoded():
    ents = yaml.safe_load(open(ENTITIES))
    ents = ents.get("entities") or ents
    prefixes = ents["timeseries_metadata"]["source_attribute_prefixes"]
    assert isinstance(prefixes, list) and prefixes


def test_each_row_carries_only_the_fields_it_actually_has():
    df = pd.DataFrame(
        {
            "tag_name": ["A", "B"],
            "pi_parent": ["p1", None],
            "pi_name": ["n1", ""],
            "alerts_prime_priority": [None, "HIGH"],
        }
    )
    packed = pack_source_attributes(df, ["pi_", "alerts_prime_"])
    first = json.loads(packed.iloc[0])
    second = json.loads(packed.iloc[1])
    assert first == {"pi": {"parent": "p1", "name": "n1"}}
    assert second == {"alerts_prime": {"priority": "HIGH"}}


def test_a_row_with_no_source_fields_packs_to_nothing():
    df = pd.DataFrame({"tag_name": ["A"], "pi_parent": [None]})
    assert pack_source_attributes(df, ["pi_"]).iloc[0] == ""


def test_an_undeclared_prefix_contributes_nothing():
    df = pd.DataFrame({"tag_name": ["A"], "sap_plant": ["X"]})
    assert source_columns(list(df.columns), ["pi_"]) == {}
    assert pack_source_attributes(df, ["pi_"]).iloc[0] == ""


def test_a_new_source_needs_only_a_prefix_not_a_schema_change():
    df = pd.DataFrame({"tag_name": ["A"], "scada_loop_id": ["L-9"]})
    packed = pack_source_attributes(df, ["pi_", "scada_"])
    assert json.loads(packed.iloc[0]) == {"scada": {"loop_id": "L-9"}}
