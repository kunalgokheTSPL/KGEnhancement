"""One canonical grouping vocabulary, used identically on upload and on read."""

from __future__ import annotations

import pytest

from p0.api.services import taxonomy as tx


def _classify(row):
    return row.get("bucket", "pending")


def _rows(*pairs):
    return [
        {"flow_uid": i + 1, "file_type": key, "bucket": bucket}
        for i, (key, bucket) in enumerate(pairs)
    ]


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("pnid_files", "pnid"),
        ("pfd_files", "pfd"),
        ("linelist_files", "line_list"),
        ("linelist", "line_list"),
        ("other_files", "others"),
        ("other", "others"),
        ("line-list", "line_list"),
        ("Line List", "line_list"),
        ("pnid", "pnid"),
    ],
)
def test_upload_side_spellings_resolve_to_the_read_side_vocabulary(raw, expected):
    """The doc's verified bug: four buckets had two names depending on direction."""
    assert tx.canonical_category(raw) == expected


def test_historian_tab_key_is_normalised():
    """The UI's internal key is 'source'; the API vocabulary is 'historian'."""
    assert tx.canonical_category("source") == "historian"
    assert tx.canonical_category("historian") == "historian"


def test_blank_category_is_none():
    assert tx.canonical_category(None) is None
    assert tx.canonical_category("   ") is None


def test_level_counts_match_the_connectors():
    assert tx.levels("pnid") == 1
    assert tx.levels("timeseries") == 1
    assert tx.levels("documents") == 2
    assert tx.levels("sap") == 2


def test_group_kinds_per_connector():
    assert tx.group_kinds("pnid") == (tx.CATEGORY, None)
    assert tx.group_kinds("timeseries") == (tx.FILE_ROLE, None)
    assert tx.group_kinds("documents") == (tx.SUBTYPE, tx.TYPE)
    assert tx.group_kinds("sap") == (tx.TABLE, tx.PIPELINE_GROUP)


def test_sap_tables_map_to_their_pipeline_group():
    assert tx.parent_of("sap", "AUFK") == "workorder"
    assert tx.parent_of("sap", "IFLOT") == "floc"
    assert tx.parent_of("sap", "PLKO") == "tasklist"
    assert tx.parent_of("sap", "MARA") == "material"


def test_all_21_sap_tables_are_mapped():
    assert len(tx.SAP_TABLE_TO_GROUP) == 21
    assert set(tx.SAP_TABLE_TO_GROUP.values()) == {
        "workorder",
        "floc",
        "tasklist",
        "material",
    }


def test_one_level_connectors_have_no_parent():
    assert tx.parent_of("pnid", "pfd") is None
    assert tx.parent_of("timeseries", "values") is None


def test_pnid_groups_include_empty_catalog_buckets():
    """The UI must render all four buckets even when three are empty."""
    groups = tx.build_groups(
        "pnid", _rows(("pnid", "processed")), classify=_classify, key_of=lambda r: r["file_type"]
    )
    keys = [g["group_key"] for g in groups]
    assert keys == ["pnid", "pfd", "line_list", "others"]
    assert groups[0]["counts"]["files_total"] == 1
    assert groups[1]["counts"]["files_total"] == 0


def test_every_group_carries_the_same_seven_counts():
    groups = tx.build_groups(
        "timeseries",
        _rows(("metadata", "processed")),
        classify=_classify,
        key_of=lambda r: r["file_type"],
    )
    for group in groups:
        assert set(group["counts"]) == {
            "files_total",
            "pending",
            "processing",
            "processed",
            "failed",
            "skipped",
            "superseded",
        }


def test_file_ids_are_listed_on_the_leaf_group():
    groups = tx.build_groups(
        "timeseries",
        _rows(("values", "processed"), ("values", "failed")),
        classify=_classify,
        key_of=lambda r: r["file_type"],
    )
    values = next(g for g in groups if g["group_key"] == "values")
    assert values["file_ids"] == [1, 2]
    assert values["counts"]["processed"] == 1
    assert values["counts"]["failed"] == 1


def test_sap_rolls_leaf_tables_up_into_pipeline_groups():
    groups = tx.build_groups(
        "sap",
        _rows(("AUFK", "processed"), ("AFKO", "processed"), ("MARA", "pending")),
        classify=_classify,
        key_of=lambda r: r["file_type"],
    )
    by_key = {g["group_key"]: g for g in groups}
    assert by_key["workorder"]["group_kind"] == tx.PIPELINE_GROUP
    assert by_key["workorder"]["counts"]["files_total"] == 2
    assert by_key["material"]["counts"]["pending"] == 1
    assert by_key["AUFK"]["parent_key"] == "workorder"


def test_parent_counts_are_the_sum_of_their_children():
    groups = tx.build_groups(
        "sap",
        _rows(("AUFK", "processed"), ("AFKO", "failed"), ("QMEL", "pending")),
        classify=_classify,
        key_of=lambda r: r["file_type"],
    )
    workorder = next(g for g in groups if g["group_key"] == "workorder")
    assert workorder["counts"]["files_total"] == 3
    assert workorder["counts"]["processed"] == 1
    assert workorder["counts"]["failed"] == 1
    assert workorder["counts"]["pending"] == 1


def test_unknown_key_becomes_a_custom_group_rather_than_being_dropped():
    groups = tx.build_groups(
        "documents",
        _rows(("my_custom_subtype", "processed"),),
        classify=_classify,
        key_of=lambda r: r["file_type"],
    )
    custom = next(g for g in groups if g["group_key"] == "my_custom_subtype")
    assert custom["is_custom"] is True
    assert custom["label"] == "My Custom Subtype"


def test_files_with_no_category_are_not_lost():
    groups = tx.build_groups(
        "documents",
        [{"flow_uid": 1, "file_type": None, "bucket": "pending"}],
        classify=_classify,
        key_of=lambda r: r["file_type"],
    )
    assert any(g["group_key"] == "uncategorised" for g in groups)


def test_file_taxonomy_block_shape():
    block = tx.taxonomy_for_file("sap", "AUFK")
    assert block["group_kind"] == tx.TABLE
    assert block["group_key"] == "AUFK"
    assert block["parent_key"] == "workorder"
    assert block["parent_label"] == "Workorder"


def test_file_taxonomy_normalises_the_upload_spelling():
    block = tx.taxonomy_for_file("pnid", "linelist_files")
    assert block["group_key"] == "line_list"
    assert block["parent_key"] is None
