"""Pins the timeseries tag to P&ID equipment link, separators and all."""

from __future__ import annotations

import contextlib
import logging

import pandas as pd

from p0.utils import ts_asset_identity as tsai


class _Sink(logging.Handler):
    """Collects warning text off one named logger."""

    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.text = ""

    def emit(self, record: logging.LogRecord) -> None:
        self.text += record.getMessage() + "\n"


@contextlib.contextmanager
def _warnings_from(name: str):
    """Capture warnings despite the shared test base disabling logging globally."""
    sink = _Sink()
    logger = logging.getLogger(name)
    previous_disable = logging.root.manager.disable
    previous_level = logger.level
    logging.disable(logging.NOTSET)
    logger.addHandler(sink)
    logger.setLevel(logging.WARNING)
    try:
        yield sink
    finally:
        logger.removeHandler(sink)
        logger.setLevel(previous_level)
        logging.disable(previous_disable)

_PID_EQUIPMENT = pd.DataFrame(
    [
        {"equipment_tag": "K-2410A", "equipment_id": "K-2410A", "normalized_asset": "K-2410A"},
        {"equipment_tag": "P-1310A", "equipment_id": "P-1310A", "normalized_asset": "P-1310A"},
        {"equipment_tag": "P-1310C", "equipment_id": "P-1310C", "normalized_asset": "P-1310C"},
    ]
)


_PID_IDS = set(_PID_EQUIPMENT["equipment_id"])


def _enrich(monkeypatch, rows):
    """Run the enrichment against a fixed P&ID reference."""
    monkeypatch.setattr(tsai, "_load_reference_df", lambda _c: _PID_EQUIPMENT)
    return tsai.enrich_timeseries_asset_identity(
        pd.DataFrame(rows), reference_candidates=["pid"]
    )


def test_match_key_ignores_separators():
    assert tsai._match_key("K-2410A") == tsai._match_key("K2410A") == "K2410A"


def test_a_prefilled_equipment_id_is_relinked_to_the_pid_spelling(monkeypatch):
    out = _enrich(monkeypatch, [{"tag_name": "K2410A.HHPC_Act_Flow", "equipment_id": "K2410A"}])
    assert out.at[0, "equipment_id"] == "K-2410A"
    assert out.at[0, "asset_match_method"] == "source_relinked_to_pid"
    assert out.at[0, "asset_match_confidence"] == 1.0


def test_an_already_correct_id_is_kept_and_marked_verified(monkeypatch):
    out = _enrich(monkeypatch, [{"tag_name": "K-2410A.Flow", "equipment_id": "K-2410A"}])
    assert out.at[0, "equipment_id"] == "K-2410A"
    assert out.at[0, "asset_match_method"] == "source_verified_against_pid"


def test_equipment_absent_from_pid_is_reported_unlinked_not_confident(monkeypatch):
    out = _enrich(monkeypatch, [{"tag_name": "G7510.C1", "equipment_id": "G7510"}])
    assert out.at[0, "equipment_id"] == "G7510"
    assert out.at[0, "asset_match_method"] == "source_only_no_pid_match"
    assert out.at[0, "asset_match_confidence"] < 0.5


def test_a_linked_row_outscores_an_unlinked_one(monkeypatch):
    """The old behaviour scored both 0.9, so a link was indistinguishable from none."""
    out = _enrich(
        monkeypatch,
        [
            {"tag_name": "P1310A.Motor_DE_VIB", "equipment_id": "P1310A"},
            {"tag_name": "G7510.C1", "equipment_id": "G7510"},
        ],
    )
    linked = out[out["equipment_id"].isin(_PID_IDS)]
    unlinked = out[~out["equipment_id"].isin(_PID_IDS)]
    assert len(linked) == 1
    assert len(unlinked) == 1
    assert (
        linked.iloc[0]["asset_match_confidence"]
        > unlinked.iloc[0]["asset_match_confidence"]
    )


def test_every_resolvable_row_joins_the_pid_reference(monkeypatch):
    out = _enrich(
        monkeypatch,
        [
            {"tag_name": "K2410A.HHPC_Act_Flow", "equipment_id": "K2410A"},
            {"tag_name": "P1310A.Motor_DE_VIB", "equipment_id": "P1310A"},
            {"tag_name": "P1310C.Seal_P_DE", "equipment_id": "P1310C"},
            {"tag_name": "G7510.C1", "equipment_id": "G7510"},
        ],
    )
    assert out["equipment_id"].isin(_PID_IDS).sum() == 3


def test_the_nodes_own_identity_spelling_wins_over_a_tag_alias(monkeypatch):
    """HAS_TAG dedupes on normalized_asset, so the tag must adopt that spelling."""
    ref = pd.DataFrame(
        [{"equipment_tag": "K2410A", "equipment_id": "K-2410A", "normalized_asset": "K-2410A"}]
    )
    monkeypatch.setattr(tsai, "_load_reference_df", lambda _c: ref)
    out = tsai.enrich_timeseries_asset_identity(
        pd.DataFrame([{"tag_name": "K2410A.Flow", "equipment_id": "K2410A"}]),
        reference_candidates=["pid"],
    )
    assert out.at[0, "normalized_asset"] == "K-2410A"
    assert out.at[0, "equipment_id"] == "K-2410A"


def test_a_template_derived_normalized_asset_is_not_clobbered_by_a_tag_guess(monkeypatch):
    """Protean derives the asset from its own hierarchy; a tag guess must not overwrite it."""
    monkeypatch.setattr(tsai, "_load_reference_df", lambda _c: pd.DataFrame())
    out = tsai.enrich_timeseries_asset_identity(
        pd.DataFrame([{"tag_name": "K2410A.HHPC_Act_Flow", "normalized_asset": "K-2410A"}]),
        reference_candidates=["pid"],
    )
    assert out.at[0, "normalized_asset"] == "K-2410A"
    assert out.at[0, "asset_match_method"] == "source_asset"


def test_a_missing_normalized_asset_still_falls_back_to_the_tag_pattern(monkeypatch):
    monkeypatch.setattr(tsai, "_load_reference_df", lambda _c: pd.DataFrame())
    out = tsai.enrich_timeseries_asset_identity(
        pd.DataFrame([{"tag_name": "K2410A_Act_Flow"}]),
        reference_candidates=["pid"],
    )
    assert out.at[0, "normalized_asset"] == "K2410A"
    assert out.at[0, "asset_match_method"] == "tag_pattern"


def test_the_tag_pattern_extractor_splits_the_asset_from_its_attribute():
    """A dot separates asset from attribute, so the asset is what survives."""
    assert tsai._extract_tag_asset_key("K2410A_Act_Flow") == "K2410A"
    assert tsai._extract_tag_asset_key("K2410A.HHPC_Act_Flow") == "K2410A"


def test_the_reference_loads_from_parquet(tmp_path):
    """resolve_entity_path prefers .parquet, so the loader has to read it."""
    p = tmp_path / "equipment.parquet"
    _PID_EQUIPMENT.to_parquet(p)
    loaded = tsai._load_reference_df([str(p)])
    assert len(loaded) == len(_PID_EQUIPMENT)
    assert not tsai._build_reference_assets(loaded).empty


def test_the_reference_still_loads_from_csv(tmp_path):
    p = tmp_path / "equipment.csv"
    _PID_EQUIPMENT.to_csv(p, index=False)
    loaded = tsai._load_reference_df([str(p)])
    assert len(loaded) == len(_PID_EQUIPMENT)


def test_a_missing_reference_is_reported_not_swallowed(tmp_path):
    """An absent reference silently disabled all linking; it has to say so."""
    with _warnings_from("p0.ts_asset_identity") as sink:
        loaded = tsai._load_reference_df([str(tmp_path / "absent.parquet")])
    assert loaded.empty
    assert "no P&ID equipment reference loaded" in sink.text


def test_an_unreadable_reference_is_reported_not_swallowed(tmp_path):
    p = tmp_path / "equipment.parquet"
    p.write_text("this is not parquet")
    with _warnings_from("p0.ts_asset_identity") as sink:
        loaded = tsai._load_reference_df([str(p)])
    assert loaded.empty
    assert "could not be read" in sink.text


def test_a_parquet_reference_actually_links_tags(tmp_path):
    p = tmp_path / "equipment.parquet"
    _PID_EQUIPMENT.to_parquet(p)
    out = tsai.enrich_timeseries_asset_identity(
        pd.DataFrame([{"tag_name": "K2410A.HHPC_Act_Flow", "equipment_id": "K2410A"}]),
        reference_candidates=[str(p)],
    )
    assert out.at[0, "equipment_id"] == "K-2410A"


def _shipped_identity_template() -> dict:
    """Load the template the pipeline actually resolves identity_frozen.yaml to."""
    import pathlib

    import yaml

    import p0

    path = (
        pathlib.Path(p0.__file__).parent
        / "config"
        / "templates"
        / "_common"
        / "identity_resolution.yaml"
    )
    return yaml.safe_load(path.read_text())


def test_the_shipped_template_declares_the_cross_reference():
    """Guards the config from being dropped, which would silently restore the old matching."""
    cfg = _shipped_identity_template()
    assert cfg["normalization"]["separator_insensitive"] is True
    assert cfg["cross_reference"]["timeseries"]["identity_column"] == "normalized_asset"


def test_the_shipped_template_drives_the_matcher():
    policy = tsai.matching_policy(_shipped_identity_template())
    assert policy["separator_insensitive"] is True
    assert policy["equipment_id_columns"][0] == "equipment_id"
    assert policy["fuzzy_threshold"] == 0.85


def test_the_template_can_turn_separator_matching_off(monkeypatch):
    """The other rule blocks in these yamls are decorative; this one has to be read."""
    monkeypatch.setattr(tsai, "_load_reference_df", lambda _c: _PID_EQUIPMENT)
    rows = pd.DataFrame([{"tag_name": "K2410A.Flow", "equipment_id": "K2410A"}])
    off = tsai.enrich_timeseries_asset_identity(
        rows.copy(),
        reference_candidates=["pid"],
        identity_cfg={"normalization": {"separator_insensitive": False}},
    )
    on = tsai.enrich_timeseries_asset_identity(
        rows.copy(),
        reference_candidates=["pid"],
        identity_cfg=_shipped_identity_template(),
    )
    assert off.at[0, "asset_match_method"] == "source_only_no_pid_match"
    assert on.at[0, "equipment_id"] == "K-2410A"


def test_a_template_without_a_cross_reference_block_warns():
    with _warnings_from("p0.ts_asset_identity") as sink:
        policy = tsai.matching_policy({})
    assert "declares no cross_reference" in sink.text
    assert policy["separator_insensitive"] is True


def test_an_empty_reference_file_is_reported_not_silently_ignored(tmp_path):
    """The pnid stage writes an empty equipment.parquet on its early return."""
    p = tmp_path / "equipment.parquet"
    pd.DataFrame().to_parquet(p)
    with _warnings_from("p0.ts_asset_identity") as sink:
        tsai.enrich_timeseries_asset_identity(
            pd.DataFrame([{"tag_name": "K2410A.Flow"}]), reference_candidates=[str(p)]
        )
    assert "present but empty" in sink.text


def test_a_reference_without_usable_columns_is_reported(tmp_path):
    p = tmp_path / "equipment.parquet"
    pd.DataFrame([{"something_else": "x"}]).to_parquet(p)
    with _warnings_from("p0.ts_asset_identity") as sink:
        tsai.enrich_timeseries_asset_identity(
            pd.DataFrame([{"tag_name": "K2410A.Flow"}]), reference_candidates=[str(p)]
        )
    assert "none of the columns" in sink.text


def test_an_alias_column_match_still_adopts_the_node_identity(monkeypatch):
    """A prefix-different alias must resolve to the node's own spelling, not the alias."""
    ref = pd.DataFrame(
        [{"normalized_asset": "G7510", "equipment_tag": "GTG-7510", "equipment_id": "G7510"}]
    )
    monkeypatch.setattr(tsai, "_load_reference_df", lambda _c: ref)
    out = tsai.enrich_timeseries_asset_identity(
        pd.DataFrame([{"tag_name": "LPC_Dis_P", "equipment_id": "GTG-7510"}]),
        reference_candidates=["pid"],
    )
    assert out.at[0, "equipment_id"] == "G7510"
    assert out.at[0, "normalized_asset"] == "G7510"
