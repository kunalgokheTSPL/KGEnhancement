"""Review and commit surface for AIF / GLOC / LOPC."""

from __future__ import annotations

import pathlib

from p0.api.services import findings, taxonomy

CONTEXT = (
    pathlib.Path(__file__).resolve().parents[2] / "api" / "routers" / "context.py"
).read_text()

FINDINGS = findings.CONNECTORS


def test_the_three_endpoints_exist():
    for name in ("getFindingsContext", "patchFindingsRows", "commitFindingsContext"):
        assert f"def {name}(" in CONTEXT


def test_each_connector_resolves_to_its_own_source():
    for connector in FINDINGS:
        assert taxonomy.resolve_source(connector) == connector


def test_an_unknown_connector_still_resolves_to_nothing():
    assert taxonomy.resolve_source("nonsense") is None


def test_the_combined_get_routes_findings_before_falling_through_to_sap():
    block = CONTEXT.split("def getContext(")[1].split("def commitContext(")[0]
    assert "_findings.is_findings_connector(source)" in block
    assert block.index("is_findings_connector") < block.index("return getSapContext(")


def test_the_combined_commit_and_patch_route_findings_too():
    commit = CONTEXT.split("def commitContext(")[1].split("def patchContextRows(")[0]
    patch = CONTEXT.split("def patchContextRows(")[1]
    assert "commitFindingsContext(" in commit
    assert "patchFindingsRows(" in patch


def test_the_combined_get_accepts_every_findings_filter():
    """A filter the individual endpoint takes must be expressible on the combined one."""
    combined = CONTEXT.split("def getContext(")[1].split("):")[0]
    individual = CONTEXT.split("def getFindingsContext(")[1].split("):")[0]
    for parameter in ("upload_batch_id", "source_file", "unmatched_only", "page", "page_size"):
        assert parameter in individual, f"{parameter} missing from getFindingsContext"
        assert parameter in combined, f"{parameter} missing from the combined getContext"


def test_commit_replaces_by_the_natural_key_so_re_uploads_do_not_duplicate():
    block = CONTEXT.split("def commitFindingsContext(")[1]
    assert '_replace_by_natural_key(' in block
    assert '"source_record_id"' in block


def test_commit_uses_the_two_value_idempotency_contract():
    block = CONTEXT.split("def commitFindingsContext(")[1]
    assert "_replayed, _conflict = _idempotency.lookup(" in block
    assert "body_mismatch" in block


def test_commit_writes_only_columns_the_canonical_table_declares():
    """Review adds display-only columns; those must not reach the insert."""
    assert "def _findings_db_row(" in CONTEXT
    assert "_findings_db_row(name, r)" in CONTEXT


def test_every_findings_table_declares_source_record_id():
    from p0.api.deps import load_yaml
    from p0.api.config import SCHEMA_FROZEN_FILE

    schema = load_yaml(str(SCHEMA_FROZEN_FILE))
    tables = schema.get("schema") or schema.get("tables") or {}
    for connector in FINDINGS:
        table = findings.target_table(connector)
        columns = (tables.get(table) or {}).get("columns") or {}
        assert "source_record_id" in columns, f"{table} has no source_record_id"
        assert "labels" in columns, f"{table} has no labels"
        assert "asset_match_confidence" in columns, f"{table} records no confidence"


def test_a_long_natural_key_is_bounded_to_the_column_width():
    """Composite keys built from titles overflow VARCHAR(200) and the row is lost."""
    row = {
        "reference_no": "UAUC/SKA/2023/160672",
        "event_date": "2023-12-22",
        "incident_title": "Discovered condensate release at the liquid surge pump " * 6,
    }
    key = findings.source_record_id("lopc", row)
    assert len(key) <= findings.MAX_RECORD_ID


def test_bounded_keys_stay_distinct_and_stable():
    base = "x" * 400
    a = findings.source_record_id("lopc", {"incident_title": base})
    b = findings.source_record_id("lopc", {"incident_title": base + "different"})
    assert a != b
    assert a == findings.source_record_id("lopc", {"incident_title": base})


def test_deduped_ids_are_bounded_too():
    rows = [{"incident_title": "y" * 400}] * 3
    ids = findings.assign_record_ids("lopc", rows)
    assert len(set(ids)) == 3
    assert all(len(i) <= findings.MAX_RECORD_ID for i in ids)
