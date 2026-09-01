"""One connector-parameterised endpoint per operation, alongside the existing per-connector ones."""

from __future__ import annotations

import pathlib

import pytest

from p0.api.services.taxonomy import resolve_source as _resolve_source

CONTEXT = (
    pathlib.Path(__file__).resolve().parents[2] / "api" / "routers" / "context.py"
).read_text()
CONNECTORS = (
    pathlib.Path(__file__).resolve().parents[2] / "api" / "routers" / "connectors.py"
).read_text()


@pytest.mark.parametrize(
    "given, expected",
    [
        ("pnid", "pnid"), ("P&ID", "pnid"), ("pid", "pnid"),
        ("documents", "docs"), ("docs", "docs"), ("Document", "docs"),
        ("timeseries", "ts"), ("ts", "ts"), ("time_series", "ts"),
        ("sap", "sap"), ("SAP", "sap"), ("  sap  ", "sap"),
    ],
)
def test_connector_names_and_internal_names_both_resolve(given, expected):
    assert _resolve_source(given) == expected


@pytest.mark.parametrize("given", ["nonsense", "", None, "p1", "excel"])
def test_unknown_connectors_do_not_resolve(given):
    assert _resolve_source(given) is None


def test_combined_context_endpoints_exist():
    assert '"/get"' in CONTEXT
    assert '"/commit"' in CONTEXT
    assert '"/rows"' in CONTEXT


def test_combined_upload_endpoint_exists():
    assert '"/upload"' in CONNECTORS
    assert "async def uploadConnectorFiles(" in CONNECTORS


def test_the_per_connector_endpoints_are_untouched():
    """The frontend must not have to change anything before the deadline."""
    for path in (
        '"/getPnidContext"', '"/getDocsContext"', '"/getTsContext"', '"/getSapContext"',
        '"/commitPnidContext"', '"/commitDocsContext"', '"/commitTsContext"', '"/commitSapContext"',
        '"/patchPnidRows"', '"/patchDocsRows"', '"/patchTsRows"', '"/patchSapRows"',
    ):
        assert path in CONTEXT, f"{path} must remain available"
    for path in ('"/pnid/upload"', '"/documents/upload"', '"/sap/upload"', '"/timeseries/upload"'):
        assert path in CONNECTORS, f"{path} must remain available"


def test_combined_endpoints_delegate_rather_than_duplicate():
    """They must call the existing handlers, so behaviour cannot drift between the two paths."""
    block = CONTEXT.split("def getContext(")[1].split("@router")[0]
    for handler in ("getPnidContext(", "getDocsContext(", "getTsContext(", "getSapContext("):
        assert handler in block

    block = CONTEXT.split("def commitContext(")[1].split("@router")[0]
    for handler in ("commitPnidContext(", "commitDocsContext(", "commitTsContext(", "commitSapContext("):
        assert handler in block

    block = CONTEXT.split("def patchContextRows(")[1].split("@router")[0]
    for handler in ("patchPnidRows(", "patchDocsRows(", "patchTsRows(", "patchSapRows("):
        assert handler in block

    block = CONNECTORS.split("async def uploadConnectorFiles(")[1]
    for handler in (
        "uploadPnidFiles(", "uploadDocumentFiles(", "uploadSapFiles(", "uploadTimeseriesFiles("
    ):
        assert handler in block


def test_pnid_context_is_not_passed_pagination_it_does_not_accept():
    """getPnidContext returns two arrays, not rows[] — passing page would TypeError."""
    block = CONTEXT.split('if source == "pnid":')[1].split("if source ==")[0]
    assert "page=page" not in block


def test_combined_commit_forwards_the_idempotency_key():
    block = CONTEXT.split("def commitContext(")[1].split("@router")[0]
    assert block.count("idempotency_key=idempotency_key") == 6


def test_documents_upload_requires_a_document_type():
    block = CONNECTORS.split("async def uploadConnectorFiles(")[1]
    assert "document_type is required" in block


def test_invalid_connector_returns_422_naming_the_valid_ones():
    for source in (CONTEXT, CONNECTORS):
        assert "is not a valid connector" in source
        assert "pnid, documents, timeseries, sap" in source


def test_combined_get_is_a_superset_of_every_individual_endpoint():
    """He must never hit a scoping param the combined endpoint cannot express."""
    import ast

    tree = ast.parse(CONTEXT)
    fns = {
        n.name: n
        for n in tree.body
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    combined = {a.arg for a in fns["getContext"].args.args} - {"connector"}
    for name in ("getPnidContext", "getDocsContext", "getTsContext", "getSapContext"):
        gap = {a.arg for a in fns[name].args.args} - combined
        assert not gap, f"{name} accepts {sorted(gap)} which /context/get cannot express"


def test_combined_get_forwards_the_connector_specific_params():
    block = CONTEXT.split("def getContext(")[1].split("@router")[0]
    assert "files=files" in block
    assert "doc_types=doc_types" in block
    assert "source_file=source_file" in block
    assert "table=table" in block
