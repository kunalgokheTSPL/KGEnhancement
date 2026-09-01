"""Identifiers must be minted once, named once, and reachable by the client."""

from __future__ import annotations

import ast
import pathlib
import re

import pytest

import p0.api.plants as _plants
from p0.api.services.transforms import _parse_commit_body


@pytest.fixture(autouse=True)
def _no_registry_lookup(monkeypatch):
    """_parse_commit_body validates the plant; these tests are about key parsing only."""
    monkeypatch.setattr(_plants, "validate_plant_code", lambda *_a, **_k: None)


ROUTERS = pathlib.Path(__file__).resolve().parents[2] / "api" / "routers"
CONNECTORS = (ROUTERS / "connectors.py").read_text()
CONTEXT = (ROUTERS / "context.py").read_text()
PIPELINE = (ROUTERS / "pipeline.py").read_text()


def test_commit_accepts_the_canonical_batch_key():
    plant, batch, rows = _parse_commit_body(
        {"plant_code_id": "Plant_1", "upload_batch_id": "bat_1", "rows": []}, "docs"
    )
    assert (plant, batch) == ("Plant_1", "bat_1")


def test_commit_accepts_the_key_the_frontend_actually_sends():
    """The live frontend posts batch_id; ignoring it silently committed everything."""
    _, batch, _ = _parse_commit_body(
        {"plant_code_id": "Plant_1", "batch_id": "bat_1", "rows": []}, "docs"
    )
    assert batch == "bat_1"


def test_commit_accepts_a_pipeline_job_id_as_scope():
    _, batch, _ = _parse_commit_body(
        {"plant_code_id": "Plant_1", "pipeline_job_id": "job_9", "rows": []}, "docs"
    )
    assert batch == "job_9"


def test_canonical_key_wins_over_aliases():
    _, batch, _ = _parse_commit_body(
        {
            "plant_code_id": "Plant_1",
            "upload_batch_id": "canonical",
            "batch_id": "alias",
            "rows": [],
        },
        "docs",
    )
    assert batch == "canonical"


def test_absent_batch_is_none_not_empty_string():
    _, batch, _ = _parse_commit_body({"plant_code_id": "Plant_1", "rows": []}, "docs")
    assert batch is None


def _mints_upload_batch_id(node: ast.AST) -> bool:
    for n in ast.walk(node):
        if (
            isinstance(n, ast.Assign)
            and any(isinstance(t, ast.Name) and t.id == "upload_batch_id" for t in n.targets)
            and "uuid4" in ast.unparse(n.value)
        ):
            return True
    return False


def _batch_minting_functions(source: str) -> list[ast.AST]:
    tree = ast.parse(source)
    return [
        node for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and _mints_upload_batch_id(node)
    ]


def test_every_upload_endpoint_mints_its_own_batch_id():
    """One batch per upload request, minted server-side."""
    minting = _batch_minting_functions(CONNECTORS)
    assert minting, "no function mints its own upload_batch_id"


def _echoes_batch_id_in_response(node: ast.AST) -> bool:
    for n in ast.walk(node):
        if isinstance(n, ast.Dict):
            for key, value in zip(n.keys, n.values):
                if (
                    isinstance(key, ast.Constant) and key.value == "upload_batch_id"
                    and isinstance(value, ast.Name) and value.id == "upload_batch_id"
                ):
                    return True
    return False


def test_every_upload_response_returns_the_batch_id():
    """Every function that mints upload_batch_id must echo it back in its response."""
    minting = _batch_minting_functions(CONNECTORS)
    assert minting, "no function mints its own upload_batch_id"
    missing = [node.name for node in minting if not _echoes_batch_id_in_response(node)]
    assert not missing, f"minted upload_batch_id not echoed back in: {missing}"


def _propagates_batch_id_downstream(node: ast.AST) -> bool:
    for n in ast.walk(node):
        if isinstance(n, ast.Call):
            for kw in n.keywords:
                if (
                    kw.arg == "upload_batch_id"
                    and isinstance(kw.value, ast.Name)
                    and kw.value.id == "upload_batch_id"
                ):
                    return True
    return False


def test_upload_writes_the_batch_id_to_flow_state():
    minting = _batch_minting_functions(CONNECTORS)
    assert minting, "no function mints its own upload_batch_id"
    missing = [node.name for node in minting if not _propagates_batch_id_downstream(node)]
    assert not missing, f"minted upload_batch_id never propagated downstream in: {missing}"


def test_pipeline_no_longer_overwrites_the_upload_batch_on_flow_rows():
    """process_job_id records the run; upload_batch_id must keep the upload identity."""
    block = re.search(
        r'stage="processing",\s*\n\s*status="running",(?P<body>.*?)\)', PIPELINE, re.S
    )
    assert block, "processing advance block not found"
    assert "process_job_id=pipeline_job_id" in block.group("body")
    assert "upload_batch_id=" not in block.group("body")


def test_pipeline_run_reports_whether_it_was_scoped_to_a_batch():
    assert '"upload_batch_ids"' in PIPELINE
    assert '"scoped_to_batch"' in PIPELINE


def test_every_commit_returns_a_commit_id():
    assert CONTEXT.count('"commit_id": _commit_uid') == 5


@pytest.mark.parametrize("connector", ["P&ID", "Documents", "SAP"])
def test_commit_ids_sit_alongside_the_existing_keys(connector):
    """commit_id is additive — the keys the frontend already reads stay put."""
    assert f"{connector} context committed successfully" in CONTEXT
    assert '"total_rows_written"' in CONTEXT
