"""A pnid commit says how many rows did not land, and never quotes the raw database error."""

from __future__ import annotations

import ast
import pathlib

SOURCE = (
    pathlib.Path(__file__).resolve().parents[2] / "api" / "routers" / "context.py"
).read_text(encoding="utf-8")


def _commit_pnid() -> ast.FunctionDef:
    for node in ast.walk(ast.parse(SOURCE)):
        if isinstance(node, ast.FunctionDef) and node.name == "commitPnidContext":
            return node
    raise AssertionError("commitPnidContext not found")


def _reported_keys() -> set[str]:
    keys: set[str] = set()
    for node in ast.walk(_commit_pnid()):
        if not isinstance(node, ast.Dict):
            continue
        literal = {
            k.value
            for k in node.keys
            if isinstance(k, ast.Constant) and isinstance(k.value, str)
        }
        if "table" in literal and "rows_written" in literal:
            keys |= literal
    return keys


def test_the_commit_reports_rows_that_did_not_land():
    assert "rows_dropped" in _reported_keys()


def test_the_commit_still_reports_what_was_written():
    assert {"table", "rows_written"} <= _reported_keys()


def test_the_failure_branch_does_not_hand_back_the_raw_exception():
    for node in ast.walk(_commit_pnid()):
        if not isinstance(node, ast.Dict):
            continue
        for key, value in zip(node.keys, node.values):
            if not (isinstance(key, ast.Constant) and key.value == "warning"):
                continue
            assert not (
                isinstance(value, ast.Call)
                and isinstance(value.func, ast.Name)
                and value.func.id == "str"
            ), "the warning must be a human_message, not str(exc)"


def test_the_failure_branch_redacts_through_human_message():
    found = False
    for node in ast.walk(_commit_pnid()):
        if not isinstance(node, ast.Dict):
            continue
        for key, value in zip(node.keys, node.values):
            if (
                isinstance(key, ast.Constant)
                and key.value == "warning"
                and isinstance(value, ast.Call)
                and isinstance(value.func, ast.Name)
                and value.func.id == "human_message"
            ):
                found = True
    assert found, "no warning built by human_message"
