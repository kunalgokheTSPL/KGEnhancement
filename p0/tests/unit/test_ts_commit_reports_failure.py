"""A commit that wrote nothing must not answer success, nor be cached as one."""

from __future__ import annotations

import ast
import pathlib

SOURCE = (
    pathlib.Path(__file__).resolve().parents[2] / "api" / "routers" / "context.py"
).read_text(encoding="utf-8")


def _commit_ts() -> ast.FunctionDef:
    for node in ast.walk(ast.parse(SOURCE)):
        if isinstance(node, ast.FunctionDef) and node.name == "commitTsContext":
            return node
    raise AssertionError("commitTsContext not found")


def _guard() -> ast.If:
    for node in ast.walk(_commit_ts()):
        if (
            isinstance(node, ast.If)
            and isinstance(node.test, ast.Name)
            and node.test.id == "_ts_commit_error"
        ):
            for inner in ast.walk(node):
                if isinstance(inner, ast.Return):
                    return node
    raise AssertionError("no early return guarded by _ts_commit_error")


def test_a_failed_commit_returns_success_false():
    guard = _guard()
    literals = [
        n.value
        for n in ast.walk(guard)
        if isinstance(n, ast.Constant) and isinstance(n.value, bool)
    ]
    assert False in literals
    assert True not in literals


def test_a_failed_commit_carries_a_structured_error():
    guard = _guard()
    keys = [
        n.value
        for n in ast.walk(guard)
        if isinstance(n, ast.Constant) and isinstance(n.value, str)
    ]
    assert "errors" in keys
    assert "field" in keys
    assert "message" in keys


def test_a_failed_commit_is_not_remembered_as_idempotent_success():
    guard = _guard()
    remembered = [
        n
        for n in ast.walk(guard)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Attribute)
        and n.func.attr == "remember"
    ]
    assert remembered == []


def test_the_failure_guard_precedes_the_success_envelope():
    fn = _commit_ts()
    guard_line = _guard().lineno
    remembers = [
        n.lineno
        for n in ast.walk(fn)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Attribute)
        and n.func.attr == "remember"
    ]
    assert remembers
    assert all(guard_line < line for line in remembers)


def test_the_commit_error_shown_to_a_caller_is_not_the_raw_exception():
    fn = _commit_ts()
    assigns = [
        n
        for n in ast.walk(fn)
        if isinstance(n, ast.Assign)
        and any(
            isinstance(t, ast.Name) and t.id == "_ts_commit_error" for t in n.targets
        )
    ]
    calls = [
        n.func.id
        for a in assigns
        for n in ast.walk(a)
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
    ]
    assert "human_message" in calls
    assert "str" not in calls


def test_a_connection_string_in_the_error_never_reaches_the_caller():
    from p0.api.responses import human_message

    leaked = "postgres://svc:S3cr3t@10.0.0.4:5432/db"
    shown = human_message(Exception(f"could not connect to server: {leaked}"))
    assert "S3cr3t" not in shown
    assert "svc" not in shown
