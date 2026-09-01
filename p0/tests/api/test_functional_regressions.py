"""Defects found by running the API against real infrastructure, not by unit tests."""

from __future__ import annotations

import json
import pathlib
import re

from fastapi import HTTPException

from p0.api.responses import human_message
from p0.api.services import connector_state as cs
from p0.api.services import flow as flow_service

FLOW_ROUTER = (
    pathlib.Path(__file__).resolve().parents[2] / "api" / "routers" / "flow.py"
).read_text()
PLANTS_SRC = (pathlib.Path(__file__).resolve().parents[2] / "api" / "plants.py").read_text()


def test_timeseries_connector_spans_both_flows():
    """Values files live under ts_values — the rollup reported 1 file instead of 6."""
    assert cs.flow_names("timeseries") == ("ts", "ts_values")
    assert cs.flow_names("documents") == ("docs",)
    assert cs.flow_names("pnid") == ("pnid",)
    assert cs.flow_names("sap") == ("sap",)


def test_unknown_connector_has_no_flows():
    assert cs.flow_names("nonsense") == ()
    assert cs.flow_name("nonsense") is None


def test_flow_name_still_returns_the_primary_flow():
    assert cs.flow_name("timeseries") == "ts"


def test_rollup_query_matches_any_of_the_flows():
    source = pathlib.Path(
        pathlib.Path(__file__).resolve().parents[2] / "api" / "services" / "connector_state.py"
    ).read_text()
    assert "flow = ANY(%s)" in source


def test_json_columns_are_serialised_before_write():
    """sheet_names is a JSON column; psycopg2 adapts a bare list to text[] and the write fails."""
    assert "sheet_names" in flow_service._JSON_COLS
    source = pathlib.Path(
        pathlib.Path(__file__).resolve().parents[2] / "api" / "services" / "flow.py"
    ).read_text()
    assert "json.dumps(write[_json_col])" in source


def test_category_is_backfilled_for_rows_that_predate_the_column():
    """A NULL category would re-insert instead of updating, duplicating every old file."""
    assert "_backfill_flow_category" in PLANTS_SRC
    assert "WHERE category IS NULL" in PLANTS_SRC


def test_http_exceptions_keep_their_status_code():
    """A 400 from validate_plant_code was being swallowed into a generic 500."""
    handlers = re.findall(
        r"except HTTPException as e:\n\s*raise as_envelope_exc\(e\)\n\s*except Exception as exc:",
        FLOW_ROUTER,
    )
    bare = re.findall(r"(?<!raise as_envelope_exc\(e\)\n)    except Exception as exc:", FLOW_ROUTER)
    assert len(handlers) >= 4, "flow endpoints must re-raise HTTPException before the catch-all"


def test_human_message_preserves_an_http_exception_detail():
    exc = HTTPException(400, "plant_code_id 'X' is not registered. Add it via POST /plants first.")
    message = human_message(exc, action="listing state")
    assert "is not registered" in message
    assert "unexpected error" not in message


def test_human_message_handles_a_dict_detail():
    exc = HTTPException(400, {"success": False, "message": "Validation failed"})
    assert human_message(exc) == "Validation failed"


def test_generic_exceptions_still_get_the_safe_message():
    message = human_message(RuntimeError("0xdeadbeef"), action="listing state")
    assert "0xdeadbeef" not in message
    assert "listing state" in message


def test_config_router_imports_the_plant_validator():
    """getUserConfig and patchUserConfig used _validate_plant_code without importing it."""
    source = pathlib.Path(
        pathlib.Path(__file__).resolve().parents[2] / "api" / "routers" / "config_router.py"
    ).read_text()
    assert source.count("validate_plant_code as _validate_plant_code") == 2, (
        "both the package and non-package import branches must provide it"
    )


def test_every_router_name_it_calls_is_importable():
    """A NameError at request time is invisible to static tests — import each router."""
    import importlib

    for module in (
        "p0.api.routers.config_router",
        "p0.api.routers.connectors",
        "p0.api.routers.context",
        "p0.api.routers.flow",
        "p0.api.routers.pipeline",
    ):
        importlib.import_module(module)


def test_no_router_uses_a_variable_it_never_binds():
    """NameError at request time is invisible to import checks and to static string tests."""
    import ast

    watch = (
        "upload_batch_id", "_commit_uid", "_idem_fingerprint", "_meta", "_dupe",
        "prefix", "dest_path", "job_id", "flow_row", "detail", "plant_code_id", "rows",
    )
    offenders = []
    for path in sorted((pathlib.Path(__file__).resolve().parents[2] / "api" / "routers").glob("*.py")):
        tree = ast.parse(path.read_text())
        for fn in [n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]:
            bound = {a.arg for a in fn.args.args + fn.args.kwonlyargs + fn.args.posonlyargs}
            for node in ast.walk(fn):
                if isinstance(node, ast.Assign):
                    for t in node.targets:
                        if isinstance(t, ast.Name):
                            bound.add(t.id)
                        elif isinstance(t, ast.Tuple):
                            bound |= {e.id for e in t.elts if isinstance(e, ast.Name)}
                elif isinstance(node, (ast.For, ast.AnnAssign)) and isinstance(
                    getattr(node, "target", None), ast.Name
                ):
                    bound.add(node.target.id)
                elif isinstance(node, ast.For) and isinstance(node.target, ast.Tuple):
                    bound |= {e.id for e in node.target.elts if isinstance(e, ast.Name)}
                elif isinstance(node, ast.ExceptHandler) and node.name:
                    bound.add(node.name)
                elif isinstance(node, ast.withitem) and isinstance(node.optional_vars, ast.Name):
                    bound.add(node.optional_vars.id)
                elif isinstance(node, (ast.Import, ast.ImportFrom)):
                    bound |= {(a.asname or a.name).split(".")[0] for a in node.names}
                elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    bound.add(node.name)
                    # nested helpers bring their own parameters into scope
                    bound |= {
                        a.arg
                        for a in node.args.args + node.args.kwonlyargs + node.args.posonlyargs
                    }
                elif isinstance(node, ast.Lambda):
                    bound |= {a.arg for a in node.args.args}
                elif isinstance(node, ast.comprehension) and isinstance(node.target, ast.Name):
                    bound.add(node.target.id)
            for name in watch:
                used = any(
                    isinstance(n, ast.Name) and n.id == name and isinstance(n.ctx, ast.Load)
                    for n in ast.walk(fn)
                )
                if used and name not in bound:
                    offenders.append(f"{path.name}:{fn.name} uses '{name}' without binding it")
    assert not offenders, "\n".join(offenders)


def test_combined_upload_passes_every_form_parameter_of_its_targets():
    """Calling a FastAPI handler directly leaves unpassed Form defaults as Form objects."""
    import ast

    path = pathlib.Path(__file__).resolve().parents[2] / "api" / "routers" / "connectors.py"
    tree = ast.parse(path.read_text())
    targets = {}
    for fn in [n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]:
        if fn.name.startswith("upload") and fn.name != "uploadConnectorFiles":
            defaults = [None] * (len(fn.args.args) - len(fn.args.defaults)) + list(fn.args.defaults)
            targets[fn.name] = {
                a.arg
                for a, d in zip(fn.args.args, defaults)
                if isinstance(d, ast.Call) and getattr(d.func, "id", "") in ("Form", "File")
            }

    dispatcher = next(n for n in tree.body if getattr(n, "name", "") == "uploadConnectorFiles")
    for call in [n for n in ast.walk(dispatcher) if isinstance(n, ast.Call)]:
        name = getattr(getattr(call, "func", None), "id", "")
        if name in targets:
            missing = targets[name] - {k.arg for k in call.keywords}
            assert not missing, f"{name}: dispatcher does not pass {sorted(missing)}"
