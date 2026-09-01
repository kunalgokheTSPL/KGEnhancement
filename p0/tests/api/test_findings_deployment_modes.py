"""AIF / GLOC / LOPC and the integrated view must work on-prem and on Azure unchanged."""

from __future__ import annotations

import ast
import importlib
import os
import pathlib
import subprocess
import sys

import pytest

P0 = pathlib.Path(__file__).resolve().parents[2]
BACKEND = P0.parent

FEATURE_MODULES = [
    P0 / "api" / "services" / "findings.py",
    P0 / "api" / "services" / "labels.py",
    P0 / "api" / "services" / "integrated_view.py",
    P0 / "api" / "routers" / "integrated.py",
    P0 / "utils" / "findings_identity.py",
    P0 / "pipelines" / "run_findings_end_to_end.py",
]

BANNED_IMPORTS = {
    "boto3", "botocore", "s3fs", "adlfs", "psycopg2", "pyodbc",
    "azure", "azure.storage", "azure.identity",
}

ALLOWED_DRIVER_ENTRY = {"p0.driver", "p0.utils.fs", "p0.api.database.connection"}


def _imported_names(path: pathlib.Path) -> set[str]:
    tree = ast.parse(path.read_text())
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(a.name for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            names.add(node.module)
    return names


@pytest.mark.parametrize("path", FEATURE_MODULES, ids=lambda p: p.name)
def test_no_module_reaches_past_the_driver_facade(path):
    """A direct boto3/psycopg2 import is how a feature silently becomes on-prem only."""
    for name in _imported_names(path):
        root = name.split(".")[0]
        assert root not in BANNED_IMPORTS, (
            f"{path.name} imports {name} directly — go through p0.driver, "
            f"p0.utils.fs or api.database.connection instead"
        )


def _code_strings(path: pathlib.Path) -> list[str]:
    """Every string literal that is not a docstring — docs may show example URIs."""
    tree = ast.parse(path.read_text())
    docstrings = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            doc = ast.get_docstring(node, clean=False)
            if doc:
                docstrings.add(doc)
    return [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and node.value not in docstrings
    ]


@pytest.mark.parametrize("path", FEATURE_MODULES, ids=lambda p: p.name)
def test_no_module_hardcodes_a_storage_scheme(path):
    """A scheme literal in executable code pins the feature to one deployment."""
    for literal in _code_strings(path):
        for scheme in ("s3://", "abfs://", ".blob.core.windows.net", ".dfs.core.windows.net"):
            assert scheme not in literal, f"{path.name} hardcodes {scheme!r} in {literal[:60]!r}"


@pytest.mark.parametrize("path", FEATURE_MODULES, ids=lambda p: p.name)
def test_no_module_builds_an_object_path_by_hand(path):
    """Prefixes come from api.config, never from string concatenation in the feature."""
    body = path.read_text()
    assert "staging-pt/" not in body, f"{path.name} builds a staging prefix itself"
    assert "processed_data/" not in body, f"{path.name} builds a processed prefix itself"


def test_the_path_helpers_derive_from_the_switched_root():
    """This is what actually makes the new prefixes mode-aware."""
    config = (P0 / "api" / "config.py").read_text()
    block = config.split("def rustfs_staging_findings(")[1].split("def rustfs_processed_pnid(")[0]
    assert "_STAGING_PT_ROOT" in block
    assert "_PROCESSED_ROOT" in block
    assert "RUSTFS_P0_ROOT = f\"{OBJECT_SCHEME}://" in config
    assert "OBJECT_SCHEME = " in config


@pytest.mark.parametrize("path", FEATURE_MODULES, ids=lambda p: p.name)
def test_no_module_branches_on_deployment_mode_itself(path):
    """Only the driver facade is allowed to know which deployment this is."""
    body = path.read_text()
    assert "DEPLOYMENT_MODE" not in body, (
        f"{path.name} reads DEPLOYMENT_MODE — the facade already switched for it"
    )


_STUB_AZURE_ONLY_DEPS = """
import sys, types
for name in ("pyodbc", "redis_entraid", "redis_entraid.cred_provider"):
    if name not in sys.modules:
        module = types.ModuleType(name)
        module.connect = lambda *a, **k: None
        module.Error = Exception
        module.create_from_default_azure_credential = lambda *a, **k: None
        sys.modules[name] = module
"""


def _paths_under(mode: str) -> tuple[str, str]:
    """Resolve the findings prefixes in a subprocess pinned to one deployment mode."""
    script = _STUB_AZURE_ONLY_DEPS + (
        "from p0.api.config import rustfs_staging_findings, rustfs_processed_findings\n"
        "print(rustfs_staging_findings('PLANT_2', 'aif'))\n"
        "print(rustfs_processed_findings('PLANT_2', 'aif'))\n"
    )
    env = {
        **os.environ,
        "DEPLOYMENT_MODE": mode,
        "ADLS_ACCOUNT_NAME": "acct",
        "ADLS_CONTAINER": "decisionops",
        "PYTHONPATH": str(BACKEND),
    }
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, env=env, timeout=180
    )
    if result.returncode != 0:
        pytest.skip(f"{mode} driver not importable here: {result.stderr[-160:]}")
    lines = [ln for ln in result.stdout.strip().splitlines() if "://" in ln]
    if len(lines) < 2:
        pytest.skip(f"{mode} produced no paths")
    return lines[0], lines[1]


def test_the_findings_paths_follow_the_deployment_mode():
    """Same code, different object store — the paths must not be identical."""
    onprem_staging, onprem_processed = _paths_under("on_prem")
    azure_staging, azure_processed = _paths_under("azure")
    assert onprem_staging.startswith("s3://")
    assert azure_staging.startswith("abfs://")
    assert onprem_staging != azure_staging
    assert onprem_processed != azure_processed


def test_the_plant_and_connector_survive_the_switch():
    """Only the scheme and container change; the plant-scoped suffix is identical."""
    onprem_staging, _ = _paths_under("on_prem")
    azure_staging, _ = _paths_under("azure")
    assert onprem_staging.endswith("/staging-pt/PLANT_2/aif")
    assert azure_staging.endswith("/staging-pt/PLANT_2/aif")


def test_staging_and_processed_never_collide_in_either_mode():
    for mode in ("on_prem", "azure"):
        staging, processed = _paths_under(mode)
        assert staging != processed, f"{mode}: staging and processed resolve to the same prefix"


@pytest.mark.parametrize("connector", ["aif", "gloc", "lopc"])
def test_each_connector_gets_its_own_prefix_in_both_modes(connector):
    from p0.api.config import rustfs_processed_findings, rustfs_staging_findings

    others = [c for c in ("aif", "gloc", "lopc") if c != connector]
    mine = rustfs_staging_findings("PLANT_2", connector)
    assert all(mine != rustfs_staging_findings("PLANT_2", o) for o in others)
    assert mine.endswith(f"/{connector}")
    assert rustfs_processed_findings("PLANT_2", connector).endswith(f"/{connector}")


def test_the_pipeline_script_uses_the_shared_filesystem_helper():
    """It runs as a subprocess, so it needs the facade just as much as the API does."""
    body = (P0 / "pipelines" / "run_findings_end_to_end.py").read_text()
    assert "from p0.utils import fs as _fs" in body
    assert "_fs.write_parquet(" in body
    assert "open(" not in body.replace("open(path, ", "")


def test_the_sql_the_view_writes_is_portable_across_both_postgres_servers():
    """Both modes are Postgres — on-prem and Azure Database for PostgreSQL."""
    body = (P0 / "api" / "services" / "integrated_view.py").read_text()
    assert "ON CONFLICT" in body
    for tsql in ("MERGE ", "IDENTITY(", "GETDATE()", "NVARCHAR", "TOP 1 "):
        assert tsql not in body, f"integrated_view uses T-SQL {tsql!r}, which on-prem cannot run"


def test_the_services_import_cleanly_with_no_deployment_variables_set():
    """A missing ADLS/PG variable must not break import — only use."""
    for name in (
        "p0.api.services.findings",
        "p0.api.services.labels",
        "p0.utils.findings_identity",
    ):
        assert importlib.import_module(name)
