"""
STORAGE_BACKEND switches the whole object-store fabric: rustfs (s3://) or adls (abfs://).

The backend is an infrastructure decision made in env.enc — one variable, and every
path helper, filesystem handle and presigned URL follows. These tests run config in a
subprocess so the import-time constants are evaluated under the environment being
tested, exactly as a real deployment would.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import importlib.util
import pytest

BACKEND = Path(__file__).resolve().parents[3]

def _pyodbc_usable() -> bool:
    """True only when pyodbc imports, not merely when it is installed."""
    try:
        importlib.import_module("pyodbc")
    except Exception:
        return False
    return True


_needs_azure_driver = pytest.mark.skipif(
    not _pyodbc_usable(),
    reason="azure driver stack (utility.drivers.azure_*) requires a working pyodbc",
)


def _probe(extra_env: dict, code: str) -> str:
    # SKIP_KEYVAULT: these tests exercise config resolution from env, not the live vault.
    env = {**os.environ, "SKIP_KEYVAULT": "1", **extra_env}
    r = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True,
        env=env, cwd=BACKEND, timeout=120,
    )
    assert r.returncode == 0, r.stderr[-800:]
    return r.stdout.strip().splitlines()[-1]


_PATHS_CODE = """
import sys; sys.path.insert(0, ".")
from p0.api import config as c
print("|".join([c.OBJECT_SCHEME, c.RUSTFS_BUCKET, c.RUSTFS_P0_ROOT, c.rustfs_out_dir("plant_1_testcase"), c.rustfs_staging_pnid("plant_1_testcase")]))
"""


def test_default_backend_is_rustfs_with_s3_paths():
    out = _probe({"STORAGE_BACKEND": ""}, _PATHS_CODE)
    scheme, bucket, root, out_dir, staging = out.split("|")
    assert scheme == "s3"
    assert root == f"s3://{bucket}/p0"
    assert out_dir.startswith("s3://") and staging.startswith("s3://")


@_needs_azure_driver
def test_deployment_mode_azure_flips_storage_without_a_second_variable():
    """The whole point: one variable. DEPLOYMENT_MODE=azure alone selects ADLS —
    no separate STORAGE_BACKEND needed."""
    out = _probe(
        {"DEPLOYMENT_MODE": "azure", "STORAGE_BACKEND": "", "ADLS_ACCOUNT_NAME": "acct", "ADLS_CONTAINER": "decisionops"},
        _PATHS_CODE,
    )
    scheme, bucket, root, out_dir, staging = out.split("|")
    assert scheme == "abfs"
    assert bucket == "decisionops"
    assert root == "abfs://decisionops/p0"
    assert out_dir.startswith("abfs://decisionops/") and staging.startswith("abfs://decisionops/")
    assert "s3://" not in out


@_needs_azure_driver
def test_adls_backend_returns_an_azure_filesystem():
    out = _probe(
        {"STORAGE_BACKEND": "adls", "ADLS_ACCOUNT_NAME": "acct", "ADLS_ACCOUNT_KEY": "a2V5"},
        """
import sys; sys.path.insert(0, ".")
from p0.drivers.azure_driver import get_object_fs
print(type(get_object_fs()).__name__)
""",
    )
    assert out == "AzureBlobFileSystem"


@_needs_azure_driver
def test_adls_without_account_name_fails_loudly():
    out = _probe(
        {"STORAGE_BACKEND": "adls", "ADLS_ACCOUNT_NAME": ""},
        """
import sys; sys.path.insert(0, ".")
from p0.drivers.azure_driver import get_object_fs
try:
    get_object_fs()
    print("NO-ERROR")
except ValueError as exc:
    print("ValueError:" + str(exc)[:40])
""",
    )
    assert out.startswith("ValueError:"), "a misconfigured backend must fail loudly, not fall back"


@_needs_azure_driver
def test_deployment_mode_switches_postgres_to_azure_with_ssl():
    """DEPLOYMENT_MODE=azure routes p0's CDM Postgres to the Azure server (host/port/creds
    from the Key Vault AZURE_PG_* names) with SSL. The database itself is chosen per-plant
    at connection time, so no cloud-fixed DB name is read here."""
    out = _probe(
        {
            "DEPLOYMENT_MODE": "azure",
            "AZURE_PG_HOST": "srv.postgres.database.azure.com",
            "AZURE_PG_USER": "azuser",
            "AZURE_PG_PASSWORD": "pw",
            "AZURE_PG_PORT": "5432",
        },
        """
import sys; sys.path.insert(0, ".")
from p0.api import config as c
print("|".join([str(c.CDM_DB_HOST), str(c.CDM_DB_PORT), str(c.CDM_DB_SSLMODE)]))
""",
    )
    host, port, sslmode = out.split("|")
    assert host == "srv.postgres.database.azure.com"
    assert port == "5432"
    assert sslmode == "require"


def test_onprem_postgres_is_byte_identical():
    """Default (onprem) must not set sslmode or touch AZURE_* — zero drift for the
    existing deployment."""
    out = _probe(
        {"DEPLOYMENT_MODE": ""},
        """
import sys; sys.path.insert(0, ".")
from p0.api import config as c
print(c.CDM_DB_SSLMODE is None)
""",
    )
    assert out == "True"
