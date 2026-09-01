"""fs.py object-store helpers delegate to the facade-selected driver (onprem in tests)."""

from __future__ import annotations

from p0.utils import fs


def test_scheme_prefix_is_s3_onprem():
    assert fs.scheme_prefix() == "s3://"


def test_scheme_prefix_is_abfs_when_driver_is_adls(monkeypatch):
    monkeypatch.setattr("p0.drivers.database_driver.object_store_scheme", lambda: "abfs")
    assert fs.scheme_prefix() == "abfs://"


def test_storage_options_adls_shape(monkeypatch):
    monkeypatch.setattr("p0.drivers.database_driver.storage_backend", lambda: "adls")
    monkeypatch.setenv("ADLS_ACCOUNT_NAME", "acct")
    monkeypatch.setenv("ADLS_ACCOUNT_KEY", "a2V5")
    opts = fs.get_storage_options()
    assert opts == {"account_name": "acct", "account_key": "a2V5"}
    assert "client_kwargs" not in opts


def test_storage_options_adls_omits_key_for_managed_identity(monkeypatch):
    monkeypatch.setattr("p0.drivers.database_driver.storage_backend", lambda: "adls")
    monkeypatch.setenv("ADLS_ACCOUNT_NAME", "acct")
    monkeypatch.delenv("ADLS_ACCOUNT_KEY", raising=False)
    assert fs.get_storage_options() == {"account_name": "acct"}


def test_storage_options_rustfs_shape():
    opts = fs.get_storage_options()
    assert set(opts) == {"key", "secret", "client_kwargs"}
    assert "endpoint_url" in opts["client_kwargs"]
