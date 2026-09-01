"""Every object-store scheme the platform emits is recognised as remote, not local."""

from __future__ import annotations

import os

os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

import pytest

from p0.utils.fs import is_s3, strip_scheme

REMOTE = ["s3://b/k", "abfs://c/k", "abfss://c@acct.dfs.core.windows.net/k", "az://c/k"]
LOCAL = ["/tmp/x", "data/out", "./rel/path", "C:/win/path"]


@pytest.mark.parametrize("path", REMOTE)
def test_an_object_store_path_is_remote(path):
    assert is_s3(path)


@pytest.mark.parametrize("path", LOCAL)
def test_a_local_path_is_not_remote(path):
    assert not is_s3(path)


@pytest.mark.parametrize("path", REMOTE)
def test_every_remote_scheme_is_stripped(path):
    assert "://" not in strip_scheme(path)


def test_the_secure_azure_scheme_is_not_mistaken_for_the_plain_one():
    assert strip_scheme("abfss://c/k") == "c/k"
    assert strip_scheme("abfs://c/k") == "c/k"
