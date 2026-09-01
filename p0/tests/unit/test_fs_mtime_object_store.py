"""mtime must work on every object store, not only the one that says LastModified."""

from __future__ import annotations

import datetime

import pandas as pd
import pytest

import p0.utils.fs as fs_module

CLOUD = "abfs://container/some/file.parquet"
EXPECTED = pd.Timestamp("2026-08-20").timestamp()


class _StubFS:
    """A filesystem whose info() reports one particular metadata shape."""

    def __init__(self, info: dict) -> None:
        self._info = info

    def info(self, path: str) -> dict:
        return self._info


@pytest.mark.parametrize(
    "info",
    [
        {"LastModified": pd.Timestamp("2026-08-20")},
        {"last_modified": datetime.datetime(2026, 8, 20)},
        {"last_modified": EXPECTED},
    ],
)
def test_every_object_store_spelling_gives_the_same_instant(monkeypatch, info):
    monkeypatch.setattr(fs_module, "get_fs", lambda: _StubFS(info))
    assert fs_module.mtime(CLOUD) == EXPECTED


def test_files_are_distinguishable_so_newest_first_can_sort(monkeypatch):
    stamps = {
        "abfs://c/old.parquet": {"last_modified": datetime.datetime(2026, 1, 1)},
        "abfs://c/new.parquet": {"last_modified": datetime.datetime(2026, 8, 20)},
    }
    monkeypatch.setattr(
        fs_module, "get_fs", lambda: _StubFS(None)
    )
    monkeypatch.setattr(
        fs_module, "get_fs", lambda: type("F", (), {"info": staticmethod(lambda p: stamps[p])})()
    )
    ordered = sorted(stamps, key=fs_module.mtime, reverse=True)
    assert ordered[0] == "abfs://c/new.parquet"
    assert fs_module.mtime(ordered[0]) != fs_module.mtime(ordered[1])


def test_missing_metadata_degrades_to_zero_without_raising(monkeypatch):
    monkeypatch.setattr(fs_module, "get_fs", lambda: _StubFS({"size": 10}))
    assert fs_module.mtime(CLOUD) == 0.0
