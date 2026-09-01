"""The docs writer must actually delete last upload's parquets, not swallow the attempt."""

from __future__ import annotations

import logging

import pandas as pd

import p0.pipelines.run_docs_end_to_end as docs_pipeline
import p0.utils.fs as fs_module


class _RecordingFS:
    """A filesystem that records the cleanup calls made against it."""

    def __init__(self) -> None:
        self.globbed: list[str] = []
        self.removed: list[str] = []

    def glob(self, pattern: str) -> list[str]:
        self.globbed.append(pattern)
        return ["bucket/processed/spec/stale.parquet"]

    def rm(self, path: str) -> None:
        self.removed.append(path)


def _run(monkeypatch) -> _RecordingFS:
    """Drive the processed-docs writer against a recording filesystem."""
    recorder = _RecordingFS()
    monkeypatch.setattr(fs_module, "get_fs", lambda: recorder)
    monkeypatch.setattr(fs_module, "write_parquet", lambda *a, **k: None)
    frame = pd.DataFrame(
        {"document_type": ["spec"], "document_type_key": ["spec"], "value": [1]}
    )
    docs_pipeline._write_processed_docs_to_rustfs(
        frame, "s3://bucket/processed", logging.getLogger("test")
    )
    return recorder


def test_stale_parquets_from_the_previous_upload_are_deleted(monkeypatch):
    recorder = _run(monkeypatch)
    assert recorder.removed == ["bucket/processed/spec/stale.parquet"]


def test_the_cleanup_globs_under_the_scheme_stripped_prefix(monkeypatch):
    recorder = _run(monkeypatch)
    assert recorder.globbed == ["bucket/processed/*/*.parquet"]
    assert not any(p.startswith("s3://") for p in recorder.globbed)


def test_the_cleanup_failure_is_not_silently_swallowed(monkeypatch, caplog):
    with caplog.at_level(logging.DEBUG):
        _run(monkeypatch)
    assert "could not clean up stale parquets" not in caplog.text
