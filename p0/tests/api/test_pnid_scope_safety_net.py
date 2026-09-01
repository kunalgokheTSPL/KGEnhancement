"""
P&ID run scope is determined from RustFS staging (newest PDF), not the client.

What we're protecting:
  The review showing the WRONG P&ID preview (e.g. a stale 'PnID.pdf' from a
  prior session) instead of the file just sent to CDM. In the current flow the
  external extractor's upload-to-cdm step writes the verified pdf+xlsx to
  staging, so the newest staged PDF is authoritative. The frontend's file_names
  can be stale, so the backend reads staging directly.

We mock the s3fs listing so no real RustFS is needed.
"""

from __future__ import annotations

from unittest.mock import patch

from p0.tests._p0_test_base import client

from p0.api.routers import pipeline as pl


def _entries(*name_mtime):
    return [
        {"name": f"staging-pt/p0/staging-pt/plant_1_testcase/pnid/{n}", "LastModified": m}
        for n, m in name_mtime
    ]


def test_picks_newest_staged_pdf():
    listing = _entries(
        ("PnID.pdf", "2026-06-15T06:00:00"),
        ("05-BNCPP-B-B-1100_TTJT-A_RECEIVER.pdf", "2026-06-15T12:00:00"),
        ("PnID.xlsx", "2026-06-15T12:00:00"),
    )

    class _FakeFS:
        def ls(self, *_a, **_k):
            return listing

    with patch.object(pl, "get_object_fs", return_value=_FakeFS()):
        assert pl._latest_staged_pnid_file("plant_1_testcase") == [
            "05-BNCPP-B-B-1100_TTJT-A_RECEIVER.pdf"
        ]


def test_empty_when_no_pdfs():
    class _FakeFS:
        def ls(self, *_a, **_k):
            return _entries(("notes.txt", "2026-06-15T12:00:00"))

    with patch.object(pl, "get_object_fs", return_value=_FakeFS()):
        assert pl._latest_staged_pnid_file("plant_1_testcase") == []


def test_empty_on_fs_error():
    with patch(
        "p0.drivers.database_driver.get_object_fs",
        side_effect=RuntimeError("rustfs down"),
    ):
        assert pl._latest_staged_pnid_file("plant_1_testcase") == []
