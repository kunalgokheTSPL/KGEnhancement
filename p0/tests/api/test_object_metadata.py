"""Every uploaded object carries a metadata document beside it in object storage."""

from __future__ import annotations

import json
import pathlib

import pytest

from p0.api.services import object_metadata as om

CONNECTORS = (
    pathlib.Path(__file__).resolve().parents[2] / "api" / "routers" / "connectors.py"
).read_text()


class _FakeFS:
    """Minimal in-memory stand-in for the object store."""

    def __init__(self):
        self.store: dict[str, bytes] = {}
        self.fail_on: set[str] = set()

    def exists(self, path):
        return path in self.store

    def open(self, path, mode="rb"):
        if path in self.fail_on:
            raise OSError("storage unavailable")
        return _FakeHandle(self.store, path, mode)


class _FakeHandle:
    def __init__(self, store, path, mode):
        self.store, self.path, self.mode = store, path, mode
        self.buffer = bytearray()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        if "w" in self.mode:
            self.store[self.path] = bytes(self.buffer)
        return False

    def write(self, data):
        self.buffer.extend(data)

    def read(self):
        return self.store.get(self.path, b"")


@pytest.fixture
def fake_fs(monkeypatch):
    fs = _FakeFS()
    monkeypatch.setattr(om._fs, "get_fs", lambda: fs)
    return fs


def _kwargs(**overrides):
    base = dict(
        plant_code_id="BOKOR",
        connector="documents",
        category="sop",
        file_name="Pump SOP.pdf",
        object_path="staging/BOKOR/docs/sop/Pump SOP.pdf",
        size_bytes=2048,
        content_hash="sha256:abc",
        content_type="application/pdf",
        upload_job_id="job_1",
        upload_batch_id="bat_1",
        flow_uid=7,
        uploaded_by="abhay@x.com",
        metadata={"kind": "pdf", "page_count": 12},
    )
    base.update(overrides)
    return base


def test_sidecar_path_sits_next_to_the_object():
    assert om.sidecar_path("a/b/file.pdf") == "a/b/file.pdf.meta.json"


def test_manifest_path_is_per_folder():
    assert om.manifest_path("a/b/") == "a/b/_manifest.json"
    assert om.manifest_path("a/b") == "a/b/_manifest.json"


def test_document_carries_path_type_and_tags():
    doc = om.build_document(**_kwargs(tags=["rev-c"]))
    assert doc["path"] == "staging/BOKOR/docs/sop/Pump SOP.pdf"
    assert doc["category"] == "sop"
    assert doc["connector"] == "documents"
    assert doc["tags"] == ["rev-c"]
    assert doc["metadata"]["page_count"] == 12
    assert doc["uploaded_at"]


def test_record_writes_sidecar_and_manifest(fake_fs):
    doc = om.record("staging/BOKOR/docs/sop", **_kwargs())
    assert doc["sidecar_written"] is True
    assert doc["manifest_updated"] is True
    assert "staging/BOKOR/docs/sop/Pump SOP.pdf.meta.json" in fake_fs.store
    assert "staging/BOKOR/docs/sop/_manifest.json" in fake_fs.store


def test_sidecar_round_trips(fake_fs):
    om.record("staging/BOKOR/docs/sop", **_kwargs())
    read = om.read_sidecar("staging/BOKOR/docs/sop/Pump SOP.pdf")
    assert read["file_name"] == "Pump SOP.pdf"
    assert read["flow_uid"] == 7
    assert read["uploaded_by"] == "abhay@x.com"


def test_missing_sidecar_reads_as_none(fake_fs):
    assert om.read_sidecar("staging/BOKOR/docs/sop/absent.pdf") is None


def test_manifest_accumulates_files(fake_fs):
    om.record("staging/BOKOR/docs/sop", **_kwargs())
    om.record(
        "staging/BOKOR/docs/sop",
        **_kwargs(file_name="Valve SOP.pdf", object_path="staging/BOKOR/docs/sop/Valve SOP.pdf"),
    )
    manifest = om.read_manifest("staging/BOKOR/docs/sop")
    assert manifest["file_count"] == 2
    assert {f["file_name"] for f in manifest["files"]} == {"Pump SOP.pdf", "Valve SOP.pdf"}


def test_reuploading_the_same_path_replaces_rather_than_duplicates(fake_fs):
    om.record("staging/BOKOR/docs/sop", **_kwargs())
    om.record("staging/BOKOR/docs/sop", **_kwargs(size_bytes=9999))
    manifest = om.read_manifest("staging/BOKOR/docs/sop")
    assert manifest["file_count"] == 1
    assert manifest["files"][0]["size_bytes"] == 9999


def test_manifest_entries_carry_what_a_listing_needs(fake_fs):
    om.record("staging/BOKOR/docs/sop", **_kwargs())
    entry = om.read_manifest("staging/BOKOR/docs/sop")["files"][0]
    for key in ("file_name", "path", "category", "size_bytes", "content_hash", "flow_uid"):
        assert key in entry


def test_a_storage_failure_never_breaks_the_upload(fake_fs):
    fake_fs.fail_on.add("staging/BOKOR/docs/sop/Pump SOP.pdf.meta.json")
    doc = om.record("staging/BOKOR/docs/sop", **_kwargs())
    assert doc["sidecar_written"] is False
    assert doc["manifest_updated"] is True


def test_corrupt_manifest_is_replaced_not_raised(fake_fs):
    fake_fs.store["staging/BOKOR/docs/sop/_manifest.json"] = b"{not json"
    assert om.record("staging/BOKOR/docs/sop", **_kwargs())["manifest_updated"] is True
    assert om.read_manifest("staging/BOKOR/docs/sop")["file_count"] == 1


def test_every_upload_records_object_metadata():
    assert CONNECTORS.count("_object_metadata.record(") == 6


def test_a_metadata_read_endpoint_exists():
    assert '"/{connector}/files/{flowUid}/metadata"' in CONNECTORS
    assert "def getFileMetadata(" in CONNECTORS


def test_documents_record_their_own_connector_not_pnid():
    """uploadDocumentFiles recorded every sidecar under connector='pnid'."""
    block = CONNECTORS.split("async def uploadDocumentFiles(")[1].split("\n@router")[0]
    assert 'connector="pnid"' not in block
    assert 'connector="documents"' in block
    assert 'category=safe_type' in block


def test_each_upload_records_metadata_under_its_own_connector():
    import re as _re

    for fn, expected in (
        ("uploadPnidFiles", '"pnid"'),
        ("uploadDocumentFiles", '"documents"'),
        ("uploadSapFiles", '"sap"'),
    ):
        block = CONNECTORS.split(f"async def {fn}(")[1].split("\n@router")[0]
        found = _re.findall(r'_object_metadata\.record\((?:.|\n)*?connector=("[a-z]+")', block)
        assert found and all(c == expected for c in found), f"{fn} records as {found}"
