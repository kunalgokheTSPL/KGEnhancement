"""Parallel document extraction keeps per-document isolation and order."""

import io
import os
import pathlib
import tempfile
import threading
import time

import pytest

os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

from botocore.exceptions import ClientError

from p0.source_processing.documents import user_doc_extract as ude

TYPES = {"fmea": 4, "rca": 5, "operating_manual": 3}
ROWS_PER_DOC = 3


def _staging() -> str:
    root = tempfile.mkdtemp(prefix="p0docs_")
    for type_key, count in TYPES.items():
        d = pathlib.Path(root, type_key)
        d.mkdir(parents=True)
        for i in range(count):
            (d / f"{type_key}_{i:02d}.pdf").write_text("stub")
    return root


def _cfg() -> dict:
    return {
        t: {"document_type": t.upper(), "source_columns": ["Equipment_Tag", "Desc"]}
        for t in TYPES
    }


def _rows(fname: str, label: str, file_reference: str) -> list[dict]:
    return [
        {
            "document_id": fname,
            "title": fname,
            "document_type": label,
            "equipment_tag": f"TAG-{fname}-{i}",
            "equipment_label": "",
            "equipment_id": "",
            "description": label,
            "source_file": fname,
            "file_reference": file_reference,
            "extracted_entities": "",
            "ingested_at": "",
        }
        for i in range(ROWS_PER_DOC)
    ]


@pytest.fixture()
def patched(monkeypatch):
    """Replace the LLM call with a deterministic sleeping stub."""
    monkeypatch.setattr(ude, "_bedrock_client", lambda model_id=None: object())

    def fake(fp, cols, label, fname, client, model, log, file_reference="", doc_type_key="", llm_pool=None, resume=None, field_descs=None):
        time.sleep(0.15)
        return ude._DocResult(_rows(fname, label, file_reference), 1)

    monkeypatch.setattr(ude, "_extract_from_document", fake)
    return fake


def _run(root, workers, monkeypatch):
    monkeypatch.setattr(ude, "_MAX_WORKERS", workers)
    ledger = os.path.join(root, f"ledger_{workers}.json")
    return ude.process_user_documents(root, _cfg(), dedup_ledger_path=ledger)


def test_parallel_matches_sequential_order(patched, monkeypatch):
    root = _staging()
    seq = _run(root, 1, monkeypatch)
    par = _run(root, 16, monkeypatch)

    assert len(par) == len(seq) == sum(TYPES.values()) * ROWS_PER_DOC
    assert par["source_file"].tolist() == seq["source_file"].tolist()
    assert par["equipment_tag"].tolist() == seq["equipment_tag"].tolist()


def test_rows_never_interleave_across_documents(patched, monkeypatch):
    root = _staging()
    df = _run(root, 16, monkeypatch)

    seen: set[str] = set()
    prev = None
    for name in df["source_file"]:
        if name != prev:
            assert name not in seen, f"rows for {name} were split apart"
            seen.add(name)
            prev = name
    assert len(seen) == sum(TYPES.values())
    assert df.groupby("source_file").size().eq(ROWS_PER_DOC).all()


def test_documents_actually_run_concurrently(patched, monkeypatch):
    root = _staging()
    live = 0
    peak = 0
    lock = threading.Lock()

    def counting(fp, cols, label, fname, client, model, log, file_reference="", doc_type_key="", llm_pool=None, resume=None, field_descs=None):
        nonlocal live, peak
        with lock:
            live += 1
            peak = max(peak, live)
        time.sleep(0.15)
        with lock:
            live -= 1
        return ude._DocResult(_rows(fname, label, file_reference), 1)

    monkeypatch.setattr(ude, "_extract_from_document", counting)
    _run(root, 8, monkeypatch)
    assert peak > 1, "extraction ran serially"


def test_one_failing_document_does_not_stop_the_rest(patched, monkeypatch):
    root = _staging()
    bad = "rca_02.pdf"

    def flaky(fp, cols, label, fname, client, model, log, file_reference="", doc_type_key="", llm_pool=None, resume=None, field_descs=None):
        if fname == bad:
            raise RuntimeError("boom")
        return ude._DocResult(_rows(fname, label, file_reference), 1)

    monkeypatch.setattr(ude, "_extract_from_document", flaky)
    df = _run(root, 8, monkeypatch)

    assert bad not in set(df["source_file"])
    assert len(df) == (sum(TYPES.values()) - 1) * ROWS_PER_DOC


def test_fatal_credential_error_aborts_the_run(patched, monkeypatch):
    root = _staging()

    def denied(fp, cols, label, fname, client, model, log, file_reference="", doc_type_key="", llm_pool=None, resume=None, field_descs=None):
        raise ClientError(
            {"Error": {"Code": "AccessDeniedException", "Message": "no"}}, "InvokeModel"
        )

    monkeypatch.setattr(ude, "_extract_from_document", denied)
    with pytest.raises(ClientError):
        _run(root, 8, monkeypatch)


def test_ledger_records_every_file_under_concurrency(patched, monkeypatch):
    root = _staging()
    ledger = os.path.join(root, "shared_ledger.json")
    monkeypatch.setattr(ude, "_MAX_WORKERS", 16)

    df = ude.process_user_documents(root, _cfg(), dedup_ledger_path=ledger)
    assert len(df) == sum(TYPES.values()) * ROWS_PER_DOC

    recorded = ude._load_done_ledger(ledger)
    names = [f for files in recorded.values() for f in files]
    assert len(names) == len(set(names)) == sum(TYPES.values())


class _FakeRemoteFS:
    """Minimal RustFS stand-in serving one folder of in-memory objects."""

    def __init__(self, payload: dict[str, bytes]):
        self.payload = payload

    def ls(self, prefix, detail=False):
        return sorted(f"{str(prefix).rstrip('/')}/{name}" for name in self.payload)

    def open(self, remote, mode="rb"):
        return io.BytesIO(self.payload[str(remote).split("/")[-1]])


def test_remote_documents_are_still_on_disk_when_extraction_opens_them(monkeypatch):
    """A RustFS-sourced file must outlive planning and be readable by its worker."""
    payload = {f"rca_{i:02d}.pdf": f"body-{i}".encode() for i in range(6)}
    monkeypatch.setattr(ude.fs, "get_fs", lambda: _FakeRemoteFS(payload))
    monkeypatch.setattr(ude, "_bedrock_client", lambda model_id=None: object())
    monkeypatch.setattr(ude, "_MAX_WORKERS", 6)

    read_back: dict[str, bytes] = {}
    used_paths: list[str] = []

    def fake(fp, cols, label, fname, client, model, log, file_reference="", doc_type_key="", llm_pool=None, resume=None, field_descs=None):
        used_paths.append(fp)
        with open(fp, "rb") as fh:
            read_back[fname] = fh.read()
        return ude._DocResult(_rows(fname, label, file_reference), 1)

    monkeypatch.setattr(ude, "_extract_from_document", fake)

    cfg = {"rca": {"document_type": "RCA", "source_columns": ["Equipment_Tag", "Desc"]}}
    df = ude.process_user_documents("s3://bucket/documents", cfg)

    assert read_back == payload
    assert len(df) == len(payload) * ROWS_PER_DOC
    assert [p for p in used_paths if os.path.exists(p)] == []
