"""Bounded parallel chunk execution and throttle backoff for document extraction."""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from botocore.exceptions import ClientError

from p0.source_processing.documents import user_doc_extract as ude

LOG = logging.getLogger("test_doc_chunk_concurrency")
COLS = ["tag"]


def _client_error(code):
    return ClientError({"Error": {"Code": code, "Message": code}}, "InvokeModel")


def _pages(n, chars=100):
    return [f"page {i} " + "x" * chars for i in range(1, n + 1)]


@pytest.fixture
def doc(monkeypatch, tmp_path):
    """A 12-page PDF, each page just under the chunk limit so it is its own chunk."""
    monkeypatch.setattr(
        ude, "_extract_pdf_pages", lambda p: _pages(12, ude._CHUNK_CHARS - 1000)
    )
    path = tmp_path / "doc.pdf"
    path.write_bytes(b"%PDF-1.4")
    return str(path)


def _extract(doc_path, pool=None):
    return ude._extract_from_document(
        doc_path, COLS, "SOP", "doc.pdf", object(), "model", LOG, llm_pool=pool
    ).rows


def test_throttle_codes_are_distinguished_from_fatal_ones():
    assert ude._is_throttle(_client_error("ThrottlingException"))
    assert ude._is_throttle(_client_error("ServiceUnavailableException"))
    assert not ude._is_throttle(_client_error("AccessDeniedException"))
    assert not ude._is_throttle(_client_error("ValidationException"))


def test_backoff_grows_and_is_capped(monkeypatch):
    monkeypatch.setattr(ude, "_RETRY_BASE_SECONDS", 1.0)
    monkeypatch.setattr(ude, "_RETRY_CAP_SECONDS", 8.0)
    assert all(0.0 <= ude._retry_delay(0) <= 1.0 for _ in range(50))
    assert all(0.0 <= ude._retry_delay(2) <= 4.0 for _ in range(50))
    assert all(0.0 <= ude._retry_delay(9) <= 8.0 for _ in range(50))


def test_backoff_is_jittered_so_retries_do_not_resynchronise(monkeypatch):
    monkeypatch.setattr(ude, "_RETRY_BASE_SECONDS", 1.0)
    monkeypatch.setattr(ude, "_RETRY_CAP_SECONDS", 30.0)
    assert len({ude._retry_delay(3) for _ in range(30)}) > 1


def test_a_throttled_call_is_retried_and_then_succeeds(monkeypatch):
    monkeypatch.setattr(ude, "_RETRY_BASE_SECONDS", 0.001)
    monkeypatch.setattr(ude, "_LLM_RETRIES", 4)
    calls = []

    def flaky(*a):
        calls.append(1)
        if len(calls) < 3:
            raise _client_error("ThrottlingException")
        return [{"tag": "T-1"}]

    monkeypatch.setattr(ude, "_call_llm", flaky)
    got = ude._call_llm_with_retry(None, "t", "{text}", "m", {}, LOG, "chunk 1")
    assert got == [{"tag": "T-1"}]
    assert len(calls) == 3


def test_retries_are_bounded_and_the_throttle_finally_surfaces(monkeypatch):
    monkeypatch.setattr(ude, "_RETRY_BASE_SECONDS", 0.001)
    monkeypatch.setattr(ude, "_LLM_RETRIES", 2)
    calls = []

    def always_throttled(*a):
        calls.append(1)
        raise _client_error("ThrottlingException")

    monkeypatch.setattr(ude, "_call_llm", always_throttled)
    with pytest.raises(ClientError):
        ude._call_llm_with_retry(None, "t", "{text}", "m", {}, LOG, "chunk 1")
    assert len(calls) == 3


def test_a_fatal_error_is_never_retried(monkeypatch):
    monkeypatch.setattr(ude, "_LLM_RETRIES", 5)
    calls = []

    def denied(*a):
        calls.append(1)
        raise _client_error("AccessDeniedException")

    monkeypatch.setattr(ude, "_call_llm", denied)
    with pytest.raises(ClientError):
        ude._call_llm_with_retry(None, "t", "{text}", "m", {}, LOG, "chunk 1")
    assert len(calls) == 1


def test_chunks_of_one_document_actually_run_concurrently(monkeypatch, doc):
    monkeypatch.setattr(ude, "_call_llm", lambda *a: time.sleep(0.05) or [{"tag": "T"}])
    with ThreadPoolExecutor(max_workers=6) as pool:
        started = time.monotonic()
        rows = _extract(doc, pool)
        parallel = time.monotonic() - started

    started = time.monotonic()
    _extract(doc, None)
    serial = time.monotonic() - started

    assert rows
    assert parallel < serial / 2


def test_in_flight_calls_never_exceed_the_pool_size(monkeypatch, doc):
    live = 0
    peak = 0
    lock = threading.Lock()

    def counted(*a):
        nonlocal live, peak
        with lock:
            live += 1
            peak = max(peak, live)
        time.sleep(0.02)
        with lock:
            live -= 1
        return [{"tag": "T"}]

    monkeypatch.setattr(ude, "_call_llm", counted)
    with ThreadPoolExecutor(max_workers=3) as pool:
        _extract(doc, pool)
    assert peak <= 3


def test_rows_keep_chunk_order_even_when_later_chunks_finish_first(monkeypatch, doc):
    def by_page(client, text, *a):
        page = text.split()[1]
        time.sleep(0.05 if page == "1" else 0.0)
        return [{"tag": f"T-{page}"}]

    monkeypatch.setattr(ude, "_call_llm", by_page)
    with ThreadPoolExecutor(max_workers=6) as pool:
        rows = _extract(doc, pool)

    assert [r["chunk_index"] for r in rows] == sorted(r["chunk_index"] for r in rows)
    assert rows[0]["extracted_entities"].endswith("T-1")


def test_one_failing_chunk_does_not_lose_the_others(monkeypatch, doc):
    def fail_on_page_three(client, text, *a):
        if text.split()[1] == "3":
            raise RuntimeError("boom")
        return [{"tag": "T"}]

    monkeypatch.setattr(ude, "_call_llm", fail_on_page_three)
    with ThreadPoolExecutor(max_workers=4) as pool:
        rows = _extract(doc, pool)
    assert len(rows) == 11


def test_a_fatal_chunk_error_aborts_the_whole_file(monkeypatch, doc):
    def denied(*a):
        raise _client_error("AccessDeniedException")

    monkeypatch.setattr(ude, "_call_llm", denied)
    with ThreadPoolExecutor(max_workers=4) as pool:
        with pytest.raises(ClientError):
            _extract(doc, pool)


def test_a_single_chunk_document_needs_no_pool(monkeypatch, tmp_path):
    monkeypatch.setattr(ude, "_extract_pdf_pages", lambda p: ["only page"])
    monkeypatch.setattr(ude, "_call_llm", lambda *a: [{"tag": "T-1"}])
    path = tmp_path / "one.pdf"
    path.write_bytes(b"%PDF-1.4")
    with ThreadPoolExecutor(max_workers=4) as pool:
        result = ude._extract_from_document(
            str(path), COLS, "SOP", "one.pdf", object(), "m", LOG, llm_pool=pool
        )
    assert len(result.rows) == 1
