"""Chunking and page provenance for the document extractor."""

from __future__ import annotations

from p0.source_processing.documents.user_doc_extract import (
    _Chunk,
    _chunk_pages,
    _chunk_plain_text,
    _split_oversized,
)


def test_pages_are_packed_up_to_the_limit_not_one_chunk_per_two_pages():
    pages = ["a" * 100 for _ in range(10)]
    chunks = _chunk_pages(pages, limit=2000, overlap=0)
    assert len(chunks) == 1
    assert chunks[0].page_start == 1
    assert chunks[0].page_end == 10


def test_packing_respects_the_separators_it_adds():
    pages = ["a" * 100 for _ in range(10)]
    chunks = _chunk_pages(pages, limit=1000, overlap=0)
    assert len(chunks) == 2
    assert all(len(c.text) <= 1000 for c in chunks)


def test_no_text_is_dropped_when_a_page_exceeds_the_limit():
    pages = ["b" * 5000]
    chunks = _chunk_pages(pages, limit=1000, overlap=0)
    assert sum(len(c.text) for c in chunks) == 5000
    assert all(c.page_start == 1 and c.page_end == 1 for c in chunks)


def test_every_chunk_stays_within_the_limit():
    pages = ["c" * 700 for _ in range(20)]
    chunks = _chunk_pages(pages, limit=1500, overlap=0)
    assert chunks
    assert all(len(c.text) <= 1500 for c in chunks)


def test_page_ranges_are_contiguous_and_cover_every_page():
    pages = [f"page {i} " + "d" * 400 for i in range(1, 13)]
    chunks = _chunk_pages(pages, limit=1000, overlap=0)
    covered = {p for c in chunks for p in range(c.page_start, c.page_end + 1)}
    assert covered == set(range(1, 13))


def test_chunk_indexes_are_dense_and_ordered():
    pages = ["e" * 400 for _ in range(9)]
    chunks = _chunk_pages(pages, limit=900, overlap=0)
    assert [c.index for c in chunks] == list(range(len(chunks)))


def test_blank_pages_do_not_become_chunks():
    chunks = _chunk_pages(["", "   ", "real content"], limit=1000, overlap=0)
    assert len(chunks) == 1
    assert chunks[0].text == "real content"


def test_split_oversized_overlaps_so_a_value_on_a_seam_survives():
    parts = _split_oversized("f" * 100, limit=40, overlap=10)
    assert all(len(p) <= 40 for p in parts)
    assert "".join(dict.fromkeys(parts)) != ""
    assert len(parts) > 1


def test_plain_text_chunks_carry_no_page_numbers():
    chunks = _chunk_plain_text("g" * 5000, limit=1000, overlap=0)
    assert chunks
    assert all(c.page_start == 0 and c.page_end == 0 for c in chunks)
    assert all(c.pages_label() == "" for c in chunks)


def test_pages_label_reads_as_a_range_or_a_single_page():
    assert _Chunk(0, "x", 3, 3).pages_label() == "3"
    assert _Chunk(0, "x", 3, 7).pages_label() == "3-7"
    assert _Chunk(0, "x", 0, 0).pages_label() == ""


def test_empty_input_produces_no_chunks():
    assert _chunk_pages([], limit=1000, overlap=0) == []
    assert _chunk_plain_text("", limit=1000, overlap=0) == []
