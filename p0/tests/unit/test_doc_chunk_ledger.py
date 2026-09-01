"""Per-chunk dedup ledger: a lost chunk is retried, a landed one is never re-paid for."""

from __future__ import annotations

import json
import logging
import os

import pandas as pd
import pytest

os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

from p0.source_processing.documents import user_doc_extract as ude

LOG = logging.getLogger("test_doc_chunk_ledger")
COLS = ["Tag"]
CFG = {"sop": {"document_type": "SOP", "source_columns": COLS}}


def _pages(n):
    return [f"page {i} " + "x" * (ude._CHUNK_CHARS - 1000) for i in range(1, n + 1)]


def _page_no(text):
    return int(text.split()[1])


@pytest.fixture
def staged(monkeypatch, tmp_path):
    """One 4-page PDF in staging, each page its own chunk, with a Bedrock stub."""
    type_dir = tmp_path / "staging" / "sop"
    type_dir.mkdir(parents=True)
    (type_dir / "manual.pdf").write_bytes(b"%PDF-1.4")
    monkeypatch.setattr(ude, "_bedrock_client", lambda model_id=None: object())
    monkeypatch.setattr(ude, "_extract_pdf_pages", lambda p: _pages(4))
    return tmp_path


def _run(root):
    """Run the extractor once against the staged tree, sharing one ledger."""
    return ude.process_user_documents(
        str(root / "staging"),
        CFG,
        dedup_ledger_path=str(root / "ledger.json"),
        processed_out=str(root / "processed"),
    )


def _publish(df, root):
    """Write the run's rows where the next run looks for historical output."""
    out = root / "processed" / "sop"
    out.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out / "docs_documents.parquet", index=False)


def _ledger(root):
    """The per-file chunk state the run recorded for this doc type and field set."""
    key = ude._ledger_key("sop", COLS)
    return ude._load_done_ledger(str(root / "ledger.json"))[key]


def test_a_legacy_list_ledger_still_reads_as_completed_files(tmp_path):
    path = tmp_path / "old.json"
    path.write_text(json.dumps({"sop@abc": ["a.pdf", "b.pdf"]}))
    loaded = ude._load_done_ledger(str(path))
    assert set(loaded["sop@abc"]) == {"a.pdf", "b.pdf"}
    assert loaded["sop@abc"]["a.pdf"] == {"chunks": 0, "failed": []}


def test_chunk_state_survives_a_save_and_load_round_trip(tmp_path):
    path = str(tmp_path / "l.json")
    ude._save_done_ledger(path, {"k": {"a.pdf": ude._ledger_entry(9, [4, 1])}})
    assert ude._load_done_ledger(path) == {
        "k": {"a.pdf": {"chunks": 9, "failed": [1, 4]}}
    }


def test_an_unreadable_ledger_is_treated_as_empty_rather_than_crashing(tmp_path):
    path = tmp_path / "broken.json"
    path.write_text("{not json")
    assert ude._load_done_ledger(str(path)) == {}


def test_a_file_that_lost_a_chunk_is_recorded_as_incomplete(monkeypatch, staged):
    def fail_on_three(client, text, *a):
        if _page_no(text) == 3:
            raise RuntimeError("boom")
        return [{"Tag": f"T-{_page_no(text)}"}]

    monkeypatch.setattr(ude, "_call_llm", fail_on_three)
    df = _run(staged)

    assert len(df) == 3
    assert _ledger(staged)["manual.pdf"] == {"chunks": 4, "failed": [2]}


def test_a_file_that_landed_every_chunk_is_recorded_as_complete(monkeypatch, staged):
    monkeypatch.setattr(ude, "_call_llm", lambda c, text, *a: [{"Tag": "T"}])
    df = _run(staged)

    assert len(df) == 4
    assert _ledger(staged)["manual.pdf"] == {"chunks": 4, "failed": []}


def test_a_resume_re_spends_calls_only_on_the_chunk_that_was_lost(monkeypatch, staged):
    def fail_on_three(client, text, *a):
        if _page_no(text) == 3:
            raise RuntimeError("boom")
        return [{"Tag": f"T-{_page_no(text)}"}]

    monkeypatch.setattr(ude, "_call_llm", fail_on_three)
    _publish(_run(staged), staged)

    seen = []

    def record(client, text, *a):
        seen.append(_page_no(text))
        return [{"Tag": f"T-{_page_no(text)}"}]

    monkeypatch.setattr(ude, "_call_llm", record)
    df = _run(staged)

    assert seen == [3]
    assert len(df) == 4
    assert _ledger(staged)["manual.pdf"] == {"chunks": 4, "failed": []}


def test_the_resumed_rows_merge_back_in_chunk_order_without_colliding(
    monkeypatch, staged
):
    def fail_on_two(client, text, *a):
        if _page_no(text) == 2:
            raise RuntimeError("boom")
        return [{"Tag": f"T-{_page_no(text)}"}]

    monkeypatch.setattr(ude, "_call_llm", fail_on_two)
    _publish(_run(staged), staged)

    monkeypatch.setattr(
        ude, "_call_llm", lambda c, text, *a: [{"Tag": f"T-{_page_no(text)}"}]
    )
    df = _run(staged)

    assert df["chunk_index"].tolist() == [0, 1, 2, 3]
    assert df["record_id"].nunique() == 4
    assert df["extracted_entities"].tolist() == [f"Tag: T-{i}" for i in range(1, 5)]


def test_a_completed_file_costs_nothing_on_the_next_run(monkeypatch, staged):
    monkeypatch.setattr(
        ude, "_call_llm", lambda c, text, *a: [{"Tag": f"T-{_page_no(text)}"}]
    )
    _publish(_run(staged), staged)

    def never(*a):
        raise AssertionError("a completed file was re-extracted")

    monkeypatch.setattr(ude, "_call_llm", never)
    df = _run(staged)
    assert len(df) == 4


def test_a_chunk_that_fails_twice_stays_on_the_retry_list(monkeypatch, staged):
    def fail_on_three(client, text, *a):
        if _page_no(text) == 3:
            raise RuntimeError("boom")
        return [{"Tag": f"T-{_page_no(text)}"}]

    monkeypatch.setattr(ude, "_call_llm", fail_on_three)
    _publish(_run(staged), staged)
    df = _run(staged)

    assert len(df) == 3
    assert _ledger(staged)["manual.pdf"] == {"chunks": 4, "failed": [2]}


def test_a_document_that_split_differently_is_re_extracted_whole(monkeypatch, staged):
    seen = []

    def record(client, text, *a):
        seen.append(_page_no(text))
        return [{"Tag": f"T-{_page_no(text)}"}]

    monkeypatch.setattr(ude, "_call_llm", record)
    result = ude._extract_from_document(
        str(staged / "staging" / "sop" / "manual.pdf"),
        COLS,
        "SOP",
        "manual.pdf",
        object(),
        "m",
        LOG,
        resume=ude._Resume(chunks=7, failed=[2]),
    )

    assert seen == [1, 2, 3, 4]
    assert len(result.rows) == 4


def test_record_ids_do_not_depend_on_which_chunks_ran(monkeypatch, staged):
    monkeypatch.setattr(
        ude, "_call_llm", lambda c, text, *a: [{"Tag": f"T-{_page_no(text)}"}]
    )
    path = str(staged / "staging" / "sop" / "manual.pdf")
    args = (COLS, "SOP", "manual.pdf", object(), "m", LOG)

    whole = ude._extract_from_document(path, *args)
    partial = ude._extract_from_document(
        path, *args, resume=ude._Resume(chunks=4, failed=[2])
    )

    assert [r["record_id"] for r in partial.rows] == [whole.rows[2]["record_id"]]


def test_several_entries_from_one_chunk_keep_distinct_record_ids(monkeypatch, staged):
    monkeypatch.setattr(
        ude,
        "_call_llm",
        lambda c, text, *a: [{"Tag": "A"}, {"Tag": "B"}, {"Tag": "C"}],
    )
    result = ude._extract_from_document(
        str(staged / "staging" / "sop" / "manual.pdf"),
        COLS,
        "SOP",
        "manual.pdf",
        object(),
        "m",
        LOG,
    )

    assert len(result.rows) == 12
    assert len({r["record_id"] for r in result.rows}) == 12


def test_a_tabular_file_is_recorded_as_a_single_complete_chunk(tmp_path):
    type_dir = tmp_path / "staging" / "sop"
    type_dir.mkdir(parents=True)
    pd.DataFrame({"Tag": ["K-1", "K-2"]}).to_excel(type_dir / "list.xlsx", index=False)

    df = _run(tmp_path)

    assert len(df) == 2
    assert _ledger(tmp_path)["list.xlsx"] == {"chunks": 1, "failed": []}
