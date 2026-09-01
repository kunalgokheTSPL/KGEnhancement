"""The pipelines keep the directory the caller named and create the one they write to."""

from __future__ import annotations

import pathlib

PIPELINES = pathlib.Path(__file__).resolve().parents[2] / "pipelines"
PNID = (PIPELINES / "run_pnid_from_pdfs.py").read_text(encoding="utf-8")
TS = (PIPELINES / "run_ts_end_to_end.py").read_text(encoding="utf-8")

FALLBACK = "args.pnid_staging_dir = args.pnid_staging_dir or args.pnid_pdf_dir"
OVERRIDE = "args.pnid_pdf_dir = _tmp_pdf_dir"


def test_the_caller_pdf_dir_is_kept_as_the_staging_fallback():
    assert FALLBACK in PNID


def test_the_fallback_is_taken_before_the_temp_dir_overwrites_it():
    assert PNID.index(FALLBACK) < PNID.index(OVERRIDE)


def test_the_temp_dir_still_becomes_the_download_target():
    assert OVERRIDE in PNID


def test_the_ts_pipeline_creates_the_processed_output_directory():
    assert "os.makedirs(args.processed_out, exist_ok=True)" in TS


def test_the_processed_directory_is_only_created_for_a_local_path():
    line = next(
        ln for ln in TS.splitlines() if "os.makedirs(args.processed_out" in ln
    )
    guard = TS.splitlines()[TS.splitlines().index(line) - 1]
    assert "is_s3" in guard
