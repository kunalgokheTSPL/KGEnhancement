"""Pins that an empty extraction never overwrites the canonical outputs."""

from __future__ import annotations

import sys

import pandas as pd
import pytest

from p0.pipelines import run_pnid_from_pdfs


def _staging_with_a_misnamed_sheet(tmp_path):
    staging = tmp_path / "staging"
    staging.mkdir()
    with pd.ExcelWriter(staging / "extract.xlsx") as writer:
        pd.DataFrame([{"Equipment Tag": "P-1001"}]).to_excel(
            writer, sheet_name="WRONG_SHEET_NAME", index=False
        )
    return staging


def test_an_empty_extraction_exits_non_zero_and_writes_nothing(tmp_path, monkeypatch):
    """It used to write four zero-column frames over the previous run's equipment."""
    out = tmp_path / "out"
    (out / "entities").mkdir(parents=True)
    (out / "relationships").mkdir(parents=True)
    survivor = out / "entities" / "equipment.parquet"
    previous = pd.DataFrame([{"equipment_uid": "eq:1", "normalized_asset": "P-1001"}])
    previous.to_parquet(survivor)

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_pnid_from_pdfs.py",
            "--config_dir", "p0/config",
            "--pnid_pdf_dir", str(tmp_path / "pdfs"),
            "--pnid_work_dir", str(tmp_path / "work"),
            "--pnid_extractor_script", "/bin/true",
            "--out_dir", str(out),
            "--plant_code_id", "M014",
            "--pnid_staging_dir", str(_staging_with_a_misnamed_sheet(tmp_path)),
        ],
    )
    with pytest.raises(SystemExit) as exit_info:
        run_pnid_from_pdfs.main()
    assert exit_info.value.code != 0

    kept = pd.read_parquet(survivor)
    assert len(kept) == 1
    assert list(kept.columns) == ["equipment_uid", "normalized_asset"]
