"""Decision-logic tests for the two quality-gate aggregators — the functions whose
verdict the worker raises on (run_staging_quality_gate, run_cdm_post_validation).

These were at 0% coverage: the leaf-check unit tests never exercised the roll-up that
turns a BLOCK check into gate_status="BLOCKED". That roll-up is fragile — some leaves
report "severity", others "status" — so a wrong key would silently downgrade a BLOCK
to PASSED and let bad data reach Postgres. Each verdict below is pinned, and both
roll-up paths (missing-file via severity, missing-column via status) are covered.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from p0.utils.cdm_post_validation import run_cdm_post_validation
from p0.utils.staging_quality_gate import (
    run_staging_gate_or_raise,
    run_staging_quality_gate,
)

BACKEND = Path(__file__).resolve().parents[3]
_REAL_CONFIG_DIR = BACKEND / "p0" / "config"

_MIN_CONFIG = {
    "settings": {
        "natural_key_null_threshold": 0.02,
        "drift_threshold": 0.50,
        "encoding": "utf-8",
    },
    "sources": {
        "sap": {
            "staging_subdir": "sap",
            "accepted_extensions": [".csv"],
            "delimiter": ",",
            "required_files": ["EQUI.csv"],
            "datasets": {
                "equipment": {
                    "file_stem": "EQUI",
                    "required_columns": ["equipment_id", "name"],
                    "natural_keys": ["equipment_id"],
                }
            },
        }
    },
}


def _config(tmp_path: Path) -> Path:
    p = tmp_path / "sqg.yaml"
    p.write_text(yaml.safe_dump(_MIN_CONFIG))
    return p


def _staging(tmp_path: Path, equi_csv: str | None) -> Path:
    sap = tmp_path / "staging" / "sap"
    sap.mkdir(parents=True)
    if equi_csv is not None:
        (sap / "EQUI.csv").write_text(equi_csv)
    return tmp_path / "staging"


def _run_staging(tmp_path: Path, equi_csv: str | None) -> dict:
    return run_staging_quality_gate(
        staging_dir=_staging(tmp_path, equi_csv),
        config_path=_config(tmp_path),
        plant_code_id="aggtest_plant",
        report_dir=tmp_path / "reports",
    )


def test_staging_gate_passes_on_clean_data(tmp_path):
    rep = _run_staging(tmp_path, "equipment_id,name\nE1,Pump\nE2,Valve\n")
    assert rep["gate_status"] == "PASSED"
    assert rep["blocked_reasons"] == []


def test_staging_gate_blocks_on_missing_required_file(tmp_path):
    # EQUI.csv absent -> BLOCK rolled up via the leaf's "severity" key
    rep = _run_staging(tmp_path, None)
    assert rep["gate_status"] == "BLOCKED"
    assert any("required file missing" in r for r in rep["blocked_reasons"])


def test_staging_gate_blocks_on_missing_required_column(tmp_path):
    # file present but no equipment_id column -> BLOCK rolled up via the leaf's "status" key
    rep = _run_staging(tmp_path, "name\nPump\n")
    assert rep["gate_status"] == "BLOCKED"
    assert any("missing columns" in r for r in rep["blocked_reasons"])


def test_staging_gate_warns_but_does_not_block_on_duplicate_keys(tmp_path):
    rep = _run_staging(tmp_path, "equipment_id,name\nE1,Pump\nE1,Valve\nE2,Tank\n")
    assert rep["gate_status"] == "WARNINGS"
    assert rep["blocked_reasons"] == []
    assert any("duplicate" in r.lower() for r in rep["warning_reasons"])


def test_cdm_gate_blocks_when_core_tables_missing(tmp_path):
    final_dir = tmp_path / "final"
    final_dir.mkdir()
    rep = run_cdm_post_validation(
        final_dir=final_dir,
        config_dir=_REAL_CONFIG_DIR,
        report_dir=tmp_path / "reports",
    )
    assert rep["gate_status"] == "BLOCKED"
    assert any("Core table" in r for r in rep["blocked_reasons"])


def test_gate_or_raise_fails_loudly_on_missing_config(tmp_path):
    # a config_dir with no template -> the resolved path does not exist -> hard fail,
    # NOT a silent skip (the hole this fix closes)
    (tmp_path / "staging").mkdir()
    with pytest.raises(RuntimeError, match="config not found"):
        run_staging_gate_or_raise(
            tmp_path, tmp_path / "staging", "aggtest_plant", tmp_path / "reports"
        )


def test_gate_or_raise_raises_on_blocked_verdict(tmp_path):
    # real config, but an empty staging dir -> required files missing -> BLOCKED -> raise
    (tmp_path / "staging").mkdir()
    with pytest.raises(RuntimeError, match="BLOCKED"):
        run_staging_gate_or_raise(
            _REAL_CONFIG_DIR, tmp_path / "staging", "aggtest_plant", tmp_path / "reports"
        )
