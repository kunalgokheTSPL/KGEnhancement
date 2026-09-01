"""A dataset with no contract passes validation without being checked, so every one has one."""

from __future__ import annotations

import ast
import pathlib

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[2]
COMMON = ROOT / "config" / "templates" / "_common"
CONTRACTS = yaml.safe_load((COMMON / "validation_contracts.yaml").read_text())
SECTIONS = ("required_after_rename_or_derive", "datasets")


def _contracted() -> set[str]:
    names: set[str] = set()
    for section in SECTIONS:
        entry = CONTRACTS.get(section) or {}
        if isinstance(entry, dict):
            names |= set(entry)
    return names


def _pnid_validated() -> set[str]:
    source = (ROOT / "pipelines" / "run_pnid_from_pdfs.py").read_text(encoding="utf-8")
    for node in ast.walk(ast.parse(source)):
        if not (isinstance(node, ast.Call) and getattr(node.func, "id", "") == "run_validations"):
            continue
        for kw in node.keywords:
            if kw.arg == "datasets" and isinstance(kw.value, ast.Dict):
                return {
                    k.value
                    for k in kw.value.keys
                    if isinstance(k, ast.Constant) and isinstance(k.value, str)
                }
    raise AssertionError("no run_validations(datasets={...}) call found")


def _timeseries_sources() -> set[str]:
    rename = yaml.safe_load((COMMON / "column_rename" / "timeseries_column_rename.yaml").read_text())
    return set(rename.get("sources") or {})


def test_the_pnid_pipeline_validates_a_known_set():
    assert len(_pnid_validated()) >= 5


def test_every_pnid_dataset_has_a_contract():
    missing = sorted(_pnid_validated() - _contracted())
    assert not missing, f"datasets validated with no contract: {missing}"


def test_every_timeseries_source_has_a_contract():
    missing = sorted(_timeseries_sources() - _contracted())
    assert not missing, f"timeseries sources with no contract: {missing}"


def test_a_contract_actually_requires_columns():
    required = CONTRACTS["required_after_rename_or_derive"]
    for name in sorted(_pnid_validated() | _timeseries_sources()):
        assert required.get(name), f"{name} has an empty contract, which checks nothing"
