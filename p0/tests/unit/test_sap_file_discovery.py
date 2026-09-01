"""SAP source files are found whatever case the supplier wrote the extension in."""

import pytest

from p0.source_processing.sap.sap_merging_xlsx import _find_files


@pytest.fixture
def drop(tmp_path):
    def _make(*names):
        for n in names:
            (tmp_path / n).write_bytes(b"")
        return str(tmp_path)

    return _make


def test_an_uppercase_extension_is_found_like_a_lowercase_one(drop):
    base = drop("WorkOrder.XLSX", "Notification.xlsx")
    found = {f.rsplit("/", 1)[-1] for f in _find_files(base)}
    assert found == {"WorkOrder.XLSX", "Notification.xlsx"}


def test_mixed_case_extensions_are_found_too(drop):
    base = drop("Mplan_M010.Xlsx", "legacy.XLS")
    found = {f.rsplit("/", 1)[-1] for f in _find_files(base)}
    assert found == {"Mplan_M010.Xlsx", "legacy.XLS"}


def test_unwanted_extensions_are_still_excluded(drop):
    base = drop("report.XLSX", "notes.pdf", "data.csv", "archive.ZIP")
    found = {f.rsplit("/", 1)[-1] for f in _find_files(base)}
    assert found == {"report.XLSX"}


def test_keyword_matching_is_unchanged_and_still_requires_all_of_them(drop):
    base = drop("WO_history_2024.XLSX", "WO_history.xlsx", "material_master.xlsx")
    found = {f.rsplit("/", 1)[-1] for f in _find_files(base, "history", "2024")}
    assert found == {"WO_history_2024.XLSX"}


def test_a_file_is_returned_once_not_once_per_extension_tried(drop):
    base = drop("orders.xlsx")
    assert len(_find_files(base)) == 1
