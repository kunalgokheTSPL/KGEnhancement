"""Tests for _doc_category — the document_type → review-tab category id.

Guards the duplicate-tab fix: one document type used to split into two tabs
because the slug fallback handled separators inconsistently (spaces → '-' but
'&', '/', '_' kept/treated differently). The canonical slug now collapses every
non-alphanumeric run to a single '-', so variants of the same type collapse to
one category.

Pure — no DB / RustFS.
"""

from __future__ import annotations

from p0.api.services.docs_taxonomy import _doc_category


def test_cause_effect_variants_collapse_to_one_category():
    a = _doc_category("Cause & Effect Matrix")
    b = _doc_category("cause_effect_matrix")
    c = _doc_category("cause-&-effect-matrix")
    assert a == b == c == "cause-effect-matrix"


def test_slug_strips_punctuation_and_parens():
    assert (
        _doc_category("HAZOP (Hazard and Operability Study)")
        == "hazop-hazard-and-operability-study"
    )
    assert (
        _doc_category("JSA / JHA (Job Safety / Hazard Analysis)")
        == "jsa-jha-job-safety-hazard-analysis"
    )
    assert _doc_category("Permit to Work (PTW)") == "permit-to-work-ptw"
    assert (
        _doc_category("Emergency Response Plan (ERP)") == "emergency-response-plan-erp"
    )


def test_known_types_map_to_stable_ids():
    assert _doc_category("Datasheets") == "datasheets"
    assert _doc_category("datasheet") == "datasheets"
    assert _doc_category("maintenance_manual") == "maintenance-manuals"


def test_empty_or_blank_falls_back_to_general():
    assert _doc_category("") == "general"
    assert _doc_category(None) == "general"
    assert _doc_category("   ") == "general"


def test_no_leading_or_trailing_hyphens():
    cat = _doc_category("  - Weird & Name -  ")
    assert not cat.startswith("-") and not cat.endswith("-")
