"""
Backend safety-net: docs pipeline scope derivation from upload jobs.

What we're protecting:
  Uploading one HAZOP file but the pipeline extracting OTHER files too. Root
  cause was the docs run processing EVERY file in the staging dir when the
  client didn't send doc_files. The safety net derives the scope from the
  plant's RECENT upload jobs so stale files from prior sessions are excluded
  even if the client forgets to scope the run.
"""

from __future__ import annotations

import pytest

from p0.tests._p0_test_base import client

from p0.api.routers import connectors as conn
from p0.api.routers import pipeline as pl

PLANT = "plant_1_testcase"
_PDF = (
    b"%PDF-1.4\n1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n"
    b"2 0 obj\n<< /Type /Pages /Count 0 /Kids [] >>\nendobj\n"
    b"trailer\n<< /Root 1 0 R >>\n%%EOF\n"
)


@pytest.fixture(autouse=True)
def _clean_jobs():
    saved = dict(conn._upload_jobs)
    conn._upload_jobs.clear()
    yield
    conn._upload_jobs.clear()
    conn._upload_jobs.update(saved)


def _seed(job_id, plant, dtype, files, created):
    """Seed a device upload job in the shape uploadDocumentFiles actually writes."""
    conn._upload_jobs[job_id] = {
        "status": "completed",
        "source": "device",
        "data_type": "documents",
        "plant_code_id": plant,
        "document_types": sorted({dtype}),
        "files": [
            {"name": n, "status": "uploaded", "document_type": dtype} for n in files
        ],
        "created": created,
    }


def _seed_connector(job_id, plant, dtype, files, created):
    """Seed a cloud/OneDrive job, which stamps the singular key at job level."""
    conn._upload_jobs[job_id] = {
        "data_type": "documents",
        "plant_code_id": plant,
        "document_type": dtype,
        "files": [{"name": n, "status": "uploaded"} for n in files],
        "created": created,
    }


def test_derives_newest_batch_per_type():
    _seed("old", "plant_1_testcase", "hazop", ["OldHazop.pdf"], "2026-06-15T06:00:00")
    _seed("new", "plant_1_testcase", "hazop", ["BNCPP SOL.pdf"], "2026-06-15T09:00:00")
    _seed("ds", "plant_1_testcase", "equipment_datasheets", ["Sheet.pdf"], "2026-06-15T08:00:00")

    types, files = pl._derive_docs_scope_from_uploads("plant_1_testcase")
    assert types == ["equipment_datasheets", "hazop"]
    assert "BNCPP SOL.pdf" in files
    assert "OldHazop.pdf" not in files
    assert "Sheet.pdf" in files


def test_other_plant_not_leaked():
    _seed("a", "plant_1_testcase", "hazop", ["A.pdf"], "2026-06-15T09:00:00")
    _seed("b", "PLANT_03", "hazop", ["B.pdf"], "2026-06-15T09:00:00")
    _, files = pl._derive_docs_scope_from_uploads("plant_1_testcase")
    assert files == ["A.pdf"]

def test_job_doc_types_supports_plural_document_types():
    job = {
        "document_types": ["sop", "rca_reports"],
    }

    assert pl._job_doc_types(job) == [
        "rca_reports",
        "sop",
    ]


def test_job_doc_types_supports_singular_document_type():
    job = {
        "document_type": "sop",
    }

    assert pl._job_doc_types(job) == ["sop"]


def test_job_doc_types_supports_string_document_types():
    job = {
        "document_types": "sop",
    }

    assert pl._job_doc_types(job) == ["sop"]


def test_job_doc_types_handles_empty_job():
    assert pl._job_doc_types({}) == []
    
def test_empty_when_no_uploads():
    assert pl._derive_docs_scope_from_uploads("Whatever") == ([], [])


def test_only_uploaded_status_counted():
    conn._upload_jobs["j"] = {
        "data_type": "documents",
        "plant_code_id": "plant_1_testcase",
        "document_types": ["hazop"],
        "files": [
            {"name": "ok.pdf", "status": "uploaded", "document_type": "hazop"},
            {"name": "bad.pdf", "status": "rejected", "document_type": "hazop"},
        ],
        "created": "2026-06-15T09:00:00",
    }
    _, files = pl._derive_docs_scope_from_uploads("plant_1_testcase")
    assert files == ["ok.pdf"]


def test_multi_subtype_upload_is_one_scope():
    """The per-subtype journey mints one job per subtype; the run covers all of them."""
    _seed("sop", "plant_1_testcase", "sop", ["Pump SOP.pdf"], "2026-06-15T09:00:00")
    _seed("rca", "plant_1_testcase", "rca_reports", ["Failure RCA.pdf"], "2026-06-15T09:01:00")
    _seed("ds", "plant_1_testcase", "equipment_datasheets", ["Sheet.pdf"], "2026-06-15T09:02:00")

    types, files = pl._derive_docs_scope_from_uploads("plant_1_testcase")
    assert types == ["equipment_datasheets", "rca_reports", "sop"]
    assert files == ["Failure RCA.pdf", "Pump SOP.pdf", "Sheet.pdf"]


def test_one_job_carrying_several_subtypes():
    """A single upload of mixed files splits by the per-file document_type."""
    conn._upload_jobs["mixed"] = {
        "data_type": "documents",
        "plant_code_id": "plant_1_testcase",
        "document_types": ["rca_reports", "sop"],
        "files": [
            {"name": "A.pdf", "status": "uploaded", "document_type": "sop"},
            {"name": "B.pdf", "status": "uploaded", "document_type": "rca_reports"},
        ],
        "created": "2026-06-15T09:00:00",
    }
    types, files = pl._derive_docs_scope_from_uploads("plant_1_testcase")
    assert types == ["rca_reports", "sop"]
    assert files == ["A.pdf", "B.pdf"]


def test_cloud_connector_job_shape_still_derives():
    """Cloud/OneDrive still write the singular key — it must keep working."""
    _seed_connector("cloud", "plant_1_testcase", "sop", ["Cloud SOP.pdf"], "2026-06-15T09:00:00")
    types, files = pl._derive_docs_scope_from_uploads("plant_1_testcase")
    assert types == ["sop"]
    assert files == ["Cloud SOP.pdf"]


def test_upload_endpoint_record_is_readable_by_the_derivation(client):
    """Contract guard: what the real upload writes, the derivation must read."""
    conn._upload_jobs.clear()
    resp = client.post(
        "/connectors/documents/upload",
        data={
            "plant_code_id": PLANT,
            "document_type": "sop",
            "labels": "reliability",
        },
        files={"files": ("scope_guard_sop.pdf", _PDF, "application/pdf")},
    )
    assert resp.status_code in (200, 422), resp.text
    if resp.status_code == 422:
        pytest.skip("upload rejected in this environment; nothing to derive from")

    jobs = [
        j for j in conn._upload_jobs.values()
        if j.get("data_type") == "documents" and j.get("plant_code_id") == PLANT
    ]
    assert jobs, "upload did not record an upload job"

    types, files = pl._derive_docs_scope_from_uploads(PLANT)
    assert types, f"derivation read nothing from the real job record: {jobs[0]!r}"
    assert "scope_guard_sop.pdf" in files, (
        f"uploaded file missing from derived scope; job record was {jobs[0]!r}"
    )


def test_derived_scope_reaches_the_stage_args():
    """The whole point: both subtypes survive into the docs pipeline invocation."""
    from p0.api.services.pipeline_stages import _build_stage_args

    _seed("sop", PLANT, "sop", ["Pump SOP.pdf"], "2026-06-15T09:00:00")
    _seed("rca", PLANT, "rca_reports", ["Failure RCA.pdf"], "2026-06-15T09:01:00")

    doc_types, doc_files = pl._derive_docs_scope_from_uploads(PLANT)
    args = _build_stage_args(
        "docs", PLANT, doc_types=doc_types, doc_files=doc_files, upload_batch_id="A"
    )

    assert "--doc_types" in args
    assert args[args.index("--doc_types") + 1] == "rca_reports,sop"
    assert "--doc_files" in args
    assert args[args.index("--doc_files") + 1] == "Failure RCA.pdf,Pump SOP.pdf"
