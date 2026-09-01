"""Commit says what actually happened: 200 wrote rows, 204 wrote none, 409 nothing staged."""

from __future__ import annotations

import ast
import pathlib

import pytest

API = pathlib.Path(__file__).resolve().parents[2] / "api"
CONTEXT = (API / "routers" / "context.py").read_text()
INTEGRATED = (API / "routers" / "integrated.py").read_text()

COMMITS = [
    "commitPnidContext",
    "commitDocsContext",
    "commitTsContext",
    "commitSapContext",
    "commitFindingsContext",
]


def test_a_204_carries_no_body_at_all():
    """JSONResponse(content=None) sends the four bytes 'null', which is not a valid 204."""
    from p0.api.responses import no_content

    response = no_content()
    assert response.status_code == 204
    assert response.body == b""


def test_the_two_empty_outcomes_are_told_apart():
    """Nothing staged is a caller mistake; staged-but-empty is a legitimate no-op."""
    from p0.api.responses import not_processed_yet, nothing_to_commit

    assert nothing_to_commit("aif", "PLANT_2").status_code == 204
    assert not_processed_yet("aif", "PLANT_2").status_code == 409


def test_the_409_names_the_step_the_caller_skipped():
    from p0.api.responses import not_processed_yet
    import json

    body = json.loads(not_processed_yet("gloc", "PLANT_2").body)
    assert body["success"] is False
    assert "gloc" in body["message"]
    assert "PLANT_2" in body["message"]
    assert "/pipeline/run" in body["message"]
    assert body["errors"][0]["field"] == "connector"


@pytest.mark.parametrize("fn", [c for c in COMMITS if c != "commitSapContext"])
def test_no_commit_reports_success_when_it_wrote_nothing(fn):
    block = CONTEXT.split(f"def {fn}(")[1].split("\n@router")[0]
    assert "nothing_to_commit(" in block, f"{fn} can still return 200 having written nothing"


def test_sap_commit_reports_success_when_it_wrote_nothing():
    """SAP's commit runs off-thread — commitSapContext itself always returns
    202 with a pipeline_job_id, so the no-op outcome can't come back as a
    synchronous 204 the way it does for the other connectors. It surfaces
    instead as a warning on the job the caller polls for."""
    block = CONTEXT.split("def _run_commit_sap_bg(")[1].split("\n@router")[0]
    assert 'job["warning"] = {"reason": "nothing_to_commit"}' in block, (
        "_run_commit_sap_bg can mark a job done without recording that nothing was written"
    )


@pytest.mark.parametrize("fn", ["commitTsContext", "commitFindingsContext"])
def test_a_commit_with_no_staged_state_is_loud(fn):
    block = CONTEXT.split(f"def {fn}(")[1].split("\n@router")[0]
    assert "not_processed_yet(" in block, f"{fn} swallows a missing pipeline run"
    assert "ReviewedStateMissing" in block


def test_sap_commit_with_no_staged_state_is_loud_via_the_job_status():
    """SAP can't return a synchronous 409 once the work is off-thread —
    commitSapContext always returns 202 before the background runner has
    even looked for reviewed state. A missing pipeline run instead fails
    the job that GET /pipeline/jobs/{jobId} reports back to the caller."""
    block = CONTEXT.split("def _run_commit_sap_bg(")[1].split("\n@router")[0]
    assert "ReviewedStateMissing" in block, "_run_commit_sap_bg swallows a missing pipeline run"
    assert 'job["status"] = "failed"' in block


def test_the_recompute_no_op_is_204_too():
    block = INTEGRATED.split("def recomputeIntegratedView(")[1]
    assert "return no_content()" in block


def test_the_reviewed_state_readers_have_exactly_one_source():
    """A second guess hides a broken pipeline behind a plausible-looking commit."""
    ts = CONTEXT.split("def _read_ts_rows_for_commit(")[1].split("\ndef ")[0]
    assert ts.count("_read_parquet_from_rustfs(") == 1
    assert "_read_stage_csvs(" not in ts
    assert "raise ReviewedStateMissing" in ts

    docs = CONTEXT.split("def commitDocsContext(")[1].split("\n@router")[0]
    assert "_read_stage_csvs(" not in docs, "docs still falls through to a second source"


def test_a_missing_reviewed_state_raises_rather_than_returning_empty():
    findings = CONTEXT.split("def _read_findings_rows(")[1].split("\ndef ")[0]
    assert "required" in findings
    assert "raise ReviewedStateMissing" in findings


def test_the_review_read_still_tolerates_a_missing_file():
    """Only the commit path is strict — the review page may legitimately be empty."""
    call = "rows = _read_findings_rows(plant_code_id, name)"
    assert call in CONTEXT, "getFindingsContext should read without required=True"


def test_no_commit_swallows_the_missing_state_exception():
    """A bare except would turn the loud 409 back into a silent success."""
    tree = ast.parse(CONTEXT)
    for fn in [n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name in COMMITS]:
        for handler in [n for n in ast.walk(fn) if isinstance(n, ast.ExceptHandler)]:
            if handler.type is None:
                pytest.fail(f"{fn.name} has a bare except that could hide a missing state")


def test_every_status_code_the_commit_can_return_is_documented():
    """The frontend has to branch on these, so they belong in the endpoint description."""
    for marker in ("204", "409"):
        assert marker in CONTEXT.split("def commitFindingsContext(")[0][-2000:], (
            f"commitFindingsContext does not document {marker}"
        )


def test_the_processed_output_is_kept_after_commit():
    """Deleting it made a committed connector look like one that never ran."""
    assert "_cleanup_stage" not in CONTEXT, (
        "commit still deletes the processed output; it is kept for reference now"
    )


def test_commit_does_not_destroy_the_state_its_own_409_depends_on():
    """If commit wiped the entity dir, a second commit's background runner
    would fail the job with 'never processed' instead of finding it again —
    the check itself lives in _run_commit_sap_bg now, not commitSapContext,
    since the endpoint returns 202 before that state is even read."""
    sap = CONTEXT.split("def commitSapContext(")[1].split("\n@router")[0]
    bg = CONTEXT.split("def _run_commit_sap_bg(")[1].split("\n@router")[0]
    assert "_cleanup_stage(" not in sap
    assert "_cleanup_stage(" not in bg
    assert "ReviewedStateMissing" in bg
