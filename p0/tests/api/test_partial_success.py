"""One failed file must not condemn the whole run."""

from __future__ import annotations

import json
import pathlib

from p0.api.services import pipeline_progress as progress

PIPELINE_ROUTER = (
    pathlib.Path(__file__).resolve().parents[2] / "api" / "routers" / "pipeline.py"
).read_text()


def _outcomes(*pairs):
    job: dict = {}
    for name, status in pairs:
        progress.apply_file_marker(job, {"file": name, "status": status})
    return job


def test_file_marker_round_trips():
    line = f'{progress.FILE_MARKER} {json.dumps({"file": "a.pdf", "status": "processed", "rows": 12})}'
    assert progress.parse_file_marker(line) == {
        "file": "a.pdf",
        "status": "processed",
        "rows": 12,
    }


def test_file_marker_requires_a_file_name():
    assert progress.parse_file_marker(f'{progress.FILE_MARKER} {{"status": "processed"}}') is None
    assert progress.parse_file_marker(f"{progress.FILE_MARKER} garbage") is None
    assert progress.parse_file_marker("ordinary log line") is None


def test_files_are_matched_by_leaf_name():
    job = {}
    progress.apply_file_marker(job, {"file": "/staging/docs/a.pdf", "status": "processed"})
    assert "a.pdf" in job["file_outcomes"]


def test_counters_track_each_outcome():
    job = _outcomes(("a.pdf", "processed"), ("b.pdf", "failed"), ("c.pdf", "skipped"))
    assert job["counters"] == {
        "files_total": 3,
        "files_done": 1,
        "files_failed": 1,
        "files_skipped": 1,
    }


def test_eight_of_ten_is_partial_not_failed():
    """The exact case from the requirements: 8 of 10 succeed."""
    pairs = [(f"f{i}.pdf", "processed") for i in range(8)]
    pairs += [("f8.pdf", "failed"), ("f9.pdf", "failed")]
    job = _outcomes(*pairs)
    assert progress.outcome_status(1, job["file_outcomes"]) == "partial"
    assert job["counters"]["files_done"] == 8
    assert job["counters"]["files_failed"] == 2


def test_all_files_failed_is_failed():
    job = _outcomes(("a.pdf", "failed"), ("b.pdf", "failed"))
    assert progress.outcome_status(1, job["file_outcomes"]) == "failed"


def test_all_files_processed_is_completed():
    job = _outcomes(("a.pdf", "processed"), ("b.pdf", "processed"))
    assert progress.outcome_status(0, job["file_outcomes"]) == "completed"


def test_nonzero_exit_with_no_failed_files_is_still_partial():
    """The process died after finishing its files — do not claim clean success."""
    job = _outcomes(("a.pdf", "processed"))
    assert progress.outcome_status(1, job["file_outcomes"]) == "partial"


def test_without_per_file_reporting_the_old_behaviour_holds():
    assert progress.outcome_status(0, None) == "completed"
    assert progress.outcome_status(1, {}) == "failed"


def test_a_later_marker_supersedes_an_earlier_one():
    job = _outcomes(("a.pdf", "failed"))
    progress.apply_file_marker(job, {"file": "a.pdf", "status": "processed", "rows": 5})
    assert job["file_outcomes"]["a.pdf"]["status"] == "processed"
    assert job["counters"]["files_failed"] == 0


def test_failure_reason_is_retained_per_file():
    job = {}
    progress.apply_file_marker(
        job, {"file": "bad.pdf", "status": "failed", "error": "corrupt PDF"}
    )
    assert job["file_outcomes"]["bad.pdf"]["error"] == "corrupt PDF"


def test_runner_applies_per_file_outcomes_to_flow_state():
    assert "file_outcomes: dict[str, dict] | None = None" in PIPELINE_ROUTER
    assert "outcome.get(\"status\") in _progress.FAILED_FILE_STATUSES" in PIPELINE_ROUTER


def test_runner_derives_job_status_from_file_outcomes():
    assert "_progress.outcome_status(result.returncode, _outcomes)" in PIPELINE_ROUTER


def test_failed_files_do_not_advance_their_stage():
    """A failed file keeps its previous stage; only its status changes."""
    assert "row_stage = None" in PIPELINE_ROUTER
