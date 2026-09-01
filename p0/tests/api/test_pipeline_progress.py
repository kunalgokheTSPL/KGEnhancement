"""Progress is server-computed, monotonic, and honest about what it cannot count."""

from __future__ import annotations

import json
import pathlib
import subprocess
import sys

import pytest

from p0.api.services import pipeline_progress as progress

PIPELINE_ROUTER = (
    pathlib.Path(__file__).resolve().parents[2] / "api" / "routers" / "pipeline.py"
).read_text()


def test_marker_round_trips():
    payload = {"phase": "extract", "items_done": 3, "items_total": 10}
    line = f"{progress.MARKER} {json.dumps(payload)}\n"
    assert progress.parse_marker(line) == payload


def test_ordinary_output_is_not_a_marker():
    assert progress.parse_marker("[docs] Processing: manual.pdf") is None
    assert progress.parse_marker("") is None


def test_malformed_marker_is_ignored_not_raised():
    assert progress.parse_marker(f"{progress.MARKER} not-json") is None
    assert progress.parse_marker(f"{progress.MARKER} [1,2,3]") is None
    assert progress.parse_marker(f"{progress.MARKER}   ") is None


def test_known_stages_are_determinate_and_unknown_ones_are_not():
    """Every real stage counts files; only an unrecognised stage is indeterminate."""
    for stage in ("docs", "full", "ts", "pnid", "sap"):
        assert progress.is_determinate(stage) is True
    assert progress.is_determinate("nonexistent") is False


def test_plan_is_returned_up_front_so_the_stepper_can_render():
    plan = progress.plan_for("docs")
    assert [p["phase"] for p in plan] == ["validate", "extract", "load", "finalize"]
    assert all("label" in p and "weight" in p for p in plan)


def test_plan_is_a_copy_callers_cannot_mutate_the_module_state():
    progress.plan_for("docs")[0]["weight"] = 999
    assert progress.plan_for("docs")[0]["weight"] == 5


def test_unknown_stage_falls_back_to_an_indeterminate_plan():
    assert progress.plan_for("nonexistent")
    assert progress.is_determinate("nonexistent") is False


def test_percent_is_weighted_by_phase():
    job = {}
    progress.apply_marker(job, "docs", {"phase": "validate", "status": "succeeded"})
    assert job["progress_percent"] == 5

    progress.apply_marker(
        job, "docs", {"phase": "extract", "items_done": 5, "items_total": 10}
    )
    assert job["progress_percent"] == 5 + 35


def test_percent_never_goes_backwards():
    job = {}
    progress.apply_marker(
        job, "docs", {"phase": "extract", "items_done": 9, "items_total": 10}
    )
    high = job["progress_percent"]
    progress.apply_marker(
        job, "docs", {"phase": "extract", "items_done": 1, "items_total": 10}
    )
    assert job["progress_percent"] == high


def test_starting_a_new_phase_closes_the_previous_one():
    job = {}
    progress.apply_marker(job, "docs", {"phase": "validate"})
    progress.apply_marker(job, "docs", {"phase": "extract"})
    assert job["phases"]["validate"]["status"] == "succeeded"
    assert job["phases"]["extract"]["status"] == "running"


def test_current_item_is_surfaced_for_the_ui():
    job = {}
    progress.apply_marker(
        job,
        "docs",
        {"phase": "extract", "current_item": "manual.pdf", "items_done": 2, "items_total": 8},
    )
    assert job["current_item"] == "manual.pdf"
    assert job["current_phase"] == "extract"
    assert (job["items_done"], job["items_total"]) == (2, 8)


def test_marker_without_a_phase_is_ignored():
    job = {"progress_percent": 12}
    progress.apply_marker(job, "docs", {"items_done": 1})
    assert job["progress_percent"] == 12


def test_only_a_successful_run_reaches_100():
    job = {}
    progress.apply_marker(job, "docs", {"phase": "extract", "items_done": 1, "items_total": 10})
    mid = job["progress_percent"]
    assert mid < 100

    failed = dict(job, phases=dict(job["phases"]))
    progress.finalise_progress(failed, succeeded=False)
    assert failed["progress_percent"] == mid

    progress.finalise_progress(job, succeeded=True)
    assert job["progress_percent"] == 100


def test_unknown_stage_reports_an_indeterminate_flag():
    """A stage we have no plan for must say so rather than invent a number."""
    job = {}
    progress.apply_marker(job, "nonexistent", {"phase": "process"})
    assert job["is_determinate"] is False


def test_stream_process_reads_lines_while_running():
    """The whole point: output must arrive before the child exits."""
    script = (
        "import sys, time\n"
        "for i in range(3):\n"
        "    print('line %d' % i, flush=True)\n"
        "sys.stderr.write('warned\\n')\n"
    )
    proc = subprocess.Popen(
        [sys.executable, "-c", script],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    seen: list[str] = []
    code, out, err = progress.stream_process(proc, timeout=30, on_line=seen.append)
    assert code == 0
    assert len(seen) == 3
    assert "line 0" in out and "line 2" in out
    assert "warned" in err


def test_stream_process_kills_on_timeout():
    proc = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(60)"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    killed: list[object] = []

    def _kill(p):
        killed.append(p)
        p.kill()

    code, _, _ = progress.stream_process(proc, timeout=1, on_kill=_kill)
    assert killed, "on_kill was not invoked"
    assert code != 0


def test_a_failing_line_handler_does_not_break_the_run():
    proc = subprocess.Popen(
        [sys.executable, "-c", "print('hello', flush=True)"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    def _boom(_line):
        raise RuntimeError("handler exploded")

    code, out, _ = progress.stream_process(proc, timeout=30, on_line=_boom)
    assert code == 0
    assert "hello" in out


def test_runner_no_longer_blocks_on_communicate():
    """The blocking read was the reason no progress existed."""
    assert "proc.communicate(timeout=_timeout)" not in PIPELINE_ROUTER
    assert "_progress.stream_process(" in PIPELINE_ROUTER


def test_job_is_seeded_with_its_plan_at_queue_time():
    assert '"stages_planned": _progress.plan_for(req.stage)' in PIPELINE_ROUTER
    assert '"progress_percent": 0' in PIPELINE_ROUTER


def test_failure_paths_finalise_progress_as_unsuccessful():
    assert (
        PIPELINE_ROUTER.count(
            "_progress.finalise_progress(_pipeline_jobs[pipeline_job_id], succeeded=False)"
        )
        == 2
    )


def test_success_path_finalises_from_the_real_outcome_not_a_hardcoded_true():
    """A run whose files all failed must not finalise as a success."""
    assert '_final_status != "failed"' in PIPELINE_ROUTER


def test_every_stage_declares_a_file_wise_phase():
    """The API knows the file set from flow_state, so every stage can count."""
    for stage in ("docs", "sap", "ts", "pnid", "full"):
        plan = progress.plan_for(stage)
        assert any(p.get("file_wise") for p in plan), f"{stage} has no file-wise phase"


def test_every_stage_is_now_determinate():
    for stage in ("docs", "sap", "ts", "pnid", "full"):
        assert progress.is_determinate(stage) is True


def test_file_percent_is_scaled_into_the_file_wise_phase():
    """docs: validate=5 before, extract=70 file-wise — half the files is 5 + 35."""
    assert progress.file_percent("docs", 0, 10) == 5
    assert progress.file_percent("docs", 5, 10) == 40
    assert progress.file_percent("docs", 10, 10) == 75


def test_file_percent_never_exceeds_the_phase_span():
    assert progress.file_percent("docs", 99, 10) == 75


def test_file_percent_handles_no_denominator():
    assert progress.file_percent("docs", 0, 0) == 0


def test_file_outcomes_drive_the_percentage():
    job = {"files_total": 4}
    for i in range(4):
        progress.apply_file_marker(job, "sap", {"file": f"t{i}.csv", "status": "processed"})
    assert job["items_done"] == 4
    assert job["items_total"] == 4
    assert job["is_determinate"] is True
    assert job["progress_percent"] == progress.file_percent("sap", 4, 4)


def test_declared_total_wins_over_files_seen_so_far():
    """One file reported out of ten must read 1/10, not 1/1."""
    job = {"files_total": 10}
    progress.apply_file_marker(job, "sap", {"file": "a.csv", "status": "processed"})
    assert job["counters"]["files_total"] == 10
    assert job["items_done"] == 1


def test_failed_and_skipped_files_still_advance_progress():
    job = {"files_total": 3}
    progress.apply_file_marker(job, "sap", {"file": "a", "status": "processed"})
    progress.apply_file_marker(job, "sap", {"file": "b", "status": "failed"})
    progress.apply_file_marker(job, "sap", {"file": "c", "status": "skipped"})
    assert job["items_done"] == 3


def test_file_marker_still_accepts_the_two_argument_form():
    job = {}
    progress.apply_file_marker(job, {"file": "a.pdf", "status": "processed"})
    assert "a.pdf" in job["file_outcomes"]


def test_current_item_tracks_the_file_being_processed():
    job = {"files_total": 2}
    progress.apply_file_marker(job, "pnid", {"file": "/stage/Unit 100.xlsx", "status": "processed"})
    assert job["current_item"] == "Unit 100.xlsx"


def test_file_count_is_seeded_from_the_client_batch_not_the_job_id_alias():
    """upload_batch_id falls back to pipeline_job_id, which matches no flow row."""
    assert "_pending_file_count(\n                req.plant_code_id, req.stage, req.upload_batch_id\n            )" in PIPELINE_ROUTER
    assert '"files_total": _pending_file_count(req.plant_code_id, req.stage, upload_batch_id)' not in PIPELINE_ROUTER
