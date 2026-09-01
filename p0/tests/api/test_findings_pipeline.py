"""The three findings stages are registered everywhere a stage has to be registered."""

from __future__ import annotations

import pathlib

from p0.api.services import findings, pipeline_progress as progress

_API = pathlib.Path(__file__).resolve().parents[2] / "api"
PIPELINE_ROUTER = (_API / "routers" / "pipeline.py").read_text()
STAGES = (_API / "services" / "pipeline_stages.py").read_text()
SCHEMA = (_API / "schemas" / "pipeline.py").read_text()

FINDINGS = findings.CONNECTORS


def test_the_pipeline_script_exists():
    script = _API.parent / "pipelines" / "run_findings_end_to_end.py"
    assert script.exists()


def test_every_findings_stage_maps_to_the_script():
    for stage in FINDINGS:
        assert f'"{stage}": "pipelines/run_findings_end_to_end.py"' in PIPELINE_ROUTER


def test_the_stage_pattern_accepts_them():
    assert "aif|gloc|lopc" in SCHEMA


def test_they_are_in_the_allowed_stage_list():
    for stage in FINDINGS:
        assert f'"{stage}"' in PIPELINE_ROUTER.split("allowed_stages = ")[1][:120]


def test_each_has_its_own_lock_so_runs_do_not_serialise_across_connectors():
    block = PIPELINE_ROUTER.split("_STAGE_LOCKS")[1][:400]
    for stage in FINDINGS:
        assert f'"{stage}": threading.Lock()' in block


def test_each_maps_to_a_flow_so_files_advance_past_staged():
    """Without _STAGE_TO_FLOW the run completes but the file never leaves 'staged'."""
    block = PIPELINE_ROUTER.split("_STAGE_TO_FLOW")[1][:300]
    for stage in FINDINGS:
        assert f'"{stage}": "{stage}"' in block


def test_each_maps_to_a_flow_for_the_progress_denominator():
    block = PIPELINE_ROUTER.split("_STAGE_FLOWS")[1][:400]
    for stage in FINDINGS:
        assert f'"{stage}": ("{stage}",)' in block


def test_stage_args_are_built_for_the_findings_stages():
    assert "elif stage in FINDINGS_STAGES:" in STAGES
    assert "--connector" in STAGES
    assert "--findings_in" in STAGES


def test_each_stage_has_a_determinate_file_wise_plan():
    for stage in FINDINGS:
        plan = progress.plan_for(stage)
        assert [p["phase"] for p in plan] == ["validate", "extract", "load", "finalize"]
        assert any(p.get("file_wise") for p in plan)
        assert progress.is_determinate(stage) is True


def test_the_plans_are_independent_copies():
    progress.plan_for("aif")[0]["weight"] = 999
    assert progress.plan_for("aif")[0]["weight"] == 10
    assert progress.plan_for("gloc")[0]["weight"] == 10


def test_the_weights_add_up_to_one_hundred():
    for stage in FINDINGS:
        assert sum(p["weight"] for p in progress.plan_for(stage)) == 100


def test_file_percent_is_scaled_into_the_extract_phase():
    assert progress.file_percent("aif", 0, 10) == 10
    assert progress.file_percent("aif", 10, 10) == 70


def test_each_stage_writes_to_its_own_processed_prefix():
    from p0.api.config import rustfs_processed_findings

    processed = {rustfs_processed_findings("PLANT_1", s) for s in FINDINGS}
    assert len(processed) == len(FINDINGS), "every findings connector must own its processed prefix"


def test_staging_is_shared_only_between_upd_event_and_trip_event():
    """upd_event and trip_event deliberately share one 'events' staging folder
    (two files uploaded together, routed to different tables downstream) — every
    other findings connector still gets its own staging prefix."""
    from p0.api.config import rustfs_processed_findings, rustfs_staging_findings

    staging = {s: rustfs_staging_findings("PLANT_1", s) for s in FINDINGS}
    processed = {rustfs_processed_findings("PLANT_1", s) for s in FINDINGS}

    solo = {c: p for c, p in staging.items() if c not in ("upd_event", "trip_event")}
    assert len(set(solo.values())) == len(solo), "non-events connectors must not share staging"

    if "upd_event" in staging and "trip_event" in staging:
        assert staging["upd_event"] == staging["trip_event"]

    assert not set(staging.values()) & processed


def test_the_run_response_carries_the_stepper_plan():
    """The UI can draw the stepper immediately instead of after its first poll."""
    block = PIPELINE_ROUTER.split('"message": "Pipeline started successfully"')[1][:700]
    for field in ("stages_planned", "is_determinate", "files_total", "progress_percent"):
        assert field in block, f"POST /pipeline/run does not return {field}"


def test_the_run_response_plan_matches_the_job_plan():
    """Two sources of truth for the plan would drift; both call plan_for(stage)."""
    assert PIPELINE_ROUTER.count("_progress.plan_for(req.stage)") == 2


def test_no_pipeline_prints_a_character_windows_cannot_encode():
    """A stray emoji in a print() killed a pnid run AFTER it had written everything."""
    import pathlib

    offenders = []
    for script in (pathlib.Path(__file__).resolve().parents[2] / "pipelines").glob("*.py"):
        for number, line in enumerate(script.read_text().splitlines(), 1):
            if "print(" in line and any(ord(c) > 127 for c in line):
                offenders.append(f"{script.name}:{number}")
    assert not offenders, "non-ASCII in print(): " + ", ".join(offenders)


def test_the_subprocess_is_pinned_to_utf8_both_ways():
    """Belt and braces: even if a print() regresses, neither side may use cp1252."""
    assert '"PYTHONIOENCODING": "utf-8"' in PIPELINE_ROUTER
    assert '"PYTHONUTF8": "1"' in PIPELINE_ROUTER
    assert 'encoding="utf-8"' in PIPELINE_ROUTER
    assert 'errors="replace"' in PIPELINE_ROUTER
