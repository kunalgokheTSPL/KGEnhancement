"""A restart must not orphan a run, and a partial run must be retryable."""

from __future__ import annotations

import os
import pathlib
import subprocess
import sys

from p0.api.services.pipeline_stages import _kill_by_pid, _pid_is_alive

PIPELINE_ROUTER = (
    pathlib.Path(__file__).resolve().parents[2] / "api" / "routers" / "pipeline.py"
).read_text()
MAIN = (pathlib.Path(__file__).resolve().parents[2] / "api" / "main.py").read_text()


def test_current_process_is_alive():
    assert _pid_is_alive(os.getpid()) is True


def test_nonsense_pids_are_not_alive():
    assert _pid_is_alive(None) is False
    assert _pid_is_alive("") is False
    assert _pid_is_alive(0) is False
    assert _pid_is_alive(-1) is False
    assert _pid_is_alive("not-a-pid") is False


def test_a_reaped_pid_is_not_alive():
    proc = subprocess.Popen([sys.executable, "-c", "pass"])
    proc.wait()
    assert _pid_is_alive(proc.pid) is False


def test_kill_by_pid_terminates_a_detached_process():
    """After an API restart the Popen handle is gone; only the pid survives."""
    proc = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(120)"],
        preexec_fn=os.setsid if hasattr(os, "setsid") else None,
    )
    try:
        assert _pid_is_alive(proc.pid) is True
        assert _kill_by_pid(proc.pid) is True
        proc.wait(timeout=30)
        assert _pid_is_alive(proc.pid) is False
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)


def test_kill_by_pid_on_a_dead_process_is_a_noop():
    proc = subprocess.Popen([sys.executable, "-c", "pass"])
    proc.wait()
    assert _kill_by_pid(proc.pid) is False


def test_cancel_falls_back_to_killing_by_pid():
    assert "_kill_by_pid(job.get(\"pid\"))" in PIPELINE_ROUTER


def test_reconciliation_exists_and_marks_interrupted():
    assert "def reconcile_interrupted_jobs(" in PIPELINE_ROUTER
    assert '"interrupted"' in PIPELINE_ROUTER


def test_reconciliation_skips_jobs_that_are_still_running():
    block = PIPELINE_ROUTER.split("def reconcile_interrupted_jobs(")[1].split("@router")[0]
    assert "_pid_is_alive(job.get(\"pid\"))" in block
    assert "if job_id in _pipeline_jobs" in block


def test_reconciliation_runs_at_startup():
    assert "reconcile_interrupted_jobs" in MAIN


def test_retry_endpoint_exists_with_scope():
    assert '"/jobs/{jobId}/retry"' in PIPELINE_ROUTER
    assert "failed_only" in PIPELINE_ROUTER


def test_retry_rejects_an_unknown_scope():
    block = PIPELINE_ROUTER.split("def retryPipelineJob(")[1]
    assert "scope must be either 'failed_only' or 'all'" in block
    assert "HTTPStatus.UNPROCESSABLE_ENTITY" in block


def test_retry_refuses_a_still_running_job():
    block = PIPELINE_ROUTER.split("def retryPipelineJob(")[1]
    assert "HTTPStatus.CONFLICT" in block
    assert "Cancel it before retrying" in block


def test_retry_reports_a_human_message_for_a_missing_job():
    block = PIPELINE_ROUTER.split("def retryPipelineJob(")[1]
    assert "No pipeline job exists with id" in block


def test_retry_starts_a_new_job_that_references_the_original():
    block = PIPELINE_ROUTER.split("def retryPipelineJob(")[1]
    assert '"retry_of": job_id_val' in block
    assert '"retry_scope": scope_val' in block


def test_retry_spawns_the_background_run_correctly():
    """spawn_with_context takes (target, args, kwargs, daemon) — not positional run args."""
    block = PIPELINE_ROUTER.split("def retryPipelineJob(")[1]
    assert "target=_run_pipeline_bg" in block
    assert "args=(" in block


def test_no_caller_passes_run_arguments_positionally_to_spawn():
    import ast

    tree = ast.parse(PIPELINE_ROUTER)
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and getattr(node.func, "id", "") == "spawn_with_context":
            assert len(node.args) <= 4, (
                f"spawn_with_context at line {node.lineno} passes {len(node.args)} "
                "positional args; it accepts target/args/kwargs/daemon"
            )
