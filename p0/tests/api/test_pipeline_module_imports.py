"""
pipeline.py imports the same names whether it is loaded as a package or as a script.

pipeline.py guards its imports with ``if __package__: ... else: ...``. In production the
module is always loaded as a package, so the ``if`` branch runs — but four names
(``_flow``, ``upsert_job``, ``_fs``, ``get_object_fs``) were only ever imported in the
``else`` branch. Every use of them raised NameError, and each caller swallowed it:

  * _persist_job          -> "pipeline_jobs upsert failed: name 'upsert_job' is not defined"
                             (no pipeline job ever reached the DB)
  * _advance_flow_for_stage -> "flow file-name resolution failed: name '_flow' is not defined"
                             (flow_state never advanced for a pipeline run)
  * _latest_staged_pnid_file -> NameError swallowed by `except Exception: return []`
                             (P&ID runs never found the staged file)

These tests pin the module surface so the two branches can never drift apart again.
"""

from __future__ import annotations

import logging

from unittest.mock import patch

import pytest

from p0.api.routers import pipeline as pl


@pytest.mark.parametrize("name", ["_flow", "upsert_job", "_fs", "get_object_fs"])
def test_name_is_bound_at_module_scope(name):
    """Loaded as a package, pipeline.py must still bind every name its body uses."""
    assert pl.__package__, "precondition: imported as a package"
    assert hasattr(pl, name), (
        f"{name} is not bound at module scope — the `if __package__:` branch is "
        f"missing an import the `else` branch has, so every use raises NameError"
    )


def _warnings_from(fn, *args, **kwargs) -> list[str]:
    """Run fn and return whatever its swallowing `except` logged."""
    caught: list[str] = []

    class _H(logging.Handler):
        def emit(self, record):
            caught.append(record.getMessage())

    h = _H()
    pl._log.addHandler(h)
    try:
        fn(*args, **kwargs)
    finally:
        pl._log.removeHandler(h)
    return caught


def test_persist_job_does_not_die_on_a_missing_name(monkeypatch):
    """_persist_job must reach upsert_job, not swallow a NameError."""
    seen = {}
    monkeypatch.setattr(
        pl, "upsert_job", lambda jid, job: seen.update(id=jid, job=job), raising=False
    )
    pl._pipeline_jobs["job-import-testcase"] = {"status": "running"}
    try:
        warnings = _warnings_from(pl._persist_job, "job-import-testcase")
    finally:
        pl._pipeline_jobs.pop("job-import-testcase", None)

    assert not any("is not defined" in w for w in warnings), warnings
    assert seen.get("id") == "job-import-testcase"


def test_advance_flow_for_stage_does_not_die_on_a_missing_name():
    """_advance_flow_for_stage must reach the flow module, not swallow a NameError."""
    warnings = _warnings_from(
        pl._advance_flow_for_stage, "pnid", "plant_1_testcase", file_names=["a.pdf"]
    )
    assert not any("is not defined" in w for w in warnings), warnings


def test_latest_staged_pnid_file_picks_the_newest_pdf():
    """Previously returned [] for every input — get_object_fs raised NameError."""

    class _FakeFS:
        def ls(self, *_a, **_k):
            return [
                {
                    "name": f"staging-pt/p0/staging-pt/plant_1_testcase/pnid/{n}",
                    "LastModified": m,
                }
                for n, m in [
                    ("PnID.pdf", "2026-06-15T06:00:00"),
                    ("05-BNCPP-B-B-1100_TTJT-A_RECEIVER.pdf", "2026-06-15T12:00:00"),
                    ("PnID.xlsx", "2026-06-15T12:00:00"),
                ]
            ]

    with patch.object(pl, "get_object_fs", return_value=_FakeFS()):
        assert pl._latest_staged_pnid_file("plant_1_testcase") == [
            "05-BNCPP-B-B-1100_TTJT-A_RECEIVER.pdf"
        ]
