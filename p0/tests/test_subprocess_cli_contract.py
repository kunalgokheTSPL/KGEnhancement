"""Every subprocess entry script still launches: its argparse CLI parses and --help exits 0.

A cheap tripwire for the subprocess half (pipelines + source_processing) that the API
suite never touches. Catches import breakage, flatten/path regressions, and argparse
mistakes in CI without needing real data or a database. Scripts are discovered by the
same predicate that makes them entrypoints (a __main__ guard plus argparse), so new
stage scripts are covered automatically.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[2]


def _entry_scripts() -> list[str]:
    scripts: list[str] = []
    for sub in ("pipelines", "source_processing"):
        for path in sorted((BACKEND / "p0" / sub).rglob("*.py")):
            if path.name == "__init__.py":
                continue
            text = path.read_text(errors="ignore")
            if "__main__" in text and "argparse" in text:
                scripts.append(str(path.relative_to(BACKEND)))
    return scripts


ENTRY_SCRIPTS = _entry_scripts()


@pytest.mark.parametrize(
    "script", ENTRY_SCRIPTS, ids=[s.rsplit("/", 1)[-1] for s in ENTRY_SCRIPTS]
)
def test_entry_script_help_launches(script):
    env = {**os.environ, "PYTHONPATH": str(BACKEND)}
    result = subprocess.run(
        [sys.executable, script, "--help"],
        cwd=BACKEND,
        capture_output=True,
        text=True,
        timeout=120,
        env=env,
    )
    assert result.returncode == 0, (
        f"{script} --help failed (rc={result.returncode}):\n{result.stderr[-600:]}"
    )
    assert "usage" in (result.stdout + result.stderr).lower()


def test_entry_script_discovery_did_not_collapse():
    """If the glob or the subprocess layout breaks, the count drops — catch that here."""
    assert len(ENTRY_SCRIPTS) >= 10, ENTRY_SCRIPTS
