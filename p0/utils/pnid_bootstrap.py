from __future__ import annotations

import glob
import os
import subprocess
import sys


def _has_xlsx(folder: str | None) -> bool:
    return bool(
        folder and os.path.isdir(folder) and glob.glob(os.path.join(folder, "*.xlsx"))
    )


def _has_pdf(folder: str | None) -> bool:
    return bool(
        folder and os.path.isdir(folder) and glob.glob(os.path.join(folder, "*.pdf"))
    )


def ensure_pnid_reference_dir(
    *,
    config_dir: str,
    pnid_dir: str | None = None,
    pnid_pdf_dir: str | None = None,
    pnid_extract_dir: str | None = None,
    pnid_extractor_script: str | None = None,
) -> str | None:
    """Resolve a P&ID reference directory containing .xlsx files."""
    repo_root = os.path.abspath(os.path.join(config_dir, ".."))

    if _has_xlsx(pnid_dir):
        return pnid_dir

    auto_candidates = [
        pnid_dir,
        os.path.join(repo_root, "data", "work", "pnid_extract"),
        os.path.join(repo_root, "data", "staging", "pnid"),
    ]
    for candidate in auto_candidates:
        if _has_xlsx(candidate):
            return candidate

    resolved_pdf_dir = pnid_pdf_dir or os.path.join(
        repo_root, "data", "staging", "pnid"
    )
    resolved_extract_dir = pnid_extract_dir or os.path.join(
        repo_root, "data", "work", "pnid_extract"
    )
    resolved_script = pnid_extractor_script or os.path.join(
        repo_root,
        "source_processing",
        "pnid",
        "complete_pnid_extraction_without_ui.py",
    )

    if not (_has_pdf(resolved_pdf_dir) and os.path.isfile(resolved_script)):
        return None

    os.makedirs(resolved_extract_dir, exist_ok=True)

    if _has_xlsx(resolved_extract_dir):
        return resolved_extract_dir

    cmd = [
        sys.executable,
        resolved_script,
        "--input",
        resolved_pdf_dir,
        "--output",
        resolved_extract_dir,
    ]

    print(f"    [pnid_bootstrap] running source processing: {resolved_script}")
    try:
        subprocess.check_call(cmd)
    except Exception as exc:
        print(f"    [pnid_bootstrap] WARNING: P&ID source processing failed: {exc}")
        return None

    return resolved_extract_dir if _has_xlsx(resolved_extract_dir) else None
